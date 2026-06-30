"""
End-to-end integration tests for bundle_processor.

These tests use minimal fixture YAMLs but make real calls to Quay.io and GitHub.
They require:
  - RHOAI_QUAY_API_TOKEN env var
  - Network access to quay.io and raw.githubusercontent.com

Run:
    cd utils/processors
    python -m pytest test/test_bundle_processor.py -v -m live
"""

import os
import sys
import json
import shutil
import subprocess
import tempfile
import pytest
import yaml
from unittest.mock import patch, wraps

processors_root = os.path.abspath(os.path.join(os.path.dirname(__file__), f".."))
if processors_root not in sys.path:
    sys.path.insert(0, processors_root)

live = pytest.mark.live
requires_quay_token = pytest.mark.skipif(
    'RHOAI_QUAY_API_TOKEN' not in os.environ,
    reason='RHOAI_QUAY_API_TOKEN not set'
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), 'fixtures', 'bundle-processor')


@pytest.fixture
def work_dir(tmp_path):
    """Copy fixtures to a temp directory so process() can write output files."""
    shutil.copytree(FIXTURES_DIR, tmp_path / 'fixtures')
    return tmp_path / 'fixtures'


class TestBundleProcessorE2E:

    @live
    @requires_quay_token
    def test_process_completes_successfully(self, work_dir):
        """Full process() run with minimal fixtures and real Quay/GitHub calls."""
        from importlib import import_module
        bp_module = import_module('bundle-processor')
        bundle_processor = bp_module.bundle_processor

        bundle_dir = work_dir / 'bundle'
        config_dir = work_dir / 'config'
        tekton_dir = work_dir / 'tekton'
        output_csv = str(work_dir / 'output' / 'rhods-operator.clusterserviceversion.yaml')
        os.makedirs(work_dir / 'output', exist_ok=True)

        processor = bundle_processor(
            build_config_path=str(config_dir / 'build-config.yaml'),
            bundle_csv_path=str(bundle_dir / 'manifests' / 'rhods-operator.clusterserviceversion.yaml'),
            patch_yaml_path=str(bundle_dir / 'bundle-patch.yaml'),
            rhoai_version='rhoai-3.4',
            output_file_path=output_csv,
            annotation_yaml_path=str(bundle_dir / 'metadata' / 'annotations.yaml'),
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=str(tekton_dir / 'push-pipeline.yaml'),
            build_type='ci',
        )
        processor.process()

        # Verify output CSV was written
        assert os.path.isfile(output_csv)
        with open(output_csv) as f:
            csv_dict = yaml.safe_load(f)

        # Verify basic CSV structure
        assert csv_dict['metadata']['name'] == 'rhods-operator.3.4.1'
        assert csv_dict['spec']['version'] == '3.4.1'

        # Verify operator image was updated to registry.redhat.io
        container_image = csv_dict['metadata']['annotations']['containerImage']
        assert container_image.startswith('registry.redhat.io/')
        assert '@sha256:' in container_image

        container_spec = csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]
        assert container_spec['image'] == container_image

        # Verify env vars were populated
        env_vars = container_spec['env']
        env_names = [e['name'] for e in env_vars]
        assert 'OPERATOR_NAME' in env_names
        assert any(name.startswith('RELATED_IMAGE_') for name in env_names)

        # Verify relatedImages was populated
        assert len(csv_dict['spec']['relatedImages']) > 0

        # Verify olm.skipRange and replaces were removed
        assert 'olm.skipRange' not in csv_dict['metadata']['annotations']
        assert 'replaces' not in csv_dict['spec']

        # Verify annotations had channel fields removed
        with open(str(work_dir / 'bundle' / 'metadata' / 'annotations.yaml')) as f:
            ann_dict = yaml.safe_load(f)
        assert 'operators.operatorframework.io.bundle.channels.v1' not in ann_dict['annotations']
        assert 'operators.operatorframework.io.bundle.channel.default.v1' not in ann_dict['annotations']

        # Verify build args file was written
        build_args_path = str(work_dir / 'bundle' / 'bundle_build_args.map')
        assert os.path.isfile(build_args_path)
        with open(build_args_path) as f:
            build_args = f.read()
        assert 'GIT_URL' in build_args
        assert 'GIT_COMMIT' in build_args

    @live
    @requires_quay_token
    def test_additional_images_merged(self, work_dir):
        """Verify additional images from additional-images-patch.yaml are in env vars."""
        from importlib import import_module
        bp_module = import_module('bundle-processor')
        bundle_processor = bp_module.bundle_processor

        bundle_dir = work_dir / 'bundle'
        config_dir = work_dir / 'config'
        tekton_dir = work_dir / 'tekton'
        output_csv = str(work_dir / 'output' / 'rhods-operator.clusterserviceversion.yaml')
        os.makedirs(work_dir / 'output', exist_ok=True)

        processor = bundle_processor(
            build_config_path=str(config_dir / 'build-config.yaml'),
            bundle_csv_path=str(bundle_dir / 'manifests' / 'rhods-operator.clusterserviceversion.yaml'),
            patch_yaml_path=str(bundle_dir / 'bundle-patch.yaml'),
            rhoai_version='rhoai-3.4',
            output_file_path=output_csv,
            annotation_yaml_path=str(bundle_dir / 'metadata' / 'annotations.yaml'),
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=str(tekton_dir / 'push-pipeline.yaml'),
            build_type='ci',
        )
        processor.process()

        with open(output_csv) as f:
            csv_dict = yaml.safe_load(f)

        env_vars = csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]['env']
        env_names = [e['name'] for e in env_vars]

        # Additional images should be present (tags stripped)
        assert 'RELATED_IMAGE_OSE_OAUTH_PROXY_IMAGE' in env_names
        assert 'RELATED_IMAGE_OSE_CLI_IMAGE' in env_names

        # Their values should point to registry.redhat.io (not stage)
        for env in env_vars:
            if env['name'] in ('RELATED_IMAGE_OSE_OAUTH_PROXY_IMAGE', 'RELATED_IMAGE_OSE_CLI_IMAGE'):
                assert env['value'].startswith('registry.redhat.io/')

    @live
    @requires_quay_token
    def test_push_pipeline_disable(self, work_dir):
        """Verify push pipeline CEL expression is disabled for nightly builds."""
        from importlib import import_module
        bp_module = import_module('bundle-processor')
        bundle_processor = bp_module.bundle_processor

        bundle_dir = work_dir / 'bundle'
        config_dir = work_dir / 'config'
        tekton_dir = work_dir / 'tekton'
        output_csv = str(work_dir / 'output' / 'rhods-operator.clusterserviceversion.yaml')
        os.makedirs(work_dir / 'output', exist_ok=True)

        processor = bundle_processor(
            build_config_path=str(config_dir / 'build-config.yaml'),
            bundle_csv_path=str(bundle_dir / 'manifests' / 'rhods-operator.clusterserviceversion.yaml'),
            patch_yaml_path=str(bundle_dir / 'bundle-patch.yaml'),
            rhoai_version='rhoai-3.4',
            output_file_path=output_csv,
            annotation_yaml_path=str(bundle_dir / 'metadata' / 'annotations.yaml'),
            push_pipeline_operation='disable',
            push_pipeline_yaml_path=str(tekton_dir / 'push-pipeline.yaml'),
            build_type='nightly',
        )
        processor.process()

        with open(str(tekton_dir / 'push-pipeline.yaml')) as f:
            pipeline_dict = yaml.safe_load(f)

        cel_expr = pipeline_dict['metadata']['annotations']['pipelinesascode.tekton.dev/on-cel-expression']
        assert 'non-existent-file.non-existent-ext' in cel_expr

    @live
    @requires_quay_token
    def test_helm_charts_patched(self, work_dir):
        """Verify XKS and OpenShift Helm charts are patched correctly."""
        from importlib import import_module
        bp_module = import_module('bundle-processor')
        bundle_processor = bp_module.bundle_processor

        bundle_dir = work_dir / 'bundle'
        config_dir = work_dir / 'config'
        tekton_dir = work_dir / 'tekton'
        helm_dir = work_dir / 'helm'
        output_csv = str(work_dir / 'output' / 'rhods-operator.clusterserviceversion.yaml')
        os.makedirs(work_dir / 'output', exist_ok=True)

        processor = bundle_processor(
            build_config_path=str(config_dir / 'build-config.yaml'),
            bundle_csv_path=str(bundle_dir / 'manifests' / 'rhods-operator.clusterserviceversion.yaml'),
            patch_yaml_path=str(bundle_dir / 'bundle-patch.yaml'),
            rhoai_version='rhoai-3.4',
            output_file_path=output_csv,
            annotation_yaml_path=str(bundle_dir / 'metadata' / 'annotations.yaml'),
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=str(tekton_dir / 'push-pipeline.yaml'),
            build_type='ci',
            xks_helm_patch_yaml_path=str(helm_dir / 'xks-values-patch.yaml'),
            xks_helm_values_yaml_path=str(helm_dir / 'xks-chart' / 'values.yaml'),
            xks_helm_push_pipeline_yaml_path=str(tekton_dir / 'xks-helm-push-pipeline.yaml'),
            openshift_helm_patch_yaml_path=str(helm_dir / 'openshift-values-patch.yaml'),
            openshift_helm_values_yaml_path=str(helm_dir / 'openshift-chart' / 'values.yaml'),
            openshift_helm_push_pipeline_yaml_path=str(tekton_dir / 'openshift-helm-push-pipeline.yaml'),
        )
        processor.process()

        # XKS values.yaml: operator image updated to registry.redhat.io
        with open(str(helm_dir / 'xks-chart' / 'values.yaml')) as f:
            xks_values = yaml.safe_load(f)
        assert xks_values['rhaiOperator']['image'].startswith('registry.redhat.io/')
        assert '@sha256:' in xks_values['rhaiOperator']['image']
        for cloud in ('azure', 'coreweave', 'aws'):
            assert xks_values[cloud]['cloudManager']['image'].startswith('registry.redhat.io/')

        # XKS Chart.yaml: version updated
        with open(str(helm_dir / 'xks-chart' / 'Chart.yaml')) as f:
            xks_chart = yaml.safe_load(f)
        assert xks_chart['version'] == '3.4.1'
        assert xks_chart['appVersion'] == '3.4.1'

        # OpenShift values.yaml: OLM channel updated
        with open(str(helm_dir / 'openshift-chart' / 'values.yaml')) as f:
            os_values = yaml.safe_load(f)
        assert os_values['operator']['rhoai']['olm']['channel'] == 'stable-3.4'

        # OpenShift Chart.yaml: version updated
        with open(str(helm_dir / 'openshift-chart' / 'Chart.yaml')) as f:
            os_chart = yaml.safe_load(f)
        assert os_chart['version'] == '3.4.1'
        assert os_chart['appVersion'] == '3.4.1'

    @live
    @requires_quay_token
    def test_sbom_metadata_injected(self, work_dir):
        """Verify SBOM metadata env vars are injected when metadata-config.yaml is present."""
        from importlib import import_module
        bp_module = import_module('bundle-processor')
        bundle_processor = bp_module.bundle_processor

        bundle_dir = work_dir / 'bundle'
        config_dir = work_dir / 'config'
        tekton_dir = work_dir / 'tekton'
        output_csv = str(work_dir / 'output' / 'rhods-operator.clusterserviceversion.yaml')
        os.makedirs(work_dir / 'output', exist_ok=True)

        processor = bundle_processor(
            build_config_path=str(config_dir / 'build-config.yaml'),
            bundle_csv_path=str(bundle_dir / 'manifests' / 'rhods-operator.clusterserviceversion.yaml'),
            patch_yaml_path=str(bundle_dir / 'bundle-patch.yaml'),
            rhoai_version='rhoai-3.4',
            output_file_path=output_csv,
            annotation_yaml_path=str(bundle_dir / 'metadata' / 'annotations.yaml'),
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=str(tekton_dir / 'push-pipeline.yaml'),
            build_type='ci',
            metadata_config_yaml_path=str(bundle_dir / 'metadata-config.yaml'),
        )
        processor.process()

        with open(output_csv) as f:
            csv_dict = yaml.safe_load(f)

        env_vars = csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]['env']
        env_map = {e['name']: e.get('value') for e in env_vars}

        # The SBOM-derived upstream version should be present
        assert 'RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE_UPSTREAM_VERSION' in env_map
        version = env_map['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE_UPSTREAM_VERSION']
        assert version
        # Should look like a semver with optional RH suffix (e.g., "0.18.0+rhaiv.7")
        assert '.' in version


# ============================================================================
# Regression tests against real RHOAI-Build-Config commits
#
# Each test case is defined by:
#   - input_sha: the commit with inputs (pre-processor state)
#   - output_sha: the commit with expected output (post-processor state)
#   - rhoai_version: the release branch name
#
# The test clones the repo once (cached in tmp), creates worktrees for
# input and output commits, runs process(), and verifies exact output match.
# ============================================================================

RBC_REPO = 'https://github.com/red-hat-data-services/RHOAI-Build-Config.git'
RBC_CACHE_DIR = os.path.join(tempfile.gettempdir(), 'rhoai-test-rbc-cache')

RELEASE_TEST_CASES = [
    pytest.param(
        'rhoai-2.25', '34bf215529', 'a7105505dc',
        id='rhoai-2.25',
    ),
    pytest.param(
        'rhoai-3.3', 'cd47b2b06e', 'ecebae1443',
        id='rhoai-3.3',
    ),
    pytest.param(
        'rhoai-3.4', '8686e625ea', '1008c15abf',
        id='rhoai-3.4',
    ),
    pytest.param(
        'rhoai-3.5-ea.1', '0d08bef814', 'bccfc4a9b2',
        id='rhoai-3.5-ea.1',
    ),
    pytest.param(
        'rhoai-3.5-ea.2', 'f820ec6d62', '93ad516b69',
        id='rhoai-3.5-ea.2',
    ),
]


@pytest.fixture(scope='session')
def rbc_clone():
    """Clone RHOAI-Build-Config once per session, reuse across tests."""
    if not os.path.isdir(os.path.join(RBC_CACHE_DIR, 'objects')):
        subprocess.run(
            ['git', 'clone', '--bare', '--filter=blob:none', RBC_REPO, RBC_CACHE_DIR],
            check=True, capture_output=True, timeout=300,
        )
    else:
        subprocess.run(
            ['git', 'fetch', '--all'],
            cwd=RBC_CACHE_DIR, check=True, capture_output=True, timeout=120,
        )
    return RBC_CACHE_DIR


def _checkout_to(bare_repo, sha, dest):
    """Export a commit from a bare repo to a directory."""
    os.makedirs(dest, exist_ok=True)
    subprocess.run(
        ['git', 'worktree', 'add', '--detach', dest, sha],
        cwd=bare_repo, check=True, capture_output=True,
    )


def _cleanup_worktree(bare_repo, dest):
    subprocess.run(
        ['git', 'worktree', 'remove', '--force', dest],
        cwd=bare_repo, capture_output=True,
    )


def _build_processor_args(rbc_dir, work_dir, rhoai_version):
    """
    Build bundle_processor constructor args by detecting which files exist,
    mirroring the GitHub Actions workflow logic.
    """
    version_suffix = rhoai_version.replace('rhoai-', 'v').replace('.', '-')

    # Copy raw inputs to work dir (processor writes output in-place)
    raw_bundle = work_dir / 'raw-bundle'
    raw_helm = work_dir / 'raw-helm'
    shutil.copytree(os.path.join(rbc_dir, 'to-be-processed', 'bundle'), str(raw_bundle))
    if os.path.isdir(os.path.join(rbc_dir, 'to-be-processed', 'helm')):
        shutil.copytree(os.path.join(rbc_dir, 'to-be-processed', 'helm'), str(raw_helm))

    output_csv = str(raw_bundle / 'manifests' / 'rhods-operator.clusterserviceversion.yaml')

    args = dict(
        build_config_path=os.path.join(rbc_dir, 'config', 'build-config.yaml'),
        bundle_csv_path=output_csv,
        patch_yaml_path=os.path.join(rbc_dir, 'bundle', 'bundle-patch.yaml'),
        rhoai_version=rhoai_version,
        output_file_path=output_csv,
        annotation_yaml_path=str(raw_bundle / 'metadata' / 'annotations.yaml'),
        push_pipeline_operation='enable',
        push_pipeline_yaml_path=os.path.join(rbc_dir, '.tekton', f'odh-operator-bundle-{version_suffix}-push.yaml'),
        build_type='ci',
    )

    # Helm args — only if the files exist
    xks_patch = os.path.join(rbc_dir, 'helm', 'xks-values-patch.yaml')
    xks_values = str(raw_helm / 'rhai-on-xks-chart' / 'values.yaml')
    xks_push = os.path.join(rbc_dir, '.tekton', f'rhai-on-xks-chart-{version_suffix}-push.yaml')
    if os.path.isfile(xks_patch) and os.path.isfile(xks_values):
        args['xks_helm_patch_yaml_path'] = xks_patch
        args['xks_helm_values_yaml_path'] = xks_values
        if os.path.isfile(xks_push):
            args['xks_helm_push_pipeline_yaml_path'] = xks_push

    os_patch = os.path.join(rbc_dir, 'helm', 'openshift-values-patch.yaml')
    os_values = str(raw_helm / 'rhai-on-openshift-chart' / 'values.yaml')
    os_push = os.path.join(rbc_dir, '.tekton', f'rhai-on-openshift-chart-{version_suffix}-push.yaml')
    if os.path.isfile(os_patch) and os.path.isfile(os_values):
        args['openshift_helm_patch_yaml_path'] = os_patch
        args['openshift_helm_values_yaml_path'] = os_values
        if os.path.isfile(os_push):
            args['openshift_helm_push_pipeline_yaml_path'] = os_push

    return args, output_csv


def _get_operator_digest_from_csv(rbc_dir):
    """Extract the operator image digest from the committed CSV."""
    csv_path = os.path.join(rbc_dir, 'bundle', 'manifests', 'rhods-operator.clusterserviceversion.yaml')
    with open(csv_path) as f:
        csv_dict = yaml.safe_load(f)
    container_image = csv_dict['metadata']['annotations']['containerImage']
    return container_image.split('@')[1]


def _pin_operator_tag(original_get_all_tags, operator_repo, pinned_digest):
    """
    Wrap quay_controller.get_all_tags so that queries for the operator repo
    return the pinned digest instead of whatever the current tag points to.
    """
    def wrapper(self_qc, repo, tag):
        result = original_get_all_tags(self_qc, repo, tag)
        if repo == operator_repo and result:
            for t in result:
                t['manifest_digest'] = pinned_digest
        return result
    return wrapper


class TestBundleProcessorRegression:

    @live
    @requires_quay_token
    @pytest.mark.parametrize('rhoai_version, input_sha, output_sha', RELEASE_TEST_CASES)
    def test_process_matches_expected_output(self, rbc_clone, tmp_path, rhoai_version, input_sha, output_sha):
        """
        Replay a real bundle-processor run and verify the output matches
        the known-good CSV that was committed.
        """
        from importlib import import_module
        bp_module = import_module('bundle-processor')
        bundle_processor = bp_module.bundle_processor
        from controller.quay_controller import quay_controller

        input_dir = str(tmp_path / 'input')
        output_dir = str(tmp_path / 'output')

        _checkout_to(rbc_clone, input_sha, input_dir)
        _checkout_to(rbc_clone, output_sha, output_dir)

        try:
            work_dir = tmp_path / 'work'
            work_dir.mkdir()

            args, output_csv = _build_processor_args(input_dir, work_dir, rhoai_version)

            pinned_digest = _get_operator_digest_from_csv(output_dir)
            original_get_all_tags = quay_controller.get_all_tags
            pinned_fn = _pin_operator_tag(original_get_all_tags, 'odh-rhel9-operator', pinned_digest)

            with patch.object(quay_controller, 'get_all_tags', pinned_fn):
                processor = bundle_processor(**args)
                processor.process()

            with open(output_csv) as f:
                output = yaml.safe_load(f)

            expected_csv = os.path.join(output_dir, 'bundle', 'manifests', 'rhods-operator.clusterserviceversion.yaml')
            with open(expected_csv) as f:
                expected = yaml.safe_load(f)

            # Version and name must match exactly
            assert output['metadata']['name'] == expected['metadata']['name']
            assert output['spec']['version'] == expected['spec']['version']

            # Operator image must match exactly (tag resolution is pinned to the bundle-patch digest)
            assert output['metadata']['annotations']['containerImage'] == expected['metadata']['annotations']['containerImage']

            container_spec = output['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]
            expected_container_spec = expected['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]
            assert container_spec['image'] == expected_container_spec['image']

            # Every env var must match (both RELATED_IMAGE_ and non-image vars)
            # Operator image env var is excluded from exact match since its digest may drift
            output_env = {e['name']: e.get('value') for e in container_spec['env']}
            expected_env = {e['name']: e.get('value') for e in expected_container_spec['env']}

            for name, expected_value in expected_env.items():
                assert name in output_env, f"Missing env var: {name}"
                assert output_env[name] == expected_value, f"Value mismatch for {name}: got {output_env[name]}, expected {expected_value}"

            # No build or stage registry URIs should remain
            build_registries = {'quay.io', 'registry.stage.redhat.io'}
            for name, value in output_env.items():
                if value and name.startswith('RELATED_IMAGE_'):
                    registry = value.split('/')[0]
                    assert registry not in build_registries, f"Unreplaced build/stage registry in {name}: {value}"

            # relatedImages count must match
            assert len(output['spec']['relatedImages']) == len(expected['spec']['relatedImages'])

        finally:
            _cleanup_worktree(rbc_clone, input_dir)
            _cleanup_worktree(rbc_clone, output_dir)
