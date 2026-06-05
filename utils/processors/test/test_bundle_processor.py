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
import pytest
import yaml

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
# Tests against real RHOAI-Build-Config files (git submodule)
#
# Input files: submodule pinned at 8686e625ea (pre-processor state)
# Expected output: fixtures/rhoai-34-expected/ from 1008c15abf (post-processor)
# ============================================================================

RBC_SUBMODULE_DIR = os.path.join(os.path.dirname(__file__), 'fixtures', 'RHOAI-Build-Config')
EXPECTED_DIR = os.path.join(os.path.dirname(__file__), 'fixtures', 'rhoai-34-expected')

requires_build_config = pytest.mark.skipif(
    not os.path.isfile(os.path.join(RBC_SUBMODULE_DIR, 'config', 'build-config.yaml')),
    reason='RHOAI-Build-Config submodule not initialized (run git submodule update --init)'
)


@pytest.fixture
def rbc_work_dir(tmp_path):
    """
    Set up a working directory mirroring how the GitHub Actions workflow
    prepares inputs for the bundle processor, using the pinned submodule.
    """
    rbc = RBC_SUBMODULE_DIR
    work = tmp_path / 'rbc'
    work.mkdir()

    shutil.copytree(os.path.join(rbc, 'to-be-processed', 'bundle'), work / 'raw-bundle')
    shutil.copytree(os.path.join(rbc, 'to-be-processed', 'helm'), work / 'raw-helm')
    shutil.copy2(os.path.join(rbc, 'config', 'build-config.yaml'), work / 'build-config.yaml')
    shutil.copytree(os.path.join(rbc, 'bundle'), work / 'bundle-config', dirs_exist_ok=True)
    shutil.copy2(os.path.join(rbc, 'helm', 'xks-values-patch.yaml'), work / 'xks-values-patch.yaml')
    shutil.copy2(os.path.join(rbc, 'helm', 'openshift-values-patch.yaml'), work / 'openshift-values-patch.yaml')

    for f in os.listdir(os.path.join(rbc, '.tekton')):
        shutil.copy2(os.path.join(rbc, '.tekton', f), work / f)

    return work


class TestBundleProcessorWithRealConfig:

    @live
    @requires_quay_token
    @requires_build_config
    def test_process_matches_expected_output(self, rbc_work_dir):
        """
        Run bundle processor with real rhoai-3.4 config files pinned at
        commit 8686e625ea and verify the output matches the known-good
        CSV from commit 1008c15abf.
        """
        from importlib import import_module
        bp_module = import_module('bundle-processor')
        bundle_processor = bp_module.bundle_processor

        work = rbc_work_dir
        output_csv = str(work / 'raw-bundle' / 'manifests' / 'rhods-operator.clusterserviceversion.yaml')

        processor = bundle_processor(
            build_config_path=str(work / 'build-config.yaml'),
            bundle_csv_path=str(work / 'raw-bundle' / 'manifests' / 'rhods-operator.clusterserviceversion.yaml'),
            patch_yaml_path=str(work / 'bundle-config' / 'bundle-patch.yaml'),
            rhoai_version='rhoai-3.4',
            output_file_path=output_csv,
            annotation_yaml_path=str(work / 'raw-bundle' / 'metadata' / 'annotations.yaml'),
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=str(work / 'odh-operator-bundle-v3-4-push.yaml'),
            build_type='ci',
            xks_helm_patch_yaml_path=str(work / 'xks-values-patch.yaml'),
            xks_helm_values_yaml_path=str(work / 'raw-helm' / 'rhai-on-xks-chart' / 'values.yaml'),
            xks_helm_push_pipeline_yaml_path=str(work / 'rhai-on-xks-chart-v3-4-push.yaml'),
            openshift_helm_patch_yaml_path=str(work / 'openshift-values-patch.yaml'),
            openshift_helm_values_yaml_path=str(work / 'raw-helm' / 'rhai-on-openshift-chart' / 'values.yaml'),
            openshift_helm_push_pipeline_yaml_path=str(work / 'rhai-on-openshift-chart-v3-4-push.yaml'),
        )
        processor.process()

        with open(output_csv) as f:
            output = yaml.safe_load(f)

        expected_csv_path = os.path.join(EXPECTED_DIR, 'rhods-operator.clusterserviceversion.yaml')
        with open(expected_csv_path) as f:
            expected = yaml.safe_load(f)

        # Version and name must match exactly
        assert output['metadata']['name'] == expected['metadata']['name']
        assert output['spec']['version'] == expected['spec']['version']

        # Operator image digest must match exactly
        assert output['metadata']['annotations']['containerImage'] == expected['metadata']['annotations']['containerImage']

        container_spec = output['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]
        expected_container_spec = expected['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]
        assert container_spec['image'] == expected_container_spec['image']

        # Every RELATED_IMAGE_ env var and its full value (registry + repo + digest) must match
        output_env = {e['name']: e.get('value') for e in container_spec['env']}
        expected_env = {e['name']: e.get('value') for e in expected_container_spec['env']}

        expected_related = {k: v for k, v in expected_env.items() if k.startswith('RELATED_IMAGE_')}
        for name, expected_value in expected_related.items():
            assert name in output_env, f"Missing env var: {name}"
            assert output_env[name] == expected_value, f"Value mismatch for {name}: got {output_env[name]}, expected {expected_value}"

        # No build or stage registry URIs should remain in any env var
        build_registries = {'quay.io', 'registry.stage.redhat.io'}
        for name, value in output_env.items():
            if value and name.startswith('RELATED_IMAGE_'):
                registry = value.split('/')[0]
                assert registry not in build_registries, f"Unreplaced build/stage registry in {name}: {value}"

        # All non-image env vars should also match
        for name, expected_value in expected_env.items():
            if not name.startswith('RELATED_IMAGE_'):
                assert name in output_env, f"Missing env var: {name}"
                assert output_env[name] == expected_value, f"Value mismatch for {name}: got {output_env[name]}, expected {expected_value}"

        # relatedImages count must match
        assert len(output['spec']['relatedImages']) == len(expected['spec']['relatedImages'])
