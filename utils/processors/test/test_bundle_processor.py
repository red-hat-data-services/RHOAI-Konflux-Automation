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
