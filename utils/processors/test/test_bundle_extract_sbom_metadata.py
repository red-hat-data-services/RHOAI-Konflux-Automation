"""
Tests for extract_sbom_metadata() and the SBOM metadata injection flow
in bundle_processor.

All SBOM calls are mocked — these test the config parsing, lookup logic,
error handling, and env var injection without hitting real registries.

Run:
    cd utils/processors
    python -m pytest test/test_sbom_metadata.py -v
"""

import os
import sys
import pytest
import yaml
from importlib import import_module
from unittest.mock import patch, MagicMock

processors_root = os.path.abspath(os.path.join(os.path.dirname(__file__), f".."))
if processors_root not in sys.path:
    sys.path.insert(0, processors_root)

bp_module = import_module('bundle-processor')


def write_yaml(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        yaml.dump(data, f)


def make_metadata_config(entries):
    return {'sbom-metadata': entries}


def make_sbom_entry(env_vars, package='vllm', suffix='_UPSTREAM_VERSION'):
    return {'env_vars': env_vars, 'package': package, 'suffix': suffix}


@pytest.fixture
def processor_env(tmp_path):
    """
    Create a minimal bundle_processor with enough state to test
    extract_sbom_metadata() directly, without running the full process().
    """
    bundle_dir = tmp_path / 'bundle'
    bundle_dir.mkdir()

    patch_dict = {
        'patch': {
            'version': '3.4.1',
            'relatedImages': [
                {'name': 'RELATED_IMAGE_ODH_OPERATOR_IMAGE', 'value': 'quay.io/rhoai/odh-rhel9-operator@sha256:abc'},
            ],
            'additional-related-images': {'file': 'additional-images-patch.yaml'},
            'additional-fields': {'file': 'csv-patch.yaml'},
        }
    }
    write_yaml(str(bundle_dir / 'bundle-patch.yaml'), patch_dict)
    write_yaml(str(bundle_dir / 'csv-patch.yaml'), {})
    write_yaml(str(bundle_dir / 'additional-images-patch.yaml'), {
        'additionalImages': [
            {'name': 'RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE', 'value': 'registry.redhat.io/rhaii/vllm-cuda-rhel9:3.4@sha256:cuda123'},
            {'name': 'RELATED_IMAGE_RHAII_VLLM_GAUDI_IMAGE', 'value': 'registry.redhat.io/rhaii/vllm-gaudi-rhel9:3.4@sha256:gaudi123'},
        ]
    })

    class FakeProcessor:
        pass

    proc = FakeProcessor()
    proc.patch_yaml_path = str(bundle_dir / 'bundle-patch.yaml')
    proc.patch_dict = patch_dict
    proc.metadata_config_yaml_path = None
    proc.operands_map_dict = {
        'relatedImages': [
            {'name': 'RELATED_IMAGE_ODH_DASHBOARD_IMAGE', 'value': 'quay.io/rhoai/odh-dashboard-rhel9@sha256:dash123'},
            {'name': 'RELATED_IMAGE_ODH_MODEL_CONTROLLER_IMAGE', 'value': 'quay.io/rhoai/odh-model-controller-rhel9@sha256:mc123'},
        ]
    }
    proc.additional_image_entries = bp_module.bundle_processor._load_additional_images(proc)
    proc.extract_sbom_metadata = bp_module.bundle_processor.extract_sbom_metadata.__get__(proc)

    return proc, bundle_dir


def _patch_get_package_info(mock_return=None, mock_side_effect=None):
    """Patch get_package_info on the bundle-processor module where it's imported."""
    m = MagicMock()
    if mock_side_effect:
        m.side_effect = mock_side_effect
    elif mock_return:
        m.return_value = mock_return
    return patch.object(bp_module, 'get_package_info', m), m


class TestExtractSbomMetadata:

    def test_no_metadata_config(self, processor_env):
        """No metadata_config_yaml_path — returns empty list."""
        proc, _ = processor_env
        proc.metadata_config_yaml_path = None
        result = proc.extract_sbom_metadata()
        assert result == []

    def test_empty_sbom_metadata(self, processor_env):
        """metadata-config.yaml exists but has no sbom-metadata entries."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, {'sbom-metadata': []})
        proc.metadata_config_yaml_path = config_path

        result = proc.extract_sbom_metadata()
        assert result == []

    def test_happy_path_operands_map(self, processor_env):
        """Package found in operands map image."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_ODH_DASHBOARD_IMAGE'], package='python', suffix='_PYTHON_VERSION')
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, mock_get_pkg = _patch_get_package_info(
            mock_return={'amd64': '3.12.0'}
        )
        with patcher:
            result = proc.extract_sbom_metadata()

        assert len(result) == 1
        assert result[0]['name'] == 'RELATED_IMAGE_ODH_DASHBOARD_IMAGE_PYTHON_VERSION'
        assert result[0]['value'] == '3.12.0'
        mock_get_pkg.assert_called_once_with('quay.io/rhoai/odh-dashboard-rhel9@sha256:dash123', 'python')

    def test_happy_path_additional_images(self, processor_env):
        """Package found in additional images (RHAII vLLM)."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, mock_get_pkg = _patch_get_package_info(
            mock_return={'amd64': '0.18.0+rhaiv.7'}
        )
        with patcher:
            result = proc.extract_sbom_metadata()

        assert len(result) == 1
        assert result[0]['name'] == 'RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE_UPSTREAM_VERSION'
        assert result[0]['value'] == '0.18.0+rhaiv.7'
        mock_get_pkg.assert_called_once_with('registry.redhat.io/rhaii/vllm-cuda-rhel9@sha256:cuda123', 'vllm')

    def test_multiple_entries(self, processor_env):
        """Multiple sbom-metadata entries with different packages and suffixes."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE', 'RELATED_IMAGE_RHAII_VLLM_GAUDI_IMAGE']),
            make_sbom_entry(['RELATED_IMAGE_ODH_DASHBOARD_IMAGE'], package='python', suffix='_PYTHON_VERSION'),
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, mock_get_pkg = _patch_get_package_info(mock_side_effect=[
            {'amd64': '0.18.0+rhaiv.7'},
            {'amd64': '0.17.1+rhaiv.0'},
            {'amd64': '3.12.0'},
        ])
        with patcher:
            result = proc.extract_sbom_metadata()

        assert len(result) == 3
        names = [r['name'] for r in result]
        assert 'RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE_UPSTREAM_VERSION' in names
        assert 'RELATED_IMAGE_RHAII_VLLM_GAUDI_IMAGE_UPSTREAM_VERSION' in names
        assert 'RELATED_IMAGE_ODH_DASHBOARD_IMAGE_PYTHON_VERSION' in names

    def test_image_not_in_lookup(self, processor_env):
        """Env var not found in operands map or additional images — warns and skips."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_NONEXISTENT_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        result = proc.extract_sbom_metadata()
        assert result == []

    def test_package_not_found_exits(self, processor_env):
        """Package not in SBOM — RuntimeError propagates."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_ODH_DASHBOARD_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, _ = _patch_get_package_info(
            mock_side_effect=RuntimeError("Package 'vllm' not found in SBOM")
        )
        with patcher:
            with pytest.raises(RuntimeError, match="not found in SBOM"):
                proc.extract_sbom_metadata()

    def test_no_amd64_exits(self, processor_env):
        """No amd64 version available — sys.exit(1)."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_ODH_DASHBOARD_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, _ = _patch_get_package_info(
            mock_return={'arm64': '0.18.0'}
        )
        with patcher:
            with pytest.raises(SystemExit):
                proc.extract_sbom_metadata()

    def test_different_versions_warns_uses_amd64(self, processor_env):
        """Different versions across arches — warns but uses amd64 value."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, _ = _patch_get_package_info(
            mock_return={'amd64': '0.18.0+rhaiv.7', 'arm64': '0.17.1+rhaiv.0'}
        )
        with patcher:
            result = proc.extract_sbom_metadata()

        assert len(result) == 1
        assert result[0]['value'] == '0.18.0+rhaiv.7'

    def test_tag_stripped_from_additional_images(self, processor_env):
        """Additional images have tags stripped (e.g., ':3.4@sha256:' becomes '@sha256:')."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, mock_get_pkg = _patch_get_package_info(
            mock_return={'amd64': '0.18.0'}
        )
        with patcher:
            proc.extract_sbom_metadata()

        called_uri = mock_get_pkg.call_args[0][0]
        assert ':3.4@' not in called_uri
        assert called_uri == 'registry.redhat.io/rhaii/vllm-cuda-rhel9@sha256:cuda123'


class TestSbomMetadataConflictCheck:

    def test_no_conflict(self, processor_env):
        """SBOM env vars don't conflict with existing env vars."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, _ = _patch_get_package_info(
            mock_return={'amd64': '0.18.0'}
        )
        with patcher:
            sbom_entries = proc.extract_sbom_metadata()

        existing_env = [
            {'name': 'OPERATOR_NAME', 'value': 'rhods-operator'},
            {'name': 'RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE', 'value': 'registry.redhat.io/rhaii/vllm-cuda-rhel9@sha256:abc'},
        ]

        existing_names = {str(entry['name']) for entry in existing_env}
        conflicts = [e['name'] for e in sbom_entries if e['name'] in existing_names]
        assert not conflicts

    def test_conflict_detected(self, processor_env):
        """SBOM env var name collides with an existing env var."""
        proc, bundle_dir = processor_env
        config_path = str(bundle_dir / 'metadata-config.yaml')
        write_yaml(config_path, make_metadata_config([
            make_sbom_entry(['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE'])
        ]))
        proc.metadata_config_yaml_path = config_path

        patcher, _ = _patch_get_package_info(
            mock_return={'amd64': '0.18.0'}
        )
        with patcher:
            sbom_entries = proc.extract_sbom_metadata()

        existing_env = [
            {'name': 'RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE_UPSTREAM_VERSION', 'value': 'already-exists'},
        ]

        existing_names = {str(entry['name']) for entry in existing_env}
        conflicts = [e['name'] for e in sbom_entries if e['name'] in existing_names]
        assert conflicts == ['RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE_UPSTREAM_VERSION']
