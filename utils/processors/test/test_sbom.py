"""
Tests for utils/sbom.py.

All subprocess calls (cosign, skopeo) are mocked so tests run offline and fast.

Run:
    cd utils/processors
    python -m pytest test/test_sbom.py -v
"""

import json
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

processors_root = os.path.abspath(os.path.join(os.path.dirname(__file__), f".." ))
if processors_root not in sys.path:
    sys.path.insert(0, processors_root)

from utils.sbom import download_sbom, get_package_info


# ============================================================================
# Helpers
# ============================================================================

def mock_subprocess_result(stdout='', stderr='', returncode=0):
    result = MagicMock()
    result.stdout = stdout
    result.stderr = stderr
    result.returncode = returncode
    return result


def make_sbom(packages=None):
    return {
        'spdxVersion': 'SPDX-2.3',
        'dataLicense': 'CC0-1.0',
        'name': 'test-image',
        'packages': packages or [],
        'relationships': [],
    }


def make_package(name, version):
    return {
        'SPDXID': f'SPDXRef-{name}',
        'name': name,
        'versionInfo': version,
        'downloadLocation': 'NOASSERTION',
        'filesAnalyzed': False,
        'licenseDeclared': 'Apache-2.0',
        'externalRefs': [],
    }


def make_manifest_list(arch_digests):
    return {
        'mediaType': 'application/vnd.oci.image.index.v1+json',
        'manifests': [
            {
                'digest': f'sha256:{digest}',
                'platform': {'architecture': arch, 'os': 'linux'},
            }
            for arch, digest in arch_digests
        ],
    }


IMAGE = 'registry.redhat.io/rhaii/vllm-gaudi-rhel9@sha256:abc123'


# ============================================================================
# download_sbom — single arch (all_arches=False)
# ============================================================================

class TestDownloadSbomSingle:

    @patch('utils.sbom.subprocess.run')
    def test_returns_sbom_directly(self, mock_run):
        sbom = make_sbom([make_package('vllm', '0.17.1')])
        mock_run.return_value = mock_subprocess_result(stdout=json.dumps(sbom))

        result = download_sbom(IMAGE, all_arches=False)

        assert result['spdxVersion'] == 'SPDX-2.3'
        assert len(result['packages']) == 1

    @patch('utils.sbom.subprocess.run')
    def test_falls_back_to_per_arch(self, mock_run):
        """If no SBOM at manifest list level, resolve arches and try each."""
        sbom = make_sbom([make_package('vllm', '0.17.1')])
        manifest_list = make_manifest_list([('amd64', 'aaa'), ('arm64', 'bbb')])

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'cosign' and cmd[1] == 'download':
                if 'abc123' in cmd[3]:
                    return mock_subprocess_result(returncode=1, stderr='not found')
                return mock_subprocess_result(stdout=json.dumps(sbom))
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(manifest_list))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect
        result = download_sbom(IMAGE, all_arches=False)

        assert result['spdxVersion'] == 'SPDX-2.3'

    @patch('utils.sbom.subprocess.run')
    def test_raises_when_no_sbom_found(self, mock_run):
        mock_run.return_value = mock_subprocess_result(returncode=1, stderr='not found')

        with pytest.raises(RuntimeError, match='No SBOM found'):
            download_sbom(IMAGE, all_arches=False)


# ============================================================================
# download_sbom — all arches (default)
# ============================================================================

class TestDownloadSbomAllArches:

    @patch('utils.sbom.subprocess.run')
    def test_resolves_per_arch_sboms(self, mock_run):
        amd64_sbom = make_sbom([make_package('vllm', '0.17.1')])
        arm64_sbom = make_sbom([make_package('vllm', '0.17.1')])
        manifest_list = make_manifest_list([('amd64', 'aaa'), ('arm64', 'bbb')])

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(manifest_list))
            if cmd[0] == 'cosign' and 'aaa' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(amd64_sbom))
            if cmd[0] == 'cosign' and 'bbb' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(arm64_sbom))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect
        result = download_sbom(IMAGE)

        assert 'amd64' in result
        assert 'arm64' in result
        assert len(result) == 2

    @patch('utils.sbom.subprocess.run')
    def test_non_manifest_list_returns_single(self, mock_run):
        """If image is not a manifest list, return SBOM keyed by 'single'."""
        sbom = make_sbom([make_package('vllm', '0.17.1')])
        single_manifest = {'mediaType': 'application/vnd.oci.image.manifest.v1+json'}

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(single_manifest))
            if cmd[0] == 'cosign':
                return mock_subprocess_result(stdout=json.dumps(sbom))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect
        result = download_sbom(IMAGE)

        assert 'single' in result
        assert len(result) == 1

    @patch('utils.sbom.subprocess.run')
    def test_raises_when_no_arch_has_sbom(self, mock_run):
        manifest_list = make_manifest_list([('amd64', 'aaa')])

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(manifest_list))
            return mock_subprocess_result(returncode=1, stderr='not found')

        mock_run.side_effect = side_effect

        with pytest.raises(RuntimeError, match='No SBOMs found'):
            download_sbom(IMAGE)

    @patch('utils.sbom.subprocess.run')
    def test_partial_arch_failure_returns_available(self, mock_run):
        """If one arch has an SBOM and another doesn't, return what's available."""
        amd64_sbom = make_sbom([make_package('vllm', '0.17.1')])
        manifest_list = make_manifest_list([('amd64', 'aaa'), ('arm64', 'bbb')])

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(manifest_list))
            if cmd[0] == 'cosign' and 'aaa' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(amd64_sbom))
            return mock_subprocess_result(returncode=1, stderr='not found')

        mock_run.side_effect = side_effect
        result = download_sbom(IMAGE)

        assert 'amd64' in result
        assert 'arm64' not in result


# ============================================================================
# get_package_info
# ============================================================================

class TestGetPackageInfo:

    @patch('utils.sbom.subprocess.run')
    def test_returns_matching_package(self, mock_run):
        sbom = make_sbom([
            make_package('python', '3.12.0'),
            make_package('vllm', '0.17.1+rhaiv.0'),
        ])
        single_manifest = {'mediaType': 'application/vnd.oci.image.manifest.v1+json'}

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(single_manifest))
            if cmd[0] == 'cosign':
                return mock_subprocess_result(stdout=json.dumps(sbom))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect
        pkg = get_package_info(IMAGE, 'vllm')

        assert pkg['name'] == 'vllm'
        assert pkg['versionInfo'] == '0.17.1+rhaiv.0'

    @patch('utils.sbom.subprocess.run')
    def test_raises_when_package_not_found(self, mock_run):
        sbom = make_sbom([make_package('python', '3.12.0')])
        single_manifest = {'mediaType': 'application/vnd.oci.image.manifest.v1+json'}

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(single_manifest))
            if cmd[0] == 'cosign':
                return mock_subprocess_result(stdout=json.dumps(sbom))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect

        with pytest.raises(RuntimeError, match="Package 'vllm' not found"):
            get_package_info(IMAGE, 'vllm')

    @patch('utils.sbom.subprocess.run')
    def test_consistent_versions_across_arches(self, mock_run):
        amd64_sbom = make_sbom([make_package('vllm', '0.17.1+rhaiv.0')])
        arm64_sbom = make_sbom([make_package('vllm', '0.17.1+rhaiv.0')])
        manifest_list = make_manifest_list([('amd64', 'aaa'), ('arm64', 'bbb')])

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(manifest_list))
            if cmd[0] == 'cosign' and 'aaa' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(amd64_sbom))
            if cmd[0] == 'cosign' and 'bbb' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(arm64_sbom))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect
        pkg = get_package_info(IMAGE, 'vllm')

        assert pkg['name'] == 'vllm'
        assert pkg['versionInfo'] == '0.17.1+rhaiv.0'

    @patch('utils.sbom.subprocess.run')
    def test_inconsistent_versions_raises(self, mock_run):
        amd64_sbom = make_sbom([make_package('vllm', '0.17.1+rhaiv.0')])
        arm64_sbom = make_sbom([make_package('vllm', '0.18.0+rhaiv.0')])
        manifest_list = make_manifest_list([('amd64', 'aaa'), ('arm64', 'bbb')])

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(manifest_list))
            if cmd[0] == 'cosign' and 'aaa' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(amd64_sbom))
            if cmd[0] == 'cosign' and 'bbb' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(arm64_sbom))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect

        with pytest.raises(ValueError, match='inconsistent versions'):
            get_package_info(IMAGE, 'vllm')

    @patch('utils.sbom.subprocess.run')
    def test_package_missing_in_one_arch(self, mock_run):
        """Package exists in amd64 but not arm64 — should still return it."""
        amd64_sbom = make_sbom([make_package('vllm', '0.17.1')])
        arm64_sbom = make_sbom([make_package('python', '3.12.0')])
        manifest_list = make_manifest_list([('amd64', 'aaa'), ('arm64', 'bbb')])

        def side_effect(cmd, **kwargs):
            if cmd[0] == 'skopeo':
                return mock_subprocess_result(stdout=json.dumps(manifest_list))
            if cmd[0] == 'cosign' and 'aaa' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(amd64_sbom))
            if cmd[0] == 'cosign' and 'bbb' in cmd[3]:
                return mock_subprocess_result(stdout=json.dumps(arm64_sbom))
            return mock_subprocess_result(returncode=1)

        mock_run.side_effect = side_effect
        pkg = get_package_info(IMAGE, 'vllm')

        assert pkg['name'] == 'vllm'


# ============================================================================
# Error handling
# ============================================================================

class TestErrorHandling:

    @patch('utils.sbom.subprocess.run')
    def test_cosign_timeout(self, mock_run):
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd='cosign', timeout=120)

        with pytest.raises(RuntimeError):
            download_sbom(IMAGE, all_arches=False)

    @patch('utils.sbom.subprocess.run')
    def test_invalid_json_from_cosign(self, mock_run):
        mock_run.return_value = mock_subprocess_result(stdout='not valid json')

        with pytest.raises(RuntimeError):
            download_sbom(IMAGE, all_arches=False)
