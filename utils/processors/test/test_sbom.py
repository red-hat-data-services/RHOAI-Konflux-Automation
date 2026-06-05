"""
Tests for utils/sbom.py.

Mocked tests run offline and fast. Live tests hit real registries and require
cosign/skopeo + auth to registry.redhat.io.

Run:
    cd utils/processors
    python -m pytest test/test_sbom.py -v              # mocked only
    python -m pytest test/test_sbom.py -v -m live      # live only
    python -m pytest test/test_sbom.py -v -m 'not live' # explicit skip live
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


# ============================================================================
# Live tests — require cosign, skopeo, and registry.redhat.io auth
# ============================================================================

live = pytest.mark.live

LIVE_IMAGES = {
    'gaudi': 'registry.redhat.io/rhaii/vllm-gaudi-rhel9@sha256:71008b2151586551ec0969ffc5b175f726ed6f6bebee29cdc289549e216609bc',
    'cuda':  'registry.redhat.io/rhaii/vllm-cuda-rhel9@sha256:ad06abf3bb5235ebb5b2df84cd1b9fd09e823f0ff2eebfc82bb4590275ccfe0b',
    'rocm':  'registry.redhat.io/rhaii/vllm-rocm-rhel9@sha256:98375507f524731a76877fb7ac5451be6fabbf8751e18802943ae8abe44a9bca',
    'spyre': 'registry.redhat.io/rhaii/vllm-spyre-rhel9@sha256:61ee874175b314a4d5425e68b4320ef53af2318c58ef7e74e77d8bbf5183fc33',
    'cpu':   'registry.redhat.io/rhaii/vllm-cpu-rhel9@sha256:dd104214095322ca92fb71149ae2bea8cfff54d6d261079740f673a840ed0795',
}


class TestLiveDownloadSbom:

    @live
    def test_single_arch_image(self):
        """Direct single-arch image (not a manifest list)."""
        single_arch = 'registry.redhat.io/rhaii/vllm-gaudi-rhel9@sha256:ce1e4fd82dd37d299ce8c64a237971d00faf602f06868c986e81aefa04c283ef'
        result = download_sbom(single_arch, all_arches=False)

        assert result['spdxVersion'] == 'SPDX-2.3'
        assert len(result['packages']) > 100

    @live
    def test_manifest_list_all_arches(self):
        """Manifest list should resolve per-arch SBOMs."""
        result = download_sbom(LIVE_IMAGES['cuda'])

        assert 'amd64' in result
        for arch, sbom in result.items():
            assert sbom['spdxVersion'] == 'SPDX-2.3'
            assert len(sbom['packages']) > 100

    @live
    def test_manifest_list_multi_arch(self):
        """Spyre image has ppc64le, s390x, and amd64."""
        result = download_sbom(LIVE_IMAGES['spyre'])

        assert len(result) >= 3
        assert 'amd64' in result
        assert 'ppc64le' in result
        assert 's390x' in result


class TestLiveGetPackageInfo:

    @live
    @pytest.mark.parametrize('variant', list(LIVE_IMAGES.keys()))
    def test_vllm_package_found(self, variant):
        pkg = get_package_info(LIVE_IMAGES[variant], 'vllm')

        assert pkg['name'] == 'vllm'
        assert pkg['versionInfo']
        assert '+' in pkg['versionInfo'] or pkg['versionInfo'].count('.') >= 2

    @live
    def test_nonexistent_package_raises(self):
        with pytest.raises(RuntimeError, match='not found'):
            get_package_info(LIVE_IMAGES['gaudi'], 'nonexistent-package-xyz')
