"""
Utilities for downloading SBOMs from container image registries using cosign.
"""

import json
import re
import subprocess
from typing import Dict, List, Optional, Union

from logger.logger import getLogger

LOGGER = getLogger('sbom')


def download_sbom(image_uri: str, all_arches: bool = True) -> Union[Dict, Dict[str, Dict]]:
    """
    Download the SBOM for a container image using cosign.

    By default (all_arches=True), resolves each architecture-specific image in a
    manifest list and returns all of their SBOMs. For non-manifest-list images,
    returns the single SBOM keyed by its architecture.

    With all_arches=False, returns a single SBOM. If the image is a manifest list,
    returns the SBOM attached at that level. If none exists, falls back to the first
    per-architecture SBOM found.

    Args:
        image_uri: Full image reference with digest
                   (e.g., 'registry.redhat.io/rhaii/vllm-gaudi-rhel9@sha256:abc123')
        all_arches: If True (default), return SBOMs for every architecture in a manifest list

    Returns:
        If all_arches=True: Dict mapping architecture (e.g., 'amd64', 'arm64') to its SBOM
        If all_arches=False: Parsed SBOM as a dictionary (typically SPDX JSON)

    Raises:
        RuntimeError: If cosign fails to download the SBOM
    """
    image_uri = _strip_tag(image_uri)
    if all_arches:
        return _download_sbom_all_arches(image_uri)
    return _download_sbom_single(image_uri)


def _download_sbom_single(image_uri: str) -> Dict:
    LOGGER.info(f"Downloading SBOM for: {image_uri}")

    sbom = _cosign_download_sbom(image_uri)
    if sbom is not None:
        return sbom

    LOGGER.info("No SBOM found at manifest list level, resolving per-architecture images...")
    arch_digests = _get_arch_digests(image_uri)

    if not arch_digests:
        raise RuntimeError(f"No SBOM found and no architecture-specific images to try for: {image_uri}")

    base_ref = image_uri.split('@')[0]
    for arch, digest in arch_digests:
        arch_uri = f"{base_ref}@{digest}"
        LOGGER.info(f"  Trying {arch} image: {arch_uri}")
        sbom = _cosign_download_sbom(arch_uri)
        if sbom is not None:
            return sbom

    raise RuntimeError(f"Failed to download SBOM from any architecture for: {image_uri}")


def _download_sbom_all_arches(image_uri: str) -> Dict[str, Dict]:
    LOGGER.info(f"Downloading SBOMs for all architectures: {image_uri}")

    arch_digests = _get_arch_digests(image_uri)

    if not arch_digests:
        sbom = _cosign_download_sbom(image_uri)
        if sbom is not None:
            arch = _get_image_arch(image_uri)
            return {arch: sbom}
        raise RuntimeError(f"Failed to download SBOM for: {image_uri}")

    base_ref = image_uri.split('@')[0]
    results = {}
    for arch, digest in arch_digests:
        arch_uri = f"{base_ref}@{digest}"
        LOGGER.info(f"  Downloading SBOM for {arch}: {arch_uri}")
        sbom = _cosign_download_sbom(arch_uri)
        if sbom is not None:
            results[arch] = sbom
        else:
            LOGGER.warning(f"  No SBOM found for {arch}")

    if not results:
        raise RuntimeError(f"No SBOMs found for any architecture of: {image_uri}")

    LOGGER.info(f"Downloaded SBOMs for {len(results)} architecture(s): {list(results.keys())}")
    return results


def get_package_info(image_uri: str, package_name: str) -> Dict[str, str]:
    """
    Look up a package by exact name in the SBOM for a container image.

    Downloads SBOMs for all architectures and returns the package version
    for each architecture that contains it.

    Args:
        image_uri: Full image reference with digest
        package_name: Exact package name to match (e.g., 'vllm')

    Returns:
        Dict mapping architecture to versionInfo
        (e.g., {'amd64': '0.18.0+rhaiv.7', 'arm64': '0.17.1+rhaiv.0'})

    Raises:
        RuntimeError: If the package is not found in any architecture's SBOM
    """
    sboms = download_sbom(image_uri)

    versions = {}
    for arch, sbom in sboms.items():
        for pkg in sbom.get('packages', []):
            if pkg.get('name') == package_name:
                versions[arch] = pkg['versionInfo']
                break

    if not versions:
        raise RuntimeError(
            f"Package '{package_name}' not found in SBOM for: {image_uri}"
        )

    LOGGER.info(f"Package '{package_name}' versions: {versions}")
    return versions


def _cosign_download_sbom(image_uri: str) -> Optional[Dict]:
    """Run cosign download sbom and return parsed JSON, or None on failure."""
    try:
        result = subprocess.run(
            ['cosign', 'download', 'sbom', image_uri],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            LOGGER.debug(f"cosign download sbom failed: {result.stderr.strip()}")
            return None

        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        LOGGER.warning(f"cosign download sbom timed out for: {image_uri}")
        return None
    except json.JSONDecodeError as e:
        LOGGER.warning(f"Failed to parse SBOM JSON: {e}")
        return None


def _get_arch_digests(image_uri: str) -> List[tuple]:
    """
    Resolve architecture-specific digests from a manifest list.

    Returns:
        List of (architecture, digest) tuples, or empty list if not a manifest list.
    """
    try:
        result = subprocess.run(
            ['skopeo', 'inspect', '--raw', f'docker://{image_uri}'],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            return []

        manifest = json.loads(result.stdout)

        if manifest.get('mediaType') not in (
            'application/vnd.docker.distribution.manifest.list.v2+json',
            'application/vnd.oci.image.index.v1+json',
        ):
            return []

        return [
            (m['platform']['architecture'], m['digest'])
            for m in manifest.get('manifests', [])
            if 'platform' in m
        ]
    except (subprocess.TimeoutExpired, json.JSONDecodeError, KeyError):
        return []


def _get_image_arch(image_uri: str) -> str:
    """Get the architecture of a single (non-manifest-list) image."""
    try:
        result = subprocess.run(
            ['skopeo', 'inspect', '--no-tags', f'docker://{image_uri}'],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            info = json.loads(result.stdout)
            return info.get('Architecture', 'unknown')
    except (subprocess.TimeoutExpired, json.JSONDecodeError):
        pass
    return 'unknown'


def _strip_tag(image_uri: str) -> str:
    """Strip the tag from an image URI that has both a tag and a digest.

    e.g., 'registry.redhat.io/rhaii/vllm-cuda-rhel9:3.4@sha256:abc' becomes
          'registry.redhat.io/rhaii/vllm-cuda-rhel9@sha256:abc'
    """
    return re.sub(r':[^\s:@]+@', '@', image_uri)
