"""
Utilities for downloading SBOMs from container image registries using cosign.
"""

import json
import subprocess
from typing import Dict, List, Optional, Union

from logger.logger import getLogger

LOGGER = getLogger('sbom')


def download_sbom(image_uri: str, all_arches: bool = False) -> Union[Dict, Dict[str, Dict]]:
    """
    Download the SBOM for a container image using cosign.

    By default, returns a single SBOM. If the image URI points to a manifest list,
    cosign returns the SBOM attached to the manifest list itself. If no SBOM is found
    at that level, this function resolves per-architecture images and returns the first
    available SBOM.

    With all_arches=True, resolves each architecture-specific image in a manifest list
    and returns all of their SBOMs.

    Args:
        image_uri: Full image reference with digest
                   (e.g., 'registry.redhat.io/rhaii/vllm-gaudi-rhel9@sha256:abc123')
        all_arches: If True, return SBOMs for every architecture in a manifest list

    Returns:
        If all_arches=False: Parsed SBOM as a dictionary (typically SPDX JSON)
        If all_arches=True: Dict mapping architecture (e.g., 'amd64', 'arm64') to its SBOM

    Raises:
        RuntimeError: If cosign fails to download the SBOM
    """
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
            return {'single': sbom}
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
            ['cosign', 'manifest', image_uri],
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
