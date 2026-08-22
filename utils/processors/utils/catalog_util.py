"""
Shared utilities for the OLM catalog operations.

Provides functions for writing, purging, and patching OLM catalog structures
stored as defaultdict(dict) keyed by schema then name.
"""

from collections import defaultdict
from typing import Dict, List, Optional, Set

import ruamel.yaml as ruyaml
from jsonupdate_ng import jsonupdate_ng

from logger.logger import getLogger
import constants.constants as CONSTANTS

LOGGER = getLogger('processor')


def build_catalog_index(docs: list) -> defaultdict:
    """
    Index a list of OLM catalog documents into a defaultdict(dict)
    keyed by schema then name.

    Args:
        docs: List of parsed YAML documents (each with 'schema' and 'name' keys).

    Returns:
        defaultdict(dict) with catalog_dict[schema][name] = doc
    """
    catalog_dict = defaultdict(dict)
    for doc in docs:
        catalog_dict[doc['schema']][doc['name']] = doc
    LOGGER.debug(f"  Indexed {len(docs)} documents into catalog_dict "
                 f"({len(catalog_dict.get(CONSTANTS.OLM_BUNDLE_SCHEMA, {}))} bundles, "
                 f"{len(catalog_dict.get(CONSTANTS.OLM_CHANNEL_SCHEMA, {}))} channels, "
                 f"{len(catalog_dict.get(CONSTANTS.OLM_PACKAGE_SCHEMA, {}))} packages)")
    return catalog_dict


def extract_document_by_schema(docs: list, schema: str) -> Dict:
    """
    Extract the first document matching the given schema from a list of
    parsed OLM catalog documents.

    Args:
        docs: List of parsed YAML documents (each with a 'schema' key).
        schema: The schema to match (e.g. CONSTANTS.OLM_BUNDLE_SCHEMA).

    Returns:
        The first matching document, or None if not found.
    """
    for doc in docs:
        if doc.get('schema') == schema:
            return doc
    return None


def write_catalog_yaml(catalog_dict: defaultdict, file_path: str) -> None:
    """
    Flatten a catalog_dict and write as multi-document YAML with round-trip
    fidelity.

    Args:
        catalog_dict: Schema-keyed catalog structure.
        file_path: Output file path.
    """
    docs = [doc for schema_val in catalog_dict.values()
            for doc in schema_val.values()]
    LOGGER.info(f"  Writing catalog ({len(docs)} documents): {file_path}")
    with open(file_path, 'w') as f:
        ruyaml.dump_all(docs, f, Dumper=ruyaml.RoundTripDumper,
                        default_flow_style=False)


def purge_bundles(catalog_dict: defaultdict, bundle_names: List[str]) -> List[str]:
    """
    Delete named bundles from the catalog.

    Args:
        catalog_dict: Catalog structure (mutated in place).
        bundle_names: Names of bundles to remove.

    Returns:
        List of names actually deleted.
    """
    to_delete = [name for name in catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]
                 if name in bundle_names]
    for name in to_delete:
        del catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA][name]
        LOGGER.info(f"  Purged: {name}")
    return to_delete


def purge_orphan_bundles(catalog_dict: defaultdict) -> List[str]:
    """
    Delete orphan bundles — bundles not referenced by any channel entry.

    Args:
        catalog_dict: Catalog structure (mutated in place).

    Returns:
        List of purged bundle names.
    """
    referenced_bundles: Set[str] = set()
    for channel_obj in catalog_dict[CONSTANTS.OLM_CHANNEL_SCHEMA].values():
        for entry in channel_obj['entries']:
            referenced_bundles.add(entry['name'])

    orphan_bundles = [name for name in catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]
                      if name not in referenced_bundles]
    if orphan_bundles:
        LOGGER.info("  Orphan bundles found:")
        for name in orphan_bundles:
            LOGGER.info(f"    - {name}")
    else:
        LOGGER.info("  No orphan bundles found.")
    purge_bundles(catalog_dict, orphan_bundles)
    return orphan_bundles



def patch_olm_package(catalog_dict: defaultdict, patch_dict: Dict) -> None:
    """
    Deep-merge the olm.package definition from catalog-patch.yaml into the catalog.

    Deep-merge means only the fields present in the patch are updated; fields not
    in the patch (e.g. icon) are preserved from the existing catalog entry.

    Args:
        catalog_dict: Catalog structure (mutated in place).
        patch_dict: Patch YAML dict containing patch.olm.package.
    """
    if CONSTANTS.OLM_PACKAGE_SCHEMA not in patch_dict.get('patch', {}):
        LOGGER.info("  No olm.package in patch — skipping package merge.")
        return

    schema = CONSTANTS.OLM_PACKAGE_SCHEMA
    patch = patch_dict['patch'][schema]
    pkg_name = patch['name']
    LOGGER.info(f"  Patching {schema}: {pkg_name}")
    catalog_dict[schema][pkg_name] = jsonupdate_ng.updateJson(
        catalog_dict[schema][pkg_name], patch)


def patch_olm_channels(
    catalog_dict: defaultdict,
    patch_dict: Dict,
    reset_channels: Optional[Set[str]] = None
) -> None:
    """
    Merge or reset OLM channel definitions from catalog-patch.yaml into the catalog.

    For normal channels: deep-merged — new entries from the patch are added to
    the existing channel, and existing entries are preserved. Entries are matched
    by name (via listPatchScheme), so an entry with the same name is updated
    rather than duplicated.

    When a channel is in RESET_CHANNELS, the existing channel definition in the
    input catalog is completely replaced by the channel definition from
    catalog-patch.yaml — all existing entries are discarded, and only the entries
    defined in the patch file remain.

    Args:
        catalog_dict: Catalog structure (mutated in place).
        patch_dict: Patch YAML dict containing patch.olm.channels.
        reset_channels: Channel names to fully replace (defaults to
                        CONSTANTS.RESET_CHANNELS).
    """
    if 'olm.channels' not in patch_dict.get('patch', {}):
        LOGGER.info("  No olm.channels in patch — skipping channel merge.")
        return

    if reset_channels is None:
        reset_channels = CONSTANTS.RESET_CHANNELS

    schema = CONSTANTS.OLM_CHANNEL_SCHEMA
    for channel in patch_dict['patch']['olm.channels']:
        ch_name = channel['name']
        if ch_name in catalog_dict[schema] and ch_name not in reset_channels:
            LOGGER.info(f"  Patching {schema}: {ch_name}")
            catalog_dict[schema][ch_name] = jsonupdate_ng.updateJson(
                catalog_dict[schema][ch_name], channel,
                meta={'listPatchScheme': {'$.entries': {'key': 'name'}}})
        else:
            LOGGER.info(f"  Resetting {schema}: {ch_name}")
            catalog_dict[schema][ch_name] = channel
