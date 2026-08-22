"""
Tests for catalog_util.py — shared OLM catalog operations.

Covers: build_catalog_index, extract_document_by_schema, purge_bundles,
        purge_orphan_bundles, patch_olm_package, patch_olm_channels.
"""

import os
import sys
import pytest
from collections import defaultdict  # used in empty catalog edge case tests

processors_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if processors_root not in sys.path:
    sys.path.insert(0, processors_root)

import utils.catalog_util as catalog_util
import constants.constants as CONSTANTS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_bundle(name, image='registry.redhat.io/rhoai/odh-operator-bundle@sha256:abc'):
    return {'schema': CONSTANTS.OLM_BUNDLE_SCHEMA, 'name': name, 'image': image, 'relatedImages': []}


def make_channel(name, entries):
    return {
        'schema': CONSTANTS.OLM_CHANNEL_SCHEMA,
        'name': name,
        'package': 'rhods-operator',
        'entries': [{'name': e} for e in entries]
    }


def make_package(name='rhods-operator', default_channel='stable', icon='base64data'):
    return {
        'schema': CONSTANTS.OLM_PACKAGE_SCHEMA,
        'name': name,
        'defaultChannel': default_channel,
        'icon': {'base64data': icon, 'mediatype': 'image/svg+xml'}
    }


def build_catalog(*docs):
    """Build a catalog_dict using the production build_catalog_index function."""
    return catalog_util.build_catalog_index(list(docs))


# ===========================================================================
# build_catalog_index
# ===========================================================================

class TestBuildCatalogIndex:

    def test_standard_3_doc_catalog(self):
        docs = [make_package(), make_channel('fast', ['A']), make_bundle('A')]
        result = catalog_util.build_catalog_index(docs)
        assert len(result[CONSTANTS.OLM_PACKAGE_SCHEMA]) == 1
        assert len(result[CONSTANTS.OLM_CHANNEL_SCHEMA]) == 1
        assert len(result[CONSTANTS.OLM_BUNDLE_SCHEMA]) == 1

    def test_multiple_bundles(self):
        docs = [make_package(), make_bundle('A'), make_bundle('B'), make_bundle('C')]
        result = catalog_util.build_catalog_index(docs)
        assert len(result[CONSTANTS.OLM_BUNDLE_SCHEMA]) == 3
        assert 'A' in result[CONSTANTS.OLM_BUNDLE_SCHEMA]
        assert 'B' in result[CONSTANTS.OLM_BUNDLE_SCHEMA]
        assert 'C' in result[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_multiple_channels(self):
        docs = [
            make_channel('fast', ['A']),
            make_channel('stable', ['A']),
            make_channel('beta', ['B'])
        ]
        result = catalog_util.build_catalog_index(docs)
        assert len(result[CONSTANTS.OLM_CHANNEL_SCHEMA]) == 3

    def test_empty_list(self):
        result = catalog_util.build_catalog_index([])
        assert len(result) == 0

    def test_duplicate_name_last_wins(self):
        bundle_v1 = make_bundle('A', image='image-v1')
        bundle_v2 = make_bundle('A', image='image-v2')
        result = catalog_util.build_catalog_index([bundle_v1, bundle_v2])
        assert result[CONSTANTS.OLM_BUNDLE_SCHEMA]['A']['image'] == 'image-v2'


# ===========================================================================
# extract_document_by_schema
# ===========================================================================

class TestExtractDocumentBySchema:

    def test_schema_found(self):
        docs = [make_package(), make_channel('fast', ['A']), make_bundle('A')]
        result = catalog_util.extract_document_by_schema(docs, CONSTANTS.OLM_BUNDLE_SCHEMA)
        assert result is not None
        assert result['name'] == 'A'

    def test_schema_not_found(self):
        docs = [make_package(), make_channel('fast', ['A'])]
        result = catalog_util.extract_document_by_schema(docs, CONSTANTS.OLM_BUNDLE_SCHEMA)
        assert result is None

    def test_empty_list(self):
        result = catalog_util.extract_document_by_schema([], CONSTANTS.OLM_BUNDLE_SCHEMA)
        assert result is None

    def test_multiple_same_schema_returns_first(self):
        docs = [make_bundle('A'), make_bundle('B')]
        result = catalog_util.extract_document_by_schema(docs, CONSTANTS.OLM_BUNDLE_SCHEMA)
        assert result['name'] == 'A'

    def test_doc_missing_schema_key(self):
        docs = [{'name': 'orphan'}]
        result = catalog_util.extract_document_by_schema(docs, CONSTANTS.OLM_BUNDLE_SCHEMA)
        assert result is None


# ===========================================================================
# purge_bundles
# ===========================================================================

class TestPurgeBundles:

    def test_purge_existing_bundle(self):
        catalog = build_catalog(make_bundle('A'), make_bundle('B'), make_bundle('C'))
        catalog_util.purge_bundles(catalog, ['B'])
        assert 'B' not in catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]
        assert 'A' in catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]
        assert 'C' in catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_purge_multiple(self):
        catalog = build_catalog(make_bundle('A'), make_bundle('B'), make_bundle('C'))
        catalog_util.purge_bundles(catalog, ['A', 'C'])
        assert len(catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]) == 1
        assert 'B' in catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_purge_nonexistent(self):
        catalog = build_catalog(make_bundle('A'), make_bundle('B'))
        result = catalog_util.purge_bundles(catalog, ['X'])
        assert len(catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]) == 2
        assert result == []

    def test_purge_from_empty_catalog(self):
        catalog = defaultdict(dict)
        result = catalog_util.purge_bundles(catalog, ['A'])
        assert result == []

    def test_return_value(self):
        catalog = build_catalog(make_bundle('A'), make_bundle('B'), make_bundle('C'))
        result = catalog_util.purge_bundles(catalog, ['B'])
        assert result == ['B']


# ===========================================================================
# purge_orphan_bundles
# ===========================================================================

class TestPurgeOrphanBundles:

    def test_all_referenced(self):
        catalog = build_catalog(
            make_bundle('A'), make_bundle('B'),
            make_channel('fast', ['A', 'B'])
        )
        result = catalog_util.purge_orphan_bundles(catalog)
        assert result == []
        assert len(catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]) == 2

    def test_one_orphan(self):
        catalog = build_catalog(
            make_bundle('A'), make_bundle('B'), make_bundle('C'),
            make_channel('fast', ['A', 'B'])
        )
        result = catalog_util.purge_orphan_bundles(catalog)
        assert 'C' in result
        assert 'C' not in catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]
        assert 'A' in catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]
        assert 'B' in catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_multiple_orphans(self):
        catalog = build_catalog(
            make_bundle('A'), make_bundle('B'), make_bundle('C'), make_bundle('D'),
            make_channel('fast', ['A'])
        )
        result = catalog_util.purge_orphan_bundles(catalog)
        assert set(result) == {'B', 'C', 'D'}
        assert len(catalog[CONSTANTS.OLM_BUNDLE_SCHEMA]) == 1

    def test_bundle_in_multiple_channels(self):
        catalog = build_catalog(
            make_bundle('A'), make_bundle('B'),
            make_channel('fast', ['A', 'B']),
            make_channel('stable', ['A'])
        )
        result = catalog_util.purge_orphan_bundles(catalog)
        assert result == []

    def test_ea_orphan_after_channel_reset(self):
        """Simulates beta channel reset: old EA becomes orphan."""
        catalog = build_catalog(
            make_bundle('rhods-operator.3.4.0-ea.2'),
            make_bundle('rhods-operator.3.5.0-ea.1'),
            make_channel('beta', ['rhods-operator.3.5.0-ea.1']),
            make_channel('fast', ['rhods-operator.3.4.0-ea.2', 'rhods-operator.3.5.0-ea.1'])
        )
        # Remove ea.2 from fast to simulate it becoming unreferenced
        catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries'] = [
            {'name': 'rhods-operator.3.5.0-ea.1'}
        ]
        result = catalog_util.purge_orphan_bundles(catalog)
        assert 'rhods-operator.3.4.0-ea.2' in result
        assert 'rhods-operator.3.5.0-ea.1' not in result

    def test_empty_catalog(self):
        catalog = defaultdict(dict)
        result = catalog_util.purge_orphan_bundles(catalog)
        assert result == []

    def test_channels_with_empty_entries(self):
        catalog = build_catalog(
            make_bundle('A'), make_bundle('B'),
            make_channel('fast', [])
        )
        # Manually set empty entries since make_channel creates [{'name': ...}]
        catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries'] = []
        result = catalog_util.purge_orphan_bundles(catalog)
        assert set(result) == {'A', 'B'}


# ===========================================================================
# patch_olm_package
# ===========================================================================

class TestPatchOlmPackage:

    def test_update_default_channel(self):
        catalog = build_catalog(make_package(default_channel='stable'))
        patch = {'patch': {'olm.package': {'name': 'rhods-operator', 'defaultChannel': 'stable-3.x'}}}
        catalog_util.patch_olm_package(catalog, patch)
        assert catalog[CONSTANTS.OLM_PACKAGE_SCHEMA]['rhods-operator']['defaultChannel'] == 'stable-3.x'

    def test_preserves_unpatched_fields(self):
        catalog = build_catalog(make_package(default_channel='stable', icon='my-icon'))
        patch = {'patch': {'olm.package': {'name': 'rhods-operator', 'defaultChannel': 'stable-3.x'}}}
        catalog_util.patch_olm_package(catalog, patch)
        assert catalog[CONSTANTS.OLM_PACKAGE_SCHEMA]['rhods-operator']['icon']['base64data'] == 'my-icon'

    def test_no_olm_package_in_patch(self):
        catalog = build_catalog(make_package(default_channel='stable'))
        patch = {'patch': {}}
        catalog_util.patch_olm_package(catalog, patch)
        assert catalog[CONSTANTS.OLM_PACKAGE_SCHEMA]['rhods-operator']['defaultChannel'] == 'stable'

    def test_empty_patch_dict(self):
        catalog = build_catalog(make_package(default_channel='stable'))
        catalog_util.patch_olm_package(catalog, {})
        assert catalog[CONSTANTS.OLM_PACKAGE_SCHEMA]['rhods-operator']['defaultChannel'] == 'stable'

    def test_patch_adds_new_field(self):
        catalog = build_catalog(make_package())
        patch = {'patch': {'olm.package': {'name': 'rhods-operator', 'description': 'RHOAI operator'}}}
        catalog_util.patch_olm_package(catalog, patch)
        assert catalog[CONSTANTS.OLM_PACKAGE_SCHEMA]['rhods-operator']['description'] == 'RHOAI operator'


# ===========================================================================
# patch_olm_channels
# ===========================================================================

class TestPatchOlmChannels:

    def test_normal_channel_new_entry_added(self):
        catalog = build_catalog(
            make_channel('fast', ['rhods-operator.2.25.6', 'rhods-operator.2.25.5'])
        )
        patch = {'patch': {'olm.channels': [{
            'name': 'fast',
            'entries': [{'name': 'rhods-operator.2.25.7', 'replaces': 'rhods-operator.2.25.6'}]
        }]}}
        catalog_util.patch_olm_channels(catalog, patch)
        entries = catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries']
        entry_names = [e['name'] for e in entries]
        assert 'rhods-operator.2.25.7' in entry_names

    def test_normal_channel_existing_entries_preserved(self):
        catalog = build_catalog(
            make_channel('fast', ['rhods-operator.2.25.6', 'rhods-operator.2.25.5'])
        )
        patch = {'patch': {'olm.channels': [{
            'name': 'fast',
            'entries': [{'name': 'rhods-operator.2.25.7'}]
        }]}}
        catalog_util.patch_olm_channels(catalog, patch)
        entries = catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries']
        entry_names = [e['name'] for e in entries]
        assert 'rhods-operator.2.25.6' in entry_names
        assert 'rhods-operator.2.25.5' in entry_names

    def test_normal_channel_same_name_updated_not_duplicated(self):
        catalog = build_catalog(make_channel('fast', ['A']))
        # Add skipRange to existing entry A
        catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries'][0]['skipRange'] = '>=1.0.0'
        patch = {'patch': {'olm.channels': [{
            'name': 'fast',
            'entries': [{'name': 'A', 'skipRange': '>=2.0.0'}]
        }]}}
        catalog_util.patch_olm_channels(catalog, patch)
        entries = catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries']
        a_entries = [e for e in entries if e['name'] == 'A']
        assert len(a_entries) == 1
        assert a_entries[0]['skipRange'] == '>=2.0.0'

    def test_reset_channel_fully_replaced(self):
        catalog = build_catalog(make_channel('beta', ['rhods-operator.3.4.0-ea.2']))
        patch = {'patch': {'olm.channels': [{
            'name': 'beta',
            'schema': 'olm.channel',
            'package': 'rhods-operator',
            'entries': [{'name': 'rhods-operator.3.5.0-ea.1', 'skipRange': '>=3.4.0 <3.5.0'}]
        }]}}
        catalog_util.patch_olm_channels(catalog, patch)
        entries = catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['beta']['entries']
        entry_names = [e['name'] for e in entries]
        assert 'rhods-operator.3.5.0-ea.1' in entry_names
        assert 'rhods-operator.3.4.0-ea.2' not in entry_names

    def test_new_channel_added(self):
        catalog = build_catalog(make_channel('fast', ['A']))
        patch = {'patch': {'olm.channels': [{
            'name': 'eus-2.25',
            'schema': 'olm.channel',
            'package': 'rhods-operator',
            'entries': [{'name': 'rhods-operator.2.25.7'}]
        }]}}
        catalog_util.patch_olm_channels(catalog, patch)
        assert 'eus-2.25' in catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]
        entries = catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['eus-2.25']['entries']
        assert entries[0]['name'] == 'rhods-operator.2.25.7'

    def test_no_olm_channels_in_patch(self):
        catalog = build_catalog(make_channel('fast', ['A']))
        patch = {'patch': {}}
        catalog_util.patch_olm_channels(catalog, patch)
        assert len(catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries']) == 1

    def test_custom_reset_channels(self):
        catalog = build_catalog(make_channel('fast', ['A', 'B']))
        patch = {'patch': {'olm.channels': [{
            'name': 'fast',
            'entries': [{'name': 'C'}]
        }]}}
        catalog_util.patch_olm_channels(catalog, patch, reset_channels={'fast'})
        entries = catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries']
        entry_names = [e['name'] for e in entries]
        assert entry_names == ['C']

    def test_multiple_channels_in_patch(self):
        catalog = build_catalog(
            make_channel('fast', ['A']),
            make_channel('beta', ['old-ea']),
            make_channel('stable', ['A'])
        )
        patch = {'patch': {'olm.channels': [
            {'name': 'fast', 'entries': [{'name': 'B'}]},
            {'name': 'beta', 'schema': 'olm.channel', 'package': 'rhods-operator',
             'entries': [{'name': 'new-ea'}]},
            {'name': 'stable', 'entries': [{'name': 'B'}]},
        ]}}
        catalog_util.patch_olm_channels(catalog, patch)

        # fast: merged (A + B)
        fast_names = [e['name'] for e in catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries']]
        assert 'A' in fast_names
        assert 'B' in fast_names

        # beta: reset (only new-ea)
        beta_names = [e['name'] for e in catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['beta']['entries']]
        assert beta_names == ['new-ea']

        # stable: merged (A + B)
        stable_names = [e['name'] for e in catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['stable']['entries']]
        assert 'A' in stable_names
        assert 'B' in stable_names

    def test_entries_matched_by_name_not_position(self):
        catalog = build_catalog(make_channel('fast', ['A', 'B']))
        patch = {'patch': {'olm.channels': [{
            'name': 'fast',
            'entries': [
                {'name': 'B', 'skipRange': '>=1.0.0'},
                {'name': 'C'}
            ]
        }]}}
        catalog_util.patch_olm_channels(catalog, patch)
        entries = catalog[CONSTANTS.OLM_CHANNEL_SCHEMA]['fast']['entries']
        entry_names = [e['name'] for e in entries]
        assert 'A' in entry_names
        assert 'B' in entry_names
        assert 'C' in entry_names
        b_entry = [e for e in entries if e['name'] == 'B'][0]
        assert b_entry['skipRange'] == '>=1.0.0'
