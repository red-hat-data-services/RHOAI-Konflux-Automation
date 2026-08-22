"""
Tests for fbc-processor.py — patch_catalog orchestration.

Tests the end-to-end patch_catalog flow including bundle extraction,
image replacement, package/channel patching, orphan cleanup, and
explicit purge. Helper functions (patch_olm_package, patch_olm_channels,
purge_orphan_bundles) are rigorously tested in test_catalog_util.py —
these tests focus on the orchestration logic unique to fbc_processor.
"""

import os
import sys
import pytest
import yaml
from collections import defaultdict
from unittest.mock import patch, MagicMock

processors_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if processors_root not in sys.path:
    sys.path.insert(0, processors_root)

import constants.constants as CONSTANTS


# ---------------------------------------------------------------------------
# Helpers — build minimal YAML fixture files
# ---------------------------------------------------------------------------

def write_multi_doc_yaml(path, docs):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        yaml.dump_all(docs, f)


def write_yaml(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        yaml.dump(data, f)


def make_bundle(name, image='quay.io/rhoai/odh-operator-bundle@sha256:abc123'):
    return {
        'schema': CONSTANTS.OLM_BUNDLE_SCHEMA,
        'name': name,
        'image': image,
        'relatedImages': [
            {'name': '', 'image': image},
            {'name': 'operator_image', 'image': 'registry.redhat.io/rhoai/odh-rhel9-operator@sha256:def456'},
        ]
    }


def make_channel(name, entry_names):
    return {
        'schema': CONSTANTS.OLM_CHANNEL_SCHEMA,
        'name': name,
        'package': 'rhods-operator',
        'entries': [{'name': n} for n in entry_names]
    }


def make_package(default_channel='stable'):
    return {
        'schema': CONSTANTS.OLM_PACKAGE_SCHEMA,
        'name': 'rhods-operator',
        'defaultChannel': default_channel,
        'icon': {'base64data': 'icondata', 'mediatype': 'image/svg+xml'}
    }


def make_push_pipeline(enabled=True):
    cel = '"catalog/**".pathChanged()'
    if not enabled:
        cel = '"non-existent-file.non-existent-ext".pathChanged() && ' + cel
    return {
        'metadata': {
            'annotations': {
                'pipelinesascode.tekton.dev/on-cel-expression': cel
            }
        }
    }


def make_build_config():
    return {
        'config': {
            'replacements': [{
                'registry': 'quay.io',
                'repo_mappings': {
                    'rhoai/odh-operator-bundle': 'rhoai/odh-operator-bundle',
                    'rhoai/odh-rhel9-operator': 'rhoai/odh-rhel9-operator',
                }
            }]
        }
    }


def make_patch_dict(default_channel='stable-3.x', channel_name='beta', entry_name='rhods-operator.3.5.0-ea.1'):
    return {
        'patch': {
            'olm.package': {'name': 'rhods-operator', 'defaultChannel': default_channel},
            'olm.channels': [{
                'name': channel_name,
                'schema': 'olm.channel',
                'package': 'rhods-operator',
                'entries': [{'name': entry_name, 'skipRange': '>=3.4.0 <3.5.0'}]
            }]
        }
    }


def make_sbc_docs(bundle_name='rhods-operator.3.5.0-ea.1',
                  image='quay.io/rhoai/odh-operator-bundle@sha256:abc123'):
    return [
        {'schema': CONSTANTS.OLM_PACKAGE_SCHEMA, 'name': 'rhods-operator', 'defaultChannel': 'stable-v3'},
        {'schema': CONSTANTS.OLM_CHANNEL_SCHEMA, 'name': 'stable-v3',
         'entries': [{'name': bundle_name}]},
        make_bundle(bundle_name, image)
    ]


def setup_processor_files(tmp_path, catalog_docs=None, sbc_docs=None,
                          patch_dict=None, build_config=None, push_pipeline=None):
    """Write all fixture files and return paths dict."""
    catalog_path = str(tmp_path / 'input-catalog.yaml')
    sbc_path = str(tmp_path / 'sbc.yaml')
    patch_path = str(tmp_path / 'catalog-patch.yaml')
    build_config_path = str(tmp_path / 'build-config.yaml')
    push_pipeline_path = str(tmp_path / 'push-pipeline.yaml')
    output_path = str(tmp_path / 'output' / 'catalog.yaml')
    build_args_path = str(tmp_path / 'catalog_build_args.map')

    write_multi_doc_yaml(catalog_path, catalog_docs or [
        make_package(),
        make_channel('fast', ['rhods-operator.2.25.6']),
        make_channel('beta', ['rhods-operator.3.4.0-ea.2']),
        make_bundle('rhods-operator.2.25.6', 'registry.redhat.io/rhoai/odh-operator-bundle@sha256:existing'),
        make_bundle('rhods-operator.3.4.0-ea.2', 'registry.redhat.io/rhoai/odh-operator-bundle@sha256:oldea'),
    ])
    write_multi_doc_yaml(sbc_path, sbc_docs or make_sbc_docs())
    write_yaml(patch_path, patch_dict or make_patch_dict())
    write_yaml(build_config_path, build_config or make_build_config())
    write_yaml(push_pipeline_path, push_pipeline or make_push_pipeline())

    return {
        'input_catalog_path': catalog_path,
        'single_bundle_catalog_path': sbc_path,
        'patch_yaml_path': patch_path,
        'build_config_path': build_config_path,
        'push_pipeline_yaml_path': push_pipeline_path,
        'output_catalog_path': output_path,
        'catalog_build_args_file_path': build_args_path,
    }


@pytest.fixture
def processor_factory(tmp_path):
    """Factory that creates an fbc_processor with controlled fixtures."""
    from importlib import import_module
    # Import as module since filename has hyphens
    spec = __import__('importlib').util.spec_from_file_location(
        'fbc_processor_mod',
        os.path.join(processors_root, 'fbc-processor.py')
    )
    mod = __import__('importlib').util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    def _factory(paths=None, purge_bundles='', bundle_git_url='https://github.com/org/repo', **overrides):
        if paths is None:
            paths = setup_processor_files(tmp_path, **overrides)

        with patch.object(mod.util, 'fetch_file_data_from_github', return_value='COMP_GIT_URL=http://x\nCOMP_GIT_COMMIT=abc\n'), \
             patch.object(mod.util, 'fetch_files_from_git_repo', return_value={CONSTANTS.BUNDLE_BUILD_ARGS_PATH: 'COMP_GIT_URL=http://x\nCOMP_GIT_COMMIT=abc\n'}):
            processor = mod.fbc_processor(
                rhoai_version='rhoai-3.5-ea.1',
                build_type='ci',
                build_config_path=paths['build_config_path'],
                patch_yaml_path=paths['patch_yaml_path'],
                single_bundle_catalog_path=paths['single_bundle_catalog_path'],
                input_catalog_path=paths['input_catalog_path'],
                output_catalog_path=paths['output_catalog_path'],
                bundle_git_url=bundle_git_url,
                bundle_git_commit='abc123',
                catalog_build_args_file_path=paths['catalog_build_args_file_path'],
                push_pipeline_yaml_path=paths['push_pipeline_yaml_path'],
                push_pipeline_operation='enable',
                purge_bundles=purge_bundles,
            )
        return processor, mod

    return _factory


# ===========================================================================
# patch_catalog — orchestration tests
# ===========================================================================

class TestPatchCatalog:

    def test_happy_path_new_bundle_added(self, processor_factory):
        """New bundle from SBC is added to catalog with image replacement."""
        processor, _ = processor_factory()
        processor.patch_catalog()

        bundle = processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA].get('rhods-operator.3.5.0-ea.1')
        assert bundle is not None
        # Top-level image replaced
        assert 'registry.redhat.io' in bundle['image']
        assert 'quay.io' not in bundle['image']

    def test_bundle_image_replacement_relatedimages_entry(self, processor_factory):
        """The relatedImages entry matching the bundle image is also replaced."""
        processor, _ = processor_factory()
        processor.patch_catalog()

        bundle = processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]['rhods-operator.3.5.0-ea.1']
        bundle_related = [e for e in bundle['relatedImages'] if e.get('image', '') == bundle['image']]
        assert len(bundle_related) >= 1

    def test_other_relatedimages_untouched(self, processor_factory):
        """relatedImages already on registry.redhat.io are not modified."""
        processor, _ = processor_factory()
        processor.patch_catalog()

        bundle = processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]['rhods-operator.3.5.0-ea.1']
        operator_entry = [e for e in bundle['relatedImages'] if e['name'] == 'operator_image']
        assert len(operator_entry) == 1
        assert operator_entry[0]['image'] == 'registry.redhat.io/rhoai/odh-rhel9-operator@sha256:def456'

    def test_idempotent_bundle_already_exists(self, processor_factory, tmp_path):
        """If bundle already in catalog, patch_catalog skips everything."""
        # Add the bundle to the catalog before patching
        catalog_docs = [
            make_package(),
            make_channel('fast', ['rhods-operator.3.5.0-ea.1']),
            make_bundle('rhods-operator.3.5.0-ea.1', 'registry.redhat.io/rhoai/odh-operator-bundle@sha256:existing'),
        ]
        paths = setup_processor_files(tmp_path, catalog_docs=catalog_docs)
        processor, _ = processor_factory(paths=paths)

        original_dict = dict(processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA])
        processor.patch_catalog()

        # Nothing changed
        assert processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA] == original_dict

    def test_missing_sbc_exits(self, processor_factory, tmp_path):
        """If SBC has no olm.bundle, sys.exit(1) is called."""
        sbc_docs = [
            {'schema': CONSTANTS.OLM_PACKAGE_SCHEMA, 'name': 'rhods-operator'},
            {'schema': CONSTANTS.OLM_CHANNEL_SCHEMA, 'name': 'stable-v3', 'entries': []},
        ]
        paths = setup_processor_files(tmp_path, sbc_docs=sbc_docs)
        processor, _ = processor_factory(paths=paths)

        with pytest.raises(SystemExit):
            processor.patch_catalog()

    def test_default_channel_updated(self, processor_factory):
        """After patching, defaultChannel reflects the patch definition."""
        processor, _ = processor_factory()
        processor.patch_catalog()

        pkg = processor.catalog_dict[CONSTANTS.OLM_PACKAGE_SCHEMA]['rhods-operator']
        assert pkg['defaultChannel'] == 'stable-3.x'

    def test_beta_channel_reset(self, processor_factory):
        """Beta channel (RESET_CHANNEL) is fully replaced — old EA entry gone."""
        processor, _ = processor_factory()
        processor.patch_catalog()

        beta = processor.catalog_dict[CONSTANTS.OLM_CHANNEL_SCHEMA]['beta']
        entry_names = [e['name'] for e in beta['entries']]
        assert 'rhods-operator.3.5.0-ea.1' in entry_names
        assert 'rhods-operator.3.4.0-ea.2' not in entry_names

    def test_orphan_purged_after_reset(self, processor_factory):
        """Old EA bundle becomes orphan after beta reset and is purged."""
        processor, _ = processor_factory()
        processor.patch_catalog()

        assert 'rhods-operator.3.4.0-ea.2' not in processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_explicit_purge(self, processor_factory):
        """--purge-bundles removes the named bundle."""
        processor, _ = processor_factory(purge_bundles='rhods-operator.2.25.6')
        processor.patch_catalog()

        assert 'rhods-operator.2.25.6' not in processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_no_explicit_purge(self, processor_factory):
        """Without --purge-bundles, referenced bundles are not removed."""
        processor, _ = processor_factory()
        processor.patch_catalog()

        # 2.25.6 is still referenced by fast channel — should remain
        assert 'rhods-operator.2.25.6' in processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_two_pass_flow(self, processor_factory, tmp_path):
        """Simulates the Taskfile two-pass: current release then previous release."""
        # Pass 1: add current release bundle
        paths = setup_processor_files(tmp_path)
        processor, mod = processor_factory(paths=paths)

        with patch.object(mod.util, 'fetch_file_data_from_github', return_value=''):
            processor.catalog_build_args = 'KEY=VALUE\n'
        processor.patch_catalog()

        # Pass 2: add previous release bundle to the same catalog
        prev_sbc_docs = make_sbc_docs(
            bundle_name='rhods-operator.2.25.7',
            image='quay.io/rhoai/odh-operator-bundle@sha256:prev123'
        )
        prev_sbc_path = str(tmp_path / 'prev-sbc.yaml')
        write_multi_doc_yaml(prev_sbc_path, prev_sbc_docs)

        prev_patch = make_patch_dict(
            default_channel='stable',
            channel_name='fast',
            entry_name='rhods-operator.2.25.7'
        )
        prev_patch_path = str(tmp_path / 'prev-patch.yaml')
        write_yaml(prev_patch_path, prev_patch)

        # Update paths for pass 2
        processor.single_bundle_catalog_path = prev_sbc_path
        processor.patch_yaml_path = prev_patch_path
        processor.patch_dict = prev_patch
        processor.patch_catalog()

        # Both bundles should be in the catalog
        assert 'rhods-operator.3.5.0-ea.1' in processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]
        assert 'rhods-operator.2.25.7' in processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]

    def test_no_matching_replacement(self, processor_factory, tmp_path):
        """Bundle image not in repo_mappings — no replacement applied."""
        sbc_docs = make_sbc_docs(
            bundle_name='rhods-operator.3.5.0-ea.1',
            image='some-other-registry.io/other-repo@sha256:xyz'
        )
        paths = setup_processor_files(tmp_path, sbc_docs=sbc_docs)
        processor, _ = processor_factory(paths=paths)
        processor.patch_catalog()

        bundle = processor.catalog_dict[CONSTANTS.OLM_BUNDLE_SCHEMA]['rhods-operator.3.5.0-ea.1']
        assert bundle['image'] == 'some-other-registry.io/other-repo@sha256:xyz'

    def test_non_github_repo_uses_git_clone(self, processor_factory):
        """Non-GitHub bundle_git_url uses fetch_files_from_git_repo instead of raw.githubusercontent."""
        processor, mod = processor_factory(bundle_git_url='https://gitlab.com/redhat/rhel-ai/repo')

        # Verify the processor was created successfully with GitLab URL
        assert processor.bundle_git_url == 'https://gitlab.com/redhat/rhel-ai/repo'

        # generate_catalog_build_args is called during process(), mock must be active
        with patch.object(mod.util, 'fetch_files_from_git_repo',
                          return_value={CONSTANTS.BUNDLE_BUILD_ARGS_PATH: 'COMP_GIT_URL=http://x\nCOMP_GIT_COMMIT=abc\n'}):
            processor.catalog_build_args = processor.generate_catalog_build_args()

        assert 'COMP_GIT_URL' in processor.catalog_build_args
