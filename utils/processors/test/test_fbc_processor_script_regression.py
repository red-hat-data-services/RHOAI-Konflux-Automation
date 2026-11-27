"""
Regression tests for the production fbc-processor script
(utils/fbc-processor/fbc-processor.py) — the catalog-patch entry point
executed by the RHOAI-Build-Config "Process FBC Fragment" workflow.

These tests guard against the guarded-write regression fixed in
RHOAI-Konflux-Automation#75: patch_catalog_yaml used to invoke
write_output_catalog() only when the current branch bundle was newly
added to the base (PCC) catalog. When the branch bundle had already
shipped and was present in the base, the processor skipped the write
entirely, so newly shipped bundles and channel entries from OTHER
branches never flowed into quiet release branch catalogs, and the
catalog validator failed with:

    ERROR - Missing rhods-operator.3.5.4 in OCP v4.19 catalog

(See RHOAI-Build-Config "Process FBC Fragment" run 45567374030 on the
rhoai-2.25 branch: the PCC checkout contained rhods-operator.3.5.4 with
all channel entries, but the output catalog was never written because
rhods-operator.2.25.13 was already present in the base.)

The script filename contains a dash, so it cannot be imported as a
regular module; it is loaded by path via importlib.
"""

import importlib.util
import os

import pytest
import yaml

SCRIPT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'fbc-processor', 'fbc-processor.py')
)


def load_script_module():
    """Load utils/fbc-processor/fbc-processor.py as a module (dash in name)."""
    spec = importlib.util.spec_from_file_location('fbc_processor_script', SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def make_bundle(name, image='registry.redhat.io/rhoai/odh-operator-bundle@sha256:existing'):
    return {
        'schema': 'olm.bundle',
        'name': name,
        'package': 'rhods-operator',
        'image': image,
        'relatedImages': [
            {'name': '', 'image': image},
        ],
    }


def make_channel(name, entry_names):
    return {
        'schema': 'olm.channel',
        'name': name,
        'package': 'rhods-operator',
        'entries': [{'name': n} for n in entry_names],
    }


def make_package(default_channel='stable-3.x'):
    return {
        'schema': 'olm.package',
        'name': 'rhods-operator',
        'defaultChannel': default_channel,
        'icon': {'base64data': 'icondata', 'mediatype': 'image/svg+xml'},
    }


def make_build_config():
    return {
        'config': {
            'replacements': [{
                'registry': 'quay.io',
                'repo_mappings': {
                    'rhoai/odh-operator-bundle': 'rhoai/odh-operator-bundle',
                },
            }],
        },
    }


def make_patch_dict(channel='stable-2.25', entry='rhods-operator.2.25.13'):
    return {
        'patch': {
            'olm.package': {'name': 'rhods-operator', 'defaultChannel': 'stable-3.x'},
            'olm.channels': [{
                'name': channel,
                'schema': 'olm.channel',
                'package': 'rhods-operator',
                'entries': [{'name': entry, 'replaces': 'rhods-operator.2.25.12'}],
            }],
        },
    }


def make_push_pipeline(enabled=True):
    cel = '"catalog/catalog-patch.yaml".pathChanged()'
    if not enabled:
        cel = '"non-existent-file.non-existent-ext".pathChanged() && ' + cel
    return {
        'metadata': {
            'annotations': {
                'pipelinesascode.tekton.dev/on-cel-expression': cel,
            },
        },
    }


def write_yaml_docs(path, docs):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        yaml.dump_all(docs, f)


def write_yaml(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        yaml.dump(data, f)


def setup_script_processor(tmp_path, base_docs, sbc_bundle_name='rhods-operator.2.25.13'):
    """Write fixture files and build an fbc_processor (script version)."""
    module = load_script_module()

    base_catalog = str(tmp_path / 'input-catalog.yaml')
    sbc = str(tmp_path / 'sbc.yaml')
    patch = str(tmp_path / 'catalog-patch.yaml')
    build_config = str(tmp_path / 'build-config.yaml')
    push_pipeline = str(tmp_path / 'push-pipeline.yaml')
    output = str(tmp_path / 'output' / 'catalog.yaml')

    write_yaml_docs(base_catalog, base_docs)
    write_yaml_docs(sbc, [
        make_package('stable-2.25'),
        make_channel('stable-2.25', [sbc_bundle_name]),
        make_bundle(sbc_bundle_name, 'quay.io/rhoai/odh-operator-bundle@sha256:sbc'),
    ])
    write_yaml(patch, make_patch_dict(entry=sbc_bundle_name))
    write_yaml(build_config, make_build_config())
    write_yaml(push_pipeline, make_push_pipeline(enabled=True))
    # the script opens the output path directly; the parent dir must exist
    # (in the workflow it is the already-checked-out branch catalog dir)
    os.makedirs(os.path.dirname(output), exist_ok=True)

    processor = module.fbc_processor(
        build_config_path=build_config,
        catalog_yaml_path=base_catalog,
        patch_yaml_path=patch,
        single_bundle_path=sbc,
        output_file_path=output,
        push_pipeline_operation='enable',
        push_pipeline_yaml_path=push_pipeline,
        purge_bundles='',
    )
    return processor, output


def read_output_bundles_and_channels(output_path):
    with open(output_path) as f:
        docs = list(yaml.safe_load_all(f))
    bundles = {d['name'] for d in docs if d and d.get('schema') == 'olm.bundle'}
    channels = {
        d['name']: [e['name'] for e in d.get('entries', [])]
        for d in docs if d and d.get('schema') == 'olm.channel'
    }
    return bundles, channels


# ---------------------------------------------------------------------------
# Regression tests
# ---------------------------------------------------------------------------

class TestGuardedWriteRegression:
    """Base catalog already contains the current bundle AND newer entries."""

    def test_writes_output_when_base_already_contains_current_bundle(self, tmp_path):
        """
        The exact production scenario from run 45567374030:

        The base (PCC) catalog already contains the branch's current
        bundle (rhods-operator.2.25.13 — shipped), so patch_olm_bundles()
        has nothing to add. The base ALSO contains newer entries shipped
        from another branch (rhods-operator.3.5.4 bundle + channel
        entries) that the release branch catalog does not have yet.

        patch_catalog_yaml() must still write the output catalog,
        carrying the newer entries into the branch catalog. The
        guarded-write bug skipped the write entirely, leaving the branch
        catalog stale and failing catalog validation.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.12', 'rhods-operator.2.25.13']),
            make_channel('stable-3.x', ['rhods-operator.3.5.3', 'rhods-operator.3.5.4']),
            make_bundle('rhods-operator.2.25.12'),
            make_bundle('rhods-operator.2.25.13'),
            make_bundle('rhods-operator.3.5.3'),
            make_bundle('rhods-operator.3.5.4'),
        ]
        processor, output = setup_script_processor(tmp_path, base_docs)

        assert not os.path.exists(output), 'output must not exist before the run'

        processor.patch_catalog_yaml()

        assert os.path.exists(output), (
            'REGRESSION: output catalog was not written even though the base '
            'catalog contains newer entries (guarded-write bug from '
            'RHOAI-Konflux-Automation#75)'
        )
        bundles, channels = read_output_bundles_and_channels(output)
        assert 'rhods-operator.3.5.4' in bundles, 'newer bundle from another branch must flow to the output'
        assert 'rhods-operator.3.5.4' in channels['stable-3.x'], 'newer channel entry must flow to the output'
        assert 'rhods-operator.2.25.13' in bundles, 'current bundle must be retained'
        assert 'rhods-operator.2.25.13' in channels['stable-2.25'], 'patched channel must be retained'

    def test_writes_output_with_newer_package_entry(self, tmp_path):
        """
        Same guarded-write scenario, focused on olm.package: the base has a
        newer defaultChannel than the branch's committed catalog. The output
        must be written with the patched package definition.
        """
        base_docs = [
            make_package(default_channel='stable-2.25'),
            make_channel('stable-2.25', ['rhods-operator.2.25.13']),
            make_channel('stable-3.x', ['rhods-operator.3.5.4']),
            make_bundle('rhods-operator.2.25.13'),
            make_bundle('rhods-operator.3.5.4'),
        ]
        processor, output = setup_script_processor(tmp_path, base_docs)

        processor.patch_catalog_yaml()

        assert os.path.exists(output), 'REGRESSION: output catalog was not written'
        with open(output) as f:
            docs = list(yaml.safe_load_all(f))
        packages = [d for d in docs if d and d.get('schema') == 'olm.package']
        assert packages and packages[0]['defaultChannel'] == 'stable-3.x', (
            'patched olm.package (newer defaultChannel) must reach the output'
        )

    def test_purges_orphan_bundles_even_when_current_bundle_present(self, tmp_path):
        """
        Always-write implies always-purge: an orphan bundle (present in the
        base but referenced by no channel) must be dropped from the output
        even when the current bundle is already present and nothing is
        added. Under the guarded-write bug the purge was skipped along with
        the write.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.13']),
            make_bundle('rhods-operator.2.25.13'),
            make_bundle('rhods-operator.9.9.9'),  # orphan: referenced by no channel
        ]
        processor, output = setup_script_processor(tmp_path, base_docs)

        processor.patch_catalog_yaml()

        assert os.path.exists(output), 'REGRESSION: output catalog was not written'
        bundles, _ = read_output_bundles_and_channels(output)
        assert 'rhods-operator.9.9.9' not in bundles, 'orphan bundle must be purged'

    def test_output_is_idempotent_on_rerun(self, tmp_path):
        """
        The always-write must not introduce churn: running the patch twice
        with the same inputs produces byte-identical output, so the
        workflow's commit step stays a no-op when nothing changed.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.12', 'rhods-operator.2.25.13']),
            make_channel('stable-3.x', ['rhods-operator.3.5.3', 'rhods-operator.3.5.4']),
            make_bundle('rhods-operator.2.25.12'),
            make_bundle('rhods-operator.2.25.13'),
            make_bundle('rhods-operator.3.5.3'),
            make_bundle('rhods-operator.3.5.4'),
        ]
        processor, output = setup_script_processor(tmp_path, base_docs)
        processor.patch_catalog_yaml()
        with open(output) as f:
            first = f.read()

        processor2, output2 = setup_script_processor(tmp_path, base_docs)
        processor2.patch_catalog_yaml()
        with open(output2) as f:
            second = f.read()

        assert first == second, 'rerunning the patch must not change the output'


class TestOriginalBehaviorPreserved:
    """The fix must not break the original add-missing-bundle flow."""

    def test_adds_current_bundle_when_missing_from_base(self, tmp_path):
        """
        Original behavior: when the current branch bundle is NOT yet in the
        base catalog (branch under active development), it is added and the
        output catalog is written.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.12']),
            make_channel('stable-3.x', ['rhods-operator.3.5.4']),
            make_bundle('rhods-operator.2.25.12'),
            make_bundle('rhods-operator.3.5.4'),
        ]
        processor, output = setup_script_processor(tmp_path, base_docs)

        processor.patch_catalog_yaml()

        assert os.path.exists(output), 'output catalog must be written'
        bundles, channels = read_output_bundles_and_channels(output)
        assert 'rhods-operator.2.25.13' in bundles, 'missing current bundle must be added'
        assert 'rhods-operator.2.25.13' in channels['stable-2.25'], (
            'current bundle entry must be merged into the patched channel'
        )
        assert 'rhods-operator.3.5.4' in bundles


class TestChannelBundleReferenceValidation:
    """Validate that channels cannot reference bundles missing from the catalog.

    Guards against the scenario where extract-snapshot-images picks a stale
    image, opm render-template produces a bundle with a stale name (e.g.
    rhods-operator.2.25.13), but the patch file's channel entries reference
    the latest version (e.g. rhods-operator.2.25.14). After purge_unused_olm_bundles
    removes unreferenced bundles, the catalog would contain dangling channel
    references. The validation must catch this before write_output_catalog.
    """

    def test_detects_missing_bundle_referenced_by_channel(self, tmp_path):
        """
        Simulate the stale-snapshot scenario:
        - Base catalog has bundle .2.25.12
        - SBC adds bundle .2.25.13 (stale opm render output)
        - Patch file's channel entries reference .2.25.14 (the latest version)

        After patching, .2.25.14 is referenced by the channel but has no
        olm.bundle definition. The validation must raise SystemExit with a
        clear error message naming the missing bundle.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.12']),
            make_bundle('rhods-operator.2.25.12'),
        ]
        # SBC adds .2.25.13 (stale), but patch references .2.25.14 (latest)
        module = load_script_module()

        base_catalog = str(tmp_path / 'input-catalog.yaml')
        sbc = str(tmp_path / 'sbc.yaml')
        patch = str(tmp_path / 'catalog-patch.yaml')
        build_config = str(tmp_path / 'build-config.yaml')
        push_pipeline = str(tmp_path / 'push-pipeline.yaml')
        output = str(tmp_path / 'output' / 'catalog.yaml')

        write_yaml_docs(base_catalog, base_docs)
        # SBC has the stale bundle name (.2.25.13)
        write_yaml_docs(sbc, [
            make_package('stable-2.25'),
            make_channel('stable-2.25', ['rhods-operator.2.25.13']),
            make_bundle('rhods-operator.2.25.13', 'quay.io/rhoai/odh-operator-bundle@sha256:stale'),
        ])
        # Patch references the LATEST version (.2.25.14) — mismatch!
        write_yaml(patch, make_patch_dict(entry='rhods-operator.2.25.14'))
        write_yaml(build_config, make_build_config())
        write_yaml(push_pipeline, make_push_pipeline(enabled=True))
        os.makedirs(os.path.dirname(output), exist_ok=True)

        processor = module.fbc_processor(
            build_config_path=build_config,
            catalog_yaml_path=base_catalog,
            patch_yaml_path=patch,
            single_bundle_path=sbc,
            output_file_path=output,
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=push_pipeline,
            purge_bundles='',
        )

        with pytest.raises(SystemExit) as exc_info:
            processor.patch_catalog_yaml()

        # Should exit with code 1
        assert exc_info.value.code == 1

    def test_error_message_names_missing_bundle_and_channel(self, tmp_path, capsys):
        """
        The error message must name the missing bundle and the channel(s)
        that reference it, so operators can diagnose the stale snapshot.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.12']),
            make_bundle('rhods-operator.2.25.12'),
        ]
        module = load_script_module()

        base_catalog = str(tmp_path / 'input-catalog.yaml')
        sbc = str(tmp_path / 'sbc.yaml')
        patch_path = str(tmp_path / 'catalog-patch.yaml')
        build_config = str(tmp_path / 'build-config.yaml')
        push_pipeline = str(tmp_path / 'push-pipeline.yaml')
        output = str(tmp_path / 'output' / 'catalog.yaml')

        write_yaml_docs(base_catalog, base_docs)
        write_yaml_docs(sbc, [
            make_package('stable-2.25'),
            make_channel('stable-2.25', ['rhods-operator.2.25.13']),
            make_bundle('rhods-operator.2.25.13', 'quay.io/rhoai/odh-operator-bundle@sha256:stale'),
        ])
        write_yaml(patch_path, make_patch_dict(entry='rhods-operator.2.25.14'))
        write_yaml(build_config, make_build_config())
        write_yaml(push_pipeline, make_push_pipeline(enabled=True))
        os.makedirs(os.path.dirname(output), exist_ok=True)

        processor = module.fbc_processor(
            build_config_path=build_config,
            catalog_yaml_path=base_catalog,
            patch_yaml_path=patch_path,
            single_bundle_path=sbc,
            output_file_path=output,
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=push_pipeline,
            purge_bundles='',
        )

        with pytest.raises(SystemExit):
            processor.patch_catalog_yaml()

        captured = capsys.readouterr()
        assert 'rhods-operator.2.25.14' in captured.out, \
            'error must name the missing bundle'
        assert 'stable-2.25' in captured.out, \
            'error must name the channel referencing the missing bundle'

    def test_valid_catalog_passes_validation(self, tmp_path):
        """
        A catalog where every channel entry has a corresponding olm.bundle
        must pass the validation without error — the happy path.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.12', 'rhods-operator.2.25.13']),
            make_bundle('rhods-operator.2.25.12'),
            make_bundle('rhods-operator.2.25.13'),
        ]
        processor, output = setup_script_processor(tmp_path, base_docs)

        # Must not raise
        processor.patch_catalog_yaml()

        assert os.path.exists(output), 'valid catalog must be written'
        bundles, channels = read_output_bundles_and_channels(output)
        assert 'rhods-operator.2.25.13' in bundles
        assert 'rhods-operator.2.25.13' in channels['stable-2.25']

    def test_multiple_channels_reference_same_missing_bundle(self, tmp_path):
        """
        When multiple channels reference the same missing bundle, the error
        message must list all channels.
        """
        base_docs = [
            make_package(),
            make_channel('stable-2.25', ['rhods-operator.2.25.12']),
            make_bundle('rhods-operator.2.25.12'),
        ]
        module = load_script_module()

        base_catalog = str(tmp_path / 'input-catalog.yaml')
        sbc = str(tmp_path / 'sbc.yaml')
        patch_path = str(tmp_path / 'catalog-patch.yaml')
        build_config = str(tmp_path / 'build-config.yaml')
        push_pipeline = str(tmp_path / 'push-pipeline.yaml')
        output = str(tmp_path / 'output' / 'catalog.yaml')

        write_yaml_docs(base_catalog, base_docs)
        write_yaml_docs(sbc, [
            make_package('stable-2.25'),
            make_channel('stable-2.25', ['rhods-operator.2.25.13']),
            make_bundle('rhods-operator.2.25.13', 'quay.io/rhoai/odh-operator-bundle@sha256:stale'),
        ])
        # Patch adds two channels both referencing the missing .2.25.14
        patch_dict = {
            'patch': {
                'olm.package': {'name': 'rhods-operator', 'defaultChannel': 'stable-3.x'},
                'olm.channels': [
                    {
                        'name': 'stable-2.25',
                        'schema': 'olm.channel',
                        'package': 'rhods-operator',
                        'entries': [{'name': 'rhods-operator.2.25.14', 'replaces': 'rhods-operator.2.25.12'}],
                    },
                    {
                        'name': 'fast',
                        'schema': 'olm.channel',
                        'package': 'rhods-operator',
                        'entries': [{'name': 'rhods-operator.2.25.14'}],
                    },
                ],
            },
        }
        write_yaml(patch_path, patch_dict)
        write_yaml(build_config, make_build_config())
        write_yaml(push_pipeline, make_push_pipeline(enabled=True))
        os.makedirs(os.path.dirname(output), exist_ok=True)

        processor = module.fbc_processor(
            build_config_path=build_config,
            catalog_yaml_path=base_catalog,
            patch_yaml_path=patch_path,
            single_bundle_path=sbc,
            output_file_path=output,
            push_pipeline_operation='enable',
            push_pipeline_yaml_path=push_pipeline,
            purge_bundles='',
        )

        with pytest.raises(SystemExit) as exc_info:
            processor.patch_catalog_yaml()

        assert exc_info.value.code == 1
