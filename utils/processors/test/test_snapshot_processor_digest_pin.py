"""
Regression tests for the --expected-bundle-digest pin in the production
fbc-processor script (utils/fbc-processor/fbc-processor.py) — the
extract-snapshot-images entry point executed by the release-nightly
build-fbc stage.

These tests guard against the stale-nightly-image race (RHOAI-Build-Config
rhoai-2.25, 2026-11-27): extract-snapshot-images picks the newest SIGNED tag
on the Quay nightly tag, so when the nightly bundle build has pushed a fresh
digest but its signature has not landed yet (or a concurrent build moved the
tag), the processor silently falls back to the previous build's image. The
single-bundle catalog is then rendered from the stale CSV while
catalog-patch.yaml references the new version, and the committed catalog
carries dangling channel references that fail the Konflux FBC build.

The fix: when the caller passes the digest the bundle build proved it pushed
(--expected-bundle-digest), the processor pins the odh-operator-bundle
selection to that digest — waiting for the tag to move to it and for its
signature to appear — and fails loudly instead of falling back to a stale
image.

The script filename contains a dash, so it cannot be imported as a regular
module; it is loaded by path via importlib (same pattern as
test_fbc_processor_script_regression.py).
"""

import importlib.util
import json
import os

import pytest
import yaml

SCRIPT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'fbc-processor', 'fbc-processor.py')
)

# Stand-ins for the two images from the 2026-11-27 incident:
# 42d9b31f... (stale, signed, CSV 2.25.13) and b25cc276... (fresh, CSV 2.25.14).
STALE_DIGEST = 'sha256:' + '4' * 64
FRESH_DIGEST = 'sha256:' + '2' * 64

GIT_LABELS = [
    {'key': 'git.url', 'value': 'https://github.com/red-hat-data-services/RHOAI-Build-Config'},
    {'key': 'git.commit', 'value': 'freshcommitsha'},
]


def load_script_module():
    """Load utils/fbc-processor/fbc-processor.py as a module (dash in name)."""
    spec = importlib.util.spec_from_file_location('fbc_processor_script', SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_tag(digest, last_modified):
    return {'name': 'rhoai-2.25-nightly', 'manifest_digest': digest, 'last_modified': last_modified}


def setup_snapshot_processor(tmp_path, expected_digest='', repo_path='rhoai/odh-operator-bundle',
                             image_filter='odh-operator-bundle', build_type='nightly'):
    """Write fixture files and build a snapshot_processor (script version)."""
    module = load_script_module()

    build_config = str(tmp_path / 'build-config.yaml')
    output = str(tmp_path / 'snapshot_images.json')
    build_args = str(tmp_path / 'catalog_build_args.map')
    with open(build_config, 'w') as f:
        yaml.dump({'config': {'replacements': [{
            'registry': 'quay.io',
            'repo_mappings': {repo_path: repo_path},
        }]}}, f)

    processor = module.snapshot_processor(
        snapshot_json_path=str(tmp_path / 'snapshot.json'),
        output_file_path=output,
        rhoai_version='rhoai-2.25',
        build_config_path=build_config,
        catalog_build_args_file_path=build_args,
        build_type=build_type,
        image_filter=image_filter,
        expected_bundle_digest=expected_digest,
    )
    # keep the polling loop fast: 3 attempts, no real sleeping
    processor.EXPECTED_DIGEST_MAX_ATTEMPTS = 3
    processor.EXPECTED_DIGEST_RETRY_INTERVAL = 0
    return processor, module, output, build_args


def install_fake_quay(module, get_tags, get_signed_digests):
    """Replace the script's quay_controller with an in-memory fake.

    *get_tags* and *get_signed_digests* are zero-arg callables so a test can
    change the registry state between polling attempts (signature landing,
    tag moving). Returns the fake class (its instances record call counts).
    """
    calls = {'get_all_tags': 0, 'get_tag_details': 0, 'get_git_labels': []}

    class FakeQuayController:
        def __init__(self, org):
            self.org = org

        def get_all_tags(self, repo, tag):
            calls['get_all_tags'] += 1
            return list(get_tags())

        def get_tag_details(self, repo, sig_tag):
            calls['get_tag_details'] += 1
            if not sig_tag.endswith('.sig'):
                return {}
            digest = sig_tag[:-len('.sig')].replace('-', ':', 1)
            return {'name': sig_tag} if digest in get_signed_digests() else {}

        def get_git_labels(self, repo, digest):
            calls['get_git_labels'].append(digest)
            return GIT_LABELS

    module.quay_controller = FakeQuayController
    # no restore needed: every test loads a fresh module instance via
    # importlib, so the patch cannot leak into other tests
    return calls


def run_get_all_latest_images(processor, sleeps):
    """Run get_all_latest_images with time.sleep recorded instead of sleeping."""
    module_time = __import__('time')
    original_sleep = module_time.sleep
    module_time.sleep = lambda s: sleeps.append(s)
    try:
        processor.get_all_latest_images()
    finally:
        module_time.sleep = original_sleep


def read_output_images(output_path):
    with open(output_path) as f:
        return {entry['name']: entry['value'] for entry in json.load(f)}


class TestExpectedDigestPin:
    """--expected-bundle-digest pins the bundle repo to the pushed digest."""

    def test_pinned_digest_wins_over_newest_signed_tag(self, tmp_path):
        """
        The exact production shape of 2026-11-27, inverted by the fix: the
        stale image is the newest SIGNED entry on the tag, the expected
        (fresh) digest is also present and signed. The pinned digest must be
        selected even though it is not the newest, and the git labels must
        come from the pinned digest's manifest.
        """
        processor, module, output, build_args = setup_snapshot_processor(
            tmp_path, expected_digest=FRESH_DIGEST)
        tags = [make_tag(STALE_DIGEST, 'Wed, 26 Nov 2026 10:00:00 -0000'),
                make_tag(FRESH_DIGEST, 'Tue, 25 Nov 2026 22:43:00 -0000')]
        calls = install_fake_quay(module, get_tags=lambda: tags,
                                  get_signed_digests=lambda: {STALE_DIGEST, FRESH_DIGEST})

        run_get_all_latest_images(processor, sleeps=[])

        images = read_output_images(output)
        assert images['RELATED_IMAGE_ODH_OPERATOR_BUNDLE_IMAGE'] == f'quay.io/rhoai/odh-operator-bundle@{FRESH_DIGEST}'
        assert calls['get_git_labels'] == [FRESH_DIGEST], 'git labels must come from the pinned digest'
        with open(build_args) as f:
            assert 'ODH_OPERATOR_BUNDLE_GIT_COMMIT=freshcommitsha' in f.read()

    def test_waits_for_signature_to_appear(self, tmp_path):
        """
        The fresh digest is on the tag but its cosign signature has not landed
        yet. The processor must poll (sleep between attempts) and pick it once
        the signature appears, instead of falling back to the stale signed
        image on the first attempt.
        """
        processor, module, output, _ = setup_snapshot_processor(
            tmp_path, expected_digest=FRESH_DIGEST)
        tags = [make_tag(STALE_DIGEST, 'Wed, 26 Nov 2026 10:00:00 -0000'),
                make_tag(FRESH_DIGEST, 'Tue, 25 Nov 2026 22:43:00 -0000')]
        state = {'signed': {STALE_DIGEST}}
        install_fake_quay(module, get_tags=lambda: tags,
                          get_signed_digests=lambda: state['signed'])

        sleeps = []
        # signature lands after the first poll attempt
        original_sleep = __import__('time').sleep
        __import__('time').sleep = lambda s: (sleeps.append(s), state['signed'].add(FRESH_DIGEST))
        try:
            processor.get_all_latest_images()
        finally:
            __import__('time').sleep = original_sleep

        assert sleeps, 'processor must wait for the signature instead of falling back'
        images = read_output_images(output)
        assert images['RELATED_IMAGE_ODH_OPERATOR_BUNDLE_IMAGE'] == f'quay.io/rhoai/odh-operator-bundle@{FRESH_DIGEST}'

    def test_waits_for_tag_to_move_to_expected_digest(self, tmp_path):
        """
        The nightly tag still points at the stale image when extraction starts
        (Quay API lag / tag move in flight). The processor must poll until the
        expected digest appears on the tag, then use it.
        """
        processor, module, output, _ = setup_snapshot_processor(
            tmp_path, expected_digest=FRESH_DIGEST)
        state = {'tags': [make_tag(STALE_DIGEST, 'Wed, 26 Nov 2026 10:00:00 -0000')]}
        install_fake_quay(module, get_tags=lambda: state['tags'],
                          get_signed_digests=lambda: {STALE_DIGEST, FRESH_DIGEST})

        sleeps = []
        original_sleep = __import__('time').sleep
        __import__('time').sleep = lambda s: (sleeps.append(s), state['tags'].append(
            make_tag(FRESH_DIGEST, 'Wed, 26 Nov 2026 22:43:00 -0000')))
        try:
            processor.get_all_latest_images()
        finally:
            __import__('time').sleep = original_sleep

        assert sleeps, 'processor must wait for the tag to move instead of using the stale image'
        images = read_output_images(output)
        assert images['RELATED_IMAGE_ODH_OPERATOR_BUNDLE_IMAGE'] == f'quay.io/rhoai/odh-operator-bundle@{FRESH_DIGEST}'

    def test_fails_when_expected_digest_never_lands(self, tmp_path, capsys):
        """
        The expected digest never shows up on the tag: the processor must exit
        non-zero WITHOUT writing the output (falling back to the newest signed
        tag would rebuild the 2026-11-27 incident) and say it refused to fall
        back.
        """
        processor, module, output, _ = setup_snapshot_processor(
            tmp_path, expected_digest=FRESH_DIGEST)
        tags = [make_tag(STALE_DIGEST, 'Wed, 26 Nov 2026 10:00:00 -0000')]
        install_fake_quay(module, get_tags=lambda: tags,
                          get_signed_digests=lambda: {STALE_DIGEST})

        with pytest.raises(SystemExit) as excinfo:
            run_get_all_latest_images(processor, sleeps=[])
        assert excinfo.value.code == 1
        assert not os.path.exists(output), 'no output may be written when the expected digest is unresolvable'
        captured = capsys.readouterr()
        assert 'refusing to fall back' in captured.out
        assert FRESH_DIGEST in captured.out

    def test_fails_when_signature_never_appears(self, tmp_path, capsys):
        """
        The expected digest is on the tag but its signature never appears:
        same loud failure as a digest that never lands.
        """
        processor, module, output, _ = setup_snapshot_processor(
            tmp_path, expected_digest=FRESH_DIGEST)
        tags = [make_tag(STALE_DIGEST, 'Wed, 26 Nov 2026 10:00:00 -0000'),
                make_tag(FRESH_DIGEST, 'Tue, 25 Nov 2026 22:43:00 -0000')]
        install_fake_quay(module, get_tags=lambda: tags,
                          get_signed_digests=lambda: {STALE_DIGEST})

        with pytest.raises(SystemExit) as excinfo:
            run_get_all_latest_images(processor, sleeps=[])
        assert excinfo.value.code == 1
        assert not os.path.exists(output)
        captured = capsys.readouterr()
        assert 'not signed yet' in captured.out

    def test_empty_expected_digest_keeps_newest_signed_behavior(self, tmp_path):
        """
        Without the flag (manual re-runs, other callers) the selection is
        unchanged: newest signed tag wins — even when that reproduces the
        stale-image pick. Opt-in only.
        """
        processor, module, output, _ = setup_snapshot_processor(tmp_path, expected_digest='')
        tags = [make_tag(STALE_DIGEST, 'Wed, 26 Nov 2026 10:00:00 -0000'),
                make_tag(FRESH_DIGEST, 'Tue, 25 Nov 2026 22:43:00 -0000')]
        install_fake_quay(module, get_tags=lambda: tags,
                          get_signed_digests=lambda: {STALE_DIGEST, FRESH_DIGEST})

        sleeps = []
        run_get_all_latest_images(processor, sleeps=sleeps)
        assert not sleeps, 'no polling without the flag'

        images = read_output_images(output)
        assert images['RELATED_IMAGE_ODH_OPERATOR_BUNDLE_IMAGE'] == f'quay.io/rhoai/odh-operator-bundle@{STALE_DIGEST}'

    def test_pin_applies_only_to_the_bundle_repo(self, tmp_path):
        """
        The flag names the BUNDLE digest, so it must not pin other repos: with
        a broad image filter the non-bundle repo keeps newest-signed behavior.
        """
        processor, module, output, _ = setup_snapshot_processor(
            tmp_path, expected_digest=FRESH_DIGEST,
            repo_path='rhoai/odh-rhel9-operator', image_filter='odh-rhel9-operator')
        tags = [make_tag(STALE_DIGEST, 'Wed, 26 Nov 2026 10:00:00 -0000'),
                make_tag(FRESH_DIGEST, 'Tue, 25 Nov 2026 22:43:00 -0000')]
        install_fake_quay(module, get_tags=lambda: tags,
                          get_signed_digests=lambda: {STALE_DIGEST, FRESH_DIGEST})

        sleeps = []
        run_get_all_latest_images(processor, sleeps=sleeps)
        assert not sleeps, 'non-bundle repos are not polled for the bundle digest'

        images = read_output_images(output)
        # name derivation strips the -rhel9 suffix: odh-rhel9-operator -> ODH_OPERATOR
        assert images['RELATED_IMAGE_ODH_OPERATOR_IMAGE'] == f'quay.io/rhoai/odh-rhel9-operator@{STALE_DIGEST}'
