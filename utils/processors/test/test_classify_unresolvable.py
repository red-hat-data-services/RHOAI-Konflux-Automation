import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.util import classify_unresolvable_image, parse_image_value

GOOD = 'sha256:' + 'a' * 64
OTHER = 'sha256:' + 'b' * 64


def _parsed(digest, repo='odh-vllm-cpu-rhel9', org='rhoai'):
    return parse_image_value(f'quay.io/{org}/{repo}@{digest}')


@pytest.mark.parametrize(
    ('repo', 'expected_component'),
    [
        ('odh-vllm-cpu-rhel8', 'odh-vllm-cpu'),
        ('odh-vllm-cpu-rhel9', 'odh-vllm-cpu'),
        ('odh-vllm-cpu-rhel10', 'odh-vllm-cpu'),
        ('odh-vllm-cpu-rhel11', 'odh-vllm-cpu'),
        ('odh-vllm-cpu', 'odh-vllm-cpu'),
        ('odh-rhel9-vllm-cpu', 'odh-rhel9-vllm-cpu'),
    ],
)
def test_parse_image_value_component_name(repo, expected_component):
    parsed = parse_image_value(f'quay.io/rhoai/{repo}@{GOOD}')
    assert parsed['component_name'] == expected_component


class TestMalformedDigest:
    def test_missing_sha256_prefix(self):
        qc = MagicMock()
        msg = classify_unresolvable_image(qc, _parsed('a' * 64), 'rhoai-3.4')
        assert 'malformed digest' in msg
        qc.get_tag_details.assert_not_called()  # no API call needed

    def test_double_sha256_prefix(self):
        qc = MagicMock()
        msg = classify_unresolvable_image(qc, _parsed('sha256:sha256:' + 'c' * 64), 'rhoai-3.4')
        assert 'malformed digest' in msg
        qc.get_tag_details.assert_not_called()


class TestWellFormedDigest:
    def test_no_version_generic_message(self):
        qc = MagicMock()
        msg = classify_unresolvable_image(qc, _parsed(GOOD), rhoai_version=None)
        assert 'cross-tenant' in msg
        qc.get_tag_details.assert_not_called()

    def test_no_tag_is_generic_cross_tenant(self):
        qc = MagicMock()
        qc.get_tag_details.return_value = {}
        msg = classify_unresolvable_image(qc, _parsed(GOOD), 'rhoai-3.4')
        assert 'cross-tenant' in msg

    def test_same_digest_is_cross_tenant(self):
        qc = MagicMock()
        qc.get_tag_details.return_value = {'manifest_digest': GOOD}
        msg = classify_unresolvable_image(qc, _parsed(GOOD), 'rhoai-3.4')
        assert 'cross-tenant' in msg

    def test_different_digest_is_stale(self):
        qc = MagicMock()
        qc.get_tag_details.return_value = {'manifest_digest': OTHER}
        msg = classify_unresolvable_image(qc, _parsed(GOOD), 'rhoai-3.4')
        assert 'stale digest' in msg
        assert OTHER in msg

    def test_tag_lookup_exception_never_raises(self):
        # Regression: a crash in get_tag_details (e.g. KeyError on an error
        # response) must not propagate — the freeze has to continue.
        qc = MagicMock()
        qc.get_tag_details.side_effect = KeyError('tags')
        msg = classify_unresolvable_image(qc, _parsed(GOOD), 'rhoai-3.4')
        assert 'cross-tenant' in msg
