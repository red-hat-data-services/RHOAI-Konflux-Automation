"""Tests for optional operator image reference validation."""

import importlib
from unittest.mock import patch

import pytest


MATCHED_REF = 'quay.io/rhoai/odh-rhel9-operator@sha256:abc123'
MISMATCHED_REF = 'quay.io/rhoai/odh-rhel9-operator@sha256:different999'
OPERATOR_ENTRY = [{'name': 'RELATED_IMAGE_ODH_OPERATOR_IMAGE', 'value': MATCHED_REF}]
GIT_METADATA = {'map': {'odh-operator': {'git_url': 'example', 'git_commit': 'abc123'}}}


def _make_processor(operator_image_ref=None, use_existing_digests=False):
    bundle_processor = importlib.import_module('bundle-processor').bundle_processor
    optional_args = {}
    if operator_image_ref is not None:
        optional_args['operator_image_ref'] = operator_image_ref
    if use_existing_digests:
        optional_args['use_existing_digests'] = True

    # Call the real constructor with its original positional arguments. No files
    # are read because the YAML loader is mocked.
    with patch('utils.util.load_yaml_file', return_value={}):
        processor = bundle_processor(
            'build-config.yaml', 'csv.yaml', 'patch.yaml', 'rhoai-3.6',
            'output.yaml', 'annotations.yaml', 'enable', 'push.yaml', 'ci',
            **optional_args,
        )
    processor.patch_dict = {'patch': {'relatedImages': [
        {'name': 'RELATED_IMAGE_ODH_OPERATOR_IMAGE', 'value': 'old-ref'},
    ]}}
    return processor


@patch('utils.util.fetch_latest_images_and_git_metadata', return_value=(OPERATOR_ENTRY, GIT_METADATA))
@patch('utils.util.filter_image_entries', return_value=OPERATOR_ENTRY)
def test_matching_ref_passes(mock_filter, mock_fetch):
    processor = _make_processor(operator_image_ref=MATCHED_REF)

    assert processor.fetch_operator_metadata() == (OPERATOR_ENTRY, GIT_METADATA)
    mock_filter.assert_called_once()
    mock_fetch.assert_called_once()


@patch('utils.util.fetch_latest_images_and_git_metadata', return_value=(OPERATOR_ENTRY, GIT_METADATA))
@patch('utils.util.filter_image_entries', return_value=OPERATOR_ENTRY)
def test_mismatching_ref_exits(mock_filter, mock_fetch):
    processor = _make_processor(operator_image_ref=MISMATCHED_REF)

    with pytest.raises(SystemExit) as exc_info:
        processor.fetch_operator_metadata()

    assert exc_info.value.code == 1
    mock_filter.assert_called_once()
    mock_fetch.assert_called_once()


@patch('utils.util.fetch_latest_images_and_git_metadata', return_value=(OPERATOR_ENTRY, GIT_METADATA))
@patch('utils.util.filter_image_entries', return_value=OPERATOR_ENTRY)
def test_ref_not_provided_preserves_old_behavior(mock_filter, mock_fetch):
    processor = _make_processor()

    assert processor.operator_image_ref is None
    assert processor.fetch_operator_metadata() == (OPERATOR_ENTRY, GIT_METADATA)
    mock_filter.assert_called_once()
    mock_fetch.assert_called_once()


@patch('utils.util.fetch_latest_images_and_git_metadata')
@patch('utils.util.fetch_git_metadata_for_existing_digests', return_value=GIT_METADATA)
@patch('utils.util.filter_image_entries', return_value=OPERATOR_ENTRY)
def test_use_existing_digests_unaffected(mock_filter, mock_fetch_git, mock_fetch_latest):
    processor = _make_processor(
        operator_image_ref=MISMATCHED_REF, use_existing_digests=True,
    )

    assert processor.fetch_operator_metadata() == (OPERATOR_ENTRY, GIT_METADATA)
    mock_filter.assert_called_once()
    mock_fetch_git.assert_called_once_with(OPERATOR_ENTRY)
    mock_fetch_latest.assert_not_called()
