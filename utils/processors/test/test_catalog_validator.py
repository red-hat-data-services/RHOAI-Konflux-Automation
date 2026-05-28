"""
Comprehensive test suite for catalog_validator.py.

Tests are organised by the validation scenario they exercise.
Each test constructs a controlled set of fixture files, runs the validator,
and asserts the expected outcome (pass / sys.exit(1)).

Run:
    cd Refactored-RHOAI-Konflux-Automation/utils/processors
    python -m pytest test/test_catalog_validator.py -v
"""

import logging
import pytest


# ============================================================================
# Shared helpers
# ============================================================================

SHIPPED_2X = [
    'v2.9.0', 'v2.9.1', 'v2.10.0', 'v2.16.0', 'v2.19.0', 'v2.25.0',
]

SHIPPED_3X_GA = ['v3.0.0', 'v3.1.0', 'v3.2.0']

SHIPPED_3X_EA = ['v3.4.0-ea.1', 'v3.4.0-ea.2', 'v3.4.0-ea.3']

ALL_SHIPPED = SHIPPED_2X + SHIPPED_3X_GA + SHIPPED_3X_EA


def bundles_for(version_tags):
    """Convert 'v2.16.0' -> 'rhods-operator.2.16.0' for catalog entries."""
    return [f'rhods-operator.{t.lstrip("v")}' for t in version_tags]


def plain_ocp(version):
    """Minimal global config entry with no special rules."""
    return {'version': version}


# ============================================================================
# 1. ALL BUNDLES PRESENT – validation passes
# ============================================================================

class TestAllBundlesPresent:
    """When every expected bundle is in the catalog, validation should pass."""

    def test_validate_catalogs_all_present(self, make_validator):
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]

        shipped = ['v2.16.0', 'v2.19.0']
        catalog_bundles = bundles_for(shipped)

        catalogs = {v: list(catalog_bundles) for v in ocp_versions}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        validator.validate()

    def test_validate_pcc_all_present(self, make_validator):
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]

        shipped = ['v2.16.0', 'v2.19.0']
        catalog_bundles = bundles_for(shipped)
        catalogs = {v: list(catalog_bundles) for v in ocp_versions}

        validator = make_validator(
            operation='validate-pcc',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        validator.validate()


# ============================================================================
# 2. MISSING BUNDLES – validation fails
# ============================================================================

class TestMissingBundles:
    """When a bundle is missing from a catalog, validation must fail."""

    def test_single_missing_bundle(self, make_validator, caplog):
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.16.0', 'v2.19.0']
        catalogs = {'v4.17': ['rhods-operator.2.16.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "Missing rhods-operator.2.19.0 in OCP v4.17 catalog" in caplog.text

    def test_multiple_missing_across_ocp_versions(self, make_validator, caplog):
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]
        shipped = ['v2.16.0', 'v2.19.0', 'v3.0.0']
        catalogs = {
            'v4.17': ['rhods-operator.2.16.0'],
            'v4.19': ['rhods-operator.2.16.0', 'rhods-operator.2.19.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "OCP v4.17" in caplog.text
        assert "rhods-operator.2.19.0" in caplog.text
        assert "OCP v4.19" in caplog.text
        assert "rhods-operator.3.0.0" in caplog.text

    def test_missing_bundle_pcc_mode(self, make_validator, caplog):
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v2.16.0', 'v2.19.0']
        catalogs = {'v4.19': ['rhods-operator.2.16.0']}

        validator = make_validator(
            operation='validate-pcc',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.2.19.0" in caplog.text
        assert "OCP v4.19" in caplog.text


# ============================================================================
# 3. INCORRECT 3.x BUNDLES ON UNSUPPORTED OCP
# ============================================================================

class TestIncorrect3xBundles:
    """3.x bundles must NOT be present on OCP < v4.19.
    If found, validation must fail."""

    def test_3x_bundle_on_ocp_417(self, make_validator, caplog):
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.16.0', 'v3.0.0']
        catalogs = {
            'v4.17': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.3.0.0" in caplog.text
        assert "OCP v4.17" in caplog.text
        assert "RHOAI 3.x requires OCP >= v4.19" in caplog.text

    def test_multiple_incorrect_3x_across_ocp(self, make_validator, caplog):
        ocp_versions = ['v4.16', 'v4.17']
        global_entries = [plain_ocp('v4.16'), plain_ocp('v4.17')]
        shipped = ['v2.16.0', 'v3.0.0', 'v3.1.0']
        catalogs = {
            'v4.16': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0', 'rhods-operator.3.1.0'],
            'v4.17': ['rhods-operator.2.16.0', 'rhods-operator.3.1.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "OCP v4.16" in caplog.text
        assert "OCP v4.17" in caplog.text

    def test_3x_on_ocp_419_is_allowed(self, make_validator):
        """3.x bundles on OCP >= v4.19 are perfectly fine."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.0.0']
        catalogs = {'v4.19': ['rhods-operator.3.0.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        validator.validate()

    def test_incorrect_3x_detected_in_pcc_mode(self, make_validator, caplog):
        """PCC mode must also detect incorrect 3.x bundles."""
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.16.0', 'v3.0.0']
        catalogs = {
            'v4.17': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0'],
        }

        validator = make_validator(
            operation='validate-pcc',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.3.0.0" in caplog.text
        assert "OCP v4.17" in caplog.text


# ============================================================================
# 4. MISSING BUNDLE EXCEPTIONS (RHOAIENG-8828)
# ============================================================================

class TestMissingBundleExceptions:
    """rhods-operator.2.9.0 and 2.9.1 are known exceptions and must be
    silently skipped without flagging a failure."""

    def test_exception_bundles_not_flagged(self, make_validator, caplog):
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.9.0', 'v2.9.1', 'v2.10.0']
        catalogs = {'v4.17': ['rhods-operator.2.10.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "known exception: RHOAIENG-8828" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_exception_bundles_do_not_mask_real_missing(self, make_validator, caplog):
        """Even with exceptions present, a genuinely missing bundle must fail."""
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.9.0', 'v2.10.0', 'v2.16.0']
        catalogs = {'v4.17': ['rhods-operator.2.10.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.2.16.0" in caplog.text


# ============================================================================
# 5. SKIP-BUNDLES LIST
# ============================================================================

class TestSkipBundles:
    """Bundles in the per-OCP skip-bundles list should be ignored."""

    def test_skip_bundle_not_flagged(self, make_validator, caplog):
        ocp_versions = ['v4.19']
        global_entries = [{
            'version': 'v4.19',
            'skip-bundles': ['rhods-operator.3.4.0-ea.1'],
        }]
        shipped = ['v3.0.0', 'v3.4.0-ea.1']
        catalogs = {'v4.19': ['rhods-operator.3.0.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.INFO):
            validator.validate()

        assert "skip-bundles list" in caplog.text
        assert "rhods-operator.3.4.0-ea.1" in caplog.text

    def test_skip_bundle_specific_to_ocp_version(self, make_validator, caplog):
        """A bundle skipped on v4.19 must still be validated on v4.20."""
        ocp_versions = ['v4.19', 'v4.20']
        global_entries = [
            {'version': 'v4.19', 'skip-bundles': ['rhods-operator.3.0.0']},
            {'version': 'v4.20'},
        ]
        shipped = ['v3.0.0']
        catalogs = {
            'v4.19': [],
            'v4.20': [],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "OCP v4.20" in caplog.text
        assert "rhods-operator.3.0.0" in caplog.text


# ============================================================================
# 6. 3.x EXPECTED ABSENCE ON UNSUPPORTED OCP (missing, but that's fine)
# ============================================================================

class TestExpected3xAbsence:
    """When a 3.x bundle is absent from an OCP < v4.19 catalog, that's
    expected and must NOT be flagged as missing."""

    def test_3x_absent_on_old_ocp_passes(self, make_validator, caplog):
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.16.0', 'v3.0.0']
        catalogs = {'v4.17': ['rhods-operator.2.16.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "3.x not shipped on OCP" in caplog.text
        assert "Validation failed" not in caplog.text


# ============================================================================
# 7. DISCONTINUITY AND ONBOARDING WINDOW
# ============================================================================

class TestDiscontinuityAndOnboarding:
    """Bundles outside the [onboarded-since, discontinued-from) window
    for an OCP version should be ignored when missing."""

    def test_discontinued_bundle_not_flagged(self, make_validator, caplog):
        """Bundle >= discontinued-from should be silently ignored."""
        ocp_versions = ['v4.19']
        global_entries = [{
            'version': 'v4.19',
            'discontinued-from': 'rhods-operator.2.19.0',
        }]
        shipped = ['v2.16.0', 'v2.19.0', 'v2.25.0']
        catalogs = {'v4.19': ['rhods-operator.2.16.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "outside supported range" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_before_onboarding_not_flagged(self, make_validator, caplog):
        """Bundle < onboarded-since should be silently ignored."""
        ocp_versions = ['v4.20']
        global_entries = [{
            'version': 'v4.20',
            'onboarded-since': 'rhods-operator.2.25.0',
        }]
        shipped = ['v2.16.0', 'v2.19.0', 'v2.25.0']
        catalogs = {'v4.20': ['rhods-operator.2.25.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "outside supported range" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_within_window_is_flagged(self, make_validator, caplog):
        """Bundle inside the support window that is missing should still fail."""
        ocp_versions = ['v4.20']
        global_entries = [{
            'version': 'v4.20',
            'onboarded-since': 'rhods-operator.2.25.0',
            'discontinued-from': 'rhods-operator.9.99.99',
        }]
        shipped = ['v2.25.0', 'v3.0.0']
        catalogs = {'v4.20': ['rhods-operator.2.25.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.3.0.0" in caplog.text


# ============================================================================
# 8. SUPERSEDED EA RELEASES
# ============================================================================

class TestSupersededEA:
    """Only the latest EA release needs to be present. Older EAs that have
    been superseded should NOT be flagged as missing."""

    def test_old_ea_superseded_by_newer(self, make_validator, caplog):
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.4.0-ea.1', 'v3.4.0-ea.2', 'v3.4.0-ea.3']
        catalogs = {'v4.19': ['rhods-operator.3.4.0-ea.3']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "superseded by newer EA release" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_latest_ea_missing_is_flagged(self, make_validator, caplog):
        """If the latest EA itself is missing, that IS a failure."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.4.0-ea.1', 'v3.4.0-ea.2', 'v3.4.0-ea.3']
        catalogs = {'v4.19': ['rhods-operator.3.4.0-ea.1']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.3.4.0-ea.3" in caplog.text

    def test_ga_missing_not_treated_as_ea(self, make_validator, caplog):
        """A missing GA bundle must NOT be dismissed by EA superseding logic."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.0.0']
        catalogs = {'v4.19': []}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.3.0.0" in caplog.text

    def test_no_skip_bundles_old_ea_still_passes(self, make_validator, caplog):
        """When config.yaml has NO skip-bundles for EA versions, old EAs
        should still pass via the superseded-EA logic alone."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.0.0', 'v3.4.0-ea.1', 'v3.4.0-ea.2', 'v3.4.0-ea.3']
        catalogs = {
            'v4.19': ['rhods-operator.3.0.0', 'rhods-operator.3.4.0-ea.3'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "superseded by newer EA release" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_no_skip_bundles_multi_ocp_ea_superseded(self, make_validator, caplog):
        """Multiple OCP versions, none with skip-bundles for EA.
        All catalogs contain only the latest EA.  Old EAs are superseded."""
        ocp_versions = ['v4.19', 'v4.20', 'v4.21']
        global_entries = [
            plain_ocp('v4.19'),
            {'version': 'v4.20', 'onboarded-since': 'rhods-operator.2.25.0'},
            {'version': 'v4.21', 'onboarded-since': 'rhods-operator.2.25.0'},
        ]
        shipped = ['v2.25.0', 'v3.0.0', 'v3.4.0-ea.1', 'v3.4.0-ea.2', 'v3.4.0-ea.3']
        latest_bundles = [
            'rhods-operator.2.25.0', 'rhods-operator.3.0.0',
            'rhods-operator.3.4.0-ea.3',
        ]
        catalogs = {v: list(latest_bundles) for v in ocp_versions}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "superseded by newer EA release" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_no_skip_bundles_pcc_mode_ea_superseded(self, make_validator, caplog):
        """PCC mode — no skip-bundles, old EAs superseded by newer EA."""
        ocp_versions = ['v4.19', 'v4.20']
        global_entries = [
            plain_ocp('v4.19'),
            {'version': 'v4.20', 'onboarded-since': 'rhods-operator.2.25.0'},
        ]
        shipped = ['v2.25.0', 'v3.0.0', 'v3.4.0-ea.1', 'v3.4.0-ea.2']
        catalogs = {
            'v4.19': ['rhods-operator.2.25.0', 'rhods-operator.3.0.0', 'rhods-operator.3.4.0-ea.2'],
            'v4.20': ['rhods-operator.2.25.0', 'rhods-operator.3.0.0', 'rhods-operator.3.4.0-ea.2'],
        }

        validator = make_validator(
            operation='validate-pcc',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "superseded by newer EA release" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_no_skip_bundles_mixed_ea_ga_shipped(self, make_validator, caplog):
        """Realistic mix: GA releases + multiple EA series, no skip-bundles.
        Only the latest EA across all series needs to be present.
        All GA must be present."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = [
            'v3.0.0', 'v3.1.0', 'v3.2.0',
            'v3.2.0-ea.1', 'v3.2.0-ea.2',
            'v3.4.0-ea.1', 'v3.4.0-ea.2', 'v3.4.0-ea.3',
        ]
        catalogs = {
            'v4.19': [
                'rhods-operator.3.0.0', 'rhods-operator.3.1.0', 'rhods-operator.3.2.0',
                'rhods-operator.3.4.0-ea.3',
            ],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "superseded by newer EA release" in caplog.text
        assert "Validation failed" not in caplog.text


# ============================================================================
# 9. COMBINED SCENARIOS (missing + incorrect 3.x)
# ============================================================================

class TestCombinedFailures:
    """Both missing bundles AND incorrect 3.x bundles in the same run."""

    def test_both_missing_and_incorrect_3x(self, make_validator, caplog):
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]
        shipped = ['v2.16.0', 'v3.0.0']
        catalogs = {
            'v4.17': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0'],
            'v4.19': [],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "Missing rhods-operator" in caplog.text
        assert "Incorrect 3.x bundle" in caplog.text

    def test_incorrect_3x_alone_causes_exit(self, make_validator, caplog):
        """Incorrect 3.x without any missing bundles must still exit(1).
        This was the critical bug in the old code (incorrect_3x_bundles = True)."""
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.16.0', 'v3.0.0']
        catalogs = {
            'v4.17': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "Validation failed" in caplog.text


# ============================================================================
# 10. _build_catalog_entries TESTS
# ============================================================================

class TestBuildCatalogEntries:
    """Test the entry-building logic for both operation modes."""

    def test_validate_catalogs_path_format(self, make_validator):
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]
        shipped = ['v2.16.0']
        catalogs = {v: ['rhods-operator.2.16.0'] for v in ocp_versions}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        for ocp_ver, cat_path in validator.catalog_entries:
            assert f'{ocp_ver}/rhods-operator/catalog.yaml' in cat_path

    def test_validate_pcc_path_format(self, make_validator):
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]
        shipped = ['v2.16.0']
        catalogs = {v: ['rhods-operator.2.16.0'] for v in ocp_versions}

        validator = make_validator(
            operation='validate-pcc',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        for ocp_ver, cat_path in validator.catalog_entries:
            assert f'catalog-{ocp_ver}.yaml' in cat_path

    def test_unknown_operation_raises(self, tmp_path):
        """An unknown operation should raise ValueError."""
        import os
        from conftest import (
            write_build_config_catalogs, write_global_config,
            write_shipped_versions,
        )
        from validator.catalog_validator import catalog_validator

        build_cfg = str(tmp_path / 'bc.yaml')
        global_cfg = str(tmp_path / 'gc.yaml')
        shipped = str(tmp_path / 'sv.txt')
        cat_folder = str(tmp_path / 'cat')
        os.makedirs(cat_folder, exist_ok=True)

        write_build_config_catalogs(build_cfg, ['v4.17'])
        write_global_config(global_cfg, [{'version': 'v4.17'}])
        write_shipped_versions(shipped, ['v2.16.0'])

        with pytest.raises(ValueError, match="Unknown operation"):
            catalog_validator(
                build_config_path=build_cfg,
                catalog_folder_path=cat_folder,
                shipped_rhoai_versions_path=shipped,
                operation='invalid-op',
                global_config_path=global_cfg,
            )

    def test_ocp_versions_are_sorted(self, make_validator):
        """Catalog entries should be sorted by OCP version regardless of input order."""
        ocp_versions = ['v4.21', 'v4.17', 'v4.19']
        global_entries = [plain_ocp(v) for v in ocp_versions]
        shipped = ['v2.16.0']
        catalogs = {v: ['rhods-operator.2.16.0'] for v in ocp_versions}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        entry_versions = [v for v, _ in validator.catalog_entries]
        assert entry_versions == ['v4.17', 'v4.19', 'v4.21']


# ============================================================================
# 11. CATALOG FILE NOT FOUND
# ============================================================================

class TestCatalogFileNotFound:
    """If a catalog file is missing from disk, the validator should exit(1)
    with a clear error naming the OCP version and path."""

    def test_missing_catalog_file_exits(self, make_validator, caplog):
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.16.0']
        catalogs = {}

        import sys, os
        processors_root = os.path.join(os.path.dirname(__file__), '..')
        if processors_root not in sys.path:
            sys.path.insert(0, os.path.abspath(processors_root))
        from conftest import (
            write_build_config_catalogs, write_global_config,
            write_shipped_versions,
        )

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs={'v4.17': ['rhods-operator.2.16.0']},
        )
        import shutil
        cat_dir = os.path.join(validator.catalog_folder_path, 'v4.17')
        shutil.rmtree(cat_dir)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "Catalog file not found" in caplog.text
        assert "OCP v4.17" in caplog.text


# ============================================================================
# 12. SHIPPED VERSIONS FILTERING
# ============================================================================

class TestShippedVersionsFiltering:
    """The VERSION_REGEX should filter out build-number tags, source tags,
    and bare minor tags, keeping only clean version strings."""

    def test_filters_build_and_source_tags(self, make_validator):
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = [
            'v2.16.0',
            'v2.16.0-1733155920',
            'v2.16.0-source',
            'v2.16',
            'v3.0.0',
            'v3.4.0-ea.1',
        ]
        catalogs = {
            'v4.19': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0', 'rhods-operator.3.4.0-ea.1'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )

        assert '2.16.0' in validator.shipped_rhoai_versions
        assert '3.0.0' in validator.shipped_rhoai_versions
        assert '3.4.0-ea.1' in validator.shipped_rhoai_versions
        assert '2.16.0-1733155920' not in validator.shipped_rhoai_versions
        assert '2.16.0-source' not in validator.shipped_rhoai_versions
        assert '2.16' not in validator.shipped_rhoai_versions

        validator.validate()


# ============================================================================
# 13. PER-OCP SUMMARY LOG (refactored code only)
# ============================================================================

class TestPerOcpSummary:
    """After validating each OCP version, a summary line should be logged."""

    def test_summary_line_present(self, make_validator, caplog):
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]
        shipped = ['v2.16.0']
        catalogs = {v: ['rhods-operator.2.16.0'] for v in ocp_versions}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.INFO):
            validator.validate()

        assert "OCP v4.17: 0 missing, 0 incorrect 3.x, 0 unreleased" in caplog.text
        assert "OCP v4.19: 0 missing, 0 incorrect 3.x, 0 unreleased" in caplog.text


# ============================================================================
# 14. LOG QUALITY CHECKS (refactored code only)
# ============================================================================

class TestLogQuality:
    """Verify that the right log levels and messages are emitted.
    These tests only apply to the refactored code which uses the logging module."""

    def test_error_on_incorrect_3x_detection(self, make_validator, caplog):
        """ERROR should be emitted when an incorrect 3.x bundle is detected."""
        ocp_versions = ['v4.17']
        global_entries = [plain_ocp('v4.17')]
        shipped = ['v2.16.0', 'v3.0.0']
        catalogs = {'v4.17': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit):
                validator.validate()

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any("Incorrect 3.x bundle" in r.message for r in error_records)

    def test_debug_log_for_found_bundles(self, make_validator, caplog):
        """DEBUG should log each bundle that is found successfully."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v2.16.0']
        catalogs = {'v4.19': ['rhods-operator.2.16.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "Found rhods-operator.2.16.0 in OCP v4.19 catalog (valid)" in caplog.text

    def test_per_bundle_error_output_format(self, make_validator, caplog):
        """Verify that each missing bundle gets its own error log line."""
        ocp_versions = ['v4.17', 'v4.19']
        global_entries = [plain_ocp('v4.17'), plain_ocp('v4.19')]
        shipped = ['v2.16.0', 'v2.19.0']
        catalogs = {
            'v4.17': ['rhods-operator.2.16.0'],
            'v4.19': ['rhods-operator.2.16.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit):
                validator.validate()

        assert "Missing rhods-operator.2.19.0 in OCP v4.17 catalog" in caplog.text
        assert "Missing rhods-operator.2.19.0 in OCP v4.19 catalog" in caplog.text


# ============================================================================
# 15. UNRELEASED BUNDLES DETECTION
# ============================================================================

class TestUnreleasedBundles:
    """Bundles present in the catalog but NOT in shipped_rhoai_versions
    should be flagged as unreleased with a WARNING."""

    def test_extra_bundle_logged_as_unreleased(self, make_validator, caplog):
        """A bundle in the catalog that isn't in shipped versions should trigger a warning."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v2.16.0']
        catalogs = {
            'v4.19': ['rhods-operator.2.16.0', 'rhods-operator.2.99.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.WARNING):
            validator.validate()

        assert "unreleased bundle" in caplog.text
        assert "rhods-operator.2.99.0" in caplog.text

    def test_multiple_unreleased_across_ocp(self, make_validator, caplog):
        """Multiple unreleased bundles across different OCP versions."""
        ocp_versions = ['v4.19', 'v4.20']
        global_entries = [plain_ocp('v4.19'), plain_ocp('v4.20')]
        shipped = ['v3.0.0']
        catalogs = {
            'v4.19': ['rhods-operator.3.0.0', 'rhods-operator.3.99.0'],
            'v4.20': ['rhods-operator.3.0.0', 'rhods-operator.3.88.0', 'rhods-operator.3.77.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.WARNING):
            validator.validate()

        assert "OCP v4.19" in caplog.text
        assert "rhods-operator.3.99.0" in caplog.text
        assert "OCP v4.20" in caplog.text
        assert "rhods-operator.3.88.0" in caplog.text
        assert "rhods-operator.3.77.0" in caplog.text

    def test_no_unreleased_when_all_shipped(self, make_validator, caplog):
        """When all catalog bundles are in the shipped list, no warning should appear."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v2.16.0', 'v3.0.0']
        catalogs = {
            'v4.19': ['rhods-operator.2.16.0', 'rhods-operator.3.0.0'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.WARNING):
            validator.validate()

        assert "unreleased bundle(s) found in catalog" not in caplog.text

    def test_unreleased_does_not_fail_validation(self, make_validator):
        """Unreleased bundles are informational — they should NOT cause sys.exit(1)."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v2.16.0']
        catalogs = {
            'v4.19': ['rhods-operator.2.16.0', 'rhods-operator.9.9.9'],
        }

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        validator.validate()

    def test_unreleased_in_pcc_mode(self, make_validator, caplog):
        """Unreleased bundle detection should also work in PCC mode."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.0.0']
        catalogs = {
            'v4.19': ['rhods-operator.3.0.0', 'rhods-operator.3.50.0'],
        }

        validator = make_validator(
            operation='validate-pcc',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.WARNING):
            validator.validate()

        assert "unreleased bundle" in caplog.text
        assert "rhods-operator.3.50.0" in caplog.text


# ============================================================================
# 16. EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_shipped_versions(self, make_validator):
        """If there are no shipped versions, validation should pass (nothing to check)."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = []
        catalogs = {'v4.19': []}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        validator.validate()

    def test_ea_hotfix_version(self, make_validator):
        """EA hotfix versions like 3.4.0-ea.1.1 should parse and validate."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.4.0-ea.1.1']
        catalogs = {'v4.19': ['rhods-operator.3.4.0-ea.1.1']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        validator.validate()

    def test_bundle_exactly_at_discontinuity_boundary(self, make_validator, caplog):
        """A bundle exactly equal to discontinued-from should be ignored (>= check)."""
        ocp_versions = ['v4.17']
        global_entries = [{
            'version': 'v4.17',
            'discontinued-from': 'rhods-operator.2.19.0',
        }]
        shipped = ['v2.16.0', 'v2.19.0']
        catalogs = {'v4.17': ['rhods-operator.2.16.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.DEBUG):
            validator.validate()

        assert "outside supported range" in caplog.text
        assert "Validation failed" not in caplog.text

    def test_bundle_exactly_at_onboarding_boundary(self, make_validator, caplog):
        """A bundle exactly equal to onboarded-since IS within the window and must be present."""
        ocp_versions = ['v4.20']
        global_entries = [{
            'version': 'v4.20',
            'onboarded-since': 'rhods-operator.2.25.0',
        }]
        shipped = ['v2.25.0']
        catalogs = {'v4.20': []}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.2.25.0" in caplog.text

    def test_ocp_version_not_in_global_config(self, make_validator, caplog):
        """OCP version in build-config but not in global config should use
        safe defaults (no discontinuity, no onboarding cutoff) via .get()."""
        ocp_versions = ['v4.22']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v3.0.0']
        catalogs = {'v4.22': []}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit) as exc_info:
                validator.validate()

        assert exc_info.value.code == 1
        assert "rhods-operator.3.0.0" in caplog.text

    def test_deduplication_of_shipped_versions(self, make_validator):
        """Duplicate version tags in the shipped file should be deduplicated."""
        ocp_versions = ['v4.19']
        global_entries = [plain_ocp('v4.19')]
        shipped = ['v2.16.0', 'v2.16.0', 'v2.16.0']
        catalogs = {'v4.19': ['rhods-operator.2.16.0']}

        validator = make_validator(
            operation='validate-catalogs',
            ocp_versions=ocp_versions,
            global_ocp_entries=global_entries,
            shipped_version_tags=shipped,
            catalogs=catalogs,
        )
        assert validator.shipped_rhoai_versions.count('2.16.0') == 1
        validator.validate()
