"""
Exhaustive test suite for version_util.py.

Covers OcpVersion, RhoaiVersion, _match_comparator(), and satisfies_range().
This is the foundation for all version-aware logic in the codebase -- a bug
here silently breaks catalog validation, so coverage must be thorough.

Run:
    cd Refactored-RHOAI-Konflux-Automation/utils/processors
    python -m pytest test/test_version_util.py -v
"""

import os
import sys
import pytest

processors_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if processors_root not in sys.path:
    sys.path.insert(0, processors_root)

from utils.version_util import (
    OcpVersion,
    RhoaiVersion,
    VERSION_REGEX,
    _match_comparator,
    satisfies_range,
)


# ============================================================================
# Helper
# ============================================================================

def v(version_str: str) -> RhoaiVersion:
    """Shorthand for constructing a RhoaiVersion from a bare version string."""
    return RhoaiVersion(version_str)


# ============================================================================
# 1. TestOcpVersion
# ============================================================================

class TestOcpVersion:
    """Parsing, comparison, and error handling for OcpVersion."""

    # -- Parsing valid inputs ------------------------------------------------

    def test_parse_v_prefix(self):
        ocp = OcpVersion('v4.19')
        assert ocp._tuple == (4, 19)

    def test_parse_no_prefix(self):
        ocp = OcpVersion('4.19')
        assert ocp._tuple == (4, 19)

    def test_parse_tuple_input(self):
        ocp = OcpVersion((4, 19))
        assert ocp._tuple == (4, 19)

    @pytest.mark.parametrize(
        ('version', 'expected'),
        [
            ('v5.0', (5, 0)),
            ('5.1', (5, 1)),
            ('v5.10', (5, 10)),
            ('v10.2', (10, 2)),
        ],
    )
    def test_parse_future_ocp_versions(self, version, expected):
        ocp = OcpVersion(version)
        assert ocp._tuple == expected

    # -- Comparison ordering -------------------------------------------------

    def test_less_than(self):
        assert OcpVersion('v4.17') < OcpVersion('v4.19')

    def test_greater_than(self):
        assert OcpVersion('v4.21') > OcpVersion('v4.19')

    def test_equal(self):
        assert OcpVersion('v4.19') == OcpVersion('4.19')

    def test_less_than_or_equal(self):
        assert OcpVersion('v4.17') <= OcpVersion('v4.19')
        assert OcpVersion('v4.19') <= OcpVersion('v4.19')

    def test_greater_than_or_equal(self):
        assert OcpVersion('v4.21') >= OcpVersion('v4.19')
        assert OcpVersion('v4.19') >= OcpVersion('v4.19')

    def test_sorting(self):
        versions = [OcpVersion('v5.10'), OcpVersion('v4.22'), OcpVersion('v5.0')]
        sorted_versions = sorted(versions)
        assert [v._tuple for v in sorted_versions] == [(4, 22), (5, 0), (5, 10)]

    # -- Repr ----------------------------------------------------------------

    def test_repr(self):
        assert repr(OcpVersion('v4.19')) == 'v4.19'
        assert repr(OcpVersion('4.17')) == 'v4.17'

    # -- Hash ----------------------------------------------------------------

    def test_hash_equal_versions(self):
        assert hash(OcpVersion('v4.19')) == hash(OcpVersion('4.19'))

    def test_usable_as_dict_key(self):
        d = {OcpVersion('v4.19'): 'value'}
        assert d[OcpVersion('4.19')] == 'value'

    # -- Invalid input -------------------------------------------------------

    def test_garbage_string_raises(self):
        with pytest.raises(ValueError, match="Cannot parse OCP version"):
            OcpVersion('not-a-version')

    def test_wrong_type_raises(self):
        with pytest.raises(TypeError, match="OcpVersion expects str or tuple"):
            OcpVersion(419)

    def test_wrong_type_list_raises(self):
        with pytest.raises(TypeError):
            OcpVersion([4, 19])

    @pytest.mark.parametrize(
        'version',
        ['v5', 'v5.0.1', 'v5.0garbage', 'v5.01', 'v5.-1', ''],
    )
    def test_malformed_ocp_version_raises(self, version):
        with pytest.raises(ValueError, match="Cannot parse OCP version"):
            OcpVersion(version)

    @pytest.mark.parametrize('version', [(5,), (5, 0, 1), (5, -1), ('5', 0), (True, 0)])
    def test_invalid_tuple_raises(self, version):
        with pytest.raises(ValueError, match="exactly two non-negative integers"):
            OcpVersion(version)


# ============================================================================
# 2. TestRhoaiVersion
# ============================================================================

class TestRhoaiVersion:
    """Parsing, comparison, helpers, and error handling for RhoaiVersion."""

    # -- Parsing valid inputs ------------------------------------------------

    def test_parse_bare_ga(self):
        rv = RhoaiVersion('3.5.0')
        assert rv.is_ga()
        assert not rv.is_ea()

    def test_parse_rhods_operator_prefix(self):
        rv = RhoaiVersion('rhods-operator.3.5.0')
        assert rv.is_ga()
        assert rv == RhoaiVersion('3.5.0')

    def test_parse_v_prefix(self):
        rv = RhoaiVersion('v3.5.0')
        assert rv.is_ga()
        assert rv == RhoaiVersion('3.5.0')

    def test_parse_ea(self):
        rv = RhoaiVersion('3.4.0-ea.1')
        assert rv.is_ea()
        assert not rv.is_ga()

    def test_parse_ea_with_prefix(self):
        rv = RhoaiVersion('rhods-operator.3.4.0-ea.1')
        assert rv.is_ea()
        assert rv == RhoaiVersion('3.4.0-ea.1')

    def test_parse_ea_hotfix(self):
        rv = RhoaiVersion('3.4.0-ea.1.1')
        assert rv.is_ea()

    def test_parse_ea_hotfix_with_prefix(self):
        rv = RhoaiVersion('rhods-operator.3.4.0-ea.1.1')
        assert rv == RhoaiVersion('3.4.0-ea.1.1')

    # -- Parse failures (should raise ValueError) ----------------------------

    def test_build_number_tag_rejected(self):
        with pytest.raises(ValueError, match="Cannot parse operator version"):
            RhoaiVersion('v2.16.0-1733155920')

    def test_source_tag_rejected(self):
        with pytest.raises(ValueError, match="Cannot parse operator version"):
            RhoaiVersion('v2.16.0-source')

    def test_bare_minor_rejected(self):
        with pytest.raises(ValueError, match="Cannot parse operator version"):
            RhoaiVersion('v2.16')

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError):
            RhoaiVersion('')

    def test_garbage_rejected(self):
        with pytest.raises(ValueError):
            RhoaiVersion('not-a-version')

    # -- Comparison ordering -------------------------------------------------

    def test_cross_major(self):
        assert v('2.25.0') < v('3.0.0')

    def test_cross_minor(self):
        assert v('3.4.0') < v('3.5.0')

    def test_cross_patch(self):
        assert v('3.5.0') < v('3.5.1')

    def test_ga_greater_than_ea_same_version(self):
        assert v('3.4.0') > v('3.4.0-ea.1')
        assert v('3.4.0') > v('3.4.0-ea.99')

    def test_ea_sequence_ordering(self):
        assert v('3.4.0-ea.1') < v('3.4.0-ea.2')
        assert v('3.4.0-ea.2') < v('3.4.0-ea.3')

    def test_ea_hotfix_ordering(self):
        assert v('3.4.0-ea.1') < v('3.4.0-ea.1.1')

    def test_equality(self):
        assert v('3.5.0') == v('3.5.0')
        assert v('3.4.0-ea.1') == v('3.4.0-ea.1')

    def test_inequality(self):
        assert v('3.5.0') != v('3.4.0')
        assert v('3.4.0-ea.1') != v('3.4.0-ea.2')
        assert v('3.4.0') != v('3.4.0-ea.1')

    # -- Hash ----------------------------------------------------------------

    def test_hash_equal_versions(self):
        assert hash(v('3.5.0')) == hash(v('rhods-operator.3.5.0'))

    def test_usable_as_set_member(self):
        s = {v('3.5.0'), v('rhods-operator.3.5.0'), v('v3.5.0')}
        assert len(s) == 1

    # -- Repr ----------------------------------------------------------------

    def test_repr_preserves_input(self):
        assert repr(RhoaiVersion('3.5.0')) == '3.5.0'
        assert repr(RhoaiVersion('rhods-operator.3.4.0-ea.1')) == 'rhods-operator.3.4.0-ea.1'

    # -- is_latest_ea() ------------------------------------------------------

    def test_is_latest_ea_true(self):
        bundles = ['rhods-operator.3.4.0-ea.1', 'rhods-operator.3.4.0-ea.2', 'rhods-operator.3.4.0-ea.3']
        assert v('3.4.0-ea.3').is_latest_ea(bundles) is True

    def test_is_latest_ea_false(self):
        bundles = ['rhods-operator.3.4.0-ea.1', 'rhods-operator.3.4.0-ea.2', 'rhods-operator.3.4.0-ea.3']
        assert v('3.4.0-ea.1').is_latest_ea(bundles) is False

    def test_is_latest_ea_across_series(self):
        bundles = [
            'rhods-operator.3.4.0-ea.1', 'rhods-operator.3.4.0-ea.2',
            'rhods-operator.3.5.0-ea.1', 'rhods-operator.3.5.0-ea.2',
        ]
        assert v('3.5.0-ea.2').is_latest_ea(bundles) is True
        assert v('3.4.0-ea.2').is_latest_ea(bundles) is False

    def test_is_latest_ea_ignores_ga_in_list(self):
        bundles = ['rhods-operator.3.4.0-ea.1', 'rhods-operator.3.4.0', 'rhods-operator.3.5.0']
        assert v('3.4.0-ea.1').is_latest_ea(bundles) is True

    def test_is_latest_ea_single_ea(self):
        bundles = ['rhods-operator.3.4.0-ea.1']
        assert v('3.4.0-ea.1').is_latest_ea(bundles) is True

    def test_is_latest_ea_raises_on_ga(self):
        with pytest.raises(ValueError, match="must only be called on EA versions"):
            v('3.5.0').is_latest_ea(['rhods-operator.3.5.0'])

    # -- VERSION_REGEX -------------------------------------------------------

    def test_regex_matches_bare(self):
        assert VERSION_REGEX.match('3.5.0') is not None

    def test_regex_matches_v_prefix(self):
        assert VERSION_REGEX.match('v3.5.0') is not None

    def test_regex_matches_rhods_prefix(self):
        assert VERSION_REGEX.match('rhods-operator.3.5.0') is not None

    def test_regex_matches_ea(self):
        m = VERSION_REGEX.match('v3.4.0-ea.1')
        assert m is not None
        assert m.group(5) == '1'

    def test_regex_matches_ea_hotfix(self):
        m = VERSION_REGEX.match('3.4.0-ea.1.1')
        assert m is not None
        assert m.group(5) == '1'
        assert m.group(6) == '1'

    def test_regex_rejects_build_number(self):
        assert VERSION_REGEX.match('v2.16.0-1733155920') is None

    def test_regex_rejects_source_tag(self):
        assert VERSION_REGEX.match('v2.16.0-source') is None

    def test_regex_rejects_bare_minor(self):
        assert VERSION_REGEX.match('v2.16') is None


# ============================================================================
# 3. TestMatchComparator
# ============================================================================

class TestMatchComparator:
    """Each of the 6 operators tested at below/at/above boundary."""

    # -- >= ------------------------------------------------------------------

    def test_gte_below(self):
        assert _match_comparator(v('3.4.0'), '>=3.5.0') is False

    def test_gte_at(self):
        assert _match_comparator(v('3.5.0'), '>=3.5.0') is True

    def test_gte_above(self):
        assert _match_comparator(v('3.6.0'), '>=3.5.0') is True

    # -- > -------------------------------------------------------------------

    def test_gt_below(self):
        assert _match_comparator(v('3.4.0'), '>3.5.0') is False

    def test_gt_at(self):
        assert _match_comparator(v('3.5.0'), '>3.5.0') is False

    def test_gt_above(self):
        assert _match_comparator(v('3.5.1'), '>3.5.0') is True

    # -- <= ------------------------------------------------------------------

    def test_lte_below(self):
        assert _match_comparator(v('3.4.0'), '<=3.5.0') is True

    def test_lte_at(self):
        assert _match_comparator(v('3.5.0'), '<=3.5.0') is True

    def test_lte_above(self):
        assert _match_comparator(v('3.6.0'), '<=3.5.0') is False

    # -- < -------------------------------------------------------------------

    def test_lt_below(self):
        assert _match_comparator(v('3.4.0'), '<3.5.0') is True

    def test_lt_at(self):
        assert _match_comparator(v('3.5.0'), '<3.5.0') is False

    def test_lt_above(self):
        assert _match_comparator(v('3.6.0'), '<3.5.0') is False

    # -- = -------------------------------------------------------------------

    def test_eq_match(self):
        assert _match_comparator(v('3.5.0'), '=3.5.0') is True

    def test_eq_no_match_below(self):
        assert _match_comparator(v('3.4.0'), '=3.5.0') is False

    def test_eq_no_match_above(self):
        assert _match_comparator(v('3.6.0'), '=3.5.0') is False

    # -- != ------------------------------------------------------------------

    def test_neq_different(self):
        assert _match_comparator(v('3.4.0'), '!=3.5.0') is True

    def test_neq_same(self):
        assert _match_comparator(v('3.5.0'), '!=3.5.0') is False

    # -- Bare version (implicit =) ------------------------------------------

    def test_bare_version_match(self):
        assert _match_comparator(v('3.5.0'), '3.5.0') is True

    def test_bare_version_no_match(self):
        assert _match_comparator(v('3.4.0'), '3.5.0') is False

    # -- Operators with EA versions ------------------------------------------

    def test_gte_ea_version_in_token(self):
        assert _match_comparator(v('3.4.0-ea.2'), '>=3.4.0-ea.1') is True

    def test_lt_ea_version_in_token(self):
        assert _match_comparator(v('3.4.0-ea.1'), '<3.4.0-ea.2') is True

    def test_eq_ea_version(self):
        assert _match_comparator(v('3.4.0-ea.1'), '=3.4.0-ea.1') is True
        assert _match_comparator(v('3.4.0-ea.2'), '=3.4.0-ea.1') is False


# ============================================================================
# 4. TestEaGaOrdering
# ============================================================================

class TestEaGaOrdering:
    """EA/GA interaction in comparators -- the subtlest area because
    RhoaiVersion sorts EA below GA at the same major.minor.patch."""

    def test_gte_ga_excludes_ea(self):
        """>=3.4.0 means GA 3.4.0 and above; EA 3.4.0-ea.1 is below GA."""
        assert _match_comparator(v('3.4.0-ea.1'), '>=3.4.0') is False

    def test_gte_ga_includes_ga(self):
        assert _match_comparator(v('3.4.0'), '>=3.4.0') is True

    def test_lt_ga_includes_ea(self):
        """<3.4.0 includes EA versions because EA sorts below GA."""
        assert _match_comparator(v('3.4.0-ea.1'), '<3.4.0') is True

    def test_lt_ga_excludes_ga(self):
        assert _match_comparator(v('3.4.0'), '<3.4.0') is False

    def test_ea_range_includes_all_ea_excludes_ga(self):
        """>=3.4.0-ea.1 <3.4.0 should match all EAs of 3.4.0 but not GA."""
        range_str = '>=3.4.0-ea.1 <3.4.0'
        assert satisfies_range(v('3.4.0-ea.1'), range_str) is True
        assert satisfies_range(v('3.4.0-ea.2'), range_str) is True
        assert satisfies_range(v('3.4.0-ea.99'), range_str) is True
        assert satisfies_range(v('3.4.0'), range_str) is False

    def test_neq_ea_still_matches_other_ea(self):
        assert _match_comparator(v('3.4.0-ea.2'), '!=3.4.0-ea.1') is True

    def test_neq_ea_still_matches_ga(self):
        assert _match_comparator(v('3.4.0'), '!=3.4.0-ea.1') is True

    def test_gte_ea_includes_later_ga(self):
        """>=3.6.0-ea.2 should include 3.6.0 GA (GA > EA at same version)."""
        assert _match_comparator(v('3.6.0-ea.1'), '>=3.6.0-ea.2') is False
        assert _match_comparator(v('3.6.0-ea.2'), '>=3.6.0-ea.2') is True
        assert _match_comparator(v('3.6.0'), '>=3.6.0-ea.2') is True

    def test_ea_hotfix_ordering_in_comparator(self):
        assert _match_comparator(v('3.4.0-ea.1.1'), '>=3.4.0-ea.1') is True
        assert _match_comparator(v('3.4.0-ea.1'), '<3.4.0-ea.1.1') is True


# ============================================================================
# 5. TestSatisfiesRangeAndClauses
# ============================================================================

class TestSatisfiesRangeAndClauses:
    """Space-separated AND logic within a single clause."""

    def test_single_token(self):
        assert satisfies_range(v('3.0.0'), '>=3.0.0') is True
        assert satisfies_range(v('2.9.0'), '>=3.0.0') is False

    def test_two_token_range_inside(self):
        assert satisfies_range(v('2.25.0'), '>=2.25.0 <2.26.0') is True

    def test_two_token_range_middle(self):
        assert satisfies_range(v('2.25.5'), '>=2.25.0 <2.26.0') is True

    def test_two_token_range_at_upper_boundary(self):
        assert satisfies_range(v('2.26.0'), '>=2.25.0 <2.26.0') is False

    def test_two_token_range_below(self):
        assert satisfies_range(v('2.24.0'), '>=2.25.0 <2.26.0') is False

    def test_impossible_range(self):
        """>=3.0.0 <2.0.0 can never be true."""
        assert satisfies_range(v('2.5.0'), '>=3.0.0 <2.0.0') is False
        assert satisfies_range(v('3.5.0'), '>=3.0.0 <2.0.0') is False

    def test_redundant_conditions(self):
        """>=2.0.0 >=3.0.0 effectively means >=3.0.0."""
        assert satisfies_range(v('2.5.0'), '>=2.0.0 >=3.0.0') is False
        assert satisfies_range(v('3.0.0'), '>=2.0.0 >=3.0.0') is True
        assert satisfies_range(v('4.0.0'), '>=2.0.0 >=3.0.0') is True


# ============================================================================
# 6. TestSatisfiesRangeOrClauses
# ============================================================================

class TestSatisfiesRangeOrClauses:
    """'||'-separated OR logic between clauses."""

    def test_v422_gap_scenario(self):
        """The core motivating example from the design doc."""
        range_str = '>=2.25.0 <2.26.0 || >=3.5.0'
        assert satisfies_range(v('2.25.0'), range_str) is True
        assert satisfies_range(v('3.2.1'), range_str) is False
        assert satisfies_range(v('3.4.1'), range_str) is False
        assert satisfies_range(v('3.5.0'), range_str) is True
        assert satisfies_range(v('2.24.0'), range_str) is False

    def test_three_clauses(self):
        range_str = '>=1.0.0 <2.0.0 || >=3.0.0 <4.0.0 || >=5.0.0'
        assert satisfies_range(v('1.5.0'), range_str) is True
        assert satisfies_range(v('2.5.0'), range_str) is False
        assert satisfies_range(v('3.5.0'), range_str) is True
        assert satisfies_range(v('4.5.0'), range_str) is False
        assert satisfies_range(v('5.0.0'), range_str) is True

    def test_first_clause_fails_second_matches(self):
        range_str = '>=9.0.0 || >=2.0.0 <3.0.0'
        assert satisfies_range(v('2.5.0'), range_str) is True

    def test_all_clauses_fail(self):
        range_str = '>=9.0.0 || >=8.0.0 <8.1.0'
        assert satisfies_range(v('2.5.0'), range_str) is False

    def test_empty_clause_between_delimiters(self):
        """'>=1.0.0 ||  || >=3.0.0' -- empty middle clause is skipped."""
        range_str = '>=1.0.0 <2.0.0 ||  || >=3.0.0'
        assert satisfies_range(v('1.5.0'), range_str) is True
        assert satisfies_range(v('2.5.0'), range_str) is False
        assert satisfies_range(v('3.0.0'), range_str) is True

    def test_single_clause_no_or(self):
        assert satisfies_range(v('3.5.0'), '>=3.5.0') is True
        assert satisfies_range(v('3.4.0'), '>=3.5.0') is False


# ============================================================================
# 7. TestSatisfiesRangeRealWorldScenarios
# ============================================================================

class TestSatisfiesRangeRealWorldScenarios:
    """End-to-end tests using exact scenarios from the design doc."""

    def test_v422_onboarded_range(self):
        """v4.22 onboarded-range from the design doc."""
        range_str = '>=2.25.0 <2.26.0 || >=3.5.0'
        assert satisfies_range(v('2.25.0'), range_str) is True
        assert satisfies_range(v('2.25.1'), range_str) is True
        assert satisfies_range(v('3.2.1'), range_str) is False
        assert satisfies_range(v('3.4.1'), range_str) is False
        assert satisfies_range(v('3.4.0-ea.1'), range_str) is False
        assert satisfies_range(v('3.5.0'), range_str) is True
        assert satisfies_range(v('3.6.0-ea.2'), range_str) is True

    def test_v416_discontinued_range(self):
        """v4.16 discontinued-range: everything from 3.0.0 onward is offboarded."""
        range_str = '>=3.0.0'
        assert satisfies_range(v('2.25.0'), range_str) is False
        assert satisfies_range(v('3.0.0'), range_str) is True
        assert satisfies_range(v('3.5.0'), range_str) is True

    def test_multi_clause_discontinuation(self):
        """Multi-clause discontinued-range with a gap."""
        range_str = '>=2.26.0 <3.0.0 || >=3.2.0'
        assert satisfies_range(v('2.25.0'), range_str) is False
        assert satisfies_range(v('2.26.0'), range_str) is True
        assert satisfies_range(v('2.27.0'), range_str) is True
        assert satisfies_range(v('3.0.0'), range_str) is False
        assert satisfies_range(v('3.1.0'), range_str) is False
        assert satisfies_range(v('3.2.0'), range_str) is True
        assert satisfies_range(v('3.5.0'), range_str) is True

    def test_default_onboarded_range_matches_everything(self):
        """>=0.0.0 matches every version (the default when no range is specified)."""
        range_str = '>=0.0.0'
        assert satisfies_range(v('0.0.0'), range_str) is True
        assert satisfies_range(v('2.25.0'), range_str) is True
        assert satisfies_range(v('9.99.99'), range_str) is True

    def test_empty_discontinued_range_matches_nothing(self):
        """Empty string means no versions are offboarded (the default)."""
        assert satisfies_range(v('0.0.0'), '') is False
        assert satisfies_range(v('9.99.99'), '') is False


# ============================================================================
# 8. TestSatisfiesRangeErrorHandling
# ============================================================================

class TestSatisfiesRangeErrorHandling:
    """Invalid inputs should fail loudly or return False as appropriate."""

    def test_garbage_version_in_range(self):
        with pytest.raises(ValueError):
            satisfies_range(v('3.0.0'), 'not-a-version')

    def test_missing_version_after_operator(self):
        with pytest.raises(ValueError):
            satisfies_range(v('3.0.0'), '>=')

    def test_tilde_operator_not_supported(self):
        """~= is not in our supported grammar."""
        with pytest.raises(ValueError):
            satisfies_range(v('3.0.0'), '~=3.0.0')

    def test_caret_operator_not_supported(self):
        """^ is not in our supported grammar."""
        with pytest.raises(ValueError):
            satisfies_range(v('3.0.0'), '^3.0.0')

    def test_rhods_operator_prefix_in_range_tolerated(self):
        """Convention is bare semver in ranges, but RhoaiVersion is tolerant
        of the rhods-operator. prefix so this happens to work."""
        assert satisfies_range(v('3.0.0'), '>=rhods-operator.3.0.0') is True

    def test_empty_string_returns_false(self):
        assert satisfies_range(v('3.0.0'), '') is False

    def test_whitespace_only_returns_false(self):
        assert satisfies_range(v('3.0.0'), '   ') is False

    def test_none_raises_or_returns_false(self):
        """None input should not crash -- either False or TypeError is acceptable."""
        try:
            result = satisfies_range(v('3.0.0'), None)
            assert result is False
        except TypeError:
            pass

    def test_valid_range_with_extra_whitespace(self):
        """Extra whitespace should be tolerated."""
        assert satisfies_range(v('3.5.0'), '  >=3.5.0  ') is True
        assert satisfies_range(v('3.5.0'), '>=3.0.0   <4.0.0') is True
