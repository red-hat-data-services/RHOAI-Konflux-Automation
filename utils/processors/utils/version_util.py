"""
Shared version parsing utilities for RHOAI processors.

Provides OcpVersion and RhoaiVersion classes for semver-aware comparison
of OpenShift Container Platform versions and RHOAI operator versions.

Also provides satisfies_range() for evaluating OLM-skipRange-inspired
semver range expressions against RhoaiVersion instances.
"""

import re

from logger.logger import getLogger

LOGGER = getLogger('processor')

# Matches both raw image tags (e.g. "v3.4.0-ea.1") and operator bundle names
# (e.g. "rhods-operator.3.4.0-ea.1"). The $ anchor rejects build-number tags
# (v2.16.0-1733155920) and source tags (v2.16.0-source) since they have
# trailing content that doesn't match end-of-string.
#
# Capture groups:
#   1 = full version string  (e.g. "3.4.0-ea.1")
#   2 = major, 3 = minor, 4 = patch
#   5 = EA sequence number   (None for GA)
#   6 = EA hotfix number     (None if no hotfix)
VERSION_REGEX = re.compile(
    r'(?:rhods-operator\.|v?)((\d+)\.(\d+)\.(\d+)(?:-ea\.(\d+)(?:\.(\d+))?)?)$'
)


class OcpVersion:
    """
    Comparable (major, minor) representation of an OpenShift version string.

    Accepts formats: 'v4.19', '4.19', or a tuple (4, 19).
    """

    _OCP_REGEX = re.compile(r'v?(0|[1-9]\d*)\.(0|[1-9]\d*)')

    def __init__(self, version):
        if isinstance(version, tuple):
            if (
                len(version) != 2
                or any(isinstance(part, bool) or not isinstance(part, int) for part in version)
                or any(part < 0 for part in version)
            ):
                raise ValueError(
                    "OcpVersion tuple must contain exactly two non-negative integers"
                )
            self._tuple = version
        elif isinstance(version, str):
            match = self._OCP_REGEX.fullmatch(version)
            if not match:
                LOGGER.warning(f"Cannot parse OCP version: {version}")
                raise ValueError(f"Cannot parse OCP version: {version}")
            self._tuple = (int(match.group(1)), int(match.group(2)))
        else:
            raise TypeError(f"OcpVersion expects str or tuple, got {type(version)}")

    def __ge__(self, other):
        return self._tuple >= other._tuple

    def __le__(self, other):
        return self._tuple <= other._tuple

    def __gt__(self, other):
        return self._tuple > other._tuple

    def __lt__(self, other):
        return self._tuple < other._tuple

    def __eq__(self, other):
        return self._tuple == other._tuple

    def __hash__(self):
        return hash(self._tuple)

    def __repr__(self):
        return f"v{self._tuple[0]}.{self._tuple[1]}"


class RhoaiVersion:
    """
    Comparable version representation for RHOAI operator bundles.

    Parses 'rhods-operator.MAJOR.MINOR.PATCH[-ea.SEQ[.HOTFIX]]' or bare
    version strings like '3.4.0-ea.1' into a sortable tuple.

    GA versions get is_ga=1 (sorts higher than EA's is_ga=0), so
    3.4.0 (GA) > 3.4.0-ea.N (any EA).
    """

    def __init__(self, version: str):
        self.version = version
        self._parsed_tuple = self._parse_version(version)

    def _parse_version(self, version_string: str):
        match_result = VERSION_REGEX.match(version_string)
        if not match_result:
            raise ValueError(f"Cannot parse operator version: {version_string}")

        major = int(match_result.group(2))
        minor = int(match_result.group(3))
        patch = int(match_result.group(4))
        ea_sequence = match_result.group(5)
        ea_hotfix = match_result.group(6)

        if ea_sequence is not None:
            is_ga = 0
            ea_sequence_num = int(ea_sequence)
            ea_hotfix_num = int(ea_hotfix) if ea_hotfix else 0
        else:
            is_ga = 1
            ea_sequence_num = 0
            ea_hotfix_num = 0

        return (major, minor, patch, is_ga, ea_sequence_num, ea_hotfix_num)

    def is_ga(self) -> bool:
        return self._parsed_tuple[3] == 1

    def is_ea(self) -> bool:
        return self._parsed_tuple[3] == 0

    def is_latest_ea(self, bundle_names_list) -> bool:
        """
        Given a list of bundle names from a catalog, determine whether this
        version is the globally highest EA release across all major.minor.patch
        versions. GA releases in the list are ignored.

        Example:
            bundles = ['3.4.0-ea.1', '3.4.0-ea.2', '3.4.0-ea.3', '3.5.0-ea.1', '3.5.0-ea.2']
            RhoaiVersion('3.4.0-ea.3').is_latest_ea(bundles)  -> False
            RhoaiVersion('3.5.0-ea.2').is_latest_ea(bundles)  -> True

        Must only be called on EA versions. Raises ValueError if called on GA.
        """
        if self.is_ga():
            raise ValueError(
                f"is_latest_ea() must only be called on EA versions, got GA: {self.version}"
            )

        latest_ea_version = self
        for bundle_name in bundle_names_list:
            try:
                parsed_version = RhoaiVersion(bundle_name)
            except ValueError:
                continue
            if parsed_version >= latest_ea_version and parsed_version.is_ea():
                latest_ea_version = parsed_version

        LOGGER.debug(f"{latest_ea_version} is the newest EA release")
        return latest_ea_version == self

    def __ge__(self, other):
        return self._parsed_tuple >= other._parsed_tuple

    def __le__(self, other):
        return self._parsed_tuple <= other._parsed_tuple

    def __gt__(self, other):
        return self._parsed_tuple > other._parsed_tuple

    def __lt__(self, other):
        return self._parsed_tuple < other._parsed_tuple

    def __eq__(self, other):
        return self._parsed_tuple == other._parsed_tuple

    def __hash__(self):
        return hash(self._parsed_tuple)

    def __getitem__(self, key):
        return self._parsed_tuple[key]

    def __repr__(self):
        return self.version


# ---------------------------------------------------------------------------
# Range matching (OLM-skipRange-inspired, deliberate subset of blang/semver)
# ---------------------------------------------------------------------------

_COMPARATOR_RE = re.compile(r'^(>=|>|<=|<|!=|=)?(.+)$')


def _match_comparator(version: RhoaiVersion, token: str) -> bool:
    """
    Evaluate a single comparator token against a RhoaiVersion.

    Supported tokens: '>=3.5.0', '<2.26.0', '!=3.2.1', '=2.25.0',
    or bare '2.25.0' (implicit equality).
    Versions in tokens are bare semver -- not 'rhods-operator.X.Y.Z'.
    """
    match = _COMPARATOR_RE.match(token.strip())
    if not match:
        raise ValueError(f"Invalid comparator token: {token!r}")
    op = match.group(1) or '='
    ver_str = match.group(2)
    target = RhoaiVersion(ver_str)
    if op == '>=':
        return version >= target
    if op == '>':
        return version > target
    if op == '<=':
        return version <= target
    if op == '<':
        return version < target
    if op == '=':
        return version == target
    if op == '!=':
        return version != target
    raise ValueError(f"Unsupported comparator operator: {op!r}")


def satisfies_range(version: RhoaiVersion, range_str: str) -> bool:
    """
    Check whether *version* satisfies an OLM-inspired semver range string.

    Clauses are OR'd (split on '||'); conditions within a clause are AND'd
    (split on whitespace).  Returns False for empty / whitespace-only strings.

    Examples:
        satisfies_range(v('2.25.0'), '>=2.25.0 <2.26.0')          -> True
        satisfies_range(v('3.2.1'),  '>=2.25.0 <2.26.0 || >=3.5.0') -> False
        satisfies_range(v('3.5.0'),  '>=2.25.0 <2.26.0 || >=3.5.0') -> True
    """
    if not range_str or not range_str.strip():
        return False
    for clause in range_str.split('||'):
        tokens = [t for t in clause.split() if t.strip()]
        if not tokens:
            continue
        if all(_match_comparator(version, t) for t in tokens):
            return True
    return False
