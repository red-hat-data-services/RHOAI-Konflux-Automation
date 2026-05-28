"""
Catalog Validator for RHOAI operator catalogs.

Validates that every shipped RHOAI operator bundle is present in the correct
catalog files, accounting for OCP-version-specific constraints: onboarding
dates, discontinuation cutoffs, skip-bundle overrides, and the 3.x / OCP 4.19
minimum boundary.

Supports two operations via the -op flag:
  validate-pcc:       Validates PCC (Production Catalog Cache) files
  validate-catalogs:  Validates final generated catalogs per release branch
"""

import sys
import json
import argparse
from collections import defaultdict

import yaml

from logger.logger import getLogger
import utils.util as util
import utils.version_util as version_util
import constants.constants as CONSTANTS

LOGGER = getLogger('processor')


class catalog_validator:

    def __init__(self, build_config_path: str, catalog_folder_path: str,
                 shipped_rhoai_versions_path: str, operation: str,
                 global_config_path: str):
        LOGGER.info("=============================================================================")
        LOGGER.info("Initializing Catalog Validator")
        LOGGER.info("=============================================================================")
        LOGGER.info(f"operation:                   {operation}")
        LOGGER.info(f"build_config_path:           {build_config_path}")
        LOGGER.info(f"global_config_path:          {global_config_path}")
        LOGGER.info(f"catalog_folder_path:         {catalog_folder_path}")
        LOGGER.info(f"shipped_rhoai_versions_path: {shipped_rhoai_versions_path}")
        LOGGER.info("")

        self.operation = operation
        self.catalog_folder_path = catalog_folder_path

        LOGGER.info("Loading configuration files...")

        with open(build_config_path, 'r') as build_config_file:
            build_config = yaml.safe_load(build_config_file)

        with open(global_config_path, 'r') as global_config_file:
            global_config = yaml.safe_load(global_config_file)

        global_ocp_versions = global_config['config']['supported-ocp-versions']

        # Maps OCP version -> bundle version at which that OCP is discontinued
        # (bundles at or above this version are not expected in the catalog)
        self.discontinued_from_map = {
            entry['version']: entry.get('discontinued-from', 'rhods-operator.9.99.99')
            for entry in global_ocp_versions
        }
        LOGGER.info(f"discontinued_from_map: {json.dumps(self.discontinued_from_map, indent=4)}")

        # Maps OCP version -> earliest bundle version that should appear
        # (bundles below this version predate this OCP's onboarding)
        self.onboarded_since_map = {
            entry['version']: entry.get('onboarded-since', 'rhods-operator.0.0.0')
            for entry in global_ocp_versions
        }
        LOGGER.info(f"onboarded_since_map: {json.dumps(self.onboarded_since_map, indent=4)}")

        # Maps OCP version -> list of bundles explicitly excluded from validation
        self.skip_bundles_map = {
            entry['version']: entry.get('skip-bundles', [])
            for entry in global_ocp_versions
        }
        LOGGER.info(f"skip_bundles_map: {json.dumps(self.skip_bundles_map, indent=4)}")

        # Build a list of (ocp_version, catalog_path) tuples mapping each supported OCP version
        # to its corresponding catalog file path, depending on the operation mode (PCC or relese version catalogs)
        self.catalog_entries = self._build_catalog_entries(
            build_config, operation, catalog_folder_path
        )
        LOGGER.info(f"catalog_entries: {json.dumps([(version, path) for version, path in self.catalog_entries], indent=4)}")

        # Deduplicate and sort all valid version strings from the shipped versions file
        # Example: ['2.25.0', '3.4.1', '3.5.0-ea.1', '3.5.0-ea.2']
        raw_versions = util.read_file_lines(shipped_rhoai_versions_path)
        self.shipped_rhoai_versions = sorted(list(set(
            version_match.group(1)
            for raw_version in raw_versions
            for version_match in [version_util.VERSION_REGEX.match(raw_version.strip())]
            if version_match
        )))
        LOGGER.info(f"shipped_rhoai_versions {self.shipped_rhoai_versions}")

        LOGGER.info("")
        LOGGER.info("All configuration files loaded successfully!")
        LOGGER.info("")

    def _build_catalog_entries(self, build_config, operation, catalog_folder_path):
        """
        Build a uniform list of (ocp_version, catalog_path) tuples regardless
        of operation mode.

        For validate-pcc:      config has a flat list of dicts with 'version' keys;
                               catalog files are flat: catalog-v4.XX.yaml
        For validate-catalogs: config has 'release' + 'build' sub-lists;
                               catalog files are nested: v4.XX/rhods-operator/catalog.yaml
        """
        catalog_entries = []
        ocp_config = build_config['config']['supported-ocp-versions']

        if operation == 'validate-pcc':
            ocp_versions = sorted(
                [entry['version'] for entry in ocp_config],
                key=lambda version: version_util.OcpVersion(version)._tuple
            )
            for ocp_version in ocp_versions:
                catalog_path = f'{catalog_folder_path}/catalog-{ocp_version}.yaml'
                catalog_entries.append((ocp_version, catalog_path))

        elif operation == 'validate-catalogs':
            release_versions = ocp_config.get('release', [])
            build_versions = [item['name'] for item in ocp_config.get('build', [])]
            ocp_versions = sorted(
                list(set(release_versions + build_versions)),
                key=lambda version: version_util.OcpVersion(version)._tuple
            )
            for ocp_version in ocp_versions:
                catalog_path = f'{catalog_folder_path}/{ocp_version}/rhods-operator/catalog.yaml'
                catalog_entries.append((ocp_version, catalog_path))

        else:
            raise ValueError(
                f"Unknown operation '{operation}'. "
                f"Expected 'validate-pcc' or 'validate-catalogs'."
            )

        return catalog_entries

    def validate(self):
        """
        Unified validation for both PCC and catalog operations.

        Checks that every shipped RHOAI version is present in each catalog
        where it should be, and flags any 3.x bundles incorrectly present
        on OCP versions below 4.19.

        TODO: Currently this only verifies "is everything that should be there
        actually there?" — it does not verify "is everything that is there
        supposed to be there?" (unreleased bundles are logged but not enforced).
        """

        missing_bundles = {}
        incorrect_3x_bundles = {}
        unreleased_bundles = {}
        # Convert "v4.19" -> (4, 19) for version comparison
        min_ocp_for_rhoai_30 = version_util.OcpVersion(CONSTANTS.MIN_OCP_VERSION_FOR_RHOAI_30)

        for ocp_version, catalog_path in self.catalog_entries:
            LOGGER.info("")
            LOGGER.info("=============================================================================")
            LOGGER.info(f"Validating catalog for OCP {ocp_version}")
            LOGGER.info("=============================================================================")

            # Initializing accumulators for storing this OCP version's validation results
            missing_bundles[ocp_version] = []
            incorrect_3x_bundles[ocp_version] = []

            try:
                docs = util.load_multi_document_yaml_file(catalog_path)
            except FileNotFoundError:
                LOGGER.error(f"Catalog file not found for OCP {ocp_version}: {catalog_path}")
                sys.exit(1)

            # Index catalog documents by schema type, then by name for O(1) lookups.
            # Example result:
            #   catalog_dict = {
            #       "olm.bundle": {
            #           "rhods-operator.2.16.0": { ...full doc... },
            #           "rhods-operator.2.17.0": { ...full doc... },
            #       },
            #       "olm.package": {
            #           "rhods-operator": { ...full doc... },
            #       },
            #       "olm.channel": {
            #           "fast": { ...full doc... },
            #       },
            #   }
            catalog_dict = defaultdict(dict)
            for doc in docs:
                catalog_dict[doc['schema']][doc['name']] = doc
            LOGGER.debug(f"catalog_dict keys: {json.dumps({schema: list(names.keys()) for schema, names in catalog_dict.items()}, indent=4)}")

            # Extract only the bundle entries
            bundles = catalog_dict['olm.bundle']  

            # Convert e.g. "v4.17" -> (4, 17) for version comparison
            parsed_ocp_version = version_util.OcpVersion(ocp_version)

            # Boundaries for this OCP version: bundles outside [onboard, discontinued) are not expected
            onboarded_since_version = version_util.RhoaiVersion(
                self.onboarded_since_map.get(ocp_version, 'rhods-operator.0.0.0')
            )
            discontinued_from_version = version_util.RhoaiVersion(
                self.discontinued_from_map.get(ocp_version, 'rhods-operator.9.99.99')
            )

            for rhoai_version in self.shipped_rhoai_versions:
                operator_name = f'{CONSTANTS.OPERATOR_NAME}.{rhoai_version}'

                # Convert e.g. "rhods-operator.2.16.0" -> (2, 16, 0) for version arithmetic
                operator_version = version_util.RhoaiVersion(operator_name)

                # RHOAI 3.x requires OCP >= 4.19; flag if present on older clusters
                is_3x_on_unsupported_ocp = (
                    rhoai_version.startswith('3')
                    and parsed_ocp_version < min_ocp_for_rhoai_30
                )

                # Step 1: Check if the bundle exists in the catalog.
                # If it does, verify it's not a 3.x bundle on an unsupported OCP (< 4.19).
                # e.g. "rhods-operator.3.0.0" should NOT be in v4.17 catalog
                if operator_name in bundles:
                    if is_3x_on_unsupported_ocp:
                        LOGGER.error(
                            f"Incorrect 3.x bundle '{operator_name}' found in OCP {ocp_version} catalog "
                            f"(RHOAI 3.x requires OCP >= {CONSTANTS.MIN_OCP_VERSION_FOR_RHOAI_30})"
                        )
                        incorrect_3x_bundles[ocp_version].append(operator_name)
                    else:
                        LOGGER.debug(f'Found {operator_name} in OCP {ocp_version} catalog (valid)')
                    continue

                # Step 2: If this is a 3.x bundle absent from an unsupported OCP, that's expected.
                # This is the counterpart to Step 1:
                #     Step 1: catches 3.x bundles that are incorrectly PRESENT,
                #     Step 2: Prevents 3.x bundles from being incorrectly flagged as MISSING.
                # e.g. "rhods-operator.3.1.0" missing from v4.17 is correct — it was never shipped there
                if is_3x_on_unsupported_ocp:
                    LOGGER.debug(f'Ignoring absence of {operator_name} for OCP {ocp_version} (3.x not shipped on OCP < {CONSTANTS.MIN_OCP_VERSION_FOR_RHOAI_30})')
                    continue

                # Step 3: Skip known exceptions — bundles with documented reasons for absence.
                # e.g. bundles excluded due to RHOAIENG-8828 are expected to be missing
                if operator_name in CONSTANTS.MISSING_BUNDLE_EXCEPTIONS:
                    LOGGER.warning(f'Ignoring absence of {operator_name} for OCP {ocp_version} (known exception: RHOAIENG-8828)')
                    continue

                # Step 4: Skip bundles explicitly listed in the config's skip-bundles for this OCP.
                # These are intentionally excluded (e.g. broken builds, hotfixes not meant for this OCP)
                if operator_name in self.skip_bundles_map.get(ocp_version, []):
                    LOGGER.warning(f'Ignoring absence of {operator_name} for OCP {ocp_version} (in skip-bundles list)')
                    continue

                # Step 5: Skip if bundle is outside this OCP's supported version range [onboarded, discontinued).
                if operator_version >= discontinued_from_version or operator_version < onboarded_since_version:
                    LOGGER.debug(f'Ignoring absence of {operator_name} for OCP {ocp_version} (outside supported range)')
                    continue

                # Step 6: EA (Early Access) bundles get replaced by newer EA releases. Only the latest EA bundle is expected to remain; older ones are overwritten.
                # e.g. if "rhods-operator.2.17.0-ea.2" exists, "rhods-operator.2.17.0-ea.1" is pruned and expected to be missing
                if operator_version.is_ea() and not operator_version.is_latest_ea(bundles):
                    LOGGER.debug(f'Ignoring absence of {operator_name} for OCP {ocp_version} (superseded by newer EA release)')
                    continue

                # If none of the above exclusions apply, this bundle is genuinely missing
                LOGGER.error(f'Missing {operator_name} in OCP {ocp_version} catalog')
                missing_bundles[ocp_version].append(operator_name)
                
            # Filter out unreleased/stale bundles: identifies bundles in the catalog that are not in the shipped versions list.
            expected_bundles = {
                f'{CONSTANTS.OPERATOR_NAME}.{v}' for v in self.shipped_rhoai_versions
            }
            unreleased_bundles[ocp_version] = sorted(
                name for name in bundles
                if name not in expected_bundles
                and name not in CONSTANTS.LEGACY_BUNDLES
            )
            if unreleased_bundles[ocp_version]:
                LOGGER.warning(
                    f"OCP {ocp_version}: {len(unreleased_bundles[ocp_version])} unreleased bundle(s) "
                    f"found in catalog (not in shipped versions): {unreleased_bundles[ocp_version]}"
                )

            LOGGER.info("")
            LOGGER.info(
                f"RESULT for OCP {ocp_version}: {len(missing_bundles[ocp_version])} missing, "
                f"{len(incorrect_3x_bundles[ocp_version])} incorrect 3.x, "
                f"{len(unreleased_bundles[ocp_version])} unreleased"
            )

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Validation Results:")
        LOGGER.info("=============================================================================")
        LOGGER.info(f"incorrect_3x_bundles: {json.dumps(incorrect_3x_bundles, indent=4)}")
        LOGGER.info(f"missing_bundles: {json.dumps(missing_bundles, indent=4)}")
        LOGGER.info(f"unreleased_bundles: {json.dumps(unreleased_bundles, indent=4)}")
        LOGGER.info("")

        if any(missing_bundles.values()) or any(incorrect_3x_bundles.values()):
            LOGGER.error('Validation failed')
            sys.exit(1)
        else:
            LOGGER.info('Validation successful')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Validate RHOAI operator catalogs (PCC or generated).'
    )
    parser.add_argument(
        '-op', '--operation', required=True,
        choices=['validate-catalogs', 'validate-pcc'],
        help='Operation: "validate-catalogs" or "validate-pcc"',
        dest='operation'
    )
    parser.add_argument(
        '-b', '--build-config-path', required=True,
        help='Path of the build-config.yaml (or global config.yaml for PCC)',
        dest='build_config_path'
    )
    parser.add_argument(
        '-c', '--catalog-folder-path', required=True,
        help='Path of the catalog folder',
        dest='catalog_folder_path'
    )
    parser.add_argument(
        '-s', '--shipped-rhoai-versions-path', required=True,
        help='Path of the shipped_rhoai_versions_granular.txt',
        dest='shipped_rhoai_versions_path'
    )
    parser.add_argument(
        '-g', '--global-config-path', required=True,
        help='Path of the global config.yaml containing discontinuity/onboarding maps',
        dest='global_config_path'
    )
    args = parser.parse_args()

    validator = catalog_validator(
        build_config_path=args.build_config_path,
        catalog_folder_path=args.catalog_folder_path,
        shipped_rhoai_versions_path=args.shipped_rhoai_versions_path,
        operation=args.operation,
        global_config_path=args.global_config_path
    )
    validator.validate()
