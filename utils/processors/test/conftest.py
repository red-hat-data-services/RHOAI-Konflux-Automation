"""
Shared fixtures for catalog_validator tests.

Provides helper functions to build YAML fixture files and construct
catalog_validator instances with controlled test data, avoiding any
real file I/O outside the tmp_path sandbox.
"""

import os
import sys
import yaml
import pytest


# ---------------------------------------------------------------------------
# Helper: write a multi-document YAML catalog file
# ---------------------------------------------------------------------------
def write_catalog_yaml(path, bundle_names):
    """
    Write a minimal multi-document YAML catalog containing olm.bundle entries
    for each name in *bundle_names*, plus a mandatory olm.package entry.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    docs = [{'schema': 'olm.package', 'name': 'rhods-operator', 'defaultChannel': 'fast'}]
    for name in bundle_names:
        docs.append({'schema': 'olm.bundle', 'name': name, 'package': 'rhods-operator'})
    with open(path, 'w') as f:
        yaml.dump_all(docs, f)


# ---------------------------------------------------------------------------
# Helper: write a shipped_rhoai_versions file
# ---------------------------------------------------------------------------
def write_shipped_versions(path, versions):
    """Write one version tag per line (e.g. 'v2.16.0', 'v3.4.0-ea.1')."""
    with open(path, 'w') as f:
        for v in versions:
            f.write(f'{v}\n')


# ---------------------------------------------------------------------------
# Helper: write global config.yaml
# ---------------------------------------------------------------------------
def write_global_config(path, ocp_entries):
    """
    Write a global config.yaml.

    *ocp_entries* is a list of dicts, each with at least 'version' and optionally
    'discontinued-from', 'onboarded-since', 'skip-bundles'.
    """
    data = {'config': {'supported-ocp-versions': ocp_entries}}
    with open(path, 'w') as f:
        yaml.dump(data, f)


# ---------------------------------------------------------------------------
# Helper: write build-config for validate-catalogs
# ---------------------------------------------------------------------------
def write_build_config_catalogs(path, release_versions, build_versions=None):
    """
    Write a build-config.yaml with the schema used by validate-catalogs.

    release_versions: list of strings like ['v4.17', 'v4.19']
    build_versions:   list of dicts like [{'name': 'v4.21'}]
    """
    data = {
        'config': {
            'supported-ocp-versions': {
                'release': release_versions,
                'build': build_versions or [],
            }
        }
    }
    with open(path, 'w') as f:
        yaml.dump(data, f)


# ---------------------------------------------------------------------------
# Helper: write build-config for validate-pcc
# ---------------------------------------------------------------------------
def write_build_config_pcc(path, ocp_versions):
    """
    Write a config.yaml with the schema used by validate-pcc.

    ocp_versions: list of strings like ['v4.17', 'v4.19']
    """
    data = {
        'config': {
            'supported-ocp-versions': [{'version': v} for v in ocp_versions]
        }
    }
    with open(path, 'w') as f:
        yaml.dump(data, f)


# ---------------------------------------------------------------------------
# Factory fixture to build a catalog_validator with controlled data
# ---------------------------------------------------------------------------
@pytest.fixture
def make_validator(tmp_path):
    """
    Returns a factory function that creates a catalog_validator instance
    with fully controlled file fixtures.
    """
    processors_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if processors_root not in sys.path:
        sys.path.insert(0, processors_root)

    from validator.catalog_validator import catalog_validator as CatalogValidator

    def _factory(
        operation,
        ocp_versions,
        global_ocp_entries,
        shipped_version_tags,
        catalogs,
    ):
        """
        Parameters
        ----------
        operation : str
            'validate-catalogs' or 'validate-pcc'
        ocp_versions : list[str]
            OCP versions to validate (e.g. ['v4.17', 'v4.19'])
        global_ocp_entries : list[dict]
            Entries for the global config (discontinuity, onboarding, skip-bundles)
        shipped_version_tags : list[str]
            Raw version tags for shipped_rhoai_versions file
        catalogs : dict[str, list[str]]
            Mapping of ocp_version -> list of bundle names in that catalog
        """
        build_config_path = str(tmp_path / 'build-config.yaml')
        global_config_path = str(tmp_path / 'global-config.yaml')
        shipped_path = str(tmp_path / 'shipped_rhoai_versions.txt')
        catalog_folder = str(tmp_path / 'catalogs')
        os.makedirs(catalog_folder, exist_ok=True)

        if operation == 'validate-catalogs':
            write_build_config_catalogs(build_config_path, ocp_versions)
        else:
            write_build_config_pcc(build_config_path, ocp_versions)

        write_global_config(global_config_path, global_ocp_entries)
        write_shipped_versions(shipped_path, shipped_version_tags)

        for ocp_ver, bundle_names in catalogs.items():
            if operation == 'validate-catalogs':
                cat_path = os.path.join(catalog_folder, ocp_ver, 'rhods-operator', 'catalog.yaml')
            else:
                cat_path = os.path.join(catalog_folder, f'catalog-{ocp_ver}.yaml')
            write_catalog_yaml(cat_path, bundle_names)

        return CatalogValidator(
            build_config_path=build_config_path,
            catalog_folder_path=catalog_folder,
            shipped_rhoai_versions_path=shipped_path,
            operation=operation,
            global_config_path=global_config_path,
        )

    return _factory
