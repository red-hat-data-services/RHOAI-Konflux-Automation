"""
Shared constants for RHOAI processors.

This module contains constants used by the RHOAI processors.
"""

# Git label keys used in container image manifests
GIT_URL_LABEL_KEY = 'git.url'
GIT_COMMIT_LABEL_KEY = 'git.commit'
GITHUB_URL_LABEL_KEY = 'github.url'
GITHUB_COMMIT_LABEL_KEY = 'github.commit'

# Registry constants
PRODUCTION_REGISTRY = 'registry.redhat.io'
STAGE_REGISTRY = 'registry.stage.redhat.io'

# Operator constants
OPERATOR_NAME = 'rhods-operator'
OPERANDS_MAP_PATH = 'build/operands-map.yaml'
MANIFESTS_CONFIG_PATH = 'build/manifests-config.yaml'

# Channel constants
RESET_CHANNELS = {'beta'}

# Catalog validator constants
MISSING_BUNDLE_EXCEPTIONS = ['rhods-operator.2.9.0', 'rhods-operator.2.9.1'] # ref - RHOAIENG-8828
MIN_OCP_VERSION_FOR_RHOAI_30 = 'v4.19'

# Legacy bundles from the deprecated rhods/odh-operator-bundle repository.
# These are expected to exist in catalogs but are NOT in shipped_rhoai_versions,
# so they should be excluded from the unreleased bundles check.
LEGACY_BUNDLES = {
    'rhods-operator.1.20.1-8',
    'rhods-operator.1.21.0-22',
    'rhods-operator.1.22.0-2',
    'rhods-operator.1.22.1-2',
    'rhods-operator.1.22.1-4',
    'rhods-operator.1.23.0',
    'rhods-operator.1.24.0',
    'rhods-operator.1.25.0',
    'rhods-operator.1.26.0',
    'rhods-operator.1.27.0',
    'rhods-operator.1.28.0',
    'rhods-operator.1.28.1',
    'rhods-operator.1.29.0',
    'rhods-operator.1.30.0',
    'rhods-operator.1.31.0',
    'rhods-operator.1.32.0',
    'rhods-operator.1.33.0',
    'rhods-operator.2.11.0-0.1727935135.p',
    'rhods-operator.2.4.0',
    'rhods-operator.2.5.0',
}
