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

# Operator constants
OPERATOR_NAME = 'rhods-operator'
OPERANDS_MAP_PATH = 'build/operands-map.yaml'
MANIFESTS_CONFIG_PATH = 'build/manifests-config.yaml'
