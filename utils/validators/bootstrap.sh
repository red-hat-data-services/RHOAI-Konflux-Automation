# Usage
#
# export BRANCH variables before running this 
#  script as required.
#
#    export BRANCH=rhoai-3.4-ea.2
#    ./bootstrap.sh
# 
# run ./bootstrap.sh --pull-repos if you want to re-pull RHOAI-Build-Config.

set -eo pipefail

BRANCH=${BRANCH:-rhoai-3.4-ea.2}


if [[ "$1" == "--pull-repos"  || ! -d main || ! -d "${BRANCH}" ]]; then
  rm -rf "${BRANCH}" main
  git init main
  git -C main remote add origin https://github.com/red-hat-data-services/RHOAI-Build-Config.git 

  # shallow fetch, use `git fetch --unshallow` to get full fetch`
  git -C main fetch origin --depth=1 main "${BRANCH}" 
  git -C main checkout main
  git -C main worktree add ../"${BRANCH}" origin/"${BRANCH}"
fi

function python3 () {
  uv run $@
}

PCC_FOLDER_PATH=main/pcc
BUILD_CONFIG_PATH=main/config/config.yaml
SHIPPED_RHOAI_VERSIONS_PATH=main/pcc/shipped_rhoai_versions_granular.txt
PCC_FOLDER_PATH=main/pcc
GLOBAL_CONFIG_PATH=main/config/config.yaml

echo PCC Cache Validation
python3 ./catalog_validator.py -op validate-pcc --build-config-path ${BUILD_CONFIG_PATH} --catalog-folder-path ${PCC_FOLDER_PATH} --shipped-rhoai-versions-path ${SHIPPED_RHOAI_VERSIONS_PATH} --global-config-path ${GLOBAL_CONFIG_PATH}

BUILD_CONFIG_PATH=${BRANCH}/config/build-config.yaml
SHIPPED_RHOAI_VERSIONS_PATH=main/pcc/shipped_rhoai_versions_granular.txt
CATALOG_FOLDER_PATH=${BRANCH}/catalog
GLOBAL_CONFIG_PATH=main/config/config.yaml

echo Running Catalog Validation
python3 ./catalog_validator.py -op validate-catalogs --build-config-path ${BUILD_CONFIG_PATH} --catalog-folder-path ${CATALOG_FOLDER_PATH} --shipped-rhoai-versions-path ${SHIPPED_RHOAI_VERSIONS_PATH} --global-config-path ${GLOBAL_CONFIG_PATH}

