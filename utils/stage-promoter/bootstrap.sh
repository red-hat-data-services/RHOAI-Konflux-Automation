
# Usage
#
# export BRANCH and PREVIOUS_BRANCH variables before running this 
#  script as required.
# make sure you already have oc configured to point to p02 cluster,
# and quay credentials to read RHOAI quay org.
#
# Before running this script, source the `bootstrap-auth.sh` script 
#  to get the required oauth token for quay API.
#
#    source ../fbc-processor/bootstrap-auth.sh # -> only need to run this once per terminal session
#
#    export BRANCH=rhoai-3.4-ea.2
#    export PREVIOUS_BRANCH=rhoai-2.25
#    export RHOAI_VERSION=v3.4.0-ea.2
#    ./bootstrap.sh
# 
# run ./bootstrap.sh --pull-repos if you want to re-pull RHOAI-Build-Config.

set -eo pipefail
BRANCH=${BRANCH:-rhoai-3.4-ea.2}
RHOAI_VERSION=${RHOAI_VERSION:-v3.4.0-ea.2}
PREVIOUS_BRANCH=${PREVIOUS_BRANCH:-rhoai-2.21}
PREVIOUS_RHOAI_VERSION=${PREVIOUS_RHOAI_VERSION:-v2.21.0}

if [[ "$1" == "--pull-repos"  || ! -d main || ! -d "${BRANCH}" || ! -d "${PREVIOUS_BRANCH}" ]]; then
  rm -rf "${BRANCH}" "${PREVIOUS_BRANCH}" main
  git init main
  git -C main remote add origin https://github.com/red-hat-data-services/RHOAI-Build-Config.git 

  # shallow fetch, use `git fetch --unshallow` to get full fetch`
  git -C main fetch origin --depth=1 main "${BRANCH}" "${PREVIOUS_BRANCH}"
  git -C main checkout main
  git -C main worktree add ../"${BRANCH}" origin/"${BRANCH}"
  git -C main worktree add ../"${PREVIOUS_BRANCH}" origin/"${PREVIOUS_BRANCH}"
fi

#Declare basic variables
COMPONENT_SUFFIX=${RHOAI_VERSION//./-}
OPERATOR_BUNDLE_COMPONENT_NAME=odh-operator-bundle

#Declare FBC processing variables
BUILD_CONFIG_PATH=${BRANCH}/config/build-config.yaml
PREVIOUS_BUILD_CONFIG_PATH=${PREVIOUS_BRANCH}/config/build-config.yaml

PATCH_YAML_PATH=${BRANCH}/catalog/catalog-patch.yaml
PREVIOUS_PATCH_YAML_PATH=${PREVIOUS_BRANCH}/catalog/catalog-patch.yaml

PCC_BUNDLE_OBJECT_CATALOG_YAML_PATH=main/pcc/bundle_object_catalog.yaml
PCC_CSV_META_CATALOG_YAML_PATH=main/pcc/csv_meta_catalog.yaml
CSV_META_MIN_OCP_VERSION=417

function python3 () {
  uv run --with-requirements requirements.txt $@
}

STAGE_PROMOTER_PATH=.

while IFS= read -r ocp_version;
do
    OPENSHIFT_VERSION=$ocp_version
    echo "OPENSHIFT_VERSION=$OPENSHIFT_VERSION"
    NUMERIC_OCP_VERSION=${OPENSHIFT_VERSION/v4./4}

    CATALOG_YAML_PATH=main/pcc/catalog-${OPENSHIFT_VERSION}.yaml

    RELEASE_CATALOG_YAML_PATH=${BRANCH}/catalog/${OPENSHIFT_VERSION}/rhods-operator/catalog.yaml
    PREVIOUS_RELEASE_CATALOG_YAML_PATH=${PREVIOUS_BRANCH}/catalog/${OPENSHIFT_VERSION}/rhods-operator/catalog.yaml

    OUTPUT_CATALOG_DIR=main/catalog/${BRANCH}/${OPENSHIFT_VERSION}/rhods-operator/
    mkdir -p ${OUTPUT_CATALOG_DIR}
    OUTPUT_CATALOG_PATH=${OUTPUT_CATALOG_DIR}/catalog.yaml

    #Invoke the stage promoter to patch the main catalog with release branch
    python3 $STAGE_PROMOTER_PATH/stage_promoter.py -op stage-catalog-patch -c ${CATALOG_YAML_PATH} -p ${PATCH_YAML_PATH} -r ${RELEASE_CATALOG_YAML_PATH} -o ${OUTPUT_CATALOG_PATH} -v ${RHOAI_VERSION}
    
    if [ -e "${PREVIOUS_RELEASE_CATALOG_YAML_PATH}" ]; then
        # Previous Release - Invoke the stage promoter to patch the main catalog with previous release branch
        python3 $STAGE_PROMOTER_PATH/stage_promoter.py -op stage-catalog-patch -c ${OUTPUT_CATALOG_PATH} -p ${PREVIOUS_PATCH_YAML_PATH} -r ${PREVIOUS_RELEASE_CATALOG_YAML_PATH} -o ${OUTPUT_CATALOG_PATH} -v ${PREVIOUS_RHOAI_VERSION}
    else
        echo "Skipping ${OPENSHIFT_VERSION} while patching previous release yaml, the OCP version doesn't exist for previous release ${PREVIOUS_BRANCH}"
    fi
done < <(yq eval '.config.supported-ocp-versions.build[].name' $BUILD_CONFIG_PATH)
