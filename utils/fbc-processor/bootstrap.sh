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
#    source ./bootstrap-auth.sh # -> only need to run this once per terminal session
#
#    export BRANCH=rhoai-3.4-ea.2
#    export PREVIOUS_BRANCH=rhoai-2.25
#    ./bootstrap.sh
# 
# run ./bootstrap.sh --pull-repos if you want to re-pull RHOAI-Build-Config.

set -eo pipefail

BRANCH=${BRANCH:-rhoai-3.4-ea.2}
PREVIOUS_BRANCH=${PREVIOUS_BRANCH:-rhoai-2.25}


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


BUILD_CONFIG_PATH=${BRANCH}/config/build-config.yaml
PREVIOUS_BUILD_CONFIG_PATH=${PREVIOUS_BRANCH}/config/build-config.yaml

RHOAI_VERSION=v${BRANCH/rhoai-/}
COMPONENT_SUFFIX=${RHOAI_VERSION//./-}
PREVIOUS_RHOAI_VERSION=v${PREVIOUS_BRANCH/rhoai-/}
PREVIOUS_COMPONENT_SUFFIX=${PREVIOUS_RHOAI_VERSION//./-}
OPERATOR_BUNDLE_COMPONENT_NAME=odh-operator-bundle

PATCH_YAML_PATH=${BRANCH}/catalog/catalog-patch.yaml
PREVIOUS_PATCH_YAML_PATH=${PREVIOUS_BRANCH}/catalog/catalog-patch.yaml

# Not needed, assuming user is running this locally
# OC_TOKEN=$(echo $OC_TOKEN | awk '{$1=$1};1' | tr -d '\n')
# BASE64_AUTH=$(echo -n "${RHOAI_QUAY_RO_USERNAME}:${RHOAI_QUAY_RO_TOKEN}" | base64 -w 0)
# mkdir -p ${HOME}/.docker
# echo '{"auths":{"quay.io/rhoai/odh-operator-bundle":{"username":"'"${RHOAI_QUAY_RO_USERNAME}"'","password":"'"${RHOAI_QUAY_RO_TOKEN}"'","email":"","auth":"'"${BASE64_AUTH}"'"}}}' > ${HOME}/.docker/config.json

OPERATOR_BUNDLE_IMAGE_NAME=RELATED_IMAGE_ODH_OPERATOR_BUNDLE_IMAGE
echo "OPERATOR_BUNDLE_IMAGE_NAME = $OPERATOR_BUNDLE_IMAGE_NAME"

APPLICATION_NAME=rhoai-${COMPONENT_SUFFIX}
echo "APPLICATION_NAME = $APPLICATION_NAME"

#Invoke the FBC processor to extract the snapshot images
CATALOG_BUILD_ARGS_FILE_PATH=${BRANCH}/catalog/catalog_build_args.map

function python3 () {
  uv run $@
}
FBC_PROCESSOR_PATH=${FBC_PROCESSOR_PATH}/
FBC_PROCESSOR_PATH=.

TEMP_DIR="./temp"
mkdir -p ${TEMP_DIR}

python3 ${FBC_PROCESSOR_PATH}/fbc-processor.py -op extract-snapshot-images -o ${TEMP_DIR}/snapshot_images.json -v ${BRANCH} -f ${OPERATOR_BUNDLE_COMPONENT_NAME} -b ${BUILD_CONFIG_PATH} --catalog-build-args-file-path ${CATALOG_BUILD_ARGS_FILE_PATH}

cp ./fbc-semver-template.yaml ${TEMP_DIR}/
LATEST_BUNDLE_IMAGE=$(jq --arg OPERATOR_BUNDLE_IMAGE_NAME "$OPERATOR_BUNDLE_IMAGE_NAME" -r '.[]   | select(.name == $OPERATOR_BUNDLE_IMAGE_NAME) | .value' ${TEMP_DIR}/snapshot_images.json)
echo "LATEST_BUNDLE_IMAGE = $LATEST_BUNDLE_IMAGE"
yq e -i '.stable.bundles[0].image = "$LATEST_BUNDLE_IMAGE"' "${TEMP_DIR}/fbc-semver-template.yaml"
LATEST_BUNDLE_IMAGE="$LATEST_BUNDLE_IMAGE" yq e -i '.stable.bundles[0].image = env(LATEST_BUNDLE_IMAGE)' "${TEMP_DIR}/fbc-semver-template.yaml"

#Previous Release - Invoke the FBC processor to extract the snapshot images
cp -f "${TEMP_DIR}/fbc-semver-template.yaml" "${TEMP_DIR}/previous-fbc-semver-template.yaml"
PREVIOUS_CATALOG_BUILD_ARGS_FILE_PATH=${PREVIOUS_BRANCH}/catalog/catalog_build_args.map
python3 ${FBC_PROCESSOR_PATH}/fbc-processor.py -op extract-snapshot-images -o ${TEMP_DIR}/previous_snapshot_images.json -v ${PREVIOUS_BRANCH} -f ${OPERATOR_BUNDLE_COMPONENT_NAME} -b ${PREVIOUS_BUILD_CONFIG_PATH} --catalog-build-args-file-path ${PREVIOUS_CATALOG_BUILD_ARGS_FILE_PATH}
PREVIOUS_LATEST_BUNDLE_IMAGE=$(jq --arg OPERATOR_BUNDLE_IMAGE_NAME "$OPERATOR_BUNDLE_IMAGE_NAME" -r '.[]   | select(.name == $OPERATOR_BUNDLE_IMAGE_NAME) | .value' ${TEMP_DIR}/previous_snapshot_images.json)
echo "PREVIOUS_LATEST_BUNDLE_IMAGE = $PREVIOUS_LATEST_BUNDLE_IMAGE"
yq e -i '.stable.bundles[0].image = "$PREVIOUS_LATEST_BUNDLE_IMAGE"' ${TEMP_DIR}/previous-fbc-semver-template.yaml
PREVIOUS_LATEST_BUNDLE_IMAGE="$PREVIOUS_LATEST_BUNDLE_IMAGE" yq e -i '.stable.bundles[0].image = env(PREVIOUS_LATEST_BUNDLE_IMAGE)' ${TEMP_DIR}/previous-fbc-semver-template.yaml


#Generate the single bundle catalog === sbc
CSV_META_MIN_OCP_VERSION=417
WORK_DIR=work_dir
mkdir -p ${WORK_DIR}
BUNDLE_OBJECT_SINGLE_BUNDLE_PATH=${WORK_DIR}/bundle_object_sbc_semver.yaml
CSV_META_SINGLE_BUNDLE_PATH=${WORK_DIR}/csv_meta_sbc_semver.yaml
CSV_META_OPM_FLAG="--migrate-level=bundle-object-to-csv-metadata"
DOCKER_CONFIG=${HOME}/.docker/ opm alpha render-template semver -o yaml ${TEMP_DIR}/fbc-semver-template.yaml > ${BUNDLE_OBJECT_SINGLE_BUNDLE_PATH}
DOCKER_CONFIG=${HOME}/.docker/ opm alpha render-template semver ${CSV_META_OPM_FLAG} -o yaml ${TEMP_DIR}/fbc-semver-template.yaml > ${CSV_META_SINGLE_BUNDLE_PATH}
#Previous Release - Generate the single bundle catalog === sbc
PREVIOUS_BUNDLE_OBJECT_SINGLE_BUNDLE_PATH=${WORK_DIR}/previous_bundle_object_sbc_semver.yaml
PREVIOUS_CSV_META_SINGLE_BUNDLE_PATH=${WORK_DIR}/previous_csv_meta_sbc_semver.yaml
DOCKER_CONFIG=${HOME}/.docker/ opm alpha render-template semver -o yaml ${TEMP_DIR}/previous-fbc-semver-template.yaml > ${PREVIOUS_BUNDLE_OBJECT_SINGLE_BUNDLE_PATH}
DOCKER_CONFIG=${HOME}/.docker/ opm alpha render-template semver ${CSV_META_OPM_FLAG} -o yaml ${TEMP_DIR}/previous-fbc-semver-template.yaml > ${PREVIOUS_CSV_META_SINGLE_BUNDLE_PATH}
PCC_BUNDLE_OBJECT_CATALOG_YAML_PATH=main/pcc/bundle_object_catalog.yaml
PCC_CSV_META_CATALOG_YAML_PATH=main/pcc/csv_meta_catalog.yaml
PUSH_PIPELINE_PATH=${BRANCH}/.tekton/rhoai-fbc-fragment-${COMPONENT_SUFFIX}-push.yaml
PREVIOUS_PUSH_PIPELINE_PATH=${PREVIOUS_BRANCH}/.tekton/rhoai-fbc-fragment-${PREVIOUS_COMPONENT_SUFFIX}-push.yaml

while IFS= read -r value;
do
    #Declare FBC processing variables
    OPENSHIFT_VERSION=$value
    NUMERIC_OCP_VERSION=${OPENSHIFT_VERSION/v4./4}
    echo "OPENSHIFT_VERSION=$OPENSHIFT_VERSION"
    OUTPUT_CATALOG_PATH=${BRANCH}/catalog/${OPENSHIFT_VERSION}/rhods-operator/catalog.yaml
    
    PCC_CATALOG_YAML_PATH=main/pcc/catalog-${OPENSHIFT_VERSION}.yaml
    SINGLE_BUNDLE_PATH=${BUNDLE_OBJECT_SINGLE_BUNDLE_PATH}
    if [[ $NUMERIC_OCP_VERSION -ge $CSV_META_MIN_OCP_VERSION ]]; then SINGLE_BUNDLE_PATH=${CSV_META_SINGLE_BUNDLE_PATH}; fi
    python3 ${FBC_PROCESSOR_PATH}/fbc-processor.py -op catalog-patch -b ${BUILD_CONFIG_PATH} -c ${PCC_CATALOG_YAML_PATH} -p ${PATCH_YAML_PATH} -s ${SINGLE_BUNDLE_PATH} -o ${OUTPUT_CATALOG_PATH} --push-pipeline-yaml-path ${PUSH_PIPELINE_PATH} --push-pipeline-operation enable 
    
    PREVIOUS_SINGLE_BUNDLE_PATH=${PREVIOUS_BUNDLE_OBJECT_SINGLE_BUNDLE_PATH}
    if [[ $NUMERIC_OCP_VERSION -ge $CSV_META_MIN_OCP_VERSION ]]; then PREVIOUS_SINGLE_BUNDLE_PATH=${PREVIOUS_CSV_META_SINGLE_BUNDLE_PATH}; fi
    python3 ${FBC_PROCESSOR_PATH}/fbc-processor.py -op catalog-patch -b ${PREVIOUS_BUILD_CONFIG_PATH} -c ${OUTPUT_CATALOG_PATH} -p ${PREVIOUS_PATCH_YAML_PATH} -s ${PREVIOUS_SINGLE_BUNDLE_PATH} -o ${OUTPUT_CATALOG_PATH} --push-pipeline-yaml-path ${PREVIOUS_PUSH_PIPELINE_PATH} --push-pipeline-operation enable
    
    
    #cat ${OUTPUT_CATALOG_PATH}
done < <(yq eval '.config.supported-ocp-versions.build[].name' $BUILD_CONFIG_PATH)
