import sys
import re
from datetime import datetime
from pathlib import Path
from jsonupdate_ng import jsonupdate_ng
import argparse
import yaml
from ruamel.yaml.scalarstring import DoubleQuotedScalarString
import json

from logger.logger import getLogger
from controller.quay_controller import quay_controller
import utils.util as util
import constants.constants as CONSTANTS

LOGGER = getLogger('processor')


class bundle_processor:

    def __init__(self, build_config_path:str, bundle_csv_path:str, patch_yaml_path:str, rhoai_version:str, output_file_path:str, annotation_yaml_path:str, push_pipeline_operation:str, push_pipeline_yaml_path:str, build_type:str):
        LOGGER.info("=============================================================================")
        LOGGER.info("Initializing Bundle Processor")
        LOGGER.info("=============================================================================")
        self.rhoai_version = rhoai_version
        self.build_type = build_type
        self.push_pipeline_operation = push_pipeline_operation
        self.build_config_path = build_config_path
        self.bundle_csv_path = bundle_csv_path
        self.patch_yaml_path = patch_yaml_path
        self.output_file_path = output_file_path
        self.annotation_yaml_path = annotation_yaml_path
        self.push_pipeline_yaml_path = push_pipeline_yaml_path
        self.build_args_file_path = f'{Path(self.patch_yaml_path).parent}/bundle_build_args.map'

        LOGGER.info(f"rhoai_version: {self.rhoai_version}")
        LOGGER.info(f"build_type: {self.build_type}")
        LOGGER.info(f"push_pipeline_operation: {self.push_pipeline_operation}")
        LOGGER.info(f"build_config_path: {self.build_config_path}")
        LOGGER.info(f"bundle_csv_path: {self.bundle_csv_path}")
        LOGGER.info(f"patch_yaml_path: {self.patch_yaml_path}")
        LOGGER.info(f"output_file_path: {self.output_file_path}")
        LOGGER.info(f"annotation_yaml_path: {self.annotation_yaml_path}")
        LOGGER.info(f"push_pipeline_yaml_path: {self.push_pipeline_yaml_path}")
        LOGGER.info(f"build_args_file_path: {self.build_args_file_path}")

        LOGGER.info("")
        LOGGER.info("Loading yaml files...")
        self.csv_dict = util.load_yaml_file(self.bundle_csv_path)
        self.patch_dict = util.load_yaml_file(self.patch_yaml_path, parser='pyyaml')
        self.build_config_dict = util.load_yaml_file(self.build_config_path, parser='pyyaml')
        self.annotation_dict = util.load_yaml_file(self.annotation_yaml_path, parser='pyyaml')
        self.push_pipeline_dict = util.load_yaml_file(self.push_pipeline_yaml_path)

        LOGGER.debug(f"csv_dict: {json.dumps(self.csv_dict, indent=4, default=str)}")
        LOGGER.debug(f"patch_dict: {json.dumps(self.patch_dict, indent=4, default=str)}")
        LOGGER.debug(f"build_config_dict: {json.dumps(self.build_config_dict, indent=4, default=str)}")
        LOGGER.debug(f"annotation_dict: {json.dumps(self.annotation_dict, indent=4, default=str)}")
        LOGGER.debug(f"push_pipeline_dict: {json.dumps(self.push_pipeline_dict, indent=4, default=str)}")
        LOGGER.info("All yaml files loaded successfully!")

    def patch_bundle_csv(self): 
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Filtering ODH Operator entry from bundle-patch...")
        LOGGER.info("=============================================================================")
        odh_operator_entry = util.filter_image_entries(
            image_entries=self.patch_dict['patch']['relatedImages'],
            include_filter=['ODH_OPERATOR']
        )
        
        # Validate exactly one ODH_OPERATOR entry exists.
        if len(odh_operator_entry) > 1:
            LOGGER.error(f"Found more than one ODH_OPERATOR entry in relatedImages: {json.dumps(odh_operator_entry, indent=4, default=str)}")
            sys.exit(1)
        elif len(odh_operator_entry) == 0:
            LOGGER.error("No ODH_OPERATOR entry found in relatedImages!")
            sys.exit(1)

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Querying Quay.io for latest ODH Operator image digest and git metadata...")
        LOGGER.info("=============================================================================")
         # Compute version tag based on build type
        version_tag = f'{self.rhoai_version}-nightly' if self.build_type.lower() == 'nightly' else self.rhoai_version
        if self.build_type.lower() == 'nightly':
            LOGGER.info(f"Build type is nightly, using image tag: {version_tag}")
        else:
            LOGGER.info(f"Using image tag: {version_tag}")

        # Fetch latest image and git metadata for ODH Operator (with GitHub override for upstream repos)
        self.operator_image_entry, self.operator_git_metadata = util.fetch_latest_images_and_git_metadata(
            image_entries=odh_operator_entry,
            image_tag=version_tag,
            use_github_override=True
        )

        # Extract operator git metadata. We already validate above that odh_operator_entry
        # has exactly one entry, and operator_git_metadata is derived from it via
        # fetch_latest_images_and_git_metadata(), so we can safely access [0] here.
        self.operator_name = list(self.operator_git_metadata["map"].keys())[0]
        self.operator_git_url = self.operator_git_metadata["map"][self.operator_name][CONSTANTS.GIT_URL_LABEL_KEY]
        self.operator_git_commit = self.operator_git_metadata["map"][self.operator_name][CONSTANTS.GIT_COMMIT_LABEL_KEY]
        LOGGER.info("Operator metadata:")
        LOGGER.info(f"  Operator name: {self.operator_name}")
        LOGGER.info(f"  Operator git URL: {self.operator_git_url}")
        LOGGER.info(f"  Operator git commit: {self.operator_git_commit}")
        
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Fetching operands-map.yaml and manifests-config.yaml from operator repo...")
        LOGGER.info("=============================================================================")
        LOGGER.info(f"Operator repo: {self.operator_git_url}")
        LOGGER.info(f"Commit: {self.operator_git_commit}")
        LOGGER.info(f"Operands map path: {CONSTANTS.OPERANDS_MAP_PATH}")
        LOGGER.info(f"Manifests config path: {CONSTANTS.MANIFESTS_CONFIG_PATH}")

        # Fetch and load operands-map.yaml
        operands_map_content = util.fetch_file_data_from_github(
            git_url=self.operator_git_url,
            git_commit=self.operator_git_commit,
            file_path=CONSTANTS.OPERANDS_MAP_PATH
        )
        self.operands_map_dict = yaml.safe_load(operands_map_content)
        LOGGER.debug(f"operands_map_dict: {json.dumps(self.operands_map_dict, indent=4, default=str)}")

        # Fetch and load manifests-config.yaml
        manifest_config_content = util.fetch_file_data_from_github(
            git_url=self.operator_git_url,
            git_commit=self.operator_git_commit,
            file_path=CONSTANTS.MANIFESTS_CONFIG_PATH
        )
        self.manifest_config_dict = yaml.safe_load(manifest_config_content)
        LOGGER.debug(f"manifest_config_dict: {json.dumps(self.manifest_config_dict, indent=4, default=str)}")
        LOGGER.info("Operands map and manifests config loaded successfully!")
        
        
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Generating operand image entries and bundle build args...")
        LOGGER.info("=============================================================================")
        self.operand_image_entries = self.operands_map_dict['relatedImages']
        self.bundle_build_args = self.generate_bundle_build_args()
        LOGGER.debug(f"operand_image_entries: {json.dumps(self.operand_image_entries, indent=4, default=str)}")
        LOGGER.debug(f"bundle_build_args: {json.dumps(self.bundle_build_args, indent=4, default=str)}")
        
        
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Applying registry and repo replacements for Operator and Operand Images...")
        LOGGER.info("=============================================================================")
        util.apply_registry_and_repo_replacements(
            image_entries=self.operator_image_entry,
            registry_mapping={self.build_config_dict['config']['replacements'][0]['registry']: CONSTANTS.PRODUCTION_REGISTRY},
            repo_mappings=self.build_config_dict['config']['replacements'][0]['repo_mappings']
        )
        
        util.apply_registry_and_repo_replacements(
            image_entries=self.operand_image_entries,
            registry_mapping={self.build_config_dict['config']['replacements'][0]['registry']: CONSTANTS.PRODUCTION_REGISTRY},
            repo_mappings=self.build_config_dict['config']['replacements'][0]['repo_mappings']
        )
        
        LOGGER.info("Dumping operator_image_entry and operand_image_entries after registry/repo replacements...")
        LOGGER.info(f"operator_image_entry: {json.dumps(self.operator_image_entry, indent=4, default=str)}")
        LOGGER.info(f"operand_image_entries: {json.dumps(self.operand_image_entries, indent=4, default=str)}")
        


        self.patch_csv_fields()
        self.patch_related_images()
        self.process_annotation_yaml()

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Processing push pipeline...")
        LOGGER.info("=============================================================================")
        self.is_push_pipeline_updated, self.push_pipeline_dict = util.process_push_pipeline(
            self.push_pipeline_dict, self.push_pipeline_operation
        )

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Writing output files...")
        LOGGER.info("=============================================================================")
        self.write_output_files()

    def generate_bundle_build_args(self):
        """
        Generates bundle build arguments from git metadata.

        Returns:
            String containing build arguments in KEY=VALUE format
        """
        bundle_build_args = ""
        for component, git_meta in {**self.operator_git_metadata['map'], **self.manifest_config_dict['map'], **self.manifest_config_dict['additional_meta']}.items():
            if 'ref_type' not in git_meta:
                bundle_build_args += f'{component.replace("-", "_").upper()}_{CONSTANTS.GIT_URL_LABEL_KEY.replace(".", "_").upper()}={git_meta[CONSTANTS.GIT_URL_LABEL_KEY]}\n'
                bundle_build_args += f'{component.replace("-", "_").upper()}_{CONSTANTS.GIT_COMMIT_LABEL_KEY.replace(".", "_").upper()}={git_meta[CONSTANTS.GIT_COMMIT_LABEL_KEY]}\n'

        return bundle_build_args

    def patch_csv_fields(self):
        """
        Patches CSV fields including operator image, version, and custom fields from csv-patch file.
        """
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Patching CSV fields...")
        LOGGER.info("=============================================================================")

        # Extract operator image after replacement (now has registry.redhat.io URL)
        # We already validate above that odh_operator_entry has exactly one entry,
        # and operator_image_entry is derived from it via fetch_latest_images_and_git_metadata(),
        # so we can safely access [0] here.
        self.operator_image = self.operator_image_entry[0]['value']

        # Set operator container image in annotations and deployment spec
        LOGGER.info("Updating operator container image...")
        self.csv_dict['metadata']['annotations']['containerImage'] = DoubleQuotedScalarString(self.operator_image)
        self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0][
            'image'] = DoubleQuotedScalarString(self.operator_image)
        LOGGER.info(f"  containerImage: {self.operator_image}")

        LOGGER.info("Updating version and metadata fields...")
        created_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        version = self.patch_dict["patch"]["version"]
        csv_name = f'{CONSTANTS.OPERATOR_NAME}.{version}'
        self.csv_dict['metadata']['annotations']['createdAt'] = created_at
        self.csv_dict['metadata']['name'] = csv_name
        self.csv_dict['spec']['version'] = DoubleQuotedScalarString(version)
        LOGGER.info(f"  createdAt: {created_at}")
        LOGGER.info(f"  metadata.name: {csv_name}")
        LOGGER.info(f"  spec.version: {version}")

        LOGGER.info("Removing olm.skipRange and replaces fields if present...")
        removed_skip_range = self.csv_dict['metadata']['annotations'].pop('olm.skipRange', None)
        removed_replaces = self.csv_dict['spec'].pop('replaces', None)
        if removed_skip_range:
            LOGGER.info(f"  Removed olm.skipRange: {removed_skip_range} field")
        if removed_replaces:
            LOGGER.info(f"  Removed replaces: {removed_replaces} field")

        LOGGER.info("Applying CSV patches...")
        csv_patch_file = self.patch_dict['patch']['additional-fields']['file']
        csv_patch_path = f'{Path(self.patch_yaml_path).parent.absolute()}/{csv_patch_file}'
        LOGGER.info(f"  Loading csv-patch file: {csv_patch_file}")
        csv_patch_dict = util.load_yaml_file(csv_patch_path, parser='pyyaml')
        LOGGER.debug(f"  csv_patch_dict: {json.dumps(csv_patch_dict, indent=4, default=str)}")
        self.csv_dict = jsonupdate_ng.updateJson(self.csv_dict, csv_patch_dict)
        LOGGER.info("  CSV patches applied successfully!")


    def process_annotation_yaml(self):
        """
        Processes annotation.yaml by removing channel-related annotations.
        
        These annotations are no longer required after migrating to File Based Catalogs (FBC).
        """
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Processing annotation.yaml...")
        LOGGER.info("=============================================================================")
        
        # Remove channel annotations (no longer required in File Based Catalogs)
        LOGGER.info("Removing channel annotations (not required in FBC)...")
        removed_channels = self.annotation_dict['annotations'].pop('operators.operatorframework.io.bundle.channels.v1', None)
        removed_default = self.annotation_dict['annotations'].pop('operators.operatorframework.io.bundle.channel.default.v1', None)
        
        if removed_channels:
            LOGGER.info(f"  Removed bundle.channels.v1: {removed_channels}")
        if removed_default:
            LOGGER.info(f"  Removed bundle.channel.default.v1: {removed_default}")
        
        LOGGER.info("  Annotation processing complete!")

    def write_output_files(self):
        """
        Writes all modified files to disk.
        """
        util.write_yaml_file(self.csv_dict, self.output_file_path)
        util.write_yaml_file(self.annotation_dict, self.annotation_yaml_path)

        util.write_file(self.bundle_build_args, self.build_args_file_path)

        if self.is_push_pipeline_updated:
            util.write_yaml_file(self.push_pipeline_dict, self.push_pipeline_yaml_path)

        LOGGER.info("  Output files written successfully!")

    def patch_related_images(self):
        """
        Patches CSV's deployment env vars and builds spec.relatedImages list for disconnected installations.
        """
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Patching related images in CSV...")
        LOGGER.info("=============================================================================")
        LOGGER.info("Updating deployment env vars...")
        # Get existing env vars from the deployment container
        existing_env_list = self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]['env']
        existing_env_list = util.to_plain_dict(existing_env_list)
        LOGGER.debug(f"  Existing deployment env vars: {json.dumps(existing_env_list, indent=4, default=str)}")

        # Load and merge additional images if defined in patch config
        if 'additional-related-images' in self.patch_dict['patch']:
            additional_images_file = self.patch_dict['patch']['additional-related-images']['file']
            additional_images_path = f'{Path(self.patch_yaml_path).parent.absolute()}/{additional_images_file}'
            LOGGER.info(f"  Adding additional images from: {additional_images_file}")
            
            additional_images_dict = util.load_yaml_file(additional_images_path, parser='pyyaml')
            
            # Drop tags from image values and deduplicate by name.
            # Later entries with the same name will overwrite earlier ones.
            additional_images = list({
                image["name"]: {
                    "name": image["name"],
                    "value": re.sub(r':[^\s:@]+@', '@', image["value"])
                }
                for image in additional_images_dict['additionalImages']
            }.values())
            LOGGER.debug(f"  additional_images: {json.dumps(additional_images, indent=4, default=str)}")

            updated_env_list = self.operand_image_entries + additional_images
        else:
            updated_env_list = self.operand_image_entries

        LOGGER.debug(f"  updated_env_list: {json.dumps(updated_env_list, indent=4, default=str)}")

        # Merge image entries into deployment env vars using jsonupdate_ng
        # Uses 'name' as key for matching/merging list items
        env_object = jsonupdate_ng.updateJson(
            {'env': existing_env_list},
            {'env': updated_env_list},
            meta={'listPatchScheme': {'$.env': {'key': 'name'}}}
        )
        self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]['env'] = env_object['env']
        LOGGER.info("  Deployment env vars updated successfully!")
        LOGGER.debug(f"  Updated deployment env vars: {json.dumps(env_object['env'], indent=4, default=str)}")

        # Build spec.relatedImages list for OLM
        LOGGER.info("Updating spec.relatedImages list...")
        relatedImages = []
        
        # Add images from annotations (registry.redhat.io with digest)
        LOGGER.info("  Adding entries from annotations...")
        for name, value in self.csv_dict['metadata']['annotations'].items():
            if value.startswith(CONSTANTS.PRODUCTION_REGISTRY) and '@sha256:' in value:
                image_name = f'{value.split("/")[-1].replace("@sha256:", "-")}-annotation'
                entry = {'name': image_name, 'image': value}
                relatedImages.append(entry)
                LOGGER.debug(entry)

        # Add updated env list images with transformed names (RELATED_IMAGE_X_IMAGE -> x_image)
        LOGGER.info("  Adding entries from deployment env vars...")
        for image in updated_env_list:
            entry = {'name': image['name'].replace('RELATED_IMAGE_', '').lower(), 'image': image['value']}
            relatedImages.append(entry)
            LOGGER.debug(entry)
        
        self.csv_dict['spec']['relatedImages'] = relatedImages
        LOGGER.info("  spec.relatedImages updated successfully!")
        LOGGER.debug(f"  Updated relatedImages: {json.dumps(relatedImages, indent=4, default=str)}")

    def get_all_latest_images(self):
        latest_images = []
        qc = quay_controller('rhoai')
        for registry_entry in self.build_config_dict['config']['replacements']:
            registry = registry_entry['registry']
            for repo_path in registry_entry['repo_mappings']:
                repo = '/'.join(repo_path.split('/')[1:])
                tags = qc.get_all_tags(repo, self.rhoai_version)
                if not tags:
                    LOGGER.warning(f'no tags found for {repo}')
                for tag in tags:
                    sig_tag = f'{tag["manifest_digest"].replace(":", "-")}.sig'
                    signature = qc.get_tag_details(repo, sig_tag)
                    if signature:
                        latest_images.append({'name': f'RELATED_IMAGE_{repo.replace("-rhel8", "").replace("-rhel9", "").replace("-", "_").upper()}_IMAGE', 'value': DoubleQuotedScalarString(f'{registry}/{repo_path}@{tag["manifest_digest"]}')})
                        break
        LOGGER.info(f'latest_images: {json.dumps(latest_images, indent=4)}')
        return latest_images



class snapshot_processor:
    def __init__(self, snapshot_json_path:str, output_file_path:str, image_filter:str=''):
        self.snapshot_json_path = snapshot_json_path
        self.output_file_path = output_file_path
        self.image_filter = image_filter

    def extract_images_from_snapshot(self):
        snapshot = json.load(open(self.snapshot_json_path))
        output_images = []
        for component in snapshot['spec']['components']:
            if 'bundle' not in component['name'] and 'fbc' not in component['name'] and 'odh-operator' not in component['name']:
                output_images.append({'name': f'RELATED_IMAGE_{component["name"].upper().split("-V2")[0].replace("-", "_")}_IMAGE', 'value': DoubleQuotedScalarString(component["containerImage"])})

        return output_images


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-op', '--operation', required=False,
                        help='Operation code, supported values are "bundle-patch"', dest='operation')
    parser.add_argument('-b', '--build-config-path', required=False,
                        help='Path of the build-config.yaml', dest='build_config_path')
    parser.add_argument('-t', '--build-type', required=False,
                        help='Path of the build-config.yaml', dest='build_type', default='ci')
    parser.add_argument('-c', '--bundle-csv-path', required=False,
                        help='Path of the bundle csv yaml from the release branch.', dest='bundle_csv_path')
    parser.add_argument('-p', '--patch-yaml-path', required=False,
                        help='Path of the bundle-patch.yaml from the release branch.', dest='patch_yaml_path')
    parser.add_argument('-o', '--output-file-path', required=False,
                        help='Path of the output bundle csv', dest='output_file_path')
    parser.add_argument('-sn', '--snapshot-json-path', required=False,
                        help='Path of the single-bundle generated using the opm.', dest='snapshot_json_path')
    parser.add_argument('-f', '--image-filter', required=False,
                        help='Path of the single-bundle generated using the opm.', dest='image_filter')
    parser.add_argument('-v', '--rhoai-version', required=False,
                        help='The version of Openshift-AI being processed', dest='rhoai_version')
    parser.add_argument('-a', '--annotation-yaml-path', required=False,
                        help='Path of the annotation.yaml from the raw inputs', dest='annotation_yaml_path')
    parser.add_argument('-y', '--push-pipeline-yaml-path', required=False,
                        help='Path of the tekton pipeline for push builds', dest='push_pipeline_yaml_path')
    parser.add_argument('-x', '--push-pipeline-operation', required=False, default="enable",
                        help='Operation code, supported values are "enable" and "disable"', dest='push_pipeline_operation')
    args = parser.parse_args()

    if args.operation.lower() == 'bundle-patch':
        processor = bundle_processor(build_config_path=args.build_config_path, bundle_csv_path=args.bundle_csv_path, patch_yaml_path=args.patch_yaml_path, rhoai_version=args.rhoai_version, output_file_path=args.output_file_path, annotation_yaml_path=args.annotation_yaml_path, push_pipeline_operation=args.push_pipeline_operation, push_pipeline_yaml_path=args.push_pipeline_yaml_path, build_type=args.build_type)
        processor.patch_bundle_csv()

    # build_config_path = '/home/dchouras/RHODS/DevOps/RHOAI-Build-Config/config/build-config.yaml'
    # bundle_csv_path = '/home/dchouras/RHODS/DevOps/RHOAI-Build-Config/to-be-processed/bundle/manifests/rhods-operator.clusterserviceversion.yaml'
    # patch_yaml_path = '/home/dchouras/RHODS/DevOps/RHOAI-Build-Config/bundle/bundle-patch.yaml'
    # annotation_yaml_path = '/home/dchouras/RHODS/DevOps/RHOAI-Build-Config/to-be-processed/bundle/metadata/annotations.yaml'
    # output_file_path = 'output.yaml'
    # rhoai_version = 'rhoai-2.13'
    # push_pipeline_operation = 'enable'
    # push_pipeline_yaml_path = '/home/dchouras/RHODS/DevOps/RHOAI-Build-Config/.tekton/odh-operator-bundle-v2-13-push.yaml'
    #
    #
    # processor = bundle_processor(build_config_path=build_config_path, bundle_csv_path=bundle_csv_path,
    #                              patch_yaml_path=patch_yaml_path, rhoai_version=rhoai_version,
    #                              output_file_path=output_file_path, annotation_yaml_path=annotation_yaml_path,
    #                              push_pipeline_yaml_path=push_pipeline_yaml_path, push_pipeline_operation=push_pipeline_operation)
    # processor.patch_bundle_csv()


