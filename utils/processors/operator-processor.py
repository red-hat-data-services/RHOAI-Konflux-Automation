import sys
import json
import copy
import yaml
from jsonupdate_ng import jsonupdate_ng
from ruamel.yaml.scalarstring import DoubleQuotedScalarString
import argparse
from logger.logger import getLogger
import utils.util as util
import constants.constants as CONSTANTS

LOGGER = getLogger('processor')

class operator_processor:

    def __init__(self, patch_yaml_path:str, rhoai_version:str, operands_map_path:str, nudging_yaml_path:str, manifest_config_path:str, push_pipeline_operation:str, push_pipeline_yaml_path:str, enable_modular_processing:bool=False):
        LOGGER.info("=============================================================================")
        LOGGER.info("Initializing Operator Processor")
        LOGGER.info("=============================================================================")
        self.patch_yaml_path = patch_yaml_path
        self.operands_map_path = operands_map_path
        self.nudging_yaml_path = nudging_yaml_path
        self.manifest_config_path = manifest_config_path
        self.rhoai_version = rhoai_version
        self.push_pipeline_operation = push_pipeline_operation
        self.push_pipeline_yaml_path = push_pipeline_yaml_path
        self.enable_modular_processing = enable_modular_processing
        LOGGER.info(f"rhoai_version: {self.rhoai_version}")
        LOGGER.info(f"push_pipeline_operation: {self.push_pipeline_operation}")
        LOGGER.info(f"enable_modular_processing: {self.enable_modular_processing}")
        LOGGER.info(f"patch_yaml_path: {self.patch_yaml_path}")
        LOGGER.info(f"operands_map_path: {self.operands_map_path}")
        LOGGER.info(f"nudging_yaml_path: {self.nudging_yaml_path}")
        LOGGER.info(f"manifest_config_path: {self.manifest_config_path}")
        LOGGER.info(f"push_pipeline_yaml_path: {self.push_pipeline_yaml_path}")

        LOGGER.info("")
        LOGGER.info("Loading yaml files...")
        self.patch_dict = util.load_yaml_file(self.patch_yaml_path, parser='pyyaml')
        self.operands_map_dict = util.load_yaml_file(self.operands_map_path)
        self.nudging_yaml_dict = util.load_yaml_file(self.nudging_yaml_path)
        self.manifest_config_dict = util.load_yaml_file(self.manifest_config_path)
        self.push_pipeline_dict = util.load_yaml_file(self.push_pipeline_yaml_path)

        # Log debug info for loaded dictionaries after YAML parsing
        LOGGER.debug(f'Patch dictionary: {json.dumps(self.patch_dict, indent=4)}')
        LOGGER.debug(f"operands_map_dict: {json.dumps(self.operands_map_dict, indent=4, default=str)}")
        LOGGER.debug(f"nudging_yaml_dict: {json.dumps(self.nudging_yaml_dict, indent=4, default=str)}")
        LOGGER.debug(f"manifest_config_dict: {json.dumps(self.manifest_config_dict, indent=4, default=str)}")
        LOGGER.debug(f"push_pipeline_dict: {json.dumps(self.push_pipeline_dict, indent=4, default=str)}")
        LOGGER.info("All yaml files loaded successfully!")

    def process(self):
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Syncing relatedImages list from Bundle Patch...")
        LOGGER.info("=============================================================================")
        self.sync_yamls_from_bundle_patch()

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Querying Quay.io for latest image digest and git metadata...")
        LOGGER.info("=============================================================================")
        self.latest_images, self.git_labels_meta = util.fetch_latest_images_and_git_metadata(
            self.operands_map_dict['relatedImages'], self.rhoai_version
        )

        if self.enable_modular_processing:
            LOGGER.info("")
            LOGGER.info("=============================================================================")
            LOGGER.info("Processing modular operators...")
            LOGGER.info("=============================================================================")
            self.process_modular_operators()
        else:
            LOGGER.info("")
            LOGGER.info("Modular operator processing is disabled. Skipping.")

        if self.latest_images:
            LOGGER.info("")
            LOGGER.info("=============================================================================")
            LOGGER.info("Updating operands-map.yaml with latest image digests...")
            LOGGER.info("=============================================================================")
            self.update_operands_map()

        if self.git_labels_meta:
            LOGGER.info("")
            LOGGER.info("=============================================================================")
            LOGGER.info("Updating manifest-config.yaml with git metadata...")
            LOGGER.info("=============================================================================")
            self.update_manifest_config()

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Processing push pipeline...")
        LOGGER.info("=============================================================================")
        self.is_push_pipeline_updated, self.push_pipeline_dict = util.process_push_pipeline(self.push_pipeline_dict, self.push_pipeline_operation)


        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Writing output files...")
        LOGGER.info("=============================================================================")
        self.write_output_files()


    def write_output_files(self):
        util.write_yaml_file(self.nudging_yaml_dict, self.nudging_yaml_path)
        util.write_yaml_file(self.operands_map_dict, self.operands_map_path)
        util.write_yaml_file(self.manifest_config_dict, self.manifest_config_path)

        if self.is_push_pipeline_updated:
            util.write_yaml_file(self.push_pipeline_dict, self.push_pipeline_yaml_path)

        LOGGER.info("Output files written successfully")

    def update_operands_map(self):
        """
        Updates operands_map_dict with the latest image digests for all relatedImages.
        """
        LOGGER.debug(f"Latest images: {json.dumps(self.latest_images, indent=4, default=str)}")
        LOGGER.debug(f"Operands map dict: {json.dumps(self.operands_map_dict, indent=4, default=str)}")
        self.operands_map_dict = jsonupdate_ng.updateJson(self.operands_map_dict, {'relatedImages': self.latest_images }, meta={'listPatchScheme': {'$.relatedImages': {'key': 'name'}}} )

        LOGGER.info("Operands Map updated successfully")
        LOGGER.debug(f"Updated Operands Map: {json.dumps(self.operands_map_dict, indent=4, default=str)}")

    def update_manifest_config(self):
        """
        Updates manifest_config_dict with git metadata.
        Exits if required components or git labels are missing.
        """
        missing_git_labels = []
        missing_components = []

        errors = self.git_labels_meta.get('errors', {})

        LOGGER.info("Updating git metadata for operator components...")
        for component, manifest_config in self.manifest_config_dict['map'].items():
            # Skip branch-based components
            if manifest_config.get('ref_type') == 'branch':
                LOGGER.info(f"  Skipping component '{component}' (ref_type=branch)")
                continue

            # Check if component exists in git_labels_meta
            if component not in self.git_labels_meta['map']:
                missing_components.append(component)
                error_reason = errors.get(component, 'unknown')
                if error_reason == 'missing_signature':
                    LOGGER.warning(f"  Component '{component}' skipped: image is not signed with cosign.")
                elif error_reason == 'missing_image':
                    LOGGER.warning(f"  Component '{component}' skipped: image tag not found in registry.")
                else:
                    LOGGER.warning(f"  Component '{component}' not found in git_labels_meta.")
                continue

            # Get git metadata
            git_meta = self.git_labels_meta['map'][component]
            git_url = git_meta.get(CONSTANTS.GIT_URL_LABEL_KEY, '')
            git_commit = git_meta.get(CONSTANTS.GIT_COMMIT_LABEL_KEY, '')

            # Update if both are present
            if git_url and git_commit:
                manifest_config[CONSTANTS.GIT_URL_LABEL_KEY] = git_url
                manifest_config[CONSTANTS.GIT_COMMIT_LABEL_KEY] = git_commit
                LOGGER.info(f"  Metadata for component '{component}' updated successfully.")
            else:
                missing_git_labels.append(component)
                LOGGER.warning(f"  Component '{component}' has image with missing git.url/git.commit labels.")

        LOGGER.info("Adding git metadata for operator components...")
        self.manifest_config_dict['additional_meta'] = {}
        for component, git_meta in self.git_labels_meta['map'].items():
            if component not in self.manifest_config_dict['map']:
                self.manifest_config_dict['additional_meta'][component] = git_meta
                LOGGER.info(f"  Metadata for component '{component}' added successfully.")

        # Error handling
        if missing_components:
            unsigned = [comp for comp in missing_components if errors.get(comp) == 'missing_signature']
            not_found = [comp for comp in missing_components if errors.get(comp) == 'missing_image']
            other = [comp for comp in missing_components if errors.get(comp) not in ('missing_signature', 'missing_image')]

            if unsigned:
                LOGGER.error(f"Cosign signature missing for {len(unsigned)} component(s): {unsigned}")
                LOGGER.error("Sign these images with cosign so the processor can verify and extract labels.")
            if not_found:
                LOGGER.error(f"Image tag not found for {len(not_found)} component(s): {not_found}")
                LOGGER.error("Verify that these images exist in the registry with the expected tag.")
            if other:
                LOGGER.error(f"No git metadata found for {len(other)} component(s): {other}")
                LOGGER.error("Check that component names in manifests-config.yaml match the image names in bundle-patch.yaml.")
            sys.exit(1)

        if missing_git_labels:
            LOGGER.error(f"git.url and/or git.commit labels missing in image metadata for {len(missing_git_labels)} component(s): {missing_git_labels}")
            LOGGER.error("Rebuild these images with git.url and git.commit labels.")
            sys.exit(1)

    def process_modular_operators(self):
        """
        Processes modular operators in three phases:
        1. Fetch: retrieve manifests-config.yaml from each modular operator's repo
        2. Conflict detection: ensure no image is claimed by multiple modular operators
        3. Apply: override image digests and git metadata for each sub-component

        Components with ref_type=branch are skipped (no image to override).
        """
        modular_components = {
            name: config for name, config in self.manifest_config_dict['map'].items()
            if config.get('is_modular')
        }

        if not modular_components:
            LOGGER.info("No modular operators found. Skipping.")
            return

        LOGGER.info(f"Found {len(modular_components)} modular operator(s): {list(modular_components.keys())}")

        errors = {
            'invalid_modular_config': [],
            'not_in_operands_map': [],
            'fetch_failed': {},
            'invalid_remote_config': [],
            'conflicts': {},
            'missing_fields': {},
        }

        invalid = [name for name, config in modular_components.items() if config.get('ref_type') == 'branch']
        if invalid:
            errors['invalid_modular_config'] = invalid
            for name in invalid:
                del modular_components[name]

        remote_configs = self._fetch_remote_configs(modular_components, errors)

        conflicts = self._detect_image_conflicts(remote_configs)
        if conflicts:
            errors['conflicts'] = conflicts
        else:
            for component_name, remote_config in remote_configs.items():
                self._apply_modular_overrides(component_name, remote_config, errors)

        self._report_modular_errors(errors)

    def _fetch_remote_configs(self, modular_components, errors):
        """Fetch and validate manifests-config.yaml from each modular operator's repo."""
        remote_configs = {}

        for component_name in modular_components:
            LOGGER.info(f"  Processing modular operator: {component_name}")

            modular_git_meta = self.git_labels_meta['map'].get(component_name)
            if not modular_git_meta:
                errors['not_in_operands_map'].append(component_name)
                continue

            git_url = modular_git_meta[CONSTANTS.GIT_URL_LABEL_KEY]
            git_commit = modular_git_meta[CONSTANTS.GIT_COMMIT_LABEL_KEY]

            LOGGER.info(f"  Fetching {CONSTANTS.MANIFESTS_CONFIG_PATH} from {git_url} @ {git_commit}")
            try:
                remote_content = util.fetch_file_data_from_github(git_url, git_commit, CONSTANTS.MANIFESTS_CONFIG_PATH)
            except Exception as e:
                errors['fetch_failed'][component_name] = str(e)
                continue

            remote_config = yaml.safe_load(remote_content)
            LOGGER.debug(f"  Remote manifests-config: {json.dumps(remote_config, indent=4, default=str)}")

            if not remote_config or 'map' not in remote_config:
                errors['invalid_remote_config'].append(component_name)
                continue

            remote_configs[component_name] = remote_config

        return remote_configs

    def _detect_image_conflicts(self, remote_configs):
        """Check that no image is claimed by multiple modular operators. Returns {image: [owners]} for conflicts."""
        image_ownership = {}
        for component_name, remote_config in remote_configs.items():
            for sub_component, sub_config in remote_config['map'].items():
                if sub_config.get('ref_type') == 'branch':
                    continue
                image_value = sub_config.get('image', '')
                if not image_value:
                    continue
                parsed = util.parse_image_value(image_value)
                image_ownership.setdefault(parsed['component_name'], []).append(component_name)

        return {image: owners for image, owners in image_ownership.items() if len(owners) > 1}

    def _apply_modular_overrides(self, component_name, remote_config, errors):
        """Apply image digest and git metadata overrides from a single modular operator's config."""
        override_count = 0

        for sub_component, sub_config in remote_config['map'].items():
            if sub_config.get('ref_type') == 'branch':
                LOGGER.info(f"    Skipping sub-component '{sub_component}' (ref_type=branch)")
                continue

            missing_fields = [f for f in ['image', CONSTANTS.GIT_URL_LABEL_KEY, CONSTANTS.GIT_COMMIT_LABEL_KEY] if not sub_config.get(f)]
            if missing_fields:
                errors['missing_fields'][f"{component_name} -> {sub_component}"] = missing_fields
                continue

            image_value = sub_config['image']
            parsed = util.parse_image_value(image_value)
            matched_component = parsed['component_name']

            LOGGER.info(f"    Overriding '{matched_component}' from modular operator '{component_name}'")

            matched = False
            for img_entry in self.latest_images:
                entry_parsed = util.parse_image_value(str(img_entry['value']))
                if entry_parsed['component_name'] == matched_component:
                    LOGGER.debug(f"      Image digest: {entry_parsed['digest']} -> {parsed['digest']}")
                    img_entry['value'] = DoubleQuotedScalarString(image_value)
                    matched = True
                    break

            if not matched:
                errors['not_in_operands_map'].append(matched_component)
                continue

            old_meta = self.git_labels_meta['map'].get(matched_component, {})
            LOGGER.debug(f"      git.url: {old_meta.get(CONSTANTS.GIT_URL_LABEL_KEY, '')} -> {sub_config[CONSTANTS.GIT_URL_LABEL_KEY]}")
            LOGGER.debug(f"      git.commit: {old_meta.get(CONSTANTS.GIT_COMMIT_LABEL_KEY, '')} -> {sub_config[CONSTANTS.GIT_COMMIT_LABEL_KEY]}")
            self.git_labels_meta['map'][matched_component] = {
                CONSTANTS.GIT_URL_LABEL_KEY: sub_config[CONSTANTS.GIT_URL_LABEL_KEY],
                CONSTANTS.GIT_COMMIT_LABEL_KEY: sub_config[CONSTANTS.GIT_COMMIT_LABEL_KEY]
            }

            override_count += 1

        LOGGER.info(f"  Modular operator '{component_name}': {override_count} component(s) overridden")

    def _report_modular_errors(self, errors):
        """Report grouped errors from modular operator processing. Exits if any errors found."""
        has_errors = False

        if errors['invalid_modular_config']:
            has_errors = True
            LOGGER.error(f"Component(s) have both is_modular=true and ref_type=branch, which is invalid: {errors['invalid_modular_config']}. "
                         f"Modular operators require an image in Quay to resolve git metadata. Remove is_modular or change ref_type to image.")

        if errors['not_in_operands_map']:
            has_errors = True
            LOGGER.error(f"Component(s) not found in operands-map relatedImages: {errors['not_in_operands_map']}. "
                         f"Ensure these are registered in bundle-patch.yaml.")

        if errors['fetch_failed']:
            has_errors = True
            LOGGER.error(f"Failed to fetch manifests-config.yaml for {len(errors['fetch_failed'])} modular operator(s):")
            for component, reason in errors['fetch_failed'].items():
                LOGGER.error(f"  - {component}: {reason}")

        if errors['invalid_remote_config']:
            has_errors = True
            LOGGER.error(f"Remote manifests-config.yaml is empty or missing 'map' section for: {errors['invalid_remote_config']}")

        if errors['conflicts']:
            has_errors = True
            LOGGER.error("Image conflict(s) detected across modular operators. Each image must be owned by exactly one modular operator:")
            for image, owners in errors['conflicts'].items():
                LOGGER.error(f"  - Image '{image}' is claimed by: {owners}")

        if errors['missing_fields']:
            has_errors = True
            LOGGER.error(f"Required field(s) missing in {len(errors['missing_fields'])} sub-component(s):")
            for key, fields in errors['missing_fields'].items():
                LOGGER.error(f"  - {key}: {fields}")

        if has_errors:
            sys.exit(1)

    def sync_yamls_from_bundle_patch(self):
        """
        Syncs the relatedImages sections of the operands map and nudging YAML files
        with the list of relatedImages from the bundle patch YAML. This ensures that
        newer components are added and offboarded components are automatically removed.

        Steps:
            1. Reads the list of relatedImages from bundle-patch.yaml.
            2. Sorts and Deduplicates entries by component name, keeping the first occurrence.
            3. Filters out FBC, BUNDLE, and ODH_OPERATOR images
            4. Syncs operands-map and nudging YAML
        """
        # Get latest list of components from patch_dict
        source_images = self.patch_dict['patch']['relatedImages']

        # Deduplicate (keep first occurrence) and sort alphabetically
        deduplicated_images = util.deduplicate_and_sort(source_images, key='name', sort=True)

        # Filter out FBC, BUNDLE, and ODH_OPERATOR images
        filtered_images = util.filter_image_entries(
            image_entries=deduplicated_images,
            exclude_filter=['FBC', 'BUNDLE', 'ODH_OPERATOR']
        )

        # Sync operands-map: The relatedImages list is completely replaced with the filtered_images
        # Uses deepcopy to avoid shared references
        self.operands_map_dict['relatedImages'] = copy.deepcopy(filtered_images)
        LOGGER.info("Operands Map synced successfully")
        LOGGER.debug(f"Synced Operands Map: {json.dumps(self.operands_map_dict, indent=4, default=str)}")

        # Sync nudging YAML: The relatedImages list is merged with deduplicated, sorted and filtered bundle-patch entries(filtered_images).
        # Components are matched by 'name' field:
        # - If component exists in both files: 'value' is preserved from nudging YAML
        # - If component exists only in bundle-patch: added with bundle-patch values
        # - If component exists only in nudging YAML: removed (no longer needed)

        # Capture component names from filtered list BEFORE merge
        bundle_patch_component_names = {entry['name'] for entry in filtered_images}

        # Convert to plain Python dicts before merge (ruamel.yaml CommentedMap/CommentedSeq
        # causes jsonupdate_ng to lose entries during merge)
        merged_nudging = jsonupdate_ng.updateJson(
            {'relatedImages': util.to_plain_dict(filtered_images)},
            util.to_plain_dict(self.nudging_yaml_dict),
            meta={'listPatchScheme': {'$.relatedImages': {'key': 'name'}}}
        )

        # Filter to only keep components that exist in bundle-patch
        self.nudging_yaml_dict['relatedImages'] = [
            entry for entry in merged_nudging['relatedImages']
            if entry['name'] in bundle_patch_component_names
        ]
        LOGGER.info("Nudging YAML synced successfully")
        LOGGER.debug(f"Synced Nudging YAML: {json.dumps(self.nudging_yaml_dict, indent=4, default=str)}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-op', '--operation', required=False,
                        help='Operation code, supported values are "process-operator-yamls"', dest='operation')
    parser.add_argument('-p', '--patch-yaml-path', required=False,
                        help='Path of the bundle-patch.yaml from the release branch.', dest='patch_yaml_path')
    parser.add_argument('-o', '--operands-map-path', required=False,
                        help='Path of the operands map yaml', dest='operands_map_path')
    parser.add_argument('-n', '--nudging-yaml-path', required=False,
                        help='Path of the nudging yaml', dest='nudging_yaml_path')
    parser.add_argument('-m', '--manifest-config-path', required=False,
                        help='Path of the manifest config yaml', dest='manifest_config_path')
    parser.add_argument('-v', '--rhoai-version', required=False,
                        help='The version of Openshift-AI being processed', dest='rhoai_version')
    parser.add_argument('-y', '--push-pipeline-yaml-path', required=False,
                        help='Path of the tekton pipeline for push builds', dest='push_pipeline_yaml_path')
    parser.add_argument('-x', '--push-pipeline-operation', required=False, default="enable",
                        help='Operation code, supported values are "enable" and "disable"', dest='push_pipeline_operation')
    parser.add_argument('--enable-modular-processing', required=False, action='store_true', default=False,
                        help='Enable modular operator processing to fetch and override image digests and git metadata from modular operator repos', dest='enable_modular_processing')
    args = parser.parse_args()

    if args.operation.lower() == 'process-operator-yamls':
        processor = operator_processor(patch_yaml_path=args.patch_yaml_path, rhoai_version=args.rhoai_version, operands_map_path=args.operands_map_path, nudging_yaml_path=args.nudging_yaml_path, manifest_config_path=args.manifest_config_path, push_pipeline_operation=args.push_pipeline_operation, push_pipeline_yaml_path=args.push_pipeline_yaml_path, enable_modular_processing=args.enable_modular_processing)
        processor.process()

    # patch_yaml_path = '/home/dchouras/RHODS/DevOps/RHOAI-Build-Config/bundle/bundle-patch.yaml'
    # operands_map_path = '/home/dchouras/RHODS/DevOps/rhods-operator/build/operands-map.yaml'
    # nudging_yaml_path = '/home/dchouras/RHODS/DevOps/rhods-operator/build/operator-nudging.yaml'
    # manifest_config_path = '/home/dchouras/RHODS/DevOps/rhods-operator/build/manifests-config.yaml'
    # rhoai_version = 'rhoai-2.13'
    # push_pipeline_operation = 'enable'
    # push_pipeline_yaml_path = '/home/dchouras/RHODS/DevOps/rhods-operator/.tekton/odh-operator-v2-13-push.yaml'
    #
    #
    # processor = operator_processor(patch_yaml_path=patch_yaml_path, rhoai_version=rhoai_version,
    #                                operands_map_path=operands_map_path, nudging_yaml_path=nudging_yaml_path,
    #                                manifest_config_path=manifest_config_path,
    #                              push_pipeline_yaml_path=push_pipeline_yaml_path, push_pipeline_operation=push_pipeline_operation)
    # processor.process()
