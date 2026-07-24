import sys
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from jsonupdate_ng import jsonupdate_ng
import argparse
import yaml
from ruamel.yaml.scalarstring import DoubleQuotedScalarString
import json

from logger.logger import getLogger
import utils.util as util
import constants.constants as CONSTANTS
from utils.sbom import get_package_info

LOGGER = getLogger('processor')


class bundle_processor:

    def __init__(self, build_config_path:str, bundle_csv_path:str, patch_yaml_path:str, rhoai_version:str, output_file_path:str, annotation_yaml_path:str, push_pipeline_operation:str, push_pipeline_yaml_path:str, build_type:str, xks_helm_patch_yaml_path:str=None, xks_helm_values_yaml_path:str=None, xks_helm_push_pipeline_yaml_path:str=None, openshift_helm_patch_yaml_path:str=None, openshift_helm_values_yaml_path:str=None, openshift_helm_push_pipeline_yaml_path:str=None, metadata_config_yaml_path:str=None, use_existing_digests:bool=False):
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
        self.xks_helm_patch_yaml_path = xks_helm_patch_yaml_path
        self.xks_helm_values_yaml_path = xks_helm_values_yaml_path
        self.xks_helm_push_pipeline_yaml_path = xks_helm_push_pipeline_yaml_path
        self.openshift_helm_patch_yaml_path = openshift_helm_patch_yaml_path
        self.openshift_helm_values_yaml_path = openshift_helm_values_yaml_path
        self.openshift_helm_push_pipeline_yaml_path = openshift_helm_push_pipeline_yaml_path
        self.xks_helm_chart_yaml_path = str(Path(xks_helm_values_yaml_path).parent / 'Chart.yaml') if xks_helm_values_yaml_path else None
        self.openshift_helm_chart_yaml_path = str(Path(openshift_helm_values_yaml_path).parent / 'Chart.yaml') if openshift_helm_values_yaml_path else None
        self.metadata_config_yaml_path = metadata_config_yaml_path
        self.use_existing_digests = use_existing_digests

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
        if self.xks_helm_patch_yaml_path:
            LOGGER.info(f"xks_helm_patch_yaml_path: {self.xks_helm_patch_yaml_path}")
        if self.xks_helm_values_yaml_path:
            LOGGER.info(f"xks_helm_values_yaml_path: {self.xks_helm_values_yaml_path}")
        if self.xks_helm_push_pipeline_yaml_path:
            LOGGER.info(f"xks_helm_push_pipeline_yaml_path: {self.xks_helm_push_pipeline_yaml_path}")
        if self.openshift_helm_patch_yaml_path:
            LOGGER.info(f"openshift_helm_patch_yaml_path: {self.openshift_helm_patch_yaml_path}")
        if self.openshift_helm_values_yaml_path:
            LOGGER.info(f"openshift_helm_values_yaml_path: {self.openshift_helm_values_yaml_path}")
        if self.openshift_helm_push_pipeline_yaml_path:
            LOGGER.info(f"openshift_helm_push_pipeline_yaml_path: {self.openshift_helm_push_pipeline_yaml_path}")
        if self.xks_helm_chart_yaml_path:
            LOGGER.info(f"xks_helm_chart_yaml_path: {self.xks_helm_chart_yaml_path}")
        if self.openshift_helm_chart_yaml_path:
            LOGGER.info(f"openshift_helm_chart_yaml_path: {self.openshift_helm_chart_yaml_path}")
        if self.metadata_config_yaml_path:
            LOGGER.info(f"metadata_config_yaml_path: {self.metadata_config_yaml_path}")

        LOGGER.info("")
        LOGGER.info("Loading yaml files...")
        self.csv_dict = util.load_yaml_file(self.bundle_csv_path)
        self.patch_dict = util.load_yaml_file(self.patch_yaml_path, parser='pyyaml')
        self.build_config_dict = util.load_yaml_file(self.build_config_path, parser='pyyaml')
        self.annotation_dict = util.load_yaml_file(self.annotation_yaml_path, parser='pyyaml')
        self.push_pipeline_dict = util.load_yaml_file(self.push_pipeline_yaml_path)

        if self.xks_helm_patch_yaml_path:
            self.xks_helm_patch_dict = util.load_yaml_file_rt(self.xks_helm_patch_yaml_path)
        if self.xks_helm_values_yaml_path:
            self.xks_helm_values_dict = util.load_yaml_file_rt(self.xks_helm_values_yaml_path)
        if self.xks_helm_push_pipeline_yaml_path:
            self.xks_helm_push_pipeline_dict = util.load_yaml_file(self.xks_helm_push_pipeline_yaml_path)
        if self.openshift_helm_patch_yaml_path:
            self.openshift_helm_patch_dict = util.load_yaml_file_rt(self.openshift_helm_patch_yaml_path)
        if self.openshift_helm_values_yaml_path:
            self.openshift_helm_values_dict = util.load_yaml_file_rt(self.openshift_helm_values_yaml_path)
        if self.openshift_helm_push_pipeline_yaml_path:
            self.openshift_helm_push_pipeline_dict = util.load_yaml_file(self.openshift_helm_push_pipeline_yaml_path)
        if self.xks_helm_chart_yaml_path:
            self.xks_helm_chart_dict = util.load_yaml_file_rt(self.xks_helm_chart_yaml_path)
        if self.openshift_helm_chart_yaml_path:
            self.openshift_helm_chart_dict = util.load_yaml_file_rt(self.openshift_helm_chart_yaml_path)

        LOGGER.debug(f"csv_dict: {json.dumps(self.csv_dict, indent=4, default=str)}")
        LOGGER.debug(f"patch_dict: {json.dumps(self.patch_dict, indent=4, default=str)}")
        LOGGER.debug(f"build_config_dict: {json.dumps(self.build_config_dict, indent=4, default=str)}")
        LOGGER.debug(f"annotation_dict: {json.dumps(self.annotation_dict, indent=4, default=str)}")
        LOGGER.debug(f"push_pipeline_dict: {json.dumps(self.push_pipeline_dict, indent=4, default=str)}")
        if self.xks_helm_patch_yaml_path:
            LOGGER.debug(f"xks_helm_patch_dict: {json.dumps(self.xks_helm_patch_dict, indent=4, default=str)}")
        if self.xks_helm_values_yaml_path:
            LOGGER.debug(f"xks_helm_values_dict: {json.dumps(self.xks_helm_values_dict, indent=4, default=str)}")
        if self.xks_helm_push_pipeline_yaml_path:
            LOGGER.debug(f"xks_helm_push_pipeline_dict: {json.dumps(self.xks_helm_push_pipeline_dict, indent=4, default=str)}")
        if self.openshift_helm_patch_yaml_path:
            LOGGER.debug(f"openshift_helm_patch_dict: {json.dumps(self.openshift_helm_patch_dict, indent=4, default=str)}")
        if self.openshift_helm_values_yaml_path:
            LOGGER.debug(f"openshift_helm_values_dict: {json.dumps(self.openshift_helm_values_dict, indent=4, default=str)}")
        if self.openshift_helm_push_pipeline_yaml_path:
            LOGGER.debug(f"openshift_helm_push_pipeline_dict: {json.dumps(self.openshift_helm_push_pipeline_dict, indent=4, default=str)}")
        if self.xks_helm_chart_yaml_path:
            LOGGER.debug(f"xks_helm_chart_dict: {json.dumps(self.xks_helm_chart_dict, indent=4, default=str)}")
        if self.openshift_helm_chart_yaml_path:
            LOGGER.debug(f"openshift_helm_chart_dict: {json.dumps(self.openshift_helm_chart_dict, indent=4, default=str)}")
        LOGGER.info("All yaml files loaded successfully!")

    def process(self):
        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Fetching ODH Operator Metadata...")
        LOGGER.info("=============================================================================")
        self.operator_image_entry, self.operator_git_metadata = self.fetch_operator_metadata()
        # Extract operator git metadata. fetch_operator_metadata() validates exactly one
        # ODH_OPERATOR entry exists, so we can safely access the first key here.
        self.operator_name = list(self.operator_git_metadata["map"].keys())[0]
        self.operator_git_url = self.operator_git_metadata["map"][self.operator_name][CONSTANTS.GIT_URL_LABEL_KEY]
        self.operator_git_commit = self.operator_git_metadata["map"][self.operator_name][CONSTANTS.GIT_COMMIT_LABEL_KEY]
        LOGGER.info("Operator metadata:")
        LOGGER.info(f"  Operator image name: {self.operator_name}")
        LOGGER.info(f"  Operator git URL: {self.operator_git_url}")
        LOGGER.info(f"  Operator git commit: {self.operator_git_commit}")

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Fetching operands-map.yaml and manifests-config.yaml from operator repo...")
        LOGGER.info("=============================================================================")
        self.operands_map_dict, self.manifest_config_dict = self.fetch_operands_map_and_manifest_config()
        LOGGER.debug(f"operands_map_dict: {json.dumps(self.operands_map_dict, indent=4, default=str)}")
        LOGGER.debug(f"manifest_config_dict: {json.dumps(self.manifest_config_dict, indent=4, default=str)}")
        LOGGER.info("Operands map and manifests config loaded successfully!")

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Generating bundle build args...")
        LOGGER.info("=============================================================================")
        self.bundle_build_args = self.generate_bundle_build_args()
        LOGGER.debug(f"bundle_build_args: {json.dumps(self.bundle_build_args, indent=4, default=str)}")

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Loading additional images...")
        LOGGER.info("=============================================================================")
        self.additional_image_entries = self._load_additional_images()

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Extracting SBOM metadata (before registry replacements)...")
        LOGGER.info("=============================================================================")
        self.sbom_metadata_entries = self.extract_sbom_metadata()

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Applying registry and repo replacements...")
        LOGGER.info("=============================================================================")
        self.apply_replacements()
        LOGGER.debug(f"operator_image_entry: {json.dumps(self.operator_image_entry, indent=4, default=str)}")
        LOGGER.debug(f"operand_image_entries: {json.dumps(self.operand_image_entries, indent=4, default=str)}")

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Processing ClusterServiceVersion (CSV)...")
        LOGGER.info("=============================================================================")
        self.patch_csv_yaml()

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Processing annotations.yaml...")
        LOGGER.info("=============================================================================")
        self.patch_annotations_yaml()

        if self.xks_helm_patch_yaml_path and self.xks_helm_values_yaml_path:
            LOGGER.info("")
            LOGGER.info("=============================================================================")
            LOGGER.info("Processing XKS Helm Chart...")
            LOGGER.info("=============================================================================")
            self.patch_xks_helm_chart()

        if self.openshift_helm_patch_yaml_path and self.openshift_helm_values_yaml_path:
            LOGGER.info("")
            LOGGER.info("=============================================================================")
            LOGGER.info("Processing OpenShift Helm Chart...")
            LOGGER.info("=============================================================================")
            self.patch_openshift_helm_chart()

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Processing Push Pipelines...")
        LOGGER.info("=============================================================================")

        LOGGER.info("")
        LOGGER.info("--- Bundle Push Pipeline ---")
        self.is_push_pipeline_updated, self.push_pipeline_dict = util.process_push_pipeline(
            self.push_pipeline_dict, self.push_pipeline_operation
        )

        if self.xks_helm_push_pipeline_yaml_path:
            LOGGER.info("")
            LOGGER.info("--- XKS Helm Push Pipeline ---")
            self.is_xks_helm_push_pipeline_updated, self.xks_helm_push_pipeline_dict = util.process_push_pipeline(
                self.xks_helm_push_pipeline_dict, self.push_pipeline_operation
            )

        if self.openshift_helm_push_pipeline_yaml_path:
            LOGGER.info("")
            LOGGER.info("--- OpenShift Helm Push pipeline ---")
            self.is_openshift_helm_push_pipeline_updated, self.openshift_helm_push_pipeline_dict = util.process_push_pipeline(
                self.openshift_helm_push_pipeline_dict, self.push_pipeline_operation
            )

        LOGGER.info("")
        LOGGER.info("=============================================================================")
        LOGGER.info("Writing output files...")
        LOGGER.info("=============================================================================")
        self.write_output_files()

    def fetch_operator_metadata(self):
        """
        Fetches ODH Operator Metadata.
        
        Filters ODH_OPERATOR entry from bundle-patch, validates exactly one exists,
        then queries Quay.io for the latest image digest and git metadata.
        
        Returns:
            tuple: (operator_image_entry, operator_git_metadata)
        """
        LOGGER.info("Filtering ODH Operator entry from bundle-patch...")
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

        if self.use_existing_digests:
            LOGGER.info("")
            LOGGER.info("--use-existing-digests: Skipping Quay tag lookup, fetching git metadata by digest...")
            operator_git_metadata = util.fetch_git_metadata_for_existing_digests(odh_operator_entry)
            return odh_operator_entry, operator_git_metadata
        else:
            LOGGER.info("")
            LOGGER.info("Querying Quay.io for latest ODH Operator image digest and git metadata...")
            # Compute version tag based on build type
            version_tag = f'{self.rhoai_version}-nightly' if self.build_type.lower() == 'nightly' else self.rhoai_version
            if self.build_type.lower() == 'nightly':
                LOGGER.info(f"  Build type is nightly, using image tag: {version_tag}")
            else:
                LOGGER.info(f"  Build type is ci, using image tag: {version_tag}")

            # Fetch latest image and git metadata for ODH Operator (with GitHub override for upstream repos)
            operator_image_entry, operator_git_metadata = util.fetch_latest_images_and_git_metadata(
                image_entries=odh_operator_entry,
                image_tag=version_tag,
                use_github_override=True
            )

            return operator_image_entry, operator_git_metadata

    def fetch_operands_map_and_manifest_config(self):
        """
        Fetches operands-map.yaml and manifests-config.yaml from the operator repo.

        Uses raw.githubusercontent.com for GitHub repos; clones the repo for
        non-GitHub hosts (e.g., private GitLab).

        Returns:
            tuple: (operands_map_dict, manifest_config_dict)
        """
        LOGGER.info(f"Operator repo: {self.operator_git_url}")
        LOGGER.info(f"Commit: {self.operator_git_commit}")
        LOGGER.info(f"Operands map path: {CONSTANTS.OPERANDS_MAP_PATH}")
        LOGGER.info(f"Manifests config path: {CONSTANTS.MANIFESTS_CONFIG_PATH}")

        file_paths = [CONSTANTS.OPERANDS_MAP_PATH, CONSTANTS.MANIFESTS_CONFIG_PATH]

        if urlparse(self.operator_git_url).hostname == 'github.com':
            operands_map_content = util.fetch_file_data_from_github(
                git_url=self.operator_git_url,
                git_commit=self.operator_git_commit,
                file_path=CONSTANTS.OPERANDS_MAP_PATH
            )
            manifest_config_content = util.fetch_file_data_from_github(
                git_url=self.operator_git_url,
                git_commit=self.operator_git_commit,
                file_path=CONSTANTS.MANIFESTS_CONFIG_PATH
            )
        else:
            LOGGER.info("Non-GitHub repo detected, cloning to fetch files...")
            fetched = util.fetch_files_from_git_repo(
                git_url=self.operator_git_url,
                git_commit=self.operator_git_commit,
                file_paths=file_paths
            )
            operands_map_content = fetched[CONSTANTS.OPERANDS_MAP_PATH]
            manifest_config_content = fetched[CONSTANTS.MANIFESTS_CONFIG_PATH]

        operands_map_dict = yaml.safe_load(operands_map_content)
        manifest_config_dict = yaml.safe_load(manifest_config_content)

        return operands_map_dict, manifest_config_dict

    def generate_bundle_build_args(self):
        """
        Generates bundle build arguments from git metadata.

        Returns:
            String containing build arguments in KEY=VALUE format
        """
        LOGGER.info("Generating build args from git metadata...")
        bundle_build_args = ""
        for component, git_meta in {**self.operator_git_metadata['map'], **self.manifest_config_dict['map'], **self.manifest_config_dict['additional_meta']}.items():
            if 'ref_type' not in git_meta:
                bundle_build_args += f'{component.replace("-", "_").upper()}_{CONSTANTS.GIT_URL_LABEL_KEY.replace(".", "_").upper()}={git_meta[CONSTANTS.GIT_URL_LABEL_KEY]}\n'
                bundle_build_args += f'{component.replace("-", "_").upper()}_{CONSTANTS.GIT_COMMIT_LABEL_KEY.replace(".", "_").upper()}={git_meta[CONSTANTS.GIT_COMMIT_LABEL_KEY]}\n'
                LOGGER.info(f"  Added build args for: {component}")

        LOGGER.info("  Bundle build args generated successfully!")
        return bundle_build_args

    def _load_additional_images(self):
        """
        Load additional images from patch config, strip tags, and deduplicate.

        Returns:
            List of image entry dicts, or empty list if not configured.
        """
        if 'additional-related-images' not in self.patch_dict['patch']:
            return []

        additional_images_file = self.patch_dict['patch']['additional-related-images']['file']
        additional_images_path = f'{Path(self.patch_yaml_path).parent.absolute()}/{additional_images_file}'
        LOGGER.info("  Loading additional images from patch...")

        additional_images_dict = util.load_yaml_file(additional_images_path, parser='pyyaml')

        return list({
            image["name"]: {
                "name": image["name"],
                "value": re.sub(r':[^\s:@]+@', '@', image["value"])
            }
            for image in additional_images_dict['additionalImages']
        }.values())

    def apply_replacements(self):
        """
        Applies registry and repo replacements for operator, operand, and additional images.

        - Operator and operand images: replaces build registry/repo with production equivalents
        - Additional images: validates registries, replaces stage with production
        """
        self.operand_image_entries = self.operands_map_dict['relatedImages']

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

        LOGGER.debug("  Validating registries and applying replacements for additional images...")
        allowed_registries = {CONSTANTS.PRODUCTION_REGISTRY, CONSTANTS.STAGE_REGISTRY}
        disallowed_images = []
        for image in self.additional_image_entries:
            registry = image['value'].split('/')[0]
            if registry not in allowed_registries:
                disallowed_images.append(image)
            else:
                original_value = image['value']
                image['value'] = image['value'].replace(CONSTANTS.STAGE_REGISTRY, CONSTANTS.PRODUCTION_REGISTRY)
                if image['value'] != original_value:
                    LOGGER.debug(f"    {original_value} -> {image['value']}")
        if disallowed_images:
            additional_images_file = self.patch_dict['patch']['additional-related-images']['file']
            LOGGER.error(f"{additional_images_file} contains entries from disallowed registries. Allowed registries are: {allowed_registries}")
            LOGGER.error(f"{json.dumps(disallowed_images, indent=4, default=str)}")
            sys.exit(1)

    def extract_sbom_metadata(self):
        """
        Extract package metadata from image SBOMs as defined in metadata-config.yaml.

        Must be called before apply_replacements() so image URIs point to the
        build registry (quay.io/stage) where images actually exist during CI.

        Expected metadata-config.yaml schema:

            sbom-metadata:
              - env_vars:                                    # list of env var names to look up
                  - "RELATED_IMAGE_RHAII_VLLM_CUDA_IMAGE"
                  - "RELATED_IMAGE_RHAII_VLLM_GAUDI_IMAGE"
                package: "vllm"                              # package name to find in the SBOM
                suffix: "_UPSTREAM_VERSION"                   # appended to each env var name

        For each env var, the image URI is resolved from the operands map or
        additional images, the SBOM is downloaded via cosign, and the package's
        versionInfo is extracted. A new env var is created with the name
        "{env_var}{suffix}" and the version as its value.

        Prerequisites:
            Registry auth must be configured before calling this method (e.g.,
            `skopeo login registry.redhat.io`). The SBOM download uses cosign
            and skopeo, which read credentials from ~/.docker/config.json or
            the containers auth config.

        Returns:
            List of env var dicts ({'name': ..., 'value': ...}) to inject into the CSV,
            or an empty list if metadata-config.yaml is not provided.
        """
        if not self.metadata_config_yaml_path:
            LOGGER.info("No metadata-config.yaml provided, skipping SBOM metadata extraction")
            return []

        LOGGER.info(f"Loading metadata config: {self.metadata_config_yaml_path}")
        metadata_config = util.load_yaml_file(self.metadata_config_yaml_path, parser='pyyaml')

        sbom_entries = metadata_config.get('sbom-metadata', [])
        if not sbom_entries:
            LOGGER.info("No sbom-metadata entries in metadata-config.yaml, skipping")
            return []

        image_lookup = {
            str(entry['name']): str(entry['value'])
            for entry in self.operands_map_dict['relatedImages']
        }
        for entry in self.additional_image_entries:
            image_lookup[str(entry['name'])] = str(entry['value'])

        new_env_vars = []
        for sbom_entry in sbom_entries:
            package_name = sbom_entry['package']
            suffix = sbom_entry['suffix']

            for env_var in sbom_entry['env_vars']:
                image_uri = image_lookup.get(env_var)
                if not image_uri:
                    LOGGER.warning(f"  '{env_var}' not found in operands map or additional images, skipping")
                    continue

                LOGGER.info(f"  Extracting '{package_name}' version from SBOM for {env_var}...")
                arch_versions = get_package_info(image_uri, package_name)
                if 'amd64' not in arch_versions:
                    LOGGER.error(f"  No amd64 version found for '{package_name}' in {env_var}, available: {list(arch_versions.keys())}")
                    sys.exit(1)
                version = arch_versions['amd64']
                if len(set(arch_versions.values())) > 1:
                    LOGGER.warning(f"  Version differs across architectures: {arch_versions} — using amd64 value")
                new_name = f"{env_var}{suffix}"
                LOGGER.info(f"    {new_name} = {version}")
                new_env_vars.append({
                    'name': new_name,
                    'value': DoubleQuotedScalarString(version)
                })

        LOGGER.info(f"Extracted {len(new_env_vars)} SBOM metadata env var(s)")
        return new_env_vars

    def patch_csv_yaml(self):
        """
        Patches CSV fields including operator image, version, and custom fields from csv-patch file.
        """

        # Extract operator image after registry and repo replacements (now has registry.redhat.io URL).
        # fetch_operator_metadata() validates exactly one entry exists, so we can safely access [0].
        self.operator_image = self.operator_image_entry[0]['value']

        LOGGER.info("Updating operator container image...")
        self.csv_dict['metadata']['annotations']['containerImage'] = DoubleQuotedScalarString(self.operator_image)
        self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0][
            'image'] = DoubleQuotedScalarString(self.operator_image)

        if 'initContainers' in self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']:
            self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['initContainers'][0][
            'image'] = DoubleQuotedScalarString(self.operator_image)

        LOGGER.info(f"  containerImage: {self.operator_image}")

        LOGGER.info("")
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

        LOGGER.info("")
        LOGGER.info("Removing olm.skipRange and replaces fields if present...")
        removed_skip_range = self.csv_dict['metadata']['annotations'].pop('olm.skipRange', None)
        removed_replaces = self.csv_dict['spec'].pop('replaces', None)
        if removed_skip_range:
            LOGGER.info(f"  Removed olm.skipRange: {removed_skip_range} field")
        if removed_replaces:
            LOGGER.info(f"  Removed replaces: {removed_replaces} field")

        LOGGER.info("")
        LOGGER.info("Applying CSV patches...")
        csv_patch_file = self.patch_dict['patch']['additional-fields']['file']
        csv_patch_path = f'{Path(self.patch_yaml_path).parent.absolute()}/{csv_patch_file}'
        LOGGER.info(f"  Loading csv-patch file: {csv_patch_file}")
        csv_patch_dict = util.load_yaml_file(csv_patch_path, parser='pyyaml')
        LOGGER.debug(f"  csv_patch_dict: {json.dumps(csv_patch_dict, indent=4, default=str)}")
        self.csv_dict = jsonupdate_ng.updateJson(self.csv_dict, csv_patch_dict)
        LOGGER.info("  CSV patches applied successfully!")

        # Prepare env and related images list
        self.env_vars, self.related_images = self.prepare_env_and_related_images()

        # Inject SBOM-derived metadata into env vars
        if self.sbom_metadata_entries:
            existing_names = {str(entry['name']) for entry in self.env_vars}
            conflicts = [e['name'] for e in self.sbom_metadata_entries if e['name'] in existing_names]
            if conflicts:
                LOGGER.error(f"SBOM metadata env var names conflict with existing env vars: {conflicts}")
                sys.exit(1)
            self.env_vars.extend(self.sbom_metadata_entries)
            LOGGER.info(f"  Injected {len(self.sbom_metadata_entries)} SBOM metadata env var(s) into deployment spec")

        LOGGER.info("")
        LOGGER.info("Updating deployment env vars...")
        self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]['env'] = self.env_vars
        LOGGER.info("  Deployment env vars updated successfully!")

        # Set spec.relatedImages for OLM disconnected support
        LOGGER.info("")
        LOGGER.info("Updating spec.relatedImages list...")
        self.csv_dict['spec']['relatedImages'] = self.related_images
        LOGGER.info("  spec.relatedImages updated successfully!")
        LOGGER.debug(f"  Updated relatedImages: {json.dumps(self.related_images, indent=4, default=str)}")

    def prepare_env_and_related_images(self):
        """
        Prepares deployment env vars and relatedImages list for CSV patching.
        
        Returns:
            Tuple of (final_env_vars, related_images):
            - final_env_vars: Complete env vars list for the deployment spec
            - related_images: Complete list for spec.relatedImages
        """
        existing_env_vars = self.csv_dict['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]['env']
        existing_env_vars = util.to_plain_dict(existing_env_vars)
        LOGGER.debug(f"Existing deployment env vars: {json.dumps(existing_env_vars, indent=4, default=str)}")

        LOGGER.info("Generating full list of deployment environment variables:")
        LOGGER.info("  Adding images from operands map...")
        
        if self.additional_image_entries:
            LOGGER.info("  Adding images from additional images patch...")
            new_env_vars = self.operand_image_entries + self.additional_image_entries
        else:
            new_env_vars = self.operand_image_entries

        LOGGER.debug(f"  new_env_vars: {json.dumps(new_env_vars, indent=4, default=str)}")
        
        # Merge image entries into deployment env vars using jsonupdate_ng
        # Uses 'name' as key for matching/merging list items
        final_env_object = jsonupdate_ng.updateJson(
            {'env': existing_env_vars},
            {'env': new_env_vars},
            meta={'listPatchScheme': {'$.env': {'key': 'name'}}}
        )
        final_env_vars = final_env_object['env']
        LOGGER.debug(f"  final_env_vars: {json.dumps(final_env_vars, indent=4, default=str)}")

        # Build relatedImages list for OLM (transformed names: RELATED_IMAGE_X_IMAGE -> x_image)
        LOGGER.info("")
        LOGGER.info("Generating relatedImages list...")
        relatedImages = []
        
        # Add images from annotations (registry.redhat.io with digest)
        LOGGER.info("  Adding entries from annotations...")
        for name, value in self.csv_dict['metadata']['annotations'].items():
            if value.startswith(CONSTANTS.PRODUCTION_REGISTRY) and '@sha256:' in value:
                image_name = f'{value.split("/")[-1].replace("@sha256:", "-")}-annotation'
                entry = {'name': image_name, 'image': value}
                relatedImages.append(entry)
                LOGGER.debug(entry)

        # Add new_env_vars images with transformed names (RELATED_IMAGE_X_IMAGE -> x_image)
        LOGGER.info("  Adding entries from operands map and additional images patch...")
        for image in new_env_vars:
            entry = {'name': image['name'].replace('RELATED_IMAGE_', '').lower(), 'image': image['value']}
            relatedImages.append(entry)
            LOGGER.debug(entry)
            
        LOGGER.debug(f"  relatedImages: {json.dumps(relatedImages, indent=4, default=str)}")
        
        return final_env_vars, relatedImages

    def compute_olm_channel(self):
        """
        Computes the OLM channel name based on the bundle patch version.

        Returns:
            str: "beta" if version contains "ea", otherwise "stable-x.y"
        """
        version = str(self.patch_dict["patch"]["version"])
        if "ea" in version:
            return "beta"
        match = re.match(r'^(\d+\.\d+)', version)
        if not match:
            LOGGER.error(f"Unable to parse major.minor from version: {version}")
            sys.exit(1)
        return f"stable-{match.group(1)}"

    def patch_xks_helm_chart(self):
        """
        Updates xks-values-patch.yaml with resolved operator image and related image digests,
        then applies the updated patch to the XKS helm chart values.yaml.

        Steps:
            1. Update xks-values-patch.yaml: replace operator image references and relatedImages
               values with fully resolved digests, then write the updated patch file.
            2. Apply the updated patch to values.yaml: set operator image, relatedImages
               (preserved as list format), and cloud-specific images. Also injects any
               SBOM metadata entries (e.g., _UPSTREAM_VERSION env vars) from
               metadata-config.yaml into values.yaml under rhaiOperator.extraEnvVars
               (separate from relatedImages, since these are version strings, not
               container image references).
        """
        cloud_providers = ['azure', 'coreweave', 'aws']

        LOGGER.info("Step 1: Updating xks-values-patch.yaml with resolved digests")
        LOGGER.info("")
        LOGGER.info("Updating operator image in values-patch...")
        if 'rhaiOperator' in self.xks_helm_patch_dict and 'image' in self.xks_helm_patch_dict['rhaiOperator']:
            self.xks_helm_patch_dict['rhaiOperator']['image'] = DoubleQuotedScalarString(self.operator_image)
            LOGGER.info(f"  rhaiOperator.image -> {self.operator_image}")
        for section in cloud_providers:
            cloud_mgr = self.xks_helm_patch_dict.get(section, {}).get('cloudManager', {})
            if 'image' in cloud_mgr:
                self.xks_helm_patch_dict[section]['cloudManager']['image'] = DoubleQuotedScalarString(self.operator_image)
                LOGGER.info(f"  {section}.cloudManager.image -> {self.operator_image}")

        LOGGER.info("")
        LOGGER.info("Updating relatedImages in values-patch with resolved digests...")
        env_vars_map = {
            str(entry['name']): str(entry['value'])
            for entry in self.env_vars
            if str(entry.get('name', '')).startswith('RELATED_IMAGE_')
        }

        helm_related_images = self.xks_helm_patch_dict.get('rhaiOperator', {}).get('relatedImages', [])
        for related_image in helm_related_images:
            name = str(related_image['name'])
            if name in env_vars_map:
                old_value = related_image['value']
                related_image['value'] = DoubleQuotedScalarString(env_vars_map[name])
                LOGGER.info(f"  {name}: {old_value} -> {env_vars_map[name]}")
            else:
                LOGGER.warning(f"  No matching env var found for: {name}")

        LOGGER.info("")
        LOGGER.info("Updating hooks.cliImage in values-patch...")
        ose_cli_image_key = 'RELATED_IMAGE_OSE_CLI_IMAGE'
        if ose_cli_image_key in env_vars_map:
            old_value = self.xks_helm_patch_dict.get('hooks', {}).get('cliImage', '')
            self.xks_helm_patch_dict['hooks']['cliImage'] = DoubleQuotedScalarString(env_vars_map[ose_cli_image_key])
            LOGGER.info(f"  hooks.cliImage: {old_value} -> {env_vars_map[ose_cli_image_key]}")
        else:
            LOGGER.warning(f"  {ose_cli_image_key} not found in env vars, skipping hooks.cliImage update")

        LOGGER.info("")
        LOGGER.info("Step 2: Applying updated values-patch to XKS values.yaml")
        LOGGER.info("")
        LOGGER.info("Updating rhaiOperator fields...")
        self.xks_helm_values_dict['rhaiOperator']['image'] = DoubleQuotedScalarString(self.operator_image)
        values_related_images = list(helm_related_images)

        self.xks_helm_values_dict['rhaiOperator']['relatedImages'] = values_related_images
        LOGGER.info("  rhaiOperator.image updated")
        LOGGER.info(f"  rhaiOperator.relatedImages updated with {len(values_related_images)} entries")

        if self.sbom_metadata_entries:
            LOGGER.info("")
            LOGGER.info("Adding SBOM metadata entries to values.yaml extraEnvVars...")
            extra_env_vars = list(self.xks_helm_values_dict.get('rhaiOperator', {}).get('extraEnvVars', []))
            existing_names = {str(ev['name']) for ev in extra_env_vars}
            for entry in self.sbom_metadata_entries:
                entry_name = str(entry['name'])
                entry_value = DoubleQuotedScalarString(str(entry['value']))
                if entry_name not in existing_names:
                    extra_env_vars.append({'name': entry_name, 'value': entry_value})
                    LOGGER.info(f"  Added {entry_name}: {entry['value']}")
                else:
                    for ev in extra_env_vars:
                        if str(ev['name']) == entry_name:
                            ev['value'] = entry_value
                            LOGGER.info(f"  Updated {entry_name}: {entry['value']}")
                            break
            self.xks_helm_values_dict['rhaiOperator']['extraEnvVars'] = extra_env_vars
            LOGGER.info(f"  rhaiOperator.extraEnvVars updated with {len(extra_env_vars)} entries")

        if 'hooks' in self.xks_helm_patch_dict and 'cliImage' in self.xks_helm_patch_dict['hooks']:
            if 'hooks' in self.xks_helm_values_dict:
                self.xks_helm_values_dict['hooks']['cliImage'] = self.xks_helm_patch_dict['hooks']['cliImage']
                LOGGER.info("  hooks.cliImage updated")

        for section in cloud_providers:
            cloud_mgr_patch = self.xks_helm_patch_dict.get(section, {}).get('cloudManager', {})
            if 'image' in cloud_mgr_patch:
                if section in self.xks_helm_values_dict and 'cloudManager' in self.xks_helm_values_dict[section]:
                    self.xks_helm_values_dict[section]['cloudManager']['image'] = DoubleQuotedScalarString(self.operator_image)
                    LOGGER.info(f"  {section}.cloudManager.image updated")

        if self.xks_helm_chart_yaml_path:
            LOGGER.info("")
            LOGGER.info("Updating XKS Chart.yaml version fields...")
            patch_version = str(self.patch_dict['patch']['version'])
            self.xks_helm_chart_dict['version'] = patch_version
            self.xks_helm_chart_dict['appVersion'] = patch_version
            LOGGER.info(f"  version -> {patch_version}")
            LOGGER.info(f"  appVersion -> {patch_version}")

    def patch_openshift_helm_chart(self):
        """
        Updates openshift-values-patch.yaml with the computed OLM channel,
        then applies the patch to the OpenShift helm chart values.yaml.
        """
        olm_channel = self.compute_olm_channel()
        LOGGER.info(f"Computed OLM channel: {olm_channel} (from version: {self.patch_dict['patch']['version']})")

        LOGGER.info("")
        LOGGER.info("Updating openshift-values-patch.yaml...")
        self.openshift_helm_patch_dict['operator']['rhoai']['olm']['channel'] = olm_channel
        LOGGER.info(f"  operator.rhoai.olm.channel -> {olm_channel}")

        LOGGER.info("")
        LOGGER.info("Applying openshift-values-patch to OpenShift values.yaml...")
        self.openshift_helm_values_dict = jsonupdate_ng.updateJson(
            self.openshift_helm_values_dict, self.openshift_helm_patch_dict
        )
        LOGGER.info("  OpenShift values.yaml patched successfully!")

        if self.openshift_helm_chart_yaml_path:
            LOGGER.info("")
            LOGGER.info("Updating OpenShift Chart.yaml version fields...")
            patch_version = str(self.patch_dict['patch']['version'])
            self.openshift_helm_chart_dict['version'] = patch_version
            self.openshift_helm_chart_dict['appVersion'] = patch_version
            LOGGER.info(f"  version -> {patch_version}")
            LOGGER.info(f"  appVersion -> {patch_version}")

    def patch_annotations_yaml(self):
        """
        Processes annotation.yaml by removing channel-related annotations.
        
        These annotations are no longer required after migrating to File Based Catalogs (FBC).
        """
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

        if self.xks_helm_patch_yaml_path and self.xks_helm_values_yaml_path:
            util.write_yaml_file_rt(self.xks_helm_patch_dict, self.xks_helm_patch_yaml_path)
            util.write_yaml_file_rt(self.xks_helm_values_dict, self.xks_helm_values_yaml_path)
        if self.xks_helm_chart_yaml_path:
            util.write_yaml_file_rt(self.xks_helm_chart_dict, self.xks_helm_chart_yaml_path)

        if self.xks_helm_push_pipeline_yaml_path and self.is_xks_helm_push_pipeline_updated:
            util.write_yaml_file(self.xks_helm_push_pipeline_dict, self.xks_helm_push_pipeline_yaml_path)

        if self.openshift_helm_patch_yaml_path and self.openshift_helm_values_yaml_path:
            util.write_yaml_file_rt(self.openshift_helm_patch_dict, self.openshift_helm_patch_yaml_path)
            util.write_yaml_file_rt(self.openshift_helm_values_dict, self.openshift_helm_values_yaml_path)
        if self.openshift_helm_chart_yaml_path:
            util.write_yaml_file_rt(self.openshift_helm_chart_dict, self.openshift_helm_chart_yaml_path)

        if self.openshift_helm_push_pipeline_yaml_path and self.is_openshift_helm_push_pipeline_updated:
            util.write_yaml_file(self.openshift_helm_push_pipeline_dict, self.openshift_helm_push_pipeline_yaml_path)

        LOGGER.info("  Output files written successfully!")

# For POC purposes
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
                        help='Build type: "ci" or "nightly"', dest='build_type', default='ci')
    parser.add_argument('-c', '--bundle-csv-path', required=False,
                        help='Path of the bundle csv yaml from the release branch.', dest='bundle_csv_path')
    parser.add_argument('-p', '--patch-yaml-path', required=False,
                        help='Path of the bundle-patch.yaml from the release branch.', dest='patch_yaml_path')
    parser.add_argument('-o', '--output-file-path', required=False,
                        help='Path of the output bundle csv', dest='output_file_path')
    parser.add_argument('-v', '--rhoai-version', required=False,
                        help='The version of Openshift-AI being processed', dest='rhoai_version')
    parser.add_argument('-a', '--annotation-yaml-path', required=False,
                        help='Path of the annotation.yaml from the raw inputs', dest='annotation_yaml_path')
    parser.add_argument('-y', '--push-pipeline-yaml-path', required=False,
                        help='Path of the tekton pipeline for push builds', dest='push_pipeline_yaml_path')
    parser.add_argument('-x', '--push-pipeline-operation', required=False, default="enable",
                        help='Operation code, supported values are "enable" and "disable"', dest='push_pipeline_operation')
    parser.add_argument('-xhp', '--xks-helm-patch-yaml-path', required=False,
                        help='Path of the XKS helm xks-values-patch.yaml', dest='xks_helm_patch_yaml_path')
    parser.add_argument('-xhv', '--xks-helm-values-yaml-path', required=False,
                        help='Path of the XKS helm chart values.yaml to be patched', dest='xks_helm_values_yaml_path')
    parser.add_argument('-xhpp', '--xks-helm-push-pipeline-yaml-path', required=False,
                        help='Path of the XKS helm tekton push pipeline', dest='xks_helm_push_pipeline_yaml_path')
    parser.add_argument('-ohp', '--openshift-helm-patch-yaml-path', required=False,
                        help='Path of the OpenShift helm openshift-values-patch.yaml', dest='openshift_helm_patch_yaml_path')
    parser.add_argument('-ohv', '--openshift-helm-values-yaml-path', required=False,
                        help='Path of the OpenShift helm chart values.yaml to be patched', dest='openshift_helm_values_yaml_path')
    parser.add_argument('-ohpp', '--openshift-helm-push-pipeline-yaml-path', required=False,
                        help='Path of the OpenShift helm tekton push pipeline', dest='openshift_helm_push_pipeline_yaml_path')
    parser.add_argument('-mc', '--metadata-config-yaml-path', required=False,
                        help='Path of the metadata-config.yaml for SBOM metadata extraction', dest='metadata_config_yaml_path')
    parser.add_argument('--use-existing-digests', action='store_true', default=False,
                        help='Preserve existing operator image digest instead of querying Quay for latest tag. Used for embargo builds.',
                        dest='use_existing_digests')
    # For POC purposes: snapshot_processor arguments
    parser.add_argument('-sn', '--snapshot-json-path', required=False,
                        help='Path of the single-bundle generated using the opm.', dest='snapshot_json_path')
    parser.add_argument('-f', '--image-filter', required=False,
                        help='Path of the single-bundle generated using the opm.', dest='image_filter')
    args = parser.parse_args()

    if args.operation.lower() == 'bundle-patch':
        processor = bundle_processor(build_config_path=args.build_config_path, bundle_csv_path=args.bundle_csv_path, patch_yaml_path=args.patch_yaml_path, rhoai_version=args.rhoai_version, output_file_path=args.output_file_path, annotation_yaml_path=args.annotation_yaml_path, push_pipeline_operation=args.push_pipeline_operation, push_pipeline_yaml_path=args.push_pipeline_yaml_path, build_type=args.build_type, xks_helm_patch_yaml_path=args.xks_helm_patch_yaml_path, xks_helm_values_yaml_path=args.xks_helm_values_yaml_path, xks_helm_push_pipeline_yaml_path=args.xks_helm_push_pipeline_yaml_path, openshift_helm_patch_yaml_path=args.openshift_helm_patch_yaml_path, openshift_helm_values_yaml_path=args.openshift_helm_values_yaml_path, openshift_helm_push_pipeline_yaml_path=args.openshift_helm_push_pipeline_yaml_path, metadata_config_yaml_path=args.metadata_config_yaml_path, use_existing_digests=args.use_existing_digests)
        processor.process()

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
    # processor.process()
