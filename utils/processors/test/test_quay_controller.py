import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from controller.quay_controller import quay_controller


class TestTokenEnvVar:
    def test_simple_org(self):
        qc = quay_controller('opendatahub')
        assert qc._token_env_var() == 'OPENDATAHUB_QUAY_API_TOKEN'

    def test_hyphenated_org(self):
        qc = quay_controller('rhoai-private')
        assert qc._token_env_var() == 'RHOAI_PRIVATE_QUAY_API_TOKEN'

    def test_multiple_hyphens(self):
        qc = quay_controller('my-org-name')
        assert qc._token_env_var() == 'MY_ORG_NAME_QUAY_API_TOKEN'

    def test_dot_in_org(self):
        qc = quay_controller('org.name')
        assert qc._token_env_var() == 'ORG_NAME_QUAY_API_TOKEN'

    def test_already_valid(self):
        qc = quay_controller('myorg')
        assert qc._token_env_var() == 'MYORG_QUAY_API_TOKEN'
