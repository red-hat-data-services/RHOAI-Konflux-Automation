# run this before bootstrap.sh, using source:
#   source ./bootstrap-auth.sh
#   ./bootstrap.sh
export VAULT_ADDR=https://vault.devshift.net
vault login -method=oidc --no-print
export RHOAI_QUAY_API_TOKEN=$(vault kv get --mount=rhoai -field=oauth_token creds/quay/quay-devops-app)
