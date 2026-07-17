#!/usr/bin/env bash

set -euo pipefail

kubeconfig="$1"
site="$2"
ca_cert="$3"
ca_key="$4"
kubectl=(kubectl --kubeconfig="${kubeconfig}")
temp_dir="$(mktemp -d)"
trap 'rm -rf "${temp_dir}"' EXIT

openssl req -newkey rsa:2048 -nodes -subj "/CN=vault.vault.svc" \
  -keyout "${temp_dir}/tls.key" -out "${temp_dir}/tls.csr" >/dev/null 2>&1
cat >"${temp_dir}/extensions.cnf" <<'EOF'
subjectAltName=DNS:vault,DNS:vault.vault,DNS:vault.vault.svc,DNS:vault.vault.svc.cluster.local,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -days 2 -sha256 -in "${temp_dir}/tls.csr" \
  -CA "${ca_cert}" -CAkey "${ca_key}" -CAserial "${temp_dir}/ca.srl" -CAcreateserial \
  -extfile "${temp_dir}/extensions.cnf" -out "${temp_dir}/tls.crt" >/dev/null 2>&1

"${kubectl[@]}" apply -f test/kind/vault.yaml
"${kubectl[@]}" -n vault create secret generic vault-tls \
  --type=kubernetes.io/tls \
  --from-file=tls.crt="${temp_dir}/tls.crt" \
  --from-file=tls.key="${temp_dir}/tls.key" \
  --from-file=ca.crt="${ca_cert}"
if ! "${kubectl[@]}" -n vault rollout status deployment/vault --timeout=180s; then
  "${kubectl[@]}" -n vault get pods -o wide
  "${kubectl[@]}" -n vault describe pods
  "${kubectl[@]}" -n vault logs deployment/vault --all-containers || true
  exit 1
fi
vault_env=(env VAULT_ADDR=https://127.0.0.1:8200 VAULT_CACERT=/vault/tls/ca.crt)
init="$("${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" \
  vault operator init -format=json -key-shares=1 -key-threshold=1)"
unseal_key="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["unseal_keys_b64"][0])' <<<"${init}")"
root_token="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["root_token"])' <<<"${init}")"
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault operator unseal "${unseal_key}"
vault_env+=(VAULT_TOKEN="${root_token}")
"${kubectl[@]}" create namespace vault-auth-test
"${kubectl[@]}" -n vault-auth-test create serviceaccount mspsql-workload

"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault secrets enable -path=mspsql kv-v2
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault auth enable -path=kubernetes kubernetes
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault write auth/kubernetes/config \
  kubernetes_host=https://kubernetes.default.svc:443

policy="$(cat <<'EOF'
path "mspsql/data/postgres/orders/*" {
  capabilities = ["create", "read", "update"]
}
path "mspsql/metadata/postgres/orders/*" {
  capabilities = ["read", "list"]
}
path "sys/mounts/mspsql" {
  capabilities = ["read"]
}
path "sys/mounts/mspsql/*" {
  capabilities = ["read"]
}
EOF
)"
"${kubectl[@]}" -n vault exec deployment/vault -- sh -ec \
  'printf "%s" "$1" | env VAULT_ADDR=https://127.0.0.1:8200 VAULT_CACERT=/vault/tls/ca.crt VAULT_TOKEN="$2" vault policy write orders -' \
  sh "${policy}" "${root_token}"
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault write \
  auth/kubernetes/role/orders-"${site}" \
  bound_service_account_names=mspsql-workload \
  bound_service_account_namespaces=orders-postgres,orders-recovered-vic,orders-recovered-nsw,vault-auth-test \
  audience=vault \
  policies=orders \
  ttl=10m

"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault kv put \
  mspsql/postgres/orders/bootstrap \
  superuserUsername=postgres superuserPassword=super \
  replicationUsername=replication replicationPassword=replication
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault kv put \
  mspsql/postgres/orders/pgpool adminUsername=admin adminPassword=pool
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault kv put \
  mspsql/postgres/orders/backup s3AccessKey=access s3SecretKey=secretsecret repositoryCipherPassphrase=cipher
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault kv put \
  mspsql/postgres/orders/users/orders-app password=application-secret

jwt="$("${kubectl[@]}" -n vault-auth-test create token mspsql-workload --audience=vault --duration=10m)"
client_token="$("${kubectl[@]}" -n vault exec deployment/vault -- \
  env VAULT_ADDR=https://127.0.0.1:8200 VAULT_CACERT=/vault/tls/ca.crt \
  vault write -field=token auth/kubernetes/login role=orders-"${site}" jwt="${jwt}")"
"${kubectl[@]}" -n vault exec deployment/vault -- \
  env VAULT_ADDR=https://127.0.0.1:8200 VAULT_CACERT=/vault/tls/ca.crt \
  VAULT_TOKEN="${client_token}" vault kv get -field=superuserPassword mspsql/postgres/orders/bootstrap |
  grep -qx super
"${kubectl[@]}" delete namespace vault-auth-test --wait=true
