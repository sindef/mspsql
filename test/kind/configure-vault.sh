#!/usr/bin/env bash

set -euo pipefail

kubeconfig="$1"
site="$2"
kubectl=(kubectl --kubeconfig="${kubeconfig}")
vault_env=(env VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root)

"${kubectl[@]}" apply -f test/kind/vault.yaml
"${kubectl[@]}" -n vault rollout status deployment/vault --timeout=120s
"${kubectl[@]}" create namespace orders-postgres
"${kubectl[@]}" -n orders-postgres create serviceaccount mspsql-workload

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
EOF
)"
"${kubectl[@]}" -n vault exec deployment/vault -- sh -ec \
  'printf "%s" "$1" | env VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root vault policy write orders -' \
  sh "${policy}"
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault write \
  auth/kubernetes/role/orders-"${site}" \
  bound_service_account_names=mspsql-workload \
  bound_service_account_namespaces=orders-postgres \
  audience=vault \
  policies=orders \
  ttl=10m

"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault kv put \
  mspsql/postgres/orders/bootstrap superuserPassword=super replicationPassword=replication
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault kv put \
  mspsql/postgres/orders/pgpool adminUsername=admin adminPassword=pool
"${kubectl[@]}" -n vault exec deployment/vault -- "${vault_env[@]}" vault kv put \
  mspsql/postgres/orders/backup s3AccessKey=access s3SecretKey=secret repositoryCipherPassphrase=cipher

jwt="$("${kubectl[@]}" -n orders-postgres create token mspsql-workload --audience=vault --duration=10m)"
client_token="$("${kubectl[@]}" -n vault exec deployment/vault -- env VAULT_ADDR=http://127.0.0.1:8200 \
  vault write -field=token auth/kubernetes/login role=orders-"${site}" jwt="${jwt}")"
"${kubectl[@]}" -n vault exec deployment/vault -- env VAULT_ADDR=http://127.0.0.1:8200 \
  VAULT_TOKEN="${client_token}" vault kv get -field=superuserPassword mspsql/postgres/orders/bootstrap |
  grep -qx super
