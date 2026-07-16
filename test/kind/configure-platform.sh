#!/usr/bin/env bash

set -euo pipefail

kubeconfig="$1"
pool_start="$2"
pool_end="$3"
ca_certificate="$4"
ca_private_key="$5"
cert_manager_manifest="$6"
metallb_manifest="$7"
kubectl=(kubectl --kubeconfig="${kubeconfig}")

"${kubectl[@]}" apply -f "${cert_manager_manifest}"
"${kubectl[@]}" -n cert-manager rollout status deployment/cert-manager --timeout=300s
"${kubectl[@]}" -n cert-manager rollout status deployment/cert-manager-webhook --timeout=300s
"${kubectl[@]}" -n cert-manager rollout status deployment/cert-manager-cainjector --timeout=300s
"${kubectl[@]}" -n cert-manager create secret tls mspsql-test-ca \
  --cert="${ca_certificate}" --key="${ca_private_key}" --dry-run=client -o yaml |
  "${kubectl[@]}" apply -f -
"${kubectl[@]}" apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: test
spec:
  ca:
    secretName: mspsql-test-ca
EOF

"${kubectl[@]}" apply -f "${metallb_manifest}"
"${kubectl[@]}" -n metallb-system rollout status deployment/controller --timeout=300s
"${kubectl[@]}" -n metallb-system rollout status daemonset/speaker --timeout=300s
"${kubectl[@]}" apply -f - <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: database-services
  namespace: metallb-system
spec:
  addresses:
  - ${pool_start}-${pool_end}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: database-services
  namespace: metallb-system
spec:
  ipAddressPools:
  - database-services
EOF
