#!/usr/bin/env bash

set -euo pipefail

namespace="$1"
hub_ip="$2"
directory="$(mktemp -d)"
trap 'rm -rf "${directory}"' EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "${directory}/ca.key"
openssl req -x509 -new -key "${directory}/ca.key" -sha256 -days 2 \
  -subj "/CN=mspsql-kind-registration-ca" -out "${directory}/ca.crt"
openssl ecparam -name prime256v1 -genkey -noout -out "${directory}/server.key"
openssl req -new -key "${directory}/server.key" -subj "/CN=${hub_ip}" \
  -out "${directory}/server.csr"
cat >"${directory}/server.ext" <<EOF
subjectAltName=IP:${hub_ip}
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
EOF
openssl x509 -req -in "${directory}/server.csr" -CA "${directory}/ca.crt" \
  -CAkey "${directory}/ca.key" -CAcreateserial -days 2 -sha256 \
  -extfile "${directory}/server.ext" -out "${directory}/server.crt"

kubectl -n "${namespace}" create secret generic mspsql-registration-ca \
  --from-file=tls.crt="${directory}/ca.crt" \
  --from-file=tls.key="${directory}/ca.key"
kubectl -n "${namespace}" create secret generic mspsql-control-tls \
  --from-file=tls.crt="${directory}/server.crt" \
  --from-file=tls.key="${directory}/server.key" \
  --from-file=ca.crt="${directory}/ca.crt"
