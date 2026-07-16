#!/usr/bin/env bash

set -euo pipefail

clusters=(mspsql-hub mspsql-vic mspsql-nsw mspsql-qld)
image="${IMG:-mspsql:test}"
agent_image="${AGENT_IMG:-mspsql-agent:test}"
vault_image="hashicorp/vault:1.21.4"

cleanup() {
  status=$?
  if [[ "${status}" -ne 0 ]]; then
    if [[ -n "${KUBECONFIG:-}" ]]; then
      kubectl -n database-platform get multisitepostgres -o yaml || true
      kubectl -n mspsql-system logs deployment/mspsql-controller-manager \
        --all-containers --tail=200 || true
    fi
    for site in vic nsw qld; do
      site_kubeconfig="$(mktemp)"
      if kind get kubeconfig --name "mspsql-${site}" >"${site_kubeconfig}" 2>/dev/null; then
        kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent get pods -o wide || true
        kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent logs deployment/mspsql-agent \
          --all-containers --tail=200 || true
        kubectl --kubeconfig="${site_kubeconfig}" get events -A \
          --sort-by=.lastTimestamp | tail -100 || true
      fi
      rm -f "${site_kubeconfig}"
    done
  fi
  for cluster in "${clusters[@]}"; do
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

docker pull "${vault_image}"

for cluster in "${clusters[@]}"; do
  kind create cluster --name "${cluster}" --wait 120s
done

for site in vic nsw qld; do
  kind load docker-image "${vault_image}" --name "mspsql-${site}"
  site_kubeconfig="$(mktemp)"
  kind get kubeconfig --name "mspsql-${site}" >"${site_kubeconfig}"
  ./test/kind/configure-vault.sh "${site_kubeconfig}" "${site}"
  rm -f "${site_kubeconfig}"
done

docker build -t "${image}" .
docker build -f Dockerfile.agent -t "${agent_image}" .
kind load docker-image "${image}" --name mspsql-hub
for site in vic nsw qld; do
  kind load docker-image "${agent_image}" --name "mspsql-${site}"
done

export KUBECONFIG
KUBECONFIG="$(kind get kubeconfig-path --name mspsql-hub 2>/dev/null || true)"
if [[ -z "${KUBECONFIG}" ]]; then
  KUBECONFIG="$(mktemp)"
  kind get kubeconfig --name mspsql-hub >"${KUBECONFIG}"
fi

make kustomize
hub_ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  mspsql-hub-control-plane)"
./bin/kustomize build test/kind/hub |
  sed "s|image: controller:latest|image: ${image}|" |
  sed "s|HUB_NODE_IP|${hub_ip}|g" |
  sed "s|SITE_AGENT_IMAGE|${agent_image}|g" |
  kubectl apply -f -
./test/kind/create-control-tls.sh mspsql-system "${hub_ip}"
kubectl -n mspsql-system rollout status deployment/mspsql-controller-manager --timeout=180s

for site in vic nsw qld; do
  kubectl apply -f - <<EOF
apiVersion: multisite-postgres.dev/v1alpha1
kind: SiteRegistration
metadata:
  name: ${site}
spec:
  permittedStorageClasses:
    etcd: [standard]
    postgres: [standard]
  permittedIssuers:
    etcd:
    - {name: test, kind: ClusterIssuer, group: cert-manager.io}
    postgres:
    - {name: test, kind: ClusterIssuer, group: cert-manager.io}
    pgpool:
    - {name: test, kind: ClusterIssuer, group: cert-manager.io}
  metallbAddressPools: [database-services]
EOF
  for _ in $(seq 1 60); do
    registration_url="$(kubectl get siteregistration "${site}" -o jsonpath='{.status.registrationURL}')"
    [[ -n "${registration_url}" ]] && break
    sleep 1
  done
  test -n "${registration_url}"
  site_kubeconfig="$(mktemp)"
  kind get kubeconfig --name "mspsql-${site}" >"${site_kubeconfig}"
  curl -fsS "${registration_url}" | kubectl --kubeconfig="${site_kubeconfig}" apply -f -
  rm -f "${site_kubeconfig}"
  for _ in $(seq 1 120); do
    phase="$(kubectl get siteregistration "${site}" -o jsonpath='{.status.phase}')"
    [[ "${phase}" == "Connected" ]] && break
    sleep 2
  done
  test "${phase}" = "Connected"
done

kubectl create namespace database-platform
kubectl apply -f test/kind/instance.yaml

for site in vic nsw qld; do
  site_kubeconfig="$(mktemp)"
  kind get kubeconfig --name "mspsql-${site}" >"${site_kubeconfig}"
  for _ in $(seq 1 90); do
    namespace_uid="$(kubectl --kubeconfig="${site_kubeconfig}" get namespace orders-postgres \
      -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    [[ -n "${namespace_uid}" ]] && break
    sleep 2
  done
  test -n "${namespace_uid}"
  if [[ "${site}" != "qld" ]]; then
    for _ in $(seq 1 60); do
      if kubectl --kubeconfig="${site_kubeconfig}" -n orders-postgres \
        get secret postgres-auth >/dev/null 2>&1; then
        break
      fi
      sleep 2
    done
    kubectl --kubeconfig="${site_kubeconfig}" -n orders-postgres get secret postgres-auth \
      -o jsonpath='{.data.superuser-password}' | base64 -d | grep -qx super
  fi
  rm -f "${site_kubeconfig}"
done

for _ in $(seq 1 60); do
  count="$(kubectl -n database-platform get configmap \
    -l multisite-postgres.dev/instance-uid -o name | wc -l | tr -d ' ')"
  [[ "${count}" == "3" ]] && break
  sleep 2
done
test "$(kubectl -n database-platform get configmap -l multisite-postgres.dev/instance-uid -o name | wc -l | tr -d ' ')" = "3"
test "$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.activeRevision}')" = "1"

kubectl -n database-platform patch multisitepostgres orders --type=merge --subresource=status \
  -p='{"status":{"sites":[{"name":"vic","siteRegistrationRef":"vic","addresses":{"etcd-vic-0":"10.0.0.1"}},{"name":"nsw","siteRegistrationRef":"nsw"},{"name":"qld","siteRegistrationRef":"qld"}]}}'
kubectl -n database-platform annotate multisitepostgres orders \
  multisite-postgres.dev/address-observation="$(date -u +%FT%TZ)" --overwrite

for _ in $(seq 1 60); do
  revision="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.activeRevision}')"
  [[ "${revision}" == "2" ]] && break
  sleep 2
done
test "$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.activeRevision}')" = "2"
kubectl -n database-platform get configmap mspsql-plan-orders-vic -o jsonpath='{.data.envelope\.json}' |
  grep -q '10.0.0.1'

kubectl -n database-platform delete multisitepostgres orders --wait=false
kubectl -n database-platform wait --for=delete multisitepostgres/orders --timeout=180s
for site in vic nsw qld; do
  kind get kubeconfig --name "mspsql-${site}" |
    kubectl --kubeconfig=/dev/stdin get namespace orders-postgres >/dev/null
done
