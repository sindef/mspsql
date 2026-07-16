#!/usr/bin/env bash

set -euo pipefail

clusters=(mspsql-hub mspsql-vic mspsql-nsw mspsql-qld)
image="${IMG:-mspsql:test}"

cleanup() {
  for cluster in "${clusters[@]}"; do
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

for cluster in "${clusters[@]}"; do
  kind create cluster --name "${cluster}" --wait 120s
done

for site in vic nsw qld; do
  site_kubeconfig="$(mktemp)"
  kind get kubeconfig --name "mspsql-${site}" >"${site_kubeconfig}"
  ./test/kind/configure-vault.sh "${site_kubeconfig}" "${site}"
  rm -f "${site_kubeconfig}"
done

docker build -t "${image}" .
kind load docker-image "${image}" --name mspsql-hub

export KUBECONFIG
KUBECONFIG="$(kind get kubeconfig-path --name mspsql-hub 2>/dev/null || true)"
if [[ -z "${KUBECONFIG}" ]]; then
  KUBECONFIG="$(mktemp)"
  kind get kubeconfig --name mspsql-hub >"${KUBECONFIG}"
fi

make kustomize
./bin/kustomize build test/kind/hub |
  sed "s|image: controller:latest|image: ${image}|" |
  kubectl apply -f -
kubectl -n mspsql-system rollout status deployment/mspsql-controller-manager --timeout=180s

for site in vic nsw qld; do
  uid="$(kind get kubeconfig --name "mspsql-${site}" | kubectl --kubeconfig=/dev/stdin get namespace kube-system -o jsonpath='{.metadata.uid}')"
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
  kubectl patch siteregistration "${site}" --type=merge --subresource=status -p "$(jq -cn \
    --arg uid "${uid}" '{status:{clusterUID:$uid,phase:"Connected",discoveredStorageClasses:[{name:"standard",provisioner:"kind"}]}}')"
done

kubectl create namespace database-platform
kubectl apply -f test/kind/instance.yaml

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
sleep 2
test "$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.phase}')" = "Deleting"
kubectl -n database-platform annotate multisitepostgres orders multisite-postgres.dev/force-orphan=true --overwrite
kubectl -n database-platform wait --for=delete multisitepostgres/orders --timeout=60s
