#!/usr/bin/env bash

set -euo pipefail

kind_bin="$(command -v "${KIND:-kind}")"
kind() {
  "${kind_bin}" "$@"
}

clusters=(mspsql-hub mspsql-vic mspsql-nsw mspsql-qld)
kind_node_image="${KIND_NODE_IMAGE:-kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f}"
image="${IMG:-mspsql:test}"
agent_image="${AGENT_IMG:-mspsql-agent:test}"
gateway_image="${GATEWAY_IMG:-mspsql-gateway:test}"
wireguard_image="${WIREGUARD_IMG:-mspsql-wireguard:test}"
tun_plugin_image="${TUN_PLUGIN_IMG:-mspsql-tun-device-plugin:test}"
vault_image="hashicorp/vault:1.21.4"
cert_manager_version="v1.21.0"
metallb_version="v0.16.0"
temp_dir="$(mktemp -d)"
diagnostics_dir="${ARTIFACT_DIR:-${temp_dir}/diagnostics}"
mkdir -p "${diagnostics_dir}"
original_inotify_instances="$(sysctl -n fs.inotify.max_user_instances)"
inotify_instances_changed=false

if (( original_inotify_instances < 8192 )); then
  if [[ "${EUID}" -eq 0 ]]; then
    sysctl -q -w fs.inotify.max_user_instances=8192
  elif command -v sudo >/dev/null && sudo -n true 2>/dev/null; then
    sudo sysctl -q -w fs.inotify.max_user_instances=8192
  else
    echo "four-cluster KIND requires fs.inotify.max_user_instances >= 8192" >&2
    echo "current value is ${original_inotify_instances}; rerun with permission to raise it" >&2
    exit 1
  fi
  inotify_instances_changed=true
fi

cleanup() {
  status=$?
  if [[ "${status}" -ne 0 ]]; then
    for cluster in "${clusters[@]}"; do
      kind export logs "${diagnostics_dir}/${cluster}" --name "${cluster}" || true
    done
    if [[ -n "${KUBECONFIG:-}" ]]; then
      kubectl -n database-platform get multisitepostgres -o yaml || true
      kubectl -n mspsql-system logs deployment/mspsql-controller-manager \
        --all-containers --tail=200 || true
      kubectl -n mspsql-system get pods -o wide || true
      kubectl -n mspsql-system logs -l app.kubernetes.io/name=mspsql-wireguard \
        --all-containers --prefix --tail=200 || true
      kubectl get events -A --sort-by=.lastTimestamp | tail -150 || true
    fi
    for site in vic nsw qld; do
      site_kubeconfig="$(mktemp)"
      if kind get kubeconfig --name "mspsql-${site}" >"${site_kubeconfig}" 2>/dev/null; then
        kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent get pods -o wide || true
        kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent logs deployment/mspsql-agent \
          --all-containers --tail=200 || true
        kubectl --kubeconfig="${site_kubeconfig}" -n orders-postgres get pods -o wide || true
        kubectl --kubeconfig="${site_kubeconfig}" -n orders-postgres logs \
          -l multisite-postgres.dev/instance-uid --all-containers --prefix --tail=100 || true
        kubectl --kubeconfig="${site_kubeconfig}" get events -A \
          --sort-by=.lastTimestamp | tail -100 || true
      fi
      rm -f "${site_kubeconfig}"
    done
  fi
  for cluster in "${clusters[@]}"; do
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  done
  if [[ "${inotify_instances_changed}" == "true" ]]; then
    if [[ "${EUID}" -eq 0 ]]; then
      sysctl -q -w "fs.inotify.max_user_instances=${original_inotify_instances}" || true
    else
      sudo sysctl -q -w "fs.inotify.max_user_instances=${original_inotify_instances}" || true
    fi
  fi
  rm -rf "${temp_dir}"
}
trap cleanup EXIT

docker pull "${vault_image}"
curl -fsSL -o "${temp_dir}/cert-manager.yaml" \
  "https://github.com/cert-manager/cert-manager/releases/download/${cert_manager_version}/cert-manager.yaml"
echo "6e499c3f1ab356abe79a7853911f80cb09c213885bfdf81092fdff142ba63c4a  ${temp_dir}/cert-manager.yaml" |
  sha256sum -c -
curl -fsSL -o "${temp_dir}/metallb.yaml" \
  "https://raw.githubusercontent.com/metallb/metallb/${metallb_version}/config/manifests/metallb-native.yaml"
echo "b0b9be2802f10aa32d45308b4457d06cde0c70544712c8d0cf5511657ffd2b69  ${temp_dir}/metallb.yaml" |
  sha256sum -c -
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=mspsql-kind-ca" \
  -keyout "${temp_dir}/ca.key" -out "${temp_dir}/ca.crt" >/dev/null 2>&1

for cluster in "${clusters[@]}"; do
  created=false
  for attempt in 1 2 3; do
    if kind create cluster --name "${cluster}" --image "${kind_node_image}" --wait 120s; then
      created=true
      break
    fi
    kind export logs "${diagnostics_dir}/${cluster}-attempt-${attempt}" \
      --name "${cluster}" || true
    kind delete cluster --name "${cluster}" || true
  done
  if [[ "${created}" != "true" ]]; then
    echo "KIND cluster ${cluster} failed to start after three attempts" >&2
    exit 1
  fi
done

kind_subnet="$(docker network inspect kind \
  --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}' | awk '!/:/ {print; exit}')"
subnet_address="${kind_subnet%/*}"
prefix_length="${kind_subnet#*/}"
IFS=. read -r subnet_a subnet_b _ _ <<<"${subnet_address}"
if [[ "${prefix_length}" -gt 16 ]]; then
  echo "KIND network ${kind_subnet} is too small for isolated MetalLB test pools" >&2
  exit 1
fi

route_site_pool() {
  local target_site="$1"
  local first_address="$2"
  local target_node="mspsql-${target_site}-control-plane"
  local gateway
  gateway="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${target_node}")"
  for source_site in vic nsw qld; do
    [[ "${source_site}" == "${target_site}" ]] && continue
    for host in $(seq "${first_address}" "$((first_address + 19))"); do
      docker exec "mspsql-${source_site}-control-plane" ip route replace \
        "${subnet_a}.${subnet_b}.100.${host}/32" via "${gateway}"
    done
  done
}

pool_offset=10
for site in vic nsw qld; do
  kind load docker-image "${vault_image}" --name "mspsql-${site}"
  site_kubeconfig="$(mktemp)"
  kind get kubeconfig --name "mspsql-${site}" >"${site_kubeconfig}"
  ./test/kind/configure-vault.sh "${site_kubeconfig}" "${site}" \
    "${temp_dir}/ca.crt" "${temp_dir}/ca.key" \
    >"${diagnostics_dir}/${site}-vault.log" 2>&1 &
  vault_pid=$!
  ./test/kind/configure-platform.sh "${site_kubeconfig}" \
    "${subnet_a}.${subnet_b}.100.${pool_offset}" \
    "${subnet_a}.${subnet_b}.100.$((pool_offset + 19))" \
    "${temp_dir}/ca.crt" "${temp_dir}/ca.key" \
    "${temp_dir}/cert-manager.yaml" "${temp_dir}/metallb.yaml" \
    >"${diagnostics_dir}/${site}-platform.log" 2>&1 &
  platform_pid=$!
  vault_status=0
  platform_status=0
  wait "${vault_pid}" || vault_status=$?
  wait "${platform_pid}" || platform_status=$?
  if (( vault_status != 0 || platform_status != 0 )); then
    echo "${site} provisioning failed: vault=${vault_status} platform=${platform_status}" >&2
    tail -200 "${diagnostics_dir}/${site}-vault.log" >&2
    tail -200 "${diagnostics_dir}/${site}-platform.log" >&2
    exit 1
  fi
  pool_offset=$((pool_offset + 20))
  rm -f "${site_kubeconfig}"
done

docker build -t "${image}" .
docker build -f Dockerfile.agent -t "${agent_image}" .
docker build -f Dockerfile.gateway -t "${gateway_image}" .
docker build -f Dockerfile.wireguard -t "${wireguard_image}" .
docker build -f Dockerfile.tun-device-plugin -t "${tun_plugin_image}" .
kind load docker-image "${image}" --name mspsql-hub
kind load docker-image "${gateway_image}" --name mspsql-hub
for site in vic nsw qld; do
  kind load docker-image "${agent_image}" --name "mspsql-${site}"
done
for cluster in "${clusters[@]}"; do
  kind load docker-image "${wireguard_image}" --name "${cluster}"
  kind load docker-image "${tun_plugin_image}" --name "${cluster}"
done

export KUBECONFIG
KUBECONFIG="$(kind get kubeconfig-path --name mspsql-hub 2>/dev/null || true)"
if [[ -z "${KUBECONFIG}" ]]; then
  KUBECONFIG="$(mktemp)"
  kind get kubeconfig --name mspsql-hub >"${KUBECONFIG}"
fi

make kustomize
./test/kind/configure-platform.sh "${KUBECONFIG}" \
  "${subnet_a}.${subnet_b}.100.70" "${subnet_a}.${subnet_b}.100.89" \
  "${temp_dir}/ca.crt" "${temp_dir}/ca.key" \
  "${temp_dir}/cert-manager.yaml" "${temp_dir}/metallb.yaml"
for cluster in "${clusters[@]}"; do
  cluster_kubeconfig="${temp_dir}/${cluster}.kubeconfig"
  kind get kubeconfig --name "${cluster}" >"${cluster_kubeconfig}"
  ./bin/kustomize build config/tun-device-plugin |
    sed "s|ghcr.io/sindef/mspsql-tun-device-plugin:latest|${tun_plugin_image}|" |
    kubectl --kubeconfig="${cluster_kubeconfig}" apply -f -
  kubectl --kubeconfig="${cluster_kubeconfig}" -n kube-system rollout status \
    daemonset/mspsql-tun-device-plugin --timeout=180s
done

kubectl create namespace mspsql-system
./bin/kustomize build config/gateway |
  sed "s|ghcr.io/sindef/mspsql-gateway:latest|${gateway_image}|" |
  sed "s|ghcr.io/sindef/mspsql-wireguard:latest|${wireguard_image}|" |
  kubectl apply -f -
for _ in $(seq 1 120); do
  wireguard_ip="$(kubectl -n mspsql-system get service mspsql-wireguard \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
  [[ -n "${wireguard_ip}" ]] && break
  sleep 2
done
test -n "${wireguard_ip}"

hub_ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  mspsql-hub-control-plane)"
./bin/kustomize build test/kind/hub |
  sed "s|image: controller:latest|image: ${image}|" |
  sed "s|HUB_NODE_IP|${hub_ip}|g" |
  sed "s|SITE_AGENT_IMAGE|${agent_image}|g" |
  sed "s|WIREGUARD_IMAGE|${wireguard_image}|g" |
  sed "s|WIREGUARD_ENDPOINT|${wireguard_ip}:51820|g" |
  kubectl apply -f -
./test/kind/create-control-tls.sh mspsql-system "${hub_ip}" 10.254.0.1
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
  kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent create secret generic vault-ca \
    --from-file=ca.crt="${temp_dir}/ca.crt"
  rm -f "${site_kubeconfig}"
  for _ in $(seq 1 120); do
    phase="$(kubectl get siteregistration "${site}" -o jsonpath='{.status.phase}')"
    [[ "${phase}" == "Connected" ]] && break
    sleep 2
  done
  test "${phase}" = "Connected"
done

test "$(kubectl -n mspsql-system get secret \
  -l multisite-postgres.dev/wireguard-peer=true -o name | wc -l | tr -d ' ')" = "3"
for _ in $(seq 1 90); do
  active_gateway="$(kubectl -n mspsql-system get pods \
    -l app.kubernetes.io/name=mspsql-wireguard \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.containerStatuses[*]}{.name}={.ready}{" "}{end}{"\n"}{end}' |
    awk '/wireguard=true/ {print $1; exit}')"
  [[ -n "${active_gateway}" ]] && break
  sleep 2
done
test -n "${active_gateway}"
test "$(kubectl -n mspsql-system exec "${active_gateway}" -c wireguard -- \
  wg show wg0 peers | wc -l | tr -d ' ')" = "3"

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

# KIND clusters share a Docker network but MetalLB L2 advertisements do not cross
# cluster bridges. Explicit routes model the routed data-plane networks required
# between production sites while retaining real LoadBalancer Services and traffic.
route_site_pool vic 10
route_site_pool nsw 30
route_site_pool qld 50

for _ in $(seq 1 450); do
  phase="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.phase}')"
  [[ "${phase}" == "Ready" ]] && break
  sleep 2
done
test "${phase}" = "Ready"
test "$(kubectl -n database-platform get configmap \
  -l multisite-postgres.dev/instance-uid -o name | wc -l | tr -d ' ')" = "3"

kubectl apply -f test/kind/tenant.yaml
kubectl -n database-platform wait --for=condition=Ready postgresdatabase/orders-api --timeout=300s
kubectl -n database-platform wait --for=condition=Ready postgresuser/orders-application --timeout=300s
test "$(kubectl -n database-platform get postgresuser orders-application \
  -o jsonpath='{.status.credentialVersion}')" = "1"

primary="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.primary}')"
case "${primary}" in
  postgres-vic-*) primary_site=vic; replica_site=nsw ;;
  postgres-nsw-*) primary_site=nsw; replica_site=vic ;;
  *) echo "unexpected primary ${primary}" >&2; exit 1 ;;
esac
primary_kubeconfig="${temp_dir}/${primary_site}.kubeconfig"
replica_kubeconfig="${temp_dir}/${replica_site}.kubeconfig"
kind get kubeconfig --name "mspsql-${primary_site}" >"${primary_kubeconfig}"
kind get kubeconfig --name "mspsql-${replica_site}" >"${replica_kubeconfig}"
primary_password="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary}" \
  -c postgres-patroni -- env PGPASSWORD="${primary_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c 'CREATE TABLE mspsql_e2e (id integer PRIMARY KEY); INSERT INTO mspsql_e2e VALUES (1);'
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary}" \
  -c postgres-patroni -- env PGPASSWORD=application-secret PGSSLMODE=require \
  psql -h 127.0.0.1 -U orders_app -d orders -v ON_ERROR_STOP=1 \
  -c 'CREATE TABLE orders.application_write (id integer PRIMARY KEY); INSERT INTO orders.application_write VALUES (1);'
test -z "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres get secrets \
  -o name | grep 'mspsql-sql-.*-credential' || true)"
replica="postgres-${replica_site}-0"
replica_password="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
for _ in $(seq 1 90); do
  value="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres exec "${replica}" \
    -c postgres-patroni -- env PGPASSWORD="${replica_password}" PGSSLMODE=require \
    psql -h 127.0.0.1 -U postgres -d postgres -Atqc \
    'SELECT id FROM mspsql_e2e' 2>/dev/null || true)"
  [[ "${value}" == "1" ]] && break
  sleep 2
done
test "${value}" = "1"

kubectl -n database-platform delete multisitepostgres orders --wait=false
kubectl -n database-platform wait --for=delete multisitepostgres/orders --timeout=180s
for site in vic nsw qld; do
  kind get kubeconfig --name "mspsql-${site}" |
    kubectl --kubeconfig=/dev/stdin get namespace orders-postgres >/dev/null
done
