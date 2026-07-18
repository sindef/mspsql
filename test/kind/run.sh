#!/usr/bin/env bash

set -euo pipefail

kind_bin="$(command -v "${KIND:-kind}")"
command -v jq >/dev/null
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
upgrade_image="${UPGRADE_IMG:-mspsql-postgres-upgrade:17-to-18}"
failed_upgrade_image="mspsql-postgres-upgrade:injected-failure"
previous_revision="006c1d864b1dfab5fcf29dbb9921fff2216ac322"
previous_image="mspsql:previous"
previous_agent_image="mspsql-agent:previous"
vault_image="hashicorp/vault:1.21.4"
minio_image="minio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
registry_image="registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
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
  for cluster in "${clusters[@]}"; do
    docker unpause "${cluster}-control-plane" >/dev/null 2>&1 || true
  done
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
  docker rm -f mspsql-minio >/dev/null 2>&1 || true
  docker rm -f mspsql-registry >/dev/null 2>&1 || true
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

docker rm -f mspsql-minio mspsql-registry >/dev/null 2>&1 || true

docker pull "${vault_image}"
docker pull "${minio_image}"
docker pull "${registry_image}"
curl -fsSL -o "${temp_dir}/cert-manager.yaml" \
  "https://github.com/cert-manager/cert-manager/releases/download/${cert_manager_version}/cert-manager.yaml"
echo "6e499c3f1ab356abe79a7853911f80cb09c213885bfdf81092fdff142ba63c4a  ${temp_dir}/cert-manager.yaml" |
  sha256sum -c -
curl -fsSL -o "${temp_dir}/metallb.yaml" \
  "https://raw.githubusercontent.com/metallb/metallb/${metallb_version}/config/manifests/metallb-native.yaml"
echo "b0b9be2802f10aa32d45308b4457d06cde0c70544712c8d0cf5511657ffd2b69  ${temp_dir}/metallb.yaml" |
  sha256sum -c -
curl -fsSL --retry 5 -o "${temp_dir}/csi-hostpath.tar.gz" \
  "https://github.com/kubernetes-csi/csi-driver-host-path/archive/refs/tags/v1.18.0.tar.gz"
curl -fsSL --retry 5 -o "${temp_dir}/snapshotter.tar.gz" \
  "https://github.com/kubernetes-csi/external-snapshotter/archive/refs/tags/v8.6.0.tar.gz"
echo "03d581877ed8a0a87851c59ca3d50b04663e17f8d5310eb964002b0a7fd73421  ${temp_dir}/csi-hostpath.tar.gz" |
  sha256sum -c -
echo "44bc3f2bb78cbd7fb6ebff5f5bb488e2171e2cef42849f85d28b85446f8956c4  ${temp_dir}/snapshotter.tar.gz" |
  sha256sum -c -
mkdir -p "${temp_dir}/csi-hostpath" "${temp_dir}/snapshotter"
tar -xzf "${temp_dir}/csi-hostpath.tar.gz" --strip-components=1 -C "${temp_dir}/csi-hostpath"
tar -xzf "${temp_dir}/snapshotter.tar.gz" --strip-components=1 -C "${temp_dir}/snapshotter"
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

mkdir -p "${temp_dir}/minio-certs/CAs"
docker create --name mspsql-minio --network kind \
  -e MINIO_ROOT_USER=access -e MINIO_ROOT_PASSWORD=secretsecret \
  -v "${temp_dir}/minio-certs:/root/.minio/certs:ro" \
  "${minio_image}" server /data --address :9000 >/dev/null
docker start mspsql-minio >/dev/null
minio_ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  mspsql-minio)"
test -n "${minio_ip}"
openssl req -newkey rsa:2048 -nodes -subj "/CN=${minio_ip}" \
  -keyout "${temp_dir}/minio-certs/private.key" \
  -out "${temp_dir}/minio.csr" >/dev/null 2>&1
printf 'subjectAltName=IP:%s\nextendedKeyUsage=serverAuth\n' "${minio_ip}" >"${temp_dir}/minio-extensions.cnf"
openssl x509 -req -days 2 -sha256 -in "${temp_dir}/minio.csr" \
  -CA "${temp_dir}/ca.crt" -CAkey "${temp_dir}/ca.key" \
  -CAserial "${temp_dir}/minio-ca.srl" -CAcreateserial \
  -extfile "${temp_dir}/minio-extensions.cnf" \
  -out "${temp_dir}/minio-certs/public.crt" >/dev/null 2>&1
cp "${temp_dir}/ca.crt" "${temp_dir}/minio-certs/CAs/ca.crt"
docker restart mspsql-minio >/dev/null
for _ in $(seq 1 60); do
  if curl -fsS --cacert "${temp_dir}/ca.crt" \
    "https://${minio_ip}:9000/minio/health/live" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS --cacert "${temp_dir}/ca.crt" "https://${minio_ip}:9000/minio/health/live" >/dev/null
docker exec mspsql-minio mc alias set local "https://${minio_ip}:9000" access secretsecret --insecure
docker exec mspsql-minio mc mb --ignore-existing local/mspsql-backups --insecure

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
  site_kubeconfig="${temp_dir}/mspsql-${site}.kubeconfig"
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
  if [[ "${site}" != "qld" ]] && ! ./test/kind/configure-csi-snapshot.sh \
    "${site_kubeconfig}" "${temp_dir}/csi-hostpath" "${temp_dir}/snapshotter" \
    >"${diagnostics_dir}/${site}-csi.log" 2>&1; then
    tail -300 "${diagnostics_dir}/${site}-csi.log" >&2
    exit 1
  fi
  pool_offset=$((pool_offset + 20))
done

docker build -t "${image}" .
docker build -f Dockerfile.agent -t "${agent_image}" .
docker build -f Dockerfile.gateway -t "${gateway_image}" .
docker build -f Dockerfile.wireguard -t "${wireguard_image}" .
docker build -f Dockerfile.tun-device-plugin -t "${tun_plugin_image}" .
docker build -f Dockerfile.upgrade -t "${upgrade_image}" .
docker build -f test/kind/Dockerfile.upgrade-failure -t "${failed_upgrade_image}" .
previous_source="${temp_dir}/previous"
mkdir -p "${previous_source}"
git archive "${previous_revision}" | tar -x -C "${previous_source}"
docker build -t "${previous_image}" "${previous_source}"
docker build -f "${previous_source}/Dockerfile.agent" -t "${previous_agent_image}" \
  "${previous_source}"
docker run -d --name mspsql-registry --network kind -p 127.0.0.1::5000 \
  "${registry_image}" >/dev/null
registry_port="$(docker port mspsql-registry 5000/tcp | awk -F: '{print $NF}')"
registry_ip="$(docker inspect mspsql-registry \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')"
test -n "${registry_port}"
test -n "${registry_ip}"
docker tag "${upgrade_image}" "localhost:${registry_port}/mspsql-postgres-upgrade:17-to-18"
upgrade_push="$(docker push "localhost:${registry_port}/mspsql-postgres-upgrade:17-to-18")"
upgrade_digest="$(awk '/digest:/ {print $3}' <<<"${upgrade_push}")"
[[ "${upgrade_digest}" =~ ^sha256:[0-9a-f]{64}$ ]]
upgrade_ref="${registry_ip}:5000/mspsql-postgres-upgrade@${upgrade_digest}"
docker tag "${failed_upgrade_image}" \
  "localhost:${registry_port}/mspsql-postgres-upgrade:injected-failure"
failed_upgrade_push="$(docker push \
  "localhost:${registry_port}/mspsql-postgres-upgrade:injected-failure")"
failed_upgrade_digest="$(awk '/digest:/ {print $3}' <<<"${failed_upgrade_push}")"
[[ "${failed_upgrade_digest}" =~ ^sha256:[0-9a-f]{64}$ ]]
failed_upgrade_ref="${registry_ip}:5000/mspsql-postgres-upgrade@${failed_upgrade_digest}"
for site in vic nsw; do
  node="mspsql-${site}-control-plane"
  registry_directory="/etc/containerd/certs.d/${registry_ip}:5000"
  docker exec "${node}" mkdir -p "${registry_directory}"
  printf 'server = "http://%s:5000"\n\n[host."http://%s:5000"]\n  capabilities = ["pull", "resolve"]\n' \
    "${registry_ip}" "${registry_ip}" |
    docker exec -i "${node}" sh -c "cat > '${registry_directory}/hosts.toml'"
done
kind load docker-image "${image}" --name mspsql-hub
kind load docker-image "${previous_image}" --name mspsql-hub
kind load docker-image "${gateway_image}" --name mspsql-hub
for site in vic nsw qld; do
  kind load docker-image "${agent_image}" --name "mspsql-${site}"
  kind load docker-image "${previous_agent_image}" --name "mspsql-${site}"
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
./bin/kustomize build "${previous_source}/test/kind/hub" |
  sed "s|image: controller:latest|image: ${previous_image}|" |
  sed "s|HUB_NODE_IP|${hub_ip}|g" |
  sed "s|SITE_AGENT_IMAGE|${previous_agent_image}|g" |
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
    postgres: [standard, csi-hostpath]
  storageRollbackPolicies:
  - storageClassName: csi-hostpath
    strategy: VolumeSnapshot
    volumeSnapshotClassName: csi-hostpath-snapclass
  permittedIssuers:
    etcd:
    - {name: test, kind: ClusterIssuer, group: cert-manager.io}
    postgres:
    - {name: test, kind: ClusterIssuer, group: cert-manager.io}
    pgpool:
    - {name: test, kind: ClusterIssuer, group: cert-manager.io}
    backup:
    - {name: test, kind: ClusterIssuer, group: cert-manager.io}
  metallbAddressPools: [database-services]
EOF
  for _ in $(seq 1 60); do
    registration_url="$(kubectl get siteregistration "${site}" -o jsonpath='{.status.registrationURL}')"
    [[ -n "${registration_url}" ]] && break
    sleep 1
  done
  test -n "${registration_url}"
  site_kubeconfig="${temp_dir}/mspsql-${site}.kubeconfig"
  registration_bundle="${temp_dir}/${site}-registration.yaml"
  for _ in $(seq 1 120); do
    if curl -fsS --connect-timeout 2 --max-time 10 "${registration_url}" \
      -o "${registration_bundle}" && test -s "${registration_bundle}"; then
      break
    fi
    sleep 1
  done
  test -s "${registration_bundle}"
  kubectl --kubeconfig="${site_kubeconfig}" apply -f "${registration_bundle}"
  kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent create secret generic vault-ca \
    --from-file=ca.crt="${temp_dir}/ca.crt"
  kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent create secret generic minio-ca \
    --from-file=ca.crt="${temp_dir}/ca.crt"
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
    -l multisite-postgres.dev/gateway-active=true \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  gateway_endpoints="$(kubectl -n mspsql-system get endpointslice \
    -l kubernetes.io/service-name=mspsql-wireguard \
    -o json | jq '[.items[]?.endpoints[]? | select(.conditions.ready == true)] | length')"
  [[ -n "${active_gateway}" && "${gateway_endpoints}" == "1" ]] && break
  sleep 2
done
test -n "${active_gateway}"
test "${gateway_endpoints}" = "1"
test "$(kubectl -n mspsql-system exec "${active_gateway}" -c wireguard -- \
  wg show wg0 peers | wc -l | tr -d ' ')" = "3"
failed_gateway="${active_gateway}"
kubectl -n mspsql-system delete pod "${failed_gateway}" --wait=false
for _ in $(seq 1 90); do
  active_gateway="$(kubectl -n mspsql-system get pods \
    -l multisite-postgres.dev/gateway-active=true \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  gateway_endpoints="$(kubectl -n mspsql-system get endpointslice \
    -l kubernetes.io/service-name=mspsql-wireguard \
    -o json | jq '[.items[]?.endpoints[]? | select(.conditions.ready == true)] | length')"
  [[ -n "${active_gateway}" && "${active_gateway}" != "${failed_gateway}" &&
    "${gateway_endpoints}" == "1" ]] && break
  sleep 2
done
test -n "${active_gateway}"
test "${active_gateway}" != "${failed_gateway}"
test "${gateway_endpoints}" = "1"
test "$(kubectl -n mspsql-system exec "${active_gateway}" -c wireguard -- \
  wg show wg0 peers | wc -l | tr -d ' ')" = "3"

kubectl create namespace database-platform
if validation_error="$(sed 's/deletionPolicy: Retain/deletionPolicy: Destroy/' \
  test/kind/instance.yaml | kubectl apply -f - 2>&1)"; then
  echo "invalid deletionPolicy was accepted" >&2
  exit 1
fi
grep -q 'Unsupported value.*Destroy' <<<"${validation_error}"
sed "s|MINIO_ENDPOINT|${minio_ip}|g" test/kind/instance.yaml | kubectl apply -f -

for site in vic nsw qld; do
  site_kubeconfig="${temp_dir}/mspsql-${site}.kubeconfig"
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
  -l multisite-postgres.dev/site-registration-uid -o name | wc -l | tr -d ' ')" = "3"
for _ in $(seq 1 30); do
  hub_events="$(kubectl -n database-platform get events.events.k8s.io -o json | jq \
    '[.items[] | select(.reportingController == "multisitepostgres" and .regarding.name == "orders")] | length')"
  [[ "${hub_events}" -gt 0 ]] && break
  sleep 2
done
test "${hub_events}" -gt 0
hub_metrics="$(kubectl get --raw \
  '/api/v1/namespaces/mspsql-system/services/http:mspsql-controller-external:8080/proxy/metrics')"
grep -q '^mspsql_agent_connected' <<<"${hub_metrics}"
grep -q '^mspsql_plan_revision_lag' <<<"${hub_metrics}"
grep -q '^mspsql_synchronous_write_available' <<<"${hub_metrics}"
for site in vic nsw qld; do
  site_kubeconfig="${temp_dir}/mspsql-${site}.kubeconfig"
  agent_metrics="$(kubectl --kubeconfig="${site_kubeconfig}" get --raw \
    '/api/v1/namespaces/mspsql-agent/services/http:mspsql-agent-metrics:8080/proxy/metrics')"
  grep -q '^mspsql_agent_connected' <<<"${agent_metrics}"
  grep -q '^mspsql_agent_certificate_expiry_timestamp_seconds' <<<"${agent_metrics}"
  for _ in $(seq 1 30); do
    agent_events="$(kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent \
      get events.events.k8s.io -o json | jq \
      '[.items[] | select(.reportingController == "multisite-postgres.dev/site-agent" and .regarding.kind == "ConfigMap")] | length')"
    [[ "${agent_events}" -gt 0 ]] && break
    sleep 2
  done
  test "${agent_events}" -gt 0
done

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
primary_kubeconfig="${temp_dir}/mspsql-${primary_site}.kubeconfig"
replica_kubeconfig="${temp_dir}/mspsql-${replica_site}.kubeconfig"
primary_pod="${primary}-0"
primary_password="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary_pod}" \
  -c postgres-patroni -- env PGPASSWORD="${primary_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c 'CREATE TABLE mspsql_e2e (id integer PRIMARY KEY); INSERT INTO mspsql_e2e VALUES (1);'
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary_pod}" \
  -c postgres-patroni -- env PGPASSWORD=application-secret PGSSLMODE=require \
  psql -h 127.0.0.1 -U orders_app -d orders -v ON_ERROR_STOP=1 \
  -c 'CREATE TABLE orders.application_write (id integer PRIMARY KEY); INSERT INTO orders.application_write VALUES (1);'
test -z "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres get secrets \
  -o name | grep 'mspsql-sql-.*-credential' || true)"
replica="postgres-${replica_site}-0"
replica_pod="${replica}-0"
replica_password="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
for _ in $(seq 1 90); do
  value="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres exec "${replica_pod}" \
    -c postgres-patroni -- env PGPASSWORD="${replica_password}" PGSSLMODE=require \
    psql -h 127.0.0.1 -U postgres -d postgres -Atqc \
    'SELECT id FROM mspsql_e2e' 2>/dev/null || true)"
  [[ "${value}" == "1" ]] && break
  sleep 2
done
test "${value}" = "1"

pgpool_address="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get service "pgpool-${primary_site}" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
for _ in $(seq 1 60); do
  pgpool_value="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres exec "${replica_pod}" \
    -c postgres-patroni -- env PGPASSWORD=application-secret PGSSLMODE=require PGCONNECT_TIMEOUT=5 \
    psql -h "${pgpool_address}" -U orders_app -d orders -Atqc \
    'SELECT id FROM orders.application_write' 2>/dev/null || true)"
  [[ "${pgpool_value}" == "1" ]] && break
  sleep 2
done
if [[ "${pgpool_value}" != "1" ]]; then
  echo "cross-site Pgpool query through ${pgpool_address} returned ${pgpool_value@Q}" >&2
  exit 1
fi

controller_uid="$(kubectl -n mspsql-system get deployment mspsql-controller-manager \
  -o jsonpath='{.metadata.uid}')"
./bin/kustomize build test/kind/hub |
  sed "s|image: controller:latest|image: ${image}|" |
  sed "s|HUB_NODE_IP|${hub_ip}|g" |
  sed "s|SITE_AGENT_IMAGE|${agent_image}|g" |
  sed "s|WIREGUARD_IMAGE|${wireguard_image}|g" |
  sed "s|WIREGUARD_ENDPOINT|${wireguard_ip}:51820|g" |
  kubectl apply -f -
kubectl -n mspsql-system rollout status deployment/mspsql-controller-manager --timeout=180s
test "${controller_uid}" = "$(kubectl -n mspsql-system get deployment \
  mspsql-controller-manager -o jsonpath='{.metadata.uid}')"
test "$(kubectl get crd multisitepostgres.multisite-postgres.dev \
  -o jsonpath='{.status.storedVersions[*]}')" = "v1alpha1"
test "$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.metadata.uid}')" != ""
kubectl -n database-platform wait --for=condition=Ready multisitepostgres/orders --timeout=300s
for site in vic nsw qld; do
  site_kubeconfig="${temp_dir}/mspsql-${site}.kubeconfig"
  agent_uid="$(kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent \
    get deployment mspsql-agent -o jsonpath='{.metadata.uid}')"
  kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent set image \
    deployment/mspsql-agent site-agent="${agent_image}"
  kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent rollout status \
    deployment/mspsql-agent --timeout=180s
  test "${agent_uid}" = "$(kubectl --kubeconfig="${site_kubeconfig}" -n mspsql-agent \
    get deployment mspsql-agent -o jsonpath='{.metadata.uid}')"
done
for site in vic nsw qld; do
  for _ in $(seq 1 90); do
    phase="$(kubectl get siteregistration "${site}" -o jsonpath='{.status.phase}')"
    [[ "${phase}" == "Connected" ]] && break
    sleep 2
  done
  test "${phase}" = "Connected"
done
kubectl -n database-platform wait --for=condition=Ready multisitepostgres/orders --timeout=300s
test "$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres exec "${replica_pod}" \
  -c postgres-patroni -- env PGPASSWORD="${replica_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc 'SELECT id FROM mspsql_e2e')" = "1"

vault_selector="$(kubectl --kubeconfig="${replica_kubeconfig}" -n vault get service vault \
  -o json | jq -c '.spec.selector')"
kubectl --kubeconfig="${replica_kubeconfig}" -n vault patch service vault --type=merge \
  -p '{"spec":{"selector":{"multisite-postgres.dev/unavailable":"true"}}}'
for _ in $(seq 1 90); do
  vault_ready="$(kubectl -n database-platform get multisitepostgres orders -o json | \
    jq -r --arg site "${replica_site}" '.status.sites[] | select(.name == $site) |
      .conditions[] | select(.type == "VaultReady") | .status')"
  [[ "${vault_ready}" == "False" ]] && break
  sleep 2
done
test "${vault_ready}" = "False"
test "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary_pod}" \
  -c postgres-patroni -- env PGPASSWORD="${primary_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc 'SELECT id FROM mspsql_e2e')" = "1"
kubectl --kubeconfig="${replica_kubeconfig}" -n vault patch service vault --type=json \
  -p "$(jq -cn --argjson selector "${vault_selector}" \
    '[{op: "replace", path: "/spec/selector", value: $selector}]')"
for _ in $(seq 1 60); do
  vault_endpoints="$(kubectl --kubeconfig="${replica_kubeconfig}" -n vault \
    get endpointslice -l kubernetes.io/service-name=vault -o json | \
    jq '[.items[]?.endpoints[]? | select(.conditions.ready == true)] | length')"
  [[ "${vault_endpoints}" -ge 1 ]] && break
  sleep 1
done
test "${vault_endpoints}" -ge 1
kubectl -n database-platform wait --for=condition=Ready multisitepostgres/orders --timeout=300s

replica_tls_uid="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
  get secret "${replica}-tls" -o jsonpath='{.metadata.uid}')"
replica_pod_uid="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
  get pod "${replica_pod}" -o jsonpath='{.metadata.uid}')"
kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres delete secret "${replica}-tls"
for _ in $(seq 1 120); do
  rotated_tls_uid="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
    get secret "${replica}-tls" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  rotated_pod_uid="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
    get pod "${replica_pod}" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  pod_ready="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
    get pod "${replica_pod}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
    2>/dev/null || true)"
  [[ -n "${rotated_tls_uid}" && "${rotated_tls_uid}" != "${replica_tls_uid}" &&
    -n "${rotated_pod_uid}" && "${rotated_pod_uid}" != "${replica_pod_uid}" &&
    "${pod_ready}" == "True" ]] && break
  sleep 2
done
test "${rotated_tls_uid}" != "${replica_tls_uid}"
test "${rotated_pod_uid}" != "${replica_pod_uid}"
test "${pod_ready}" = "True"
kubectl -n database-platform wait --for=condition=Ready multisitepostgres/orders --timeout=300s
test "$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres exec "${replica_pod}" \
  -c postgres-patroni -- env PGPASSWORD="${replica_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc 'SELECT id FROM mspsql_e2e')" = "1"

postgres_signature="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get statefulset "${primary}" \
  -o jsonpath='{.metadata.uid}:{.metadata.generation}:{.spec.template.metadata.annotations}')"
pgpool_signature="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get deployment "pgpool-${primary_site}" \
  -o jsonpath='{.metadata.uid}:{.metadata.generation}:{.spec.template.metadata.annotations}')"
patroni_signature="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get configmap "patroni-${primary_site}" -o jsonpath='{.metadata.uid}:{.metadata.resourceVersion}')"
sleep 70
test "${postgres_signature}" = "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get statefulset "${primary}" \
  -o jsonpath='{.metadata.uid}:{.metadata.generation}:{.spec.template.metadata.annotations}')"
test "${pgpool_signature}" = "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get deployment "pgpool-${primary_site}" \
  -o jsonpath='{.metadata.uid}:{.metadata.generation}:{.spec.template.metadata.annotations}')"
test "${patroni_signature}" = "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get configmap "patroni-${primary_site}" -o jsonpath='{.metadata.uid}:{.metadata.resourceVersion}')"

kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres patch \
  deployment "pgpool-${primary_site}" --type=merge -p '{"spec":{"replicas":0}}'
for _ in $(seq 1 90); do
  replicas="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres get \
    deployment "pgpool-${primary_site}" -o jsonpath='{.spec.replicas}')"
  [[ "${replicas}" == "1" ]] && break
  sleep 2
done
test "${replicas}" = "1"
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres rollout status \
  deployment/"pgpool-${primary_site}" --timeout=180s
pgpool_config_uid="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get configmap "pgpool-${primary_site}" -o jsonpath='{.metadata.uid}')"
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres delete \
  configmap "pgpool-${primary_site}"
for _ in $(seq 1 90); do
  recreated_uid="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
    get configmap "pgpool-${primary_site}" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  [[ -n "${recreated_uid}" && "${recreated_uid}" != "${pgpool_config_uid}" ]] && break
  sleep 2
done
test -n "${recreated_uid}"
test "${recreated_uid}" != "${pgpool_config_uid}"
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres get \
  configmap "pgpool-${primary_site}" -o jsonpath='{.data.pgpool\.conf}' |
  grep -q "backend_clustering_mode = 'streaming_replication'"

kubectl -n database-platform wait --for=condition=BackupReady \
  multisitepostgres/orders --timeout=900s
test -n "$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.status.lastBackupTime}')"
test -n "$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.status.recoveryWindowStart}')"
backup_jobs=0
for site in vic nsw; do
  site_config="${temp_dir}/mspsql-${site}.kubeconfig"
  completed="$(kubectl --kubeconfig="${site_config}" -n orders-postgres get jobs \
    -l multisite-postgres.dev/operation-uid \
    -o jsonpath='{range .items[?(@.status.succeeded==1)]}{.metadata.name}{"\n"}{end}' | wc -l | tr -d ' ')"
  backup_jobs=$((backup_jobs + completed))
done
test "${backup_jobs}" -ge 1
test "$(docker exec mspsql-minio mc find local/mspsql-backups --insecure | wc -l | tr -d ' ')" -gt 0

upgrade_source_primary="${primary}"
kubectl -n database-platform apply -f - <<'EOF'
apiVersion: multisite-postgres.dev/v1alpha1
kind: PostgresUpgrade
metadata:
  name: orders-patch
spec:
  instanceRef: orders
  targetImage: percona/percona-distribution-postgresql@sha256:f10a110088699edd09ab706446f2c55db9390dd56381d5d0032ee70e3fe01d2a
  targetMajorVersion: 17
  serviceRestorationTarget: 10m
EOF
kubectl -n database-platform wait --for=condition=Ready \
  postgresupgrade/orders-patch --timeout=900s
test "$(kubectl -n database-platform get postgresupgrade orders-patch \
  -o jsonpath='{.status.phase}')" = "Completed"
test "$(kubectl -n database-platform get postgresupgrade orders-patch \
  -o json | jq '.status.upgradedMembers | length')" = "2"
target_image="$(kubectl -n database-platform get postgresupgrade orders-patch \
  -o jsonpath='{.spec.targetImage}')"
test "$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.spec.postgres.image}')" = "${target_image}"
for site in vic nsw; do
  site_config="${temp_dir}/mspsql-${site}.kubeconfig"
  test "$(kubectl --kubeconfig="${site_config}" -n orders-postgres get statefulset \
    "postgres-${site}-0" -o jsonpath='{.spec.template.spec.containers[?(@.name=="postgres-patroni")].image}')" = \
    "${target_image}"
done
primary="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.primary}')"
test -n "${primary}"
test "${primary}" != "${upgrade_source_primary}"
case "${primary}" in
  postgres-vic-*) primary_site=vic; replica_site=nsw ;;
  postgres-nsw-*) primary_site=nsw; replica_site=vic ;;
  *) echo "unexpected primary after patch upgrade ${primary}" >&2; exit 1 ;;
esac
primary_kubeconfig="${temp_dir}/mspsql-${primary_site}.kubeconfig"
replica_kubeconfig="${temp_dir}/mspsql-${replica_site}.kubeconfig"
primary_pod="${primary}-0"
replica="postgres-${replica_site}-0"
replica_pod="${replica}-0"
primary_password="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
replica_password="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
test "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary_pod}" \
  -c postgres-patroni -- env PGPASSWORD="${primary_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc 'SELECT id FROM mspsql_e2e')" = "1"

# A completed rolling upgrade can briefly precede Patroni's refreshed
# synchronous-member observation. Prove the failover target has replayed the
# current primary flush position before removing the primary site.
for _ in $(seq 1 120); do
  primary_lsn="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
    exec "${primary_pod}" -c postgres-patroni -- \
    env PGPASSWORD="${primary_password}" PGSSLMODE=require \
    psql -h 127.0.0.1 -U postgres -d postgres -Atqc 'SELECT pg_current_wal_flush_lsn()')"
  replica_caught_up="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
    exec "${replica_pod}" -c postgres-patroni -- \
    env PGPASSWORD="${replica_password}" PGSSLMODE=require \
    psql -h 127.0.0.1 -U postgres -d postgres -Atqc \
    "SELECT pg_last_wal_replay_lsn() >= '${primary_lsn}'::pg_lsn" 2>/dev/null || true)"
  synchronous_state="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
    exec "${primary_pod}" -c postgres-patroni -- \
    env PGPASSWORD="${primary_password}" PGSSLMODE=require \
    psql -h 127.0.0.1 -U postgres -d postgres -Atqc \
    "SELECT sync_state FROM pg_stat_replication WHERE application_name = '${replica}'" \
    2>/dev/null || true)"
  [[ "${replica_caught_up}" == "t" && "${synchronous_state}" == "sync" ]] && break
  sleep 2
done
test "${replica_caught_up}" = "t"
test "${synchronous_state}" = "sync"

# Patroni's DCS sync assignment can trail pg_stat_replication briefly after a
# rolling restart. Promotion is deliberately unsafe until every observer sees
# the target in Patroni's synchronous failover set.
for _ in $(seq 1 120); do
  synchronous_ready="$(kubectl -n database-platform get multisitepostgres orders \
    -o jsonpath='{.status.conditions[?(@.type=="SynchronousReplicationReady")].status}')"
  observed_standby="$(kubectl -n database-platform get multisitepostgres orders -o json | \
    jq -r --arg replica "${replica}" \
      '[.status.sites[] | select(.synchronousStandbys // [] | index($replica))] | length')"
  [[ "${synchronous_ready}" == "True" && "${observed_standby}" -eq 2 ]] && break
  sleep 2
done
test "${synchronous_ready}" = "True"
test "${observed_standby}" -eq 2

docker pause "mspsql-${primary_site}-control-plane" >/dev/null
for _ in $(seq 1 120); do
  promoted="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres exec "${replica_pod}" \
    -c postgres-patroni -- env PGPASSWORD="${replica_password}" PGSSLMODE=require \
    psql -h 127.0.0.1 -U postgres -d postgres -Atqc \
    'SELECT NOT pg_is_in_recovery()' 2>/dev/null || true)"
  [[ "${promoted}" == "t" ]] && break
  sleep 2
done
test "${promoted}" = "t"
test "$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres exec "${replica_pod}" \
  -c postgres-patroni -- env PGPASSWORD="${replica_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc 'SELECT id FROM mspsql_e2e')" = "1"
for _ in $(seq 1 180); do
  ready_status="$(kubectl -n database-platform get multisitepostgres orders \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
  [[ "${ready_status}" == "False" ]] && break
  sleep 2
done
test "${ready_status}" = "False"
docker unpause "mspsql-${primary_site}-control-plane" >/dev/null
for _ in $(seq 1 120); do
  kubectl --kubeconfig="${primary_kubeconfig}" get --raw=/readyz >/dev/null 2>&1 && break
  sleep 2
done
kubectl --kubeconfig="${primary_kubeconfig}" get --raw=/readyz >/dev/null
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres wait --for=condition=Ready \
  pod/"${primary_pod}" --timeout=300s
for _ in $(seq 1 300); do
  primary="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.primary}')"
  ready_status="$(kubectl -n database-platform get multisitepostgres orders \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
  [[ -n "${primary}" && "${ready_status}" == "True" ]] && break
  sleep 2
done
test -n "${primary}"
test "${ready_status}" = "True"
test "${primary}" = "${replica}"
case "${primary}" in
  postgres-vic-*) primary_site=vic; replica_site=nsw ;;
  postgres-nsw-*) primary_site=nsw; replica_site=vic ;;
  *) echo "unexpected primary after failover ${primary}" >&2; exit 1 ;;
esac
primary_kubeconfig="${temp_dir}/mspsql-${primary_site}.kubeconfig"
replica_kubeconfig="${temp_dir}/mspsql-${replica_site}.kubeconfig"
primary_pod="${primary}-0"
replica="postgres-${replica_site}-0"
replica_pod="${replica}-0"
primary_password="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
replica_password="$(kubectl --kubeconfig="${replica_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"

kubectl -n database-platform patch multisitepostgres orders --type=merge \
  -p '{"spec":{"backup":{"schedules":{"full":"0 0 1 1 *","timezone":"UTC"}}}}'
expected_generation="$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.metadata.generation}')"
kubectl -n database-platform wait --for=jsonpath='{.status.observedGeneration}'="${expected_generation}" \
  multisitepostgres/orders --timeout=300s
restore_target_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
sleep 2
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary_pod}" \
  -c postgres-patroni -- env PGPASSWORD="${primary_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c 'INSERT INTO mspsql_e2e VALUES (2); SELECT pg_switch_wal();'
sleep 20
source_uid="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.metadata.uid}')"
kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary_pod}" \
  -c postgres-patroni -- pgbackrest --config=/etc/pgbackrest/pgbackrest.conf \
  --stanza="mspsql-${source_uid}" check

kubectl -n database-platform apply -f - <<EOF
apiVersion: multisite-postgres.dev/v1alpha1
kind: PostgresRestore
metadata:
  name: orders-pitr
spec:
  sourceInstanceRef: orders
  targetInstanceRef: orders-recovered
  targetTime: "${restore_target_time}"
EOF
kubectl -n database-platform wait --for=condition=Completed \
  postgresrestore/orders-pitr --timeout=1200s
test "$(kubectl -n database-platform get postgresrestore orders-pitr \
  -o jsonpath='{.status.recoveredTo}')" = "${restore_target_time}"
restored_primary="$(kubectl -n database-platform get multisitepostgres orders-recovered \
  -o jsonpath='{.status.primary}')"
case "${restored_primary}" in
  postgres-vic-*) restored_site=vic ;;
  postgres-nsw-*) restored_site=nsw ;;
  *) echo "unexpected restored primary ${restored_primary}" >&2; exit 1 ;;
esac
restored_kubeconfig="${temp_dir}/mspsql-${restored_site}.kubeconfig"
restored_namespace="orders-recovered-${restored_site}"
restored_password="$(kubectl --kubeconfig="${restored_kubeconfig}" -n "${restored_namespace}" \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
restored_rows="$(kubectl --kubeconfig="${restored_kubeconfig}" -n "${restored_namespace}" \
  exec "${restored_primary}-0" -c postgres-patroni -- \
  env PGPASSWORD="${restored_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc 'SELECT count(*) FROM mspsql_e2e')"
test "${restored_rows}" = "1"

benchmark_tested_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kubectl -n database-platform apply -f - <<EOF
apiVersion: multisite-postgres.dev/v1alpha1
kind: PostgresUpgrade
metadata:
  name: orders-pg18-failure
spec:
  instanceRef: orders
  targetImage: percona/percona-distribution-postgresql@sha256:e2cc221e458a11b8cc39cbf74e947289568d3e182de5036eadef66d9c15569fb
  targetMajorVersion: 18
  upgradeImage: ${failed_upgrade_ref}
  serviceRestorationTarget: 15m
  rollbackRetention: 1h
  benchmark:
    testedAt: "${benchmark_tested_at}"
    estimatedWriteOutage: 14m
    upgradeImage: ${failed_upgrade_ref}
    sourceMajorVersion: 17
    targetMajorVersion: 18
    tdeEnabled: false
    postgresStorageClasses: [csi-hostpath]
    evidence: file://injected-failure@${failed_upgrade_digest}
EOF
for _ in $(seq 1 900); do
  failed_phase="$(kubectl -n database-platform get postgresupgrade orders-pg18-failure \
    -o jsonpath='{.status.phase}')"
  failed_reason="$(kubectl -n database-platform get postgresupgrade orders-pg18-failure \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}')"
  [[ "${failed_phase}" == "Failed" && "${failed_reason}" == "RolledBack" ]] && break
  sleep 2
done
test "${failed_phase}" = "Failed"
test "${failed_reason}" = "RolledBack"
test "$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.spec.postgres.majorVersion}')" = "17"
kubectl -n database-platform wait --for=condition=Ready multisitepostgres/orders --timeout=600s
primary="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.primary}')"
case "${primary}" in
  postgres-vic-*) primary_site=vic ;;
  postgres-nsw-*) primary_site=nsw ;;
  *) echo "unexpected primary after rollback ${primary}" >&2; exit 1 ;;
esac
primary_kubeconfig="${temp_dir}/mspsql-${primary_site}.kubeconfig"
primary_password="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
rollback_rows="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary}-0" \
  -c postgres-patroni -- env PGPASSWORD="${primary_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc \
  'SELECT count(*) FROM mspsql_e2e')"
if [[ "${rollback_rows}" != "2" ]]; then
  echo "major-upgrade rollback row count = ${rollback_rows@Q}, want '2'" >&2
  exit 1
fi
failure_events=0
for site in vic nsw; do
  site_config="${temp_dir}/mspsql-${site}.kubeconfig"
  count="$(kubectl --kubeconfig="${site_config}" -n mspsql-agent \
    get events.events.k8s.io -o json | jq \
    '[.items[] | select((.note // "") | contains("injected pg_upgrade conversion failure"))] | length')"
  failure_events=$((failure_events + count))
done
if [[ "${failure_events}" -lt 1 ]]; then
  echo "expected at least one injected pg_upgrade failure event, got ${failure_events}" >&2
  exit 1
fi

benchmark_file="${diagnostics_dir}/major-upgrade-benchmark.json"
jq -n --sort-keys \
  --arg testedAt "${benchmark_tested_at}" \
  --arg upgradeImage "${upgrade_ref}" \
  '{testedAt: $testedAt, estimatedWriteOutageSeconds: 840,
    upgradeImage: $upgradeImage, sourceMajorVersion: 17, targetMajorVersion: 18,
    tdeEnabled: false, postgresStorageClasses: ["csi-hostpath"]}' >"${benchmark_file}"
benchmark_digest="$(sha256sum "${benchmark_file}" | awk '{print $1}')"
kubectl -n database-platform apply -f - <<EOF
apiVersion: multisite-postgres.dev/v1alpha1
kind: PostgresUpgrade
metadata:
  name: orders-pg18
spec:
  instanceRef: orders
  targetImage: percona/percona-distribution-postgresql@sha256:e2cc221e458a11b8cc39cbf74e947289568d3e182de5036eadef66d9c15569fb
  targetMajorVersion: 18
  upgradeImage: ${upgrade_ref}
  serviceRestorationTarget: 15m
  rollbackRetention: 1h
  benchmark:
    testedAt: "${benchmark_tested_at}"
    estimatedWriteOutage: 14m
    upgradeImage: ${upgrade_ref}
    sourceMajorVersion: 17
    targetMajorVersion: 18
    tdeEnabled: false
    postgresStorageClasses: [csi-hostpath]
    evidence: file://major-upgrade-benchmark.json@sha256:${benchmark_digest}
EOF
kubectl -n database-platform wait --for=condition=Ready \
  postgresupgrade/orders-pg18 --timeout=2400s
test "$(kubectl -n database-platform get postgresupgrade orders-pg18 \
  -o jsonpath='{.status.phase}')" = "Completed"
test "$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.spec.postgres.majorVersion}')" = "18"
test "$(kubectl -n database-platform get multisitepostgres orders \
  -o jsonpath='{.spec.postgres.image}')" = \
  "percona/percona-distribution-postgresql@sha256:e2cc221e458a11b8cc39cbf74e947289568d3e182de5036eadef66d9c15569fb"
outage_started="$(kubectl -n database-platform get postgresupgrade orders-pg18 \
  -o jsonpath='{.status.writeOutageStartedAt}')"
service_restored="$(kubectl -n database-platform get postgresupgrade orders-pg18 \
  -o jsonpath='{.status.writeServiceRestoredAt}')"
test -n "${outage_started}"
test -n "${service_restored}"
outage_seconds="$(( $(date -d "${service_restored}" +%s) - $(date -d "${outage_started}" +%s) ))"
test "${outage_seconds}" -ge 0
test "${outage_seconds}" -le 900
jq -n --sort-keys --argjson writeOutageSeconds "${outage_seconds}" \
  --arg completedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{completedAt: $completedAt, writeOutageSeconds: $writeOutageSeconds,
    serviceRestorationObjectiveSeconds: 900, passed: ($writeOutageSeconds <= 900)}' \
  >"${diagnostics_dir}/major-upgrade-result.json"
for site in vic nsw; do
  site_config="${temp_dir}/mspsql-${site}.kubeconfig"
  test "$(kubectl --kubeconfig="${site_config}" -n orders-postgres get volumesnapshots \
    -o json | jq '[.items[] | select(.metadata.name | startswith("rollback-")) |
      select(.status.readyToUse == true)] | length')" -ge 1
  test "$(kubectl --kubeconfig="${site_config}" -n orders-postgres get statefulset \
    "postgres-${site}-0" -o jsonpath='{.spec.template.spec.containers[?(@.name=="postgres-patroni")].image}')" = \
    "percona/percona-distribution-postgresql@sha256:e2cc221e458a11b8cc39cbf74e947289568d3e182de5036eadef66d9c15569fb"
done
primary="$(kubectl -n database-platform get multisitepostgres orders -o jsonpath='{.status.primary}')"
case "${primary}" in
  postgres-vic-*) primary_site=vic ;;
  postgres-nsw-*) primary_site=nsw ;;
  *) echo "unexpected primary after major upgrade ${primary}" >&2; exit 1 ;;
esac
primary_kubeconfig="${temp_dir}/mspsql-${primary_site}.kubeconfig"
primary_password="$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres \
  get secret postgres-auth -o jsonpath='{.data.superuser-password}' | base64 -d)"
test "$(kubectl --kubeconfig="${primary_kubeconfig}" -n orders-postgres exec "${primary}-0" \
  -c postgres-patroni -- env PGPASSWORD="${primary_password}" PGSSLMODE=require \
  psql -h 127.0.0.1 -U postgres -d postgres -Atqc \
  'SELECT count(*) FROM mspsql_e2e')" = "2"

kubectl -n database-platform patch multisitepostgres orders --type=merge \
  -p '{"spec":{"deletionPolicy":"Delete"}}'
kubectl -n database-platform wait --for=condition=Ready multisitepostgres/orders --timeout=300s
docker pause mspsql-qld-control-plane >/dev/null
kubectl -n database-platform delete multisitepostgres orders --wait=false
kubectl -n database-platform wait --for=condition=DeletionBlocked \
  multisitepostgres/orders --timeout=300s
kubectl -n database-platform get multisitepostgres orders >/dev/null
docker unpause mspsql-qld-control-plane >/dev/null
for _ in $(seq 1 120); do
  kubectl --kubeconfig="${temp_dir}/mspsql-qld.kubeconfig" get --raw=/readyz >/dev/null 2>&1 && break
  sleep 2
done
kubectl --kubeconfig="${temp_dir}/mspsql-qld.kubeconfig" get --raw=/readyz >/dev/null
kubectl -n database-platform wait --for=delete multisitepostgres/orders --timeout=600s
for site in vic nsw qld; do
  if kubectl --kubeconfig="${temp_dir}/mspsql-${site}.kubeconfig" \
    get namespace orders-postgres >/dev/null 2>&1; then
    echo "Delete policy retained orders-postgres in ${site}" >&2
    exit 1
  fi
done

kubectl -n database-platform delete multisitepostgres orders-recovered --wait=false
kubectl -n database-platform wait --for=delete multisitepostgres/orders-recovered --timeout=300s
for site in vic nsw qld; do
  kubectl --kubeconfig="${temp_dir}/mspsql-${site}.kubeconfig" \
    get namespace "orders-recovered-${site}" >/dev/null
done
