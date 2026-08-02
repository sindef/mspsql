#!/usr/bin/env bash
set -euo pipefail

kubectl_bin="${KUBECTL:-kubectl}"
hub_context=""
namespace=""
instance=""
output_dir=""
site_contexts=()

usage() {
  cat <<'USAGE'
Usage: hack/collect-diagnostics.sh --hub-context CTX --namespace NS --instance NAME --output DIR [--site-context SITE=CTX ...]

Collect hub and site Kubernetes objects, events, Job logs, and Pod logs needed
to repair stuck mspsql lifecycle operations. The bundle never reads Secret data.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hub-context)
      hub_context="$2"
      shift 2
      ;;
    --namespace)
      namespace="$2"
      shift 2
      ;;
    --instance)
      instance="$2"
      shift 2
      ;;
    --output)
      output_dir="$2"
      shift 2
      ;;
    --site-context)
      site_contexts+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${hub_context}" || -z "${namespace}" || -z "${instance}" || -z "${output_dir}" ]]; then
  usage >&2
  exit 2
fi

mkdir -p "${output_dir}/hub" "${output_dir}/sites"

run() {
  local destination="$1"
  shift
  if ! "$@" >"${destination}" 2>&1; then
    printf 'command failed: %q' "$@" >"${destination}.failed"
  fi
}

hub() {
  "${kubectl_bin}" --context "${hub_context}" "$@"
}

collect_hub() {
  run "${output_dir}/hub/multisitepostgres.yaml" \
    hub -n "${namespace}" get multisitepostgres "${instance}" -o yaml
  run "${output_dir}/hub/events.txt" \
    hub -n "${namespace}" get events.events.k8s.io --sort-by=.metadata.creationTimestamp
  run "${output_dir}/hub/directives.yaml" \
    hub -n "${namespace}" get configmaps \
      -l "multisite-postgres.dev/instance=${instance}" -o yaml
  run "${output_dir}/hub/plan-configmaps.yaml" \
    hub -n "${namespace}" get configmaps \
      -l "multisite-postgres.dev/instance-uid" -o yaml
  run "${output_dir}/hub/restores.yaml" \
    hub -n "${namespace}" get postgresrestores -o yaml
  run "${output_dir}/hub/upgrades.yaml" \
    hub -n "${namespace}" get postgresupgrades -o yaml
  run "${output_dir}/hub/databases.yaml" \
    hub -n "${namespace}" get postgresdatabases -o yaml
  run "${output_dir}/hub/users.yaml" \
    hub -n "${namespace}" get postgresusers -o yaml
  run "${output_dir}/hub/sites.yaml" \
    hub get siteregistrations -o yaml
}

site_context_name() {
  local item="$1"
  printf '%s' "${item#*=}"
}

site_name() {
  local item="$1"
  printf '%s' "${item%%=*}"
}

collect_site() {
  local site="$1"
  local context="$2"
  local dir="${output_dir}/sites/${site}"
  mkdir -p "${dir}"
  run "${dir}/events.txt" \
    "${kubectl_bin}" --context "${context}" -n "${namespace}" get events.events.k8s.io \
      --sort-by=.metadata.creationTimestamp
  run "${dir}/workloads.yaml" \
    "${kubectl_bin}" --context "${context}" -n "${namespace}" get pods,jobs,statefulsets,services,endpointslices,configmaps,pvc \
      -l "multisite-postgres.dev/instance=${instance}" -o yaml
  run "${dir}/operation-jobs.yaml" \
    "${kubectl_bin}" --context "${context}" -n "${namespace}" get jobs \
      -l "multisite-postgres.dev/operation-uid" -o yaml
  run "${dir}/agent-events.txt" \
    "${kubectl_bin}" --context "${context}" -n mspsql-agent get events.events.k8s.io \
      --sort-by=.metadata.creationTimestamp
  run "${dir}/agent-logs.txt" \
    "${kubectl_bin}" --context "${context}" -n mspsql-agent logs \
      -l app.kubernetes.io/name=mspsql-agent --all-containers --tail=500
  run "${dir}/job-logs.txt" \
    "${kubectl_bin}" --context "${context}" -n "${namespace}" logs \
      -l "multisite-postgres.dev/operation-uid" --all-containers --tail=500
}

collect_hub
for item in "${site_contexts[@]}"; do
  collect_site "$(site_name "${item}")" "$(site_context_name "${item}")"
done
