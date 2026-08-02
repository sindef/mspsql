#!/usr/bin/env bash
set -euo pipefail

failed=0
while IFS=: read -r file line from image rest; do
  [[ "${from}" == "FROM" ]] || continue
  [[ "${file}" == ./test/kind/* ]] && continue
  [[ "${image}" == "scratch" ]] && continue
  if [[ "${image}" != *@sha256:* ]]; then
    printf '%s:%s: Dockerfile base image is not pinned by sha256 digest: %s %s %s\n' \
      "${file}" "${line}" "${from}" "${image}" "${rest}" >&2
    failed=1
  fi
done < <(grep -RIn --include='Dockerfile*' '^FROM ' .)

release=.github/workflows/release-images.yml
for required in 'id-token: write' 'provenance: mode=max' 'sbom: true' 'cmd/cosign'; do
  if ! grep -q "${required}" "${release}"; then
    printf '%s: missing release image policy marker: %s\n' "${release}" "${required}" >&2
    failed=1
  fi
done

exit "${failed}"
