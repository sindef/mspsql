#!/usr/bin/env bash

set -euo pipefail

kubeconfig="$1"
hostpath_dir="$2"
snapshotter_dir="$3"
kubectl=(kubectl "--kubeconfig=${kubeconfig}")

"${kubectl[@]}" kustomize "${snapshotter_dir}/client/config/crd" | "${kubectl[@]}" apply -f -
"${kubectl[@]}" kustomize "${snapshotter_dir}/deploy/kubernetes/snapshot-controller" |
  "${kubectl[@]}" apply -f -
"${kubectl[@]}" rollout status deployment/snapshot-controller --timeout=300s

KUBECONFIG="${kubeconfig}" "${hostpath_dir}/deploy/kubernetes-1.34/deploy.sh"
"${kubectl[@]}" apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-hostpath
provisioner: hostpath.csi.k8s.io
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mspsql-snapshot-probe
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: csi-hostpath
  resources:
    requests:
      storage: 64Mi
EOF
"${kubectl[@]}" wait --for=jsonpath='{.status.phase}'=Bound \
  pvc/mspsql-snapshot-probe --timeout=180s
"${kubectl[@]}" apply -f - <<'EOF'
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: mspsql-snapshot-probe
spec:
  volumeSnapshotClassName: csi-hostpath-snapclass
  source:
    persistentVolumeClaimName: mspsql-snapshot-probe
EOF
"${kubectl[@]}" wait --for=jsonpath='{.status.readyToUse}'=true \
  volumesnapshot/mspsql-snapshot-probe --timeout=180s
"${kubectl[@]}" delete volumesnapshot/mspsql-snapshot-probe --wait=true
"${kubectl[@]}" delete pvc/mspsql-snapshot-probe --wait=true
