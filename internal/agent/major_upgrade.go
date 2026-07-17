/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agent

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

var volumeSnapshotListGVK = schema.GroupVersionKind{
	Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshotList",
}

func (r *Reconciler) prepareMajorUpgrade(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	upgrade := desired.MajorUpgrade
	switch upgrade.Phase {
	case plan.MajorUpgradePhaseSnapshot:
		result.Phase = "CapturingRollback"
		return r.reconcileRollbackStorage(ctx, desired, result)
	case plan.MajorUpgradePhaseUpgradePrimary:
		if !memberBelongsToSite(upgrade.Primary, desired.Site.Name) {
			return true, nil
		}
		result.Phase = "UpgradingPrimary"
		for _, job := range []*batchv1.Job{
			r.Renderer.MajorUpgradeJob(desired),
			r.Renderer.MajorDCSResetJob(desired),
		} {
			if err := r.apply(ctx, job); err != nil {
				return false, err
			}
			ready, message, err := r.workloadsReady(ctx, []client.Object{job})
			if err != nil {
				return false, err
			}
			if !ready {
				reason := "PrimaryConversionPending"
				if strings.Contains(message, " failed:") {
					reason = "PrimaryConversionFailed"
				}
				setLocalCondition(&result.Conditions, "MajorUpgradeBlocked", metav1.ConditionTrue,
					reason, message)
				return false, nil
			}
		}
		if err := r.markOldDataRetention(ctx, desired); err != nil {
			return false, err
		}
		setLocalCondition(&result.Conditions, "MajorUpgradeBlocked", metav1.ConditionFalse,
			"PrimaryConverted", "Primary data and Patroni DCS initialization state are upgraded")
	case plan.MajorUpgradePhaseStanzaUpgrade:
		if !memberBelongsToSite(upgrade.Primary, desired.Site.Name) {
			return true, nil
		}
		result.Phase = "UpgradingBackupStanza"
		job := r.Renderer.MajorStanzaUpgradeJob(desired)
		if err := r.apply(ctx, job); err != nil {
			return false, err
		}
		ready, message, err := r.workloadsReady(ctx, []client.Object{job})
		if err != nil {
			return false, err
		}
		if !ready {
			reason := "BackupStanzaUpgradePending"
			if strings.Contains(message, " failed:") {
				reason = "BackupStanzaUpgradeFailed"
			}
			setLocalCondition(&result.Conditions, "MajorUpgradeBlocked", metav1.ConditionTrue,
				reason, message)
			return false, nil
		}
	case plan.MajorUpgradePhaseReplicas:
		result.Phase = "ResettingReplicas"
		for _, job := range r.Renderer.MajorReplicaResetJobs(desired) {
			if err := r.apply(ctx, job); err != nil {
				return false, err
			}
			ready, message, err := r.workloadsReady(ctx, []client.Object{job})
			if err != nil {
				return false, err
			}
			if !ready {
				reason := "ReplicaResetPending"
				if strings.Contains(message, " failed:") {
					reason = "ReplicaResetFailed"
				}
				setLocalCondition(&result.Conditions, "MajorUpgradeBlocked", metav1.ConditionTrue,
					reason, message)
				return false, nil
			}
		}
	case plan.MajorUpgradePhaseRollback:
		result.Phase = "RollingBack"
		ready, err := r.ensureRollbackWorkloadsStopped(ctx, desired, result)
		if err != nil || !ready {
			return ready, err
		}
		ready, err = r.reconcileRollbackRestore(ctx, desired, result)
		if err != nil || !ready {
			return ready, err
		}
		job := r.Renderer.MajorRollbackDCSResetJob(desired)
		if err := r.apply(ctx, job); err != nil {
			return false, err
		}
		ready, message, err := r.workloadsReady(ctx, []client.Object{job})
		if err != nil {
			return false, err
		}
		if !ready {
			setLocalCondition(&result.Conditions, "MajorUpgradeBlocked", metav1.ConditionTrue,
				"RollbackDCSResetPending", message)
			return false, nil
		}
	}
	return true, nil
}

func (r *Reconciler) markOldDataRetention(ctx context.Context, desired plan.SitePlan) error {
	claimName := "data-" + desired.MajorUpgrade.Primary + "-0"
	var claim corev1.PersistentVolumeClaim
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: desired.Site.Namespace, Name: claimName,
	}, &claim); err != nil {
		return err
	}
	if claim.Annotations["multisite-postgres.dev/old-data-operation"] ==
		desired.MajorUpgrade.OperationUID {
		return nil
	}
	base := claim.DeepCopy()
	if claim.Annotations == nil {
		claim.Annotations = map[string]string{}
	}
	claim.Annotations["multisite-postgres.dev/old-data-operation"] =
		desired.MajorUpgrade.OperationUID
	claim.Annotations["multisite-postgres.dev/old-data-path"] =
		fmt.Sprintf("data/.mspsql-old-%d", desired.MajorUpgrade.SourceMajor)
	claim.Annotations["multisite-postgres.dev/old-data-expires-at"] =
		time.Now().Add(desired.MajorUpgrade.RollbackRetention).UTC().Format(time.RFC3339)
	return r.Client.Patch(ctx, &claim, client.MergeFrom(base))
}

func (r *Reconciler) ensureRollbackWorkloadsStopped(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	objects, err := r.Renderer.Workloads(desired)
	if err != nil {
		return false, err
	}
	var stopped []client.Object
	for _, object := range objects {
		if isRollbackStoppedWorkload(object, desired.Site.Name) {
			stopped = append(stopped, object)
		}
	}
	for _, object := range stopped {
		if err := r.apply(ctx, object); err != nil {
			return false, err
		}
	}
	ready, message, err := r.workloadsReady(ctx, stopped)
	if err != nil {
		return false, err
	}
	if !ready {
		setLocalCondition(&result.Conditions, "RollbackRestored", metav1.ConditionFalse,
			"WorkloadsStopping", message)
	}
	return ready, nil
}

func isRollbackStoppedWorkload(object client.Object, siteName string) bool {
	switch object := object.(type) {
	case *appsv1.StatefulSet:
		return strings.HasPrefix(object.Name, "postgres-")
	case *appsv1.Deployment:
		return object.Name == "pgpool-"+siteName
	default:
		return false
	}
}

func (r *Reconciler) reconcileRollbackStorage(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	policy, found := desired.MajorUpgrade.RollbackPolicies[desired.Site.Name]
	if !found || desired.Site.Role != api.SiteRoleData {
		return true, nil
	}
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		member := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		sourcePVC := "data-" + member + "-0"
		name := "rollback-" + operationHash(desired.MajorUpgrade.OperationUID+"/"+member)
		switch policy.Strategy {
		case "VolumeSnapshot":
			snapshot := volumeSnapshot(desired, name, sourcePVC, policy.VolumeSnapshotClassName)
			if err := r.apply(ctx, snapshot); err != nil {
				return false, err
			}
			observed := &unstructured.Unstructured{}
			observed.SetGroupVersionKind(snapshot.GroupVersionKind())
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(snapshot), observed); err != nil {
				return false, err
			}
			ready, _, _ := unstructured.NestedBool(observed.Object, "status", "readyToUse")
			if !ready {
				setLocalCondition(&result.Conditions, "RollbackReady", metav1.ConditionFalse,
					"SnapshotPending", "Waiting for VolumeSnapshot "+name)
				return false, nil
			}
		case "PVCClone":
			claim := rollbackClonePVC(desired, name, sourcePVC)
			if err := r.apply(ctx, claim); err != nil {
				return false, err
			}
			var observed corev1.PersistentVolumeClaim
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(claim), &observed); err != nil {
				return false, err
			}
			if observed.Status.Phase != corev1.ClaimBound {
				setLocalCondition(&result.Conditions, "RollbackReady", metav1.ConditionFalse,
					"ClonePending", "Waiting for rollback PVC "+name)
				return false, nil
			}
		default:
			return false, fmt.Errorf("unsupported rollback strategy %q", policy.Strategy)
		}
	}
	setLocalCondition(&result.Conditions, "RollbackReady", metav1.ConditionTrue,
		"RollbackCaptured", "Every local PostgreSQL PVC has a retained rollback artifact")
	return true, nil
}

func (r *Reconciler) reconcileRollbackRestore(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	policy, found := desired.MajorUpgrade.RollbackPolicies[desired.Site.Name]
	if !found || desired.Site.Role != api.SiteRoleData {
		return true, nil
	}
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		member := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		claimName := "data-" + member + "-0"
		rollbackName := "rollback-" + operationHash(desired.MajorUpgrade.OperationUID+"/"+member)
		var observed corev1.PersistentVolumeClaim
		err := r.Client.Get(ctx, client.ObjectKey{
			Namespace: desired.Site.Namespace, Name: claimName,
		}, &observed)
		if err == nil && observed.Annotations["multisite-postgres.dev/rollback-source"] != rollbackName {
			if err := r.Client.Delete(ctx, &observed); err != nil {
				return false, err
			}
			setLocalCondition(&result.Conditions, "RollbackRestored", metav1.ConditionFalse,
				"SourcePVCDeleting", "Deleting failed data PVC "+claimName)
			return false, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if apierrors.IsNotFound(err) {
			claim := rollbackRestorePVC(desired, claimName, rollbackName, policy)
			if err := r.Client.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
				return false, err
			}
			setLocalCondition(&result.Conditions, "RollbackRestored", metav1.ConditionFalse,
				"RestorePVCCreating", "Restoring data PVC "+claimName)
			return false, nil
		}
		if observed.Status.Phase != corev1.ClaimBound {
			setLocalCondition(&result.Conditions, "RollbackRestored", metav1.ConditionFalse,
				"RestorePVCBinding", "Waiting for restored PVC "+claimName)
			return false, nil
		}
	}
	setLocalCondition(&result.Conditions, "RollbackRestored", metav1.ConditionTrue,
		"AllPVCsRestored", "Every local PostgreSQL PVC was restored from rollback storage")
	return true, nil
}

func rollbackRestorePVC(desired plan.SitePlan, name, rollbackName string,
	policy api.StorageRollbackPolicy,
) *corev1.PersistentVolumeClaim {
	storage := desired.Site.Storage.Postgres
	dataSource := &corev1.TypedLocalObjectReference{Name: rollbackName}
	if policy.Strategy == "VolumeSnapshot" {
		dataSource.APIGroup = ptr("snapshot.storage.k8s.io")
		dataSource.Kind = "VolumeSnapshot"
	} else {
		dataSource.Kind = "PersistentVolumeClaim"
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: name,
			Labels: resourceLabels(desired),
			Annotations: map[string]string{
				"multisite-postgres.dev/rollback-source": rollbackName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storage.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: storage.Size,
			}},
			DataSource: dataSource,
		},
	}
}

func volumeSnapshot(desired plan.SitePlan, name, sourcePVC, className string) *unstructured.Unstructured {
	expiresAt := desired.GeneratedAt.Add(desired.MajorUpgrade.RollbackRetention).UTC().Format(time.RFC3339)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata": map[string]any{
			"namespace": desired.Site.Namespace, "name": name,
			"labels":      stringMapAny(resourceLabels(desired)),
			"annotations": map[string]any{"multisite-postgres.dev/expires-at": expiresAt},
		},
		"spec": map[string]any{
			"volumeSnapshotClassName": className,
			"source":                  map[string]any{"persistentVolumeClaimName": sourcePVC},
		},
	}}
}

func rollbackClonePVC(desired plan.SitePlan, name, sourcePVC string) *corev1.PersistentVolumeClaim {
	storage := desired.Site.Storage.Postgres
	expiresAt := desired.GeneratedAt.Add(desired.MajorUpgrade.RollbackRetention).UTC().Format(time.RFC3339)
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: name, Labels: resourceLabels(desired),
			Annotations: map[string]string{"multisite-postgres.dev/expires-at": expiresAt},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storage.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: storage.Size,
			}},
			DataSource: &corev1.TypedLocalObjectReference{
				Kind: "PersistentVolumeClaim", Name: sourcePVC,
			},
		},
	}
}

func (r Renderer) MajorPreflightJob(desired plan.SitePlan) *batchv1.Job {
	tool := "pg_upgrade"
	tdeChecks := ""
	if desired.TDE.Enabled {
		tool = "pg_tde_upgrade"
		tdeChecks = `
find /opt/mspsql/old/lib -name pg_tde.so -print -quit | grep -q .
find /opt/mspsql/new/lib -name pg_tde.so -print -quit | grep -q .
test -r /vault/token
`
	}
	script := fmt.Sprintf(`set -eu
test -x /opt/mspsql/old/bin/postgres
test -x /opt/mspsql/old/bin/pg_controldata
test -x /opt/mspsql/new/bin/postgres
test -x /opt/mspsql/new/bin/initdb
test -x /opt/mspsql/new/bin/pg_ctl
command -v %s
%s`, tool, tdeChecks)
	volumes := []corev1.Volume(nil)
	mounts := []corev1.VolumeMount(nil)
	if desired.TDE.Enabled {
		volumes = append(volumes, corev1.Volume{
			Name: "pg-tde-vault", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "pg-tde-vault"},
			},
		})
		mounts = append(mounts,
			corev1.VolumeMount{Name: "pg-tde-vault", MountPath: "/vault", ReadOnly: true})
	}
	job := majorJob(desired, "major-preflight-"+operationHash(
		desired.MajorUpgrade.OperationUID+"/"+desired.Site.Name),
		desired.MajorUpgrade.UpgradeImage, script, volumes, mounts)
	job.Spec.Template.Spec.InitContainers = []corev1.Container{{
		Name: "target-image-contract", Image: desired.MajorUpgrade.TargetImage,
		Command: []string{"/bin/sh", "-ec",
			"command -v postgres; command -v psql; command -v pgbackrest; command -v envsubst"},
		SecurityContext: restrictedContainer(),
	}}
	return job
}

func (r Renderer) MajorUpgradeJob(desired plan.SitePlan) *batchv1.Job {
	upgrade := desired.MajorUpgrade
	tool := "/opt/mspsql/new/bin/pg_upgrade"
	extraOptions := ""
	if desired.TDE.Enabled {
		tool = "pg_tde_upgrade"
		extraOptions = ` --old-options='-c shared_preload_libraries=pg_tde'` +
			` --new-options='-c shared_preload_libraries=pg_tde'`
	}
	script := fmt.Sprintf(`set -eu
cd /pgdata/data
mkdir -p .mspsql-old-%d .mspsql-new-%d
find . -mindepth 1 -maxdepth 1 ! -name '.mspsql-old-%d' ! -name '.mspsql-new-%d' \
  -exec mv -- {} .mspsql-old-%d/ \;
export PATH=/opt/mspsql/new/bin:/opt/mspsql/old/bin:$PATH
export LD_LIBRARY_PATH=/opt/mspsql/new/lib:/opt/mspsql/old/lib
/opt/mspsql/new/bin/initdb -D .mspsql-new-%d --encoding=UTF8 --data-checksums
%s --check --link --old-bindir=/opt/mspsql/old/bin --new-bindir=/opt/mspsql/new/bin%s \
  --old-datadir=.mspsql-old-%d --new-datadir=.mspsql-new-%d
%s --link --old-bindir=/opt/mspsql/old/bin --new-bindir=/opt/mspsql/new/bin%s \
  --old-datadir=.mspsql-old-%d --new-datadir=.mspsql-new-%d
find .mspsql-new-%d -mindepth 1 -maxdepth 1 -exec mv -- {} . \;
rmdir .mspsql-new-%d
`, upgrade.SourceMajor, upgrade.TargetMajor, upgrade.SourceMajor, upgrade.TargetMajor,
		upgrade.SourceMajor, upgrade.TargetMajor, tool, extraOptions, upgrade.SourceMajor,
		upgrade.TargetMajor, tool, extraOptions, upgrade.SourceMajor, upgrade.TargetMajor,
		upgrade.TargetMajor, upgrade.TargetMajor)
	claim := "data-" + upgrade.Primary + "-0"
	volumes := []corev1.Volume{{
		Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
		},
	}}
	mounts := []corev1.VolumeMount{{Name: "data", MountPath: "/pgdata"}}
	if desired.TDE.Enabled {
		volumes = append(volumes, corev1.Volume{
			Name: "pg-tde-vault", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "pg-tde-vault"},
			},
		})
		mounts = append(mounts,
			corev1.VolumeMount{Name: "pg-tde-vault", MountPath: "/vault", ReadOnly: true})
	}
	return majorJob(desired, "major-upgrade-"+operationHash(upgrade.OperationUID),
		upgrade.UpgradeImage, script, volumes, mounts)
}

func (r Renderer) MajorStanzaUpgradeJob(desired plan.SitePlan) *batchv1.Job {
	upgrade := desired.MajorUpgrade
	stanza := "mspsql-" + desired.InstanceUID
	script := fmt.Sprintf(`set -eu
umask 077
export S3_ACCESS_KEY="$(cat /repository/s3-access-key)"
export S3_SECRET_KEY="$(cat /repository/s3-secret-key)"
export REPO_CIPHER_PASSPHRASE="$(cat /repository/repo-cipher-passphrase)"
envsubst < /template/pgbackrest.conf > /tmp/pgbackrest.conf
pgbackrest --config=/tmp/pgbackrest.conf --stanza=%q --no-online stanza-upgrade
`, stanza)
	claim := "data-" + upgrade.Primary + "-0"
	return majorJob(desired, "major-stanza-upgrade-"+operationHash(upgrade.OperationUID),
		upgrade.TargetImage, script, []corev1.Volume{
			{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			}},
			{Name: "template", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{
					Name: "pgbackrest-" + desired.Site.Name,
				}},
			}},
			{Name: "repository", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "pgbackrest-repository"},
			}},
			{Name: "pgbackrest-tls", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: upgrade.Primary + "-pgbackrest-tls"},
			}},
		}, []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql/data"},
			{Name: "template", MountPath: "/template", ReadOnly: true},
			{Name: "repository", MountPath: "/repository", ReadOnly: true},
			{Name: "pgbackrest-tls", MountPath: "/pgbackrest-tls", ReadOnly: true},
		})
}

func (r Renderer) MajorAcceptanceJob(desired plan.SitePlan) *batchv1.Job {
	upgrade := desired.MajorUpgrade
	return r.majorAcceptanceJob(desired, "major-acceptance-", upgrade.TargetImage,
		upgrade.TargetMajor)
}

func (r Renderer) MajorRollbackAcceptanceJob(desired plan.SitePlan) *batchv1.Job {
	upgrade := desired.MajorUpgrade
	return r.majorAcceptanceJob(desired, "major-rollback-acceptance-", desired.Postgres.Image,
		upgrade.SourceMajor)
}

func (r Renderer) majorAcceptanceJob(desired plan.SitePlan, prefix, image string,
	expectedMajor int32,
) *batchv1.Job {
	upgrade := desired.MajorUpgrade
	host := upgrade.Primary + "." + desired.Site.Namespace + ".svc"
	schemaName := "mspsql_upgrade_" + operationHash(upgrade.OperationUID)
	script := fmt.Sprintf(`set -eu
export PGUSER="$(cat /credentials/superuser-username)"
export PGPASSWORD="$(cat /credentials/superuser-password)"
export PGSSLMODE=verify-full
export PGSSLROOTCERT=/postgres-tls/ca.crt
ready=false
for attempt in $(seq 1 90); do
  if psql -X -h %q -d postgres -Atqc 'SELECT 1' >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 10
done
test "$ready" = true
test "$(psql -X -h %q -d postgres -Atqc 'SHOW server_version_num')" -ge %d0000
psql -X -h %q -d postgres -v ON_ERROR_STOP=1 <<'SQL'
DROP SCHEMA IF EXISTS %s CASCADE;
CREATE SCHEMA %s;
CREATE TABLE %s.write_test (value integer PRIMARY KEY);
INSERT INTO %s.write_test VALUES (1);
SELECT 1 / (count(*) = 1)::int FROM %s.write_test;
DROP SCHEMA %s CASCADE;
SQL
`, host, host, expectedMajor, host, schemaName, schemaName, schemaName, schemaName, schemaName, schemaName)
	return majorJob(desired, prefix+operationHash(upgrade.OperationUID),
		image, script, []corev1.Volume{
			{Name: "credentials", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "postgres-auth"},
			}},
			{Name: "postgres-tls", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: upgrade.Primary + "-tls"},
			}},
		}, []corev1.VolumeMount{
			{Name: "credentials", MountPath: "/credentials", ReadOnly: true},
			{Name: "postgres-tls", MountPath: "/postgres-tls", ReadOnly: true},
		})
}

func (r Renderer) MajorDCSResetJob(desired plan.SitePlan) *batchv1.Job {
	return r.majorDCSResetJob(desired, "major-dcs-reset-")
}

func (r Renderer) MajorRollbackDCSResetJob(desired plan.SitePlan) *batchv1.Job {
	return r.majorDCSResetJob(desired, "major-rollback-dcs-reset-")
}

func (r Renderer) majorDCSResetJob(desired plan.SitePlan, prefix string) *batchv1.Job {
	var endpoints []string
	for member, address := range desired.MemberAddresses {
		if strings.HasPrefix(member, "etcd-") {
			endpoints = append(endpoints, "https://"+address+":2379")
		}
	}
	slices.Sort(endpoints)
	script := fmt.Sprintf(`set -eu
export ETCDCTL_API=3
etcdctl --endpoints=%q --cacert=/tls/ca.crt --cert=/tls/tls.crt --key=/tls/tls.key \
  del --prefix %q
`, strings.Join(endpoints, ","), "/service/"+desired.InstanceUID+"/")
	job := majorJob(desired, prefix+operationHash(desired.MajorUpgrade.OperationUID),
		r.Images.Etcd, script, []corev1.Volume{{
			Name: "tls", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "etcd-maintenance-client-tls"},
			},
		}}, []corev1.VolumeMount{{Name: "tls", MountPath: "/tls", ReadOnly: true}})
	job.Spec.BackoffLimit = ptr(int32(3))
	return job
}

func (r Renderer) MajorReplicaResetJobs(desired plan.SitePlan) []*batchv1.Job {
	var jobs []*batchv1.Job
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		member := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		if member == desired.MajorUpgrade.Primary {
			continue
		}
		claim := "data-" + member + "-0"
		job := majorJob(desired, "major-reset-"+operationHash(
			desired.MajorUpgrade.OperationUID+"/"+member), desired.MajorUpgrade.UpgradeImage,
			`set -eu
find /pgdata -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
`, []corev1.Volume{{
				Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
				},
			}}, []corev1.VolumeMount{{Name: "data", MountPath: "/pgdata"}})
		job.Spec.BackoffLimit = ptr(int32(3))
		jobs = append(jobs, job)
	}
	return jobs
}

func majorJob(desired plan.SitePlan, name, image, script string,
	volumes []corev1.Volume, mounts []corev1.VolumeMount,
) *batchv1.Job {
	backoff := int32(0)
	deadline := int64(4 * 60 * 60)
	ttl := int32(7 * 24 * 60 * 60)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: name, Labels: resourceLabels(desired),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline, TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: stableWorkloadLabels(resourceLabels(desired))},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           workloadServiceAccount,
					AutomountServiceAccountToken: ptr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), FSGroup: ptr(int64(26)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "upgrade", Image: image, Command: []string{"/bin/sh", "-ec", script},
						SecurityContext: restrictedContainer(), VolumeMounts: mounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

func (r *Reconciler) cleanupExpiredRollback(ctx context.Context, desired plan.SitePlan,
	now time.Time,
) error {
	snapshots := &unstructured.UnstructuredList{}
	snapshots.SetGroupVersionKind(volumeSnapshotListGVK)
	lists := []client.ObjectList{&corev1.PersistentVolumeClaimList{}, snapshots}
	for _, list := range lists {
		if err := r.Client.List(ctx, list, client.InNamespace(desired.Site.Namespace),
			client.MatchingLabels{
				"multisite-postgres.dev/instance-uid": desired.InstanceUID,
			}); err != nil {
			if meta.IsNoMatchError(err) {
				continue
			}
			return err
		}
		objects, err := meta.ExtractList(list)
		if err != nil {
			return err
		}
		for _, object := range objects {
			managed, ok := object.(client.Object)
			if !ok {
				continue
			}
			if claim, ok := managed.(*corev1.PersistentVolumeClaim); ok {
				cleaned, err := r.cleanupExpiredOldData(ctx, desired, claim, now)
				if err != nil {
					return err
				}
				if cleaned {
					continue
				}
			}
			if !strings.HasPrefix(managed.GetName(), "rollback-") {
				continue
			}
			expiresAt, err := time.Parse(time.RFC3339,
				managed.GetAnnotations()["multisite-postgres.dev/expires-at"])
			if err != nil || now.Before(expiresAt) {
				continue
			}
			if err := r.Client.Delete(ctx, managed); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Reconciler) cleanupExpiredOldData(ctx context.Context, desired plan.SitePlan,
	claim *corev1.PersistentVolumeClaim, now time.Time,
) (bool, error) {
	path := claim.Annotations["multisite-postgres.dev/old-data-path"]
	expires := claim.Annotations["multisite-postgres.dev/old-data-expires-at"]
	operation := claim.Annotations["multisite-postgres.dev/old-data-operation"]
	if path == "" || expires == "" || operation == "" {
		return false, nil
	}
	if !strings.HasPrefix(path, ".mspsql-old-") || strings.Contains(path, "/") {
		return true, fmt.Errorf("refusing invalid retained old-data path %q", path)
	}
	expiresAt, err := time.Parse(time.RFC3339, expires)
	if err != nil || now.Before(expiresAt) {
		return true, nil
	}
	job := majorJob(desired, "major-cleanup-"+operationHash(operation),
		desired.Postgres.Image, fmt.Sprintf("set -eu\nrm -rf -- /pgdata/%s\n", path),
		[]corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim.Name,
				},
			},
		}}, []corev1.VolumeMount{{Name: "data", MountPath: "/pgdata"}})
	if err := r.apply(ctx, job); err != nil {
		return true, err
	}
	ready, _, err := r.workloadsReady(ctx, []client.Object{job})
	if err != nil || !ready {
		return true, err
	}
	base := claim.DeepCopy()
	delete(claim.Annotations, "multisite-postgres.dev/old-data-operation")
	delete(claim.Annotations, "multisite-postgres.dev/old-data-path")
	delete(claim.Annotations, "multisite-postgres.dev/old-data-expires-at")
	return true, r.Client.Patch(ctx, claim, client.MergeFrom(base))
}
