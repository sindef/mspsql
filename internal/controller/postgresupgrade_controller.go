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

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// PostgresUpgradeReconciler reconciles a PostgresUpgrade object
type PostgresUpgradeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Now    func() time.Time
}

// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresupgrades,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresupgrades/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresupgrades/finalizers,verbs=update
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=multisitepostgres;postgresrestores;siteregistrations,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *PostgresUpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var upgrade multisitepostgresv1alpha1.PostgresUpgrade
	if err := r.Get(ctx, req.NamespacedName, &upgrade); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !upgrade.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	var instance multisitepostgresv1alpha1.MultiSitePostgres
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: upgrade.Namespace, Name: upgrade.Spec.InstanceRef,
	}, &instance); err != nil {
		return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "InstanceUnavailable", err.Error())
	}
	if !conditionTrue(instance.Status.Conditions, "Ready") ||
		!conditionTrue(instance.Status.Conditions, "BackupReady") ||
		instance.Status.LastBackupTime == nil {
		return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "PreflightFailed",
			"instance, synchronous replication and a recent verified backup must be healthy")
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	if now().Sub(instance.Status.LastBackupTime.Time) > 24*time.Hour {
		return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "BackupTooOld",
			"the most recent verified backup is older than 24 hours")
	}
	var upgrades multisitepostgresv1alpha1.PostgresUpgradeList
	if err := r.List(ctx, &upgrades, client.InNamespace(upgrade.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for _, other := range upgrades.Items {
		if other.Name != upgrade.Name && other.Spec.InstanceRef == upgrade.Spec.InstanceRef &&
			other.Status.Phase != "Completed" && other.Status.Phase != "Failed" {
			return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "OperationConflict",
				"another upgrade targets this instance")
		}
	}
	var restores multisitepostgresv1alpha1.PostgresRestoreList
	if err := r.List(ctx, &restores, client.InNamespace(upgrade.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for _, restore := range restores.Items {
		if restore.Spec.TargetInstanceRef == upgrade.Spec.InstanceRef &&
			restore.Status.Phase != "Completed" && restore.Status.Phase != "Failed" {
			return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "OperationConflict",
				"a restore targets this instance")
		}
	}
	directiveType := "MinorUpgrade"
	if upgrade.Spec.TargetMajorVersion != instance.Spec.Postgres.MajorVersion {
		if err := r.validateMajorUpgradeContract(ctx, &upgrade, &instance); err != nil {
			return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "PlatformContractRejected", err.Error())
		}
		directiveType = "MajorUpgrade"
		setCondition(&upgrade.Status.Conditions, upgrade.Generation, "ServiceRestorationTargetAtRisk",
			metav1.ConditionUnknown, "AwaitingBenchmark",
			"site agents must verify the measured upgrade path before write outage begins")
	}
	if err := reconcileDirective(ctx, r.Client, r.Scheme, &upgrade,
		"mspsql-upgrade-"+upgrade.Name, directiveType, upgrade.Spec.InstanceRef,
		upgrade.Spec, false); err != nil {
		return ctrl.Result{}, err
	}
	if upgrade.Status.StartedAt == nil {
		startedAt := metav1.NewTime(now())
		upgrade.Status.StartedAt = &startedAt
	}
	upgrade.Status.ObservedGeneration = upgrade.Generation
	upgrade.Status.Phase = "Preflight"
	setCondition(&upgrade.Status.Conditions, upgrade.Generation, "Ready", metav1.ConditionFalse,
		"UpgradeIssued", "Upgrade directive issued; site preflight must pass before disruption")
	if err := r.Status().Update(ctx, &upgrade); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *PostgresUpgradeReconciler) validateMajorUpgradeContract(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) error {
	if upgrade.Spec.TargetMajorVersion < instance.Spec.Postgres.MajorVersion {
		return fmt.Errorf("major-version downgrade is not supported")
	}
	if upgrade.Spec.UpgradeImage == "" {
		return fmt.Errorf("major upgrade requires a purpose-built upgradeImage")
	}
	if !strings.Contains(upgrade.Spec.UpgradeImage, "@sha256:") {
		return fmt.Errorf("upgradeImage must be pinned by sha256 digest")
	}
	if upgrade.Spec.RollbackRetention.Duration <= 0 {
		return fmt.Errorf("rollbackRetention must be positive")
	}
	for _, site := range instance.Spec.Sites {
		if site.Role != multisitepostgresv1alpha1.SiteRoleData || site.Storage.Postgres == nil {
			continue
		}
		var registration multisitepostgresv1alpha1.SiteRegistration
		if err := r.Get(ctx, client.ObjectKey{Name: site.SiteRegistrationRef}, &registration); err != nil {
			return fmt.Errorf("read site %s registration: %w", site.Name, err)
		}
		policy, found := rollbackPolicy(registration.Spec.StorageRollbackPolicies,
			site.Storage.Postgres.StorageClassName)
		if !found {
			return fmt.Errorf("site %s has no rollback policy for StorageClass %s",
				site.Name, site.Storage.Postgres.StorageClassName)
		}
		if policy.Strategy == "VolumeSnapshot" &&
			!snapshotClassDiscovered(registration.Status.DiscoveredVolumeSnapshotClasses,
				policy.VolumeSnapshotClassName) {
			return fmt.Errorf("site %s VolumeSnapshotClass %s is not discovered",
				site.Name, policy.VolumeSnapshotClassName)
		}
	}
	return nil
}

func rollbackPolicy(policies []multisitepostgresv1alpha1.StorageRollbackPolicy,
	storageClass string,
) (multisitepostgresv1alpha1.StorageRollbackPolicy, bool) {
	for _, policy := range policies {
		if policy.StorageClassName == storageClass {
			return policy, true
		}
	}
	return multisitepostgresv1alpha1.StorageRollbackPolicy{}, false
}

func snapshotClassDiscovered(classes []multisitepostgresv1alpha1.VolumeSnapshotClassInventory,
	name string,
) bool {
	if name == "" {
		return false
	}
	for _, snapshotClass := range classes {
		if snapshotClass.Name == name && snapshotClass.Driver != "" {
			return true
		}
	}
	return false
}

func (r *PostgresUpgradeReconciler) upgradeBlocked(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade, reason, message string,
) error {
	upgrade.Status.ObservedGeneration = upgrade.Generation
	upgrade.Status.Phase = "Preflight"
	setCondition(&upgrade.Status.Conditions, upgrade.Generation, "Ready", metav1.ConditionFalse, reason, message)
	return r.Status().Update(ctx, upgrade)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresUpgradeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.PostgresUpgrade{}).
		Owns(&corev1.ConfigMap{}).
		Named("postgresupgrade").
		Complete(r)
}
