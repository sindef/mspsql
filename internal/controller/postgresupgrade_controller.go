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
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

const (
	upgradeUIDAnnotation       = "multisite-postgres.dev/upgrade-uid"
	upgradeNameAnnotation      = "multisite-postgres.dev/upgrade-name"
	upgradePhaseAnnotation     = "multisite-postgres.dev/upgrade-phase"
	upgradeMemberAnnotation    = "multisite-postgres.dev/upgrade-member"
	upgradeMembersAnnotation   = "multisite-postgres.dev/upgraded-members"
	upgradeFromAnnotation      = "multisite-postgres.dev/upgrade-from-primary"
	upgradeCandidateAnnotation = "multisite-postgres.dev/upgrade-candidate"
	upgradeSwitchedAnnotation  = "multisite-postgres.dev/upgrade-switched"
	upgradeRevisionAnnotation  = "multisite-postgres.dev/upgrade-expected-revision"
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
	if upgrade.Status.Phase == "Completed" {
		if instance.Annotations[upgradeUIDAnnotation] == string(upgrade.UID) {
			base := instance.DeepCopy()
			clearUpgradeAnnotations(&instance)
			return ctrl.Result{}, r.Patch(ctx, &instance, client.MergeFrom(base))
		}
		return ctrl.Result{}, nil
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	if upgrade.Spec.TargetMajorVersion == instance.Spec.Postgres.MajorVersion &&
		instance.Annotations[upgradeUIDAnnotation] == string(upgrade.UID) {
		return r.reconcileMinorUpgrade(ctx, &upgrade, &instance, now())
	}
	if !conditionTrue(instance.Status.Conditions, "Ready") ||
		!conditionTrue(instance.Status.Conditions, "BackupReady") ||
		instance.Status.LastBackupTime == nil {
		return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "PreflightFailed",
			"instance, synchronous replication and a recent verified backup must be healthy")
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
	directiveType := "MajorUpgrade"
	if upgrade.Spec.TargetMajorVersion != instance.Spec.Postgres.MajorVersion {
		if err := r.validateMajorUpgradeContract(ctx, &upgrade, &instance); err != nil {
			return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "PlatformContractRejected", err.Error())
		}
		setCondition(&upgrade.Status.Conditions, upgrade.Generation, "ServiceRestorationTargetAtRisk",
			metav1.ConditionUnknown, "AwaitingBenchmark",
			"site agents must verify the measured upgrade path before write outage begins")
	} else {
		return r.reconcileMinorUpgrade(ctx, &upgrade, &instance, now())
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

func (r *PostgresUpgradeReconciler) reconcileMinorUpgrade(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
	now time.Time,
) (ctrl.Result, error) {
	if upgrade.Spec.TargetImage == instance.Spec.Postgres.Image &&
		instance.Annotations[upgradeUIDAnnotation] == "" {
		return ctrl.Result{}, r.completeUpgrade(ctx, upgrade, now)
	}
	if instance.Annotations[upgradeUIDAnnotation] == "" {
		candidate := selectUpgradeCandidate(instance, nil)
		if candidate == "" {
			return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "NoUpgradeCandidate",
				"a healthy synchronous standby is required for the first rollout")
		}
		if err := r.patchUpgradeAnnotations(ctx, instance, map[string]string{
			upgradeUIDAnnotation:      string(upgrade.UID),
			upgradeNameAnnotation:     upgrade.Name,
			upgradePhaseAnnotation:    string(plan.UpgradePhaseMember),
			upgradeMemberAnnotation:   candidate,
			upgradeMembersAnnotation:  "",
			upgradeRevisionAnnotation: fmt.Sprintf("%d", instance.Status.ActiveRevision+1),
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "RollingReplicas",
			"ReplicaSelected", "Upgrading synchronous standby "+candidate, now)
	}
	if instance.Annotations[upgradeUIDAnnotation] != string(upgrade.UID) {
		return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "OperationConflict",
			"instance is owned by another upgrade operation")
	}

	upgraded := splitMembers(instance.Annotations[upgradeMembersAnnotation])
	switch plan.UpgradePhase(instance.Annotations[upgradePhaseAnnotation]) {
	case plan.UpgradePhaseMember:
		member := instance.Annotations[upgradeMemberAnnotation]
		if member == "" {
			return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "InvalidState",
				"member rollout phase has no target member")
		}
		if !instanceReadyForOperation(instance) {
			return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "RollingReplicas",
				"MemberProgressing", "Waiting for verified rollout of "+member, now)
		}
		upgraded = appendUnique(upgraded, member)
		if instance.Annotations[upgradeSwitchedAnnotation] != "true" {
			if err := r.patchUpgradeAnnotations(ctx, instance, map[string]string{
				upgradePhaseAnnotation:     string(plan.UpgradePhaseSwitchover),
				upgradeMembersAnnotation:   strings.Join(upgraded, ","),
				upgradeFromAnnotation:      instance.Status.Primary,
				upgradeCandidateAnnotation: member,
				upgradeRevisionAnnotation:  fmt.Sprintf("%d", instance.Status.ActiveRevision+1),
			}); err != nil {
				return ctrl.Result{}, err
			}
			upgrade.Status.UpgradedMembers = upgraded
			return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "SwitchingOver",
				"ReplicaVerified", "Requesting Patroni switchover to "+member, now)
		}
		next := selectUpgradeCandidate(instance, upgraded)
		if next == "" {
			base := instance.DeepCopy()
			instance.Spec.Postgres.Image = upgrade.Spec.TargetImage
			instance.Annotations[upgradePhaseAnnotation] = string(plan.UpgradePhaseFinalize)
			instance.Annotations[upgradeRevisionAnnotation] =
				fmt.Sprintf("%d", instance.Status.ActiveRevision+1)
			if err := r.Patch(ctx, instance, client.MergeFrom(base)); err != nil {
				return ctrl.Result{}, err
			}
			upgrade.Status.UpgradedMembers = upgraded
			return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "Finalizing",
				"MembersVerified", "Reconciling the stable target image plan", now)
		}
		if err := r.patchUpgradeAnnotations(ctx, instance, map[string]string{
			upgradeMemberAnnotation:   next,
			upgradeMembersAnnotation:  strings.Join(upgraded, ","),
			upgradeRevisionAnnotation: fmt.Sprintf("%d", instance.Status.ActiveRevision+1),
		}); err != nil {
			return ctrl.Result{}, err
		}
		upgrade.Status.UpgradedMembers = upgraded
		return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "RollingMembers",
			"MemberVerified", "Upgrading member "+next, now)
	case plan.UpgradePhaseSwitchover:
		candidate := instance.Annotations[upgradeCandidateAnnotation]
		if candidate == "" || instance.Annotations[upgradeFromAnnotation] == "" {
			return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "InvalidState",
				"switchover phase is missing primary or candidate")
		}
		if instance.Status.Primary != candidate ||
			!conditionTrue(instance.Status.Conditions, "TopologyReady") {
			return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "SwitchingOver",
				"SwitchoverProgressing", "Waiting for Patroni to promote "+candidate, now)
		}
		next := selectUpgradeCandidate(instance, upgraded)
		if next == "" {
			return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "InvalidState",
				"no remaining member was found after switchover")
		}
		if err := r.patchUpgradeAnnotations(ctx, instance, map[string]string{
			upgradePhaseAnnotation:    string(plan.UpgradePhaseMember),
			upgradeMemberAnnotation:   next,
			upgradeSwitchedAnnotation: "true",
			upgradeRevisionAnnotation: fmt.Sprintf("%d", instance.Status.ActiveRevision+1),
		}); err != nil {
			return ctrl.Result{}, err
		}
		upgrade.Status.UpgradedMembers = upgraded
		return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "RollingMembers",
			"SwitchoverVerified", "Primary moved safely; upgrading member "+next, now)
	case plan.UpgradePhaseFinalize:
		if !instanceReadyForOperation(instance) {
			return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "Finalizing",
				"StablePlanProgressing", "Waiting for every site to apply the stable target image", now)
		}
		if err := r.completeUpgrade(ctx, upgrade, now); err != nil {
			return ctrl.Result{}, err
		}
		base := instance.DeepCopy()
		clearUpgradeAnnotations(instance)
		if err := r.Patch(ctx, instance, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "InvalidState",
			"instance has an unknown minor-upgrade phase")
	}
}

func selectUpgradeCandidate(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	upgraded []string,
) string {
	var candidates []string
	if len(upgraded) == 0 {
		for _, standby := range instance.Status.SynchronousStandbys {
			if standby != instance.Status.Primary {
				candidates = append(candidates, standby)
			}
		}
	} else {
		for _, member := range postgresMembers(instance.Spec.Sites) {
			if member != instance.Status.Primary && !slices.Contains(upgraded, member) {
				candidates = append(candidates, member)
			}
		}
	}
	slices.Sort(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

func postgresMembers(sites []multisitepostgresv1alpha1.PostgresSiteSpec) []string {
	var members []string
	for _, site := range sites {
		for ordinal := int32(0); ordinal < site.Components.PostgresReplicas; ordinal++ {
			members = append(members, fmt.Sprintf("postgres-%s-%d", site.Name, ordinal))
		}
	}
	slices.Sort(members)
	return members
}

func splitMembers(value string) []string {
	if value == "" {
		return nil
	}
	members := strings.Split(value, ",")
	slices.Sort(members)
	return slices.Compact(members)
}

func appendUnique(members []string, member string) []string {
	if !slices.Contains(members, member) {
		members = append(members, member)
	}
	slices.Sort(members)
	return members
}

func instanceReadyForOperation(instance *multisitepostgresv1alpha1.MultiSitePostgres) bool {
	expectedRevision, err := strconv.ParseInt(instance.Annotations[upgradeRevisionAnnotation], 10, 64)
	return err == nil && instance.Status.ActiveRevision >= expectedRevision &&
		allSitesApplied(instance.Status.Sites, instance.Status.ActiveRevision) &&
		instance.Status.ObservedGeneration == instance.Generation &&
		conditionTrue(instance.Status.Conditions, "Ready") &&
		conditionTrue(instance.Status.Conditions, "TopologyReady")
}

func (r *PostgresUpgradeReconciler) patchUpgradeAnnotations(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres, values map[string]string,
) error {
	base := instance.DeepCopy()
	if instance.Annotations == nil {
		instance.Annotations = map[string]string{}
	}
	maps.Copy(instance.Annotations, values)
	return r.Patch(ctx, instance, client.MergeFrom(base))
}

func clearUpgradeAnnotations(instance *multisitepostgresv1alpha1.MultiSitePostgres) {
	for _, key := range []string{
		upgradeUIDAnnotation, upgradeNameAnnotation, upgradePhaseAnnotation, upgradeMemberAnnotation,
		upgradeMembersAnnotation, upgradeFromAnnotation, upgradeCandidateAnnotation, upgradeSwitchedAnnotation,
		upgradeRevisionAnnotation,
	} {
		delete(instance.Annotations, key)
	}
}

func (r *PostgresUpgradeReconciler) setUpgradePhase(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade, phase, reason, message string, now time.Time,
) error {
	if upgrade.Status.StartedAt == nil {
		startedAt := metav1.NewTime(now)
		upgrade.Status.StartedAt = &startedAt
	}
	upgrade.Status.ObservedGeneration = upgrade.Generation
	upgrade.Status.Phase = phase
	setCondition(&upgrade.Status.Conditions, upgrade.Generation, "Ready", metav1.ConditionFalse, reason, message)
	return r.Status().Update(ctx, upgrade)
}

func (r *PostgresUpgradeReconciler) completeUpgrade(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade, _ time.Time,
) error {
	upgrade.Status.ObservedGeneration = upgrade.Generation
	upgrade.Status.Phase = "Completed"
	setCondition(&upgrade.Status.Conditions, upgrade.Generation, "Ready", metav1.ConditionTrue,
		"UpgradeCompleted", "All PostgreSQL members run the requested image")
	return r.Status().Update(ctx, upgrade)
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
		Watches(&multisitepostgresv1alpha1.MultiSitePostgres{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, object client.Object) []ctrl.Request {
				name := object.GetAnnotations()[upgradeNameAnnotation]
				if name == "" {
					return nil
				}
				return []ctrl.Request{{NamespacedName: types.NamespacedName{
					Namespace: object.GetNamespace(), Name: name,
				}}}
			})).
		Named("postgresupgrade").
		Complete(r)
}
