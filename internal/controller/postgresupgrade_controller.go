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
	"encoding/json"
	"fmt"
	"hash/fnv"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

const (
	upgradeUIDAnnotation         = "multisite-postgres.dev/upgrade-uid"
	upgradeNameAnnotation        = "multisite-postgres.dev/upgrade-name"
	upgradePhaseAnnotation       = "multisite-postgres.dev/upgrade-phase"
	upgradeMemberAnnotation      = "multisite-postgres.dev/upgrade-member"
	upgradeMembersAnnotation     = "multisite-postgres.dev/upgraded-members"
	upgradeFromAnnotation        = "multisite-postgres.dev/upgrade-from-primary"
	upgradeCandidateAnnotation   = "multisite-postgres.dev/upgrade-candidate"
	upgradeSwitchedAnnotation    = "multisite-postgres.dev/upgrade-switched"
	upgradeRevisionAnnotation    = "multisite-postgres.dev/upgrade-expected-revision"
	upgradeSourceMajorAnnotation = "multisite-postgres.dev/upgrade-source-major"

	maxPostUpgradeBackupAttempts = int32(5)
	postUpgradeBackupRetryBase   = time.Minute
	postUpgradeBackupRetryMax    = 30 * time.Minute
	postUpgradeBackupDeadline    = 24 * time.Hour
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
		return r.reconcileUpgradeDeletion(ctx, &upgrade)
	}
	if upgrade.Status.Phase == "Completed" || upgrade.Status.Phase == "Failed" {
		var instance multisitepostgresv1alpha1.MultiSitePostgres
		err := r.Get(ctx, client.ObjectKey{
			Namespace: upgrade.Namespace, Name: upgrade.Spec.InstanceRef,
		}, &instance)
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		if err == nil {
			if _, err := r.reconcileTerminalUpgrade(ctx, &upgrade, &instance); err != nil {
				return ctrl.Result{}, err
			}
		}
		if controllerutil.ContainsFinalizer(&upgrade, operationFinalizer) {
			controllerutil.RemoveFinalizer(&upgrade, operationFinalizer)
			return ctrl.Result{}, r.Update(ctx, &upgrade)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&upgrade, operationFinalizer) {
		controllerutil.AddFinalizer(&upgrade, operationFinalizer)
		return ctrl.Result{}, r.Update(ctx, &upgrade)
	}
	var instance multisitepostgresv1alpha1.MultiSitePostgres
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: upgrade.Namespace, Name: upgrade.Spec.InstanceRef,
	}, &instance); err != nil {
		if (upgrade.Status.Phase == "Completed" || upgrade.Status.Phase == "Failed") &&
			controllerutil.ContainsFinalizer(&upgrade, operationFinalizer) {
			controllerutil.RemoveFinalizer(&upgrade, operationFinalizer)
			return ctrl.Result{}, r.Update(ctx, &upgrade)
		}
		return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "InstanceUnavailable", err.Error())
	}
	if handled, err := r.reconcileTerminalUpgrade(ctx, &upgrade, &instance); handled {
		if err != nil {
			return ctrl.Result{}, err
		}
		if controllerutil.ContainsFinalizer(&upgrade, operationFinalizer) {
			controllerutil.RemoveFinalizer(&upgrade, operationFinalizer)
			return ctrl.Result{}, r.Update(ctx, &upgrade)
		}
		return ctrl.Result{}, nil
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	if strings.HasPrefix(instance.Annotations[upgradePhaseAnnotation], "Major") &&
		instance.Annotations[upgradeUIDAnnotation] == string(upgrade.UID) {
		return r.reconcileMajorUpgrade(ctx, &upgrade, &instance, now())
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
	if upgrade.Spec.TargetMajorVersion != instance.Spec.Postgres.MajorVersion {
		freshBackupReady, err := r.ensureFreshUpgradeBackup(ctx, &upgrade, now())
		if err != nil {
			return ctrl.Result{}, err
		}
		if !freshBackupReady {
			return ctrl.Result{}, nil
		}
		if err := r.validateMajorUpgradeContract(ctx, &upgrade, &instance, now()); err != nil {
			return ctrl.Result{}, r.upgradeBlocked(ctx, &upgrade, "PlatformContractRejected", err.Error())
		}
		setCondition(&upgrade.Status.Conditions, upgrade.Generation, "ServiceRestorationTargetAtRisk",
			metav1.ConditionFalse, "BenchmarkWithinTarget",
			"the qualified upgrade path is within the requested service-restoration target")
		return r.reconcileMajorUpgrade(ctx, &upgrade, &instance, now())
	} else {
		return r.reconcileMinorUpgrade(ctx, &upgrade, &instance, now())
	}
}

func (r *PostgresUpgradeReconciler) reconcileUpgradeDeletion(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(upgrade, operationFinalizer) {
		return ctrl.Result{}, nil
	}
	var instance multisitepostgresv1alpha1.MultiSitePostgres
	err := r.Get(ctx, client.ObjectKey{
		Namespace: upgrade.Namespace, Name: upgrade.Spec.InstanceRef,
	}, &instance)
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	if err != nil {
		controllerutil.RemoveFinalizer(upgrade, operationFinalizer)
		return ctrl.Result{}, r.Update(ctx, upgrade)
	}
	owned := instance.Annotations[upgradeUIDAnnotation] == string(upgrade.UID)
	if upgrade.Annotations[forceAbandonAnnotation] == "true" {
		if owned {
			base := instance.DeepCopy()
			clearUpgradeAnnotations(&instance)
			if err := r.Patch(ctx, &instance, client.MergeFrom(base)); err != nil {
				return ctrl.Result{}, err
			}
		}
		_ = r.upgradeBlocked(ctx, upgrade, "ForceAbandoned",
			"Upgrade deletion was force-abandoned; residual resources require operator review")
		controllerutil.RemoveFinalizer(upgrade, operationFinalizer)
		return ctrl.Result{}, r.Update(ctx, upgrade)
	}
	if !owned || upgrade.Status.Phase == "" ||
		upgrade.Status.Phase == "Completed" || upgrade.Status.Phase == "Failed" {
		controllerutil.RemoveFinalizer(upgrade, operationFinalizer)
		return ctrl.Result{}, r.Update(ctx, upgrade)
	}
	phase := plan.MajorUpgradePhase(instance.Annotations[upgradePhaseAnnotation])
	switch phase {
	case plan.MajorUpgradePhasePreflight:
		base := instance.DeepCopy()
		clearUpgradeAnnotations(&instance)
		if err := r.Patch(ctx, &instance, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(upgrade, operationFinalizer)
		return ctrl.Result{}, r.Update(ctx, upgrade)
	case plan.MajorUpgradePhaseDrain, plan.MajorUpgradePhaseStop,
		plan.MajorUpgradePhaseSnapshot, plan.MajorUpgradePhaseUpgradePrimary,
		plan.MajorUpgradePhaseStanzaUpgrade, plan.MajorUpgradePhaseStartPrimary:
		if _, err := r.advanceMajorPhase(ctx, upgrade, &instance, plan.MajorUpgradePhaseRollback,
			"RollingBack", "DeletionRequested", "Rolling back before accepting operation deletion",
			r.now(), false); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: restoreProgressRequeue}, nil
	case plan.MajorUpgradePhaseRollback, plan.MajorUpgradePhaseRollbackStart,
		plan.MajorUpgradePhaseRollbackRestoreWrites:
		return ctrl.Result{RequeueAfter: restoreProgressRequeue},
			r.setUpgradePhase(ctx, upgrade, upgrade.Status.Phase,
				"CancellationInProgress", "Waiting for rollback to complete before deletion", r.now())
	default:
		return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, upgrade.Status.Phase,
			"ForwardRepairRequired",
			"Upgrade deletion is blocked after writes may have resumed; repair forward or force-abandon with explicit approval",
			r.now())
	}
}

func (r *PostgresUpgradeReconciler) reconcileTerminalUpgrade(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (bool, error) {
	if upgrade.Status.Phase != "Completed" && upgrade.Status.Phase != "Failed" {
		return false, nil
	}
	if instance.Annotations[upgradeUIDAnnotation] == string(upgrade.UID) {
		base := instance.DeepCopy()
		clearUpgradeAnnotations(instance)
		return true, r.Patch(ctx, instance, client.MergeFrom(base))
	}
	return true, nil
}

func (r *PostgresUpgradeReconciler) ensureFreshUpgradeBackup(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade, now time.Time,
) (bool, error) {
	condition := statusCondition(upgrade.Status.Conditions, "FreshBackupReady")
	if condition != nil && condition.Status == metav1.ConditionTrue {
		return true, nil
	}
	if condition != nil && condition.Status == metav1.ConditionFalse &&
		condition.Reason == "BackupFailed" {
		return false, r.upgradeBlocked(ctx, upgrade, "FreshBackupFailed", condition.Message)
	}
	if upgrade.Status.PreflightBackupRequestedAt == nil {
		requestedAt := metav1.NewTime(now)
		upgrade.Status.PreflightBackupRequestedAt = &requestedAt
		upgrade.Status.ObservedGeneration = upgrade.Generation
		upgrade.Status.Phase = "Preflight"
		setCondition(&upgrade.Status.Conditions, upgrade.Generation, "FreshBackupReady",
			metav1.ConditionFalse, "BackupRequested",
			"Waiting for a new full backup and archived WAL verification")
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return false, err
		}
		return false, nil
	}
	name := "mspsql-upgrade-backup-" + string(upgrade.UID)
	var directive corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{Namespace: upgrade.Namespace, Name: name}, &directive)
	if client.IgnoreNotFound(err) != nil {
		return false, err
	}
	if err != nil {
		spec, marshalErr := json.Marshal(map[string]string{
			"backupType":  "full",
			"scheduledAt": upgrade.Status.PreflightBackupRequestedAt.UTC().Format(time.RFC3339),
		})
		if marshalErr != nil {
			return false, marshalErr
		}
		directive = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: upgrade.Namespace, Name: name,
				Labels: map[string]string{
					"multisite-postgres.dev/directive":    "Backup",
					"multisite-postgres.dev/instance-ref": upgrade.Spec.InstanceRef,
				},
			},
			Data: map[string]string{
				"type": "Backup", "instanceRef": upgrade.Spec.InstanceRef, "deleting": "false",
				"operationUID":       string(upgrade.UID) + "-preflight-backup",
				"upgradeBackupPhase": "preflight",
				"spec.json":          string(spec),
			},
		}
		if err := controllerutil.SetControllerReference(upgrade, &directive, r.Scheme); err != nil {
			return false, err
		}
		if err := r.Create(ctx, &directive); err != nil {
			return false, err
		}
	}
	return false, r.setUpgradePhase(ctx, upgrade, "Preflight",
		"FreshBackupProgressing", "Waiting for the preflight full backup to complete", now)
}

func (r *PostgresUpgradeReconciler) reconcileMajorUpgrade(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
	now time.Time,
) (ctrl.Result, error) {
	if instance.Annotations[upgradeUIDAnnotation] == "" {
		if instance.Status.Primary == "" || !conditionTrue(instance.Status.Conditions, "TopologyReady") {
			return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "TopologyUnavailable",
				"major upgrade requires an observed primary and synchronous topology")
		}
		if err := r.patchUpgradeAnnotations(ctx, instance, map[string]string{
			upgradeUIDAnnotation:         string(upgrade.UID),
			upgradeNameAnnotation:        upgrade.Name,
			upgradePhaseAnnotation:       string(plan.MajorUpgradePhasePreflight),
			upgradeFromAnnotation:        instance.Status.Primary,
			upgradeSourceMajorAnnotation: strconv.FormatInt(int64(instance.Spec.Postgres.MajorVersion), 10),
			upgradeRevisionAnnotation:    fmt.Sprintf("%d", instance.Status.ActiveRevision+1),
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "Preflight",
			"PreflightIssued", "Validating upgrade binaries and rollback capabilities", now)
	}
	if instance.Annotations[upgradeUIDAnnotation] != string(upgrade.UID) {
		return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "OperationConflict",
			"instance is owned by another upgrade operation")
	}
	phase := plan.MajorUpgradePhase(instance.Annotations[upgradePhaseAnnotation])
	if majorUpgradeFailed(instance) {
		switch phase {
		case plan.MajorUpgradePhaseUpgradePrimary, plan.MajorUpgradePhaseStanzaUpgrade,
			plan.MajorUpgradePhaseStartPrimary:
			return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseRollback,
				"RollingBack", "PhaseFailed", "Restoring every PostgreSQL PVC from rollback storage",
				now, false)
		case plan.MajorUpgradePhaseRollback, plan.MajorUpgradePhaseRollbackStart,
			plan.MajorUpgradePhaseRollbackRestoreWrites:
		default:
			return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, string(phase),
				"ForwardRepairRequired",
				"Automatic rollback is unsafe or no complete rollback checkpoint exists for this phase", now)
		}
	}
	if !majorPhaseApplied(instance) {
		return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, string(phase),
			"PhaseProgressing", "Waiting for all sites to complete "+string(phase), now)
	}
	switch phase {
	case plan.MajorUpgradePhasePreflight:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseDrain,
			"DrainingWrites", "PreflightPassed", "Removing Pgpool write endpoints", now, true)
	case plan.MajorUpgradePhaseDrain:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseStop,
			"Stopping", "WritesDrained", "Stopping every PostgreSQL member cleanly", now, false)
	case plan.MajorUpgradePhaseStop:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseSnapshot,
			"CapturingRollback", "MembersStopped", "Capturing rollback storage", now, false)
	case plan.MajorUpgradePhaseSnapshot:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseUpgradePrimary,
			"UpgradingPrimary", "RollbackCaptured", "Converting the former primary data directory", now, false)
	case plan.MajorUpgradePhaseUpgradePrimary:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseStanzaUpgrade,
			"UpgradingBackupStanza", "PrimaryConverted",
			"Updating pgBackRest repository metadata for the target PostgreSQL version", now, false)
	case plan.MajorUpgradePhaseStanzaUpgrade:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseStartPrimary,
			"RestoringService", "BackupStanzaUpgraded",
			"Starting and verifying the upgraded primary", now, false)
	case plan.MajorUpgradePhaseStartPrimary:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseReplicas,
			"ReseedingReplicas", "PrimaryAccepted",
			"Recloning every standby from the upgraded primary before write service is restored", now, false)
	case plan.MajorUpgradePhaseReplicas:
		if !conditionTrue(instance.Status.Conditions, "TopologyReady") ||
			instance.Spec.Postgres.SynchronousStandbyCount > 0 &&
				!conditionTrue(instance.Status.Conditions, "SynchronousReplicationReady") {
			return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "ReseedingReplicas",
				"ReplicaSyncPending",
				"Waiting for a target-version standby to catch up and become synchronous before restoring writes",
				now)
		}
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseRestoreWrites,
			"RestoringWrites", "ReplicaSyncVerified", "Starting Pgpool write service after synchronous replay proof",
			now, false)
	case plan.MajorUpgradePhaseRestoreWrites:
		if upgrade.Status.WriteServiceRestoredAt == nil {
			restored := metav1.NewTime(now)
			upgrade.Status.WriteServiceRestoredAt = &restored
		}
		base := instance.DeepCopy()
		instance.Spec.Postgres.MajorVersion = upgrade.Spec.TargetMajorVersion
		instance.Spec.Postgres.Image = upgrade.Spec.TargetImage
		instance.Annotations[upgradePhaseAnnotation] = string(plan.MajorUpgradePhaseFinalize)
		instance.Annotations[upgradeRevisionAnnotation] = fmt.Sprintf("%d", instance.Status.ActiveRevision+1)
		if err := r.Patch(ctx, instance, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "Finalizing",
			"ReplicasReady", "Running final acceptance with the stable target specification", now)
	case plan.MajorUpgradePhaseFinalize:
		backupReady, result, err := r.ensurePostUpgradeBackup(ctx, upgrade, now)
		if err != nil || !backupReady {
			return result, err
		}
		if err := r.completeUpgrade(ctx, upgrade, now); err != nil {
			return ctrl.Result{}, err
		}
		base := instance.DeepCopy()
		clearUpgradeAnnotations(instance)
		return ctrl.Result{}, r.Patch(ctx, instance, client.MergeFrom(base))
	case plan.MajorUpgradePhaseRollback:
		return r.advanceMajorPhase(ctx, upgrade, instance, plan.MajorUpgradePhaseRollbackStart,
			"VerifyingRollback", "RollbackStorageRestored",
			"Starting and verifying the source PostgreSQL version", now, false)
	case plan.MajorUpgradePhaseRollbackStart:
		return r.advanceMajorPhase(ctx, upgrade, instance,
			plan.MajorUpgradePhaseRollbackRestoreWrites,
			"RestoringWrites", "RollbackAccepted", "Starting Pgpool on the restored source version",
			now, false)
	case plan.MajorUpgradePhaseRollbackRestoreWrites:
		restored := metav1.NewTime(now)
		upgrade.Status.WriteServiceRestoredAt = &restored
		upgrade.Status.ObservedGeneration = upgrade.Generation
		upgrade.Status.Phase = "Failed"
		setCondition(&upgrade.Status.Conditions, upgrade.Generation, "Ready", metav1.ConditionFalse,
			"RolledBack", "The failed major upgrade was rolled back to the source version")
		if err := r.Status().Update(ctx, upgrade); err != nil {
			return ctrl.Result{}, err
		}
		base := instance.DeepCopy()
		clearUpgradeAnnotations(instance)
		return ctrl.Result{}, r.Patch(ctx, instance, client.MergeFrom(base))
	default:
		return ctrl.Result{}, r.upgradeBlocked(ctx, upgrade, "InvalidState",
			"instance has an unknown major-upgrade phase")
	}
}

func (r *PostgresUpgradeReconciler) ensurePostUpgradeBackup(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade, now time.Time,
) (bool, ctrl.Result, error) {
	operationUID := fmt.Sprintf("%s-post-upgrade-backup-%d",
		upgrade.UID, upgrade.Status.PostUpgradeBackupAttempt)
	ensureUpgradeOperationProgress(upgrade, operationUID, now)
	condition := statusCondition(upgrade.Status.Conditions, "PostUpgradeBackupReady")
	if condition != nil && condition.Status == metav1.ConditionTrue {
		upgrade.Status.Operation.Terminal = true
		upgrade.Status.Operation.ManualInterventionRequired = false
		upgrade.Status.Operation.LastErrorReason = ""
		upgrade.Status.Operation.LastErrorMessage = ""
		upgrade.Status.Operation.NextRetryAt = nil
		return true, ctrl.Result{}, nil
	}
	if condition != nil && condition.Status == metav1.ConditionFalse &&
		condition.Reason == "BackupFailed" {
		upgrade.Status.Operation.Attempt = upgrade.Status.PostUpgradeBackupAttempt
		upgrade.Status.Operation.LastErrorReason = condition.Reason
		upgrade.Status.Operation.LastErrorMessage = condition.Message
		if upgrade.Status.PostUpgradeBackupAttempt >= maxPostUpgradeBackupAttempts {
			upgrade.Status.Operation.Terminal = true
			upgrade.Status.Operation.ManualInterventionRequired = true
			upgrade.Status.Operation.NextRetryAt = nil
			setCondition(&upgrade.Status.Conditions, upgrade.Generation, "PostUpgradeBackupReady",
				metav1.ConditionFalse, "ManualInterventionRequired",
				"Post-upgrade backup retry limit reached; fix repository access and request operator repair")
			setCondition(&upgrade.Status.Conditions, upgrade.Generation, "Ready",
				metav1.ConditionFalse, "ManualInterventionRequired",
				"Major upgrade is blocked until the required post-upgrade backup is repaired")
			upgrade.Status.Phase = "Finalizing"
			return false, ctrl.Result{}, r.Status().Update(ctx, upgrade)
		}
		upgrade.Status.PostUpgradeBackupAttempt++
		upgrade.Status.PostUpgradeBackupRequestedAt = nil
		operationUID = fmt.Sprintf("%s-post-upgrade-backup-%d",
			upgrade.UID, upgrade.Status.PostUpgradeBackupAttempt)
		upgrade.Status.Operation.OperationUID = operationUID
		upgrade.Status.Operation.Attempt = upgrade.Status.PostUpgradeBackupAttempt
		next := metav1.NewTime(now.Add(postUpgradeBackupRetryDelay(upgrade.UID,
			upgrade.Status.PostUpgradeBackupAttempt)))
		upgrade.Status.Operation.NextRetryAt = &next
		upgrade.Status.Operation.Terminal = false
		upgrade.Status.Operation.ManualInterventionRequired = false
		setCondition(&upgrade.Status.Conditions, upgrade.Generation, "PostUpgradeBackupReady",
			metav1.ConditionFalse, "BackupRetryRequested",
			"Retrying the required post-upgrade full backup")
		return false, ctrl.Result{RequeueAfter: next.Sub(now)}, r.Status().Update(ctx, upgrade)
	}
	if condition != nil && condition.Status == metav1.ConditionFalse &&
		condition.Reason == "BackupRetryRequested" &&
		upgrade.Status.Operation != nil &&
		upgrade.Status.Operation.NextRetryAt != nil &&
		now.Before(upgrade.Status.Operation.NextRetryAt.Time) {
		return false, ctrl.Result{RequeueAfter: upgrade.Status.Operation.NextRetryAt.Sub(now)}, nil
	}
	if upgrade.Status.PostUpgradeBackupRequestedAt == nil {
		requestedAt := metav1.NewTime(now)
		upgrade.Status.PostUpgradeBackupRequestedAt = &requestedAt
		upgrade.Status.Operation.OperationUID = operationUID
		upgrade.Status.Operation.Phase = "PostUpgradeBackup"
		upgrade.Status.Operation.Attempt = upgrade.Status.PostUpgradeBackupAttempt
		upgrade.Status.Operation.NextRetryAt = nil
		setCondition(&upgrade.Status.Conditions, upgrade.Generation, "PostUpgradeBackupReady",
			metav1.ConditionFalse, "BackupRequested",
			"Waiting for the post-upgrade full backup and archived WAL verification")
		return false, ctrl.Result{}, r.Status().Update(ctx, upgrade)
	}
	name := fmt.Sprintf("mspsql-post-upgrade-backup-%s-%d",
		upgrade.UID, upgrade.Status.PostUpgradeBackupAttempt)
	var directive corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{Namespace: upgrade.Namespace, Name: name}, &directive)
	if client.IgnoreNotFound(err) != nil {
		return false, ctrl.Result{}, err
	}
	if err != nil {
		spec, marshalErr := json.Marshal(map[string]string{
			"backupType":  "full",
			"scheduledAt": upgrade.Status.PostUpgradeBackupRequestedAt.UTC().Format(time.RFC3339),
		})
		if marshalErr != nil {
			return false, ctrl.Result{}, marshalErr
		}
		directive = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: upgrade.Namespace, Name: name,
				Labels: map[string]string{
					"multisite-postgres.dev/directive":    "Backup",
					"multisite-postgres.dev/instance-ref": upgrade.Spec.InstanceRef,
				},
			},
			Data: map[string]string{
				"type": "Backup", "instanceRef": upgrade.Spec.InstanceRef, "deleting": "false",
				"operationUID":       operationUID,
				"upgradeBackupPhase": "post-upgrade",
				"spec.json":          string(spec),
			},
		}
		if err := controllerutil.SetControllerReference(upgrade, &directive, r.Scheme); err != nil {
			return false, ctrl.Result{}, err
		}
		if err := r.Create(ctx, &directive); err != nil {
			return false, ctrl.Result{}, err
		}
	}
	return false, ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, "Finalizing",
		"PostUpgradeBackupProgressing", "Waiting for the post-upgrade full backup", now)
}

func ensureUpgradeOperationProgress(upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
	operationUID string, now time.Time,
) {
	if upgrade.Status.Operation == nil {
		deadline := metav1.NewTime(now.Add(postUpgradeBackupDeadline))
		upgrade.Status.Operation = &multisitepostgresv1alpha1.OperationProgressStatus{
			OperationUID: operationUID,
			Phase:        "PostUpgradeBackup",
			DeadlineAt:   &deadline,
		}
	}
	if upgrade.Status.Operation.DeadlineAt == nil {
		deadline := metav1.NewTime(now.Add(postUpgradeBackupDeadline))
		upgrade.Status.Operation.DeadlineAt = &deadline
	}
}

func postUpgradeBackupRetryDelay(operation types.UID, attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := postUpgradeBackupRetryBase
	for i := int32(1); i < attempt; i++ {
		delay *= 2
		if delay >= postUpgradeBackupRetryMax {
			delay = postUpgradeBackupRetryMax
			break
		}
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(operation))
	_, _ = hash.Write([]byte{byte(attempt)})
	jitter := time.Duration(hash.Sum32()%uint32(postUpgradeBackupRetryBase/time.Second)) * time.Second
	delay += jitter
	if delay > postUpgradeBackupRetryMax {
		return postUpgradeBackupRetryMax
	}
	return delay
}

func majorUpgradeFailed(instance *multisitepostgresv1alpha1.MultiSitePostgres) bool {
	for _, site := range instance.Status.Sites {
		condition := statusCondition(site.Conditions, "MajorUpgradeBlocked")
		if condition != nil && condition.Status == metav1.ConditionTrue &&
			strings.HasSuffix(condition.Reason, "Failed") {
			return true
		}
	}
	return false
}

func (r *PostgresUpgradeReconciler) advanceMajorPhase(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
	next plan.MajorUpgradePhase, statusPhase, reason, message string, now time.Time,
	startOutage bool,
) (ctrl.Result, error) {
	if err := r.patchUpgradeAnnotations(ctx, instance, map[string]string{
		upgradePhaseAnnotation:    string(next),
		upgradeRevisionAnnotation: fmt.Sprintf("%d", instance.Status.ActiveRevision+1),
	}); err != nil {
		return ctrl.Result{}, err
	}
	if startOutage && upgrade.Status.WriteOutageStartedAt == nil {
		value := metav1.NewTime(now)
		upgrade.Status.WriteOutageStartedAt = &value
	}
	return ctrl.Result{}, r.setUpgradePhase(ctx, upgrade, statusPhase, reason, message, now)
}

func majorPhaseApplied(instance *multisitepostgresv1alpha1.MultiSitePostgres) bool {
	expected, err := strconv.ParseInt(instance.Annotations[upgradeRevisionAnnotation], 10, 64)
	return err == nil && instance.Status.ActiveRevision >= expected &&
		allSitesApplied(instance.Status.Sites, instance.Status.ActiveRevision)
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
		upgradeRevisionAnnotation, upgradeSourceMajorAnnotation,
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

func (r *PostgresUpgradeReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PostgresUpgradeReconciler) validateMajorUpgradeContract(ctx context.Context,
	upgrade *multisitepostgresv1alpha1.PostgresUpgrade,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
	now time.Time,
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
	if !strings.Contains(upgrade.Spec.TargetImage, "@sha256:") {
		return fmt.Errorf("targetImage must be pinned by sha256 digest")
	}
	if upgrade.Spec.RollbackRetention.Duration <= 0 {
		return fmt.Errorf("rollbackRetention must be positive")
	}
	if instance.Spec.Backup == nil {
		return fmt.Errorf("major upgrade requires a configured pgBackRest repository")
	}
	benchmark := upgrade.Spec.Benchmark
	if benchmark == nil {
		return fmt.Errorf("major upgrade requires a qualified benchmark")
	}
	if benchmark.EstimatedWriteOutage.Duration <= 0 || benchmark.Evidence == "" {
		return fmt.Errorf("benchmark must include a positive outage estimate and evidence reference")
	}
	if !strings.Contains(benchmark.Evidence, "@sha256:") {
		return fmt.Errorf("benchmark evidence must be pinned by sha256 digest")
	}
	if benchmark.TestedAt.After(now.Add(5 * time.Minute)) {
		return fmt.Errorf("benchmark testedAt is in the future")
	}
	if now.Sub(benchmark.TestedAt.Time) > 30*24*time.Hour {
		return fmt.Errorf("benchmark is older than 30 days")
	}
	if benchmark.UpgradeImage != upgrade.Spec.UpgradeImage {
		return fmt.Errorf("benchmark upgradeImage does not match this operation")
	}
	if benchmark.SourceMajorVersion != instance.Spec.Postgres.MajorVersion ||
		benchmark.TargetMajorVersion != upgrade.Spec.TargetMajorVersion {
		return fmt.Errorf("benchmark PostgreSQL versions do not match this operation")
	}
	if benchmark.TDEEnabled != instance.Spec.TDE.Enabled {
		return fmt.Errorf("benchmark TDE mode does not match this instance")
	}
	if benchmark.EstimatedWriteOutage.Duration > upgrade.Spec.ServiceRestorationTarget.Duration {
		return fmt.Errorf("benchmarked write outage %s exceeds restoration target %s",
			benchmark.EstimatedWriteOutage.Duration, upgrade.Spec.ServiceRestorationTarget.Duration)
	}
	testedStorage := make(map[string]struct{}, len(benchmark.PostgresStorageClasses))
	for _, storageClass := range benchmark.PostgresStorageClasses {
		testedStorage[storageClass] = struct{}{}
	}
	for _, site := range instance.Spec.Sites {
		if site.Role != multisitepostgresv1alpha1.SiteRoleData || site.Storage.Postgres == nil {
			continue
		}
		if _, found := testedStorage[site.Storage.Postgres.StorageClassName]; !found {
			return fmt.Errorf("benchmark does not cover site %s StorageClass %s",
				site.Name, site.Storage.Postgres.StorageClassName)
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
			r.upgradeRequestsForInstance)).
		Named("postgresupgrade").
		Complete(r)
}

func (r *PostgresUpgradeReconciler) upgradeRequestsForInstance(ctx context.Context,
	object client.Object,
) []ctrl.Request {
	requests := map[types.NamespacedName]struct{}{}
	if name := object.GetAnnotations()[upgradeNameAnnotation]; name != "" {
		requests[types.NamespacedName{Namespace: object.GetNamespace(), Name: name}] = struct{}{}
	}
	var upgrades multisitepostgresv1alpha1.PostgresUpgradeList
	if err := r.List(ctx, &upgrades, client.InNamespace(object.GetNamespace())); err != nil {
		return namedRequests(requests)
	}
	for _, upgrade := range upgrades.Items {
		if upgrade.Spec.InstanceRef != object.GetName() ||
			upgrade.Status.Phase == "Completed" || upgrade.Status.Phase == "Failed" {
			continue
		}
		requests[types.NamespacedName{Namespace: upgrade.Namespace, Name: upgrade.Name}] = struct{}{}
	}
	return namedRequests(requests)
}

func namedRequests(names map[types.NamespacedName]struct{}) []ctrl.Request {
	requests := make([]ctrl.Request, 0, len(names))
	for name := range names {
		requests = append(requests, ctrl.Request{NamespacedName: name})
	}
	slices.SortFunc(requests, func(a, b ctrl.Request) int {
		return strings.Compare(a.String(), b.String())
	})
	return requests
}
