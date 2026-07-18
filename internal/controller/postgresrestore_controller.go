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
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

const (
	restoreUIDAnnotation       = "multisite-postgres.dev/restore-uid"
	restoreNameAnnotation      = "multisite-postgres.dev/restore-name"
	restoreSourceUIDAnnotation = "multisite-postgres.dev/restore-source-uid"
	restorePhaseAnnotation     = "multisite-postgres.dev/restore-phase"
	restoreProgressRequeue     = 10 * time.Second
)

// PostgresRestoreReconciler reconciles a PostgresRestore object.
type PostgresRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Now    func() time.Time
}

// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=multisitepostgres,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresupgrades,verbs=get;list;watch

func (r *PostgresRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var restore api.PostgresRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !restore.DeletionTimestamp.IsZero() || restore.Status.Phase == "Completed" {
		return ctrl.Result{}, nil
	}

	var source api.MultiSitePostgres
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: restore.Namespace, Name: restore.Spec.SourceInstanceRef,
	}, &source); err != nil {
		return restoreProgressOrError(r.restoreBlocked(ctx, &restore, "SourceUnavailable", err.Error()))
	}
	if err := r.preflight(ctx, &restore, &source); err != nil {
		return restoreProgressOrError(r.restoreBlocked(ctx, &restore, "PreflightFailed", err.Error()))
	}

	var target api.MultiSitePostgres
	err := r.Get(ctx, client.ObjectKey{
		Namespace: restore.Namespace, Name: restore.Spec.TargetInstanceRef,
	}, &target)
	if apierrors.IsNotFound(err) {
		target, err = r.newTarget(&restore, &source)
		if err != nil {
			return restoreProgressOrError(r.restoreBlocked(ctx, &restore, "InvalidTarget", err.Error()))
		}
		if err := r.Create(ctx, &target); err != nil {
			return ctrl.Result{}, err
		}
		return restoreProgressOrError(r.setRestorePhase(ctx, &restore, "Provisioning",
			"TargetCreated", "The isolated restore target has been created"))
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if target.Annotations[restoreUIDAnnotation] != string(restore.UID) ||
		target.Annotations[restoreSourceUIDAnnotation] != string(source.UID) {
		return restoreProgressOrError(r.restoreBlocked(ctx, &restore, "TargetExists",
			"target instance exists and is not an empty placeholder owned by this restore"))
	}

	switch plan.RestorePhase(target.Annotations[restorePhaseAnnotation]) {
	case plan.RestorePhaseSeed:
		seedSite, seedMember, selectErr := selectRestoreSeed(target.Spec.Sites)
		if selectErr != nil {
			return restoreProgressOrError(r.restoreBlocked(ctx, &restore, "InvalidTarget", selectErr.Error()))
		}
		if target.Status.Primary != seedMember ||
			!conditionTrue(target.Status.Conditions, "TopologyReady") {
			return restoreProgressOrError(r.setRestorePhase(ctx, &restore, "Restoring",
				"SeedRecoveryInProgress",
				fmt.Sprintf("Waiting for %s/%s to recover and promote", seedSite, seedMember)))
		}
		if err := r.setTargetPhase(ctx, &target, plan.RestorePhaseReplicas); err != nil {
			return ctrl.Result{}, err
		}
		return restoreProgressOrError(r.setRestorePhase(ctx, &restore, "SeedingReplicas",
			"SeedPromoted", "Point-in-time recovery completed; replicas are now being cloned"))
	case plan.RestorePhaseReplicas:
		if !conditionTrue(target.Status.Conditions, "TopologyReady") ||
			int32(len(target.Status.SynchronousStandbys)) < target.Spec.Postgres.SynchronousStandbyCount {
			return restoreProgressOrError(r.setRestorePhase(ctx, &restore, "SeedingReplicas",
				"ReplicationConverging", "Waiting for the required synchronous replicas"))
		}
		if err := r.setTargetPhase(ctx, &target, plan.RestorePhaseVerify); err != nil {
			return ctrl.Result{}, err
		}
		return restoreProgressOrError(r.setRestorePhase(ctx, &restore, "Verifying",
			"ReplicationReady", "Synchronous replication is ready; acceptance checks are running"))
	case plan.RestorePhaseVerify:
		if !conditionTrue(target.Status.Conditions, "Ready") ||
			!conditionTrue(target.Status.Conditions, "TopologyReady") ||
			source.Spec.TDE.Enabled && !conditionTrue(target.Status.Conditions, "TDEVerified") {
			return restoreProgressOrError(r.setRestorePhase(ctx, &restore, "Verifying",
				"AcceptancePending", "Waiting for target topology, Pgpool and TDE acceptance"))
		}
		if err := r.setTargetPhase(ctx, &target, "Completed"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.completeRestore(ctx, &restore)
	case "Completed":
		return ctrl.Result{}, r.completeRestore(ctx, &restore)
	default:
		return restoreProgressOrError(r.restoreBlocked(ctx, &restore, "InvalidTarget",
			"target instance has an unknown restore phase"))
	}
}

func restoreProgressResult() ctrl.Result {
	return ctrl.Result{RequeueAfter: restoreProgressRequeue}
}

func restoreProgressOrError(err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return restoreProgressResult(), nil
}

func (r *PostgresRestoreReconciler) completeRestore(ctx context.Context,
	restore *api.PostgresRestore,
) error {
	restore.Status.ObservedGeneration = restore.Generation
	restore.Status.Phase = "Completed"
	restore.Status.RecoveredTo = restore.Spec.TargetTime.DeepCopy()
	if restore.Spec.BackupSet != "" {
		restore.Status.SelectedBackupSet = restore.Spec.BackupSet
	}
	setCondition(&restore.Status.Conditions, restore.Generation, "Ready", metav1.ConditionTrue,
		"RestoreCompleted", "The target instance passed restore acceptance")
	setCondition(&restore.Status.Conditions, restore.Generation, "Completed", metav1.ConditionTrue,
		"AcceptancePassed", "Point-in-time recovery completed at "+r.now().UTC().Format(time.RFC3339))
	return r.Status().Update(ctx, restore)
}

func (r *PostgresRestoreReconciler) preflight(ctx context.Context, restore *api.PostgresRestore,
	source *api.MultiSitePostgres,
) error {
	if source.Spec.Backup == nil {
		return fmt.Errorf("source has no pgBackRest repository")
	}
	if !conditionTrue(source.Status.Conditions, "Ready") ||
		!conditionTrue(source.Status.Conditions, "RecoveryWindowAvailable") ||
		source.Status.RecoveryWindowStart == nil {
		return fmt.Errorf("source must be Ready with a verified recovery window")
	}
	if restore.Spec.TargetTime.Before(source.Status.RecoveryWindowStart) {
		return fmt.Errorf("target time precedes the verified recovery window")
	}
	if restore.Spec.TargetTime.After(r.now()) {
		return fmt.Errorf("target time is in the future")
	}
	if conflict, err := r.operationConflict(ctx, restore.Namespace,
		restore.Spec.TargetInstanceRef, restore.Name, "restore"); err != nil {
		return err
	} else if conflict != "" {
		return fmt.Errorf("%s", conflict)
	}
	return nil
}

func (r *PostgresRestoreReconciler) newTarget(restore *api.PostgresRestore,
	source *api.MultiSitePostgres,
) (api.MultiSitePostgres, error) {
	spec := source.DeepCopy().Spec
	spec.Backup = restore.Spec.TargetBackup.DeepCopy()
	if len(restore.Spec.RestoreTopology.Sites) > 0 {
		spec.Sites = append([]api.PostgresSiteSpec(nil), restore.Spec.RestoreTopology.Sites...)
	} else {
		spec.Sites = append([]api.PostgresSiteSpec(nil), source.Spec.Sites...)
		for i := range spec.Sites {
			spec.Sites[i].Namespace = restore.Spec.TargetInstanceRef + "-" + spec.Sites[i].Name
		}
	}
	if _, _, err := selectRestoreSeed(spec.Sites); err != nil {
		return api.MultiSitePostgres{}, err
	}
	for _, site := range spec.Sites {
		if problems := validation.IsDNS1123Label(site.Namespace); len(problems) > 0 {
			return api.MultiSitePostgres{}, fmt.Errorf("target namespace %q is invalid: %s",
				site.Namespace, problems[0])
		}
	}
	return api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: restore.Namespace,
			Name:      restore.Spec.TargetInstanceRef,
			Annotations: map[string]string{
				restoreUIDAnnotation:       string(restore.UID),
				restoreNameAnnotation:      restore.Name,
				restoreSourceUIDAnnotation: string(source.UID),
				restorePhaseAnnotation:     string(plan.RestorePhaseSeed),
			},
		},
		Spec: spec,
	}, nil
}

func selectRestoreSeed(sites []api.PostgresSiteSpec) (string, string, error) {
	var selected *api.PostgresSiteSpec
	for i := range sites {
		site := &sites[i]
		if site.Role != api.SiteRoleData || site.Components.PostgresReplicas < 1 {
			continue
		}
		if selected == nil || site.PrimaryPreference > selected.PrimaryPreference ||
			site.PrimaryPreference == selected.PrimaryPreference && site.Name < selected.Name {
			selected = site
		}
	}
	if selected == nil {
		return "", "", fmt.Errorf("restore topology has no PostgreSQL member")
	}
	return selected.Name, fmt.Sprintf("postgres-%s-0", selected.Name), nil
}

func (r *PostgresRestoreReconciler) setTargetPhase(ctx context.Context,
	target *api.MultiSitePostgres, phase plan.RestorePhase,
) error {
	base := target.DeepCopy()
	target.Annotations[restorePhaseAnnotation] = string(phase)
	return r.Patch(ctx, target, client.MergeFrom(base))
}

func (r *PostgresRestoreReconciler) setRestorePhase(ctx context.Context, restore *api.PostgresRestore,
	phase, reason, message string,
) error {
	before := restore.Status.DeepCopy()
	restore.Status.ObservedGeneration = restore.Generation
	restore.Status.Phase = phase
	setCondition(&restore.Status.Conditions, restore.Generation, "Ready", metav1.ConditionFalse, reason, message)
	if apiequality.Semantic.DeepEqual(before, &restore.Status) {
		return nil
	}
	return r.Status().Update(ctx, restore)
}

func (r *PostgresRestoreReconciler) restoreBlocked(ctx context.Context,
	restore *api.PostgresRestore, reason, message string,
) error {
	before := restore.Status.DeepCopy()
	restore.Status.ObservedGeneration = restore.Generation
	restore.Status.Phase = "Preflight"
	setCondition(&restore.Status.Conditions, restore.Generation, "Ready", metav1.ConditionFalse, reason, message)
	if apiequality.Semantic.DeepEqual(before, &restore.Status) {
		return nil
	}
	return r.Status().Update(ctx, restore)
}

func (r *PostgresRestoreReconciler) operationConflict(ctx context.Context, namespace, instanceRef,
	operationName, operationKind string,
) (string, error) {
	var upgrades api.PostgresUpgradeList
	if err := r.List(ctx, &upgrades, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, upgrade := range upgrades.Items {
		if upgrade.Spec.InstanceRef == instanceRef && upgrade.Status.Phase != "Completed" &&
			upgrade.Status.Phase != "Failed" {
			return "another upgrade targets this instance", nil
		}
	}
	var restores api.PostgresRestoreList
	if err := r.List(ctx, &restores, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, restore := range restores.Items {
		if restore.Spec.TargetInstanceRef == instanceRef &&
			(operationKind != "restore" || restore.Name != operationName) &&
			restore.Status.Phase != "Completed" && restore.Status.Phase != "Failed" {
			return "another restore targets this instance", nil
		}
	}
	return "", nil
}

func (r *PostgresRestoreReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&api.PostgresRestore{}).
		Watches(&api.MultiSitePostgres{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, object client.Object) []ctrl.Request {
				name := object.GetAnnotations()[restoreNameAnnotation]
				if name == "" {
					return nil
				}
				return []ctrl.Request{{NamespacedName: types.NamespacedName{
					Namespace: object.GetNamespace(), Name: name,
				}}}
			})).
		Named("postgresrestore").
		Complete(r)
}
