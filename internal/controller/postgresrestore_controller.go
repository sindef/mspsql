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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// PostgresRestoreReconciler reconciles a PostgresRestore object
type PostgresRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=postgresrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=postgresrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=postgresrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=multisitepostgres;postgresupgrades,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *PostgresRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var restore multisitepostgresv1alpha1.PostgresRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !restore.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	var source multisitepostgresv1alpha1.MultiSitePostgres
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: restore.Namespace, Name: restore.Spec.SourceInstanceRef,
	}, &source); err != nil {
		return ctrl.Result{}, r.restoreBlocked(ctx, &restore, "SourceUnavailable", err.Error())
	}
	var target multisitepostgresv1alpha1.MultiSitePostgres
	err := r.Get(ctx, client.ObjectKey{
		Namespace: restore.Namespace, Name: restore.Spec.TargetInstanceRef,
	}, &target)
	if err == nil && target.Annotations["multisite-postgres.dev/restore-uid"] != string(restore.UID) {
		return ctrl.Result{}, r.restoreBlocked(ctx, &restore, "TargetExists",
			"target instance exists and is not owned by this restore")
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if !conditionTrue(source.Status.Conditions, "Ready") ||
		!conditionTrue(source.Status.Conditions, "RecoveryWindowAvailable") {
		return ctrl.Result{}, r.restoreBlocked(ctx, &restore, "PreflightFailed",
			"source must be Ready with a verified recovery window")
	}
	if conflict, err := r.operationConflict(ctx, restore.Namespace,
		restore.Spec.TargetInstanceRef, restore.Name, "restore"); err != nil {
		return ctrl.Result{}, err
	} else if conflict != "" {
		return ctrl.Result{}, r.restoreBlocked(ctx, &restore, "OperationConflict", conflict)
	}
	if err := reconcileDirective(ctx, r.Client, r.Scheme, &restore,
		"mspsql-restore-"+restore.Name, "Restore", restore.Spec.TargetInstanceRef,
		restore.Spec, false); err != nil {
		return ctrl.Result{}, err
	}
	restore.Status.ObservedGeneration = restore.Generation
	restore.Status.Phase = "Restoring"
	setCondition(&restore.Status.Conditions, restore.Generation, "Ready", metav1.ConditionFalse,
		"RestoreIssued", "PITR restore directive has been issued")
	if err := r.Status().Update(ctx, &restore); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *PostgresRestoreReconciler) restoreBlocked(ctx context.Context,
	restore *multisitepostgresv1alpha1.PostgresRestore, reason, message string,
) error {
	restore.Status.ObservedGeneration = restore.Generation
	restore.Status.Phase = "Preflight"
	setCondition(&restore.Status.Conditions, restore.Generation, "Ready", metav1.ConditionFalse, reason, message)
	return r.Status().Update(ctx, restore)
}

func (r *PostgresRestoreReconciler) operationConflict(ctx context.Context, namespace, instanceRef,
	operationName, operationKind string,
) (string, error) {
	var upgrades multisitepostgresv1alpha1.PostgresUpgradeList
	if err := r.List(ctx, &upgrades, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, upgrade := range upgrades.Items {
		if upgrade.Spec.InstanceRef == instanceRef && upgrade.Status.Phase != "Completed" &&
			upgrade.Status.Phase != "Failed" {
			return "another upgrade targets this instance", nil
		}
	}
	var restores multisitepostgresv1alpha1.PostgresRestoreList
	if err := r.List(ctx, &restores, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, restore := range restores.Items {
		if restore.Spec.TargetInstanceRef == instanceRef &&
			!(operationKind == "restore" && restore.Name == operationName) &&
			restore.Status.Phase != "Completed" && restore.Status.Phase != "Failed" {
			return "another restore targets this instance", nil
		}
	}
	return "", nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.PostgresRestore{}).
		Owns(&corev1.ConfigMap{}).
		Named("postgresrestore").
		Complete(r)
}
