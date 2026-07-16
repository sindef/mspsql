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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// PostgresDatabaseReconciler reconciles a PostgresDatabase object
type PostgresDatabaseReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=postgresdatabases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=postgresdatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=postgresdatabases/finalizers,verbs=update
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=multisitepostgres,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *PostgresDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var database multisitepostgresv1alpha1.PostgresDatabase
	if err := r.Get(ctx, req.NamespacedName, &database); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if database.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(&database, childFinalizer) {
		controllerutil.AddFinalizer(&database, childFinalizer)
		return ctrl.Result{}, r.Update(ctx, &database)
	}
	if !database.DeletionTimestamp.IsZero() {
		if database.Spec.DeletionPolicy == multisitepostgresv1alpha1.DeletionPolicyRetain ||
			database.Status.Phase == "Deleted" {
			controllerutil.RemoveFinalizer(&database, childFinalizer)
			return ctrl.Result{}, r.Update(ctx, &database)
		}
		if err := reconcileDirective(ctx, r.Client, r.Scheme, &database,
			"mspsql-database-"+database.Name, "Database", database.Spec.InstanceRef,
			database.Spec, true); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	var instance multisitepostgresv1alpha1.MultiSitePostgres
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: database.Namespace, Name: database.Spec.InstanceRef,
	}, &instance); err != nil {
		database.Status.Phase = "Pending"
		setCondition(&database.Status.Conditions, database.Generation, "Ready", metav1.ConditionFalse,
			"InstanceUnavailable", err.Error())
		return ctrl.Result{}, r.Status().Update(ctx, &database)
	}
	if err := reconcileDirective(ctx, r.Client, r.Scheme, &database,
		"mspsql-database-"+database.Name, "Database", database.Spec.InstanceRef,
		database.Spec, false); err != nil {
		return ctrl.Result{}, err
	}
	database.Status.ObservedGeneration = database.Generation
	database.Status.Phase = "Pending"
	setCondition(&database.Status.Conditions, database.Generation, "Ready", metav1.ConditionFalse,
		"DeclarationIssued", "Waiting for a site agent to reconcile the database")
	if conditionTrue(instance.Status.Conditions, "Ready") {
		database.Status.Phase = "Reconciling"
	}
	if err := r.Status().Update(ctx, &database); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.PostgresDatabase{}).
		Owns(&corev1.ConfigMap{}).
		Named("postgresdatabase").
		Complete(r)
}
