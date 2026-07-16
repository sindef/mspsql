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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// PostgresUserReconciler reconciles a PostgresUser object
type PostgresUserReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresusers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=postgresusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=multisitepostgres;postgresdatabases,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *PostgresUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var user multisitepostgresv1alpha1.PostgresUser
	if err := r.Get(ctx, req.NamespacedName, &user); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if user.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(&user, childFinalizer) {
		controllerutil.AddFinalizer(&user, childFinalizer)
		return ctrl.Result{}, r.Update(ctx, &user)
	}
	if !user.DeletionTimestamp.IsZero() {
		if user.Spec.DeletionPolicy == multisitepostgresv1alpha1.DeletionPolicyRetain ||
			user.Status.Phase == "Deleted" {
			controllerutil.RemoveFinalizer(&user, childFinalizer)
			return ctrl.Result{}, r.Update(ctx, &user)
		}
		if err := reconcileDirective(ctx, r.Client, r.Scheme, &user,
			"mspsql-user-"+user.Name, "User", user.Spec.InstanceRef, user.Spec, true); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	for _, membership := range user.Spec.MemberOf {
		var database multisitepostgresv1alpha1.PostgresDatabase
		if err := r.Get(ctx, client.ObjectKey{
			Namespace: user.Namespace, Name: membership.DatabaseRef,
		}, &database); err != nil {
			user.Status.Phase = "Pending"
			setCondition(&user.Status.Conditions, user.Generation, "Ready", metav1.ConditionFalse,
				"DatabaseUnavailable", err.Error())
			return ctrl.Result{}, r.Status().Update(ctx, &user)
		}
		if !conditionTrue(database.Status.Conditions, "Ready") {
			user.Status.Phase = "Pending"
			setCondition(&user.Status.Conditions, user.Generation, "Ready", metav1.ConditionFalse,
				"DatabaseNotReady", "Waiting for database "+membership.DatabaseRef+" to be reconciled")
			if err := r.Status().Update(ctx, &user); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	if err := reconcileDirective(ctx, r.Client, r.Scheme, &user,
		"mspsql-user-"+user.Name, "User", user.Spec.InstanceRef, user.Spec, false); err != nil {
		return ctrl.Result{}, err
	}
	user.Status.ObservedGeneration = user.Generation
	user.Status.Phase = "Reconciling"
	setCondition(&user.Status.Conditions, user.Generation, "Ready", metav1.ConditionFalse,
		"DeclarationIssued", "Waiting for a site agent to reconcile the login role")
	if err := r.Status().Update(ctx, &user); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.PostgresUser{}).
		Owns(&corev1.ConfigMap{}).
		Named("postgresuser").
		Complete(r)
}
