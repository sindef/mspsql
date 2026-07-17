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
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// PostgresUserReconciler reconciles a PostgresUser object
type PostgresUserReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const userDatabaseRefField = ".spec.memberOf.databaseRef"

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
	observedCurrentGeneration := user.Status.ObservedGeneration == user.Generation
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
		if database.Spec.InstanceRef != user.Spec.InstanceRef {
			user.Status.Phase = "Pending"
			setCondition(&user.Status.Conditions, user.Generation, "Ready", metav1.ConditionFalse,
				"DatabaseInstanceMismatch", "Database "+membership.DatabaseRef+" belongs to another instance")
			return ctrl.Result{}, r.Status().Update(ctx, &user)
		}
		roleDeclared := slices.ContainsFunc(database.Spec.Roles, func(role multisitepostgresv1alpha1.DatabaseRole) bool {
			return role.Name == membership.Role
		})
		if !roleDeclared {
			user.Status.Phase = "Pending"
			setCondition(&user.Status.Conditions, user.Generation, "Ready", metav1.ConditionFalse,
				"DatabaseRoleUnavailable", "Database "+membership.DatabaseRef+" does not declare role "+membership.Role)
			return ctrl.Result{}, r.Status().Update(ctx, &user)
		}
		if !conditionTrue(database.Status.Conditions, "Ready") {
			user.Status.Phase = "Pending"
			setCondition(&user.Status.Conditions, user.Generation, "Ready", metav1.ConditionFalse,
				"DatabaseNotReady", "Waiting for database "+membership.DatabaseRef+" to be reconciled")
			if err := r.Status().Update(ctx, &user); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}
	if err := reconcileDirective(ctx, r.Client, r.Scheme, &user,
		"mspsql-user-"+user.Name, "User", user.Spec.InstanceRef, user.Spec, false); err != nil {
		return ctrl.Result{}, err
	}
	if observedCurrentGeneration && conditionTrue(user.Status.Conditions, "Succeeded") {
		if user.Status.Phase == "Ready" && conditionTrue(user.Status.Conditions, "Ready") {
			return ctrl.Result{}, nil
		}
		user.Status.Phase = "Ready"
		setCondition(&user.Status.Conditions, user.Generation, "Ready", metav1.ConditionTrue,
			"UserReconciled", "The login role and credentials are applied")
		return ctrl.Result{}, r.Status().Update(ctx, &user)
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
	if err := mgr.GetFieldIndexer().IndexField(context.Background(),
		&multisitepostgresv1alpha1.PostgresUser{}, databaseInstanceRefField,
		func(object client.Object) []string {
			return []string{object.(*multisitepostgresv1alpha1.PostgresUser).Spec.InstanceRef}
		}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(),
		&multisitepostgresv1alpha1.PostgresUser{}, userDatabaseRefField,
		func(object client.Object) []string {
			user := object.(*multisitepostgresv1alpha1.PostgresUser)
			values := make([]string, 0, len(user.Spec.MemberOf))
			for _, membership := range user.Spec.MemberOf {
				values = append(values, membership.DatabaseRef)
			}
			return values
		}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.PostgresUser{}).
		Owns(&corev1.ConfigMap{}).
		Watches(&multisitepostgresv1alpha1.MultiSitePostgres{},
			handler.EnqueueRequestsFromMapFunc(r.usersForInstance)).
		Watches(&multisitepostgresv1alpha1.PostgresDatabase{},
			handler.EnqueueRequestsFromMapFunc(r.usersForDatabase)).
		Named("postgresuser").
		Complete(r)
}

func (r *PostgresUserReconciler) usersForInstance(ctx context.Context,
	object client.Object,
) []reconcile.Request {
	return r.usersForField(ctx, object.GetNamespace(), databaseInstanceRefField, object.GetName())
}

func (r *PostgresUserReconciler) usersForDatabase(ctx context.Context,
	object client.Object,
) []reconcile.Request {
	return r.usersForField(ctx, object.GetNamespace(), userDatabaseRefField, object.GetName())
}

func (r *PostgresUserReconciler) usersForField(ctx context.Context, namespace, field, value string,
) []reconcile.Request {
	var users multisitepostgresv1alpha1.PostgresUserList
	if err := r.List(ctx, &users, client.InNamespace(namespace),
		client.MatchingFields{field: value}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(users.Items))
	for i := range users.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&users.Items[i]),
		})
	}
	return requests
}
