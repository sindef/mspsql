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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestPostgresUserWaitsForMembershipDatabase(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	database := &api.PostgresDatabase{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-api"},
		Status: api.PostgresDatabaseStatus{Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: "Reconciling",
		}}},
	}
	user := &api.PostgresUser{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-app", UID: types.UID("user-uid"),
			Finalizers: []string{childFinalizer},
		},
		Spec: api.PostgresUserSpec{
			InstanceRef: "orders", RoleName: "orders_app",
			MemberOf: []api.RoleMembership{{DatabaseRef: "orders-api", Role: "orders_rw"}},
			PasswordVaultRef: api.VaultSecretReference{
				Mount: "mspsql", Path: "postgres/orders/users/orders-app", Key: "password",
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(user, database).WithObjects(user, database).Build()
	reconciler := &PostgresUserReconciler{Client: kube, Scheme: scheme}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(user)}
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("user was not requeued while its membership database was pending")
	}
	var observed api.PostgresUser
	if err := kube.Get(context.Background(), request.NamespacedName, &observed); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(observed.Status.Conditions, "Ready")
	if ready == nil || ready.Reason != "DatabaseNotReady" {
		t.Fatalf("user Ready condition = %#v", ready)
	}
	var directives corev1.ConfigMapList
	if err := kube.List(context.Background(), &directives, client.InNamespace(user.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(directives.Items) != 0 {
		t.Fatal("user directive was issued before its membership database was ready")
	}
}
