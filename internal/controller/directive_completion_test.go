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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestDatabaseCompletionIsAuthoritativeForCurrentGeneration(t *testing.T) {
	for _, test := range []struct {
		name               string
		observedGeneration int64
		wantReady          bool
	}{
		{name: "current", observedGeneration: 2, wantReady: true},
		{name: "stale", observedGeneration: 1, wantReady: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := testScheme(t)
			instance := &api.MultiSitePostgres{
				ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
				Status: api.MultiSitePostgresStatus{Conditions: []metav1.Condition{{
					Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSitesReady",
				}}},
			}
			database := &api.PostgresDatabase{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "platform", Name: "orders-api", UID: "database-uid", Generation: 2,
					Finalizers: []string{childFinalizer},
				},
				Spec: api.PostgresDatabaseSpec{InstanceRef: "orders", DatabaseName: "orders"},
				Status: api.PostgresDatabaseStatus{
					Phase: "Reconciling", ObservedGeneration: test.observedGeneration,
					Conditions: []metav1.Condition{{
						Type: "Succeeded", Status: metav1.ConditionTrue, Reason: "SQLApplied",
					}},
				},
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(instance, database).WithObjects(instance, database).Build()
			reconciler := &PostgresDatabaseReconciler{Client: kube, Scheme: scheme}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(database)}
			if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			var observed api.PostgresDatabase
			if err := kube.Get(context.Background(), request.NamespacedName, &observed); err != nil {
				t.Fatal(err)
			}
			ready := meta.IsStatusConditionTrue(observed.Status.Conditions, "Ready")
			if ready != test.wantReady {
				t.Fatalf("Ready=%t phase=%s conditions=%#v", ready, observed.Status.Phase,
					observed.Status.Conditions)
			}
			if test.wantReady && observed.Status.Phase != "Ready" {
				t.Fatalf("phase = %s", observed.Status.Phase)
			}
			var directives corev1.ConfigMapList
			if err := kube.List(context.Background(), &directives,
				client.InNamespace(database.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(directives.Items) != 1 {
				t.Fatalf("directives = %d", len(directives.Items))
			}
		})
	}
}
