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

package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	api "github.com/sindef/mspsql/api/v1alpha1"
	controlv1 "github.com/sindef/mspsql/gen/control/v1"
)

func TestSiteConditionTransitionTimeChangesOnlyWithStatus(t *testing.T) {
	original := metav1.NewTime(time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC))
	conditions := []metav1.Condition{{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "Pending",
		LastTransitionTime: original,
	}}
	setSiteCondition(&conditions, "Ready", metav1.ConditionFalse, "StillPending", "waiting")
	if !conditions[0].LastTransitionTime.Equal(&original) {
		t.Fatalf("transition time changed without a status transition: %v", conditions[0].LastTransitionTime)
	}
	setSiteCondition(&conditions, "Ready", metav1.ConditionTrue, "Completed", "ready")
	if conditions[0].LastTransitionTime.Equal(&original) {
		t.Fatal("transition time did not change when status transitioned")
	}
}

func TestDirectiveResultRetriesStatusConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	restore := &api.PostgresRestore{ObjectMeta: metav1.ObjectMeta{
		Namespace: "platform", Name: "orders-restore", Generation: 3,
	}}
	attempts := 0
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(restore).
		WithObjects(restore).WithInterceptorFuncs(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, underlying client.Client, subresource string,
			object client.Object, options ...client.SubResourceUpdateOption,
		) error {
			attempts++
			if attempts == 1 {
				return apierrors.NewConflict(schema.GroupResource{
					Group: api.GroupVersion.Group, Resource: "postgresrestores",
				}, object.GetName(), errors.New("injected conflict"))
			}
			return underlying.Status().Update(ctx, object, options...)
		},
	}).Build()
	server := &Server{Client: kube}
	result := &controlv1.PlanResult{Conditions: []*controlv1.Condition{
		{Type: "Succeeded", Status: string(metav1.ConditionTrue), Reason: "RestoreVerified",
			Message: "restore completed"},
	}}
	if err := server.recordRestoreDirectiveResult(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform"},
	}, restore.Name, result); err != nil {
		t.Fatal(err)
	}
	var observed api.PostgresRestore
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(restore), &observed); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(observed.Status.Conditions, "Succeeded")
	if attempts != 2 || observed.Status.Phase != "Ready" || observed.Status.ObservedGeneration != 3 ||
		condition == nil || condition.ObservedGeneration != 3 {
		t.Fatalf("attempts=%d status=%#v", attempts, observed.Status)
	}
}

func TestDirectiveOwnerMustMatchAuthoritativeObject(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	database := &api.PostgresDatabase{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-api", UID: types.UID("database-uid"), Generation: 2,
		},
		Spec: api.PostgresDatabaseSpec{InstanceRef: "orders", DatabaseName: "orders"},
	}
	encoded, err := json.Marshal(database.Spec)
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	owner := metav1.OwnerReference{
		APIVersion: api.GroupVersion.String(), Kind: "PostgresDatabase",
		Name: database.Name, UID: database.UID, Controller: &controller,
	}
	directive := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: database.Namespace, Name: "mspsql-database-" + database.Name,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Data: map[string]string{
			"type": "Database", "instanceRef": "orders", "deleting": "false",
			"operationUID": "database-uid-2-false", "spec.json": string(encoded),
		},
	}
	server := &Server{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(database).Build()}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders", UID: "instance-uid"},
	}
	trusted, err := server.directiveOwnerTrusted(context.Background(), directive,
		metav1.GetControllerOf(directive), instance)
	if err != nil || !trusted {
		t.Fatalf("authoritative directive rejected: trusted=%v err=%v", trusted, err)
	}
	directive.Data["spec.json"] = `{"databaseName":"injected"}`
	trusted, err = server.directiveOwnerTrusted(context.Background(), directive,
		metav1.GetControllerOf(directive), instance)
	if err != nil || trusted {
		t.Fatalf("tampered directive accepted: trusted=%v err=%v", trusted, err)
	}
	directive.OwnerReferences = nil
	trusted, err = server.directiveOwnerTrusted(context.Background(), directive, nil, instance)
	if err != nil || trusted {
		t.Fatalf("ownerless directive accepted: trusted=%v err=%v", trusted, err)
	}
}

func TestPlanResultDoesNotSetAggregateReady(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "platform",
			Name:       "orders",
			UID:        types.UID("instance-uid"),
			Generation: 7,
		},
		Status: api.MultiSitePostgresStatus{
			Phase:          "Reconciling",
			ActiveRevision: 3,
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionFalse, Reason: "TopologyPending",
				Message: "Waiting for current topology consensus across all data sites",
			}},
			Sites: []api.SiteRevisionStatus{
				{Name: "vic", DesiredRevision: 3, AppliedRevision: 2, Phase: "Applying"},
				{Name: "qld", DesiredRevision: 3, AppliedRevision: 3, Phase: "Ready"},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(instance).
		WithObjects(instance).
		Build()
	server := &Server{
		Client: kube,
		Now: func() time.Time {
			return time.Date(2026, 8, 2, 1, 2, 3, 4, time.UTC)
		},
	}
	result := &controlv1.PlanResult{
		InstanceUid:     string(instance.UID),
		AppliedRevision: 3,
		Conditions: []*controlv1.Condition{{
			Type: "Ready", Status: string(metav1.ConditionTrue), Reason: "SiteReady",
			Message: "site applied the plan",
		}},
	}
	if err := server.recordResult(context.Background(), "vic", result); err != nil {
		t.Fatal(err)
	}
	var observed api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Status.Phase != "Reconciling" {
		t.Fatalf("aggregate phase changed to %q", observed.Status.Phase)
	}
	ready := meta.FindStatusCondition(observed.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "TopologyPending" {
		t.Fatalf("aggregate Ready condition = %#v", ready)
	}
	if observed.Status.Sites[0].AppliedRevision != 3 || observed.Status.Sites[0].Phase != "Ready" {
		t.Fatalf("site status was not updated: %#v", observed.Status.Sites[0])
	}
	if observed.Annotations["multisite-postgres.dev/address-observation"] == "" {
		t.Fatalf("result did not trigger instance reconcile: annotations=%#v", observed.Annotations)
	}
}

func TestInterleavedPlanResultsDoNotPromoteAggregateReady(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance-uid"), Generation: 9,
		},
		Status: api.MultiSitePostgresStatus{
			Phase:          "Reconciling",
			ActiveRevision: 5,
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionFalse, Reason: "TopologyPending",
			}},
			Sites: []api.SiteRevisionStatus{
				{Name: "vic", DesiredRevision: 5, AppliedRevision: 4, Phase: "Applying"},
				{Name: "qld", DesiredRevision: 5, AppliedRevision: 4, Phase: "Applying"},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(instance).
		WithObjects(instance).
		Build()
	replicaA := &Server{Client: kube}
	replicaB := &Server{Client: kube}
	report := func(server *Server, site string) {
		t.Helper()
		if err := server.recordResult(context.Background(), site, &controlv1.PlanResult{
			InstanceUid:     string(instance.UID),
			AppliedRevision: 5,
			Conditions: []*controlv1.Condition{{
				Type: "Ready", Status: string(metav1.ConditionTrue), Reason: "SiteReady",
			}},
		}); err != nil {
			t.Fatal(err)
		}
		var observed api.MultiSitePostgres
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &observed); err != nil {
			t.Fatal(err)
		}
		ready := meta.FindStatusCondition(observed.Status.Conditions, "Ready")
		if observed.Status.Phase != "Reconciling" || ready == nil ||
			ready.Status != metav1.ConditionFalse || ready.Reason != "TopologyPending" {
			t.Fatalf("aggregate status after %s result = %#v", site, observed.Status)
		}
	}
	report(replicaA, "vic")
	report(replicaB, "qld")
}

func TestUpdateInstanceSiteSkipsUnchangedStatusWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance-uid"),
		},
		Status: api.MultiSitePostgresStatus{Sites: []api.SiteRevisionStatus{{
			Name: "vic", DesiredRevision: 4, AppliedRevision: 4, Phase: "Ready",
		}}},
	}
	statusUpdates := 0
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).
		WithObjects(instance).WithInterceptorFuncs(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, underlying client.Client, subresource string,
			object client.Object, options ...client.SubResourceUpdateOption,
		) error {
			statusUpdates++
			return underlying.Status().Update(ctx, object, options...)
		},
	}).Build()
	server := &Server{Client: kube}
	if err := server.updateInstanceSite(context.Background(), string(instance.UID), "vic",
		func(_ *api.SiteRevisionStatus) {}); err != nil {
		t.Fatal(err)
	}
	if statusUpdates != 0 {
		t.Fatalf("unchanged site report wrote status %d times", statusUpdates)
	}
}

func BenchmarkUpdateInstanceSiteSkipsUnchangedStatusWriteAtFleetScale(b *testing.B) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		b.Fatal(err)
	}
	objects := make([]client.Object, 0, 1000)
	targetUID := types.UID("instance-999")
	for i := range 1000 {
		instance := &api.MultiSitePostgres{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "platform", Name: fmt.Sprintf("orders-%04d", i),
				UID: types.UID(fmt.Sprintf("instance-%03d", i)),
			},
			Status: api.MultiSitePostgresStatus{ActiveRevision: 7},
		}
		instance.Status.Sites = make([]api.SiteRevisionStatus, 0, 100)
		for site := range 100 {
			instance.Status.Sites = append(instance.Status.Sites, api.SiteRevisionStatus{
				Name: fmt.Sprintf("site-%03d", site), DesiredRevision: 7,
				AppliedRevision: 7, Phase: "Ready",
			})
		}
		objects = append(objects, instance)
	}
	statusUpdates := 0
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}).
		WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, underlying client.Client, subresource string,
			object client.Object, options ...client.SubResourceUpdateOption,
		) error {
			statusUpdates++
			return underlying.Status().Update(ctx, object, options...)
		},
	}).Build()
	server := &Server{Client: kube}
	b.ResetTimer()
	for range b.N {
		if err := server.updateInstanceSite(context.Background(), string(targetUID), "site-099",
			func(_ *api.SiteRevisionStatus) {}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if statusUpdates != 0 {
		b.Fatalf("unchanged fleet benchmark wrote status %d times", statusUpdates)
	}
}

func TestAggregateConditionsExcludesWitnessFromPatroni(t *testing.T) {
	healthy := func(conditionTypes ...string) []metav1.Condition {
		conditions := make([]metav1.Condition, 0, len(conditionTypes))
		for _, conditionType := range conditionTypes {
			conditions = append(conditions, metav1.Condition{
				Type: conditionType, Status: metav1.ConditionTrue, Reason: "Healthy",
			})
		}
		return conditions
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Generation: 4},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", Role: api.SiteRoleData},
			{Name: "nsw", Role: api.SiteRoleWitness},
		}},
		Status: api.MultiSitePostgresStatus{Sites: []api.SiteRevisionStatus{
			{Name: "vic", Conditions: healthy(
				"LoadBalancersAllocated", "CertificatesReady", "EtcdQuorate", "PatroniReady",
			)},
			{Name: "nsw", Conditions: healthy(
				"LoadBalancersAllocated", "CertificatesReady", "EtcdQuorate",
			)},
		}},
	}
	aggregateInstanceConditions(instance)
	patroni := meta.FindStatusCondition(instance.Status.Conditions, "PatroniReady")
	if patroni == nil || patroni.Status != metav1.ConditionTrue || patroni.ObservedGeneration != 4 {
		t.Fatalf("PatroniReady = %#v", patroni)
	}
}

func TestSQLDirectivesTargetObservedPrimarySite(t *testing.T) {
	instance := &api.MultiSitePostgres{
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", SiteRegistrationRef: "production-vic"},
			{Name: "qld", SiteRegistrationRef: "production-qld"},
		}},
		Status: api.MultiSitePostgresStatus{
			Primary: "postgres-qld-0",
			Sites: []api.SiteRevisionStatus{
				{Name: "vic", Addresses: map[string]string{"postgres-vic-0": "10.0.0.1"}},
				{Name: "qld", Addresses: map[string]string{"postgres-qld-0": "10.0.1.1"}},
			},
		},
	}
	if directiveTargetsSite(instance, "Database", "production-vic") {
		t.Fatal("database directive targeted a non-primary site")
	}
	if !directiveTargetsSite(instance, "User", "production-qld") {
		t.Fatal("user directive did not target the primary site")
	}
	instance.Status.SynchronousStandbys = []string{"postgres-vic-0"}
	if source := selectBackupSource(instance); source != "postgres-vic-0" {
		t.Fatalf("backup source = %q", source)
	}
}

func TestBackupTLSRequiresCommonTrustBundle(t *testing.T) {
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Spec: api.MultiSitePostgresSpec{
			Backup: &api.BackupSpec{},
			Sites: []api.PostgresSiteSpec{
				{Name: "vic", Role: api.SiteRoleData},
				{Name: "qld", Role: api.SiteRoleData},
			},
		},
		Status: api.MultiSitePostgresStatus{Sites: []api.SiteRevisionStatus{
			{Name: "vic", Conditions: []metav1.Condition{{
				Type: "BackupTLSReady", Status: metav1.ConditionTrue, Message: "ca-a",
			}}},
			{Name: "qld", Conditions: []metav1.Condition{{
				Type: "BackupTLSReady", Status: metav1.ConditionTrue, Message: "ca-b",
			}}},
		}},
	}
	aggregateInstanceConditions(instance)
	condition := meta.FindStatusCondition(instance.Status.Conditions, "BackupTLSReady")
	if condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != "TrustBundleMismatch" {
		t.Fatalf("BackupTLSReady = %#v", condition)
	}
	instance.Status.Sites[1].Conditions[0].Message = "ca-a"
	aggregateInstanceConditions(instance)
	condition = meta.FindStatusCondition(instance.Status.Conditions, "BackupTLSReady")
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("BackupTLSReady = %#v", condition)
	}
}

func TestEtcdTLSRequiresCommonTrustBundleAcrossWitnesses(t *testing.T) {
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Generation: 3},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", Role: api.SiteRoleData},
			{Name: "qld", Role: api.SiteRoleWitness},
		}},
		Status: api.MultiSitePostgresStatus{Sites: []api.SiteRevisionStatus{
			{Name: "vic", Conditions: []metav1.Condition{{
				Type: "EtcdTLSReady", Status: metav1.ConditionTrue, Message: "ca-a",
			}}},
			{Name: "qld", Conditions: []metav1.Condition{{
				Type: "EtcdTLSReady", Status: metav1.ConditionTrue, Message: "ca-b",
			}}},
		}},
	}
	aggregateInstanceConditions(instance)
	condition := meta.FindStatusCondition(instance.Status.Conditions, "EtcdTLSReady")
	if condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != "TrustBundleMismatch" || condition.ObservedGeneration != 3 {
		t.Fatalf("EtcdTLSReady = %#v", condition)
	}
	instance.Status.Sites[1].Conditions[0].Message = "ca-a"
	aggregateInstanceConditions(instance)
	condition = meta.FindStatusCondition(instance.Status.Conditions, "EtcdTLSReady")
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("EtcdTLSReady = %#v", condition)
	}
}
