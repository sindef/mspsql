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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
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

func TestDirectiveResultRecordsDeclarationOperation(t *testing.T) {
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
	controller := true
	directive := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: database.Namespace, Name: "mspsql-database-" + database.Name,
			Labels: map[string]string{"multisite-postgres.dev/directive": "Database"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: api.GroupVersion.String(), Kind: "PostgresDatabase",
				Name: database.Name, UID: database.UID, Controller: &controller,
			}},
		},
		Data: map[string]string{
			"type": "Database", "instanceRef": "orders", "deleting": "false",
			"operationUID": "database-uid-2-false",
		},
	}
	server := &Server{Client: fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresDatabase{}).
		WithObjects(database, directive).Build()}
	err := server.recordResult(context.Background(), "vic", &controlv1.PlanResult{
		OperationUid: "database-uid-2-false", InstanceUid: "instance",
		Conditions: []*controlv1.Condition{{
			Type: "Succeeded", Status: string(metav1.ConditionFalse),
			Reason: "SQLFailed", Message: "permission denied",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var current api.PostgresDatabase
	if err := server.Client.Get(context.Background(), client.ObjectKeyFromObject(database), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Operation == nil ||
		current.Status.Operation.OperationUID != "database-uid-2-false" ||
		current.Status.Operation.Phase != "Failed" ||
		current.Status.Operation.Site != "vic" ||
		!current.Status.Operation.Terminal ||
		current.Status.Operation.LastErrorReason != "SQLFailed" ||
		current.Status.Operation.LastErrorMessage != "permission denied" {
		t.Fatalf("database operation = %#v", current.Status.Operation)
	}
}

func TestDirectiveResultRecordsScheduledBackupOperation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance-uid"), Generation: 4,
		},
		Status: api.MultiSitePostgresStatus{BackupSchedules: []api.BackupScheduleStatus{{
			Type: "full",
			Operation: &api.OperationProgressStatus{
				OperationUID: "instance-uid-backup-full-1785844800",
				Phase:        "ScheduledBackup",
			},
		}}},
	}
	controller := true
	directive := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: instance.Namespace, Name: "mspsql-backup-full-1785844800",
			Labels: map[string]string{"multisite-postgres.dev/directive": "Backup"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: api.GroupVersion.String(), Kind: "MultiSitePostgres",
				Name: instance.Name, UID: instance.UID, Controller: &controller,
			}},
		},
		Data: map[string]string{
			"type": "Backup", "instanceRef": "orders", "deleting": "false",
			"operationUID": "instance-uid-backup-full-1785844800",
		},
	}
	server := &Server{Client: fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}).
		WithObjects(instance, directive).Build()}
	err := server.recordResult(context.Background(), "vic", &controlv1.PlanResult{
		OperationUid: "instance-uid-backup-full-1785844800", InstanceUid: string(instance.UID),
		Conditions: []*controlv1.Condition{{
			Type: "Succeeded", Status: string(metav1.ConditionTrue),
			Reason: "BackupCompleted", Message: "ok",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var current api.MultiSitePostgres
	if err := server.Client.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	operation := current.Status.BackupSchedules[0].Operation
	if operation == nil ||
		operation.OperationUID != "instance-uid-backup-full-1785844800" ||
		operation.Phase != "ScheduledBackup" ||
		operation.Site != "vic" ||
		!operation.Terminal ||
		operation.LastErrorReason != "" {
		t.Fatalf("backup schedule operation = %#v", operation)
	}
}

func TestSendDirectivesAppliesBackpressureLimit(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance-uid"),
		},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{{
			Name: "vic", SiteRegistrationRef: "production-vic", Role: api.SiteRoleData,
		}}},
		Status: api.MultiSitePostgresStatus{
			Primary: "postgres-vic-0",
			Sites: []api.SiteRevisionStatus{{
				Name: "vic", Addresses: map[string]string{"postgres-vic-0": "10.0.0.10"},
			}},
		},
	}
	objects := []client.Object{&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mspsql-system", Name: "mspsql-plan-signing-key"},
		Data: map[string][]byte{
			"privateKey": []byte(base64.RawStdEncoding.EncodeToString(privateKey)),
		},
	}}
	controller := true
	baseTime := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	for i := range 3 {
		scheduledAt := baseTime.Add(time.Duration(i) * time.Minute)
		instance.Status.BackupSchedules = append(instance.Status.BackupSchedules,
			api.BackupScheduleStatus{
				Type:            "full",
				LastScheduledAt: &metav1.Time{Time: scheduledAt},
			})
		operationUID := fmt.Sprintf("%s-backup-full-%d", instance.UID, scheduledAt.Unix())
		objects = append(objects, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "platform",
				Name:      fmt.Sprintf("mspsql-backup-full-%d", scheduledAt.Unix()),
				Labels: map[string]string{
					"multisite-postgres.dev/directive": "Backup",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: api.GroupVersion.String(), Kind: "MultiSitePostgres",
					Name: instance.Name, UID: instance.UID, Controller: &controller,
				}},
			},
			Data: map[string]string{
				"type": "Backup", "instanceRef": instance.Name, "deleting": "false",
				"operationUID": operationUID,
				"spec.json": fmt.Sprintf(`{"backupType":"full","scheduledAt":%q}`,
					scheduledAt.UTC().Format(time.RFC3339)),
			},
		})
	}
	objects = append(objects, instance)
	server := &Server{
		Client:               fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		MaxDirectivesPerSync: 2,
	}
	sent := map[string]struct{}{}
	firstStream := &countingHubStream{}
	if err := server.sendDirectives(context.Background(), firstStream, "production-vic", "site-uid", sent); err != nil {
		t.Fatal(err)
	}
	if firstStream.sent != 2 || len(sent) != 2 {
		t.Fatalf("first sync sent=%d tracked=%d", firstStream.sent, len(sent))
	}
	secondStream := &countingHubStream{}
	if err := server.sendDirectives(context.Background(), secondStream, "production-vic", "site-uid", sent); err != nil {
		t.Fatal(err)
	}
	if secondStream.sent != 1 || len(sent) != 3 {
		t.Fatalf("second sync sent=%d tracked=%d", secondStream.sent, len(sent))
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
		WithIndex(&api.MultiSitePostgres{}, InstanceUIDField, func(object client.Object) []string {
			return []string{string(object.GetUID())}
		}).
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
		WithIndex(&api.MultiSitePostgres{}, InstanceUIDField, func(object client.Object) []string {
			return []string{string(object.GetUID())}
		}).
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

func BenchmarkSendDirectivesAtFleetScale(b *testing.B) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		b.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		b.Fatal(err)
	}
	const (
		siteCount          = 100
		instanceCount      = 1000
		directivesPerInst  = 10
		expectedDirectives = instanceCount * directivesPerInst
	)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	objects := make([]client.Object, 0, 1+(directivesPerInst+1)*instanceCount)
	objects = append(objects, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mspsql-system", Name: "mspsql-plan-signing-key"},
		Data: map[string][]byte{
			"privateKey": []byte(base64.RawStdEncoding.EncodeToString(privateKey)),
		},
	})
	for i := range instanceCount {
		siteIndex := i % siteCount
		siteName := fmt.Sprintf("site-%03d", siteIndex)
		namespace := fmt.Sprintf("platform-%04d", i)
		instanceName := fmt.Sprintf("orders-%04d", i)
		instanceUID := types.UID(fmt.Sprintf("instance-%04d", i))
		instance := &api.MultiSitePostgres{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: instanceName, UID: instanceUID,
			},
			Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{{
				Name: siteName, SiteRegistrationRef: siteName, Role: api.SiteRoleData,
			}}},
			Status: api.MultiSitePostgresStatus{
				Primary: fmt.Sprintf("postgres-%s-0", siteName),
				Sites: []api.SiteRevisionStatus{{
					Name: siteName,
					Addresses: map[string]string{
						fmt.Sprintf("postgres-%s-0", siteName): fmt.Sprintf("10.%d.%d.%d",
							siteIndex/256, siteIndex%256, i%250+1),
					},
				}},
			},
		}
		controller := true
		baseTime := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
		for directiveIndex := range directivesPerInst {
			scheduledAt := baseTime.Add(time.Duration(directiveIndex) * time.Minute)
			instance.Status.BackupSchedules = append(instance.Status.BackupSchedules,
				api.BackupScheduleStatus{
					Type:            "full",
					LastScheduledAt: &metav1.Time{Time: scheduledAt},
				})
			operationUID := fmt.Sprintf("%s-backup-full-%d", instanceUID, scheduledAt.Unix())
			spec := fmt.Sprintf(`{"backupType":"full","scheduledAt":%q}`,
				scheduledAt.UTC().Format(time.RFC3339))
			objects = append(objects, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      fmt.Sprintf("mspsql-backup-full-%d", scheduledAt.Unix()),
					Labels: map[string]string{
						"multisite-postgres.dev/directive": "Backup",
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: api.GroupVersion.String(), Kind: "MultiSitePostgres",
						Name: instanceName, UID: instanceUID, Controller: &controller,
					}},
				},
				Data: map[string]string{
					"type": "Backup", "instanceRef": instanceName, "deleting": "false",
					"operationUID": operationUID, "spec.json": spec,
				},
			})
		}
		objects = append(objects, instance)
	}
	server := &Server{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
	perDirectiveLatency := make([]float64, 0, b.N)
	b.ResetTimer()
	for range b.N {
		stream := &countingHubStream{}
		start := time.Now()
		if err := server.sendDirectives(context.Background(), stream, "site-000", "site-uid", map[string]struct{}{}); err != nil {
			b.Fatal(err)
		}
		elapsed := time.Since(start)
		if stream.sent == 0 || stream.sent > expectedDirectives {
			b.Fatalf("sent directives = %d", stream.sent)
		}
		perDirectiveLatency = append(perDirectiveLatency, float64(elapsed.Nanoseconds())/float64(stream.sent))
		b.ReportMetric(float64(stream.sent), "directives_sent")
		b.ReportMetric(expectedDirectives, "directives_queued")
	}
	b.StopTimer()
	sort.Float64s(perDirectiveLatency)
	p99Index := int(float64(len(perDirectiveLatency))*0.99) - 1
	if p99Index < 0 {
		p99Index = 0
	}
	b.ReportMetric(perDirectiveLatency[p99Index], "p99_ns_per_directive")
}

type countingHubStream struct {
	grpc.ServerStream
	sent int
}

func (s *countingHubStream) Send(message *controlv1.HubMessage) error {
	if message.GetDirective() != nil {
		s.sent++
	}
	return nil
}

func (s *countingHubStream) Recv() (*controlv1.AgentMessage, error) {
	return nil, io.EOF
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
