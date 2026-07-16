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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
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
