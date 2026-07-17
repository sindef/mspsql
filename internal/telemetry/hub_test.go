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

package telemetry

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/agent"
)

func TestHubCollectorPublishesObservedState(t *testing.T) {
	now := time.Date(2026, 7, 17, 5, 0, 0, 0, time.UTC)
	heartbeat := metav1.NewTime(now.Add(-30 * time.Second))
	backup := metav1.NewTime(now.Add(-time.Hour))
	recovery := metav1.NewTime(now.Add(-24 * time.Hour))
	threshold := resource.MustParse("10Gi")
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&api.SiteRegistration{
			ObjectMeta: metav1.ObjectMeta{Name: "vic"},
			Status:     api.SiteRegistrationStatus{Phase: "Connected", LastHeartbeatTime: &heartbeat},
		},
		&api.MultiSitePostgres{
			ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
			Status: api.MultiSitePostgresStatus{
				Primary: "postgres-vic-0", SynchronousStandbys: []string{"postgres-nsw-0"},
				LastBackupTime: &backup, RecoveryWindowStart: &recovery,
				Sites: []api.SiteRevisionStatus{{Name: "vic", DesiredRevision: 8, AppliedRevision: 6}},
				Conditions: []metav1.Condition{
					{Type: "EtcdQuorate", Status: metav1.ConditionTrue},
					{Type: "PatroniReady", Status: metav1.ConditionTrue},
					{Type: "SynchronousReplicationReady", Status: metav1.ConditionTrue},
				},
			},
		},
		&api.PostgresDatabase{
			ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-api"},
			Spec: api.PostgresDatabaseSpec{InstanceRef: "orders", Quotas: api.DatabaseQuotas{
				StorageAlertThreshold: &threshold,
			}},
			Status: api.PostgresDatabaseStatus{ObservedSize: resource.MustParse("2Gi")},
		},
	).Build()
	collector := NewHubCollector(kube)
	collector.now = func() time.Time { return now }
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	assertMetric(t, families, "mspsql_agent_connected", 1, map[string]string{"site": "vic"})
	assertMetric(t, families, "mspsql_agent_heartbeat_age_seconds", 30, map[string]string{"site": "vic"})
	assertMetric(t, families, "mspsql_plan_revision_lag", 2,
		map[string]string{"namespace": "platform", "instance": "orders", "site": "vic"})
	assertMetric(t, families, "mspsql_backup_age_seconds", 3600,
		map[string]string{"namespace": "platform", "instance": "orders"})
	assertMetric(t, families, "mspsql_database_size_bytes", 2*1024*1024*1024,
		map[string]string{"namespace": "platform", "database": "orders-api", "instance": "orders"})
}

func TestAgentMetricsPublishFailuresAndObservedHealth(t *testing.T) {
	metrics := NewAgentMetrics("site-uid")
	metrics.SetConnected(true)
	metrics.SetCacheAge("instance-uid", 45*time.Second)
	metrics.ObserveReconcile("instance-uid", 2*time.Second, agent.ApplyResult{
		Phase: "Ready", Primary: "postgres-vic-0",
		Addresses: map[string]string{"postgres-vic-0": "10.0.0.10"},
		Conditions: []metav1.Condition{
			{Type: "EtcdQuorate", Status: metav1.ConditionTrue},
			{Type: "PatroniReady", Status: metav1.ConditionTrue},
		},
	}, nil)
	metrics.ObserveReconcile("instance-uid", time.Second, agent.ApplyResult{
		Phase: "Retrying", Primary: "postgres-nsw-0",
		Addresses: map[string]string{"postgres-vic-0": "10.0.0.11"},
	}, context.DeadlineExceeded)
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"registration_uid": "site-uid"}
	assertMetric(t, families, "mspsql_agent_connected", 1, labels)
	labels["instance_uid"] = "instance-uid"
	assertMetric(t, families, "mspsql_agent_cache_age_seconds", 45, labels)
	assertMetric(t, families, "mspsql_agent_plan_revision_lag", 1, labels)
	assertMetric(t, families, "mspsql_agent_reconcile_errors_total", 1, labels)
	addressLabels := maps.Clone(labels)
	addressLabels["member"] = "postgres-vic-0"
	assertMetric(t, families, "mspsql_agent_load_balancer_address_changes_total", 1, addressLabels)
	primaryLabels := maps.Clone(labels)
	primaryLabels["member"] = "postgres-nsw-0"
	assertMetric(t, families, "mspsql_agent_patroni_primary", 1, primaryLabels)
}

func assertMetric(t *testing.T, families []*dto.MetricFamily, name string, expected float64,
	expectedLabels map[string]string,
) {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, pair := range metric.Label {
				labels[pair.GetName()] = pair.GetValue()
			}
			matches := true
			for key, value := range expectedLabels {
				matches = matches && labels[key] == value
			}
			value := metric.GetGauge().GetValue()
			if metric.Counter != nil {
				value = metric.GetCounter().GetValue()
			}
			if matches && value == expected {
				return
			}
		}
	}
	t.Fatalf("metric %s%v = %v not found", name, expectedLabels, expected)
}
