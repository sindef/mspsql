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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/sindef/mspsql/internal/agent"
)

type AgentMetrics struct {
	registry       *prometheus.Registry
	connected      prometheus.Gauge
	certificate    prometheus.Gauge
	cacheAge       *prometheus.GaugeVec
	revisionLag    *prometheus.GaugeVec
	reconcileTime  *prometheus.HistogramVec
	reconcileError *prometheus.CounterVec
	backoff        prometheus.Gauge
	etcdQuorate    *prometheus.GaugeVec
	patroniReady   *prometheus.GaugeVec
	synchronous    *prometheus.GaugeVec
	tdeVerified    *prometheus.GaugeVec
	primary        *prometheus.GaugeVec
	addressChanges *prometheus.CounterVec
	mu             sync.Mutex
	primaries      map[string]string
	addresses      map[string]map[string]string
}

func NewAgentMetrics(registrationUID string) *AgentMetrics {
	constant := prometheus.Labels{"registration_uid": registrationUID}
	m := &AgentMetrics{
		registry: prometheus.NewRegistry(),
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mspsql_agent_connected", Help: "Whether this agent has an authenticated hub stream.",
			ConstLabels: constant,
		}),
		certificate: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mspsql_agent_certificate_expiry_timestamp_seconds",
			Help: "Unix timestamp at which the mounted agent identity certificate expires.", ConstLabels: constant,
		}),
		cacheAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mspsql_agent_cache_age_seconds", Help: "Age of the last authenticated cached plan.",
			ConstLabels: constant,
		}, []string{"instance_uid"}),
		revisionLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mspsql_agent_plan_revision_lag", Help: "Whether a cached plan is not yet fully applied.",
			ConstLabels: constant,
		}, []string{"instance_uid"}),
		reconcileTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mspsql_agent_reconcile_duration_seconds", Help: "Agent plan reconciliation duration.",
			ConstLabels: constant, Buckets: prometheus.DefBuckets,
		}, []string{"instance_uid"}),
		reconcileError: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mspsql_agent_reconcile_errors_total", Help: "Agent plan reconciliation errors.",
			ConstLabels: constant,
		}, []string{"instance_uid"}),
		backoff: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mspsql_agent_connection_backoff_seconds", Help: "Current hub reconnection backoff.",
			ConstLabels: constant,
		}),
		etcdQuorate:  conditionGauge("mspsql_agent_etcd_quorate", "Whether local etcd health checks passed.", constant),
		patroniReady: conditionGauge("mspsql_agent_patroni_ready", "Whether local Patroni checks passed.", constant),
		synchronous: conditionGauge("mspsql_agent_synchronous_write_available",
			"Whether local synchronous write acceptance passed.", constant),
		tdeVerified: conditionGauge("mspsql_agent_tde_verified", "Whether the local pg_tde audit passed.", constant),
		primary: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mspsql_agent_patroni_primary", Help: "Locally observed Patroni primary (value is always one).",
			ConstLabels: constant,
		}, []string{"instance_uid", "member"}),
		addressChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mspsql_agent_load_balancer_address_changes_total",
			Help: "Observed changes to managed LoadBalancer ingress addresses.", ConstLabels: constant,
		}, []string{"instance_uid", "member"}),
		primaries: map[string]string{},
		addresses: map[string]map[string]string{},
	}
	m.registry.MustRegister(m.connected, m.certificate, m.cacheAge, m.revisionLag,
		m.reconcileTime, m.reconcileError, m.backoff, m.etcdQuorate, m.patroniReady,
		m.synchronous, m.tdeVerified, m.primary, m.addressChanges)
	return m
}

func conditionGauge(name, help string, labels prometheus.Labels) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name, Help: help, ConstLabels: labels,
	}, []string{"instance_uid"})
}

func (m *AgentMetrics) Registry() *prometheus.Registry { return m.registry }

func (m *AgentMetrics) SetConnected(connected bool) {
	m.connected.Set(boolFloat(connected))
	if connected {
		m.backoff.Set(0)
	}
}

func (m *AgentMetrics) SetCertificateExpiry(expiry time.Time) {
	m.certificate.Set(float64(expiry.Unix()))
}

func (m *AgentMetrics) SetBackoff(delay time.Duration) { m.backoff.Set(delay.Seconds()) }

func (m *AgentMetrics) SetCacheAge(instanceUID string, age time.Duration) {
	m.cacheAge.WithLabelValues(instanceUID).Set(max(0, age.Seconds()))
}

func (m *AgentMetrics) ObserveReconcile(instanceUID string, duration time.Duration,
	result agent.ApplyResult, err error,
) {
	m.reconcileTime.WithLabelValues(instanceUID).Observe(duration.Seconds())
	if err != nil {
		m.reconcileError.WithLabelValues(instanceUID).Inc()
	}
	m.revisionLag.WithLabelValues(instanceUID).Set(boolFloat(result.Phase != "Ready" && result.Phase != "Deleted"))
	m.etcdQuorate.WithLabelValues(instanceUID).Set(localCondition(result.Conditions, "EtcdQuorate"))
	m.patroniReady.WithLabelValues(instanceUID).Set(localCondition(result.Conditions, "PatroniReady"))
	m.synchronous.WithLabelValues(instanceUID).Set(localCondition(result.Conditions, "SynchronousReplicationReady"))
	m.tdeVerified.WithLabelValues(instanceUID).Set(localCondition(result.Conditions, "TDEVerified"))
	m.observePrimary(instanceUID, result.Primary)
	m.observeAddresses(instanceUID, result.Addresses)
}

func (m *AgentMetrics) observePrimary(instanceUID, primary string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous := m.primaries[instanceUID]; previous != "" && previous != primary {
		m.primary.DeleteLabelValues(instanceUID, previous)
	}
	if primary != "" {
		m.primary.WithLabelValues(instanceUID, primary).Set(1)
		m.primaries[instanceUID] = primary
	}
}

func (m *AgentMetrics) observeAddresses(instanceUID string, addresses map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := m.addresses[instanceUID]
	if previous == nil {
		previous = map[string]string{}
	}
	for member, address := range addresses {
		if old := previous[member]; old != "" && old != address {
			m.addressChanges.WithLabelValues(instanceUID, member).Inc()
		}
		previous[member] = address
	}
	m.addresses[instanceUID] = previous
}

func localCondition(conditions []metav1.Condition, conditionType string) float64 {
	condition := meta.FindStatusCondition(conditions, conditionType)
	return boolFloat(condition != nil && condition.Status == metav1.ConditionTrue)
}
