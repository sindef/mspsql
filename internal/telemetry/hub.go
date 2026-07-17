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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

type HubCollector struct {
	client client.Reader
	now    func() time.Time
	desc   map[string]*prometheus.Desc
}

func NewHubCollector(kube client.Reader) *HubCollector {
	labels := []string{"namespace", "instance"}
	return &HubCollector{
		client: kube,
		now:    time.Now,
		desc: map[string]*prometheus.Desc{
			"agent_connected": prometheus.NewDesc("mspsql_agent_connected",
				"Whether the registered site agent is connected.", []string{"site"}, nil),
			"agent_heartbeat_age": prometheus.NewDesc("mspsql_agent_heartbeat_age_seconds",
				"Seconds since the hub received the site's last heartbeat.", []string{"site"}, nil),
			"agent_certificate_expiry": prometheus.NewDesc("mspsql_agent_certificate_expiry_timestamp_seconds",
				"Unix timestamp at which the site agent identity certificate expires.", []string{"site"}, nil),
			"plan_revision_lag": prometheus.NewDesc("mspsql_plan_revision_lag",
				"Desired minus applied signed-plan revision.", append(labels, "site"), nil),
			"etcd_quorate": prometheus.NewDesc("mspsql_etcd_quorate",
				"Whether every site reports a healthy etcd quorum.", labels, nil),
			"patroni_ready": prometheus.NewDesc("mspsql_patroni_ready",
				"Whether Patroni topology is ready.", labels, nil),
			"synchronous_write": prometheus.NewDesc("mspsql_synchronous_write_available",
				"Whether the required synchronous standby count is available.", labels, nil),
			"synchronous_standbys": prometheus.NewDesc("mspsql_synchronous_standbys",
				"Number of synchronous PostgreSQL standbys observed by the hub.", labels, nil),
			"primary": prometheus.NewDesc("mspsql_patroni_primary",
				"Current Patroni primary member (value is always one).", append(labels, "member"), nil),
			"tde_verified": prometheus.NewDesc("mspsql_tde_verified",
				"Whether the most recent pg_tde acceptance audit succeeded.", labels, nil),
			"address_migration": prometheus.NewDesc("mspsql_load_balancer_address_change_blocked",
				"Whether a serialized LoadBalancer address migration is blocked.", labels, nil),
			"backup_age": prometheus.NewDesc("mspsql_backup_age_seconds",
				"Seconds since the most recent verified backup.", labels, nil),
			"recovery_window": prometheus.NewDesc("mspsql_recovery_window_seconds",
				"Seconds covered by the verified backup recovery window.", labels, nil),
			"database_size": prometheus.NewDesc("mspsql_database_size_bytes",
				"Observed PostgreSQL database size.", []string{"namespace", "database", "instance"}, nil),
			"database_threshold": prometheus.NewDesc("mspsql_database_storage_threshold_bytes",
				"Configured database storage alert threshold.", []string{"namespace", "database", "instance"}, nil),
			"upgrade_phase": prometheus.NewDesc("mspsql_upgrade_phase",
				"Current upgrade phase (value is always one).", []string{"namespace", "upgrade", "instance", "phase"}, nil),
			"upgrade_outage": prometheus.NewDesc("mspsql_upgrade_write_outage_seconds",
				"Observed or current write-service outage duration.", []string{"namespace", "upgrade", "instance"}, nil),
		},
	}
}

func (c *HubCollector) Describe(output chan<- *prometheus.Desc) {
	for _, description := range c.desc {
		output <- description
	}
}

func (c *HubCollector) Collect(output chan<- prometheus.Metric) {
	now := c.now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.collectSites(ctx, now, output); err != nil {
		output <- prometheus.NewInvalidMetric(c.desc["agent_connected"], err)
	}
	if err := c.collectInstances(ctx, now, output); err != nil {
		output <- prometheus.NewInvalidMetric(c.desc["plan_revision_lag"], err)
	}
	if err := c.collectDatabases(ctx, output); err != nil {
		output <- prometheus.NewInvalidMetric(c.desc["database_size"], err)
	}
	if err := c.collectUpgrades(ctx, now, output); err != nil {
		output <- prometheus.NewInvalidMetric(c.desc["upgrade_phase"], err)
	}
}

func (c *HubCollector) collectSites(ctx context.Context, now time.Time,
	output chan<- prometheus.Metric,
) error {
	var sites api.SiteRegistrationList
	if err := c.client.List(ctx, &sites); err != nil {
		return err
	}
	for i := range sites.Items {
		site := &sites.Items[i]
		output <- gauge(c.desc["agent_connected"], boolFloat(site.Status.Phase == "Connected"), site.Name)
		if site.Status.LastHeartbeatTime != nil {
			output <- gauge(c.desc["agent_heartbeat_age"],
				max(0, now.Sub(site.Status.LastHeartbeatTime.Time).Seconds()), site.Name)
		}
		if site.Status.AgentCertificateExpiresAt != nil {
			output <- gauge(c.desc["agent_certificate_expiry"],
				float64(site.Status.AgentCertificateExpiresAt.Unix()), site.Name)
		}
	}
	return nil
}

func (c *HubCollector) collectInstances(ctx context.Context, now time.Time,
	output chan<- prometheus.Metric,
) error {
	var instances api.MultiSitePostgresList
	if err := c.client.List(ctx, &instances); err != nil {
		return err
	}
	for i := range instances.Items {
		instance := &instances.Items[i]
		labels := []string{instance.Namespace, instance.Name}
		for _, site := range instance.Status.Sites {
			output <- gauge(c.desc["plan_revision_lag"],
				float64(max(0, site.DesiredRevision-site.AppliedRevision)),
				instance.Namespace, instance.Name, site.Name)
		}
		output <- gauge(c.desc["etcd_quorate"], conditionValue(instance.Status.Conditions, "EtcdQuorate"), labels...)
		output <- gauge(c.desc["patroni_ready"], conditionValue(instance.Status.Conditions, "PatroniReady"), labels...)
		output <- gauge(c.desc["synchronous_write"],
			conditionValue(instance.Status.Conditions, "SynchronousReplicationReady"), labels...)
		output <- gauge(c.desc["synchronous_standbys"], float64(len(instance.Status.SynchronousStandbys)), labels...)
		if instance.Status.Primary != "" {
			output <- gauge(c.desc["primary"], 1, instance.Namespace, instance.Name, instance.Status.Primary)
		}
		output <- gauge(c.desc["tde_verified"], conditionValue(instance.Status.Conditions, "TDEVerified"), labels...)
		output <- gauge(c.desc["address_migration"],
			conditionValue(instance.Status.Conditions, "AddressChangeBlocked"), labels...)
		if instance.Status.LastBackupTime != nil {
			output <- gauge(c.desc["backup_age"],
				max(0, now.Sub(instance.Status.LastBackupTime.Time).Seconds()), labels...)
		}
		if instance.Status.RecoveryWindowStart != nil {
			output <- gauge(c.desc["recovery_window"],
				max(0, now.Sub(instance.Status.RecoveryWindowStart.Time).Seconds()), labels...)
		}
	}
	return nil
}

func (c *HubCollector) collectDatabases(ctx context.Context, output chan<- prometheus.Metric) error {
	var databases api.PostgresDatabaseList
	if err := c.client.List(ctx, &databases); err != nil {
		return err
	}
	for i := range databases.Items {
		database := &databases.Items[i]
		labels := []string{database.Namespace, database.Name, database.Spec.InstanceRef}
		output <- gauge(c.desc["database_size"], database.Status.ObservedSize.AsApproximateFloat64(), labels...)
		if database.Spec.Quotas.StorageAlertThreshold != nil {
			output <- gauge(c.desc["database_threshold"],
				database.Spec.Quotas.StorageAlertThreshold.AsApproximateFloat64(), labels...)
		}
	}
	return nil
}

func (c *HubCollector) collectUpgrades(ctx context.Context, now time.Time,
	output chan<- prometheus.Metric,
) error {
	var upgrades api.PostgresUpgradeList
	if err := c.client.List(ctx, &upgrades); err != nil {
		return err
	}
	for i := range upgrades.Items {
		upgrade := &upgrades.Items[i]
		phase := upgrade.Status.Phase
		if phase == "" {
			phase = "Pending"
		}
		output <- gauge(c.desc["upgrade_phase"], 1,
			upgrade.Namespace, upgrade.Name, upgrade.Spec.InstanceRef, phase)
		if upgrade.Status.WriteOutageStartedAt != nil {
			ended := now
			if upgrade.Status.WriteServiceRestoredAt != nil {
				ended = upgrade.Status.WriteServiceRestoredAt.Time
			}
			output <- gauge(c.desc["upgrade_outage"],
				max(0, ended.Sub(upgrade.Status.WriteOutageStartedAt.Time).Seconds()),
				upgrade.Namespace, upgrade.Name, upgrade.Spec.InstanceRef)
		}
	}
	return nil
}

func gauge(description *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(description, prometheus.GaugeValue, value, labels...)
}

func conditionValue(conditions []metav1.Condition, conditionType string) float64 {
	condition := meta.FindStatusCondition(conditions, conditionType)
	return boolFloat(condition != nil && condition.Status == metav1.ConditionTrue)
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
