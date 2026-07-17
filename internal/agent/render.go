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

package agent

import (
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

type Images struct {
	Etcd   string
	Pgpool string
}

type Renderer struct {
	HubDomain string
	Images    Images
}

func (r Renderer) ServiceAccount(desired plan.SitePlan) client.Object {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Namespace: desired.Site.Namespace,
		Name:      workloadServiceAccount,
		Labels:    resourceLabels(desired),
	}}
}

func (r Renderer) LoadBalancers(desired plan.SitePlan) []client.Object {
	labels := resourceLabels(desired)
	objects := make([]client.Object, 0,
		desired.Site.Components.EtcdReplicas+desired.Site.Components.PostgresReplicas+1)
	for ordinal := int32(0); ordinal < desired.Site.Components.EtcdReplicas; ordinal++ {
		name := fmt.Sprintf("etcd-%s-%d", desired.Site.Name, ordinal)
		objects = append(objects, memberService(desired.Site.Namespace, name, name+"-0", labels,
			desired.Site.LoadBalancer, []corev1.ServicePort{
				{Name: "client", Port: 2379, TargetPort: intstr.FromInt32(2379)},
				{Name: "peer", Port: 2380, TargetPort: intstr.FromInt32(2380)},
			}))
	}
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		name := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		ports := []corev1.ServicePort{
			{Name: "postgres", Port: 5432, TargetPort: intstr.FromInt32(5432)},
			{Name: "patroni", Port: 8008, TargetPort: intstr.FromInt32(8008)},
		}
		if desired.Backup != nil {
			ports = append(ports, corev1.ServicePort{
				Name: "pgbackrest", Port: 8432, TargetPort: intstr.FromInt32(8432),
			})
		}
		objects = append(objects, memberService(desired.Site.Namespace, name, name+"-0", labels,
			desired.Site.LoadBalancer, ports))
	}
	if desired.Site.Components.PgpoolReplicas > 0 {
		name := "pgpool-" + desired.Site.Name
		selector := copyMap(labels)
		selector["multisite-postgres.dev/component"] = "pgpool"
		objects = append(objects, loadBalancerService(desired.Site.Namespace, name, labels, selector,
			desired.Site.LoadBalancer, []corev1.ServicePort{{Name: "postgres", Port: 5432, TargetPort: intstr.FromInt32(5432)}}))
	}
	return objects
}

func (r Renderer) Certificates(desired plan.SitePlan) []client.Object {
	labels := resourceLabels(desired)
	objects := []client.Object{
		clientCertificate(desired.Site.Namespace, "etcd-maintenance-client",
			"etcd-maintenance-client-tls", desired.Site.Certificates.EtcdIssuerRef, labels),
	}
	for ordinal := int32(0); ordinal < desired.Site.Components.EtcdReplicas; ordinal++ {
		name := fmt.Sprintf("etcd-%s-%d", desired.Site.Name, ordinal)
		objects = append(objects, certificate(desired.Site.Namespace, name, name+"-tls",
			desired.Site.Certificates.EtcdIssuerRef, labels, certificateAddresses(desired, name),
			[]string{name, name + "." + desired.Site.Namespace + ".svc"}))
	}
	if desired.Site.Role == api.SiteRoleData {
		objects = append(objects, clientCertificate(desired.Site.Namespace, "patroni-etcd-client",
			"patroni-etcd-client-tls", desired.Site.Certificates.EtcdIssuerRef, labels))
		if desired.Backup != nil {
			objects = append(objects, clientCertificate(desired.Site.Namespace, "pgbackrest-client",
				"pgbackrest-client-tls", desired.Site.Certificates.BackupIssuerRef, labels))
		}
	}
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		name := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		objects = append(objects, certificate(desired.Site.Namespace, name, name+"-tls",
			desired.Site.Certificates.PostgresIssuerRef, labels, certificateAddresses(desired, name),
			[]string{name, name + "." + desired.Site.Namespace + ".svc"}))
		if desired.Backup != nil {
			objects = append(objects, certificate(desired.Site.Namespace, name+"-pgbackrest",
				name+"-pgbackrest-tls", desired.Site.Certificates.BackupIssuerRef, labels,
				certificateAddresses(desired, name),
				[]string{name, name + "." + desired.Site.Namespace + ".svc"}))
		}
	}
	if desired.Site.Components.PgpoolReplicas > 0 {
		name := "pgpool-" + desired.Site.Name
		objects = append(objects, certificate(desired.Site.Namespace, name, name+"-tls",
			desired.Site.Certificates.PgpoolIssuerRef, labels, certificateAddresses(desired, name),
			[]string{name, name + "." + desired.Site.Namespace + ".svc"}))
	}
	return objects
}

func (r Renderer) Workloads(desired plan.SitePlan) ([]client.Object, error) {
	labels := resourceLabels(desired)
	var objects []client.Object
	initialCluster, err := etcdInitialCluster(desired)
	if err != nil {
		return nil, err
	}
	for ordinal := int32(0); ordinal < desired.Site.Components.EtcdReplicas; ordinal++ {
		name := fmt.Sprintf("etcd-%s-%d", desired.Site.Name, ordinal)
		address := desired.MemberAddresses[name]
		if address == "" {
			return nil, fmt.Errorf("address for %s is not allocated", name)
		}
		objects = append(objects, r.etcdStatefulSet(desired, name, address, initialCluster, labels))
	}
	if desired.Site.Role == api.SiteRoleWitness {
		return objects, nil
	}
	if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed &&
		desired.Site.Name != desired.Restore.SeedSite {
		return objects, nil
	}
	workloadPlan := desired
	if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed {
		workloadPlan.Backup = desired.Restore.SourceBackup.DeepCopy()
	}
	patroniConfig := r.patroniConfig(workloadPlan, labels)
	objects = append(objects, patroniConfig)
	if workloadPlan.Backup != nil {
		pgBackRestConfig, configErr := r.pgBackRestConfig(workloadPlan, labels)
		if configErr != nil {
			return nil, configErr
		}
		objects = append(objects, pgBackRestConfig)
	}
	if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed {
		objects = append(objects, r.restoreBootstrapConfig(workloadPlan, labels))
	}
	if desired.TDE.Enabled {
		objects = append(objects, r.tdeBootstrapConfig(desired, labels))
	}
	postgresObjects, err := r.postgresWorkloads(workloadPlan, desired, labels)
	if err != nil {
		return nil, err
	}
	objects = append(objects, postgresObjects...)
	objects = append(objects, r.majorUpgradePhaseJobs(desired)...)
	return objects, nil
}

func (r Renderer) postgresWorkloads(workloadPlan, desired plan.SitePlan,
	labels map[string]string,
) ([]client.Object, error) {
	var objects []client.Object
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		name := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed &&
			name != desired.Restore.SeedMember {
			continue
		}
		address := desired.MemberAddresses[name]
		if address == "" {
			return nil, fmt.Errorf("address for %s is not allocated", name)
		}
		statefulSet := r.postgresStatefulSet(workloadPlan, name, address, labels)
		if majorMemberStopped(desired, name) {
			statefulSet.Spec.Replicas = ptr(int32(0))
		}
		objects = append(objects, statefulSet)
	}
	if desired.Site.Components.PgpoolReplicas > 0 &&
		(desired.Restore == nil || desired.Restore.Phase == plan.RestorePhaseVerify) {
		deployment := r.pgpoolDeployment(desired, labels)
		if majorWriteServiceStopped(desired) {
			deployment.Spec.Replicas = ptr(int32(0))
		}
		objects = append(objects, r.pgpoolConfig(desired, labels), deployment)
	}
	if desired.TDE.Enabled && !majorWriteServiceStopped(desired) {
		objects = append(objects, r.tdeAuditJob(desired, labels))
	}
	return objects, nil
}

func (r Renderer) majorUpgradePhaseJobs(desired plan.SitePlan) []client.Object {
	var objects []client.Object
	if desired.MajorUpgrade != nil &&
		desired.MajorUpgrade.Phase == plan.MajorUpgradePhasePreflight &&
		desired.Site.Role == api.SiteRoleData {
		objects = append(objects, r.MajorPreflightJob(desired))
	}
	if desired.MajorUpgrade != nil &&
		desired.MajorUpgrade.Phase == plan.MajorUpgradePhaseStartPrimary &&
		memberBelongsToSite(desired.MajorUpgrade.Primary, desired.Site.Name) {
		objects = append(objects, r.MajorAcceptanceJob(desired))
	}
	if desired.MajorUpgrade != nil &&
		desired.MajorUpgrade.Phase == plan.MajorUpgradePhaseRollbackStart &&
		memberBelongsToSite(desired.MajorUpgrade.Primary, desired.Site.Name) {
		objects = append(objects, r.MajorRollbackAcceptanceJob(desired))
	}
	return objects
}

func majorMemberStopped(desired plan.SitePlan, member string) bool {
	if desired.MajorUpgrade == nil {
		return false
	}
	switch desired.MajorUpgrade.Phase {
	case plan.MajorUpgradePhaseStop, plan.MajorUpgradePhaseSnapshot,
		plan.MajorUpgradePhaseUpgradePrimary, plan.MajorUpgradePhaseStanzaUpgrade,
		plan.MajorUpgradePhaseRollback:
		return true
	case plan.MajorUpgradePhaseStartPrimary, plan.MajorUpgradePhaseRestoreWrites:
		return member != desired.MajorUpgrade.Primary
	default:
		return false
	}
}

func majorWriteServiceStopped(desired plan.SitePlan) bool {
	if desired.MajorUpgrade == nil {
		return false
	}
	switch desired.MajorUpgrade.Phase {
	case plan.MajorUpgradePhaseDrain, plan.MajorUpgradePhaseStop, plan.MajorUpgradePhaseSnapshot,
		plan.MajorUpgradePhaseUpgradePrimary, plan.MajorUpgradePhaseStanzaUpgrade,
		plan.MajorUpgradePhaseStartPrimary, plan.MajorUpgradePhaseRollback,
		plan.MajorUpgradePhaseRollbackStart:
		return true
	default:
		return false
	}
}

func (r Renderer) AddressMigrationJob(desired plan.SitePlan) (*batchv1.Job, error) {
	migration := desired.AddressMigration
	if migration == nil || !strings.HasPrefix(migration.Member, "etcd-"+desired.Site.Name+"-") {
		return nil, nil
	}
	var endpoints []string
	for member, address := range desired.MemberAddresses {
		if strings.HasPrefix(member, "etcd-") && member != migration.Member {
			if candidate := desired.AddressCandidates[member]; candidate != "" {
				address = candidate
			}
			endpoints = append(endpoints, "https://"+address+":2379")
		}
	}
	slices.Sort(endpoints)
	totalMembers := len(endpoints) + 1
	quorum := totalMembers/2 + 1
	if len(endpoints) < quorum {
		return nil, fmt.Errorf("cannot migrate %s without %d healthy remaining voters",
			migration.Member, quorum)
	}
	backoff := int32(1)
	deadline := int64(300)
	command := fmt.Sprintf(`set -eu
export ETCDCTL_API=3
endpoints=%q
etcdctl --endpoints="${endpoints}" --cacert=/tls/ca.crt --cert=/tls/tls.crt \
  --key=/tls/tls.key endpoint health
member_id="$(etcdctl --endpoints="${endpoints}" --cacert=/tls/ca.crt --cert=/tls/tls.crt \
  --key=/tls/tls.key member list | awk -F', ' -v name=%q '$3 == name {print $1}')"
test -n "${member_id}"
etcdctl --endpoints="${endpoints}" --cacert=/tls/ca.crt --cert=/tls/tls.crt \
  --key=/tls/tls.key member update "${member_id}" --peer-urls=%q
etcdctl --endpoints="${endpoints}" --cacert=/tls/ca.crt --cert=/tls/tls.crt \
  --key=/tls/tls.key member list | grep -F -- %q
`, strings.Join(endpoints, ","), migration.Member,
		"https://"+migration.NewAddress+":2380", "https://"+migration.NewAddress+":2380")
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace,
			Name:      "address-migration-" + operationHash(migration.OperationUID),
			Labels:    resourceLabels(desired),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: stableWorkloadLabels(resourceLabels(desired))},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           workloadServiceAccount,
					AutomountServiceAccountToken: ptr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr(true),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "etcdctl", Image: r.Images.Etcd,
						Command:         []string{"/bin/sh", "-ec", command},
						SecurityContext: restrictedContainer(),
						VolumeMounts: []corev1.VolumeMount{{
							Name: "tls", MountPath: "/tls", ReadOnly: true,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "tls", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: "etcd-maintenance-client-tls"},
						},
					}},
				},
			},
		},
	}, nil
}

func clientCertificate(namespace, name, secretName string, issuer api.IssuerReference,
	labels map[string]string,
) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"namespace": namespace, "name": name, "labels": stringMapAny(labels),
		},
		"spec": map[string]any{
			"secretName": secretName,
			"issuerRef": map[string]any{
				"name": issuer.Name, "kind": issuer.Kind, "group": issuer.Group,
			},
			"commonName": name,
			"usages":     []any{"digital signature", "client auth"},
		},
	}}
}

func memberService(namespace, name, podName string, labels map[string]string, loadBalancer *api.LoadBalancerSpec,
	ports []corev1.ServicePort,
) *corev1.Service {
	selector := map[string]string{"statefulset.kubernetes.io/pod-name": podName}
	return loadBalancerService(namespace, name, labels, selector, loadBalancer, ports)
}

func loadBalancerService(namespace, name string, labels, selector map[string]string,
	loadBalancer *api.LoadBalancerSpec, ports []corev1.ServicePort,
) *corev1.Service {
	annotations := map[string]string{}
	if loadBalancer != nil && loadBalancer.AddressPool != "" {
		annotations["metallb.io/address-pool"] = loadBalancer.AddressPool
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: name, Labels: copyMap(labels), Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:                  corev1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
			Selector:              selector,
			Ports:                 ports,
		},
	}
}

func certificate(namespace, name, secretName string, issuer api.IssuerReference,
	labels map[string]string, addresses []string, dnsNames []string,
) *unstructured.Unstructured {
	for _, address := range addresses {
		if address != "" && net.ParseIP(address) == nil {
			dnsNames = append(dnsNames, address)
		}
	}
	slices.Sort(dnsNames)
	dnsNames = slices.Compact(dnsNames)
	spec := map[string]any{
		"secretName": secretName,
		"issuerRef": map[string]any{
			"name": issuer.Name, "kind": issuer.Kind, "group": issuer.Group,
		},
		"dnsNames": dnsNames,
		"usages":   []any{"digital signature", "key encipherment", "server auth", "client auth"},
	}
	var ipAddresses []any
	for _, address := range addresses {
		if net.ParseIP(address) != nil {
			ipAddresses = append(ipAddresses, address)
		}
	}
	if len(ipAddresses) > 0 {
		spec["ipAddresses"] = ipAddresses
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"namespace": namespace, "name": name, "labels": stringMapAny(labels),
		},
		"spec": spec,
	}}
}

func certificateAddresses(desired plan.SitePlan, member string) []string {
	addresses := []string{desired.MemberAddresses[member], desired.AddressCandidates[member]}
	if migration := desired.AddressMigration; migration != nil && migration.Member == member &&
		migration.OldAddress != "" && migration.OldAddress != migration.NewAddress {
		addresses = append(addresses, migration.OldAddress)
	}
	slices.Sort(addresses)
	return slices.Compact(addresses)
}

func (r Renderer) etcdStatefulSet(desired plan.SitePlan, name, address, initialCluster string,
	labels map[string]string,
) *appsv1.StatefulSet {
	memberLabels := stableWorkloadLabels(labels)
	memberLabels["multisite-postgres.dev/member"] = name
	replicas := int32(1)
	storage := desired.Site.Storage.Etcd
	etcdUser := int64(1000)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: desired.Site.Namespace, Name: name, Labels: copyMap(labels)},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name, Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: memberLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: memberLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName:            workloadServiceAccount,
					AutomountServiceAccountToken:  ptr(false),
					TerminationGracePeriodSeconds: ptr(int64(30)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), RunAsUser: &etcdUser, RunAsGroup: &etcdUser, FSGroup: &etcdUser,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "etcd", Image: r.Images.Etcd,
						Command: []string{"etcd"},
						Args: []string{
							"--name=" + name, "--data-dir=/var/lib/etcd",
							"--listen-client-urls=https://0.0.0.0:2379",
							"--advertise-client-urls=https://" + address + ":2379",
							"--listen-peer-urls=https://0.0.0.0:2380",
							"--initial-advertise-peer-urls=https://" + address + ":2380",
							"--initial-cluster=" + initialCluster, "--initial-cluster-state=new",
							"--initial-cluster-token=" + desired.InstanceUID,
							"--cert-file=/tls/tls.crt", "--key-file=/tls/tls.key",
							"--trusted-ca-file=/tls/ca.crt", "--client-cert-auth=true",
							"--peer-cert-file=/tls/tls.crt", "--peer-key-file=/tls/tls.key",
							"--peer-trusted-ca-file=/tls/ca.crt", "--peer-client-cert-auth=true",
							"--auto-compaction-retention=1h",
						},
						Ports: []corev1.ContainerPort{{Name: "client", ContainerPort: 2379}, {Name: "peer", ContainerPort: 2380}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{
								"etcdctl", "--endpoints=https://127.0.0.1:2379",
								"--cacert=/tls/ca.crt", "--cert=/tls/tls.crt", "--key=/tls/tls.key", "endpoint", "health",
							}}},
							PeriodSeconds: 10, FailureThreshold: 6,
						},
						SecurityContext: restrictedContainer(),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/etcd"},
							{Name: "tls", MountPath: "/tls", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "tls", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: name + "-tls"},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvcTemplate(storage, memberLabels)},
		},
	}
}

func (r Renderer) patroniConfig(desired plan.SitePlan, labels map[string]string) *corev1.ConfigMap {
	endpoints := make([]string, 0)
	for member, address := range desired.MemberAddresses {
		if strings.HasPrefix(member, "etcd-") {
			endpoints = append(endpoints, address+":2379")
		}
	}
	slices.Sort(endpoints)
	postInit := ""
	tdeParameters := ""
	tdeBinaries := ""
	if desired.TDE.Enabled {
		if desired.Restore == nil {
			postInit = "  post_init: /operator/tde-bootstrap.sh\n"
		}
		tdeParameters = `    shared_preload_libraries: pg_tde
`
		tdeBinaries = `  bin_name:
    pg_basebackup: pg_tde_basebackup
    pg_rewind: pg_tde_rewind
`
	}
	backupParameters := ""
	if desired.Backup != nil {
		stanza := backupStanza(desired)
		backupParameters = fmt.Sprintf(`    archive_mode: "on"
    archive_command: 'pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=%s archive-push %%p'
    archive_timeout: 60s
`, stanza)
	}
	initdbTDE := ""
	if desired.TDE.Enabled {
		initdbTDE = "    - set: shared_preload_libraries=pg_tde\n"
	}
	synchronousConfig := `    synchronous_mode: "off"
`
	if desired.Postgres.SynchronousStandbyCount > 0 {
		synchronousConfig = fmt.Sprintf(`    synchronous_mode: "on"
    synchronous_mode_strict: true
    synchronous_node_count: %d
`, desired.Postgres.SynchronousStandbyCount)
	}
	bootstrapMethod := `  initdb:
    - encoding: UTF8
    - data-checksums
` + initdbTDE + postInit
	if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed {
		bootstrapMethod = `  method: pgbackrest
  pgbackrest:
    command: /restore/restore.sh
    keep_existing_recovery_conf: true
    no_params: true
`
	}
	bootstrap := fmt.Sprintf(`bootstrap:
  dcs:
%s
    failsafe_mode: true
    postgresql:
      use_pg_rewind: true
      use_slots: true
      parameters:
        password_encryption: scram-sha-256
      pg_hba:
        - local all all peer
        - hostssl replication all 0.0.0.0/0 scram-sha-256
        - hostssl all all 0.0.0.0/0 scram-sha-256
%s`, synchronousConfig, bootstrapMethod)
	config := fmt.Sprintf(`scope: %s
name: ${MEMBER_NAME}
%srestapi:
  listen: 0.0.0.0:8008
  connect_address: ${PATRONI_CONNECT_ADDRESS}:8008
  certfile: /postgres-tls/tls.crt
  keyfile: /postgres-tls/tls.key
  cafile: /postgres-tls/ca.crt
  verify_client: optional
ctl:
  cacert: /postgres-tls/ca.crt
  certfile: /postgres-tls/tls.crt
  keyfile: /postgres-tls/tls.key
etcd3:
  hosts: %s
  protocol: https
  cacert: /etcd-tls/ca.crt
  cert: /etcd-tls/tls.crt
  key: /etcd-tls/tls.key
postgresql:
  listen: 0.0.0.0:5432
  connect_address: ${PATRONI_CONNECT_ADDRESS}:5432
  data_dir: /var/lib/postgresql/data
%s  parameters:
%s%s    ssl: "on"
    ssl_cert_file: /postgres-tls/tls.crt
    ssl_key_file: /postgres-tls/tls.key
    ssl_ca_file: /postgres-tls/ca.crt
  authentication:
    superuser:
      username: ${POSTGRES_SUPERUSER_USERNAME}
      password: ${POSTGRES_SUPERUSER_PASSWORD}
    replication:
      username: ${POSTGRES_REPLICATION_USERNAME}
      password: ${POSTGRES_REPLICATION_PASSWORD}
tags:
  failover_priority: %d
`, desired.InstanceUID, bootstrap, strings.Join(endpoints, ","), tdeBinaries,
		tdeParameters, backupParameters, desired.Site.PrimaryPreference)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "patroni-" + desired.Site.Name, Labels: copyMap(labels),
		},
		Data: map[string]string{"patroni.yml": config},
	}
}

func (r Renderer) pgBackRestConfig(desired plan.SitePlan, labels map[string]string) (*corev1.ConfigMap, error) {
	repository := desired.Backup.Repository
	if repository.Region == "" {
		repository.Region = "us-east-1"
	}
	if repository.URIStyle == "" {
		repository.URIStyle = "host"
	}
	for name, value := range map[string]string{
		"bucket": repository.Bucket, "prefix": repository.Prefix, "region": repository.Region,
	} {
		if strings.ContainsAny(value, "\r\n") {
			return nil, fmt.Errorf("pgBackRest repository %s must be a single line", name)
		}
	}
	endpoint := repository.Endpoint
	if endpoint == "" {
		endpoint = "https://s3." + repository.Region + ".amazonaws.com"
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return nil, fmt.Errorf("pgBackRest endpoint must be an absolute HTTPS URL")
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	stanza := backupStanza(desired)
	caConfig := ""
	if repository.CABundleSecretRef != nil {
		caConfig = "repo1-storage-ca-file=/repository/ca.crt\n"
	}
	config := fmt.Sprintf(`[global]
repo1-type=s3
repo1-path=/%s
repo1-s3-bucket=%s
repo1-s3-endpoint=%s
repo1-s3-region=%s
repo1-s3-uri-style=%s
repo1-storage-port=%s
repo1-storage-verify-tls=y
%srepo1-s3-key=${S3_ACCESS_KEY}
repo1-s3-key-secret=${S3_SECRET_KEY}
repo1-cipher-type=aes-256-cbc
repo1-cipher-pass=${REPO_CIPHER_PASSPHRASE}
archive-async=y
spool-path=/var/spool/pgbackrest
process-max=4
start-fast=y
tls-server-address=*
tls-server-port=8432
tls-server-ca-file=/pgbackrest-tls/ca.crt
tls-server-cert-file=/pgbackrest-tls/tls.crt
tls-server-key-file=/pgbackrest-tls/tls.key
tls-server-auth=pgbackrest-client=%s

[%s]
pg1-path=/var/lib/postgresql/data
`, strings.Trim(repository.Prefix, "/"), repository.Bucket, parsed.Hostname(), repository.Region,
		repository.URIStyle, port, caConfig, stanza, stanza)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "pgbackrest-" + desired.Site.Name, Labels: copyMap(labels),
		},
		Data: map[string]string{"pgbackrest.conf": config},
	}, nil
}

func backupStanza(desired plan.SitePlan) string {
	if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed {
		return "mspsql-" + desired.Restore.SourceInstanceUID
	}
	return "mspsql-" + desired.InstanceUID
}

func (r Renderer) restoreBootstrapConfig(desired plan.SitePlan,
	labels map[string]string,
) *corev1.ConfigMap {
	args := []string{
		"pgbackrest",
		"--config=/etc/pgbackrest/pgbackrest.conf",
		"--stanza=" + backupStanza(desired),
		"--pg1-path=/var/lib/postgresql/data",
		"--delta",
		"--type=time",
		"--target=" + desired.Restore.TargetTime.UTC().Format(time.RFC3339Nano),
		"--target-action=promote",
	}
	if desired.Restore.BackupSet != "" {
		args = append(args, "--set="+desired.Restore.BackupSet)
	}
	args = append(args, "restore")
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuote(arg))
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "restore-bootstrap", Labels: copyMap(labels),
		},
		Data: map[string]string{
			"restore.sh": "#!/bin/bash\nset -euo pipefail\nexec " + strings.Join(quoted, " ") + "\n",
		},
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func (r Renderer) tdeBootstrapConfig(desired plan.SitePlan, labels map[string]string) *corev1.ConfigMap {
	script := `#!/bin/sh
set -eu

dsn="$1"
provider="$(cat /vault/provider-name)"
principal_key="$(cat /vault/principal-key-name)"
vault_address="$(cat /vault/address)"
key_path="$(cat /vault/key-path)"

psql "$dsn" -v ON_ERROR_STOP=1 \
  -v provider="$provider" \
  -v vault_address="$vault_address" \
  -v key_path="$key_path" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_tde;
SELECT pg_tde_add_global_key_provider_vault_v2(
  :'provider', :'vault_address', :'key_path', '/vault/token', __CA_ARGUMENT__
) WHERE NOT EXISTS (
  SELECT 1 FROM pg_tde_list_all_global_key_providers() WHERE name = :'provider'
);
SELECT pg_tde_change_global_key_provider_vault_v2(
  :'provider', :'vault_address', :'key_path', '/vault/token', __CA_ARGUMENT__
);
SQL

set_default_key() {
  psql "$dsn" -v ON_ERROR_STOP=1 \
    -v provider="$provider" -v principal_key="$principal_key" <<'SQL'
SELECT pg_tde_set_default_key_using_global_key_provider(:'principal_key', :'provider');
SQL
}

if __KEY_CREATOR__; then
  if ! set_default_key; then
    psql "$dsn" -v ON_ERROR_STOP=1 \
      -v provider="$provider" -v principal_key="$principal_key" <<'SQL'
SELECT pg_tde_create_key_using_global_key_provider(:'principal_key', :'provider');
SQL
    set_default_key
  fi
else
  attached=false
  for attempt in $(seq 1 60); do
    if set_default_key; then
      attached=true
      break
    fi
    sleep 5
  done
  test "$attached" = true
fi

psql "$dsn" -v ON_ERROR_STOP=1 <<'SQL'
ALTER DATABASE postgres SET default_table_access_method = tde_heap;
ALTER DATABASE postgres SET pg_tde.enforce_encryption = on;
SQL

psql "$dsn" -d template1 -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_tde;
ALTER DATABASE template1 SET default_table_access_method = tde_heap;
ALTER DATABASE template1 SET pg_tde.enforce_encryption = on;
SQL
`
	caArgument := "NULL"
	if desired.Site.VaultAuth != nil && desired.Site.VaultAuth.CABundleSecretRef != nil {
		caArgument = "'/vault/ca.crt'"
	}
	script = strings.ReplaceAll(script, "__CA_ARGUMENT__", caArgument)
	script = strings.ReplaceAll(script, "__KEY_CREATOR__", strconv.FormatBool(desired.TDEKeyCreator))
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "pg-tde-bootstrap", Labels: copyMap(labels),
		},
		Data: map[string]string{"tde-bootstrap.sh": script},
	}
}

func (r Renderer) tdeAuditJob(desired plan.SitePlan, labels map[string]string) *batchv1.Job {
	var members []string
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		name := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed &&
			name != desired.Restore.SeedMember {
			continue
		}
		members = append(members, name+"."+desired.Site.Namespace+".svc")
	}
	script := `set -euo pipefail
for host in "$@"; do
  ready=false
  for attempt in $(seq 1 60); do
    if psql -X -h "$host" -d postgres -Atqc 'SELECT 1' >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 10
  done
  test "$ready" = true
  psql -X -h "$host" -d postgres -Atqc \
    "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY datname" |
  while IFS= read -r database; do
    psql -X -h "$host" -d "$database" -v ON_ERROR_STOP=1 -Atq <<'SQL'
SELECT 1 / ((SELECT count(*) FROM pg_extension WHERE extname = 'pg_tde') = 1)::int;
SELECT 1 / (current_setting('default_table_access_method') = 'tde_heap')::int;
SELECT 1 / (current_setting('pg_tde.enforce_encryption') = 'on')::int;
SELECT pg_tde_verify_key();
SELECT 1 / (NOT EXISTS (
  SELECT 1
  FROM pg_class AS relation
  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
  JOIN pg_am AS access_method ON access_method.oid = relation.relam
  WHERE relation.relkind IN ('r', 'm')
    AND relation.relpersistence <> 't'
    AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND namespace.nspname !~ '^pg_toast'
    AND access_method.amname <> 'tde_heap'
    AND NOT EXISTS (
      SELECT 1 FROM pg_depend
      WHERE classid = 'pg_class'::regclass
        AND objid = relation.oid
        AND deptype = 'e'
    )
))::int;
SQL
  done
done
`
	jobLabels := copyMap(labels)
	jobLabels["multisite-postgres.dev/component"] = "tde-audit"
	backoffLimit := int32(3)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace,
			Name:      fmt.Sprintf("mspsql-tde-audit-%d", desired.Revision),
			Labels:    jobLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: jobLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName:           workloadServiceAccount,
					AutomountServiceAccountToken: ptr(false),
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), FSGroup: ptr(int64(26)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "audit", Image: desired.Postgres.Image,
						Command:         append([]string{"/bin/bash", "-ec", script, "tde-audit"}, members...),
						SecurityContext: restrictedContainer(),
						Env: []corev1.EnvVar{
							{Name: "PGUSER", ValueFrom: secretKeySelector(
								"postgres-auth", "superuser-username")},
							{Name: "PGPASSWORD", ValueFrom: secretKeySelector("postgres-auth", "superuser-password")},
							{Name: "PGSSLMODE", Value: "verify-full"},
							{Name: "PGSSLROOTCERT", Value: "/postgres-tls/ca.crt"},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "postgres-tls", MountPath: "/postgres-tls", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "postgres-tls", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: fmt.Sprintf("postgres-%s-0-tls", desired.Site.Name),
							},
						},
					}},
				},
			},
		},
	}
}

func (r Renderer) postgresStatefulSet(desired plan.SitePlan, name, address string,
	labels map[string]string,
) *appsv1.StatefulSet {
	image := postgresMemberImage(desired, name)
	workloadLabels := stableWorkloadLabels(labels)
	workloadLabels["multisite-postgres.dev/component"] = "postgres"
	workloadLabels["multisite-postgres.dev/member"] = name
	podAnnotations := map[string]string{}
	if version := credentialVersionForMember(desired, name); version != "" {
		podAnnotations["multisite-postgres.dev/credential-version"] = version
	}
	replicas := int32(1)
	command := fmt.Sprintf(`export POSTGRES_SUPERUSER_USERNAME="$(cat /credentials/superuser-username)"
export POSTGRES_SUPERUSER_PASSWORD="$(cat /credentials/superuser-password)"
export POSTGRES_REPLICATION_USERNAME="$(cat /credentials/replication-username)"
export POSTGRES_REPLICATION_PASSWORD="$(cat /credentials/replication-password)"
export MEMBER_NAME=%q
export PATRONI_CONNECT_ADDRESS=%q
envsubst < /config/patroni.yml > /tmp/patroni.yml
exec patroni /tmp/patroni.yml`, name, address)
	volumeMounts := []corev1.VolumeMount{
		{Name: "data", MountPath: "/var/lib/postgresql/data"},
		{Name: "config", MountPath: "/config", ReadOnly: true},
		{Name: "credentials", MountPath: "/credentials", ReadOnly: true},
		{Name: "etcd-tls", MountPath: "/etcd-tls", ReadOnly: true},
		{Name: "postgres-tls", MountPath: "/postgres-tls", ReadOnly: true},
	}
	volumes := []corev1.Volume{
		{Name: "config", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{
				Name: "patroni-" + desired.Site.Name,
			}},
		}},
		{Name: "credentials", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "postgres-auth"},
		}},
		{Name: "etcd-tls", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "patroni-etcd-client-tls"},
		}},
		{Name: "postgres-tls", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: name + "-tls"},
		}},
	}
	if desired.TDE.Enabled {
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{Name: "operator", MountPath: "/operator", ReadOnly: true},
			corev1.VolumeMount{Name: "pg-tde-vault", MountPath: "/vault", ReadOnly: true},
		)
		volumes = append(volumes,
			corev1.Volume{Name: "operator", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "pg-tde-bootstrap"},
					DefaultMode:          ptr(int32(0o550)),
				},
			}},
			corev1.Volume{Name: "pg-tde-vault", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "pg-tde-vault"},
			}},
		)
	}
	if desired.Restore != nil && desired.Restore.Phase == plan.RestorePhaseSeed {
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{Name: "restore-bootstrap", MountPath: "/restore", ReadOnly: true},
		)
		volumes = append(volumes,
			corev1.Volume{Name: "restore-bootstrap", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "restore-bootstrap"},
					DefaultMode:          ptr(int32(0o550)),
				},
			}},
		)
	}
	containers := []corev1.Container{{
		Name: "postgres-patroni", Image: image,
		Command: []string{"/bin/bash", "-ec", command},
		Ports: []corev1.ContainerPort{
			{Name: "postgres", ContainerPort: 5432},
			{Name: "patroni", ContainerPort: 8008},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: "/readiness", Port: intstr.FromString("patroni"),
				Scheme: corev1.URISchemeHTTPS,
			}},
			PeriodSeconds: 10, FailureThreshold: 6,
		},
		SecurityContext: restrictedContainer(),
		VolumeMounts:    volumeMounts,
	}}
	var initContainers []corev1.Container
	if desired.Backup != nil {
		volumes = append(volumes,
			corev1.Volume{Name: "pgbackrest-template", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{
					Name: "pgbackrest-" + desired.Site.Name,
				}},
			}},
			corev1.Volume{Name: "pgbackrest-runtime", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			}},
			corev1.Volume{Name: "pgbackrest-repository", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "pgbackrest-repository"},
			}},
			corev1.Volume{Name: "pgbackrest-spool", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			}},
			corev1.Volume{Name: "pgbackrest-tls", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: name + "-pgbackrest-tls"},
			}},
		)
		containers[0].VolumeMounts = append(containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "pgbackrest-runtime", MountPath: "/etc/pgbackrest", ReadOnly: true},
			corev1.VolumeMount{Name: "pgbackrest-repository", MountPath: "/repository", ReadOnly: true},
			corev1.VolumeMount{Name: "pgbackrest-spool", MountPath: "/var/spool/pgbackrest"},
		)
		secretEnv := []corev1.EnvVar{
			{Name: "S3_ACCESS_KEY", ValueFrom: secretKeySelector("pgbackrest-repository", "s3-access-key")},
			{Name: "S3_SECRET_KEY", ValueFrom: secretKeySelector("pgbackrest-repository", "s3-secret-key")},
			{Name: "REPO_CIPHER_PASSPHRASE", ValueFrom: secretKeySelector(
				"pgbackrest-repository", "repo-cipher-passphrase")},
		}
		initContainers = append(initContainers, corev1.Container{
			Name: "pgbackrest-config", Image: image,
			Command: []string{"/bin/bash", "-ec",
				"umask 077; envsubst < /template/pgbackrest.conf > /runtime/pgbackrest.conf"},
			Env:             secretEnv,
			SecurityContext: restrictedContainer(),
			VolumeMounts: []corev1.VolumeMount{
				{Name: "pgbackrest-template", MountPath: "/template", ReadOnly: true},
				{Name: "pgbackrest-runtime", MountPath: "/runtime"},
			},
		})
		containers = append(containers, corev1.Container{
			Name: "pgbackrest", Image: image,
			Command: []string{"pgbackrest", "--config=/etc/pgbackrest/pgbackrest.conf", "server"},
			Ports:   []corev1.ContainerPort{{Name: "pgbackrest", ContainerPort: 8432}},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromString("pgbackrest"),
				}},
				PeriodSeconds: 10, FailureThreshold: 6,
			},
			SecurityContext: restrictedContainer(),
			VolumeMounts: []corev1.VolumeMount{
				{Name: "data", MountPath: "/var/lib/postgresql/data", ReadOnly: true},
				{Name: "pgbackrest-runtime", MountPath: "/etc/pgbackrest", ReadOnly: true},
				{Name: "pgbackrest-repository", MountPath: "/repository", ReadOnly: true},
				{Name: "pgbackrest-spool", MountPath: "/var/spool/pgbackrest"},
				{Name: "pgbackrest-tls", MountPath: "/pgbackrest-tls", ReadOnly: true},
			},
		})
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: name, Labels: copyMap(labels),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name, Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: workloadLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: workloadLabels, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					ServiceAccountName:            workloadServiceAccount,
					AutomountServiceAccountToken:  ptr(false),
					TerminationGracePeriodSeconds: ptr(int64(60)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), FSGroup: ptr(int64(26)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Affinity:       antiAffinity(workloadLabels),
					InitContainers: initContainers,
					Containers:     containers,
					Volumes:        volumes,
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				pvcTemplate(desired.Site.Storage.Postgres, workloadLabels),
			},
		},
	}
}

func credentialVersionForMember(desired plan.SitePlan, member string) string {
	rotation := desired.CredentialRotation
	if rotation != nil &&
		(member == rotation.TargetMember || slices.Contains(rotation.UpdatedMembers, member)) {
		return strconv.FormatInt(rotation.Version, 10)
	}
	if rotation != nil && rotation.PreviousVersion > 0 {
		return strconv.FormatInt(rotation.PreviousVersion, 10)
	}
	if desired.RuntimeCredentialVersion > 0 {
		return strconv.FormatInt(desired.RuntimeCredentialVersion, 10)
	}
	return ""
}

func postgresMemberImage(desired plan.SitePlan, member string) string {
	if desired.MajorUpgrade != nil {
		switch desired.MajorUpgrade.Phase {
		case plan.MajorUpgradePhaseStartPrimary, plan.MajorUpgradePhaseRestoreWrites:
			if member == desired.MajorUpgrade.Primary {
				return desired.MajorUpgrade.TargetImage
			}
		case plan.MajorUpgradePhaseReplicas, plan.MajorUpgradePhaseFinalize:
			return desired.MajorUpgrade.TargetImage
		}
	}
	if desired.Upgrade == nil {
		return desired.Postgres.Image
	}
	if member == desired.Upgrade.TargetMember ||
		slices.Contains(desired.Upgrade.UpgradedMembers, member) {
		return desired.Upgrade.TargetImage
	}
	return desired.Postgres.Image
}

func (r Renderer) pgpoolConfig(desired plan.SitePlan, labels map[string]string) *corev1.ConfigMap {
	var backends []string
	var ordinal int
	for member, address := range desired.MemberAddresses {
		if !strings.HasPrefix(member, "postgres-") {
			continue
		}
		backends = append(backends, fmt.Sprintf(
			"backend_hostname%d = '%s'\nbackend_port%d = 5432\nbackend_weight%d = 1\n",
			ordinal, address, ordinal, ordinal))
		ordinal++
	}
	slices.Sort(backends)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "pgpool-" + desired.Site.Name, Labels: copyMap(labels),
		},
		Data: map[string]string{
			"pgpool.conf": "listen_addresses = '*'\nport = 5432\n" +
				"enable_pool_hba = on\nssl = on\n" +
				"ssl_key = '/tls/tls.key'\nssl_cert = '/tls/tls.crt'\n" +
				"ssl_ca_cert = '/backend-ca/ca.crt'\n" + strings.Join(backends, ""),
			"pool_hba.conf": "hostssl all all 0.0.0.0/0 password\n",
		},
	}
}

func (r Renderer) pgpoolDeployment(desired plan.SitePlan, labels map[string]string) *appsv1.Deployment {
	workloadLabels := stableWorkloadLabels(labels)
	workloadLabels["multisite-postgres.dev/component"] = "pgpool"
	replicas := desired.Site.Components.PgpoolReplicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "pgpool-" + desired.Site.Name, Labels: copyMap(labels),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: workloadLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: workloadLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName:           workloadServiceAccount,
					AutomountServiceAccountToken: ptr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Affinity: antiAffinity(workloadLabels),
					InitContainers: []corev1.Container{{
						Name: "prepare-tls", Image: r.Images.Pgpool,
						Command: []string{"/bin/sh", "-ec",
							"cp /tls-source/tls.crt /tls/tls.crt; " +
								"cp /tls-source/tls.key /tls/tls.key; " +
								"cp /tls-source/ca.crt /tls/ca.crt; " +
								"chmod 644 /tls/tls.crt /tls/ca.crt; chmod 600 /tls/tls.key"},
						SecurityContext: restrictedContainer(),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "tls-source", MountPath: "/tls-source", ReadOnly: true},
							{Name: "tls", MountPath: "/tls"},
						},
					}},
					Containers: []corev1.Container{{
						Name: "pgpool", Image: r.Images.Pgpool,
						Command: []string{"pgpool", "-n", "-f", "/config/pgpool.conf", "-a", "/config/pool_hba.conf"},
						Ports:   []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
								Port: intstr.FromString("postgres"),
							}},
							PeriodSeconds: 5,
						},
						SecurityContext: restrictedContainer(),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config", MountPath: "/config", ReadOnly: true},
							{Name: "tls", MountPath: "/tls", ReadOnly: true},
							{Name: "backend-ca", MountPath: "/backend-ca", ReadOnly: true},
							{Name: "runtime", MountPath: "/var/run/pgpool"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "config", VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{
								Name: "pgpool-" + desired.Site.Name,
							}},
						}},
						{Name: "tls-source", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: "pgpool-" + desired.Site.Name + "-tls"},
						}},
						{Name: "tls", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "backend-ca", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: "postgres-" + desired.Site.Name + "-0-tls",
								Items:      []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
							},
						}},
						{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
}

func etcdInitialCluster(desired plan.SitePlan) (string, error) {
	var members []string
	for member, address := range desired.MemberAddresses {
		if strings.HasPrefix(member, "etcd-") {
			if address == "" {
				return "", fmt.Errorf("etcd address for %s is not allocated", member)
			}
			members = append(members, member+"=https://"+address+":2380")
		}
	}
	slices.Sort(members)
	if len(members) < 3 || len(members)%2 == 0 {
		return "", fmt.Errorf("complete odd etcd member address set is required")
	}
	return strings.Join(members, ","), nil
}

func pvcTemplate(storage *api.StorageRequest, labels map[string]string) corev1.PersistentVolumeClaim {
	requests := corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	var className *string
	if storage != nil {
		requests[corev1.ResourceStorage] = storage.Size
		className = &storage.StorageClassName
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: copyMap(labels)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: className,
			Resources:        corev1.VolumeResourceRequirements{Requests: requests},
		},
	}
}

func restrictedContainer() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr(false),
		ReadOnlyRootFilesystem:   ptr(false),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

func secretKeySelector(name, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
		Key:                  key,
	}}
}

func antiAffinity(labels map[string]string) *corev1.Affinity {
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
			Weight: 100,
			PodAffinityTerm: corev1.PodAffinityTerm{
				TopologyKey:   "kubernetes.io/hostname",
				LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
			},
		}},
	}}
}

func resourceLabels(desired plan.SitePlan) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by":            "mspsql-agent",
		"multisite-postgres.dev/hub-namespace":    desired.HubNamespace,
		"multisite-postgres.dev/hub-name":         desired.HubName,
		"multisite-postgres.dev/instance-uid":     desired.InstanceUID,
		"multisite-postgres.dev/site-name":        desired.Site.Name,
		"multisite-postgres.dev/desired-revision": fmt.Sprintf("%d", desired.Revision),
	}
}

func copyMap(input map[string]string) map[string]string {
	return maps.Clone(input)
}

func stableWorkloadLabels(input map[string]string) map[string]string {
	labels := copyMap(input)
	delete(labels, "multisite-postgres.dev/desired-revision")
	return labels
}

func stringMapAny(input map[string]string) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func ptr[T any](value T) *T {
	return &value
}
