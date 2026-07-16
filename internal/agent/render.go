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
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
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
		objects = append(objects, memberService(desired.Site.Namespace, name, name, labels,
			desired.Site.LoadBalancer, []corev1.ServicePort{
				{Name: "postgres", Port: 5432, TargetPort: intstr.FromInt32(5432)},
				{Name: "patroni", Port: 8008, TargetPort: intstr.FromInt32(8008)},
			}))
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
	var objects []client.Object
	for ordinal := int32(0); ordinal < desired.Site.Components.EtcdReplicas; ordinal++ {
		name := fmt.Sprintf("etcd-%s-%d", desired.Site.Name, ordinal)
		objects = append(objects, certificate(desired.Site.Namespace, name, name+"-tls",
			desired.Site.Certificates.EtcdIssuerRef, labels, desired.MemberAddresses[name],
			[]string{name, name + "." + desired.Site.Namespace + ".svc"}))
	}
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		name := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		objects = append(objects, certificate(desired.Site.Namespace, name, name+"-tls",
			desired.Site.Certificates.PostgresIssuerRef, labels, desired.MemberAddresses[name],
			[]string{name, name + "." + desired.Site.Namespace + ".svc"}))
	}
	if desired.Site.Components.PgpoolReplicas > 0 {
		name := "pgpool-" + desired.Site.Name
		objects = append(objects, certificate(desired.Site.Namespace, name, name+"-tls",
			desired.Site.Certificates.PgpoolIssuerRef, labels, desired.MemberAddresses[name],
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
	patroniConfig := r.patroniConfig(desired, labels)
	objects = append(objects, patroniConfig, r.postgresStatefulSet(desired, labels))
	if desired.Site.Components.PgpoolReplicas > 0 {
		objects = append(objects, r.pgpoolConfig(desired, labels), r.pgpoolDeployment(desired, labels))
	}
	return objects, nil
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
	labels map[string]string, address string, dnsNames []string,
) *unstructured.Unstructured {
	spec := map[string]any{
		"secretName": secretName,
		"issuerRef": map[string]any{
			"name": issuer.Name, "kind": issuer.Kind, "group": issuer.Group,
		},
		"dnsNames": dnsNames,
		"usages":   []any{"digital signature", "key encipherment", "server auth", "client auth"},
	}
	if net.ParseIP(address) != nil {
		spec["ipAddresses"] = []any{address}
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

func (r Renderer) etcdStatefulSet(desired plan.SitePlan, name, address, initialCluster string,
	labels map[string]string,
) *appsv1.StatefulSet {
	memberLabels := copyMap(labels)
	memberLabels["multisite-postgres.dev/member"] = name
	replicas := int32(1)
	storage := desired.Site.Storage.Etcd
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: desired.Site.Namespace, Name: name, Labels: copyMap(labels)},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name, Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: memberLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: memberLabels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(30)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "etcd", Image: r.Images.Etcd,
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
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvcTemplate(storage)},
		},
	}
}

func (r Renderer) patroniConfig(desired plan.SitePlan, labels map[string]string) *corev1.ConfigMap {
	endpoints := make([]string, 0)
	for member, address := range desired.MemberAddresses {
		if strings.HasPrefix(member, "etcd-") {
			endpoints = append(endpoints, "https://"+address+":2379")
		}
	}
	slices.Sort(endpoints)
	config := fmt.Sprintf(`scope: %s
name: ${POD_NAME}
restapi:
  listen: 0.0.0.0:8008
  connect_address: ${POD_IP}:8008
etcd3:
  hosts: %s
  protocol: https
  cacert: /etcd-tls/ca.crt
  cert: /etcd-tls/tls.crt
  key: /etcd-tls/tls.key
postgresql:
  listen: 0.0.0.0:5432
  connect_address: ${POD_IP}:5432
  data_dir: /var/lib/postgresql/data
  authentication:
    superuser:
      username: postgres
      password: ${POSTGRES_SUPERUSER_PASSWORD}
    replication:
      username: replication
      password: ${POSTGRES_REPLICATION_PASSWORD}
tags:
  failover_priority: %d
`, desired.InstanceUID, strings.Join(endpoints, ","), desired.Site.PrimaryPreference)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "patroni-" + desired.Site.Name, Labels: copyMap(labels),
		},
		Data: map[string]string{"patroni.yml": config},
	}
}

func (r Renderer) postgresStatefulSet(desired plan.SitePlan, labels map[string]string) *appsv1.StatefulSet {
	workloadLabels := copyMap(labels)
	workloadLabels["multisite-postgres.dev/component"] = "postgres"
	replicas := desired.Site.Components.PostgresReplicas
	command := `export POSTGRES_SUPERUSER_PASSWORD="$(cat /credentials/superuser-password)"
export POSTGRES_REPLICATION_PASSWORD="$(cat /credentials/replication-password)"
envsubst < /config/patroni.yml > /tmp/patroni.yml
exec patroni /tmp/patroni.yml`
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: "postgres-" + desired.Site.Name, Labels: copyMap(labels),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "postgres-" + desired.Site.Name, Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: workloadLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: workloadLabels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(60)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), FSGroup: ptr(int64(26)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Affinity: antiAffinity(workloadLabels),
					Containers: []corev1.Container{{
						Name: "postgres-patroni", Image: desired.Postgres.Image,
						Command: []string{"/bin/bash", "-ec", command},
						Env: []corev1.EnvVar{
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							}},
							{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
							}},
						},
						Ports: []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432}, {Name: "patroni", ContainerPort: 8008}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
								Path: "/readiness", Port: intstr.FromString("patroni"),
							}},
							PeriodSeconds: 10, FailureThreshold: 6,
						},
						SecurityContext: restrictedContainer(),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/postgresql/data"},
							{Name: "config", MountPath: "/config", ReadOnly: true},
							{Name: "credentials", MountPath: "/credentials", ReadOnly: true},
							{Name: "etcd-tls", MountPath: "/etcd-tls", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
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
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvcTemplate(desired.Site.Storage.Postgres)},
		},
	}
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
			"pgpool.conf":   "listen_addresses = '*'\nport = 5432\nssl = on\n" + strings.Join(backends, ""),
			"pool_hba.conf": "hostssl all all 0.0.0.0/0 scram-sha-256\n",
		},
	}
}

func (r Renderer) pgpoolDeployment(desired plan.SitePlan, labels map[string]string) *appsv1.Deployment {
	workloadLabels := copyMap(labels)
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
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Affinity: antiAffinity(workloadLabels),
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
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "config", VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{
								Name: "pgpool-" + desired.Site.Name,
							}},
						}},
						{Name: "tls", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: "pgpool-" + desired.Site.Name + "-tls"},
						}},
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

func pvcTemplate(storage *api.StorageRequest) corev1.PersistentVolumeClaim {
	requests := corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	className := ""
	if storage != nil {
		requests[corev1.ResourceStorage] = storage.Size
		className = storage.StorageClassName
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &className,
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
