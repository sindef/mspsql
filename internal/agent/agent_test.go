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
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

func TestCacheRejectsRollback(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	cache := Cache{
		Client: kube, Namespace: "system", PublicKey: publicKey, SiteUID: "site",
		Now: func() time.Time { return time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC) },
	}
	sign := func(revision int64) plan.Envelope {
		envelope, err := plan.Sign(privateKey, plan.SitePlan{
			SiteUID: "site", InstanceUID: "instance", Revision: revision,
		})
		if err != nil {
			t.Fatal(err)
		}
		return envelope
	}
	if _, err := cache.Store(context.Background(), sign(2), "instance"); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Store(context.Background(), sign(1), "instance"); err == nil {
		t.Fatal("rollback plan was accepted")
	}
}

func TestNamespaceOwnershipIsNotAdopted(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres"},
	}).Build()
	err := EnsureNamespace(context.Background(), kube, "postgres", "hub", "site", "instance", true)
	if !errors.Is(err, ErrNamespaceOwnershipConflict) {
		t.Fatalf("error = %v", err)
	}
}

func TestRendererCreatesMemberLoadBalancersAndWorkloads(t *testing.T) {
	desired := plan.SitePlan{
		SiteUID: "site", InstanceUID: "instance", HubNamespace: "platform", HubName: "orders", Revision: 1,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders", Role: api.SiteRoleData,
			Components: api.SiteComponents{EtcdReplicas: 2, PostgresReplicas: 2, PgpoolReplicas: 2},
			Storage: api.SiteStorage{
				Etcd:     &api.StorageRequest{StorageClassName: "standard"},
				Postgres: &api.StorageRequest{StorageClassName: "standard"},
			},
		},
		Postgres: api.PostgresSpec{Image: "postgres:17"},
		MemberAddresses: map[string]string{
			"etcd-vic-0": "10.0.0.1", "etcd-vic-1": "10.0.0.2", "etcd-nsw-0": "10.0.1.1",
			"postgres-vic-0": "10.0.0.10", "postgres-vic-1": "10.0.0.11",
		},
	}
	renderer := Renderer{Images: Images{Etcd: "etcd:3.6", Pgpool: "pgpool:4.6"}}
	if got := len(renderer.LoadBalancers(desired)); got != 5 {
		t.Fatalf("LoadBalancer count = %d", got)
	}
	certificates := renderer.Certificates(desired)
	if got := len(certificates); got != 7 {
		t.Fatalf("Certificate count = %d", got)
	}
	clientSecrets := map[string]bool{}
	for _, object := range certificates {
		certificate := object.(*unstructured.Unstructured)
		secretName, _, err := unstructured.NestedString(certificate.Object, "spec", "secretName")
		if err != nil {
			t.Fatal(err)
		}
		clientSecrets[secretName] = true
	}
	if !clientSecrets["patroni-etcd-client-tls"] || !clientSecrets["etcd-maintenance-client-tls"] {
		t.Fatalf("client certificate Secrets = %#v", clientSecrets)
	}
	objects, err := renderer.Workloads(desired)
	if err != nil {
		t.Fatal(err)
	}
	var statefulSets, deployments int
	for _, object := range objects {
		switch object.(type) {
		case *appsv1.StatefulSet:
			statefulSets++
		case *appsv1.Deployment:
			deployments++
		}
	}
	if statefulSets != 4 || deployments != 1 {
		t.Fatalf("statefulSets=%d deployments=%d", statefulSets, deployments)
	}
	for _, object := range objects {
		statefulSet, ok := object.(*appsv1.StatefulSet)
		if !ok || statefulSet.Name != "postgres-vic-0" {
			continue
		}
		if *statefulSet.Spec.Replicas != 1 {
			t.Fatalf("member replicas = %d", *statefulSet.Spec.Replicas)
		}
		command := statefulSet.Spec.Template.Spec.Containers[0].Command[2]
		if !strings.Contains(command, "10.0.0.10") {
			t.Fatalf("member command does not advertise its LoadBalancer address: %s", command)
		}
	}
}

func TestReadinessUsesObservedControllerStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	certificateGVK := schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
	}
	scheme.AddKnownTypeWithName(certificateGVK, &unstructured.Unstructured{})
	certificate := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"namespace": "orders",
			"name":      "postgres-vic-0",
		},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "True",
		}}},
	}}
	replicas := int32(2)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: "postgres-vic", Generation: 3},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 3,
			ReadyReplicas:      1,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(certificate, statefulSet).Build()
	reconciler := Reconciler{Client: kube}
	ready, message, err := reconciler.certificatesReady(context.Background(), []client.Object{certificate})
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatalf("certificate was not Ready: %s", message)
	}
	ready, message, err = reconciler.workloadsReady(context.Background(), []client.Object{statefulSet})
	if err != nil {
		t.Fatal(err)
	}
	if ready || message == "" {
		t.Fatalf("partially available StatefulSet was reported Ready: %q", message)
	}
}

func TestDiscoverInventoryReportsStorageAndIssuers(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	issuerGVK := schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Issuer"}
	clusterIssuerGVK := schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "ClusterIssuer"}
	scheme.AddKnownTypeWithName(issuerGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(issuerGVK.GroupVersion().WithKind("IssuerList"), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(clusterIssuerGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(clusterIssuerGVK.GroupVersion().WithKind("ClusterIssuerList"),
		&unstructured.UnstructuredList{})
	allowExpansion := true
	reclaimPolicy := corev1.PersistentVolumeReclaimRetain
	bindingMode := storagev1.VolumeBindingWaitForFirstConsumer
	storageClass := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "nvme"},
		Provisioner:          "csi.example",
		AllowVolumeExpansion: &allowExpansion,
		ReclaimPolicy:        &reclaimPolicy,
		VolumeBindingMode:    &bindingMode,
		AllowedTopologies: []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{
			Key: "topology.kubernetes.io/zone", Values: []string{"vic-a"},
		}}}},
	}
	issuer := &unstructured.Unstructured{}
	issuer.SetGroupVersionKind(clusterIssuerGVK)
	issuer.SetName("etcd-root")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(storageClass, issuer).Build()
	encoded, err := DiscoverInventory(context.Background(), kube)
	if err != nil {
		t.Fatal(err)
	}
	var inventory SiteInventory
	if err := json.Unmarshal(encoded, &inventory); err != nil {
		t.Fatal(err)
	}
	if len(inventory.StorageClasses) != 1 || inventory.StorageClasses[0].Provisioner != "csi.example" {
		t.Fatalf("storage inventory = %#v", inventory.StorageClasses)
	}
	if len(inventory.Issuers) != 1 || inventory.Issuers[0].Name != "etcd-root" {
		t.Fatalf("issuer inventory = %#v", inventory.Issuers)
	}
}

func TestPruneDeletesOnlyObjectsFromOlderRevisions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	certificateGVK := schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
	}
	scheme.AddKnownTypeWithName(certificateGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(certificateGVK.GroupVersion().WithKind("CertificateList"),
		&unstructured.UnstructuredList{})
	labels := func(revision string) map[string]string {
		return map[string]string{
			"app.kubernetes.io/managed-by":            "mspsql-agent",
			"multisite-postgres.dev/instance-uid":     "instance",
			"multisite-postgres.dev/desired-revision": revision,
		}
	}
	stale := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "orders", Name: "postgres-old", Labels: labels("1"),
	}}
	current := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: "orders", Name: "patroni", Labels: labels("2"),
	}}
	unmanaged := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Namespace: "orders", Name: "application",
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale, current, unmanaged).Build()
	reconciler := Reconciler{Client: kube}
	if err := reconciler.pruneStaleObjects(context.Background(), plan.SitePlan{
		InstanceUID: "instance", Revision: 2,
		Site: api.PostgresSiteSpec{Namespace: "orders"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(stale), &appsv1.StatefulSet{}); err == nil {
		t.Fatal("stale StatefulSet was not deleted")
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(current), &corev1.ConfigMap{}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(unmanaged), &corev1.Service{}); err != nil {
		t.Fatal(err)
	}
}

func TestRetainDeletionRemovesWorkloadsAndKeepsPVCs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	certificateGVK := schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
	}
	scheme.AddKnownTypeWithName(certificateGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(certificateGVK.GroupVersion().WithKind("CertificateList"),
		&unstructured.UnstructuredList{})
	ownership := map[string]string{
		enabledLabel: "true", hubDomainLabel: "hub.example", siteUIDLabel: "site",
		instanceUIDLabel: "instance",
	}
	managed := map[string]string{
		"app.kubernetes.io/managed-by":        "mspsql-agent",
		"multisite-postgres.dev/instance-uid": "instance",
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "orders", Labels: ownership}}
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "orders", Name: "postgres-vic-0", Labels: managed,
	}}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "orders", Name: "data-postgres-vic-0-0", Labels: maps.Clone(managed),
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(namespace, statefulSet, claim).Build()
	reconciler := Reconciler{Client: kube, HubDomain: "hub.example", SiteUID: "site"}
	result, err := reconciler.Apply(context.Background(), plan.SitePlan{
		SiteUID: "site", InstanceUID: "instance", Revision: 2,
		Site:     api.PostgresSiteSpec{Namespace: "orders"},
		Deletion: &plan.DeletionPlan{Policy: api.DeletionPolicyRetain},
	}, plan.SitePlan{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.Phase != "Deleted" {
		t.Fatalf("phase = %s", result.Phase)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(statefulSet),
		&appsv1.StatefulSet{}); err == nil {
		t.Fatal("StatefulSet was retained")
	}
	var retained corev1.PersistentVolumeClaim
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(claim), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Labels["multisite-postgres.dev/retained-instance-uid"] != "instance" {
		t.Fatalf("retained PVC labels = %#v", retained.Labels)
	}
}

func TestRendererConfiguresPgTDEBootstrap(t *testing.T) {
	desired := plan.SitePlan{
		SiteUID: "site", InstanceUID: "instance", Revision: 1,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders", Role: api.SiteRoleData,
			Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 1},
			Storage: api.SiteStorage{
				Etcd: &api.StorageRequest{}, Postgres: &api.StorageRequest{},
			},
		},
		Postgres: api.PostgresSpec{Image: "percona-postgres:17"},
		TDE: api.TDESpec{Enabled: true, Vault: &api.TDEVaultSpec{
			KVMount: "tde", KeyPath: "postgres/orders",
		}},
		MemberAddresses: map[string]string{
			"etcd-vic-0": "10.0.0.1", "etcd-nsw-0": "10.0.1.1", "etcd-qld-0": "10.0.2.1",
			"postgres-vic-0": "10.0.0.10",
		},
	}
	renderer := Renderer{Images: Images{Etcd: "etcd", Pgpool: "pgpool"}}
	objects, err := renderer.Workloads(desired)
	if err != nil {
		t.Fatal(err)
	}
	var patroniConfig, bootstrapScript string
	var postgres *appsv1.StatefulSet
	for _, object := range objects {
		switch object := object.(type) {
		case *corev1.ConfigMap:
			patroniConfig += object.Data["patroni.yml"]
			bootstrapScript += object.Data["tde-bootstrap.sh"]
		case *appsv1.StatefulSet:
			if strings.HasPrefix(object.Name, "postgres-") {
				postgres = object
			}
		}
	}
	for _, expected := range []string{
		"shared_preload_libraries=pg_tde", "pg_tde_basebackup", "post_init: /operator/tde-bootstrap.sh",
		"verify_client: optional", "certfile: /postgres-tls/tls.crt",
	} {
		if !strings.Contains(patroniConfig, expected) {
			t.Fatalf("Patroni config is missing %q:\n%s", expected, patroniConfig)
		}
	}
	for _, expected := range []string{
		"pg_tde_add_global_key_provider_vault_v2", "pg_tde.enforce_encryption", "template1",
	} {
		if !strings.Contains(bootstrapScript, expected) {
			t.Fatalf("TDE bootstrap is missing %q", expected)
		}
	}
	if postgres == nil || !hasVolume(postgres.Spec.Template.Spec.Volumes, "pg-tde-vault") {
		t.Fatal("PostgreSQL member does not mount the TDE Vault token")
	}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}
