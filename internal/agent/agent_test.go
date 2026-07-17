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
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"maps"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func TestApplyResolvesTypedObjectGVK(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := Reconciler{Client: kube}
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "workload"},
	}
	encoded, err := encodeApplyObject(serviceAccount, scheme)
	if err != nil {
		t.Fatal(err)
	}
	var applyDocument map[string]any
	if err := json.Unmarshal(encoded, &applyDocument); err != nil {
		t.Fatal(err)
	}
	if applyDocument["apiVersion"] != "v1" || applyDocument["kind"] != "ServiceAccount" {
		t.Fatalf("apply GVK = %v, %v", applyDocument["apiVersion"], applyDocument["kind"])
	}
	if err := reconciler.apply(context.Background(), serviceAccount); err != nil {
		t.Fatal(err)
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
		if !ok {
			continue
		}
		if statefulSet.Name == "etcd-vic-0" {
			security := statefulSet.Spec.Template.Spec.SecurityContext
			if security.RunAsUser == nil || *security.RunAsUser == 0 ||
				security.RunAsGroup == nil || *security.RunAsGroup == 0 ||
				security.FSGroup == nil || *security.FSGroup == 0 {
				t.Fatalf("etcd pod does not declare a writable non-root identity: %#v", security)
			}
		}
		if statefulSet.Name != "postgres-vic-0" {
			continue
		}
		if *statefulSet.Spec.Replicas != 1 {
			t.Fatalf("member replicas = %d", *statefulSet.Spec.Replicas)
		}
		if _, found := statefulSet.Spec.Selector.MatchLabels["multisite-postgres.dev/desired-revision"]; found {
			t.Fatal("revision label was included in immutable StatefulSet selector")
		}
		command := statefulSet.Spec.Template.Spec.Containers[0].Command[2]
		if !strings.Contains(command, "10.0.0.10") {
			t.Fatalf("member command does not advertise its LoadBalancer address: %s", command)
		}
	}
}

func TestPVCTemplateUsesClusterDefaultStorageClass(t *testing.T) {
	defaulted := pvcTemplate(nil, nil)
	if defaulted.Spec.StorageClassName != nil {
		t.Fatalf("default StorageClass disabled with %q", *defaulted.Spec.StorageClassName)
	}
	if defaulted.Spec.Resources.Requests.Storage().String() != "1Gi" {
		t.Fatalf("default storage request = %s", defaulted.Spec.Resources.Requests.Storage())
	}

	explicit := pvcTemplate(&api.StorageRequest{
		StorageClassName: "standard", Size: resource.MustParse("2Gi"),
	}, nil)
	if explicit.Spec.StorageClassName == nil || *explicit.Spec.StorageClassName != "standard" {
		t.Fatalf("explicit StorageClass = %v", explicit.Spec.StorageClassName)
	}
	if explicit.Spec.Resources.Requests.Storage().String() != "2Gi" {
		t.Fatalf("explicit storage request = %s", explicit.Spec.Resources.Requests.Storage())
	}
}

func TestRendererStagesEtcdAddressMigration(t *testing.T) {
	desired := plan.SitePlan{
		InstanceUID: "instance", Revision: 2,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders",
			Components: api.SiteComponents{EtcdReplicas: 1},
		},
		MemberAddresses: map[string]string{
			"etcd-vic-0": "10.0.0.9",
			"etcd-nsw-0": "10.0.1.1",
			"etcd-qld-0": "10.0.2.1",
		},
		AddressMigration: &plan.AddressMigrationPlan{
			OperationUID: "migration", Member: "etcd-vic-0",
			OldAddress: "10.0.0.1", NewAddress: "10.0.0.9",
		},
	}
	renderer := Renderer{Images: Images{Etcd: "etcd:3.6"}}
	certificates := renderer.Certificates(desired)
	var addresses []any
	for _, object := range certificates {
		certificate := object.(*unstructured.Unstructured)
		if certificate.GetName() == "etcd-vic-0" {
			var found bool
			addresses, found, _ = unstructured.NestedSlice(certificate.Object, "spec", "ipAddresses")
			if !found {
				t.Fatal("migration certificate has no IP SANs")
			}
		}
	}
	if len(addresses) != 2 {
		t.Fatalf("migration certificate IP SANs = %#v", addresses)
	}
	job, err := renderer.AddressMigrationJob(desired)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("affected site has no migration Job")
	}
	command := job.Spec.Template.Spec.Containers[0].Command[2]
	if strings.Contains(command, "https://10.0.0.9:2379") ||
		!strings.Contains(command, "member update") ||
		!strings.Contains(command, "https://10.0.0.9:2380") {
		t.Fatalf("migration command = %s", command)
	}
}

func TestFillMissingAddressesPreservesSerializedPlan(t *testing.T) {
	planned := map[string]string{"etcd-vic-0": "10.0.0.1"}
	observed := map[string]string{
		"etcd-vic-0": "10.0.0.9",
		"etcd-nsw-0": "10.0.1.1",
	}
	merged := fillMissingAddresses(planned, observed)
	if merged["etcd-vic-0"] != "10.0.0.1" || merged["etcd-nsw-0"] != "10.0.1.1" {
		t.Fatalf("merged addresses = %#v", merged)
	}
}

func TestRendererRollsOnlyCredentialTarget(t *testing.T) {
	desired := plan.SitePlan{
		InstanceUID: "instance", Revision: 4, RuntimeCredentialVersion: 1,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders", Role: api.SiteRoleData,
			Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 2},
			Storage: api.SiteStorage{
				Etcd: &api.StorageRequest{}, Postgres: &api.StorageRequest{},
			},
		},
		Postgres: api.PostgresSpec{Image: "postgres:17"},
		MemberAddresses: map[string]string{
			"etcd-vic-0":     "10.0.0.1",
			"etcd-nsw-0":     "10.0.1.1",
			"etcd-qld-0":     "10.0.2.1",
			"postgres-vic-0": "10.0.0.2", "postgres-vic-1": "10.0.0.3",
		},
		CredentialRotation: &plan.CredentialRotationPlan{
			Version: 2, PreviousVersion: 1, Phase: plan.CredentialRotationPhaseMember,
			TargetMember: "postgres-vic-1",
		},
	}
	objects, err := (Renderer{Images: Images{Etcd: "etcd"}}).Workloads(desired)
	if err != nil {
		t.Fatal(err)
	}
	versions := map[string]string{}
	for _, object := range objects {
		statefulSet, ok := object.(*appsv1.StatefulSet)
		if ok && strings.HasPrefix(statefulSet.Name, "postgres-") {
			versions[statefulSet.Name] =
				statefulSet.Spec.Template.Annotations["multisite-postgres.dev/credential-version"]
		}
	}
	if versions["postgres-vic-0"] != "1" || versions["postgres-vic-1"] != "2" {
		t.Fatalf("credential rollout versions = %#v", versions)
	}
	job := (Renderer{}).CredentialCatalogJob(desired, "postgres-vic-0")
	command := job.Spec.Template.Spec.Containers[0].Command[2]
	if strings.Contains(command, "new-repl") || !strings.Contains(command, `\getenv`) {
		t.Fatalf("catalog command exposes credentials: %s", command)
	}
}

func TestRendererRestoresOnlyTheSeedMember(t *testing.T) {
	desired := plan.SitePlan{
		SiteUID: "site", InstanceUID: "target", Revision: 1,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders-restored", Role: api.SiteRoleData,
			Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 2, PgpoolReplicas: 2},
			Storage: api.SiteStorage{
				Etcd:     &api.StorageRequest{StorageClassName: "standard"},
				Postgres: &api.StorageRequest{StorageClassName: "standard"},
			},
		},
		Postgres: api.PostgresSpec{Image: "postgres:17"},
		MemberAddresses: map[string]string{
			"etcd-vic-0": "10.0.0.1", "etcd-qld-0": "10.0.1.1", "etcd-nsw-0": "10.0.2.1",
			"postgres-vic-0": "10.0.0.10",
			"postgres-vic-1": "10.0.0.11",
		},
		Restore: &plan.RestorePlan{
			OperationUID: "restore", SourceInstanceUID: "source",
			SourceBackup: api.BackupSpec{Repository: api.BackupRepositorySpec{
				Type: "S3", Bucket: "backups", Prefix: "orders",
				Endpoint: "https://s3.example", Region: "ap-southeast-2",
			}},
			TargetTime: time.Date(2026, 7, 16, 2, 15, 0, 0, time.UTC),
			BackupSet:  "20260716-010000F", SeedSite: "vic", SeedMember: "postgres-vic-0",
			Phase: plan.RestorePhaseSeed,
		},
	}
	objects, err := (Renderer{Images: Images{Etcd: "etcd:3.6", Pgpool: "pgpool:4.6"}}).
		Workloads(desired)
	if err != nil {
		t.Fatal(err)
	}
	var postgresMembers, pgpool int
	var patroni, restoreScript string
	for _, object := range objects {
		switch object := object.(type) {
		case *appsv1.StatefulSet:
			if strings.HasPrefix(object.Name, "postgres-") {
				postgresMembers++
			}
		case *appsv1.Deployment:
			pgpool++
		case *corev1.ConfigMap:
			patroni += object.Data["patroni.yml"]
			restoreScript += object.Data["restore.sh"]
		}
	}
	if postgresMembers != 1 || pgpool != 0 {
		t.Fatalf("postgres=%d pgpool=%d", postgresMembers, pgpool)
	}
	for _, expected := range []string{
		"method: pgbackrest", "keep_existing_recovery_conf: true",
	} {
		if !strings.Contains(patroni, expected) {
			t.Fatalf("Patroni restore config is missing %q:\n%s", expected, patroni)
		}
	}
	for _, expected := range []string{
		"--stanza=mspsql-source", "--type=time", "--target-action=promote",
		"--set=20260716-010000F",
	} {
		if !strings.Contains(restoreScript, expected) {
			t.Fatalf("restore script is missing %q:\n%s", expected, restoreScript)
		}
	}
}

func TestReadinessUsesObservedControllerStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
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
			"namespace":  "orders",
			"name":       "postgres-vic-0",
			"generation": int64(3),
		},
		"spec": map[string]any{
			"secretName":  "postgres-vic-0-tls",
			"dnsNames":    []any{"postgres-vic-0", "postgres-vic-0.orders.svc"},
			"ipAddresses": []any{"10.0.0.10"},
			"usages":      []any{"digital signature", "key encipherment", "server auth", "client auth"},
		},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "True", "observedGeneration": int64(3),
		}}},
	}}
	tlsSecret := testTLSSecret(t, "orders", "postgres-vic-0-tls",
		[]string{"postgres-vic-0", "postgres-vic-0.orders.svc"}, []net.IP{net.ParseIP("10.0.0.10")})
	replicas := int32(2)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: "postgres-vic", Generation: 3},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 3,
			ReadyReplicas:      1,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(certificate, tlsSecret, statefulSet).Build()
	reconciler := Reconciler{Client: kube}
	ready, message, err := reconciler.certificatesReady(context.Background(), []client.Object{certificate})
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatalf("certificate was not Ready: %s", message)
	}
	stale := certificate.DeepCopy()
	stale.Object["status"].(map[string]any)["conditions"].([]any)[0].(map[string]any)["observedGeneration"] = int64(2)
	if err := kube.Update(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	ready, message, err = reconciler.certificatesReady(context.Background(), []client.Object{certificate})
	if err != nil {
		t.Fatal(err)
	}
	if ready || !strings.Contains(message, "generation 3") {
		t.Fatalf("stale Certificate status was accepted: ready=%v message=%q", ready, message)
	}
	wrongSAN := stale.DeepCopy()
	wrongSAN.Object["status"].(map[string]any)["conditions"].([]any)[0].(map[string]any)["observedGeneration"] = int64(3)
	wrongSAN.Object["spec"].(map[string]any)["ipAddresses"] = []any{"10.0.0.11"}
	if err := kube.Update(context.Background(), wrongSAN); err != nil {
		t.Fatal(err)
	}
	ready, message, err = reconciler.certificatesReady(context.Background(), []client.Object{certificate})
	if err != nil {
		t.Fatal(err)
	}
	if ready || !strings.Contains(message, `required IP SAN "10.0.0.11" is absent`) {
		t.Fatalf("wrong Certificate SAN was accepted: ready=%v message=%q", ready, message)
	}
	ready, message, err = reconciler.workloadsReady(context.Background(), []client.Object{statefulSet})
	if err != nil {
		t.Fatal(err)
	}
	if ready || message == "" {
		t.Fatalf("partially available StatefulSet was reported Ready: %q", message)
	}
}

func testTLSSecret(t *testing.T, namespace, name string, dnsNames []string, ipAddresses []net.IP) *corev1.Secret {
	t.Helper()
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: name},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		DNSNames: dnsNames, IPAddresses: ipAddresses,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Data: map[string][]byte{
		corev1.TLSCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		"ca.crt":          pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
	}}
}

func TestDiscoverInventoryReportsStorageAndIssuers(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	issuerGVK := schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Issuer"}
	clusterIssuerGVK := schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "ClusterIssuer"}
	snapshotClassGVK := schema.GroupVersionKind{
		Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshotClass",
	}
	scheme.AddKnownTypeWithName(issuerGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(issuerGVK.GroupVersion().WithKind("IssuerList"), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(clusterIssuerGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(clusterIssuerGVK.GroupVersion().WithKind("ClusterIssuerList"),
		&unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(snapshotClassGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(snapshotClassGVK.GroupVersion().WithKind("VolumeSnapshotClassList"),
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
	snapshotClass := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass",
		"metadata": map[string]any{"name": "nvme-snapshots"},
		"driver":   "csi.example", "deletionPolicy": "Retain",
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(storageClass, issuer, snapshotClass).Build()
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
	if len(inventory.VolumeSnapshotClasses) != 1 ||
		inventory.VolumeSnapshotClasses[0].Driver != "csi.example" {
		t.Fatalf("snapshot inventory = %#v", inventory.VolumeSnapshotClasses)
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
		Postgres:      api.PostgresSpec{Image: "percona-postgres:17", SynchronousStandbyCount: 1},
		TDEKeyCreator: true,
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
	var audit *batchv1.Job
	for _, object := range objects {
		switch object := object.(type) {
		case *corev1.ConfigMap:
			patroniConfig += object.Data["patroni.yml"]
			bootstrapScript += object.Data["tde-bootstrap.sh"]
		case *appsv1.StatefulSet:
			if strings.HasPrefix(object.Name, "postgres-") {
				postgres = object
			}
		case *batchv1.Job:
			audit = object
		}
	}
	for _, expected := range []string{
		"shared_preload_libraries=pg_tde", "pg_tde_basebackup", "post_init: /operator/tde-bootstrap.sh",
		"verify_client: optional", "certfile: /postgres-tls/tls.crt",
		"synchronous_mode_strict: true", "hostssl all all 0.0.0.0/0 scram-sha-256",
	} {
		if !strings.Contains(patroniConfig, expected) {
			t.Fatalf("Patroni config is missing %q:\n%s", expected, patroniConfig)
		}
	}
	for _, expected := range []string{
		"pg_tde_add_global_key_provider_vault_v2", "pg_tde_change_global_key_provider_vault_v2",
		"pg_tde_create_key_using_global_key_provider", "if true; then",
		"pg_tde.enforce_encryption", "template1",
	} {
		if !strings.Contains(bootstrapScript, expected) {
			t.Fatalf("TDE bootstrap is missing %q", expected)
		}
	}
	desired.TDEKeyCreator = false
	followerScript := renderer.tdeBootstrapConfig(desired, nil).Data["tde-bootstrap.sh"]
	if !strings.Contains(followerScript, "if false; then") ||
		!strings.Contains(followerScript, "for attempt in $(seq 1 60)") {
		t.Fatalf("TDE follower does not wait for the shared key:\n%s", followerScript)
	}
	if postgres == nil || !hasVolume(postgres.Spec.Template.Spec.Volumes, "pg-tde-vault") {
		t.Fatal("PostgreSQL member does not mount the TDE Vault token")
	}
	if audit == nil {
		t.Fatal("TDE acceptance audit Job was not rendered")
	}
	auditCommand := audit.Spec.Template.Spec.Containers[0].Command[2]
	for _, expected := range []string{
		"pg_tde_verify_key()", "default_table_access_method", "access_method.amname <> 'tde_heap'",
	} {
		if !strings.Contains(auditCommand, expected) {
			t.Fatalf("TDE audit is missing %q:\n%s", expected, auditCommand)
		}
	}
}

func TestRendererConfiguresPgBackRestDataPlane(t *testing.T) {
	desired := plan.SitePlan{
		SiteUID: "site", InstanceUID: "instance", Revision: 1,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders", Role: api.SiteRoleData,
			Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 1},
			Storage: api.SiteStorage{
				Etcd: &api.StorageRequest{}, Postgres: &api.StorageRequest{},
			},
			Certificates: api.SiteCertificateSpec{
				PostgresIssuerRef: api.IssuerReference{Name: "postgres-ca"},
				BackupIssuerRef:   api.IssuerReference{Name: "backup-ca"},
			},
		},
		Postgres: api.PostgresSpec{Image: "percona-postgres:17"},
		Backup: &api.BackupSpec{Repository: api.BackupRepositorySpec{
			Type: "S3", Bucket: "backups", Prefix: "orders", Region: "ap-southeast-2",
			Endpoint: "https://minio.example:9443", URIStyle: "path",
			CABundleSecretRef: &api.SecretKeyReference{Name: "minio-ca"},
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
	var config strings.Builder
	var postgres *appsv1.StatefulSet
	for _, object := range objects {
		switch object := object.(type) {
		case *corev1.ConfigMap:
			config.WriteString(object.Data["pgbackrest.conf"])
			config.WriteString(object.Data["patroni.yml"])
		case *appsv1.StatefulSet:
			if strings.HasPrefix(object.Name, "postgres-") {
				postgres = object
			}
		}
	}
	for _, expected := range []string{
		"repo1-cipher-type=aes-256-cbc", "repo1-storage-ca-file=/repository/ca.crt",
		"repo1-s3-endpoint=minio.example", "repo1-storage-port=9443", "archive-push %p",
		"tls-server-ca-file=/pgbackrest-tls/ca.crt",
	} {
		if !strings.Contains(config.String(), expected) {
			t.Fatalf("pgBackRest config is missing %q:\n%s", expected, config.String())
		}
	}
	if strings.Contains(config.String(), "s3-secret-key") {
		t.Fatal("repository credentials were rendered into a ConfigMap")
	}
	if postgres == nil || len(postgres.Spec.Template.Spec.InitContainers) != 1 ||
		!hasContainer(postgres.Spec.Template.Spec.Containers, "pgbackrest") ||
		!hasVolume(postgres.Spec.Template.Spec.Volumes, "pgbackrest-tls") {
		t.Fatalf("PostgreSQL pgBackRest pod layout = %#v", postgres)
	}
	certificates := renderer.Certificates(desired)
	foundClient := false
	foundServer := false
	for _, certificate := range certificates {
		foundClient = foundClient || certificate.GetName() == "pgbackrest-client"
		foundServer = foundServer || certificate.GetName() == "postgres-vic-0-pgbackrest"
	}
	if !foundClient || !foundServer {
		t.Fatal("dedicated pgBackRest certificates were not rendered")
	}
}

func TestRendererRollsOnlySelectedPostgresMember(t *testing.T) {
	desired := plan.SitePlan{
		SiteUID: "site", InstanceUID: "instance", Revision: 2,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders", Role: api.SiteRoleData,
			Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 2},
			Storage: api.SiteStorage{
				Etcd: &api.StorageRequest{}, Postgres: &api.StorageRequest{},
			},
		},
		Postgres: api.PostgresSpec{Image: "postgres:17.1"},
		Upgrade: &plan.UpgradePlan{
			OperationUID: "upgrade", TargetImage: "postgres:17.2",
			TargetMember: "postgres-vic-1", Phase: plan.UpgradePhaseMember,
		},
		MemberAddresses: map[string]string{
			"etcd-vic-0": "10.0.0.1", "etcd-qld-0": "10.0.1.1", "etcd-nsw-0": "10.0.2.1",
			"postgres-vic-0": "10.0.0.10", "postgres-vic-1": "10.0.0.11",
		},
	}
	objects, err := (Renderer{Images: Images{Etcd: "etcd", Pgpool: "pgpool"}}).Workloads(desired)
	if err != nil {
		t.Fatal(err)
	}
	images := map[string]string{}
	for _, object := range objects {
		statefulSet, ok := object.(*appsv1.StatefulSet)
		if ok && strings.HasPrefix(statefulSet.Name, "postgres-") {
			images[statefulSet.Name] = statefulSet.Spec.Template.Spec.Containers[0].Image
		}
	}
	if images["postgres-vic-0"] != "postgres:17.1" ||
		images["postgres-vic-1"] != "postgres:17.2" {
		t.Fatalf("member images = %#v", images)
	}
}

func TestRendererMajorUpgradeControlsServiceRestorationBoundary(t *testing.T) {
	desired := plan.SitePlan{
		SiteUID: "site", InstanceUID: "instance", Revision: 2,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders", Role: api.SiteRoleData,
			Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 2, PgpoolReplicas: 2},
			Storage: api.SiteStorage{
				Etcd: &api.StorageRequest{}, Postgres: &api.StorageRequest{},
			},
		},
		Postgres: api.PostgresSpec{Image: "postgres:17"},
		MemberAddresses: map[string]string{
			"etcd-vic-0": "10.0.0.1", "etcd-nsw-0": "10.0.1.1", "etcd-qld-0": "10.0.2.1",
			"postgres-vic-0": "10.0.0.10", "postgres-vic-1": "10.0.0.11",
		},
		MajorUpgrade: &plan.MajorUpgradePlan{
			OperationUID: "upgrade", Primary: "postgres-vic-0", SourceMajor: 17,
			TargetMajor: 18, TargetImage: "postgres:18", UpgradeImage: "upgrade@sha256:abc",
		},
	}
	renderer := Renderer{Images: Images{Etcd: "etcd", Pgpool: "pgpool"}}

	desired.MajorUpgrade.Phase = plan.MajorUpgradePhaseDrain
	assertMajorWorkloadState(t, renderer, desired, map[string]workloadExpectation{
		"postgres-vic-0": {replicas: 1, image: "postgres:17"},
		"postgres-vic-1": {replicas: 1, image: "postgres:17"},
		"pgpool-vic":     {replicas: 0},
	})

	desired.MajorUpgrade.Phase = plan.MajorUpgradePhaseStop
	assertMajorWorkloadState(t, renderer, desired, map[string]workloadExpectation{
		"postgres-vic-0": {replicas: 0, image: "postgres:17"},
		"postgres-vic-1": {replicas: 0, image: "postgres:17"},
		"pgpool-vic":     {replicas: 0},
	})

	desired.MajorUpgrade.Phase = plan.MajorUpgradePhaseStartPrimary
	assertMajorWorkloadState(t, renderer, desired, map[string]workloadExpectation{
		"postgres-vic-0": {replicas: 1, image: "postgres:18"},
		"postgres-vic-1": {replicas: 0, image: "postgres:17"},
		"pgpool-vic":     {replicas: 0},
	})

	desired.MajorUpgrade.Phase = plan.MajorUpgradePhaseReplicas
	assertMajorWorkloadState(t, renderer, desired, map[string]workloadExpectation{
		"postgres-vic-0": {replicas: 1, image: "postgres:18"},
		"postgres-vic-1": {replicas: 1, image: "postgres:18"},
		"pgpool-vic":     {replicas: 2},
	})
}

type workloadExpectation struct {
	replicas int32
	image    string
}

func assertMajorWorkloadState(t *testing.T, renderer Renderer, desired plan.SitePlan,
	expected map[string]workloadExpectation,
) {
	t.Helper()
	objects, err := renderer.Workloads(desired)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range objects {
		var replicas int32
		var image string
		switch object := object.(type) {
		case *appsv1.StatefulSet:
			if !strings.HasPrefix(object.Name, "postgres-") {
				continue
			}
			replicas = *object.Spec.Replicas
			image = object.Spec.Template.Spec.Containers[0].Image
		case *appsv1.Deployment:
			if !strings.HasPrefix(object.Name, "pgpool-") {
				continue
			}
			replicas = *object.Spec.Replicas
		default:
			continue
		}
		want, ok := expected[object.GetName()]
		if !ok {
			continue
		}
		if replicas != want.replicas || want.image != "" && image != want.image {
			t.Errorf("%s replicas/image = %d/%s, want %d/%s",
				object.GetName(), replicas, image, want.replicas, want.image)
		}
		delete(expected, object.GetName())
	}
	for name := range expected {
		t.Errorf("%s was not rendered", name)
	}
}

func TestMajorUpgradeJobsUsePinnedToolingWithoutRetry(t *testing.T) {
	desired := plan.SitePlan{
		InstanceUID: "instance",
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders",
		},
		MajorUpgrade: &plan.MajorUpgradePlan{
			OperationUID: "operation", Primary: "postgres-vic-1", SourceMajor: 17,
			TargetMajor: 18, UpgradeImage: "upgrade@sha256:abc",
		},
	}
	renderer := Renderer{}
	for _, enabled := range []bool{false, true} {
		desired.TDE.Enabled = enabled
		job := renderer.MajorUpgradeJob(desired)
		command := job.Spec.Template.Spec.Containers[0].Command[2]
		expected := "/opt/mspsql/new/bin/pg_upgrade"
		if enabled {
			expected = "pg_tde_upgrade"
		}
		if !strings.Contains(command, expected) {
			t.Fatalf("TDE=%t upgrade command is missing %q:\n%s", enabled, expected, command)
		}
		if enabled && (!strings.Contains(command, "shared_preload_libraries=pg_tde") ||
			!hasVolume(job.Spec.Template.Spec.Volumes, "pg-tde-vault")) {
			t.Fatalf("TDE upgrade does not preload pg_tde with Vault access: %#v", job)
		}
		if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
			t.Fatalf("destructive upgrade backoffLimit = %v", job.Spec.BackoffLimit)
		}
		claim := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
		if claim != "data-postgres-vic-1-0" {
			t.Fatalf("upgrade PVC = %q", claim)
		}
	}
	stanzaJob := renderer.MajorStanzaUpgradeJob(desired)
	stanzaCommand := stanzaJob.Spec.Template.Spec.Containers[0].Command[2]
	if !strings.Contains(stanzaCommand, "--no-online stanza-upgrade") {
		t.Fatalf("stanza upgrade command = %q", stanzaCommand)
	}
	acceptanceJob := renderer.MajorAcceptanceJob(desired)
	acceptanceCommand := acceptanceJob.Spec.Template.Spec.Containers[0].Command[2]
	for _, expected := range []string{"SHOW server_version_num", "CREATE SCHEMA", "write_test", "DROP SCHEMA"} {
		if !strings.Contains(acceptanceCommand, expected) {
			t.Fatalf("acceptance command is missing %q:\n%s", expected, acceptanceCommand)
		}
	}
}

func TestMajorUpgradeRollbackArtifactsAreRetainedAndRestorable(t *testing.T) {
	generatedAt := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	desired := plan.SitePlan{
		InstanceUID: "instance", GeneratedAt: generatedAt,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders",
			Storage: api.SiteStorage{Postgres: &api.StorageRequest{
				StorageClassName: "fast",
			}},
		},
		MajorUpgrade: &plan.MajorUpgradePlan{RollbackRetention: 48 * time.Hour},
	}
	snapshot := volumeSnapshot(desired, "rollback-a", "data-a", "snapshots")
	expiresAt := snapshot.GetAnnotations()["multisite-postgres.dev/expires-at"]
	if expiresAt != generatedAt.Add(48*time.Hour).Format(time.RFC3339) {
		t.Fatalf("snapshot expiry = %q", expiresAt)
	}
	restore := rollbackRestorePVC(desired, "data-a", "rollback-a", api.StorageRollbackPolicy{
		Strategy: "VolumeSnapshot",
	})
	if restore.Spec.DataSource == nil || restore.Spec.DataSource.Kind != "VolumeSnapshot" ||
		restore.Spec.DataSource.APIGroup == nil ||
		*restore.Spec.DataSource.APIGroup != "snapshot.storage.k8s.io" {
		t.Fatalf("snapshot restore dataSource = %#v", restore.Spec.DataSource)
	}
	clone := rollbackRestorePVC(desired, "data-a", "rollback-a", api.StorageRollbackPolicy{
		Strategy: "PVCClone",
	})
	if clone.Spec.DataSource == nil || clone.Spec.DataSource.Kind != "PersistentVolumeClaim" ||
		clone.Spec.DataSource.APIGroup != nil {
		t.Fatalf("clone restore dataSource = %#v", clone.Spec.DataSource)
	}
}

func hasContainer(containers []corev1.Container, name string) bool {
	for _, container := range containers {
		if container.Name == name {
			return true
		}
	}
	return false
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}
