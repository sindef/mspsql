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

package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func TestSiteRegistrationIssuesHashedToken(t *testing.T) {
	scheme := testScheme(t)
	site := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{Name: "vic", UID: types.UID("site-uid")},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.SiteRegistration{}).WithObjects(site).Build()
	now := time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC)
	reconciler := SiteRegistrationReconciler{
		Client: kube, Scheme: scheme, SystemNamespace: "system",
		RegistrationPublicURL: "https://hub.example", Now: func() time.Time { return now },
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "vic"},
	}); err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: "system", Name: "registration-site-uid",
	}, &secret); err != nil {
		t.Fatal(err)
	}
	if len(secret.Data["sha256"]) != 32 {
		t.Fatalf("stored token hash length = %d", len(secret.Data["sha256"]))
	}
	var signingKey corev1.Secret
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: "system", Name: signingKeySecretName,
	}, &signingKey); err != nil {
		t.Fatal(err)
	}
	if len(signingKey.Data["privateKey"]) == 0 ||
		len(signingKey.Data["publicKey"]) == 0 {
		t.Fatal("directive signing key was not initialized during site registration")
	}
	var updated api.SiteRegistration
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "vic"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.RegistrationURL == "" {
		t.Fatal("registration URL was not published")
	}
}

func TestInstanceIssuesOneSignedPlanPerSite(t *testing.T) {
	scheme := testScheme(t)
	issuer := api.IssuerReference{Name: "issuer", Kind: "ClusterIssuer", Group: "cert-manager.io"}
	registration := func(name string) *api.SiteRegistration {
		return &api.SiteRegistration{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")},
			Spec: api.SiteRegistrationSpec{
				PermittedStorageClasses: api.StorageClassPolicy{
					Etcd: []string{"standard"}, Postgres: []string{"standard"},
				},
				PermittedIssuers: api.IssuerPolicy{
					Etcd: []api.IssuerReference{issuer}, Postgres: []api.IssuerReference{issuer},
					Pgpool: []api.IssuerReference{issuer},
				},
				MetallbAddressPools: []string{"default"},
			},
			Status: api.SiteRegistrationStatus{
				Phase:                    "Connected",
				DiscoveredStorageClasses: []api.StorageClassInventory{{Name: "standard"}},
			},
		}
	}
	storage := func() *api.StorageRequest {
		request := &api.StorageRequest{StorageClassName: "standard"}
		request.Size.Set(1)
		return request
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance-uid"), Generation: 1,
		},
		Spec: api.MultiSitePostgresSpec{
			Postgres: api.PostgresSpec{MajorVersion: 17, Image: "postgres:17", SynchronousStandbyCount: 1},
			Sites: []api.PostgresSiteSpec{
				{
					Name: "vic", SiteRegistrationRef: "vic", Namespace: "orders",
					Role:         api.SiteRoleData,
					Components:   api.SiteComponents{EtcdReplicas: 2, PostgresReplicas: 1, PgpoolReplicas: 1},
					Storage:      api.SiteStorage{Etcd: storage(), Postgres: storage()},
					LoadBalancer: &api.LoadBalancerSpec{AddressPool: "default"},
					VaultAuth:    &api.VaultAuthSpec{Address: "https://vault", AuthMount: "k8s", AuthRole: "vic"},
					Certificates: api.SiteCertificateSpec{
						EtcdIssuerRef: issuer, PostgresIssuerRef: issuer, PgpoolIssuerRef: issuer,
					},
				},
				{
					Name: "nsw", SiteRegistrationRef: "nsw", Namespace: "orders",
					Role:         api.SiteRoleData,
					Components:   api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 1, PgpoolReplicas: 1},
					Storage:      api.SiteStorage{Etcd: storage(), Postgres: storage()},
					LoadBalancer: &api.LoadBalancerSpec{AddressPool: "default"},
					VaultAuth:    &api.VaultAuthSpec{Address: "https://vault", AuthMount: "k8s", AuthRole: "nsw"},
					Certificates: api.SiteCertificateSpec{
						EtcdIssuerRef: issuer, PostgresIssuerRef: issuer, PgpoolIssuerRef: issuer,
					},
				},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}).
		WithObjects(instance, registration("vic"), registration("nsw")).Build()
	reconciler := MultiSitePostgresReconciler{
		Client: kube, Scheme: scheme, SystemNamespace: "system",
		Now: func() time.Time { return time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC) },
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders"}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	for _, site := range []string{"vic", "nsw"} {
		var configMap corev1.ConfigMap
		if err := kube.Get(context.Background(), types.NamespacedName{
			Namespace: "platform", Name: "mspsql-plan-orders-" + site,
		}, &configMap); err != nil {
			t.Fatal(err)
		}
		if configMap.Data["envelope.json"] == "" {
			t.Fatalf("site %s plan is empty", site)
		}
	}
	var active api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &active); err != nil {
		t.Fatal(err)
	}
	if err := kube.Delete(context.Background(), &active); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var deletionPlan corev1.ConfigMap
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: "platform", Name: "mspsql-plan-orders-vic",
	}, &deletionPlan); err != nil {
		t.Fatal(err)
	}
	var envelope plan.Envelope
	if err := json.Unmarshal([]byte(deletionPlan.Data["envelope.json"]), &envelope); err != nil {
		t.Fatal(err)
	}
	var desired plan.SitePlan
	if err := json.Unmarshal(envelope.Plan, &desired); err != nil {
		t.Fatal(err)
	}
	if desired.Deletion == nil || desired.Deletion.Policy != api.DeletionPolicyRetain {
		t.Fatalf("deletion plan = %#v", desired.Deletion)
	}
}

func TestPlanFingerprintIgnoresEmptyObservedAddresses(t *testing.T) {
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Spec: api.MultiSitePostgresSpec{
			Sites: []api.PostgresSiteSpec{
				{Name: "vic"},
				{Name: "nsw"},
			},
		},
	}
	before, err := planFingerprint(instance)
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.Sites = []api.SiteRevisionStatus{
		{Name: "vic"},
		{Name: "nsw", Addresses: map[string]string{}},
	}
	after, err := planFingerprint(instance)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("empty observed addresses changed fingerprint: %s != %s", before, after)
	}
}

func TestAggregateTopologyRequiresCurrentConsensus(t *testing.T) {
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	observed := metav1.NewTime(now.Add(-time.Minute))
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Generation: 3},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", Role: api.SiteRoleData},
			{Name: "qld", Role: api.SiteRoleData},
			{Name: "nsw", Role: api.SiteRoleWitness},
		}},
		Status: api.MultiSitePostgresStatus{Sites: []api.SiteRevisionStatus{
			{
				Name: "vic", Primary: "postgres-vic-0",
				SynchronousStandbys: []string{"postgres-qld-0"}, TopologyObservedAt: &observed,
			},
			{
				Name: "qld", Primary: "postgres-vic-0",
				SynchronousStandbys: []string{"postgres-qld-0"}, TopologyObservedAt: &observed,
			},
		}},
	}
	aggregateTopology(instance, now)
	if instance.Status.Primary != "postgres-vic-0" ||
		len(instance.Status.SynchronousStandbys) != 1 ||
		!conditionTrue(instance.Status.Conditions, "TopologyReady") {
		t.Fatalf("status = %#v", instance.Status)
	}

	instance.Status.Sites[1].Primary = "postgres-qld-0"
	aggregateTopology(instance, now)
	if instance.Status.Primary != "" || conditionTrue(instance.Status.Conditions, "TopologyReady") {
		t.Fatalf("conflicting topology was accepted: %#v", instance.Status)
	}
}

func TestBackupSchedulerIssuesOneCatchUpDirective(t *testing.T) {
	scheme := testScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube, Scheme: scheme}
	now := time.Date(2026, 7, 16, 9, 0, 0, 0, time.UTC)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "database-platform", Name: "orders", UID: types.UID("instance"),
		},
		Spec: api.MultiSitePostgresSpec{Backup: &api.BackupSpec{
			Schedules: api.BackupSchedules{Full: "0 * * * *", Timezone: "UTC"},
		}},
	}
	if _, err := reconciler.reconcileBackupSchedules(context.Background(), instance, now, true); err != nil {
		t.Fatal(err)
	}
	if instance.Status.BackupSchedules[0].NextScheduledAt == nil {
		t.Fatal("initial next backup was not recorded")
	}
	past := metav1.NewTime(now.Add(-3 * time.Hour))
	instance.Status.BackupSchedules[0].NextScheduledAt = &past
	if _, err := reconciler.reconcileBackupSchedules(context.Background(), instance, now, true); err != nil {
		t.Fatal(err)
	}
	var directives corev1.ConfigMapList
	if err := kube.List(context.Background(), &directives, client.MatchingLabels{
		"multisite-postgres.dev/directive": "Backup",
	}); err != nil {
		t.Fatal(err)
	}
	if len(directives.Items) != 1 {
		t.Fatalf("backup directives = %d", len(directives.Items))
	}
	if instance.Status.BackupSchedules[0].LastScheduledAt == nil ||
		!instance.Status.BackupSchedules[0].NextScheduledAt.After(now) {
		t.Fatalf("backup schedule status = %#v", instance.Status.BackupSchedules[0])
	}
}

func TestInstanceSecretClaimsAreExclusive(t *testing.T) {
	backup := func(prefix, path string) *api.BackupSpec {
		return &api.BackupSpec{Repository: api.BackupRepositorySpec{
			Type: "S3", Bucket: "backups", Prefix: prefix,
			CredentialVaultRef: api.VaultSecretReference{Mount: "secret", Path: path},
		}}
	}
	if !backupClaimsConflict(backup("orders", "postgres/orders"), backup("/orders/", "postgres/other")) {
		t.Fatal("equivalent backup prefixes were not rejected")
	}
	if !backupClaimsConflict(backup("orders-a", "postgres/shared"), backup("orders-b", "postgres/shared")) {
		t.Fatal("shared backup credentials were not rejected")
	}
	if backupClaimsConflict(backup("orders-a", "postgres/a"), backup("orders-b", "postgres/b")) {
		t.Fatal("independent backup claims conflict")
	}
	tde := func(path string) api.TDESpec {
		return api.TDESpec{Enabled: true, Vault: &api.TDEVaultSpec{
			KVMount: "tde", KeyPath: path, ProviderName: "vault", PrincipalKeyName: "default",
		}}
	}
	if !tdeClaimsConflict(tde("postgres/orders"), tde("postgres/orders")) {
		t.Fatal("shared TDE identity was not rejected")
	}
	if tdeClaimsConflict(tde("postgres/orders"), tde("postgres/reporting")) {
		t.Fatal("independent TDE identities conflict")
	}
}

func TestRestoreCreatesIsolatedTargetAndAdvancesAfterPromotion(t *testing.T) {
	scheme := testScheme(t)
	now := time.Date(2026, 7, 16, 3, 0, 0, 0, time.UTC)
	window := metav1.NewTime(now.Add(-24 * time.Hour))
	source := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("source-uid"),
		},
		Spec: api.MultiSitePostgresSpec{
			Postgres: api.PostgresSpec{
				MajorVersion: 17, Image: "postgres:17", SynchronousStandbyCount: 1,
			},
			Sites: []api.PostgresSiteSpec{
				{
					Name: "vic", SiteRegistrationRef: "vic", Namespace: "orders",
					Role: api.SiteRoleData, PrimaryPreference: 100,
					Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 1},
				},
				{
					Name: "qld", SiteRegistrationRef: "qld", Namespace: "orders",
					Role: api.SiteRoleData, PrimaryPreference: 50,
					Components: api.SiteComponents{EtcdReplicas: 1, PostgresReplicas: 1},
				},
			},
			Backup: &api.BackupSpec{Repository: api.BackupRepositorySpec{
				Type: "S3", Bucket: "backups", Prefix: "orders",
				CredentialVaultRef: api.VaultSecretReference{Mount: "secret", Path: "orders/backup"},
			}},
		},
		Status: api.MultiSitePostgresStatus{
			RecoveryWindowStart: &window,
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
				{Type: "RecoveryWindowAvailable", Status: metav1.ConditionTrue},
			},
		},
	}
	restore := &api.PostgresRestore{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-restore", UID: types.UID("restore-uid"),
		},
		Spec: api.PostgresRestoreSpec{
			SourceInstanceRef: "orders", TargetInstanceRef: "orders-recovered",
			TargetTime: metav1.NewTime(now.Add(-time.Hour)),
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}, &api.PostgresRestore{}).
		WithObjects(source, restore).Build()
	reconciler := PostgresRestoreReconciler{
		Client: kube, Scheme: scheme, Now: func() time.Time { return now },
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: restore.Namespace, Name: restore.Name,
	}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var target api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKey{
		Namespace: "platform", Name: "orders-recovered",
	}, &target); err != nil {
		t.Fatal(err)
	}
	if target.Annotations[restorePhaseAnnotation] != string(plan.RestorePhaseSeed) ||
		target.Spec.Sites[0].Namespace != "orders-recovered-vic" ||
		target.Spec.Backup != nil {
		t.Fatalf("restore target = %#v", target)
	}

	target.Status.Primary = "postgres-vic-0"
	target.Status.Conditions = []metav1.Condition{
		{Type: "TopologyReady", Status: metav1.ConditionTrue},
	}
	if err := kube.Status().Update(context.Background(), &target); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(&target), &target); err != nil {
		t.Fatal(err)
	}
	if target.Annotations[restorePhaseAnnotation] != string(plan.RestorePhaseReplicas) {
		t.Fatalf("restore phase = %q", target.Annotations[restorePhaseAnnotation])
	}
}
