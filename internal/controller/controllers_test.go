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
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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

func TestSiteRegistrationRevocationRemovesCredentials(t *testing.T) {
	scheme := testScheme(t)
	site := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{Name: "vic", UID: types.UID("site-uid"), Generation: 2},
		Spec:       api.SiteRegistrationSpec{Revoked: true},
		Status: api.SiteRegistrationStatus{
			ClusterUID: "cluster-uid", RegistrationURL: "https://hub.example/token/registration.yaml",
		},
	}
	token := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "registration-site-uid",
	}}
	peer := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "wireguard-peer-site-uid",
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.SiteRegistration{}).WithObjects(site, token, peer).Build()
	reconciler := SiteRegistrationReconciler{
		Client: kube, Scheme: scheme, SystemNamespace: "system",
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "vic"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{token.Name, peer.Name} {
		var secret corev1.Secret
		err := kube.Get(context.Background(), types.NamespacedName{Namespace: "system", Name: name}, &secret)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("%s still exists: %v", name, err)
		}
	}
	var updated api.SiteRegistration
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "vic"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != "Revoked" || updated.Status.RegistrationURL != "" {
		t.Fatalf("revoked status = %#v", updated.Status)
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, "Connected")
	if condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != "AdministrativelyRevoked" {
		t.Fatalf("Connected condition = %#v", condition)
	}
}

func TestRevokedRegistrationFailsSitePolicy(t *testing.T) {
	err := validateSitePolicy(api.PostgresSiteSpec{Name: "vic"}, &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{Name: "production-vic"},
		Spec:       api.SiteRegistrationSpec{Revoked: true},
	})
	if err == nil || !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("policy error = %v", err)
	}
}

func TestRegistrationStatusEnqueuesReferencingInstances(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{{
			Name: "vic", SiteRegistrationRef: "production-vic",
		}}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube}
	requests := reconciler.instancesForRegistration(context.Background(),
		&api.SiteRegistration{ObjectMeta: metav1.ObjectMeta{Name: "production-vic"}})
	if len(requests) != 1 || requests[0].Name != "orders" || requests[0].Namespace != "platform" {
		t.Fatalf("requests = %#v", requests)
	}
}

func TestMergeReconciledStatusPreservesAgentObservations(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC))
	current := api.MultiSitePostgresStatus{
		ActiveRevision: 2,
		Sites: []api.SiteRevisionStatus{{
			Name: "vic", DesiredRevision: 1, AppliedRevision: 2, Phase: "Ready",
			Addresses:         map[string]string{"postgres-vic-0": "10.0.0.10"},
			LastHeartbeatTime: &now,
		}},
		LastBackupTime: &now,
		Conditions: []metav1.Condition{{
			Type: "BackupReady", Status: metav1.ConditionTrue, Reason: "BackupVerified",
		}},
	}
	desired := api.MultiSitePostgresStatus{
		ObservedGeneration: 4, ActiveRevision: 2, PlanFingerprint: "fingerprint",
		Phase: "Reconciling",
		Sites: []api.SiteRevisionStatus{{
			Name: "vic", SiteRegistrationRef: "production-vic", DesiredRevision: 2,
			Phase: "PlanIssued",
		}},
		Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: "PlansIssued",
		}},
	}
	mergeReconciledStatus(&current, &desired)
	if current.Sites[0].AppliedRevision != 2 || current.Sites[0].Phase != "Ready" ||
		current.Sites[0].Addresses["postgres-vic-0"] != "10.0.0.10" {
		t.Fatalf("agent site observation was overwritten: %#v", current.Sites[0])
	}
	if current.Sites[0].DesiredRevision != 2 ||
		current.Sites[0].SiteRegistrationRef != "production-vic" {
		t.Fatalf("controller site state was not merged: %#v", current.Sites[0])
	}
	if current.LastBackupTime == nil ||
		meta.FindStatusCondition(current.Conditions, "BackupReady") == nil ||
		meta.FindStatusCondition(current.Conditions, "Ready") == nil {
		t.Fatalf("concurrent status fields were lost: %#v", current)
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
	before, err := planFingerprint(instance, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.Sites = []api.SiteRevisionStatus{
		{Name: "vic"},
		{Name: "nsw", Addresses: map[string]string{}},
	}
	after, err := planFingerprint(instance, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("empty observed addresses changed fingerprint: %s != %s", before, after)
	}
}

func TestAddressPlanSerializesObservedChanges(t *testing.T) {
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance"),
		},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", Components: api.SiteComponents{EtcdReplicas: 1}},
			{Name: "nsw", Components: api.SiteComponents{EtcdReplicas: 1}},
			{Name: "qld", Components: api.SiteComponents{EtcdReplicas: 1}},
		}},
		Status: api.MultiSitePostgresStatus{Sites: []api.SiteRevisionStatus{
			{Name: "vic", Addresses: map[string]string{"etcd-vic-0": "10.0.0.9"}},
			{Name: "nsw", Addresses: map[string]string{"etcd-nsw-0": "10.0.1.9"}},
			{Name: "qld", Addresses: map[string]string{"etcd-qld-0": "10.0.2.1"}},
		}},
	}
	active := plan.SitePlan{MemberAddresses: map[string]string{
		"etcd-vic-0": "10.0.0.1",
		"etcd-nsw-0": "10.0.1.1",
		"etcd-qld-0": "10.0.2.1",
	}}
	rawPlan, err := json.Marshal(active)
	if err != nil {
		t.Fatal(err)
	}
	rawEnvelope, err := json.Marshal(plan.Envelope{Plan: rawPlan})
	if err != nil {
		t.Fatal(err)
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "mspsql-plan-orders-vic"},
		Data:       map[string]string{"envelope.json": string(rawEnvelope)},
	}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(configMap).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube}
	addresses, candidates, migration, err := reconciler.addressPlan(context.Background(), instance)
	if err != nil {
		t.Fatal(err)
	}
	if migration == nil || migration.Member != "etcd-nsw-0" {
		t.Fatalf("migration = %#v", migration)
	}
	if addresses["etcd-nsw-0"] != "10.0.1.9" || addresses["etcd-vic-0"] != "10.0.0.1" {
		t.Fatalf("serialized addresses = %#v", addresses)
	}
	if candidates["etcd-vic-0"] != "10.0.0.9" {
		t.Fatalf("certificate candidates = %#v", candidates)
	}
}

func TestCredentialRotationUsesCatalogThenStandby(t *testing.T) {
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", Role: api.SiteRoleData, Components: api.SiteComponents{PostgresReplicas: 1}},
			{Name: "qld", Role: api.SiteRoleData, Components: api.SiteComponents{PostgresReplicas: 1}},
		}},
		Status: api.MultiSitePostgresStatus{
			Primary:             "postgres-qld-0",
			SynchronousStandbys: []string{"postgres-vic-0"},
			Sites: []api.SiteRevisionStatus{
				{Name: "vic", Conditions: []metav1.Condition{{
					Type: "CredentialRotationPending", Status: metav1.ConditionTrue, Message: "2",
				}, {
					Type: "PostgresCredentialsActive", Status: metav1.ConditionTrue, Message: "1",
				}}},
				{Name: "qld", Conditions: []metav1.Condition{{
					Type: "CredentialRotationPending", Status: metav1.ConditionTrue, Message: "2",
				}, {
					Type: "PostgresCredentialsActive", Status: metav1.ConditionTrue, Message: "1",
				}}},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube}
	rotation, err := reconciler.credentialRotationPlan(context.Background(), instance, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rotation == nil || rotation.Phase != plan.CredentialRotationPhaseCatalog {
		t.Fatalf("catalog rotation = %#v", rotation)
	}
	instance.Status.Sites[1].Conditions = append(instance.Status.Sites[1].Conditions, metav1.Condition{
		Type: "CredentialCatalogUpdated", Status: metav1.ConditionTrue, Message: "2",
	})
	rotation, err = reconciler.credentialRotationPlan(context.Background(), instance, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rotation.Phase != plan.CredentialRotationPhaseMember ||
		rotation.TargetMember != "postgres-vic-0" {
		t.Fatalf("member rotation = %#v", rotation)
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

func TestMajorUpgradeRequiresDiscoveredRollbackStorage(t *testing.T) {
	scheme := testScheme(t)
	registration := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{Name: "vic"},
		Spec: api.SiteRegistrationSpec{StorageRollbackPolicies: []api.StorageRollbackPolicy{{
			StorageClassName: "premium", Strategy: "VolumeSnapshot",
			VolumeSnapshotClassName: "premium-snapshots",
		}}},
		Status: api.SiteRegistrationStatus{
			DiscoveredVolumeSnapshotClasses: []api.VolumeSnapshotClassInventory{{
				Name: "premium-snapshots", Driver: "csi.example", DeletionPolicy: "Retain",
			}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(registration).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	instance := &api.MultiSitePostgres{Spec: api.MultiSitePostgresSpec{
		Postgres: api.PostgresSpec{MajorVersion: 17},
		Backup:   &api.BackupSpec{},
		Sites: []api.PostgresSiteSpec{{
			Name: "vic", SiteRegistrationRef: "vic", Role: api.SiteRoleData,
			Storage: api.SiteStorage{Postgres: &api.StorageRequest{StorageClassName: "premium"}},
		}},
	}}
	upgrade := &api.PostgresUpgrade{Spec: api.PostgresUpgradeSpec{
		TargetMajorVersion:       18,
		TargetImage:              "registry.example/postgres@sha256:" + strings.Repeat("b", 64),
		UpgradeImage:             "registry.example/mspsql-upgrade@sha256:" + strings.Repeat("a", 64),
		ServiceRestorationTarget: metav1.Duration{Duration: 15 * time.Minute},
		RollbackRetention:        metav1.Duration{Duration: 24 * time.Hour},
		Benchmark: &api.MajorUpgradeBenchmark{
			TestedAt:             metav1.NewTime(time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)),
			EstimatedWriteOutage: metav1.Duration{Duration: 10 * time.Minute},
			UpgradeImage:         "registry.example/mspsql-upgrade@sha256:" + strings.Repeat("a", 64),
			SourceMajorVersion:   17, TargetMajorVersion: 18,
			PostgresStorageClasses: []string{"premium"}, Evidence: "oci://evidence@sha256:abc",
		},
	}}
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	if err := reconciler.validateMajorUpgradeContract(context.Background(), upgrade, instance, now); err != nil {
		t.Fatalf("valid upgrade contract rejected: %v", err)
	}
	registration.Status.DiscoveredVolumeSnapshotClasses = nil
	if err := kube.Update(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.validateMajorUpgradeContract(context.Background(), upgrade, instance, now); err == nil {
		t.Fatal("undiscovered VolumeSnapshotClass was accepted")
	}
	registration.Status.DiscoveredVolumeSnapshotClasses = []api.VolumeSnapshotClassInventory{{
		Name: "premium-snapshots", Driver: "csi.example", DeletionPolicy: "Retain",
	}}
	if err := kube.Update(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	upgrade.Spec.Benchmark.EstimatedWriteOutage.Duration = 16 * time.Minute
	if err := reconciler.validateMajorUpgradeContract(context.Background(), upgrade, instance, now); err == nil {
		t.Fatal("benchmark exceeding the restoration target was accepted")
	}
}

func TestMajorUpgradeTransitionsToOutageAndRollback(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance"), Generation: 1,
		},
		Spec: api.MultiSitePostgresSpec{
			Postgres: api.PostgresSpec{MajorVersion: 17, Image: "postgres:17"},
		},
		Status: api.MultiSitePostgresStatus{
			ObservedGeneration: 1, ActiveRevision: 3, Primary: "postgres-vic-0",
			Conditions: []metav1.Condition{{Type: "TopologyReady", Status: metav1.ConditionTrue}},
			Sites:      []api.SiteRevisionStatus{{Name: "vic", AppliedRevision: 3}},
		},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"), Generation: 1,
		},
		Spec: api.PostgresUpgradeSpec{
			InstanceRef: "orders", TargetMajorVersion: 18, TargetImage: "postgres:18",
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}, &api.PostgresUpgrade{}).
		WithObjects(instance, upgrade).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	now := time.Date(2026, 7, 16, 5, 0, 0, 0, time.UTC)

	reconcile := func() {
		t.Helper()
		var currentInstance api.MultiSitePostgres
		var currentUpgrade api.PostgresUpgrade
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &currentInstance); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &currentUpgrade); err != nil {
			t.Fatal(err)
		}
		if _, err := reconciler.reconcileMajorUpgrade(
			context.Background(), &currentUpgrade, &currentInstance, now); err != nil {
			t.Fatal(err)
		}
	}

	reconcile()
	var current api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradePhaseAnnotation] != string(plan.MajorUpgradePhasePreflight) ||
		current.Annotations[upgradeFromAnnotation] != "postgres-vic-0" {
		t.Fatalf("initial major-upgrade annotations = %#v", current.Annotations)
	}
	expected, err := strconv.ParseInt(current.Annotations[upgradeRevisionAnnotation], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	current.Status.ActiveRevision = expected
	current.Status.Sites = []api.SiteRevisionStatus{{Name: "vic", AppliedRevision: expected}}
	if err := kube.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradePhaseAnnotation] != string(plan.MajorUpgradePhaseDrain) {
		t.Fatalf("phase after preflight = %q", current.Annotations[upgradePhaseAnnotation])
	}
	var currentUpgrade api.PostgresUpgrade
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &currentUpgrade); err != nil {
		t.Fatal(err)
	}
	if currentUpgrade.Status.WriteOutageStartedAt == nil {
		t.Fatal("write outage start was not recorded")
	}
	expected, err = strconv.ParseInt(current.Annotations[upgradeRevisionAnnotation], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	current.Status.ActiveRevision = expected
	current.Status.Sites = []api.SiteRevisionStatus{{Name: "vic", AppliedRevision: expected}}
	if err := kube.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradePhaseAnnotation] != string(plan.MajorUpgradePhaseStop) {
		t.Fatalf("phase after drain = %q", current.Annotations[upgradePhaseAnnotation])
	}

	current.Annotations[upgradePhaseAnnotation] = string(plan.MajorUpgradePhaseUpgradePrimary)
	if err := kube.Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.Sites[0].Conditions = []metav1.Condition{{
		Type: "MajorUpgradeBlocked", Status: metav1.ConditionTrue, Reason: "PrimaryConversionFailed",
	}}
	if err := kube.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradePhaseAnnotation] != string(plan.MajorUpgradePhaseRollback) {
		t.Fatalf("failure phase = %q", current.Annotations[upgradePhaseAnnotation])
	}
}

func TestMajorUpgradeRequestsFreshFullBackup(t *testing.T) {
	scheme := testScheme(t)
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"), Generation: 1,
		},
		Spec: api.PostgresUpgradeSpec{InstanceRef: "orders"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUpgrade{}).WithObjects(upgrade).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	now := time.Date(2026, 7, 16, 5, 0, 0, 0, time.UTC)

	var current api.PostgresUpgrade
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	ready, err := reconciler.ensureFreshUpgradeBackup(context.Background(), &current, now)
	if err != nil || ready {
		t.Fatalf("first backup preflight = %t, %v", ready, err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	ready, err = reconciler.ensureFreshUpgradeBackup(context.Background(), &current, now)
	if err != nil || ready {
		t.Fatalf("second backup preflight = %t, %v", ready, err)
	}
	var directive corev1.ConfigMap
	if err := kube.Get(context.Background(), client.ObjectKey{
		Namespace: "platform", Name: "mspsql-upgrade-backup-upgrade",
	}, &directive); err != nil {
		t.Fatal(err)
	}
	if directive.Data["type"] != "Backup" ||
		!strings.Contains(directive.Data["spec.json"], `"backupType":"full"`) ||
		len(directive.OwnerReferences) != 1 ||
		directive.OwnerReferences[0].Kind != "PostgresUpgrade" {
		t.Fatalf("preflight backup directive = %#v", directive)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	setCondition(&current.Status.Conditions, current.Generation, "FreshBackupReady",
		metav1.ConditionTrue, "BackupVerified", "ready")
	if err := kube.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	ready, err = reconciler.ensureFreshUpgradeBackup(context.Background(), &current, now)
	if err != nil || !ready {
		t.Fatalf("completed backup preflight = %t, %v", ready, err)
	}
}

func TestMajorUpgradeRetriesPostUpgradeBackup(t *testing.T) {
	scheme := testScheme(t)
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"), Generation: 1,
		},
		Spec: api.PostgresUpgradeSpec{InstanceRef: "orders"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUpgrade{}).WithObjects(upgrade).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	now := time.Date(2026, 7, 16, 5, 0, 0, 0, time.UTC)

	var current api.PostgresUpgrade
	for range 2 {
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
			t.Fatal(err)
		}
		ready, err := reconciler.ensurePostUpgradeBackup(context.Background(), &current, now)
		if err != nil || ready {
			t.Fatalf("post-upgrade backup preflight = %t, %v", ready, err)
		}
	}
	var directive corev1.ConfigMap
	if err := kube.Get(context.Background(), client.ObjectKey{
		Namespace: "platform", Name: "mspsql-post-upgrade-backup-upgrade-0",
	}, &directive); err != nil {
		t.Fatal(err)
	}
	if directive.Data["upgradeBackupPhase"] != "post-upgrade" {
		t.Fatalf("post-upgrade directive = %#v", directive.Data)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	setCondition(&current.Status.Conditions, current.Generation, "PostUpgradeBackupReady",
		metav1.ConditionFalse, "BackupFailed", "failed")
	if err := kube.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	if ready, err := reconciler.ensurePostUpgradeBackup(context.Background(), &current, now); err != nil || ready {
		t.Fatalf("failed backup retry = %t, %v", ready, err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.PostUpgradeBackupAttempt != 1 ||
		current.Status.PostUpgradeBackupRequestedAt != nil {
		t.Fatalf("post-upgrade retry status = %#v", current.Status)
	}
}

func TestMinorUpgradeRollsReplicaThenSwitchesPrimary(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance"), Generation: 1,
		},
		Spec: api.MultiSitePostgresSpec{
			Postgres: api.PostgresSpec{MajorVersion: 17, Image: "postgres:17.1"},
			Sites: []api.PostgresSiteSpec{
				{Name: "vic", Role: api.SiteRoleData, Components: api.SiteComponents{PostgresReplicas: 1}},
				{Name: "qld", Role: api.SiteRoleData, Components: api.SiteComponents{PostgresReplicas: 1}},
			},
		},
		Status: api.MultiSitePostgresStatus{
			ObservedGeneration: 1, Primary: "postgres-vic-0",
			SynchronousStandbys: []string{"postgres-qld-0"},
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
				{Type: "TopologyReady", Status: metav1.ConditionTrue},
			},
		},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-17-2", UID: types.UID("upgrade"), Generation: 1,
		},
		Spec: api.PostgresUpgradeSpec{
			InstanceRef: "orders", TargetImage: "postgres:17.2", TargetMajorVersion: 17,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}, &api.PostgresUpgrade{}).
		WithObjects(instance, upgrade).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	now := time.Date(2026, 7, 16, 5, 0, 0, 0, time.UTC)

	reconcile := func() {
		t.Helper()
		var currentInstance api.MultiSitePostgres
		var currentUpgrade api.PostgresUpgrade
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &currentInstance); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &currentUpgrade); err != nil {
			t.Fatal(err)
		}
		if _, err := reconciler.reconcileMinorUpgrade(
			context.Background(), &currentUpgrade, &currentInstance, now); err != nil {
			t.Fatal(err)
		}
	}

	reconcile()
	var current api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradeMemberAnnotation] != "postgres-qld-0" {
		t.Fatalf("first member = %q", current.Annotations[upgradeMemberAnnotation])
	}
	markApplied := func(primary string) {
		t.Helper()
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
			t.Fatal(err)
		}
		revision, err := strconv.ParseInt(current.Annotations[upgradeRevisionAnnotation], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		current.Status.ActiveRevision = revision
		current.Status.Primary = primary
		current.Status.Sites = []api.SiteRevisionStatus{
			{Name: "vic", AppliedRevision: revision},
			{Name: "qld", AppliedRevision: revision},
		}
		if err := kube.Status().Update(context.Background(), &current); err != nil {
			t.Fatal(err)
		}
	}
	markApplied("postgres-vic-0")
	reconcile()
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradePhaseAnnotation] != string(plan.UpgradePhaseSwitchover) {
		t.Fatalf("phase = %q", current.Annotations[upgradePhaseAnnotation])
	}
	markApplied("postgres-qld-0")
	reconcile()
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradeMemberAnnotation] != "postgres-vic-0" {
		t.Fatalf("remaining member = %q", current.Annotations[upgradeMemberAnnotation])
	}
	markApplied("postgres-qld-0")
	reconcile()
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradePhaseAnnotation] != string(plan.UpgradePhaseFinalize) {
		t.Fatalf("final phase = %q", current.Annotations[upgradePhaseAnnotation])
	}
	markApplied("postgres-qld-0")
	reconcile()
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Postgres.Image != "postgres:17.2" ||
		current.Annotations[upgradeUIDAnnotation] != "" {
		t.Fatalf("completed instance = %#v", current)
	}
	var completed api.PostgresUpgrade
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Phase != "Completed" || len(completed.Status.UpgradedMembers) != 2 {
		t.Fatalf("upgrade status = %#v", completed.Status)
	}
}
