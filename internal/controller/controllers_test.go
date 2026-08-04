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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
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
	if len(signingKey.Data["keyID"]) == 0 ||
		string(signingKey.Data["revocationEpoch"]) != "0" {
		t.Fatalf("signing key rotation metadata = %#v", signingKey.Data)
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

func TestSiteRegistrationDeletionBlocksWhileReferenced(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 4, 0, 0, 0, time.UTC))
	site := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vic", UID: types.UID("site-uid"), Generation: 2,
			Finalizers: []string{siteFinalizer}, DeletionTimestamp: &deletingAt,
		},
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", SiteRegistrationRef: "vic"},
		}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.SiteRegistration{}).
		WithObjects(site, instance).Build()
	reconciler := SiteRegistrationReconciler{Client: kube, Scheme: scheme, SystemNamespace: "system"}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "vic"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("referenced site deletion did not requeue")
	}
	var current api.SiteRegistration
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(site), &current); err != nil {
		t.Fatal(err)
	}
	blocked := statusCondition(current.Status.Conditions, "DeletionBlocked")
	if blocked == nil || blocked.Reason != "ReferencedByInstances" ||
		!controllerutil.ContainsFinalizer(&current, siteFinalizer) {
		t.Fatalf("site deletion state = status %#v finalizers %#v", current.Status, current.Finalizers)
	}
}

func TestSiteRegistrationDeletionRevokesPeerAndToken(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 5, 0, 0, 0, time.UTC))
	site := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vic", UID: types.UID("site-uid"), Generation: 2,
			Finalizers: []string{siteFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Status: api.SiteRegistrationStatus{ClusterUID: "cluster-uid"},
	}
	token := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "registration-site-uid",
	}}
	peer := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "wireguard-peer-site-uid",
		Labels: map[string]string{"multisite-postgres.dev/wireguard-peer": "true"},
	}, Data: map[string][]byte{
		"publicKey": []byte("peer-key"), "address": []byte("10.60.0.3"),
		"siteName": []byte("vic"), "state": []byte("authorized"),
	}}
	allocations := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "mspsql-wireguard-addresses"},
		Data:       map[string]string{"site-uid": "10.60.0.3"},
	}
	rendered := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "mspsql-wireguard-peers"},
		Data:       map[string]string{"peers.conf": "old peer"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.SiteRegistration{}).
		WithObjects(site, token, peer, allocations, rendered).Build()
	reconciler := SiteRegistrationReconciler{Client: kube, Scheme: scheme, SystemNamespace: "system"}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "vic"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []types.NamespacedName{
		{Namespace: "system", Name: token.Name},
		{Namespace: "system", Name: peer.Name},
	} {
		var secret corev1.Secret
		err := kube.Get(context.Background(), key, &secret)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("%s still exists: %v", key.Name, err)
		}
	}
	var currentAllocations corev1.ConfigMap
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(allocations), &currentAllocations); err != nil {
		t.Fatal(err)
	}
	if _, found := currentAllocations.Data["site-uid"]; found {
		t.Fatalf("allocation still present: %#v", currentAllocations.Data)
	}
	var currentRendered corev1.ConfigMap
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(rendered), &currentRendered); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(currentRendered.Data["peers.conf"], "peer-key") ||
		currentRendered.Data["peers.conf"] == "old peer" {
		t.Fatalf("rendered peers not refreshed: %q", currentRendered.Data["peers.conf"])
	}
}

func TestSiteRegistrationDeletionWaitsForGatewayObservation(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 5, 0, 0, 0, time.UTC))
	site := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vic", UID: types.UID("site-uid"), Generation: 2,
			Finalizers: []string{siteFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Status: api.SiteRegistrationStatus{ClusterUID: "cluster-uid"},
	}
	peer := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "wireguard-peer-site-uid",
		Labels: map[string]string{"multisite-postgres.dev/wireguard-peer": "true"},
	}, Data: map[string][]byte{
		"publicKey": []byte("peer-key"), "address": []byte("10.60.0.3"),
		"siteName": []byte("vic"), "state": []byte("authorized"),
	}}
	rendered := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "mspsql-wireguard-peers"},
		Data:       map[string]string{"peers.conf": "old peer"},
	}
	replicas := int32(2)
	gateway := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "mspsql-wireguard", Generation: 7,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"multisite-postgres.dev/wireguard-peers-hash": "old"},
			}},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 6, AvailableReplicas: 1},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.SiteRegistration{}, &appsv1.Deployment{}).
		WithObjects(site, peer, rendered, gateway).Build()
	reconciler := SiteRegistrationReconciler{Client: kube, Scheme: scheme, SystemNamespace: "system"}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "vic"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Fatalf("requeue after = %s", result.RequeueAfter)
	}
	var current api.SiteRegistration
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "vic"}, &current); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&current, siteFinalizer) {
		t.Fatalf("site finalized before gateway observation: %#v", current.Finalizers)
	}
	condition := statusCondition(current.Status.Conditions, "WireGuardReady")
	if condition == nil || condition.Reason != "GatewayObservationPending" {
		t.Fatalf("wireguard condition = %#v", condition)
	}
	var patched appsv1.Deployment
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(gateway), &patched); err != nil {
		t.Fatal(err)
	}
	hash := patched.Spec.Template.Annotations["multisite-postgres.dev/wireguard-peers-hash"]
	patched.Status.ObservedGeneration = patched.Generation
	patched.Status.AvailableReplicas = replicas
	if err := kube.Status().Update(context.Background(), &patched); err != nil {
		t.Fatal(err)
	}
	if result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "vic"},
	}); err != nil {
		t.Fatal(err)
	} else if result.RequeueAfter != 0 {
		t.Fatalf("second requeue after = %s", result.RequeueAfter)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "vic"}, &current); err != nil {
		if !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	} else if controllerutil.ContainsFinalizer(&current, siteFinalizer) {
		t.Fatalf("site retained finalizer after gateway observed hash %s: %#v",
			hash, current.Finalizers)
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

func TestRegistrationStatusMergePreservesConcurrentHeartbeat(t *testing.T) {
	scheme := testScheme(t)
	now := time.Date(2026, 7, 17, 7, 0, 0, 0, time.UTC)
	heartbeat := metav1.NewTime(now.Add(-time.Second))
	expires := metav1.NewTime(now.Add(12 * time.Hour))
	current := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{Name: "vic", Generation: 3},
		Status: api.SiteRegistrationStatus{
			ClusterUID: "cluster", LastHeartbeatTime: &heartbeat,
			AgentCertificateExpiresAt: &expires,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(current).WithObjects(current).Build()
	desired := current.DeepCopy()
	desired.Status.LastHeartbeatTime = nil
	desired.Status.RegistrationURL = "https://hub/register"
	reconciler := &SiteRegistrationReconciler{Client: kube}
	if _, err := reconciler.updateControllerStatus(context.Background(), desired, now, true); err != nil {
		t.Fatal(err)
	}
	var observed api.SiteRegistration
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(current), &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Status.LastHeartbeatTime == nil || observed.Status.Phase != "Connected" ||
		observed.Status.RegistrationURL != desired.Status.RegistrationURL ||
		!conditionTrue(observed.Status.Conditions, "Connected") ||
		!conditionTrue(observed.Status.Conditions, "IdentityReady") {
		t.Fatalf("status = %#v", observed.Status)
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

func TestMissingSiteClearsStaleReadiness(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", Generation: 2,
			Finalizers: []string{instanceFinalizer},
		},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{{
			Name: "vic", SiteRegistrationRef: "missing",
		}}},
		Status: api.MultiSitePostgresStatus{Phase: "Ready", Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSitesReady",
		}}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(instance).WithObjects(instance).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(instance),
	}); err != nil {
		t.Fatal(err)
	}
	var observed api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &observed); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(observed.Status.Conditions, "Ready")
	if observed.Status.Phase != "ValidatingSites" || ready == nil ||
		ready.Status != metav1.ConditionFalse || ready.Reason != "SiteNotFound" ||
		ready.ObservedGeneration != observed.Generation {
		t.Fatalf("status = %#v", observed.Status)
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

func TestStatusTransitionEmitsKubernetesEvent(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders", UID: "instance"},
		Status: api.MultiSitePostgresStatus{Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSitesReady",
		}}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(instance).WithObjects(instance).Build()
	recorder := events.NewFakeRecorder(1)
	reconciler := &MultiSitePostgresReconciler{Client: kube, Recorder: recorder}
	desired := instance.DeepCopy()
	setCondition(&desired.Status.Conditions, desired.Generation, "Ready",
		metav1.ConditionFalse, "AgentDisconnected", "A data site is unavailable")
	if err := reconciler.updateInstanceStatus(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "Warning AgentDisconnected Ready: A data site is unavailable") {
			t.Fatalf("event = %q", event)
		}
	default:
		t.Fatal("condition transition emitted no Kubernetes Event")
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
			TDE: api.TDESpec{Enabled: true, Vault: &api.TDEVaultSpec{
				KVMount: "mspsql", KeyPath: "postgres/orders/tde",
				ProviderName: "orders-vault", PrincipalKeyName: "orders-principal",
			}},
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
	creators := 0
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
		var envelope plan.Envelope
		if err := json.Unmarshal([]byte(configMap.Data["envelope.json"]), &envelope); err != nil {
			t.Fatal(err)
		}
		var desired plan.SitePlan
		if err := json.Unmarshal(envelope.Plan, &desired); err != nil {
			t.Fatal(err)
		}
		if desired.TDEKeyCreator {
			creators++
			if site != "vic" {
				t.Fatalf("TDE key creator = %s", site)
			}
		}
	}
	if creators != 1 {
		t.Fatalf("TDE key creator count = %d", creators)
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
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &active); err != nil {
		t.Fatal(err)
	}
	if active.Status.Operation == nil ||
		active.Status.Operation.OperationUID != string(active.UID)+"-delete-"+strconv.FormatInt(active.Generation, 10) ||
		active.Status.Operation.Phase != "AwaitingSites" ||
		active.Status.Operation.Terminal {
		t.Fatalf("instance deletion operation = %#v", active.Status.Operation)
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

func TestPlanEnvelopeEqualityIgnoresGenerationTime(t *testing.T) {
	desired := plan.SitePlan{
		ProtocolVersion: plan.ProtocolVersion,
		SiteUID:         "site", InstanceUID: "instance", Revision: 3,
		GeneratedAt: time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC),
	}
	currentPlan := desired
	currentPlan.GeneratedAt = desired.GeneratedAt.Add(-time.Minute)
	currentRaw, err := json.Marshal(currentPlan)
	if err != nil {
		t.Fatal(err)
	}
	currentEnvelope, err := json.Marshal(plan.Envelope{Plan: currentRaw, Signature: "old"})
	if err != nil {
		t.Fatal(err)
	}
	desiredRaw, err := json.Marshal(desired)
	if err != nil {
		t.Fatal(err)
	}
	if !planEnvelopeEqual(string(currentEnvelope), plan.Envelope{Plan: desiredRaw, Signature: "new"}) {
		t.Fatal("equivalent plan revisions should not be rewritten solely to refresh generatedAt")
	}
	desired.Revision++
	desiredRaw, err = json.Marshal(desired)
	if err != nil {
		t.Fatal(err)
	}
	if planEnvelopeEqual(string(currentEnvelope), plan.Envelope{Plan: desiredRaw}) {
		t.Fatal("a changed plan revision must be persisted")
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
		}, Postgres: api.PostgresSpec{SynchronousStandbyCount: 1}},
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
		!conditionTrue(instance.Status.Conditions, "TopologyReady") ||
		!conditionTrue(instance.Status.Conditions, "SynchronousReplicationReady") {
		t.Fatalf("status = %#v", instance.Status)
	}
	instance.Status.Sites[1].SynchronousStandbys = nil
	aggregateTopology(instance, now)
	if conditionTrue(instance.Status.Conditions, "SynchronousReplicationReady") {
		t.Fatalf("missing synchronous observer consensus was accepted: %#v", instance.Status)
	}
	instance.Status.Sites[1].SynchronousStandbys = []string{"postgres-qld-0"}

	instance.Status.Sites[1].Primary = "postgres-qld-0"
	aggregateTopology(instance, now)
	if instance.Status.Primary != "" || conditionTrue(instance.Status.Conditions, "TopologyReady") {
		t.Fatalf("conflicting topology was accepted: %#v", instance.Status)
	}
}

func TestReadyRequiresTopologyConsensusWithoutSynchronousStandbys(t *testing.T) {
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Generation: 4},
		Status: api.MultiSitePostgresStatus{Conditions: []metav1.Condition{
			{Type: "EtcdTLSReady", Status: metav1.ConditionTrue},
			{Type: "TopologyReady", Status: metav1.ConditionFalse},
		}},
	}

	setAppliedInstanceReady(instance, nil, nil)

	ready := meta.FindStatusCondition(instance.Status.Conditions, "Ready")
	if instance.Status.Phase != "Reconciling" || ready == nil ||
		ready.Status != metav1.ConditionFalse || ready.Reason != "TopologyPending" ||
		ready.ObservedGeneration != instance.Generation {
		t.Fatalf("status = %#v", instance.Status)
	}
}

func TestBackupSchedulingUsesDataSiteReadinessDuringControlDegradation(t *testing.T) {
	base := api.MultiSitePostgres{
		Spec: api.MultiSitePostgresSpec{
			Backup: &api.BackupSpec{},
			Sites: []api.PostgresSiteSpec{
				{Name: "vic", Role: api.SiteRoleData},
				{Name: "qld", Role: api.SiteRoleData},
				{Name: "nsw", Role: api.SiteRoleWitness},
			},
		},
		Status: api.MultiSitePostgresStatus{
			ActiveRevision: 6,
			Sites: []api.SiteRevisionStatus{
				{Name: "vic", AppliedRevision: 6},
				{Name: "qld", AppliedRevision: 6},
				{Name: "nsw", AppliedRevision: 5},
			},
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionFalse, Reason: "ControlPlaneDegraded"},
				{Type: "TopologyReady", Status: metav1.ConditionTrue},
				{Type: "BackupTLSReady", Status: metav1.ConditionTrue},
			},
		},
	}
	tests := []struct {
		name            string
		mutate          func(*api.MultiSitePostgres)
		restorePlan     *plan.RestorePlan
		upgradePlan     *plan.UpgradePlan
		majorUpgrade    *plan.MajorUpgradePlan
		operationActive bool
		want            bool
	}{
		{
			name: "witness outage allows backup",
			want: true,
		},
		{
			name: "standby data site stale blocks backup",
			mutate: func(instance *api.MultiSitePostgres) {
				instance.Status.Sites[1].AppliedRevision = 5
			},
		},
		{
			name: "primary data site stale blocks backup",
			mutate: func(instance *api.MultiSitePostgres) {
				instance.Status.Sites[0].AppliedRevision = 5
			},
		},
		{
			name: "topology uncertainty blocks backup",
			mutate: func(instance *api.MultiSitePostgres) {
				meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
					Type: "TopologyReady", Status: metav1.ConditionFalse, Reason: "InsufficientObservations",
				})
			},
		},
		{
			name: "backup TLS gap blocks backup",
			mutate: func(instance *api.MultiSitePostgres) {
				meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
					Type: "BackupTLSReady", Status: metav1.ConditionFalse, Reason: "CertificatePending",
				})
			},
		},
		{
			name:        "restore operation blocks backup",
			restorePlan: &plan.RestorePlan{},
		},
		{
			name:        "minor upgrade operation blocks backup",
			upgradePlan: &plan.UpgradePlan{},
		},
		{
			name:         "major upgrade operation blocks backup",
			majorUpgrade: &plan.MajorUpgradePlan{},
		},
		{
			name:            "active child operation blocks backup",
			operationActive: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instance := base.DeepCopy()
			if test.mutate != nil {
				test.mutate(instance)
			}
			got := backupSchedulingReady(instance, test.restorePlan, test.upgradePlan,
				test.majorUpgrade, test.operationActive)
			if got != test.want {
				t.Fatalf("backupSchedulingReady = %t, want %t", got, test.want)
			}
		})
	}
}

func TestTopologyChangingOperationDecisionTable(t *testing.T) {
	tests := []struct {
		name         string
		restorePlan  *plan.RestorePlan
		upgradePlan  *plan.UpgradePlan
		majorUpgrade *plan.MajorUpgradePlan
		want         bool
	}{
		{name: "steady reconciliation"},
		{name: "restore", restorePlan: &plan.RestorePlan{}, want: true},
		{name: "minor upgrade", upgradePlan: &plan.UpgradePlan{}, want: true},
		{name: "major upgrade", majorUpgrade: &plan.MajorUpgradePlan{}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := topologyChangingOperationPending(test.restorePlan, test.upgradePlan, test.majorUpgrade)
			if got != test.want {
				t.Fatalf("topologyChangingOperationPending = %t, want %t", got, test.want)
			}
		})
	}
}

func TestWitnessDisconnectBlocksTopologyChangingPlan(t *testing.T) {
	scheme := testScheme(t)
	issuer := api.IssuerReference{Name: "issuer", Kind: "ClusterIssuer", Group: "cert-manager.io"}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance-uid"), Generation: 3,
			Finalizers: []string{instanceFinalizer},
			Annotations: map[string]string{
				upgradeNameAnnotation:   "orders-minor",
				upgradePhaseAnnotation:  string(plan.UpgradePhaseMember),
				upgradeMemberAnnotation: "postgres-vic-0",
				upgradeFromAnnotation:   "postgres-vic-0",
			},
		},
		Spec: api.MultiSitePostgresSpec{
			Postgres: api.PostgresSpec{MajorVersion: 17, Image: "postgres:17"},
			Sites: []api.PostgresSiteSpec{
				{
					Name: "vic", SiteRegistrationRef: "vic", Role: api.SiteRoleData,
					Certificates: api.SiteCertificateSpec{
						EtcdIssuerRef: issuer, PostgresIssuerRef: issuer, PgpoolIssuerRef: issuer,
					},
				},
				{
					Name: "qld", SiteRegistrationRef: "qld", Role: api.SiteRoleData,
					Certificates: api.SiteCertificateSpec{
						EtcdIssuerRef: issuer, PostgresIssuerRef: issuer, PgpoolIssuerRef: issuer,
					},
				},
				{
					Name: "nsw", SiteRegistrationRef: "nsw", Role: api.SiteRoleWitness,
					Certificates: api.SiteCertificateSpec{
						EtcdIssuerRef: issuer, PostgresIssuerRef: issuer, PgpoolIssuerRef: issuer,
					},
				},
			},
		},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-minor", UID: types.UID("upgrade-uid"),
		},
		Spec: api.PostgresUpgradeSpec{InstanceRef: "orders", TargetImage: "postgres:17.6"},
	}
	registration := func(name, phase string) *api.SiteRegistration {
		return &api.SiteRegistration{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")},
			Spec: api.SiteRegistrationSpec{PermittedIssuers: api.IssuerPolicy{
				Etcd: []api.IssuerReference{issuer}, Postgres: []api.IssuerReference{issuer},
				Pgpool: []api.IssuerReference{issuer},
			}},
			Status: api.SiteRegistrationStatus{Phase: phase},
		}
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}).
		WithObjects(instance, upgrade,
			registration("vic", "Connected"),
			registration("qld", "Connected"),
			registration("nsw", "Pending"),
		).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(instance),
	}); err != nil {
		t.Fatal(err)
	}
	var observed api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &observed); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(observed.Status.Conditions, "Ready")
	topology := meta.FindStatusCondition(observed.Status.Conditions, "TopologyAuthoritative")
	if observed.Status.Phase != "Blocked" ||
		ready == nil || ready.Reason != "ControlPlaneDegraded" ||
		topology == nil || topology.Status != metav1.ConditionFalse {
		t.Fatalf("status = %#v", observed.Status)
	}
	var plans corev1.ConfigMapList
	if err := kube.List(context.Background(), &plans, client.InNamespace("platform"),
		client.MatchingLabels{"multisite-postgres.dev/instance-uid": string(instance.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(plans.Items) != 0 {
		t.Fatalf("plans were issued while topology was degraded: %#v", plans.Items)
	}
}

func TestMajorUpgradeRequiresAgentCapability(t *testing.T) {
	instance := &api.MultiSitePostgres{
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", SiteRegistrationRef: "production-vic"},
			{Name: "qld", SiteRegistrationRef: "production-qld"},
		}},
	}
	registrations := map[string]*api.SiteRegistration{
		"vic": {Status: api.SiteRegistrationStatus{
			Capabilities: []string{capabilityMajorUpgradeSyncBeforeWrites},
		}},
		"qld": {Status: api.SiteRegistrationStatus{
			Capabilities: []string{"inventory-v1"},
		}},
	}
	missing := missingPlanCapabilities(instance, registrations, &plan.MajorUpgradePlan{})
	if len(missing) != 1 ||
		missing[0] != "qld/production-qld:"+capabilityMajorUpgradeSyncBeforeWrites {
		t.Fatalf("missing capabilities = %#v", missing)
	}
	registrations["qld"].Status.Capabilities = append(registrations["qld"].Status.Capabilities,
		capabilityMajorUpgradeSyncBeforeWrites)
	if missing := missingPlanCapabilities(instance, registrations, &plan.MajorUpgradePlan{}); len(missing) != 0 {
		t.Fatalf("capability unexpectedly missing: %#v", missing)
	}
	if missing := missingPlanCapabilities(instance, registrations, nil); len(missing) != 0 {
		t.Fatalf("non-major plan required capabilities: %#v", missing)
	}
	required := requiredPlanCapabilities(&plan.MajorUpgradePlan{})
	if len(required) != 1 || required[0] != capabilityMajorUpgradeSyncBeforeWrites {
		t.Fatalf("required major-upgrade capabilities = %#v", required)
	}
	if required := requiredPlanCapabilities(nil); len(required) != 0 {
		t.Fatalf("non-major required capabilities = %#v", required)
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
	operation := instance.Status.BackupSchedules[0].Operation
	if operation == nil ||
		operation.OperationUID != string(instance.UID)+"-backup-full-"+
			strconv.FormatInt(instance.Status.BackupSchedules[0].LastScheduledAt.Unix(), 10) ||
		operation.Phase != "ScheduledBackup" ||
		operation.Attempt != 1 ||
		operation.Terminal {
		t.Fatalf("backup schedule operation = %#v", operation)
	}
}

func TestLifecycleOperationBlocksBackupBeforeInstanceAnnotation(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-patch"},
		Spec:       api.PostgresUpgradeSpec{InstanceRef: "orders"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, upgrade).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube}
	active, err := reconciler.lifecycleOperationActive(context.Background(), instance)
	if err != nil {
		t.Fatal(err)
	}
	if !active {
		t.Fatal("unannotated pending upgrade did not block backup scheduling")
	}
	upgrade.Status.Phase = "Completed"
	if err := kube.Update(context.Background(), upgrade); err != nil {
		t.Fatal(err)
	}
	active, err = reconciler.lifecycleOperationActive(context.Background(), instance)
	if err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("completed upgrade continued to block backup scheduling")
	}
}

func TestInstanceAggregatesDeclarationAndUpgradeConditions(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders", Generation: 5},
	}
	database := &api.PostgresDatabase{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-api", Generation: 2},
		Spec:       api.PostgresDatabaseSpec{InstanceRef: "orders"},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-patch"},
		Spec:       api.PostgresUpgradeSpec{InstanceRef: "orders"},
		Status:     api.PostgresUpgradeStatus{Phase: "RollingMembers"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(database, upgrade).Build()
	reconciler := &MultiSitePostgresReconciler{Client: kube}
	if err := reconciler.aggregateDeclarationAndUpgradeStatus(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	declarations := meta.FindStatusCondition(instance.Status.Conditions, "DatabasesReconciled")
	upgrading := meta.FindStatusCondition(instance.Status.Conditions, "UpgradeInProgress")
	if declarations == nil || declarations.Status != metav1.ConditionFalse ||
		declarations.ObservedGeneration != instance.Generation ||
		upgrading == nil || upgrading.Status != metav1.ConditionTrue {
		t.Fatalf("conditions = %#v", instance.Status.Conditions)
	}
	database.Status.ObservedGeneration = database.Generation
	database.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
	if err := kube.Update(context.Background(), database); err != nil {
		t.Fatal(err)
	}
	upgrade.Status.Phase = "Completed"
	if err := kube.Update(context.Background(), upgrade); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.aggregateDeclarationAndUpgradeStatus(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	if !conditionTrue(instance.Status.Conditions, "DatabasesReconciled") ||
		conditionTrue(instance.Status.Conditions, "UpgradeInProgress") {
		t.Fatalf("completed conditions = %#v", instance.Status.Conditions)
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
			RecoveryWindowStart:        &window,
			RestoreDrillLastVerifiedAt: &metav1.Time{Time: now.Add(-time.Hour)},
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
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("finalizer adoption requeue = %s", result.RequeueAfter)
	}
	result, err = reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != restoreProgressRequeue {
		t.Fatalf("target creation requeue = %s", result.RequeueAfter)
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
	var currentRestore api.PostgresRestore
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(restore), &currentRestore); err != nil {
		t.Fatal(err)
	}
	assertRestoreOperation(t, currentRestore.Status.Operation, string(restore.UID), "Provisioning", false)

	target.Status.Primary = "postgres-vic-0"
	target.Status.Conditions = []metav1.Condition{
		{Type: "TopologyReady", Status: metav1.ConditionTrue},
	}
	if err := kube.Status().Update(context.Background(), &target); err != nil {
		t.Fatal(err)
	}
	result, err = reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != restoreProgressRequeue {
		t.Fatalf("seed promotion requeue = %s", result.RequeueAfter)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(&target), &target); err != nil {
		t.Fatal(err)
	}
	if target.Annotations[restorePhaseAnnotation] != string(plan.RestorePhaseReplicas) {
		t.Fatalf("restore phase = %q", target.Annotations[restorePhaseAnnotation])
	}
	target.Status.Conditions = []metav1.Condition{
		{Type: "TopologyReady", Status: metav1.ConditionTrue},
	}
	target.Status.SynchronousStandbys = []string{"postgres-qld-0"}
	if err := kube.Status().Update(context.Background(), &target); err != nil {
		t.Fatal(err)
	}
	result, err = reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != restoreProgressRequeue {
		t.Fatalf("replica seed requeue = %s", result.RequeueAfter)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(&target), &target); err != nil {
		t.Fatal(err)
	}
	if target.Annotations[restorePhaseAnnotation] != string(plan.RestorePhaseVerify) {
		t.Fatalf("restore phase = %q", target.Annotations[restorePhaseAnnotation])
	}
	target.Status.Conditions = []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue},
		{Type: "TopologyReady", Status: metav1.ConditionTrue},
	}
	if err := kube.Status().Update(context.Background(), &target); err != nil {
		t.Fatal(err)
	}
	result, err = reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("completed restore requeue = %s", result.RequeueAfter)
	}
	var completed api.PostgresRestore
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(restore), &completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Phase != "Completed" ||
		completed.Status.RecoveredTo == nil ||
		!completed.Status.RecoveredTo.Equal(&restore.Spec.TargetTime) {
		t.Fatalf("restore status = %#v", completed.Status)
	}
	assertRestoreOperation(t, completed.Status.Operation, string(restore.UID), "Completed", true)
}

func assertRestoreOperation(t *testing.T, operation *api.OperationProgressStatus,
	operationUID, phase string, terminal bool,
) {
	t.Helper()
	if operation == nil ||
		operation.OperationUID != operationUID ||
		operation.Phase != phase ||
		operation.Attempt != 1 ||
		operation.DeadlineAt == nil ||
		operation.Terminal != terminal ||
		operation.ManualInterventionRequired {
		t.Fatalf("restore operation status = %#v", operation)
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

func TestRestorePreflightRequiresDisposableRestoreEvidence(t *testing.T) {
	scheme := testScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	now := time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC)
	window := metav1.NewTime(now.Add(-24 * time.Hour))
	source := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
		Spec: api.MultiSitePostgresSpec{
			Backup: &api.BackupSpec{Repository: api.BackupRepositorySpec{
				Type: "S3", Bucket: "backups", Prefix: "orders",
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
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-restore"},
		Spec: api.PostgresRestoreSpec{
			SourceInstanceRef: "orders", TargetInstanceRef: "orders-recovered",
			TargetTime: metav1.NewTime(now.Add(-time.Hour)),
		},
	}
	reconciler := PostgresRestoreReconciler{
		Client: kube, Scheme: scheme, Now: func() time.Time { return now },
	}
	err := reconciler.preflight(context.Background(), restore, source)
	if err == nil || !strings.Contains(err.Error(), "disposable restore verification") {
		t.Fatalf("preflight error = %v", err)
	}
	drill := metav1.NewTime(now.Add(-time.Hour))
	source.Status.RestoreDrillLastVerifiedAt = &drill
	if err := reconciler.preflight(context.Background(), restore, source); err != nil {
		t.Fatalf("preflight with drill evidence failed: %v", err)
	}
}

func TestRestoreDrillCanCreateAndRecordDisposableEvidence(t *testing.T) {
	scheme := testScheme(t)
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	window := metav1.NewTime(now.Add(-24 * time.Hour))
	source := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("source-uid"), Generation: 4,
		},
		Spec: api.MultiSitePostgresSpec{
			Backup: &api.BackupSpec{Repository: api.BackupRepositorySpec{
				Type: "S3", Bucket: "backups", Prefix: "orders",
			}},
			Postgres: api.PostgresSpec{MajorVersion: 17, Image: "postgres:17"},
			Sites: []api.PostgresSiteSpec{{
				Name: "vic", SiteRegistrationRef: "vic", Namespace: "orders",
				Role:       api.SiteRoleData,
				Components: api.SiteComponents{PostgresReplicas: 1},
			}},
		},
		Status: api.MultiSitePostgresStatus{
			RecoveryWindowStart: &window,
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
				{Type: "BackupReady", Status: metav1.ConditionTrue},
				{Type: "RecoveryWindowAvailable", Status: metav1.ConditionFalse, Reason: "RestoreDrillRequired"},
			},
		},
	}
	restore := &api.PostgresRestore{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-drill", UID: types.UID("restore-uid"),
			Annotations: map[string]string{restoreDrillAnnotation: "true"},
		},
		Spec: api.PostgresRestoreSpec{
			SourceInstanceRef: "orders", TargetInstanceRef: "orders-drill-target",
			TargetTime: metav1.NewTime(now.Add(-time.Hour)), BackupSet: "full-20260802",
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}, &api.PostgresRestore{}).
		WithObjects(source, restore).Build()
	reconciler := PostgresRestoreReconciler{
		Client: kube, Scheme: scheme, Now: func() time.Time { return now },
	}
	if err := reconciler.preflight(context.Background(), restore, source); err != nil {
		t.Fatalf("drill preflight failed: %v", err)
	}
	if err := reconciler.completeRestore(context.Background(), restore); err != nil {
		t.Fatal(err)
	}
	var current api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(source), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.RestoreDrillLastVerifiedAt == nil ||
		!current.Status.RestoreDrillLastVerifiedAt.Equal(&metav1.Time{Time: now}) ||
		current.Status.RestoreDrillBackupSet != "full-20260802" {
		t.Fatalf("restore drill evidence = %#v", current.Status)
	}
	condition := statusCondition(current.Status.Conditions, "RecoveryWindowAvailable")
	if condition == nil || condition.Status != metav1.ConditionTrue ||
		condition.Reason != "DisposableRestoreVerified" {
		t.Fatalf("recovery window condition = %#v", condition)
	}
}

func TestFailedUpgradeIsTerminal(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", UID: types.UID("instance"), Generation: 1,
			Annotations: map[string]string{upgradeUIDAnnotation: "upgrade"},
		},
		Status: api.MultiSitePostgresStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
				{Type: "BackupReady", Status: metav1.ConditionTrue},
			},
			LastBackupTime: &metav1.Time{Time: time.Date(2026, 7, 16, 4, 0, 0, 0, time.UTC)},
		},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"), Generation: 1,
		},
		Spec: api.PostgresUpgradeSpec{
			InstanceRef: "orders", TargetMajorVersion: 18, TargetImage: "postgres:18",
		},
		Status: api.PostgresUpgradeStatus{
			Phase: "Failed",
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionFalse, Reason: "RolledBack",
			}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}, &api.PostgresUpgrade{}).
		WithObjects(instance, upgrade).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-pg18"},
	}); err != nil {
		t.Fatal(err)
	}
	var currentUpgrade api.PostgresUpgrade
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &currentUpgrade); err != nil {
		t.Fatal(err)
	}
	if currentUpgrade.Status.Phase != "Failed" ||
		statusCondition(currentUpgrade.Status.Conditions, "Ready").Reason != "RolledBack" {
		t.Fatalf("failed upgrade was reconciled again: %#v", currentUpgrade.Status)
	}
	var currentInstance api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &currentInstance); err != nil {
		t.Fatal(err)
	}
	if _, found := currentInstance.Annotations[upgradeUIDAnnotation]; found {
		t.Fatalf("failed upgrade annotation was not cleared: %#v", currentInstance.Annotations)
	}
}

func TestRestoreDeletionDeletesOwnedTarget(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 1, 0, 0, 0, time.UTC))
	restore := &api.PostgresRestore{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-restore", UID: types.UID("restore-uid"),
			Finalizers:        []string{operationFinalizer},
			DeletionTimestamp: &deletingAt,
		},
		Spec: api.PostgresRestoreSpec{
			SourceInstanceRef: "orders", TargetInstanceRef: "orders-recovered",
		},
		Status: api.PostgresRestoreStatus{Phase: "Restoring"},
	}
	target := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-recovered",
			Annotations: map[string]string{
				restoreUIDAnnotation:   string(restore.UID),
				restoreNameAnnotation:  restore.Name,
				restorePhaseAnnotation: string(plan.RestorePhaseSeed),
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresRestore{}).
		WithObjects(restore, target).Build()
	reconciler := PostgresRestoreReconciler{Client: kube, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-restore"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != restoreProgressRequeue {
		t.Fatalf("restore delete requeue = %s", result.RequeueAfter)
	}
	var currentTarget api.MultiSitePostgres
	err = kube.Get(context.Background(), client.ObjectKeyFromObject(target), &currentTarget)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("target still exists or unexpected error: %v", err)
	}
	var currentRestore api.PostgresRestore
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(restore), &currentRestore); err != nil {
		t.Fatal(err)
	}
	ready := statusCondition(currentRestore.Status.Conditions, "Ready")
	if ready == nil || ready.Reason != "CancellationInProgress" {
		t.Fatalf("restore deletion condition = %#v", ready)
	}
}

func TestRestoreDeletionHandlesEveryPhase(t *testing.T) {
	for _, phase := range []plan.RestorePhase{
		plan.RestorePhaseSeed,
		plan.RestorePhaseReplicas,
		plan.RestorePhaseVerify,
	} {
		t.Run(string(phase), func(t *testing.T) {
			scheme := testScheme(t)
			deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 1, 30, 0, 0, time.UTC))
			restore := &api.PostgresRestore{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "platform", Name: "orders-restore", UID: types.UID("restore-uid"),
					Finalizers:        []string{operationFinalizer},
					DeletionTimestamp: &deletingAt,
				},
				Spec: api.PostgresRestoreSpec{
					SourceInstanceRef: "orders", TargetInstanceRef: "orders-recovered",
				},
				Status: api.PostgresRestoreStatus{Phase: string(phase)},
			}
			target := &api.MultiSitePostgres{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "platform", Name: "orders-recovered",
					Annotations: map[string]string{
						restoreUIDAnnotation:       string(restore.UID),
						restoreNameAnnotation:      restore.Name,
						restoreSourceUIDAnnotation: "source-uid",
						restorePhaseAnnotation:     string(phase),
					},
				},
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&api.PostgresRestore{}).
				WithObjects(restore, target).Build()
			reconciler := PostgresRestoreReconciler{Client: kube, Scheme: scheme}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: "platform", Name: restore.Name},
			}); err != nil {
				t.Fatal(err)
			}
			var currentTarget api.MultiSitePostgres
			err := kube.Get(context.Background(), client.ObjectKeyFromObject(target), &currentTarget)
			if !apierrors.IsNotFound(err) {
				t.Fatalf("target for phase %s still exists or unexpected error: %v", phase, err)
			}
		})
	}
}

func TestRestoreDeletionForceAbandonClearsOperationAnnotations(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 1, 45, 0, 0, time.UTC))
	restore := &api.PostgresRestore{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-restore", UID: types.UID("restore-uid"),
			Finalizers:        []string{operationFinalizer},
			DeletionTimestamp: &deletingAt,
			Annotations:       map[string]string{forceAbandonAnnotation: "true"},
		},
		Spec: api.PostgresRestoreSpec{
			SourceInstanceRef: "orders", TargetInstanceRef: "orders-recovered",
		},
		Status: api.PostgresRestoreStatus{Phase: "Restoring"},
	}
	target := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-recovered",
			Annotations: map[string]string{
				restoreUIDAnnotation:       string(restore.UID),
				restoreNameAnnotation:      restore.Name,
				restoreSourceUIDAnnotation: "source-uid",
				restorePhaseAnnotation:     string(plan.RestorePhaseReplicas),
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresRestore{}).
		WithObjects(restore, target).Build()
	reconciler := PostgresRestoreReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: restore.Name},
	}); err != nil {
		t.Fatal(err)
	}
	var currentTarget api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(target), &currentTarget); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		restoreUIDAnnotation, restoreNameAnnotation, restoreSourceUIDAnnotation, restorePhaseAnnotation,
	} {
		if _, found := currentTarget.Annotations[key]; found {
			t.Fatalf("force-abandoned target retained %s: %#v", key, currentTarget.Annotations)
		}
	}
}

func TestMajorUpgradeDeletionBeforeWritesRollsBack(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 2, 0, 0, 0, time.UTC))
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", Generation: 4,
			Annotations: map[string]string{
				upgradeUIDAnnotation:      "upgrade",
				upgradeNameAnnotation:     "orders-pg18",
				upgradePhaseAnnotation:    string(plan.MajorUpgradePhaseStartPrimary),
				upgradeRevisionAnnotation: "9",
			},
		},
		Status: api.MultiSitePostgresStatus{ActiveRevision: 9, Sites: []api.SiteRevisionStatus{
			{Name: "vic", AppliedRevision: 9},
		}},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"),
			Generation: 1, Finalizers: []string{operationFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Spec:   api.PostgresUpgradeSpec{InstanceRef: "orders"},
		Status: api.PostgresUpgradeStatus{Phase: "RestoringService"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUpgrade{}).
		WithObjects(instance, upgrade).Build()
	reconciler := PostgresUpgradeReconciler{
		Client: kube, Scheme: scheme, Now: func() time.Time { return deletingAt.Time },
	}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-pg18"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != restoreProgressRequeue {
		t.Fatalf("upgrade rollback requeue = %s", result.RequeueAfter)
	}
	var current api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	if current.Annotations[upgradePhaseAnnotation] != string(plan.MajorUpgradePhaseRollback) {
		t.Fatalf("upgrade phase annotation = %q", current.Annotations[upgradePhaseAnnotation])
	}
}

func TestMajorUpgradeDeletionAfterWritesBlocks(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 3, 0, 0, 0, time.UTC))
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders",
			Annotations: map[string]string{
				upgradeUIDAnnotation:      "upgrade",
				upgradeNameAnnotation:     "orders-pg18",
				upgradePhaseAnnotation:    string(plan.MajorUpgradePhaseReplicas),
				upgradeRevisionAnnotation: "10",
			},
		},
	}
	restoredAt := metav1.NewTime(deletingAt.Add(-time.Minute))
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"),
			Generation: 1, Finalizers: []string{operationFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Spec: api.PostgresUpgradeSpec{InstanceRef: "orders"},
		Status: api.PostgresUpgradeStatus{
			Phase: "ReseedingReplicas", WriteServiceRestoredAt: &restoredAt,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUpgrade{}).
		WithObjects(instance, upgrade).Build()
	reconciler := PostgresUpgradeReconciler{
		Client: kube, Scheme: scheme, Now: func() time.Time { return deletingAt.Time },
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-pg18"},
	}); err != nil {
		t.Fatal(err)
	}
	var current api.PostgresUpgrade
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	ready := statusCondition(current.Status.Conditions, "Ready")
	if ready == nil || ready.Reason != "ForwardRepairRequired" ||
		!controllerutil.ContainsFinalizer(&current, operationFinalizer) {
		t.Fatalf("upgrade deletion state = status %#v finalizers %#v", current.Status, current.Finalizers)
	}
}

func TestMajorUpgradeDeletionPhaseMatrix(t *testing.T) {
	tests := []struct {
		name        string
		phase       plan.MajorUpgradePhase
		statusPhase string
		wantPhase   plan.MajorUpgradePhase
		wantReason  string
		wantFinal   bool
	}{
		{
			name: "preflight clears", phase: plan.MajorUpgradePhasePreflight,
			statusPhase: "Preflight", wantPhase: "", wantFinal: false,
		},
		{
			name: "drain rolls back", phase: plan.MajorUpgradePhaseDrain,
			statusPhase: "DrainingWrites", wantPhase: plan.MajorUpgradePhaseRollback,
			wantReason: "DeletionRequested", wantFinal: true,
		},
		{
			name: "stop rolls back", phase: plan.MajorUpgradePhaseStop,
			statusPhase: "Stopping", wantPhase: plan.MajorUpgradePhaseRollback,
			wantReason: "DeletionRequested", wantFinal: true,
		},
		{
			name: "snapshot rolls back", phase: plan.MajorUpgradePhaseSnapshot,
			statusPhase: "CapturingRollback", wantPhase: plan.MajorUpgradePhaseRollback,
			wantReason: "DeletionRequested", wantFinal: true,
		},
		{
			name: "upgrade primary rolls back", phase: plan.MajorUpgradePhaseUpgradePrimary,
			statusPhase: "UpgradingPrimary", wantPhase: plan.MajorUpgradePhaseRollback,
			wantReason: "DeletionRequested", wantFinal: true,
		},
		{
			name: "stanza upgrade rolls back", phase: plan.MajorUpgradePhaseStanzaUpgrade,
			statusPhase: "UpgradingBackupStanza", wantPhase: plan.MajorUpgradePhaseRollback,
			wantReason: "DeletionRequested", wantFinal: true,
		},
		{
			name: "start primary rolls back", phase: plan.MajorUpgradePhaseStartPrimary,
			statusPhase: "RestoringService", wantPhase: plan.MajorUpgradePhaseRollback,
			wantReason: "DeletionRequested", wantFinal: true,
		},
		{
			name: "rollback waits", phase: plan.MajorUpgradePhaseRollback,
			statusPhase: "RollingBack", wantPhase: plan.MajorUpgradePhaseRollback,
			wantReason: "CancellationInProgress", wantFinal: true,
		},
		{
			name: "rollback start waits", phase: plan.MajorUpgradePhaseRollbackStart,
			statusPhase: "VerifyingRollback", wantPhase: plan.MajorUpgradePhaseRollbackStart,
			wantReason: "CancellationInProgress", wantFinal: true,
		},
		{
			name: "rollback restore writes waits", phase: plan.MajorUpgradePhaseRollbackRestoreWrites,
			statusPhase: "RestoringWrites", wantPhase: plan.MajorUpgradePhaseRollbackRestoreWrites,
			wantReason: "CancellationInProgress", wantFinal: true,
		},
		{
			name: "replicas blocks", phase: plan.MajorUpgradePhaseReplicas,
			statusPhase: "ReseedingReplicas", wantPhase: plan.MajorUpgradePhaseReplicas,
			wantReason: "ForwardRepairRequired", wantFinal: true,
		},
		{
			name: "restore writes blocks", phase: plan.MajorUpgradePhaseRestoreWrites,
			statusPhase: "RestoringWrites", wantPhase: plan.MajorUpgradePhaseRestoreWrites,
			wantReason: "ForwardRepairRequired", wantFinal: true,
		},
		{
			name: "finalize blocks", phase: plan.MajorUpgradePhaseFinalize,
			statusPhase: "Finalizing", wantPhase: plan.MajorUpgradePhaseFinalize,
			wantReason: "ForwardRepairRequired", wantFinal: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := testScheme(t)
			deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 3, 30, 0, 0, time.UTC))
			annotations := map[string]string{
				upgradeUIDAnnotation:      "upgrade",
				upgradeNameAnnotation:     "orders-pg18",
				upgradePhaseAnnotation:    string(test.phase),
				upgradeRevisionAnnotation: "10",
			}
			instance := &api.MultiSitePostgres{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "platform", Name: "orders", Annotations: annotations,
				},
			}
			upgrade := &api.PostgresUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"),
					Generation: 1, Finalizers: []string{operationFinalizer},
					DeletionTimestamp: &deletingAt,
				},
				Spec:   api.PostgresUpgradeSpec{InstanceRef: "orders"},
				Status: api.PostgresUpgradeStatus{Phase: test.statusPhase},
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&api.PostgresUpgrade{}).
				WithObjects(instance, upgrade).Build()
			reconciler := PostgresUpgradeReconciler{
				Client: kube, Scheme: scheme, Now: func() time.Time { return deletingAt.Time },
			}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-pg18"},
			}); err != nil {
				t.Fatal(err)
			}
			var currentInstance api.MultiSitePostgres
			if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &currentInstance); err != nil {
				t.Fatal(err)
			}
			if test.wantPhase == "" {
				if _, found := currentInstance.Annotations[upgradePhaseAnnotation]; found {
					t.Fatalf("preflight delete retained upgrade annotations: %#v", currentInstance.Annotations)
				}
			} else if currentInstance.Annotations[upgradePhaseAnnotation] != string(test.wantPhase) {
				t.Fatalf("phase = %q, want %q", currentInstance.Annotations[upgradePhaseAnnotation],
					test.wantPhase)
			}
			var currentUpgrade api.PostgresUpgrade
			err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &currentUpgrade)
			if !test.wantFinal && apierrors.IsNotFound(err) {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantReason != "" {
				ready := statusCondition(currentUpgrade.Status.Conditions, "Ready")
				if ready == nil || ready.Reason != test.wantReason {
					t.Fatalf("Ready condition = %#v", ready)
				}
			}
			if test.wantFinal && !controllerutil.ContainsFinalizer(&currentUpgrade, operationFinalizer) {
				t.Fatalf("finalizer removed too early: %#v", currentUpgrade.Finalizers)
			}
		})
	}
}

func TestMajorUpgradeDeletionForceAbandonClearsAnnotations(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 3, 45, 0, 0, time.UTC))
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders",
			Annotations: map[string]string{
				upgradeUIDAnnotation:         "upgrade",
				upgradeNameAnnotation:        "orders-pg18",
				upgradePhaseAnnotation:       string(plan.MajorUpgradePhaseFinalize),
				upgradeMemberAnnotation:      "postgres-vic-1",
				upgradeRevisionAnnotation:    "10",
				upgradeSourceMajorAnnotation: "17",
			},
		},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"),
			Generation: 1, Finalizers: []string{operationFinalizer},
			DeletionTimestamp: &deletingAt,
			Annotations:       map[string]string{forceAbandonAnnotation: "true"},
		},
		Spec:   api.PostgresUpgradeSpec{InstanceRef: "orders"},
		Status: api.PostgresUpgradeStatus{Phase: "Finalizing"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUpgrade{}).
		WithObjects(instance, upgrade).Build()
	reconciler := PostgresUpgradeReconciler{
		Client: kube, Scheme: scheme, Now: func() time.Time { return deletingAt.Time },
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-pg18"},
	}); err != nil {
		t.Fatal(err)
	}
	var current api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		upgradeUIDAnnotation, upgradeNameAnnotation, upgradePhaseAnnotation,
		upgradeMemberAnnotation, upgradeRevisionAnnotation, upgradeSourceMajorAnnotation,
	} {
		if _, found := current.Annotations[key]; found {
			t.Fatalf("force-abandoned upgrade retained %s: %#v", key, current.Annotations)
		}
	}
}

func TestMajorUpgradeRestoresWritesOnlyAfterSynchronousReplica(t *testing.T) {
	scheme := testScheme(t)
	now := time.Date(2026, 8, 2, 7, 0, 0, 0, time.UTC)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", Generation: 4,
			Annotations: map[string]string{
				upgradeUIDAnnotation:      "upgrade",
				upgradeNameAnnotation:     "orders-pg18",
				upgradePhaseAnnotation:    string(plan.MajorUpgradePhaseReplicas),
				upgradeRevisionAnnotation: "12",
			},
		},
		Spec: api.MultiSitePostgresSpec{
			Postgres: api.PostgresSpec{SynchronousStandbyCount: 1},
		},
		Status: api.MultiSitePostgresStatus{
			ActiveRevision: 12,
			Sites:          []api.SiteRevisionStatus{{Name: "vic", AppliedRevision: 12}},
		},
	}
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"), Generation: 1,
		},
		Spec: api.PostgresUpgradeSpec{
			InstanceRef: "orders", TargetMajorVersion: 18, TargetImage: "postgres:18",
		},
		Status: api.PostgresUpgradeStatus{Phase: "ReseedingReplicas"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}, &api.PostgresUpgrade{}).
		WithObjects(instance, upgrade).Build()
	reconciler := PostgresUpgradeReconciler{
		Client: kube, Scheme: scheme, Now: func() time.Time { return now },
	}
	if _, err := reconciler.reconcileMajorUpgrade(context.Background(), upgrade, instance, now); err != nil {
		t.Fatal(err)
	}
	var currentUpgrade api.PostgresUpgrade
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &currentUpgrade); err != nil {
		t.Fatal(err)
	}
	if statusCondition(currentUpgrade.Status.Conditions, "Ready").Reason != "ReplicaSyncPending" {
		t.Fatalf("replica wait status = %#v", currentUpgrade.Status)
	}
	var currentInstance api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &currentInstance); err != nil {
		t.Fatal(err)
	}
	if currentInstance.Annotations[upgradePhaseAnnotation] != string(plan.MajorUpgradePhaseReplicas) {
		t.Fatalf("phase advanced before sync: %q", currentInstance.Annotations[upgradePhaseAnnotation])
	}
	currentInstance.Status.Conditions = []metav1.Condition{
		{Type: "TopologyReady", Status: metav1.ConditionTrue},
		{Type: "SynchronousReplicationReady", Status: metav1.ConditionTrue},
	}
	if err := kube.Status().Update(context.Background(), &currentInstance); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileMajorUpgrade(context.Background(), &currentUpgrade, &currentInstance, now); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &currentInstance); err != nil {
		t.Fatal(err)
	}
	if currentInstance.Annotations[upgradePhaseAnnotation] != string(plan.MajorUpgradePhaseRestoreWrites) {
		t.Fatalf("phase after sync = %q", currentInstance.Annotations[upgradePhaseAnnotation])
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &currentUpgrade); err != nil {
		t.Fatal(err)
	}
	if currentUpgrade.Status.WriteServiceRestoredAt != nil {
		t.Fatal("write restoration was recorded before restore-writes phase applied")
	}
}

func TestDatabaseDeletionBlocksWhenParentMissing(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 8, 0, 0, 0, time.UTC))
	database := &api.PostgresDatabase{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-api", UID: types.UID("database-uid"), Generation: 2,
			Finalizers: []string{childFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Spec: api.PostgresDatabaseSpec{
			InstanceRef: "orders", DatabaseName: "orders", DeletionPolicy: api.DeletionPolicyDelete,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresDatabase{}).
		WithObjects(database).Build()
	reconciler := PostgresDatabaseReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-api"},
	}); err != nil {
		t.Fatal(err)
	}
	var current api.PostgresDatabase
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(database), &current); err != nil {
		t.Fatal(err)
	}
	blocked := statusCondition(current.Status.Conditions, "DeletionBlocked")
	if blocked == nil || blocked.Reason != "ParentUnavailable" ||
		!controllerutil.ContainsFinalizer(&current, childFinalizer) {
		t.Fatalf("database deletion state = status %#v finalizers %#v", current.Status, current.Finalizers)
	}
}

func TestDatabaseDeletionWithParentIssuesCleanupDirective(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 8, 30, 0, 0, time.UTC))
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
	}
	database := &api.PostgresDatabase{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-api", UID: types.UID("database-uid"), Generation: 2,
			Finalizers: []string{childFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Spec: api.PostgresDatabaseSpec{
			InstanceRef: "orders", DatabaseName: "orders", DeletionPolicy: api.DeletionPolicyDelete,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresDatabase{}).
		WithObjects(instance, database).Build()
	reconciler := PostgresDatabaseReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-api"},
	}); err != nil {
		t.Fatal(err)
	}
	var directive corev1.ConfigMap
	if err := kube.Get(context.Background(), client.ObjectKey{
		Namespace: "platform", Name: "mspsql-database-orders-api",
	}, &directive); err != nil {
		t.Fatal(err)
	}
	if directive.Data["type"] != "Database" ||
		directive.Data["instanceRef"] != "orders" ||
		directive.Data["deleting"] != "true" ||
		len(directive.OwnerReferences) != 1 ||
		directive.OwnerReferences[0].Kind != "PostgresDatabase" {
		t.Fatalf("database cleanup directive = %#v", directive)
	}
	var current api.PostgresDatabase
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(database), &current); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&current, childFinalizer) {
		t.Fatalf("database finalizer removed before cleanup completed: %#v", current.Finalizers)
	}
	if current.Status.Operation == nil ||
		current.Status.Operation.OperationUID != "database-uid-2-true" ||
		current.Status.Operation.Phase != "Deleting" ||
		current.Status.Operation.Terminal {
		t.Fatalf("database cleanup operation = %#v", current.Status.Operation)
	}
}

func TestUserDeletionForceOrphansWhenParentMissing(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC))
	user := &api.PostgresUser{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-writer", Generation: 2,
			Finalizers: []string{childFinalizer}, DeletionTimestamp: &deletingAt,
			Annotations: map[string]string{forceOrphanAnnotation: "true"},
		},
		Spec: api.PostgresUserSpec{
			InstanceRef: "orders", RoleName: "orders_writer", DeletionPolicy: api.DeletionPolicyDelete,
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUser{}).
		WithObjects(user).Build()
	reconciler := PostgresUserReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-writer"},
	}); err != nil {
		t.Fatal(err)
	}
	var current api.PostgresUser
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(user), &current); apierrors.IsNotFound(err) {
		return
	} else if err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&current, childFinalizer) {
		t.Fatalf("user finalizer was not removed: %#v", current.Finalizers)
	}
}

func TestUserTerminalDeletionReleasesFinalizer(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 9, 30, 0, 0, time.UTC))
	user := &api.PostgresUser{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-writer", Generation: 2,
			Finalizers: []string{childFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Spec: api.PostgresUserSpec{
			InstanceRef: "orders", RoleName: "orders_writer", DeletionPolicy: api.DeletionPolicyDelete,
		},
		Status: api.PostgresUserStatus{Phase: "Deleted"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUser{}).
		WithObjects(user).Build()
	reconciler := PostgresUserReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders-writer"},
	}); err != nil {
		t.Fatal(err)
	}
	var current api.PostgresUser
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(user), &current); apierrors.IsNotFound(err) {
		return
	} else if err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&current, childFinalizer) {
		t.Fatalf("terminal user retained finalizer: %#v", current.Finalizers)
	}
}

func TestInstanceDeletionBlocksWhileChildDeclarationsExist(t *testing.T) {
	scheme := testScheme(t)
	deletingAt := metav1.NewTime(time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC))
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders", Generation: 4, UID: types.UID("orders-uid"),
			Finalizers: []string{instanceFinalizer}, DeletionTimestamp: &deletingAt,
		},
		Spec: api.MultiSitePostgresSpec{
			DeletionPolicy: api.DeletionPolicyDelete,
			Sites: []api.PostgresSiteSpec{{
				Name: "vic", SiteRegistrationRef: "vic", Namespace: "orders",
			}},
		},
	}
	database := &api.PostgresDatabase{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-api"},
		Spec: api.PostgresDatabaseSpec{
			InstanceRef: "orders", DatabaseName: "orders",
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.MultiSitePostgres{}).
		WithObjects(instance, database).Build()
	reconciler := MultiSitePostgresReconciler{Client: kube, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "platform", Name: "orders"},
	}); err != nil {
		t.Fatal(err)
	}
	var current api.MultiSitePostgres
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(instance), &current); err != nil {
		t.Fatal(err)
	}
	blocked := statusCondition(current.Status.Conditions, "DeletionBlocked")
	if blocked == nil || blocked.Reason != "ChildDeclarationsPresent" ||
		!strings.Contains(blocked.Message, "PostgresDatabase/orders-api") {
		t.Fatalf("parent deletion condition = %#v", blocked)
	}
	if current.Status.Operation == nil ||
		current.Status.Operation.OperationUID != "orders-uid-delete-4" ||
		current.Status.Operation.Phase != "DeletionBlocked" ||
		current.Status.Operation.LastErrorReason != "ChildDeclarationsPresent" {
		t.Fatalf("parent deletion operation = %#v", current.Status.Operation)
	}
	if !controllerutil.ContainsFinalizer(&current, instanceFinalizer) {
		t.Fatalf("parent finalized before child deletion: %#v", current.Finalizers)
	}
}

func TestUpgradeInstanceWatchIncludesPreflightBlockedUpgrade(t *testing.T) {
	scheme := testScheme(t)
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders"},
	}
	blocked := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-pg18"},
		Spec:       api.PostgresUpgradeSpec{InstanceRef: "orders"},
		Status:     api.PostgresUpgradeStatus{Phase: "Preflight"},
	}
	terminal := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-old"},
		Spec:       api.PostgresUpgradeSpec{InstanceRef: "orders"},
		Status:     api.PostgresUpgradeStatus{Phase: "Failed"},
	}
	other := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "inventory-pg18"},
		Spec:       api.PostgresUpgradeSpec{InstanceRef: "inventory"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(instance, blocked, terminal, other).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	requests := reconciler.upgradeRequestsForInstance(context.Background(), instance)
	if len(requests) != 1 ||
		requests[0].NamespacedName != (types.NamespacedName{Namespace: "platform", Name: "orders-pg18"}) {
		t.Fatalf("upgrade requests = %#v", requests)
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

//nolint:gocyclo // The retry lifecycle is asserted end-to-end to preserve status transitions.
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
		ready, result, err := reconciler.ensurePostUpgradeBackup(context.Background(), &current, now)
		if err != nil || ready || result.RequeueAfter != 0 {
			t.Fatalf("post-upgrade backup preflight = %t, result %#v, %v", ready, result, err)
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
	if ready, result, err := reconciler.ensurePostUpgradeBackup(context.Background(), &current, now); err != nil || ready ||
		result.RequeueAfter <= 0 {
		t.Fatalf("failed backup retry = %t, result %#v, %v", ready, result, err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.PostUpgradeBackupAttempt != 1 ||
		current.Status.PostUpgradeBackupRequestedAt != nil ||
		current.Status.Operation == nil ||
		current.Status.Operation.OperationUID != "upgrade-post-upgrade-backup-1" ||
		current.Status.Operation.Attempt != 1 ||
		current.Status.Operation.NextRetryAt == nil ||
		current.Status.Operation.LastErrorReason != "BackupFailed" ||
		current.Status.Operation.LastErrorMessage != "failed" {
		t.Fatalf("post-upgrade retry status = %#v", current.Status)
	}
	nextRetry := current.Status.Operation.NextRetryAt.DeepCopy()
	ready, result, err := reconciler.ensurePostUpgradeBackup(context.Background(), &current,
		nextRetry.Add(-time.Second))
	if err != nil || ready || result.RequeueAfter != time.Second {
		t.Fatalf("early retry wait = %t, result %#v, %v", ready, result, err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	ready, result, err = reconciler.ensurePostUpgradeBackup(context.Background(), &current, nextRetry.Time)
	if err != nil || ready || result.RequeueAfter != 0 {
		t.Fatalf("due retry request = %t, result %#v, %v", ready, result, err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.PostUpgradeBackupRequestedAt == nil ||
		current.Status.Operation.NextRetryAt != nil {
		t.Fatalf("due retry status = %#v", current.Status)
	}
}

func TestMajorUpgradePostUpgradeBackupRetryLimit(t *testing.T) {
	scheme := testScheme(t)
	upgrade := &api.PostgresUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "platform", Name: "orders-pg18", UID: types.UID("upgrade"), Generation: 1,
		},
		Spec: api.PostgresUpgradeSpec{InstanceRef: "orders"},
		Status: api.PostgresUpgradeStatus{
			Phase:                    "Finalizing",
			PostUpgradeBackupAttempt: maxPostUpgradeBackupAttempts,
			Conditions: []metav1.Condition{{
				Type: "PostUpgradeBackupReady", Status: metav1.ConditionFalse,
				Reason: "BackupFailed", Message: "repository denied access",
			}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.PostgresUpgrade{}).WithObjects(upgrade).Build()
	reconciler := PostgresUpgradeReconciler{Client: kube, Scheme: scheme}
	ready, result, err := reconciler.ensurePostUpgradeBackup(context.Background(), upgrade,
		time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC))
	if err != nil || ready || result.RequeueAfter != 0 {
		t.Fatalf("retry limit result = %t, result %#v, %v", ready, result, err)
	}
	var current api.PostgresUpgrade
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(upgrade), &current); err != nil {
		t.Fatal(err)
	}
	backup := statusCondition(current.Status.Conditions, "PostUpgradeBackupReady")
	readyCondition := statusCondition(current.Status.Conditions, "Ready")
	if current.Status.PostUpgradeBackupAttempt != maxPostUpgradeBackupAttempts ||
		backup == nil || backup.Reason != "ManualInterventionRequired" ||
		readyCondition == nil || readyCondition.Reason != "ManualInterventionRequired" ||
		current.Status.Operation == nil ||
		!current.Status.Operation.Terminal ||
		!current.Status.Operation.ManualInterventionRequired ||
		current.Status.Operation.LastErrorReason != "BackupFailed" ||
		current.Status.Operation.LastErrorMessage != "repository denied access" {
		t.Fatalf("manual intervention status = %#v", current.Status)
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
