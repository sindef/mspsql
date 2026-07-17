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

package registration

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestRegistrationBindingConsumesToken(t *testing.T) {
	now := time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC)
	token, err := NewToken(now)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	site := &api.SiteRegistration{ObjectMeta: metav1.ObjectMeta{
		Name: "vic", UID: types.UID("site-uid"),
	}}
	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "registration-site-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: api.GroupVersion.String(), Kind: "SiteRegistration",
				Name: site.Name, UID: site.UID,
			}},
		},
		Data: map[string][]byte{
			"sha256": token.Hash, "expiresAt": []byte(token.ExpiresAt.Format(time.RFC3339Nano)),
		},
	}
	signingKey := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "mspsql-plan-signing-key"},
		Data:       map[string][]byte{"publicKey": []byte("public-key")},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.SiteRegistration{}).
		WithObjects(site, tokenSecret, signingKey).Build()
	server := HTTPServer{
		Client: kube, SystemNamespace: "system", PublicURL: "http://hub.example:30082",
		HubDomain: "hub.example", HubAddress: "10.0.0.1:9444",
		AgentImage: "agent:test", WireGuardImage: "wireguard:test",
		WireGuardNetworkCIDR: "10.254.0.0/16", WireGuardEndpoint: "wireguard.example:51820",
		Now: func() time.Time { return now },
	}

	get := httptest.NewRequest(http.MethodGet, "/"+token.Value+"/registration.yaml", nil)
	getResponse := httptest.NewRecorder()
	server.handle(getResponse, get)
	if getResponse.Code != http.StatusOK || !bytes.Contains(getResponse.Body.Bytes(), []byte("kind: Deployment")) {
		t.Fatalf("bundle response: code=%d body=%s", getResponse.Code, getResponse.Body.String())
	}
	for _, expected := range []string{
		"kind: NetworkPolicy", "kind: Service", "name: mspsql-agent-metrics",
		"containerPort: 8080", "port: 30082", "- events.k8s.io",
	} {
		if !bytes.Contains(getResponse.Body.Bytes(), []byte(expected)) {
			t.Fatalf("registration bundle is missing %q:\n%s", expected, getResponse.Body.String())
		}
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "bootstrap"},
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(BindRequest{
		ClusterUID:         "cluster-uid",
		CSRPEM:             string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})),
		WireGuardPublicKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
	})
	if err != nil {
		t.Fatal(err)
	}
	post := httptest.NewRequest(http.MethodPost, "/"+token.Value+"/bind", bytes.NewReader(body))
	postResponse := httptest.NewRecorder()
	server.handle(postResponse, post)
	if postResponse.Code != http.StatusOK {
		t.Fatalf("bind response: code=%d body=%s", postResponse.Code, postResponse.Body.String())
	}
	var updated api.SiteRegistration
	if err := kube.Get(context.Background(), types.NamespacedName{Name: "vic"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.ClusterUID != "cluster-uid" {
		t.Fatalf("cluster UID = %q", updated.Status.ClusterUID)
	}
	var consumed corev1.Secret
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: "system", Name: tokenSecret.Name,
	}, &consumed); !apierrors.IsNotFound(err) {
		t.Fatalf("token Secret still exists: %v", err)
	}
	var peer corev1.Secret
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: "system", Name: "wireguard-peer-site-uid",
	}, &peer); err != nil {
		t.Fatal(err)
	}
	if len(peer.OwnerReferences) != 1 || peer.OwnerReferences[0].UID != site.UID {
		t.Fatalf("peer owner references = %#v", peer.OwnerReferences)
	}
}

func TestRevokedRegistrationTokenIsRejected(t *testing.T) {
	now := time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC)
	token, err := NewToken(now)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	site := &api.SiteRegistration{
		ObjectMeta: metav1.ObjectMeta{Name: "vic", UID: types.UID("site-uid")},
		Spec:       api.SiteRegistrationSpec{Revoked: true},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "registration-site-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: api.GroupVersion.String(), Kind: "SiteRegistration",
				Name: site.Name, UID: site.UID,
			}},
		},
		Data: map[string][]byte{
			"sha256": token.Hash, "expiresAt": []byte(token.ExpiresAt.Format(time.RFC3339Nano)),
		},
	}
	server := HTTPServer{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(site, secret).Build(),
		SystemNamespace: "system", Now: func() time.Time { return now },
	}
	if _, _, err := server.authorize(context.Background(), token.Value); err == nil ||
		!strings.Contains(err.Error(), "revoked") {
		t.Fatalf("authorize error = %v", err)
	}
}

func TestAgentDeploymentCanUseDirectNetwork(t *testing.T) {
	site := &api.SiteRegistration{ObjectMeta: metav1.ObjectMeta{
		Name: "vic", UID: types.UID("site-uid"),
	}}
	raw, err := json.Marshal(agentDeployment(site, "agent:test", ""))
	if err != nil {
		t.Fatal(err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal(raw, &deployment); err != nil {
		t.Fatal(err)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(deployment.Spec.Template.Spec.Containers))
	}
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name == "tun" {
			t.Fatal("direct-network deployment contains tun volume")
		}
	}
}

func TestAgentDeploymentRequestsTunWithoutHostPath(t *testing.T) {
	site := &api.SiteRegistration{ObjectMeta: metav1.ObjectMeta{
		Name: "vic", UID: types.UID("site-uid"),
	}}
	raw, err := json.Marshal(agentDeployment(site, "agent:test", "wireguard:test"))
	if err != nil {
		t.Fatal(err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal(raw, &deployment); err != nil {
		t.Fatal(err)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 2 {
		t.Fatalf("containers = %d, want 2", len(deployment.Spec.Template.Spec.Containers))
	}
	if deployment.Spec.Strategy.RollingUpdate == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue() != 0 {
		t.Fatalf("agent rolling strategy = %#v", deployment.Spec.Strategy)
	}
	if deployment.Spec.Template.Spec.SecurityContext == nil ||
		deployment.Spec.Template.Spec.SecurityContext.SeccompProfile == nil ||
		deployment.Spec.Template.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("agent pod security context = %#v", deployment.Spec.Template.Spec.SecurityContext)
	}
	wireGuard := deployment.Spec.Template.Spec.Containers[1]
	if wireGuard.Resources.Limits.Name("multisite-postgres.dev/tun", resource.DecimalSI).String() != "1" {
		t.Fatalf("WireGuard TUN limit = %s",
			wireGuard.Resources.Limits.Name("multisite-postgres.dev/tun", resource.DecimalSI).String())
	}
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.HostPath != nil {
			t.Fatalf("agent workload contains hostPath volume %q", volume.Name)
		}
	}
	agent := deployment.Spec.Template.Spec.Containers[0]
	if agent.SecurityContext == nil || agent.SecurityContext.RunAsNonRoot == nil ||
		!*agent.SecurityContext.RunAsNonRoot || agent.SecurityContext.ReadOnlyRootFilesystem == nil ||
		!*agent.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("site agent security context = %#v", agent.SecurityContext)
	}
	assertAgentVolumeAccess(t, deployment.Spec.Template.Spec.SecurityContext,
		deployment.Spec.Template.Spec.Volumes)
	if agent.ReadinessProbe == nil || agent.ReadinessProbe.Exec == nil ||
		len(agent.ReadinessProbe.Exec.Command) != 2 ||
		agent.ReadinessProbe.Exec.Command[0] != "/site-agent" {
		t.Fatalf("site agent readiness probe = %#v", agent.ReadinessProbe)
	}
	env := map[string]corev1.EnvVar{}
	for _, variable := range agent.Env {
		env[variable.Name] = variable
	}
	for name, fieldPath := range map[string]string{
		"POD_NAME": "metadata.name", "POD_NAMESPACE": "metadata.namespace",
	} {
		variable, found := env[name]
		if !found || variable.ValueFrom == nil || variable.ValueFrom.FieldRef == nil ||
			variable.ValueFrom.FieldRef.FieldPath != fieldPath {
			t.Fatalf("%s downward API environment = %#v", name, variable)
		}
	}
}

func assertAgentVolumeAccess(t *testing.T, securityContext *corev1.PodSecurityContext,
	volumes []corev1.Volume,
) {
	t.Helper()
	if securityContext == nil || securityContext.FSGroup == nil || *securityContext.FSGroup != 65532 {
		t.Fatalf("agent pod fsGroup = %#v", securityContext)
	}
	for _, volume := range volumes {
		if (volume.Name == "bootstrap" || volume.Name == "identity") &&
			(volume.Secret == nil || volume.Secret.DefaultMode == nil ||
				*volume.Secret.DefaultMode != 0o440) {
			t.Fatalf("%s Secret mode = %#v", volume.Name, volume.Secret)
		}
	}
}

func TestAgentWireGuardStopsOnLeadershipLoss(t *testing.T) {
	site := &api.SiteRegistration{ObjectMeta: metav1.ObjectMeta{
		Name: "vic", UID: types.UID("site-uid"),
	}}
	raw, err := json.Marshal(agentDeployment(site, "agent:test", "wireguard:test"))
	if err != nil {
		t.Fatal(err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal(raw, &deployment); err != nil {
		t.Fatal(err)
	}
	command := deployment.Spec.Template.Spec.Containers[1].Command
	if len(command) != 3 || !strings.Contains(command[2], "ip link delete wg0") ||
		!strings.Contains(command[2], "wg-quick down") ||
		!strings.Contains(command[2], "WG_QUICK_USERSPACE_IMPLEMENTATION=wireguard-go") ||
		strings.Contains(command[2], "wireguard-go wg0 &") ||
		!strings.Contains(command[2], "while [ -f /run/mspsql/leader ]") {
		t.Fatalf("WireGuard leadership cleanup command = %#v", command)
	}
}
