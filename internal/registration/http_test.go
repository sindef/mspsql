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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		Client: kube, SystemNamespace: "system", PublicURL: "https://hub.example",
		HubDomain: "hub.example", HubAddress: "10.0.0.1:9444",
		AgentImage: "agent:test", WireGuardImage: "wireguard:test",
		Now: func() time.Time { return now },
	}

	get := httptest.NewRequest(http.MethodGet, "/"+token.Value+"/registration.yaml", nil)
	getResponse := httptest.NewRecorder()
	server.handle(getResponse, get)
	if getResponse.Code != http.StatusOK || !bytes.Contains(getResponse.Body.Bytes(), []byte("kind: Deployment")) {
		t.Fatalf("bundle response: code=%d body=%s", getResponse.Code, getResponse.Body.String())
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
		WireGuardPublicKey: "wireguard-public-key",
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
}
