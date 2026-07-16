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
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

func TestPatroniObserverReportsHealthyTopology(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/cluster" {
			http.NotFound(response, request)
			return
		}
		_, _ = response.Write([]byte(`{"members":[
			{"name":"postgres-vic-0","role":"leader","state":"running"},
			{"name":"postgres-qld-0","role":"sync_standby","state":"streaming"},
			{"name":"postgres-vic-1","role":"replica","state":"streaming"}]}`))
	}))
	defer server.Close()

	certificate := server.Certificate()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: "postgres-vic-0-tls"},
		Data:       map[string][]byte{"ca.crt": caPEM},
	}).Build()
	observer := PatroniObserver{
		Client: kube,
		HTTP: func(_ *x509.CertPool) *http.Client {
			return server.Client()
		},
	}
	originalTransport := observer.HTTP(nil).Transport
	observer.HTTP = func(_ *x509.CertPool) *http.Client {
		return &http.Client{Transport: rewriteTransport{
			target: server.URL, delegate: originalTransport,
		}}
	}
	topology, err := observer.Observe(context.Background(), plan.SitePlan{Site: api.PostgresSiteSpec{
		Name: "vic", Namespace: "orders", Components: api.SiteComponents{PostgresReplicas: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if topology.Primary != "postgres-vic-0" ||
		len(topology.SynchronousStandbys) != 1 ||
		topology.SynchronousStandbys[0] != "postgres-qld-0" {
		t.Fatalf("topology = %#v", topology)
	}
}

type rewriteTransport struct {
	target   string
	delegate http.RoundTripper
}

func (r rewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	cloned := request.Clone(request.Context())
	target, _ := cloned.URL.Parse(r.target + request.URL.Path)
	cloned.URL = target
	return r.delegate.RoundTrip(cloned)
}
