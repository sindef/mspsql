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
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
	"github.com/sindef/mspsql/internal/vault"
)

func TestSecretMaterializerUsesDocumentedVaultSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/auth/kubernetes/login":
			_, _ = response.Write([]byte(`{"auth":{"client_token":"short-lived","lease_duration":600}}`))
		case "/v1/secret/data/postgres/orders":
			_, _ = response.Write([]byte(`{"data":{"data":{"superuserPassword":"super","replicationPassword":"repl"},"metadata":{"version":2}}}`))
		case "/v1/secret/data/pgpool/orders":
			_, _ = response.Write([]byte(`{"data":{"data":{"adminUsername":"admin","adminPassword":"pool"},"metadata":{"version":3}}}`))
		case "/v1/secret/data/backup/orders":
			_, _ = response.Write([]byte(`{"data":{"data":{"s3AccessKey":"access","s3SecretKey":"secret","repositoryCipherPassphrase":"cipher"},"metadata":{"version":4}}}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mspsql-agent", Name: "vault-ca"},
		Data:       map[string][]byte{"ca.crt": []byte("site-ca")},
	}).Build()
	materializer := SecretMaterializer{
		Client:          kube,
		SourceNamespace: "mspsql-agent",
		Token: func(_ context.Context, namespace, serviceAccount string) (string, error) {
			if namespace != "orders" || serviceAccount != workloadServiceAccount {
				t.Fatalf("token identity = %s/%s", namespace, serviceAccount)
			}
			return "projected", nil
		},
		Vault: func(auth api.VaultAuthSpec, caBundle []byte) (*vault.Client, error) {
			if string(caBundle) != "site-ca" {
				t.Fatalf("CA bundle = %q", caBundle)
			}
			return &vault.Client{
				Address: server.URL, AuthMount: auth.AuthMount, Role: auth.AuthRole, HTTP: server.Client(),
			}, nil
		},
	}
	desired := plan.SitePlan{
		InstanceUID: "instance", Revision: 7,
		Site: api.PostgresSiteSpec{
			Name: "vic", Namespace: "orders", Role: api.SiteRoleData,
			Components: api.SiteComponents{PgpoolReplicas: 1},
			VaultAuth: &api.VaultAuthSpec{
				AuthMount: "kubernetes", AuthRole: "orders-vic",
				CABundleSecretRef: &api.SecretKeyReference{Name: "vault-ca"},
			},
		},
		Credentials: api.InstanceCredentialsSpec{
			PostgresVaultRef: api.VaultSecretReference{Mount: "secret", Path: "postgres/orders"},
			PgpoolVaultRef:   api.VaultSecretReference{Mount: "secret", Path: "pgpool/orders"},
		},
		Backup: &api.BackupSpec{Repository: api.BackupRepositorySpec{
			CredentialVaultRef: api.VaultSecretReference{Mount: "secret", Path: "backup/orders"},
		}},
		TDE: api.TDESpec{Enabled: true, Vault: &api.TDEVaultSpec{
			KVMount: "tde", KeyPath: "postgres/orders", ProviderName: "orders",
			PrincipalKeyName: "orders-principal",
		}},
	}
	if err := materializer.Reconcile(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	assertSecretValue(t, kube, "postgres-auth", "replication-password", "repl")
	assertSecretValue(t, kube, "pgpool-auth", "admin-password", "pool")
	assertSecretValue(t, kube, "pgbackrest-repository", "repo-cipher-passphrase", "cipher")
	assertSecretValue(t, kube, "pg-tde-vault", "token", "short-lived")
	assertSecretValue(t, kube, "pg-tde-vault", "ca.crt", "site-ca")
}

func assertSecretValue(t *testing.T, kube client.Client, name, key, expected string) {
	t.Helper()
	var secret corev1.Secret
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "orders", Name: name}, &secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data[key]) != expected {
		t.Fatalf("Secret %s key %s = %q", name, key, secret.Data[key])
	}
}
