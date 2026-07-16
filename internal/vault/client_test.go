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

package vault

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestKubernetesLoginAndKV2Read(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/auth/site/login":
			var login map[string]string
			if err := json.NewDecoder(request.Body).Decode(&login); err != nil {
				t.Fatal(err)
			}
			if login["jwt"] != "projected-token" || login["role"] != "orders-vic" {
				t.Fatalf("login = %#v", login)
			}
			_, _ = response.Write([]byte(`{"auth":{"client_token":"vault-token","lease_duration":600,"renewable":true}}`))
		case "/v1/secret/data/postgres/orders/bootstrap":
			if request.Header.Get("X-Vault-Token") != "vault-token" {
				t.Fatal("Vault token was not sent")
			}
			_, _ = response.Write([]byte(`{"data":{"data":{"superuserPassword":"a","replicationPassword":"b"},"metadata":{"version":7}}}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client := Client{Address: server.URL, AuthMount: "site", Role: "orders-vic", HTTP: server.Client()}
	token, err := client.LoginKubernetes(context.Background(), "projected-token")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := client.ReadKV2(context.Background(), token.Value, api.VaultSecretReference{
		Mount: "secret", Path: "postgres/orders/bootstrap",
	})
	if err != nil {
		t.Fatal(err)
	}
	if secret.Version != 7 || secret.Data["replicationPassword"] != "b" {
		t.Fatalf("secret = %#v", secret)
	}
	if err := RequireFields(secret, "superuserPassword", "replicationPassword"); err != nil {
		t.Fatal(err)
	}
}

func TestKV2RejectsNonStringValues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`{"data":{"data":{"password":{"nested":true}},"metadata":{"version":1}}}`))
	}))
	defer server.Close()
	client := Client{Address: server.URL, HTTP: server.Client()}
	if _, err := client.ReadKV2(context.Background(), "token", api.VaultSecretReference{
		Mount: "secret", Path: "invalid",
	}); err == nil {
		t.Fatal("non-string Vault value was accepted")
	}
}

func TestNewClientRejectsInvalidCABundle(t *testing.T) {
	if _, err := NewClient(api.VaultAuthSpec{Address: "https://vault.example"}, []byte("invalid")); err == nil {
		t.Fatal("invalid Vault CA bundle was accepted")
	}
}
