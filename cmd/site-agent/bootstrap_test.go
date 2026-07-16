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

package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestScrubBootstrapCredentialRetainsRuntimeConfiguration(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "mspsql-agent", Name: "mspsql-agent-bootstrap",
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "contains-token",
			},
		},
		Data: map[string][]byte{
			"registration-token": []byte("single-use"),
			"registration-url":   []byte("https://hub.example"),
			"hub-address":        []byte("10.254.0.1:9444"),
			"hub-domain":         []byte("example"),
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	if err := scrubBootstrapCredential(context.Background(), kube, secret.Namespace); err != nil {
		t.Fatal(err)
	}
	var updated corev1.Secret
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(secret), &updated); err != nil {
		t.Fatal(err)
	}
	if _, found := updated.Data["registration-token"]; found {
		t.Fatal("single-use registration token remains in the bootstrap Secret")
	}
	if _, found := updated.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; found {
		t.Fatal("kubectl apply annotation retained the registration token")
	}
	if string(updated.Data["hub-address"]) != "10.254.0.1:9444" ||
		string(updated.Data["hub-domain"]) != "example" {
		t.Fatal("runtime hub configuration was removed with the credential")
	}
}
