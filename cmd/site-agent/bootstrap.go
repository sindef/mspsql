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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sindef/mspsql/internal/registration"
	"golang.org/x/crypto/curve25519"
)

func bootstrapIfRequired(ctx context.Context, kube client.Client, namespace, identityPath,
	bootstrapPath, clusterUID string,
) (bool, error) {
	if _, err := os.Stat(identityPath); err == nil {
		return false, scrubBootstrapCredential(ctx, kube, namespace)
	} else if !os.IsNotExist(err) {
		return false, err
	}
	publicURL, err := readTrimmed(bootstrapPath + "/registration-url")
	if err != nil {
		return false, err
	}
	token, err := readTrimmed(bootstrapPath + "/registration-token")
	if err != nil {
		return false, err
	}
	tlsKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return false, err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "mspsql-site-agent-bootstrap"},
	}, tlsKey)
	if err != nil {
		return false, err
	}
	wireGuardPrivate := make([]byte, curve25519.ScalarSize)
	if _, err := rand.Read(wireGuardPrivate); err != nil {
		return false, err
	}
	wireGuardPrivate[0] &= 248
	wireGuardPrivate[31] &= 127
	wireGuardPrivate[31] |= 64
	wireGuardPublic, err := curve25519.X25519(wireGuardPrivate, curve25519.Basepoint)
	if err != nil {
		return false, err
	}
	requestBody, err := json.Marshal(registration.BindRequest{
		ClusterUID:         clusterUID,
		CSRPEM:             string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})),
		WireGuardPublicKey: base64.StdEncoding.EncodeToString(wireGuardPublic),
	})
	if err != nil {
		return false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(publicURL, "/")+"/"+token+"/bind", bytes.NewReader(requestBody))
	if err != nil {
		return false, err
	}
	request.Header.Set("Content-Type", "application/json")
	httpClient := &http.Client{Timeout: 30 * time.Second}
	response, err := httpClient.Do(request)
	if err != nil {
		return false, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return false, fmt.Errorf("registration failed with HTTP %d: %s",
			response.StatusCode, strings.TrimSpace(string(message)))
	}
	var binding registration.BindResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&binding); err != nil {
		return false, err
	}
	keyDER, err := x509.MarshalECPrivateKey(tlsKey)
	if err != nil {
		return false, err
	}
	identity := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "mspsql-agent-identity"},
		Immutable:  ptr(true),
		StringData: map[string]string{
			"tls.crt":               binding.CertificatePEM,
			"tls.key":               string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})),
			"ca.crt":                binding.CABundlePEM,
			"plan-public-key":       binding.PlanPublicKey,
			"wireguard-private-key": base64.StdEncoding.EncodeToString(wireGuardPrivate),
			"wireguard-public-key":  base64.StdEncoding.EncodeToString(wireGuardPublic),
			"wg0.conf": "[Interface]\nPrivateKey = " +
				base64.StdEncoding.EncodeToString(wireGuardPrivate) + "\n" + binding.WireGuardPeerState,
		},
	}
	if err := kube.Create(ctx, identity); err != nil {
		return false, err
	}
	return true, scrubBootstrapCredential(ctx, kube, namespace)
}

func scrubBootstrapCredential(ctx context.Context, kube client.Client, namespace string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var secret corev1.Secret
		key := client.ObjectKey{Namespace: namespace, Name: "mspsql-agent-bootstrap"}
		if err := kube.Get(ctx, key, &secret); err != nil {
			return client.IgnoreNotFound(err)
		}
		_, hasToken := secret.Data["registration-token"]
		_, hasApplyAnnotation := secret.Annotations["kubectl.kubernetes.io/last-applied-configuration"]
		if !hasToken && !hasApplyAnnotation {
			return nil
		}
		delete(secret.Data, "registration-token")
		delete(secret.Annotations, "kubectl.kubernetes.io/last-applied-configuration")
		return kube.Update(ctx, &secret)
	})
}

func readTrimmed(path string) (string, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func ptr[T any](value T) *T {
	return &value
}
