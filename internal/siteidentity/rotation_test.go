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

package siteidentity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCertificateRotationSwitchesDeploymentIdentity(t *testing.T) {
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	caCertificate, caKey, caPEM := testCA(t, now)
	oldCertificate, oldKey := testCertificate(t, caCertificate, caKey, "site-uid",
		now.Add(-time.Hour), now.Add(4*time.Hour))
	mountPath := t.TempDir()
	writeIdentity(t, mountPath, oldCertificate, oldKey, caPEM)

	replicas := int32(2)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "mspsql-agent", Name: "mspsql-agent", Generation: 3,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: "mspsql-agent-identity",
				}},
			}}}},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 3, UpdatedReplicas: 2, AvailableReplicas: 2,
		},
	}
	identity := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mspsql-agent", Name: "mspsql-agent-identity"},
		Data: map[string][]byte{
			"tls.crt": oldCertificate, "tls.key": oldKey, "ca.crt": caPEM,
			"plan-public-key": []byte("plan-key"),
		},
	}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(deployment, identity).Build()
	rotator := &Rotator{
		Client: kube, Namespace: "mspsql-agent", DeploymentName: "mspsql-agent",
		MountPath: mountPath, RegistrationUID: "site-uid", Now: func() time.Time { return now },
	}

	request, err := rotator.Request(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.RequestId == "" {
		t.Fatal("expiring certificate did not produce a signing request")
	}
	csrBlock, _ := pem.Decode(request.CsrPem)
	csr, err := x509.ParseCertificateRequest(csrBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	renewed := signCSR(t, caCertificate, caKey, csr, "site-uid", now, now.Add(24*time.Hour))
	if err := rotator.Install(context.Background(), &controlv1.CertificateResponse{
		RequestId: request.RequestId, CertificatePem: renewed, CaBundlePem: caPEM,
		NotAfter: timestamppb.New(now.Add(24 * time.Hour)),
	}); err != nil {
		t.Fatal(err)
	}

	var updated appsv1.Deployment
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(deployment), &updated); err != nil {
		t.Fatal(err)
	}
	secretName := updated.Spec.Template.Spec.Volumes[0].Secret.SecretName
	if secretName == "mspsql-agent-identity" || secretName == "" {
		t.Fatalf("Deployment identity Secret = %q", secretName)
	}
	var generated corev1.Secret
	if err := kube.Get(context.Background(), client.ObjectKey{
		Namespace: "mspsql-agent", Name: secretName,
	}, &generated); err != nil {
		t.Fatal(err)
	}
	if generated.Immutable == nil || !*generated.Immutable {
		t.Fatal("renewed identity Secret is mutable")
	}
	if string(generated.Data["plan-public-key"]) != "plan-key" {
		t.Fatal("renewal did not retain non-certificate identity data")
	}

	request, err = rotator.Request(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if request != nil {
		t.Fatal("old pod requested another certificate while its rollout was pending")
	}
}

func TestCertificateRotationSkipsHealthyCertificate(t *testing.T) {
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	caCertificate, caKey, caPEM := testCA(t, now)
	certificate, key := testCertificate(t, caCertificate, caKey, "site-uid",
		now.Add(-time.Hour), now.Add(12*time.Hour))
	mountPath := t.TempDir()
	writeIdentity(t, mountPath, certificate, key, caPEM)
	replicas := int32(2)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "agent", Name: "agent"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: volumeName, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: "identity",
				}},
			}}}},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "agent", Name: "identity"},
		Data:       map[string][]byte{"tls.crt": certificate},
	}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(deployment, secret).Build()
	rotator := &Rotator{
		Client: kube, Namespace: "agent", DeploymentName: "agent", MountPath: mountPath,
		RegistrationUID: "site-uid", Now: func() time.Time { return now },
	}
	request, err := rotator.Request(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if request != nil {
		t.Fatal("healthy certificate produced a signing request")
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func testCA(t *testing.T, now time.Time) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(365 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func testCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, uid string,
	notBefore, notAfter time.Time,
) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: uid},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatal(err)
	}
	certificate := signCSR(t, ca, caKey, csr, uid, notBefore, notAfter)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func signCSR(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey,
	csr *x509.CertificateRequest, uid string, notBefore, notAfter time.Time,
) []byte {
	t.Helper()
	identity, _ := url.Parse("spiffe://multisite-postgres.dev/site/" + uid)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(notAfter.Unix()), Subject: pkix.Name{CommonName: uid},
		URIs: []*url.URL{identity}, NotBefore: notBefore, NotAfter: notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, csr.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeIdentity(t *testing.T, directory string, certificate, key, ca []byte) {
	t.Helper()
	values := map[string][]byte{
		"tls.crt": certificate, "tls.key": key, "ca.crt": ca,
		"plan-public-key": []byte("plan-key"),
	}
	for name, value := range values {
		if err := os.WriteFile(filepath.Join(directory, name), value, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
