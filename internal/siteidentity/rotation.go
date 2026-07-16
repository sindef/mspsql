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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controlv1 "github.com/sindef/mspsql/gen/control/v1"
)

const (
	managedLabel = "multisite-postgres.dev/agent-identity"
	volumeName   = "identity"
)

type Rotator struct {
	Client          client.Client
	Namespace       string
	DeploymentName  string
	MountPath       string
	RegistrationUID string
	RenewBefore     time.Duration
	Now             func() time.Time

	pending *pendingRequest
}

type pendingRequest struct {
	id     string
	keyPEM []byte
}

func (r *Rotator) CurrentCertificate() (*x509.Certificate, error) {
	value, err := os.ReadFile(filepath.Join(r.MountPath, "tls.crt"))
	if err != nil {
		return nil, err
	}
	return parseCertificate(value)
}

func (r *Rotator) Request(ctx context.Context) (*controlv1.CertificateSigningRequest, error) {
	current, err := r.CurrentCertificate()
	if err != nil {
		return nil, err
	}
	deployment, desiredCertificate, err := r.deploymentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(current.Raw, desiredCertificate.Raw) {
		return nil, nil
	}
	if err := r.cleanup(ctx, deployment, current); err != nil {
		return nil, err
	}
	renewBefore := r.RenewBefore
	if renewBefore == 0 {
		renewBefore = 8 * time.Hour
	}
	if r.now().Add(renewBefore).Before(current.NotAfter) {
		return nil, nil
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: r.RegistrationUID},
	}, privateKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	requestID, err := randomID()
	if err != nil {
		return nil, err
	}
	r.pending = &pendingRequest{
		id:     requestID,
		keyPEM: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
	return &controlv1.CertificateSigningRequest{
		RequestId: requestID,
		CsrPem:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}),
	}, nil
}

func (r *Rotator) Install(ctx context.Context, response *controlv1.CertificateResponse) error {
	if r.pending == nil || response.RequestId != r.pending.id {
		return errors.New("certificate response does not match the pending request")
	}
	certificate, err := r.validateResponse(response)
	if err != nil {
		return err
	}
	data, err := r.readCurrentIdentity()
	if err != nil {
		return err
	}
	data["tls.crt"] = response.CertificatePem
	data["tls.key"] = r.pending.keyPEM
	data["ca.crt"] = response.CaBundlePem
	sum := sha256.Sum256(certificate.Raw)
	name := "mspsql-agent-identity-" + hex.EncodeToString(sum[:6])
	immutable := true
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Namespace,
			Name:      name,
			Labels:    map[string]string{managedLabel: "true"},
		},
		Immutable: &immutable,
		Data:      data,
	}
	if err := r.Client.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		var existing corev1.Secret
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(secret), &existing); err != nil {
			return err
		}
		if !bytes.Equal(existing.Data["tls.crt"], response.CertificatePem) {
			return fmt.Errorf("identity Secret %s already exists with different certificate data", name)
		}
	}
	if err := r.useSecret(ctx, name, sum[:]); err != nil {
		return err
	}
	r.pending = nil
	return nil
}

func (r *Rotator) validateResponse(response *controlv1.CertificateResponse) (*x509.Certificate, error) {
	certificate, err := parseCertificate(response.CertificatePem)
	if err != nil {
		return nil, err
	}
	if _, err := tlsKeyPair(response.CertificatePem, r.pending.keyPEM); err != nil {
		return nil, fmt.Errorf("renewed certificate does not match generated private key: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(response.CaBundlePem) {
		return nil, errors.New("renewed certificate response contains no CA certificates")
	}
	if _, err := certificate.Verify(x509.VerifyOptions{
		Roots: roots, CurrentTime: r.now(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return nil, fmt.Errorf("verify renewed certificate: %w", err)
	}
	expected, _ := url.Parse("spiffe://multisite-postgres.dev/site/" + r.RegistrationUID)
	if certificate.Subject.CommonName != r.RegistrationUID || len(certificate.URIs) != 1 ||
		certificate.URIs[0].String() != expected.String() {
		return nil, errors.New("renewed certificate identity does not match the site registration")
	}
	if response.NotAfter == nil || !response.NotAfter.AsTime().Equal(certificate.NotAfter) {
		return nil, errors.New("renewed certificate expiry does not match the signed certificate")
	}
	return certificate, nil
}

func (r *Rotator) deploymentIdentity(ctx context.Context) (*appsv1.Deployment, *x509.Certificate, error) {
	var deployment appsv1.Deployment
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: r.Namespace, Name: r.DeploymentName,
	}, &deployment); err != nil {
		return nil, nil, err
	}
	secretName := ""
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name == volumeName && volume.Secret != nil {
			secretName = volume.Secret.SecretName
			break
		}
	}
	if secretName == "" {
		return nil, nil, errors.New("agent Deployment has no identity Secret volume")
	}
	var secret corev1.Secret
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: r.Namespace, Name: secretName,
	}, &secret); err != nil {
		return nil, nil, err
	}
	certificate, err := parseCertificate(secret.Data["tls.crt"])
	return &deployment, certificate, err
}

func (r *Rotator) useSecret(ctx context.Context, name string, certificateHash []byte) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var deployment appsv1.Deployment
		key := client.ObjectKey{Namespace: r.Namespace, Name: r.DeploymentName}
		if err := r.Client.Get(ctx, key, &deployment); err != nil {
			return err
		}
		updated := false
		for i := range deployment.Spec.Template.Spec.Volumes {
			volume := &deployment.Spec.Template.Spec.Volumes[i]
			if volume.Name == volumeName && volume.Secret != nil && volume.Secret.SecretName != name {
				volume.Secret.SecretName = name
				updated = true
			}
		}
		if !updated {
			return nil
		}
		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = map[string]string{}
		}
		deployment.Spec.Template.Annotations["multisite-postgres.dev/agent-certificate"] =
			hex.EncodeToString(certificateHash[:8])
		return r.Client.Update(ctx, &deployment)
	})
}

func (r *Rotator) cleanup(ctx context.Context, deployment *appsv1.Deployment,
	current *x509.Certificate,
) error {
	if deployment.Spec.Replicas == nil {
		return errors.New("agent Deployment has no replica count")
	}
	if deployment.Status.ObservedGeneration != deployment.Generation ||
		deployment.Status.UpdatedReplicas != *deployment.Spec.Replicas ||
		deployment.Status.AvailableReplicas != *deployment.Spec.Replicas {
		return nil
	}
	var secrets corev1.SecretList
	if err := r.Client.List(ctx, &secrets, client.InNamespace(r.Namespace),
		client.MatchingLabels{managedLabel: "true"}); err != nil {
		return err
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		certificate, err := parseCertificate(secret.Data["tls.crt"])
		if err != nil {
			return fmt.Errorf("parse managed identity Secret %s: %w", secret.Name, err)
		}
		if bytes.Equal(certificate.Raw, current.Raw) {
			continue
		}
		if err := r.Client.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *Rotator) readCurrentIdentity() (map[string][]byte, error) {
	keys := []string{
		"ca.crt", "plan-public-key", "tls.crt", "tls.key",
		"wg0.conf", "wireguard-private-key", "wireguard-public-key",
	}
	data := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := os.ReadFile(filepath.Join(r.MountPath, key))
		if err != nil {
			if os.IsNotExist(err) && (key == "wg0.conf" ||
				key == "wireguard-private-key" || key == "wireguard-public-key") {
				continue
			}
			return nil, err
		}
		data[key] = value
	}
	return data, nil
}

func parseCertificate(value []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(value)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("identity contains no PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func tlsKeyPair(certificatePEM, privateKeyPEM []byte) (*ecdsa.PrivateKey, error) {
	certificateBlock, _ := pem.Decode(certificatePEM)
	keyBlock, _ := pem.Decode(privateKeyPEM)
	if certificateBlock == nil || keyBlock == nil {
		return nil, errors.New("certificate or private key PEM is invalid")
	}
	certificate, err := x509.ParseCertificate(certificateBlock.Bytes)
	if err != nil {
		return nil, err
	}
	privateKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, err
	}
	public, ok := certificate.PublicKey.(*ecdsa.PublicKey)
	if !ok || !public.Equal(&privateKey.PublicKey) {
		return nil, errors.New("certificate public key differs from private key")
	}
	return privateKey, nil
}

func randomID() (string, error) {
	value, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%032x", value), nil
}

func (r *Rotator) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
