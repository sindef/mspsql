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
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/wireguard"
	"sigs.k8s.io/yaml"
)

const registrationCASecret = "mspsql-registration-ca"

type HTTPServer struct {
	Address              string
	Client               client.Client
	SystemNamespace      string
	PublicURL            string
	HubDomain            string
	HubAddress           string
	AgentImage           string
	WireGuardImage       string
	WireGuardNetworkCIDR string
	WireGuardEndpoint    string
	Now                  func() time.Time
}

type BindRequest struct {
	ClusterUID         string `json:"clusterUID"`
	CSRPEM             string `json:"csrPEM"`
	WireGuardPublicKey string `json:"wireGuardPublicKey"`
}

type BindResponse struct {
	CertificatePEM     string `json:"certificatePEM"`
	CABundlePEM        string `json:"caBundlePEM"`
	PlanPublicKey      string `json:"planPublicKey"`
	WireGuardPeerState string `json:"wireGuardPeerState"`
}

func (s *HTTPServer) Start(ctx context.Context) error {
	server := &http.Server{
		Addr: s.Address, Handler: http.HandlerFunc(s.handle),
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	err := server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *HTTPServer) NeedLeaderElection() bool {
	return false
}

func (s *HTTPServer) handle(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Referrer-Policy", "no-referrer")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	token, action, ok := parseCapabilityPath(request.URL.Path)
	if !ok {
		http.NotFound(response, request)
		return
	}
	site, secret, err := s.authorize(request.Context(), token)
	if err != nil {
		http.Error(response, "registration capability is invalid or expired", http.StatusUnauthorized)
		return
	}
	switch {
	case request.Method == http.MethodGet && action == "registration.yaml":
		bundle, err := s.bundle(site, token)
		if err != nil {
			http.Error(response, "could not generate registration bundle", http.StatusInternalServerError)
			return
		}
		response.Header().Set("Content-Type", "application/yaml")
		_, _ = response.Write(bundle)
	case request.Method == http.MethodPost && action == "bind":
		s.bind(response, request, site, secret)
	default:
		response.Header().Set("Allow", "GET, POST")
		http.Error(response, "method or action is not supported", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) authorize(ctx context.Context, value string) (*api.SiteRegistration,
	*corev1.Secret, error,
) {
	var secrets corev1.SecretList
	if err := s.Client.List(ctx, &secrets, client.InNamespace(s.SystemNamespace)); err != nil {
		return nil, nil, err
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if !strings.HasPrefix(secret.Name, "registration-") {
			continue
		}
		expiresAt, err := time.Parse(time.RFC3339Nano, string(secret.Data["expiresAt"]))
		if err != nil || Verify(value, secret.Data["sha256"], expiresAt, s.now()) != nil {
			continue
		}
		for _, owner := range secret.OwnerReferences {
			if owner.Kind != "SiteRegistration" {
				continue
			}
			var site api.SiteRegistration
			if err := s.Client.Get(ctx, client.ObjectKey{Name: owner.Name}, &site); err != nil {
				return nil, nil, err
			}
			if site.UID != owner.UID {
				return nil, nil, fmt.Errorf("registration owner UID changed")
			}
			if site.Spec.Revoked {
				return nil, nil, fmt.Errorf("site registration is revoked")
			}
			return &site, secret, nil
		}
	}
	return nil, nil, fmt.Errorf("registration token not found")
}

func (s *HTTPServer) bundle(site *api.SiteRegistration, token string) ([]byte, error) {
	objects := []any{
		map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "mspsql-agent"}},
		map[string]any{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{
			"name": "mspsql-agent", "namespace": "mspsql-agent"}},
		map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": map[string]any{"name": "mspsql-agent"},
			"rules": []any{
				rule([]string{""}, []string{"namespaces", "services", "serviceaccounts", "configmaps", "persistentvolumeclaims", "pods", "secrets"},
					[]string{"get", "list", "watch", "create", "update", "patch", "delete"}),
				rule([]string{""}, []string{"serviceaccounts/token"}, []string{"create"}),
				rule([]string{"apps"}, []string{"deployments", "statefulsets"},
					[]string{"get", "list", "watch", "create", "update", "patch", "delete"}),
				rule([]string{"batch"}, []string{"jobs", "cronjobs"},
					[]string{"get", "list", "watch", "create", "update", "patch", "delete"}),
				rule([]string{"coordination.k8s.io"}, []string{"leases"},
					[]string{"get", "list", "watch", "create", "update", "patch", "delete"}),
				rule([]string{"cert-manager.io"}, []string{"certificates"},
					[]string{"get", "list", "watch", "create", "update", "patch", "delete"}),
				rule([]string{"cert-manager.io"}, []string{"issuers", "clusterissuers"},
					[]string{"get", "list", "watch"}),
				rule([]string{"storage.k8s.io"}, []string{"storageclasses"}, []string{"get", "list", "watch"}),
				rule([]string{"snapshot.storage.k8s.io"}, []string{"volumesnapshotclasses"},
					[]string{"get", "list", "watch"}),
				rule([]string{"snapshot.storage.k8s.io"}, []string{"volumesnapshots"},
					[]string{"get", "list", "watch", "create", "update", "patch", "delete"}),
			},
		},
		map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
			"metadata": map[string]any{"name": "mspsql-agent"},
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "mspsql-agent"},
			"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "mspsql-agent", "namespace": "mspsql-agent"}},
		},
		map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": "mspsql-agent-bootstrap", "namespace": "mspsql-agent"},
			"stringData": map[string]any{
				"registration-token": token,
				"registration-url":   strings.TrimRight(s.PublicURL, "/"),
				"registration-uid":   string(site.UID),
				"hub-domain":         s.HubDomain,
				"hub-address":        s.HubAddress,
			},
		},
		agentDeployment(site, s.AgentImage, s.WireGuardImage),
	}
	var output strings.Builder
	for i, object := range objects {
		encoded, err := yaml.Marshal(object)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			output.WriteString("---\n")
		}
		output.Write(encoded)
	}
	return []byte(output.String()), nil
}

func (s *HTTPServer) bind(response http.ResponseWriter, request *http.Request,
	site *api.SiteRegistration, tokenSecret *corev1.Secret,
) {
	var binding BindRequest
	if err := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64<<10)).Decode(&binding); err != nil {
		http.Error(response, "invalid binding request", http.StatusBadRequest)
		return
	}
	if binding.ClusterUID == "" || binding.CSRPEM == "" || binding.WireGuardPublicKey == "" {
		http.Error(response, "clusterUID, csrPEM and wireGuardPublicKey are required", http.StatusBadRequest)
		return
	}
	if site.Status.ClusterUID != "" && site.Status.ClusterUID != binding.ClusterUID {
		http.Error(response, "registration is bound to another cluster", http.StatusConflict)
		return
	}
	if duplicate, err := s.clusterUIDClaimed(request.Context(), binding.ClusterUID, site.Name); err != nil {
		http.Error(response, "could not validate cluster identity", http.StatusInternalServerError)
		return
	} else if duplicate {
		http.Error(response, "cluster UID is already registered", http.StatusConflict)
		return
	}
	certificatePEM, caPEM, err := s.SignCSR(request.Context(), site, []byte(binding.CSRPEM))
	if err != nil {
		http.Error(response, "CSR validation or signing failed", http.StatusBadRequest)
		return
	}
	peerConfiguration := ""
	if s.WireGuardImage != "" {
		peerConfiguration, err = wireguard.AuthorizePeer(request.Context(), s.Client,
			s.SystemNamespace, s.WireGuardNetworkCIDR, s.WireGuardEndpoint, site,
			binding.WireGuardPublicKey)
		if err != nil {
			http.Error(response, "could not authorize WireGuard peer", http.StatusInternalServerError)
			return
		}
	}
	site.Status.ClusterUID = binding.ClusterUID
	site.Status.Phase = "Registered"
	if err := s.Client.Status().Update(request.Context(), site); err != nil {
		http.Error(response, "could not bind cluster identity", http.StatusInternalServerError)
		return
	}
	if err := s.Client.Delete(request.Context(), tokenSecret); err != nil {
		http.Error(response, "could not consume registration token", http.StatusInternalServerError)
		return
	}
	var signingKey corev1.Secret
	if err := s.Client.Get(request.Context(), types.NamespacedName{
		Namespace: s.SystemNamespace, Name: "mspsql-plan-signing-key",
	}, &signingKey); err != nil {
		http.Error(response, "plan signing key is not initialized", http.StatusServiceUnavailable)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(BindResponse{
		CertificatePEM: string(certificatePEM), CABundlePEM: string(caPEM),
		PlanPublicKey:      string(signingKey.Data["publicKey"]),
		WireGuardPeerState: peerConfiguration,
	})
}

func (s *HTTPServer) SignCSR(ctx context.Context, site *api.SiteRegistration,
	csrPEM []byte,
) ([]byte, []byte, error) {
	caCertificate, caKey, caPEM, err := s.ensureCA(ctx)
	if err != nil {
		return nil, nil, err
	}
	block, trailing := pem.Decode(csrPEM)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, nil, fmt.Errorf("CSR PEM is invalid")
	}
	if len(bytes.TrimSpace(trailing)) != 0 {
		return nil, nil, fmt.Errorf("CSR PEM contains trailing data")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || csr.CheckSignature() != nil {
		return nil, nil, fmt.Errorf("CSR signature is invalid")
	}
	publicKey, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || publicKey.Curve != elliptic.P256() {
		return nil, nil, fmt.Errorf("CSR must use an ECDSA P-256 key")
	}
	identity, _ := url.Parse("spiffe://multisite-postgres.dev/site/" + string(site.UID))
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: string(site.UID)},
		URIs: []*url.URL{identity}, NotBefore: s.now().Add(-5 * time.Minute),
		NotAfter: s.now().Add(24 * time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCertificate, csr.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), caPEM, nil
}

func (s *HTTPServer) ensureCA(ctx context.Context) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: s.SystemNamespace, Name: registrationCASecret}
	if err := s.Client.Get(ctx, key, &secret); err == nil {
		certBlock, _ := pem.Decode(secret.Data["tls.crt"])
		keyBlock, _ := pem.Decode(secret.Data["tls.key"])
		if certBlock == nil || keyBlock == nil {
			return nil, nil, nil, fmt.Errorf("registration CA Secret is invalid")
		}
		certificate, err := x509.ParseCertificate(certBlock.Bytes)
		if err != nil {
			return nil, nil, nil, err
		}
		privateKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
		return certificate, privateKey, secret.Data["tls.crt"], err
	} else if !apierrors.IsNotFound(err) {
		return nil, nil, nil, err
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "mspsql registration CA"},
		NotBefore: s.now().Add(-5 * time.Minute), NotAfter: s.now().Add(10 * 365 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return nil, nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	immutable := true
	secret = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name},
		Immutable:  &immutable, Data: map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM},
	}
	if err := s.Client.Create(ctx, &secret); err != nil {
		return nil, nil, nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	return certificate, privateKey, certPEM, err
}

func (s *HTTPServer) clusterUIDClaimed(ctx context.Context, clusterUID, except string) (bool, error) {
	var sites api.SiteRegistrationList
	if err := s.Client.List(ctx, &sites); err != nil {
		return false, err
	}
	for _, site := range sites.Items {
		if site.Name != except && site.Status.ClusterUID == clusterUID {
			return true, nil
		}
	}
	return false, nil
}

func parseCapabilityPath(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	returnValue := len(parts) == 2 && parts[0] != ""
	if !returnValue {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func rule(groups, resources, verbs []string) map[string]any {
	return map[string]any{"apiGroups": groups, "resources": resources, "verbs": verbs}
}

func agentDeployment(site *api.SiteRegistration, agentImage, wireGuardImage string) map[string]any {
	agentLabels := map[string]any{"app.kubernetes.io/name": "mspsql-agent"}
	containers := []any{map[string]any{
		"name": "site-agent", "image": agentImage,
		"args": []any{
			"--hub-address=$(HUB_ADDRESS)", "--hub-domain=$(HUB_DOMAIN)",
			"--registration-uid=" + string(site.UID),
		},
		"env": []any{
			map[string]any{"name": "POD_NAME", "valueFrom": map[string]any{
				"fieldRef": map[string]any{"fieldPath": "metadata.name"}}},
			map[string]any{"name": "POD_NAMESPACE", "valueFrom": map[string]any{
				"fieldRef": map[string]any{"fieldPath": "metadata.namespace"}}},
			map[string]any{"name": "HUB_ADDRESS", "valueFrom": map[string]any{
				"secretKeyRef": map[string]any{"name": "mspsql-agent-bootstrap", "key": "hub-address"}}},
			map[string]any{"name": "HUB_DOMAIN", "valueFrom": map[string]any{
				"secretKeyRef": map[string]any{"name": "mspsql-agent-bootstrap", "key": "hub-domain"}}},
		},
		"volumeMounts": []any{
			map[string]any{"name": "bootstrap", "mountPath": "/etc/mspsql/bootstrap", "readOnly": true},
			map[string]any{"name": "identity", "mountPath": "/etc/mspsql/identity", "readOnly": true},
			map[string]any{"name": "runtime", "mountPath": "/run/mspsql"},
		},
		"readinessProbe": map[string]any{
			"exec": map[string]any{"command": []any{
				"/site-agent", "--check-ready=/etc/mspsql/identity/tls.crt",
			}},
			"periodSeconds": 2,
		},
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "100m", "memory": "128Mi"},
			"limits":   map[string]any{"cpu": "1", "memory": "512Mi"},
		},
		"securityContext": map[string]any{
			"allowPrivilegeEscalation": false,
			"readOnlyRootFilesystem":   true,
			"runAsNonRoot":             true,
			"capabilities":             map[string]any{"drop": []any{"ALL"}},
		},
	}}
	volumes := []any{
		map[string]any{"name": "bootstrap", "secret": map[string]any{
			"secretName": "mspsql-agent-bootstrap", "defaultMode": 256}},
		map[string]any{"name": "identity", "secret": map[string]any{
			"secretName": "mspsql-agent-identity", "optional": true, "defaultMode": 256}},
		map[string]any{"name": "runtime", "emptyDir": map[string]any{"medium": "Memory"}},
	}
	if wireGuardImage != "" {
		containers = append(containers, map[string]any{
			"name": "wireguard", "image": wireGuardImage,
			"command": []any{"/bin/sh", "-ec",
				"while [ ! -f /run/mspsql/leader ]; do sleep 1; done; " +
					"wireguard-go wg0; wg-quick up /etc/wireguard/wg0.conf; " +
					"while true; do sleep 3600; done"},
			"securityContext": map[string]any{
				"allowPrivilegeEscalation": false,
				"readOnlyRootFilesystem":   true,
				"capabilities":             map[string]any{"drop": []any{"ALL"}, "add": []any{"NET_ADMIN"}},
			},
			"resources": map[string]any{
				"requests": map[string]any{
					"cpu": "25m", "memory": "32Mi", "multisite-postgres.dev/tun": 1,
				},
				"limits": map[string]any{
					"cpu": "250m", "memory": "128Mi", "multisite-postgres.dev/tun": 1,
				},
			},
			"volumeMounts": []any{
				map[string]any{"name": "identity", "mountPath": "/etc/wireguard", "readOnly": true},
				map[string]any{"name": "runtime", "mountPath": "/run/mspsql"},
				map[string]any{"name": "wireguard-runtime", "mountPath": "/run/wireguard"},
			},
		})
		volumes = append(volumes, map[string]any{
			"name": "wireguard-runtime", "emptyDir": map[string]any{"medium": "Memory"},
		})
	}
	return map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "mspsql-agent", "namespace": "mspsql-agent"},
		"spec": map[string]any{
			"replicas": 2,
			"strategy": map[string]any{"rollingUpdate": map[string]any{
				"maxUnavailable": 0, "maxSurge": 1,
			}},
			"selector": map[string]any{"matchLabels": agentLabels},
			"template": map[string]any{
				"metadata": map[string]any{"labels": agentLabels},
				"spec": map[string]any{
					"serviceAccountName":            "mspsql-agent",
					"terminationGracePeriodSeconds": 30,
					"securityContext": map[string]any{
						"seccompProfile": map[string]any{"type": "RuntimeDefault"},
					},
					"affinity": map[string]any{"podAntiAffinity": map[string]any{
						"preferredDuringSchedulingIgnoredDuringExecution": []any{
							map[string]any{
								"weight": 100,
								"podAffinityTerm": map[string]any{
									"topologyKey":   "kubernetes.io/hostname",
									"labelSelector": map[string]any{"matchLabels": agentLabels},
								},
							},
						},
					}},
					"containers": containers,
					"volumes":    volumes,
				},
			},
		},
	}
}

func (s *HTTPServer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
