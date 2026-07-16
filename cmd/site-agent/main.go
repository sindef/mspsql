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
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"github.com/sindef/mspsql/internal/agent"
	"github.com/sindef/mspsql/internal/control"
	"github.com/sindef/mspsql/internal/plan"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var version = "dev"

func main() {
	var target, namespace, registrationUID, hubDomain, publicKeyPath string
	var certificatePath, privateKeyPath, caPath, activationPath string
	var bootstrapPath string
	var etcdImage, pgpoolImage string
	flag.StringVar(&target, "hub-address", "", "Hub gRPC address reachable through WireGuard.")
	flag.StringVar(&namespace, "namespace", envOrDefault("POD_NAMESPACE", "mspsql-agent"), "Agent system namespace.")
	flag.StringVar(&registrationUID, "registration-uid", "", "Immutable SiteRegistration UID.")
	flag.StringVar(&hubDomain, "hub-domain", "", "Hub ownership domain.")
	flag.StringVar(&publicKeyPath, "plan-public-key", "/etc/mspsql/identity/plan-public-key", "Plan verification key.")
	flag.StringVar(&certificatePath, "tls-certificate", "/etc/mspsql/identity/tls.crt", "Agent mTLS certificate.")
	flag.StringVar(&privateKeyPath, "tls-private-key", "/etc/mspsql/identity/tls.key", "Agent mTLS private key.")
	flag.StringVar(&caPath, "tls-ca", "/etc/mspsql/identity/ca.crt", "Hub control CA.")
	flag.StringVar(&activationPath, "wireguard-activation-file", "/run/mspsql/leader", "WireGuard leader activation file.")
	flag.StringVar(&bootstrapPath, "bootstrap-path", "/etc/mspsql/bootstrap", "Registration bootstrap Secret path.")
	flag.StringVar(&etcdImage, "etcd-image", "quay.io/coreos/etcd:v3.6.6", "etcd image.")
	flag.StringVar(&pgpoolImage, "pgpool-image", "bitnami/pgpool:4.6.3", "Pgpool image.")
	zapOptions := zap.Options{Development: false}
	zapOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))
	log := ctrl.Log.WithName("site-agent")
	if target == "" || registrationUID == "" || hubDomain == "" {
		log.Error(fmt.Errorf("hub-address, registration-uid and hub-domain are required"), "Invalid configuration")
		os.Exit(2)
	}

	config := ctrl.GetConfigOrDie()
	kube := clients(config)
	clusterUID := clusterUID(context.Background(), kube)
	bootstrapped, err := bootstrapIfRequired(context.Background(), kube, namespace, publicKeyPath,
		bootstrapPath, string(clusterUID))
	if err != nil {
		log.Error(err, "Site registration failed")
		os.Exit(1)
	}
	if bootstrapped {
		log.Info("Site identity created; restarting to mount it")
		return
	}
	publicKey := readPublicKey(publicKeyPath)
	tlsConfig := clientTLS(certificatePath, privateKeyPath, caPath)
	cache := &agent.Cache{
		Client: kube, Namespace: namespace, PublicKey: publicKey, SiteUID: registrationUID,
	}
	reconciler := &agent.Reconciler{
		Client: kube, HubDomain: hubDomain, SiteUID: registrationUID,
		Renderer: agent.Renderer{
			HubDomain: hubDomain, Images: agent.Images{Etcd: etcdImage, Pgpool: pgpoolImage},
		},
	}
	identity := envOrDefault("POD_NAME", fmt.Sprintf("site-agent-%d", os.Getpid()))
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: "mspsql-site-agent", Namespace: namespace},
		Client:     kubernetes.NewForConfigOrDie(config).CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	ctx := ctrl.SetupSignalHandler()
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second,
		RetryPeriod: 15 * time.Second, ReleaseOnCancel: true, Name: "mspsql-site-agent",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				if err := os.MkdirAll(filepath.Dir(activationPath), 0o750); err != nil {
					log.Error(err, "Could not create WireGuard activation directory")
					return
				}
				if err := os.WriteFile(activationPath, []byte(identity), 0o600); err != nil {
					log.Error(err, "Could not activate WireGuard")
					return
				}
				defer os.Remove(activationPath)
				reconcileCached(leaderCtx, cache, reconciler, log)
				runControlLoop(leaderCtx, target, tlsConfig, cache, reconciler, registrationUID,
					string(clusterUID), log)
			},
			OnStoppedLeading: func() {
				_ = os.Remove(activationPath)
				log.Info("Leadership lost")
			},
		},
	})
}

func clients(config *rest.Config) client.Client {
	scheme := runtime.NewScheme()
	must(corev1.AddToScheme(scheme))
	must(appsv1.AddToScheme(scheme))
	must(batchv1.AddToScheme(scheme))
	must(coordinationv1.AddToScheme(scheme))
	kube, err := client.New(config, client.Options{Scheme: scheme})
	must(err)
	return kube
}

func runControlLoop(ctx context.Context, target string, tlsConfig *tls.Config, cache *agent.Cache,
	reconciler *agent.Reconciler, registrationUID, clusterUID string, log logr.Logger,
) {
	backoff := time.Second
	for ctx.Err() == nil {
		client := &control.AgentClient{
			Target: target,
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time: time.Minute, Timeout: 20 * time.Second, PermitWithoutStream: false,
				}),
			},
			Hello: &controlv1.AgentHello{
				RegistrationUid: registrationUID, ClusterUid: clusterUID, AgentVersion: version,
				ProtocolVersion: plan.ProtocolVersion,
				Capabilities:    []string{"signed-cache", "server-side-apply", "cert-manager-v1", "metallb"},
			},
			Cache: cache, Reconciler: reconciler,
		}
		if err := client.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error(err, "Control stream disconnected")
		}
		delay := backoff + time.Duration(rand.Int64N(int64(backoff/2+1)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}

func reconcileCached(ctx context.Context, cache *agent.Cache, reconciler *agent.Reconciler, log logr.Logger) {
	plans, err := cache.List(ctx)
	if err != nil {
		log.Error(err, "Could not load signed plan cache")
		return
	}
	for _, desired := range plans {
		if _, err := reconciler.Apply(ctx, desired, desired, false); err != nil {
			log.Error(err, "Cached reconciliation failed", "instanceUID", desired.InstanceUID,
				"revision", desired.Revision)
		}
	}
}

func readPublicKey(path string) ed25519.PublicKey {
	encoded, err := os.ReadFile(path)
	must(err)
	key, err := base64.RawStdEncoding.DecodeString(string(encoded))
	must(err)
	if len(key) != ed25519.PublicKeySize {
		panic("plan verification key has an invalid size")
	}
	return ed25519.PublicKey(key)
}

func clientTLS(certificatePath, privateKeyPath, caPath string) *tls.Config {
	certificate, err := tls.LoadX509KeyPair(certificatePath, privateKeyPath)
	must(err)
	caPEM, err := os.ReadFile(caPath)
	must(err)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		panic("hub CA contains no certificates")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots,
	}
}

func clusterUID(ctx context.Context, kube client.Client) types.UID {
	var namespace corev1.Namespace
	must(kube.Get(ctx, client.ObjectKey{Name: "kube-system"}, &namespace))
	return namespace.UID
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
