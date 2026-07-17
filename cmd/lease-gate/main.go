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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	var namespace, leaseName, activationPath string
	flag.StringVar(&namespace, "namespace", envOrDefault("POD_NAMESPACE", "mspsql-system"), "Lease namespace.")
	flag.StringVar(&leaseName, "lease-name", "mspsql-wireguard-gateway", "Lease name.")
	flag.StringVar(&activationPath, "activation-file", "/run/mspsql/leader", "Leader activation file.")
	zapOptions := zap.Options{Development: false}
	zapOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))
	identity := envOrDefault("POD_NAME", fmt.Sprintf("gateway-%d", os.Getpid()))
	kube := kubernetes.NewForConfigOrDie(ctrl.GetConfigOrDie())
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: leaseName, Namespace: namespace},
		Client:    kube.CoordinationV1(), LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	ctx := ctrl.SetupSignalHandler()
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second,
		RetryPeriod: 15 * time.Second, ReleaseOnCancel: true, Name: leaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				if err := os.MkdirAll(filepath.Dir(activationPath), 0o750); err != nil {
					panic(err)
				}
				if err := os.WriteFile(activationPath, []byte(identity), 0o600); err != nil {
					panic(err)
				}
				readyPath := filepath.Join(filepath.Dir(activationPath), "wireguard-ready")
				if err := waitForFile(ctx, readyPath); err != nil {
					_ = os.Remove(activationPath)
					return
				}
				if err := setActiveLabel(ctx, kube, namespace, identity, true); err != nil {
					_ = os.Remove(activationPath)
					return
				}
				<-ctx.Done()
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = setActiveLabel(cleanupCtx, kube, namespace, identity, false)
				_ = os.Remove(activationPath)
				_ = os.Remove(readyPath)
			},
			OnStoppedLeading: func() {
				_ = os.Remove(activationPath)
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = setActiveLabel(cleanupCtx, kube, namespace, identity, false)
			},
		},
	})
}

func waitForFile(ctx context.Context, path string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func setActiveLabel(ctx context.Context, kube kubernetes.Interface, namespace, pod string,
	active bool,
) error {
	value := "null"
	if active {
		value = `"true"`
	}
	patch := []byte(`{"metadata":{"labels":{"multisite-postgres.dev/gateway-active":` + value + `}}}`)
	_, err := kube.CoreV1().Pods(namespace).Patch(ctx, pod, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
