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
	client := kubernetes.NewForConfigOrDie(ctrl.GetConfigOrDie()).CoordinationV1()
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: leaseName, Namespace: namespace},
		Client:    client, LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
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
				<-ctx.Done()
				_ = os.Remove(activationPath)
			},
			OnStoppedLeading: func() {
				_ = os.Remove(activationPath)
			},
		},
	})
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
