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
	"crypto/tls"
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/control"
	"github.com/sindef/mspsql/internal/controller"
	"github.com/sindef/mspsql/internal/registration"
	"github.com/sindef/mspsql/internal/telemetry"
	webhookv1alpha1 "github.com/sindef/mspsql/internal/webhook/v1alpha1"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(multisitepostgresv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var systemNamespace, registrationPublicURL string
	var controlAddress, controlCertificate, controlPrivateKey, controlClientCA string
	var registrationAddress, hubDomain, hubControlAddress, agentImage, wireGuardImage string
	var wireGuardNetworkCIDR, wireGuardEndpoint string
	var defaultBackupSchedule string
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&systemNamespace, "system-namespace", envOrDefault("POD_NAMESPACE", "mspsql-system"),
		"Namespace containing hub identity and signing Secrets.")
	flag.StringVar(&registrationPublicURL, "registration-public-url", "",
		"Public HTTPS base URL used in registration capability URLs.")
	flag.StringVar(&registrationAddress, "registration-address", ":8082",
		"Internal HTTP address for registration bundle retrieval and binding.")
	flag.StringVar(&hubDomain, "hub-domain", "", "Stable ownership domain recorded on target namespaces.")
	flag.StringVar(&hubControlAddress, "hub-control-address", "",
		"Hub gRPC address placed in registration bundles.")
	flag.StringVar(&agentImage, "site-agent-image", "ghcr.io/sindef/mspsql-agent:latest",
		"Site agent image placed in registration bundles.")
	flag.StringVar(&wireGuardImage, "wireguard-image", "ghcr.io/sindef/mspsql-wireguard:latest",
		"WireGuard userspace image placed in registration bundles.")
	flag.StringVar(&wireGuardNetworkCIDR, "wireguard-network-cidr", "10.254.0.0/16",
		"Private IPv4 network allocated to the hub and registered site peers.")
	flag.StringVar(&wireGuardEndpoint, "wireguard-endpoint", "",
		"Public host:port used by site peers to reach the hub WireGuard gateway.")
	flag.StringVar(&defaultBackupSchedule, "default-backup-schedule", "30 6 * * 0",
		"Weekly backup cron used when an instance declares no schedules.")
	flag.StringVar(&controlAddress, "control-address", ":9444", "Address for the agent gRPC control service.")
	flag.StringVar(&controlCertificate, "control-certificate", "/etc/mspsql/control/tls.crt",
		"Agent gRPC server certificate.")
	flag.StringVar(&controlPrivateKey, "control-private-key", "/etc/mspsql/control/tls.key",
		"Agent gRPC server private key.")
	flag.StringVar(&controlClientCA, "control-client-ca", "/etc/mspsql/control/ca.crt",
		"CA used to authenticate site agents.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "41421668.multisite-postgres.dev",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}
	controllermetrics.Registry.MustRegister(telemetry.NewHubCollector(mgr.GetClient()))

	if err := (&controller.SiteRegistrationReconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		SystemNamespace:       systemNamespace,
		RegistrationPublicURL: registrationPublicURL,
		WireGuardNetworkCIDR:  wireGuardNetworkCIDR,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "siteregistration")
		os.Exit(1)
	}
	if err := (&controller.MultiSitePostgresReconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		SystemNamespace:       systemNamespace,
		DefaultBackupSchedule: defaultBackupSchedule,
		Recorder:              mgr.GetEventRecorderFor("multisitepostgres"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "multisitepostgres")
		os.Exit(1)
	}
	if err := (&controller.PostgresDatabaseReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "postgresdatabase")
		os.Exit(1)
	}
	if err := (&controller.PostgresUserReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "postgresuser")
		os.Exit(1)
	}
	if err := (&controller.PostgresRestoreReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "postgresrestore")
		os.Exit(1)
	}
	if err := (&controller.PostgresUpgradeReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "postgresupgrade")
		os.Exit(1)
	}
	registrationServer := &registration.HTTPServer{
		Address: registrationAddress, Client: mgr.GetClient(), SystemNamespace: systemNamespace,
		PublicURL: registrationPublicURL, HubDomain: hubDomain, HubAddress: hubControlAddress,
		AgentImage: agentImage, WireGuardImage: wireGuardImage,
		WireGuardNetworkCIDR: wireGuardNetworkCIDR, WireGuardEndpoint: wireGuardEndpoint,
	}
	if controlAddress != "" {
		if err := mgr.Add(&control.RunnableServer{
			Address: controlAddress, Certificate: controlCertificate,
			PrivateKey: controlPrivateKey, ClientCA: controlClientCA,
			Service: &control.Server{
				Client: mgr.GetClient(), SystemNamespace: systemNamespace,
				SignCertificate: registrationServer.SignCSR,
			},
		}); err != nil {
			setupLog.Error(err, "Failed to add agent control server")
			os.Exit(1)
		}
	}
	if registrationAddress != "" {
		if err := mgr.Add(registrationServer); err != nil {
			setupLog.Error(err, "Failed to add registration server")
			os.Exit(1)
		}
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1alpha1.SetupSiteRegistrationWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "SiteRegistration")
			os.Exit(1)
		}
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1alpha1.SetupMultiSitePostgresWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "MultiSitePostgres")
			os.Exit(1)
		}
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1alpha1.SetupPostgresDatabaseWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "PostgresDatabase")
			os.Exit(1)
		}
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1alpha1.SetupPostgresUserWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "PostgresUser")
			os.Exit(1)
		}
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1alpha1.SetupPostgresRestoreWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "PostgresRestore")
			os.Exit(1)
		}
	}
	// nolint:goconst
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1alpha1.SetupPostgresUpgradeWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "PostgresUpgrade")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
