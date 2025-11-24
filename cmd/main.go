// everest-operator
// Copyright (C) 2022 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package main ...
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	vmv1beta1 "github.com/VictoriaMetrics/operator/api/operator/v1beta1"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Import all Kubernetes client auth plugins to ensure that exec-entrypoint and run can make use of them.
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	enginefeatureseverestcontroller "github.com/percona/everest-operator/internal/controller/enginefeatures.everest"
	controllers "github.com/percona/everest-operator/internal/controller/everest"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/predicates"
	webhookenginefeatureseverestv1alpha1 "github.com/percona/everest-operator/internal/webhook/enginefeatures.everest/v1alpha1"
	webhookeverestv1alpha1 "github.com/percona/everest-operator/internal/webhook/everest/v1alpha1"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

const (
	systemNamespaceEnvVar     = "SYSTEM_NAMESPACE"
	monitoringNamespaceEnvVar = "MONITORING_NAMESPACE"
	dbNamespacesEnvVar        = "DB_NAMESPACES"
	podNameEnvVar             = "POD_NAME"
)

// Config contains the configuration for Everest Operator.
type Config struct {
	// MetricsAddr is the address the metric endpoint binds to.
	MetricsAddr string
	// EnableLeaderElection enables leader election for controller manager.
	EnableLeaderElection bool
	// LeaderElectionID is the name of the leader election ID.
	LeaderElectionID string
	// ProbeAddr is the address the health probe endpoint binds to.
	ProbeAddr string
	// SecureMetrics is true if the metrics endpoint is served securely via HTTPS.
	SecureMetrics bool
	// EnableHTTP2 is true if HTTP/2 is enabled for the metrics and webhook servers.
	EnableHTTP2 bool
	// DBNamespaces is the namespaces to watch for DB resources.
	// If set, NamespaceLabels is ignored.
	DBNamespaces string
	// MonitoringNamespace is the namespace where the monitoring resources are.
	MonitoringNamespace string
	// SystemNamespace is the namespace where the operator is running.
	SystemNamespace string
	// Name of the pod.
	PodName string
	// If set, watches only those namespaces that have the specified labels.
	// This setting is ignored if DBNamespaces is set.
	NamespaceLabels map[string]string
	// DisableWebhookServer is true if the webhook server should not be started.
	DisableWebhookServer bool

	WebhookCertPath string
	WebhookCertName string
	WebhookCertKey  string
}

var cfg = &Config{}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(everestv1alpha1.AddToScheme(scheme))
	utilruntime.Must(enginefeatureseverestv1alpha1.AddToScheme(scheme))
	utilruntime.Must(vmv1beta1.AddToScheme(scheme))

	utilruntime.Must(pgv2.SchemeBuilder.AddToScheme(scheme))
	utilruntime.Must(psmdbv1.SchemeBuilder.AddToScheme(scheme))
	utilruntime.Must(pxcv1.SchemeBuilder.AddToScheme(scheme))
	utilruntime.Must(crunchyv1beta1.SchemeBuilder.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

//nolint:gocognit,maintidx
func main() {
	var tlsOpts []func(*tls.Config)
	err := parseConfig()
	if err != nil {
		setupLog.Error(err, "unable to parse config")
		os.Exit(1)
	}
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	var dbNamespaces []string
	if s := cfg.DBNamespaces; s != "" {
		dbNamespaces = strings.Split(s, ",")
	}
	defaultCache := cache.Options{}
	if len(dbNamespaces) > 0 {
		defaultCache.DefaultNamespaces = make(map[string]cache.Config)
		defaultCache.DefaultNamespaces[cfg.SystemNamespace] = cache.Config{}
		defaultCache.DefaultNamespaces[cfg.MonitoringNamespace] = cache.Config{}
		for _, ns := range dbNamespaces {
			defaultCache.DefaultNamespaces[ns] = cache.Config{}
		}
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !cfg.EnableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	ctx := ctrl.SetupSignalHandler()
	if cfg.WebhookCertPath != "" {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", cfg.WebhookCertPath, "webhook-cert-name", cfg.WebhookCertName, "webhook-cert-key", cfg.WebhookCertKey)

		var err error
		webhookCertWatcher, err := certwatcher.New(
			filepath.Join(cfg.WebhookCertPath, cfg.WebhookCertName),
			filepath.Join(cfg.WebhookCertPath, cfg.WebhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		go func() {
			if err := webhookCertWatcher.Start(ctx); err != nil {
				setupLog.Error(err, "Failed to start webhook certificate watcher")
				os.Exit(1)
			}
		}()

		tlsOpts = append(tlsOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   cfg.MetricsAddr,
		SecureServing: cfg.SecureMetrics,
		// TODO(user): TLSOpts is used to allow configuring the TLS config used for the server. If certificates are
		// not provided, self-signed certificates will be generated by default. This option is not recommended for
		// production environments as self-signed certificates do not offer the same level of trust and security
		// as certificates issued by a trusted Certificate Authority (CA). The primary risk is potentially allowing
		// unauthorized access to sensitive metrics data. Consider replacing with CertDir, CertName, and KeyName
		// to provide certificates, ensuring the server communicates using trusted and secure certificates.
		TLSOpts: tlsOpts,
	}

	if cfg.SecureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	managerOptions := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		HealthProbeBindAddress: cfg.ProbeAddr,
		LeaderElection:         cfg.EnableLeaderElection,
		LeaderElectionID:       cfg.LeaderElectionID,
		Cache:                  defaultCache,
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
	}
	if !cfg.DisableWebhookServer {
		managerOptions.WebhookServer = webhookServer
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), managerOptions)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// We filter namespaces only if no DBNamespaces are defined.
	if len(dbNamespaces) == 0 {
		common.DefaultNamespaceFilter = &predicates.NamespaceFilter{
			MatchLabels: cfg.NamespaceLabels,
			GetNamespace: func(ctx context.Context, name string) (*corev1.Namespace, error) {
				namespace := &corev1.Namespace{}
				if err := mgr.GetClient().Get(ctx, types.NamespacedName{Name: name}, namespace); err != nil {
					return nil, err
				}
				return namespace, nil
			},
		}
	}

	// Ensure specified DB namespaces exist.
	namespace := &unstructured.Unstructured{}
	namespace.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "",
		Kind:    "Namespace",
		Version: "v1",
	})
	for _, ns := range dbNamespaces {
		if err := mgr.GetClient().Get(context.Background(), types.NamespacedName{Name: ns}, namespace); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "DatabaseCluster")
			os.Exit(1)
		}
	}
	// Initialise the controllers.
	clusterReconciler := &controllers.DatabaseClusterReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Cache:  mgr.GetCache(),
	}
	if err := clusterReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DatabaseCluster")
		os.Exit(1)
	}
	restoreReconciler := &controllers.DatabaseClusterRestoreReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Cache:  mgr.GetCache(),
	}
	if err := restoreReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DatabaseClusterRestore")
		os.Exit(1)
	}
	backupReconciler := &controllers.DatabaseClusterBackupReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
		Cache:     mgr.GetCache(),
	}
	if err := backupReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DatabaseClusterBackup")
		os.Exit(1)
	}
	if err = (&controllers.DatabaseEngineReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		Controllers: []controllers.DatabaseController{clusterReconciler, restoreReconciler, backupReconciler},
	}).SetupWithManager(mgr, dbNamespaces); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DatabaseEngine")
		os.Exit(1)
	}
	if err = (&controllers.BackupStorageReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BackupStorage")
		os.Exit(1)
	}
	if err = (&controllers.MonitoringConfigReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr, cfg.MonitoringNamespace); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MonitoringConfig")
		os.Exit(1)
	}
	if err = (&controllers.DataImportJobReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DataImportJob")
	}
	if err = (&controllers.PodSchedulingPolicyReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PodSchedulingPolicy")
		os.Exit(1)
	}

	if err = (&controllers.LoadBalancerConfigReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "LoadBalancerConfig")
		os.Exit(1)
	}
	// ------------------ Engine Features controllers ------------------
	// SplitHorizonDNSConfig controller
	if err := (&enginefeatureseverestcontroller.SplitHorizonDNSConfigReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SplitHorizonDNSConfig")
		os.Exit(1)
	}
	// ------------------ End of Engine Features controllers ------------------

	// register webhooks
	if !cfg.DisableWebhookServer {
		if err := webhookeverestv1alpha1.SetupDatabaseClusterWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "DatabaseCluster")
			os.Exit(1)
		}
		if err := webhookeverestv1alpha1.SetupDataImportJobWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "DataImportJob")
			os.Exit(1)
		}

		if err := webhookeverestv1alpha1.SetupMonitoringConfigWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "MonitoringConfig")
			os.Exit(1)
		}

		if err := webhookeverestv1alpha1.SetupLoadBalancerConfigWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "LoadBalancerConfig")
			os.Exit(1)
		}

		// ------------------ Engine Features webhooks ------------------
		if err := webhookenginefeatureseverestv1alpha1.SetupSplitHorizonDNSConfigWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create validation webhook", "webhook", "SplitHorizonDNSConfig")
			os.Exit(1)
		}
		if err := webhookenginefeatureseverestv1alpha1.SetupSplitHorizonDNSConfigMutationWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create defaulter webhook", "webhook", "SplitHorizonDNSConfig")
			os.Exit(1)
		}
		// ------------------ End of Engine Features webhooks ------------------
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func parseConfig() error {
	systemNamespace := os.Getenv(systemNamespaceEnvVar)
	monitoringNamespace := os.Getenv(monitoringNamespaceEnvVar)
	dbNamespacesString := os.Getenv(dbNamespacesEnvVar)
	podName := os.Getenv(podNameEnvVar)
	cfg.PodName = podName
	defaultNamespaceLabelFilter := fmt.Sprintf("%s=%s", consts.KubernetesManagedByLabel, consts.Everest)
	var namespaceLabelFilter string
	flag.BoolVar(&cfg.DisableWebhookServer, "disable-webhook-server", false,
		"If set, the webhook server will not be started. This is useful for testing purposes or if you don't need webhooks.")
	flag.StringVar(&cfg.MetricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&cfg.ProbeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&cfg.EnableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&cfg.LeaderElectionID, "leader-election-id", "9094838c.percona.com",
		"The name of the leader election ID.")
	flag.BoolVar(&cfg.SecureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.BoolVar(&cfg.EnableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&namespaceLabelFilter, "namespace-label-filter", defaultNamespaceLabelFilter,
		"If set, reconciles objects only from those namespaces that have the specified label. "+
			"This setting is ignored if db-namespaces is set. "+
			fmt.Sprintf("Default is `%s`", defaultNamespaceLabelFilter))
	flag.StringVar(&cfg.DBNamespaces, "db-namespaces", dbNamespacesString, "The namespaces to watch for DB resources."+
		"Defaults to the value of the DB_NAMESPACES environment variable. If set, watch-namespace-labels is ignored.")
	flag.StringVar(&cfg.SystemNamespace, "system-namespace", systemNamespace, "The namespace where the operator is running."+
		"Defaults to the value of the SYSTEM_NAMESPACE environment variable.")
	flag.StringVar(&cfg.MonitoringNamespace, "monitoring-namespace", monitoringNamespace, "The namespace where the monitoring resources are."+
		"Defaults to the value of the MONITORING_NAMESPACE environment variable.")

	flag.StringVar(&cfg.WebhookCertPath, "webhook-cert-path", "",
		"The path to the directory where the webhook server's TLS certificate and key are stored.")
	flag.StringVar(&cfg.WebhookCertName, "webhook-cert-name", "tls.crt",
		"The name of the webhook server's TLS certificate file. Defaults to 'tls.crt'.")
	flag.StringVar(&cfg.WebhookCertKey, "webhook-cert-key", "tls.key",
		"The name of the webhook server's TLS key file. Defaults to 'tls.key'.")
	flag.Parse()

	if cfg.SystemNamespace == "" {
		return fmt.Errorf("%s or --system-namespace must be set", systemNamespaceEnvVar)
	}
	if cfg.MonitoringNamespace == "" {
		return fmt.Errorf("%s or --monitoring-namespace must be set", monitoringNamespaceEnvVar)
	}

	cfg.NamespaceLabels = make(map[string]string)
	labels := strings.Split(namespaceLabelFilter, ",")
	for _, label := range labels {
		parts := strings.Split(label, "=")
		if len(parts) != 2 { //nolint:mnd
			return fmt.Errorf("invalid label filter: %s", label)
		}
		cfg.NamespaceLabels[parts[0]] = parts[1]
	}
	if cfg.DBNamespaces == "" && len(cfg.NamespaceLabels) == 0 {
		return fmt.Errorf("either %s or --namespace-label-filter must be set", dbNamespacesEnvVar)
	}
	return nil
}
