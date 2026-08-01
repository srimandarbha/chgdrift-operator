package main

import (
	"flag"
	"os"
	"strings"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC)
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
	"example.com/drift-operator/internal/controller"
	"example.com/drift-operator/internal/kafka"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(gitopsv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. Enabling this will ensure only one manager is active.")
	opts := zap.Options{
		Development: false,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Create a context that is cancelled on SIGTERM/SIGINT.
	// We thread this context to both mgr.Start and the Kafka consumer so
	// all goroutines share the same shutdown signal.
	ctx := ctrl.SetupSignalHandler()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "drift-operator-leader-lock",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// -------------------------------------------------------------------
	// Optional Kafka integration — enabled when KAFKA_BROKERS is set.
	// Configuration is read from environment variables so the same binary
	// works across environments without recompilation (see configmap.yaml).
	// -------------------------------------------------------------------
	var kafkaBridge *kafka.KafkaBridge
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		namespace := getenv("OPERATOR_NAMESPACE", "gitops-fleet")
		cfg := kafka.KafkaConfig{
			Brokers:          strings.Split(brokers, ","),
			IngestTopic:      getenv("KAFKA_INGEST_TOPIC", "gitops.chg.events"),
			EmitTopic:        getenv("KAFKA_EMIT_TOPIC", "gitops.change.validation"),
			SecurityProtocol: os.Getenv("KAFKA_SECURITY_PROTOCOL"),
			CAFilePath:       os.Getenv("KAFKA_CA_FILE"),
			ClientCertPath:   os.Getenv("KAFKA_CLIENT_CERT_FILE"),
			ClientKeyPath:    os.Getenv("KAFKA_CLIENT_KEY_FILE"),
			ServerCN:         os.Getenv("KAFKA_SERVER_CN"),
		}
		bridge, err := kafka.NewKafkaBridge(cfg, mgr.GetClient(), namespace)
		if err != nil {
			setupLog.Error(err, "failed to initialise Kafka bridge — continuing without Kafka")
		} else {
			kafkaBridge = bridge
			if err := mgr.Add(bridge); err != nil {
				setupLog.Error(err, "failed to add Kafka bridge runnable to manager")
				os.Exit(1)
			}
			setupLog.Info("Kafka bridge initialised and registered as leader-elected runnable",
				"ingestTopic", cfg.IngestTopic,
				"emitTopic", cfg.EmitTopic,
				"brokers", cfg.Brokers)
		}
	} else {
		setupLog.Info("KAFKA_BROKERS not set — Kafka integration disabled")
	}

	// Extensible controller registration
	if err := controller.SetupAllControllers(mgr, kafkaBridge); err != nil {
		setupLog.Error(err, "unable to setup controllers")
		os.Exit(1)
	}

	// Rule 9: Readiness and Liveness probes
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

// getenv returns the environment variable value or a default.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
