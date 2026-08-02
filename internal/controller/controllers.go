package controller

import (
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	"example.com/drift-operator/internal/kafka"
)

// Controller interface for extensible registration
type Controller interface {
	SetupWithManager(mgr ctrl.Manager) error
}

// SetupAllControllers registers all current and future controllers with the manager.
// kafkaBridge may be nil when Kafka is disabled; all controllers handle a nil bridge safely.
func SetupAllControllers(mgr ctrl.Manager, kafkaBridge *kafka.KafkaBridge) error {
	// Autonomous peer mode (recommended): CLUSTER_NAME env var enables the
	// LocalAppWatchReconciler, which writes ClusterAppReports from local platform
	// and workload state. Each cluster operates independently.
	clusterName := os.Getenv("CLUSTER_NAME")

	type reg struct {
		name       string
		controller Controller
	}
	var controllers []reg

	if clusterName != "" {
		clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("unable to create typed kubernetes clientset for spoke reconciler: %w", err)
		}

		// Autonomous peer deployment mode: run local platform telemetry controller.
		controllers = []reg{
			{
				name: "LocalAppWatch",
				controller: &LocalAppWatchReconciler{
					Client:      mgr.GetClient(),
					ClusterName: clusterName,
					Clientset:   clientset,
				},
			},
		}
	} else {
		// Fleet aggregation mode (optional): run aggregation and change window controllers.
		// In the peer-to-peer model, this path is used for optional central fleet visibility.
		controllers = []reg{
			{
				name: "PropagationStatus",
				controller: &PropagationStatusReconciler{
					Client:   mgr.GetClient(),
					Recorder: mgr.GetEventRecorderFor("propagationstatus-controller"),
				},
			},
			{
				name: "ChangeWindow",
				controller: &ChangeWindowReconciler{
					Client:      mgr.GetClient(),
					Recorder:    mgr.GetEventRecorderFor("changewindow-controller"),
					KafkaBridge: kafkaBridge, // nil when Kafka is disabled — handled safely
				},
			},
		}
	}

	for _, c := range controllers {
		if err := c.controller.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("unable to setup controller %s: %w", c.name, err)
		}
	}

	return nil
}
