package controller

import (
	"fmt"
	"os"

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
	// Spoke mode: CLUSTER_NAME env var enables the LocalAppWatchReconciler, which
	// writes ClusterAppReports from local ArgoCD/workload state.
	clusterName := os.Getenv("CLUSTER_NAME")

	controllers := []struct {
		name       string
		controller Controller
	}{
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

	// Register the spoke-side controller when running on a spoke cluster.
	// The hub runs both reconcilers above; a dedicated spoke deployment sets
	// CLUSTER_NAME and omits hub RBAC grants.
	if clusterName != "" {
		controllers = append(controllers, struct {
			name       string
			controller Controller
		}{
			name: "LocalAppWatch",
			controller: &LocalAppWatchReconciler{
				Client:      mgr.GetClient(),
				ClusterName: clusterName,
			},
		})
	}

	for _, c := range controllers {
		if err := c.controller.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("unable to setup controller %s: %w", c.name, err)
		}
	}

	return nil
}
