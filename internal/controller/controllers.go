package controller

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
)

// Controller interface for extensible registration
type Controller interface {
	SetupWithManager(mgr ctrl.Manager) error
}

// SetupAllControllers registers all current and future controllers with the manager.
func SetupAllControllers(mgr ctrl.Manager) error {
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
				Client:   mgr.GetClient(),
				Recorder: mgr.GetEventRecorderFor("changewindow-controller"),
			},
		},
		// Future controllers registered here with 1 line:
		// {
		// 	name: "RemediationJob",
		// 	controller: &RemediationJobReconciler{...},
		// },
	}

	for _, c := range controllers {
		if err := c.controller.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("unable to setup controller %s: %w", c.name, err)
		}
	}

	return nil
}
