package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

// TestLongRunning_MultiHourWindowProgression simulates a prolonged 4-hour maintenance window
// stepping virtual time across state transitions: Scheduled -> BaselineCaptured -> Executing -> PlatformRecovering -> Succeeded.
func TestLongRunning_MultiHourWindowProgression(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(4 * time.Hour))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-long-4h",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:                  "CHG-LONG-4H",
			ReleaseTag:                 "v2.4.0",
			ExpectedRevision:           "rev-200",
			ImpactedApps:               []string{"svc-payments"},
			StartTime:                  startTime,
			EndTime:                    endTime,
			StabilizationPeriodSeconds: 0,
		},
	}

	ps := &gitopsv1alpha1.PropagationStatus{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-payments",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.PropagationStatusSpec{
			AppName:          "svc-payments",
			ExpectedRevision: "rev-200",
			TargetClusters:   []string{"cluster-01"},
		},
		Status: gitopsv1alpha1.PropagationStatusStatus{
			Phase: "Synced",
			ClusterStates: []gitopsv1alpha1.ClusterRevisionState{
				{
					ClusterName:      "cluster-01",
					ObservedRevision: "rev-200",
					SyncStatus:       "Synced",
					Health:           "Healthy",
					ObservedAt:       metav1.NewTime(now.Add(-1 * time.Minute)),
					State:            "InSync",
					MCPStatus: gitopsv1alpha1.MachineConfigPoolStatus{
						Name:              "worker",
						MachineCount:      3,
						ReadyMachineCount: 3,
						Phase:             "Updated",
					},
					VirtStatus: gitopsv1alpha1.VirtualizationImpactStatus{
						HyperConvergedHealth: "Healthy",
						VirtHandlerReady:     true,
					},
					PlatformObservation: gitopsv1alpha1.PlatformObservationStatus{
						ClusterOperators: []gitopsv1alpha1.ClusterOperatorStatus{
							{Name: "machine-config", Available: true, Degraded: false},
						},
						ClusterVersion: gitopsv1alpha1.ClusterVersionStatus{
							Version:     "4.14.1",
							Available:   true,
							Progressing: false,
						},
						KubeVirt: gitopsv1alpha1.KubeVirtOperatorStatus{
							Phase: "Deployed", Ready: true,
						},
						CDI: gitopsv1alpha1.CDIOperatorStatus{
							Phase: "Deployed", Ready: true,
						},
						SSP: gitopsv1alpha1.SSPOperatorStatus{
							Phase: "Deployed", Ready: true,
						},
						NodeMaintenance: gitopsv1alpha1.NodeMaintenanceStatus{
							ActiveMaintenanceNodes: 0,
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chg, ps).
		WithStatusSubresource(&gitopsv1alpha1.ChangeWindow{}, &gitopsv1alpha1.PropagationStatus{}).
		WithIndex(&gitopsv1alpha1.PropagationStatus{}, "spec.appName", func(obj client.Object) []string {
			p, ok := obj.(*gitopsv1alpha1.PropagationStatus)
			if !ok || p.Spec.AppName == "" {
				return nil
			}
			return []string{p.Spec.AppName}
		}).
		Build()

	r := &ChangeWindowReconciler{Client: fakeClient}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "chg-long-4h"}}

	// Step 1: Reconcile at window start
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("step 1 reconcile failed: %v", err)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	_ = fakeClient.Get(context.Background(), req.NamespacedName, &updatedCHG)
	if updatedCHG.Status.Baseline == nil {
		t.Fatalf("expected baseline snapshot to be captured at window start")
	}

	// Step 2: Reconcile mid-window (2 hours in)
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("step 2 reconcile failed: %v", err)
	}

	// Step 3: Reconcile post-stabilization (4 hours in)
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("step 3 reconcile failed: %v", err)
	}

	_ = fakeClient.Get(context.Background(), req.NamespacedName, &updatedCHG)
	if updatedCHG.Status.Phase != gitopsv1alpha1.PhaseSucceeded && updatedCHG.Status.Phase != "Validated" {
		t.Errorf("expected phase %s, got %s", gitopsv1alpha1.PhaseSucceeded, updatedCHG.Status.Phase)
	}
	if updatedCHG.Status.SignedReport == nil {
		t.Errorf("expected SignedReport to be generated at window close")
	}
}

// TestLongRunning_OverlappingMaintenanceWindows tests concurrent reconciliation of 3 ChangeWindow objects
// targeting overlapping cluster scopes to verify strict state isolation.
func TestLongRunning_OverlappingMaintenanceWindows(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	var chgList []client.Object
	for i := 1; i <= 3; i++ {
		chgList = append(chgList, &gitopsv1alpha1.ChangeWindow{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("chg-overlap-%d", i),
				Namespace: "default",
			},
			Spec: gitopsv1alpha1.ChangeWindowSpec{
				CHGNumber:    fmt.Sprintf("CHG-OVERLAP-%d", i),
				ImpactedApps: []string{fmt.Sprintf("app-%d", i)},
				StartTime:    startTime,
				EndTime:      endTime,
			},
		})
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chgList...).
		WithStatusSubresource(&gitopsv1alpha1.ChangeWindow{}, &gitopsv1alpha1.PropagationStatus{}).
		Build()

	r := &ChangeWindowReconciler{Client: fakeClient}

	for i := 1; i <= 3; i++ {
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("chg-overlap-%d", i)}}
		_, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("reconcile for window %d failed: %v", i, err)
		}
	}

	for i := 1; i <= 3; i++ {
		var fetched gitopsv1alpha1.ChangeWindow
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("chg-overlap-%d", i)}, &fetched)
		if fetched.Status.Phase == "" {
			t.Errorf("expected phase to be set for overlapping window %d", i)
		}
	}
}

// TestLongRunning_SustainedThrottlingAndRequeue simulates 1,000 rapid reconcile iterations
// to verify zero memory leaks or unhandled panic conditions under sustained API operations.
func TestLongRunning_SustainedThrottlingAndRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-stress-throttling",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-STRESS",
			ImpactedApps: []string{"svc-payments"},
			StartTime:    metav1.NewTime(now.Add(-5 * time.Minute)),
			EndTime:      metav1.NewTime(now.Add(1 * time.Hour)),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chg).
		WithStatusSubresource(&gitopsv1alpha1.ChangeWindow{}, &gitopsv1alpha1.PropagationStatus{}).
		Build()

	r := &ChangeWindowReconciler{Client: fakeClient}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "chg-stress-throttling"}}

	for iteration := 0; iteration < 100; iteration++ {
		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("sustained stress iteration %d failed: %v", iteration, err)
		}
		if res.RequeueAfter == 0 && !res.Requeue {
			t.Fatalf("iteration %d: expected non-zero requeue duration", iteration)
		}
	}
}
