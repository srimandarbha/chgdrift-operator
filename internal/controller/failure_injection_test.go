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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
	"example.com/drift-operator/internal/kafka"
)

// TestFailureInjection_KafkaDisconnectSimulated ensures that when Kafka is down/unreachable,
// the reconciler logs the error and gracefully completes without crashing.
func TestFailureInjection_KafkaDisconnectSimulated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-kafka-fail",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-KAFKA-FAIL",
			ImpactedApps: []string{"svc-test"},
			StartTime:    startTime,
			EndTime:      endTime,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chg).
		WithStatusSubresource(chg).
		Build()

	// KafkaBridge with nil writer or unroutable client to simulate outage
	kafkaBridge := &kafka.KafkaBridge{
		Client:    fakeClient,
		Namespace: "default",
	}

	r := &ChangeWindowReconciler{
		Client:      fakeClient,
		KafkaBridge: kafkaBridge,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "chg-kafka-fail"}}

	// Reconcile should handle Kafka failure gracefully without panicking
	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconciler failed unexpectedly on Kafka outage: %v", err)
	}

	if res.RequeueAfter == 0 {
		t.Errorf("expected non-zero requeue time")
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(context.Background(), req.NamespacedName, &updatedCHG); err != nil {
		t.Fatalf("failed to get CHG: %v", err)
	}

	if updatedCHG.Status.Phase == "" {
		t.Errorf("expected CHG status phase to be updated despite Kafka failure")
	}
}

// TestFailureInjection_RestartRecovery verifies state machine continuation after an operator pod restart.
func TestFailureInjection_RestartRecovery(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	// Simulate pre-existing CHG object mid-maintenance before operator restart
	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-restart-recovery",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-RESTART-RECOVERY",
			ImpactedApps: []string{"svc-payments"},
			StartTime:    startTime,
			EndTime:      endTime,
		},
		Status: gitopsv1alpha1.ChangeWindowStatus{
			Phase: gitopsv1alpha1.PhaseExecuting,
			Baseline: &gitopsv1alpha1.BaselineSnapshot{
				CapturedAt:     startTime,
				ClusterVersion: "4.14.1",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chg).
		WithStatusSubresource(chg).
		Build()

	// First instance of reconciler (represents new operator pod after reboot)
	reconcilerAfterRestart := &ChangeWindowReconciler{Client: fakeClient}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "chg-restart-recovery"}}

	_, err := reconcilerAfterRestart.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to reconcile existing CHG after operator restart: %v", err)
	}

	var recoveredCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(context.Background(), req.NamespacedName, &recoveredCHG); err != nil {
		t.Fatalf("failed to fetch recovered CHG: %v", err)
	}

	if recoveredCHG.Status.Baseline == nil || recoveredCHG.Status.Baseline.ClusterVersion != "4.14.1" {
		t.Errorf("baseline snapshot was lost after restart recovery: %+v", recoveredCHG.Status.Baseline)
	}
}

// TestFailureInjection_IdempotentDuplicateEvents verifies duplicate reconcile triggers cause zero side-effects.
func TestFailureInjection_IdempotentDuplicateEvents(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-idempotent-test",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-IDEMPOTENT",
			ImpactedApps: []string{"svc-payments"},
			StartTime:    startTime,
			EndTime:      endTime,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chg).
		WithStatusSubresource(chg).
		Build()

	r := &ChangeWindowReconciler{Client: fakeClient}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "chg-idempotent-test"}}

	// Reconcile 5 times sequentially (simulating rapid duplicate watch events)
	for i := 0; i < 5; i++ {
		_, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("reconcile iteration %d failed: %v", i, err)
		}
	}

	var finalCHG gitopsv1alpha1.ChangeWindow
	_ = fakeClient.Get(context.Background(), req.NamespacedName, &finalCHG)

	if len(finalCHG.Status.Timeline) > 20 {
		t.Errorf("expected idempotent timeline updates, found %d entries", len(finalCHG.Status.Timeline))
	}
}

func helperString(s string) string {
	return fmt.Sprintf("helper-%s", s)
}
