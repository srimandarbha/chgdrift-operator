package controller

import (
	"context"
	"fmt"
	"sync"
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

// TestConcurrency_ParallelReconcileTicks validates thread safety by running 10 concurrent reconcilers
// against the same ChangeWindow CR instance simultaneously.
func TestConcurrency_ParallelReconcileTicks(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-concurrent-test",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-CONCURRENT",
			ImpactedApps: []string{"svc-payments"},
			StartTime:    startTime,
			EndTime:      endTime,
		},
	}

	ps := &gitopsv1alpha1.PropagationStatus{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-payments",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.PropagationStatusSpec{
			AppName:        "svc-payments",
			TargetClusters: []string{"cluster-01"},
		},
		Status: gitopsv1alpha1.PropagationStatusStatus{
			Phase: "Synced",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chg, ps).
		WithStatusSubresource(chg, ps).
		WithIndex(&gitopsv1alpha1.PropagationStatus{}, "spec.appName", func(obj client.Object) []string {
			p, ok := obj.(*gitopsv1alpha1.PropagationStatus)
			if !ok || p.Spec.AppName == "" {
				return nil
			}
			return []string{p.Spec.AppName}
		}).
		Build()

	r := &ChangeWindowReconciler{Client: fakeClient}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "chg-concurrent-test"}}

	const numGoroutines = 10
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			_, err := r.Reconcile(context.Background(), req)
			if err != nil {
				errChan <- fmt.Errorf("worker %d failed: %w", workerID, err)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrency error observed: %v", err)
	}

	var finalCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(context.Background(), req.NamespacedName, &finalCHG); err != nil {
		t.Fatalf("failed to fetch final CHG state: %v", err)
	}

	if finalCHG.Status.Phase == "" {
		t.Errorf("expected final CHG status phase to be populated after concurrent reconcile ticks")
	}
}

// BenchmarkPlatformEvaluation benchmarks graph and topological evaluation performance
// against 1,000 simulated cluster resources to verify performance scale.
func BenchmarkPlatformEvaluation(b *testing.B) {
	var mcpList []gitopsv1alpha1.MachineConfigPoolStatus
	for i := 0; i < 100; i++ {
		mcpList = append(mcpList, gitopsv1alpha1.MachineConfigPoolStatus{
			Name:              fmt.Sprintf("pool-%d", i),
			MachineCount:      10,
			ReadyMachineCount: 10,
			Phase:             "Updated",
		})
	}

	var coList []gitopsv1alpha1.ClusterOperatorStatus
	for i := 0; i < 30; i++ {
		coList = append(coList, gitopsv1alpha1.ClusterOperatorStatus{
			Name:      fmt.Sprintf("co-%d", i),
			Available: true,
			Degraded:  false,
		})
	}

	obs := gitopsv1alpha1.PlatformObservationStatus{
		MachineConfigPools: mcpList,
		ClusterOperators:   coList,
		VirtHealth: gitopsv1alpha1.VirtualizationImpactStatus{
			HyperConvergedHealth: "Healthy",
			VirtHandlerReady:     true,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EvaluatePlatformDependencyGraph(obs)
		_ = EvaluateTopologicalDAG(obs)
		_ = EvaluateClusterOperatorOrdering(obs.ClusterOperators)
	}
}
