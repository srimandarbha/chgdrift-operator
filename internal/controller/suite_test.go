package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

func psNamespacedName(ps *gitopsv1alpha1.PropagationStatus) types.NamespacedName {
	return types.NamespacedName{
		Namespace: ps.Namespace,
		Name:      ps.Name,
	}
}

func ctrlRequest(namespace, name string) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}
}

func TestPropagationStatusController(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	ps := &gitopsv1alpha1.PropagationStatus{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-payments",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.PropagationStatusSpec{
			AppName:          "svc-payments",
			ExpectedRevision: "a1b2c3d9",
			TargetClusters:   []string{"us-east-01", "us-east-02"},
		},
	}

	report1 := &gitopsv1alpha1.ClusterAppReport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "us-east-01-svc-payments",
			Namespace: "default",
			Labels:    map[string]string{AppLabelKey: "svc-payments"},
		},
		Spec: gitopsv1alpha1.ClusterAppReportSpec{
			ClusterName:      "us-east-01",
			AppName:          "svc-payments",
			AppNamespace:     "payments-prod",
			ObservedRevision: "a1b2c3d9",
			SyncStatus:       "Synced",
			Health:           "Healthy",
			ObservedAt:       metav1.Now(),
		},
	}

	report2 := &gitopsv1alpha1.ClusterAppReport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "us-east-02-svc-payments",
			Namespace: "default",
			Labels:    map[string]string{AppLabelKey: "svc-payments"},
		},
		Spec: gitopsv1alpha1.ClusterAppReportSpec{
			ClusterName:      "us-east-02",
			AppName:          "svc-payments",
			AppNamespace:     "payments-prod",
			ObservedRevision: "old-revision",
			SyncStatus:       "Synced",
			Health:           "Healthy",
			ObservedAt:       metav1.Now(),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ps, report1, report2).
		WithStatusSubresource(ps).
		WithIndex(&gitopsv1alpha1.ClusterAppReport{}, "spec.appName", func(obj client.Object) []string {
			rep, ok := obj.(*gitopsv1alpha1.ClusterAppReport)
			if !ok || rep.Spec.AppName == "" {
				return nil
			}
			return []string{rep.Spec.AppName}
		}).
		Build()

	r := &PropagationStatusReconciler{
		Client: fakeClient,
	}

	req := ctrlRequest("default", "svc-payments")
	ctx := context.Background()

	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("expected RequeueAfter 30s, got %v", res.RequeueAfter)
	}

	var updatedPS gitopsv1alpha1.PropagationStatus
	if err := fakeClient.Get(ctx, psNamespacedName(ps), &updatedPS); err != nil {
		t.Fatalf("failed to fetch updated PropagationStatus: %v", err)
	}

	if updatedPS.Status.Phase != StatePropagating {
		t.Errorf("expected phase %s, got %s", StatePropagating, updatedPS.Status.Phase)
	}
}

func TestChangeWindowReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-100",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:        "CHG-100",
			ReleaseTag:       "v1.0.0",
			ExpectedRevision: "rev-100",
			RootApp:          "app-root",
			ImpactedApps:     []string{"svc-payments"},
			StartTime:        startTime,
			EndTime:          endTime,
		},
	}

	ps := &gitopsv1alpha1.PropagationStatus{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-payments",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.PropagationStatusSpec{
			AppName:          "svc-payments",
			ExpectedRevision: "rev-100",
			TargetClusters:   []string{"us-east-01"},
		},
		Status: gitopsv1alpha1.PropagationStatusStatus{
			Phase: "Synced",
			ClusterStates: []gitopsv1alpha1.ClusterRevisionState{
				{
					ClusterName:      "us-east-01",
					ObservedRevision: "rev-100",
					SyncStatus:       "Synced",
					Health:           "Healthy",
					ObservedAt:       metav1.NewTime(now.Add(-1 * time.Minute)),
					State:            "InSync",
					MCPStatus: gitopsv1alpha1.MachineConfigPoolStatus{
						Name:                  "virt-worker",
						MachineCount:          3,
						ReadyMachineCount:     3,
						UpdatedNodeCount:      3,
						UpdatingNodeCount:     0,
						UnavailableNodeCount:  0,
						DegradedNodeCount:     0,
						CurrentRenderedConfig: "rendered-1",
						DesiredRenderedConfig: "rendered-1",
						Phase:                 "Updated",
					},
					VirtStatus: gitopsv1alpha1.VirtualizationImpactStatus{
						HyperConvergedHealth: "Healthy",
						VirtHandlerReady:     true,
					},
					PlatformObservation: gitopsv1alpha1.PlatformObservationStatus{
						ClusterOperators: []gitopsv1alpha1.ClusterOperatorStatus{
							{Name: "machine-config", Available: true, Degraded: false},
							{Name: "kubevirt", Available: true, Degraded: false},
						},
						ClusterVersion: gitopsv1alpha1.ClusterVersionStatus{
							Version:     "4.14.1",
							Available:   true,
							Progressing: false,
						},
						KubeVirt: gitopsv1alpha1.KubeVirtOperatorStatus{Phase: "Deployed", Ready: true},
						CDI:      gitopsv1alpha1.CDIOperatorStatus{Phase: "Deployed", Ready: true},
						SSP:      gitopsv1alpha1.SSPOperatorStatus{Phase: "Deployed", Ready: true},
					},
				},
			},
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

	r := &ChangeWindowReconciler{
		Client: fakeClient,
	}

	req := ctrlRequest("default", "chg-100")
	ctx := context.Background()

	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != 60*time.Second {
		t.Errorf("expected requeueAfter 60s for continuous Validated observation, got %v", res.RequeueAfter)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "chg-100"}, &updatedCHG); err != nil {
		t.Fatalf("failed to fetch updated ChangeWindow: %v", err)
	}

	if !updatedCHG.Status.Validation.Passed {
		t.Errorf("expected validation.Passed to be true, got false; issues: %v", updatedCHG.Status.Validation.IssuesFound)
	}
	if updatedCHG.Status.Phase != "Validated" {
		t.Errorf("expected phase Validated, got %s", updatedCHG.Status.Phase)
	}
	if !updatedCHG.Status.Validation.ClusterOperatorsHealthy {
		t.Errorf("expected ClusterOperatorsHealthy to be true, got false")
	}
}

func TestClusterOperatorDegradedFailGate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-co-degraded",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-CO-DEGRADED",
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
			TargetClusters: []string{"us-east-01"},
		},
		Status: gitopsv1alpha1.PropagationStatusStatus{
			Phase: "Synced",
			ClusterStates: []gitopsv1alpha1.ClusterRevisionState{
				{
					ClusterName: "us-east-01",
					SyncStatus:  "Synced",
					Health:      "Healthy",
					State:       "InSync",
					PlatformObservation: gitopsv1alpha1.PlatformObservationStatus{
						ClusterOperators: []gitopsv1alpha1.ClusterOperatorStatus{
							{Name: "network", Available: true, Degraded: true}, // Degraded ClusterOperator
						},
					},
				},
			},
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

	r := &ChangeWindowReconciler{
		Client: fakeClient,
	}

	req := ctrlRequest("default", "chg-co-degraded")
	ctx := context.Background()

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "chg-co-degraded"}, &updatedCHG); err != nil {
		t.Fatalf("failed to fetch updated ChangeWindow: %v", err)
	}

	if updatedCHG.Status.Validation.Passed {
		t.Errorf("expected validation.Passed to be false due to degraded ClusterOperator")
	}
	if updatedCHG.Status.Validation.ClusterOperatorsHealthy {
		t.Errorf("expected ClusterOperatorsHealthy to be false, got true")
	}
}

func TestTriStateFailClosedGating(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chgEmpty := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-empty",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-EMPTY",
			ReleaseTag:   "v1.0.0",
			RootApp:      "app-root",
			ImpactedApps: []string{},
			StartTime:    startTime,
			EndTime:      endTime,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chgEmpty).
		WithStatusSubresource(chgEmpty).
		Build()

	r := &ChangeWindowReconciler{
		Client: fakeClient,
	}

	req := ctrlRequest("default", "chg-empty")
	ctx := context.Background()

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "chg-empty"}, &updatedCHG); err != nil {
		t.Fatalf("failed to fetch updated ChangeWindow: %v", err)
	}

	if updatedCHG.Status.Validation.Passed {
		t.Errorf("expected validation.Passed to be false for empty impactedApps, got true")
	}
	if len(updatedCHG.Status.Validation.GateResults) == 0 {
		t.Fatalf("expected GateResults to be populated")
	}
	if updatedCHG.Status.Validation.GateResults[0].Status != gitopsv1alpha1.GateStatusUnknown {
		t.Errorf("expected GateStatusUnknown for empty impactedApps, got %s", updatedCHG.Status.Validation.GateResults[0].Status)
	}
}

func TestStabilizationResetOnRegression(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))
	stabStart := metav1.NewTime(now.Add(-5 * time.Minute))

	// CHG previously had stabilization started, but now state is OutOfSync
	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-regress",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:                  "CHG-REGRESS",
			ReleaseTag:                 "v1.0.0",
			ImpactedApps:               []string{"svc-payments"},
			StartTime:                  startTime,
			EndTime:                    endTime,
			StabilizationPeriodSeconds: 300,
		},
		Status: gitopsv1alpha1.ChangeWindowStatus{
			StabilizationStartedAt: &stabStart,
		},
	}

	ps := &gitopsv1alpha1.PropagationStatus{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-payments",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.PropagationStatusSpec{
			AppName:        "svc-payments",
			TargetClusters: []string{"us-east-01"},
		},
		Status: gitopsv1alpha1.PropagationStatusStatus{
			Phase: "Diverged",
			ClusterStates: []gitopsv1alpha1.ClusterRevisionState{
				{
					ClusterName: "us-east-01",
					State:       "Diverged", // OutOfSync regression
				},
			},
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

	r := &ChangeWindowReconciler{
		Client: fakeClient,
	}

	req := ctrlRequest("default", "chg-regress")
	ctx := context.Background()

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "chg-regress"}, &updatedCHG); err != nil {
		t.Fatalf("failed to fetch updated ChangeWindow: %v", err)
	}

	if updatedCHG.Status.StabilizationStartedAt != nil {
		t.Errorf("expected StabilizationStartedAt to be reset to nil on regression, got %v", updatedCHG.Status.StabilizationStartedAt)
	}
}

func TestParkedActionEmptyLogRef(t *testing.T) {
	r := &ChangeWindowReconciler{}
	now := time.Now()

	chg := &gitopsv1alpha1.ChangeWindow{
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:       "CHG-999",
			EvidenceRepoURL: "https://nexus.example.com",
		},
		Status: gitopsv1alpha1.ChangeWindowStatus{
			Actions: make(map[string]gitopsv1alpha1.ActionRecord),
		},
	}

	cs := gitopsv1alpha1.ClusterRevisionState{
		ClusterName: "us-east-01",
		State:       "Diverged",
	}

	r.runParkedHardRefreshAction(chg, "svc-payments", cs, now)

	action := chg.Status.Actions["svc-payments/us-east-01"]
	if len(action.History) == 0 {
		t.Fatalf("expected action history record")
	}
	if action.History[0].LogRef != "" {
		t.Errorf("expected empty LogRef for Parked action, got %s", action.History[0].LogRef)
	}
}

func TestLocalAppWatchReconciler(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-payments-descriptor",
			Namespace: "payments-prod",
			Labels: map[string]string{
				AppLabelKey: "svc-payments",
			},
		},
		Data: map[string]string{
			"syncStatus":       "Synced",
			"health":           "Healthy",
			"observedRevision": "rev-100",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cm).
		Build()

	r := &LocalAppWatchReconciler{
		Client:      fakeClient,
		ClusterName: "spoke-01",
	}

	req := ctrlRequest("payments-prod", "svc-payments-descriptor")
	ctx := context.Background()

	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("expected requeueAfter 30s, got %v", res.RequeueAfter)
	}

	var report gitopsv1alpha1.ClusterAppReport
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: reportNamespace, Name: "spoke-01-svc-payments"}, &report); err != nil {
		t.Fatalf("failed to fetch created report: %v", err)
	}

	if report.Spec.AppName != "svc-payments" || report.Spec.ClusterName != "spoke-01" {
		t.Errorf("unexpected report spec: %+v", report.Spec)
	}
}

func TestObjectChangesFromAnnotation(t *testing.T) {
	r := &LocalAppWatchReconciler{}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"gitops.example.com/last-sync-resources": "Deployment/payments-api=Updated:spec.template.spec.containers,metadata.labels\nService/payments-svc=Created",
			},
		},
	}
	changes := r.objectChangesFromAnnotation(cm)
	if len(changes) != 2 {
		t.Fatalf("expected 2 object changes, got %d", len(changes))
	}
	if changes[0].ChangeType != "Updated" || len(changes[0].ChangedFields) != 2 {
		t.Errorf("expected ChangeType 'Updated' and 2 changed fields, got %+v", changes[0])
	}
	if changes[1].ChangeType != "Created" || len(changes[1].ChangedFields) != 0 {
		t.Errorf("expected ChangeType 'Created' and 0 changed fields, got %+v", changes[1])
	}
}

func TestFailClosedDependencyCheck(t *testing.T) {
	r := &LocalAppWatchReconciler{}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"gitops.example.com/dependencies": "UnknownKind/some-resource",
			},
		},
	}
	deps := r.checkDependencies(context.Background(), "default", cm)
	if len(deps) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(deps))
	}
	if deps[0].Ready {
		t.Errorf("expected ready=false for unknown dependency kind, got true")
	}
}
