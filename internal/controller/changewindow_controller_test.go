package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

func TestChangeWindow_11GatesAllPass(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-11-gates-pass",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:        "CHG-11-GATES",
			ReleaseTag:       "v2.0.0",
			ExpectedRevision: "rev-200",
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
	req := ctrlRequest("default", "chg-11-gates-pass")

	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != 60*time.Second {
		t.Errorf("expected 60s requeue for validated window, got %v", res.RequeueAfter)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "chg-11-gates-pass"}, &updatedCHG); err != nil {
		t.Fatalf("failed to fetch updated CHG: %v", err)
	}

	if !updatedCHG.Status.Validation.Passed {
		t.Errorf("expected validation.Passed to be true; issues: %v", updatedCHG.Status.Validation.IssuesFound)
	}
	if len(updatedCHG.Status.Validation.GateResults) != 11 {
		t.Errorf("expected exactly 11 gate results, got %d", len(updatedCHG.Status.Validation.GateResults))
	}
	if updatedCHG.Status.Phase != "Validated" {
		t.Errorf("expected phase Validated, got %s", updatedCHG.Status.Phase)
	}
	if updatedCHG.Status.Baseline == nil {
		t.Errorf("expected baseline snapshot to be captured")
	} else if updatedCHG.Status.Baseline.ClusterVersion != "4.14.1" {
		t.Errorf("expected baseline ClusterVersion 4.14.1, got %s", updatedCHG.Status.Baseline.ClusterVersion)
	}
}

func TestChangeWindow_ClusterVersionProgressingGateFails(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-cv-progressing",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-CV-PROGRESSING",
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
			ClusterStates: []gitopsv1alpha1.ClusterRevisionState{
				{
					ClusterName: "cluster-01",
					SyncStatus:  "Synced",
					Health:      "Healthy",
					State:       "InSync",
					PlatformObservation: gitopsv1alpha1.PlatformObservationStatus{
						ClusterVersion: gitopsv1alpha1.ClusterVersionStatus{
							Version:        "4.14.1",
							DesiredVersion: "4.14.2",
							Progressing:    true,
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

	r := &ChangeWindowReconciler{Client: fakeClient}
	_, err := r.Reconcile(context.Background(), ctrlRequest("default", "chg-cv-progressing"))
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "chg-cv-progressing"}, &updatedCHG)

	if updatedCHG.Status.Validation.Passed {
		t.Errorf("expected validation to fail when ClusterVersion is progressing")
	}
}

func TestChangeWindow_PlatformOperatorsNotDeployedGateFails(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-platform-op-unready",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-PLATFORM-OP",
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
			ClusterStates: []gitopsv1alpha1.ClusterRevisionState{
				{
					ClusterName: "cluster-01",
					SyncStatus:  "Synced",
					Health:      "Healthy",
					State:       "InSync",
					PlatformObservation: gitopsv1alpha1.PlatformObservationStatus{
						KubeVirt: gitopsv1alpha1.KubeVirtOperatorStatus{Phase: "Deploying", Ready: false},
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

	r := &ChangeWindowReconciler{Client: fakeClient}
	_, err := r.Reconcile(context.Background(), ctrlRequest("default", "chg-platform-op-unready"))
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "chg-platform-op-unready"}, &updatedCHG)

	if updatedCHG.Status.Validation.Passed {
		t.Errorf("expected validation to fail when KubeVirt phase is Deploying")
	}
}

func TestChangeWindow_NodeMaintenanceActiveGateFails(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-10 * time.Minute))
	endTime := metav1.NewTime(now.Add(50 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg-node-maint",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:    "CHG-NODE-MAINT",
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
			ClusterStates: []gitopsv1alpha1.ClusterRevisionState{
				{
					ClusterName: "cluster-01",
					SyncStatus:  "Synced",
					Health:      "Healthy",
					State:       "InSync",
					PlatformObservation: gitopsv1alpha1.PlatformObservationStatus{
						NodeMaintenance: gitopsv1alpha1.NodeMaintenanceStatus{
							ActiveMaintenanceNodes: 2,
							MaintenanceNodeNames:   []string{"node-01", "node-02"},
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

	r := &ChangeWindowReconciler{Client: fakeClient}
	_, err := r.Reconcile(context.Background(), ctrlRequest("default", "chg-node-maint"))
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	var updatedCHG gitopsv1alpha1.ChangeWindow
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "chg-node-maint"}, &updatedCHG)

	if updatedCHG.Status.Validation.Passed {
		t.Errorf("expected validation to fail when NodeMaintenance is active")
	}
}

func TestBuildKafkaReportJSON_IncludesBaseline(t *testing.T) {
	r := &ChangeWindowReconciler{}
	now := time.Now()

	chg := &gitopsv1alpha1.ChangeWindow{
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:  "CHG-KAFKA-BASELINE",
			ReleaseTag: "v2.0.0",
			StartTime:  metav1.NewTime(now),
			EndTime:    metav1.NewTime(now.Add(1 * time.Hour)),
		},
		Status: gitopsv1alpha1.ChangeWindowStatus{
			Phase: "Validated",
			Baseline: &gitopsv1alpha1.BaselineSnapshot{
				CapturedAt:     metav1.NewTime(now),
				ClusterVersion: "4.14.1",
			},
		},
	}

	payload, err := r.BuildKafkaReportJSON(chg, now)
	if err != nil {
		t.Fatalf("unexpected error building report JSON: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatalf("invalid JSON generated: %v", err)
	}

	baseline, ok := parsed["baseline"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected baseline object in report JSON")
	}
	if baseline["clusterVersion"] != "4.14.1" {
		t.Errorf("expected clusterVersion 4.14.1 in baseline, got %v", baseline["clusterVersion"])
	}
}

func TestClusterDetector_SpokeByAgentNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	spokeNS := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "open-cluster-management-agent"},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(spokeNS).Build()
	ctx := context.Background()

	isHub := DetectClusterRole(ctx, client)
	if isHub {
		t.Errorf("expected DetectClusterRole to return false (spoke) when open-cluster-management-agent namespace exists")
	}
}

func TestClusterDetector_HubByMultiClusterHub(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	mch := &unstructured.Unstructured{}
	mch.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operator.open-cluster-management.io",
		Version: "v1",
		Kind:    "MultiClusterHub",
	})
	mch.SetName("multiclusterhub")
	mch.SetNamespace("open-cluster-management")

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mch).Build()
	ctx := context.Background()

	isHub := DetectClusterRole(ctx, client)
	if !isHub {
		t.Errorf("expected DetectClusterRole to return true (hub) when MultiClusterHub resource exists")
	}
}
