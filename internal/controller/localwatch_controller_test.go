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

func TestLocalAppWatch_Idempotency(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "payments-api-descriptor",
			Namespace: "payments-prod",
			Labels: map[string]string{
				"gitops.example.com/app-descriptor": "true",
				"gitops.example.com/app":            "payments-api",
			},
		},
		Data: map[string]string{
			"syncStatus": "Synced",
			"health":     "Healthy",
			"revision":   "a1b2c3d4",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cm).
		Build()

	r := &LocalAppWatchReconciler{
		Client:      fakeClient,
		ClusterName: "us-east-01",
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "payments-prod",
			Name:      "payments-api-descriptor",
		},
	}

	// Reconcile 5 consecutive times to verify idempotency
	for i := 0; i < 5; i++ {
		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("iteration %d: unexpected reconcile error: %v", i, err)
		}
		if res.RequeueAfter != 30*time.Second {
			t.Errorf("iteration %d: expected RequeueAfter 30s, got %v", i, res.RequeueAfter)
		}
	}

	// Verify only 1 report exists
	var reportList gitopsv1alpha1.ClusterAppReportList
	if err := fakeClient.List(context.Background(), &reportList); err != nil {
		t.Fatalf("failed to list reports: %v", err)
	}
	if len(reportList.Items) != 1 {
		t.Fatalf("expected exactly 1 ClusterAppReport, got %d", len(reportList.Items))
	}

	rep := reportList.Items[0]
	if rep.Spec.ClusterName != "us-east-01" || rep.Spec.AppName != "payments-api" {
		t.Errorf("unexpected report spec: %+v", rep.Spec)
	}
	if len(rep.Spec.PlatformObservation.DependencyGraph.Nodes) == 0 {
		t.Errorf("expected DependencyGraph to be populated on report spec")
	}
}

func TestLocalAppWatch_StatusOnlySuppression(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "auth-service-descriptor",
			Namespace: "auth-prod",
			Labels: map[string]string{
				"gitops.example.com/app-descriptor": "true",
				"gitops.example.com/app":            "auth-service",
			},
		},
		Data: map[string]string{
			"syncStatus": "Synced",
			"health":     "Healthy",
			"revision":   "f9e8d7c6",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cm).
		Build()

	r := &LocalAppWatchReconciler{
		Client:      fakeClient,
		ClusterName: "us-west-02",
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "auth-prod",
			Name:      "auth-service-descriptor",
		},
	}

	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	var report1 gitopsv1alpha1.ClusterAppReport
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "gitops-fleet", Name: "us-west-02-auth-service"}, &report1); err != nil {
		t.Fatalf("failed to fetch report: %v", err)
	}

	// Reconcile again without mutating descriptor
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	var report2 gitopsv1alpha1.ClusterAppReport
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "gitops-fleet", Name: "us-west-02-auth-service"}, &report2); err != nil {
		t.Fatalf("failed to fetch report: %v", err)
	}

	if report1.Spec.ObservedRevision != report2.Spec.ObservedRevision {
		t.Errorf("expected observed revision to remain identical: %s vs %s", report1.Spec.ObservedRevision, report2.Spec.ObservedRevision)
	}
}
