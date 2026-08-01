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

// psNamespacedName returns the NamespacedName for a PropagationStatus.
// Go does not allow attaching methods to types from external packages, so we
// use a package-level helper instead of a method receiver.
func psNamespacedName(ps *gitopsv1alpha1.PropagationStatus) types.NamespacedName {
	return types.NamespacedName{
		Namespace: ps.Namespace,
		Name:      ps.Name,
	}
}

// ctrlRequest constructs a controller-runtime reconcile request.
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

	// WithIndex registers the same field indexer that SetupWithManager adds,
	// so the reconciler's List(MatchingFields{"spec.appName": ...}) works
	// against the fake client.
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ps, report1).
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
	if res.RequeueAfter != RequeueInterval {
		t.Errorf("expected requeueAfter %v, got %v", RequeueInterval, res.RequeueAfter)
	}

	var updatedPS gitopsv1alpha1.PropagationStatus
	if err := fakeClient.Get(ctx, psNamespacedName(ps), &updatedPS); err != nil {
		t.Fatalf("failed to fetch updated PS: %v", err)
	}

	if len(updatedPS.Status.MissingClusters) != 1 || updatedPS.Status.MissingClusters[0] != "us-east-02" {
		t.Errorf("expected us-east-02 to be missing, got missing=%v", updatedPS.Status.MissingClusters)
	}

	if len(updatedPS.Status.ClusterStates) == 0 || updatedPS.Status.ClusterStates[0].AppNamespace != "payments-prod" {
		t.Errorf("expected cluster state appNamespace 'payments-prod', got %s", updatedPS.Status.ClusterStates[0].AppNamespace)
	}
}

func TestChangeWindowSilenceClassification(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	now := time.Now()
	startTime := metav1.NewTime(now.Add(-30 * time.Minute))
	endTime := metav1.NewTime(now.Add(30 * time.Minute))

	chg := &gitopsv1alpha1.ChangeWindow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chg0012345",
			Namespace: "default",
		},
		Spec: gitopsv1alpha1.ChangeWindowSpec{
			CHGNumber:                   "CHG0012345",
			ReleaseTag:                  "v2.4.0",
			ExpectedRevision:            "a1b2c3d9",
			RootApp:                     "platform-root",
			ImpactedApps:                []string{"svc-payments"},
			StartTime:                   startTime,
			EndTime:                     endTime,
			StaleReportThresholdSeconds: 300,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(chg).
		WithStatusSubresource(chg).
		Build()

	r := &ChangeWindowReconciler{
		Client: fakeClient,
	}

	// Test 1: Active cluster reporting within threshold
	csActive := gitopsv1alpha1.ClusterRevisionState{
		ClusterName:      "us-east-01",
		ObservedRevision: "a1b2c3d9",
		SyncStatus:       "Synced",
		Health:           "Healthy",
		ObservedAt:       metav1.NewTime(now.Add(-1 * time.Minute)),
	}

	silenceActive := r.classifySilence("svc-payments", csActive, chg, now, 300*time.Second)
	if silenceActive.State != "Reporting" {
		t.Errorf("expected active cluster state Reporting, got %s", silenceActive.State)
	}

	// Test 2: Cluster dark before CHG start
	csDarkBefore := gitopsv1alpha1.ClusterRevisionState{
		ClusterName:            "us-east-02",
		ObservedRevision:       "a1b2c3d9",
		SyncStatus:             "Synced",
		Health:                 "Healthy",
		ObservedAt:             metav1.NewTime(now.Add(-40 * time.Minute)),
		SawReportSinceChgStart: false,
	}

	silenceBefore := r.classifySilence("svc-payments", csDarkBefore, chg, now, 300*time.Second)
	if silenceBefore.State != "SilentBeforeChgStart" {
		t.Errorf("expected state SilentBeforeChgStart, got %s", silenceBefore.State)
	}

	// Test 3: Cluster went silent during CHG
	csWentSilent := gitopsv1alpha1.ClusterRevisionState{
		ClusterName:            "us-east-03",
		ObservedRevision:       "a1b2c3d9",
		SyncStatus:             "Synced",
		Health:                 "Healthy",
		ObservedAt:             metav1.NewTime(now.Add(-10 * time.Minute)),
		SawReportSinceChgStart: true,
	}

	silenceDuring := r.classifySilence("svc-payments", csWentSilent, chg, now, 300*time.Second)
	if silenceDuring.State != "WentSilentDuringChg" {
		t.Errorf("expected state WentSilentDuringChg, got %s", silenceDuring.State)
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
	if res.RequeueAfter != 0 {
		t.Errorf("expected requeueAfter 0s for Validated window, got %v", res.RequeueAfter)
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

	// Reconcile again to test patch logic
	res, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected reconcile error on update: %v", err)
	}
}
