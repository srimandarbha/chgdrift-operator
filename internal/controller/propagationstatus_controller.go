package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
	"example.com/drift-operator/internal/metrics"
)

const (
	AppLabelKey = "gitops.example.com/app"

	StateInSync      = "InSync"
	StatePropagating = "Propagating"
	StateLagging     = "Lagging"
	StateDiverged    = "Diverged"
	StateStale       = "Stale"
	StateMissing     = "Missing"

	RequeueInterval = 30 * time.Second
)

// PropagationStatusReconciler aggregates per-cluster reports into a fleet-wide view.
type PropagationStatusReconciler struct {
	client.Client
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=gitops.example.com,resources=propagationstatuses,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gitops.example.com,resources=propagationstatuses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitops.example.com,resources=clusterappreports,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *PropagationStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var ps gitopsv1alpha1.PropagationStatus
	if err := r.Get(ctx, req.NamespacedName, &ps); err != nil {
		if apierrors.IsNotFound(err) {
			// Rule 2: NotFound - wait and return empty Result without error
			return ctrl.Result{}, nil
		}
		// Rule 2: Transient error - manage backoff explicitly
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Rule 3: Deletion check
	if !ps.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	originalStatus := ps.Status.DeepCopy()
	previousPhase := ps.Status.Phase

	// Reset lag clock if expected revision moved
	if ps.Status.LastExpectedRevision != ps.Spec.ExpectedRevision {
		ps.Status.LastExpectedRevision = ps.Spec.ExpectedRevision
		ps.Status.ExpectedRevisionSince = metav1.Now()
	}

	var reports gitopsv1alpha1.ClusterAppReportList
	// List matching reports from informer cache
	if err := r.List(ctx, &reports,
		client.InNamespace(ps.Namespace),
		client.MatchingFields{"spec.appName": ps.Spec.AppName},
	); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	byCluster := make(map[string]gitopsv1alpha1.ClusterAppReport, len(reports.Items))
	for _, rep := range reports.Items {
		byCluster[rep.Spec.ClusterName] = rep
	}

	lagThreshold := time.Duration(orDefault(ps.Spec.LagThresholdSeconds, 600)) * time.Second
	staleThreshold := time.Duration(orDefault(ps.Spec.StaleReportThresholdSeconds, 900)) * time.Second
	now := time.Now()

	var (
		states   []gitopsv1alpha1.ClusterRevisionState
		diverged []string
		lagging  []string
		stale    []string
		missing  []string
	)

	for _, clusterName := range ps.Spec.TargetClusters {
		rep, ok := byCluster[clusterName]

		if !ok {
			missing = append(missing, clusterName)
			states = append(states, gitopsv1alpha1.ClusterRevisionState{
				ClusterName: clusterName,
				State:       StateMissing,
			})
			metrics.RecordClusterMetrics(ps.Spec.AppName, clusterName, StateMissing, false, 0, -1)
			continue
		}

		reportAge := now.Sub(rep.Spec.ObservedAt.Time)
		state := StateInSync
		lagSeconds := 0.0

		switch {
		case reportAge > staleThreshold:
			state = StateStale
			stale = append(stale, clusterName)

		case rep.Spec.SyncStatus == "OutOfSync":
			state = StateDiverged
			diverged = append(diverged, clusterName)

		case rep.Spec.ObservedRevision != ps.Spec.ExpectedRevision:
			since := now.Sub(ps.Status.ExpectedRevisionSince.Time)
			lagSeconds = since.Seconds()
			if since > lagThreshold {
				state = StateLagging
				lagging = append(lagging, clusterName)
			} else {
				state = StatePropagating
			}

		default:
			state = StateInSync
		}

		states = append(states, gitopsv1alpha1.ClusterRevisionState{
			ClusterName:      clusterName,
			ObservedRevision: rep.Spec.ObservedRevision,
			SyncStatus:       rep.Spec.SyncStatus,
			Health:           rep.Spec.Health,
			ObservedAt:       rep.Spec.ObservedAt,
			MCPStatus:        rep.Spec.MCPStatus,
			State:            state,
		})
		metrics.RecordClusterMetrics(ps.Spec.AppName, clusterName, state, state == StateInSync, lagSeconds, reportAge.Seconds())
	}

	ps.Status.ClusterStates = states
	ps.Status.DivergedClusters = diverged
	ps.Status.LaggingClusters = lagging
	ps.Status.StaleClusters = stale
	ps.Status.MissingClusters = missing
	// Rule 4: Always set ObservedGeneration
	ps.Status.ObservedGeneration = ps.Generation
	ps.Status.Phase = rollUpPhase(diverged, stale, missing, lagging)

	setCondition(&ps.Status.Conditions, ps.Status.Phase)
	emitTransitionEvent(r.Recorder, &ps, previousPhase, diverged, lagging, stale, missing)

	// Rule 4: Patch status independently of spec using MergeFrom
	if err := r.Status().Patch(ctx, &ps, client.MergeFrom(&gitopsv1alpha1.PropagationStatus{Status: *originalStatus})); err != nil {
		if apierrors.IsConflict(err) {
			// Rule 2: Conflict error - retry immediately
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if len(diverged) > 0 || len(lagging) > 0 {
		logger.Info("propagation status reconciling", "app", ps.Spec.AppName, "phase", ps.Status.Phase, "divergedCount", len(diverged))
	}

	return ctrl.Result{RequeueAfter: RequeueInterval}, nil
}

func rollUpPhase(diverged, stale, missing, lagging []string) string {
	switch {
	case len(diverged) > 0:
		return "Diverged"
	case len(stale) > 0 || len(missing) > 0:
		return "Stale"
	case len(lagging) > 0:
		return "Propagating"
	default:
		return "Synced"
	}
}

func setCondition(conditions *[]metav1.Condition, phase string) {
	status := metav1.ConditionTrue
	if phase == "Diverged" || phase == "Stale" {
		status = metav1.ConditionFalse
	}
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:    "FullyPropagated",
		Status:  status,
		Reason:  phase,
		Message: fmt.Sprintf("fleet propagation phase: %s", phase),
	})
}

func emitTransitionEvent(rec record.EventRecorder, ps *gitopsv1alpha1.PropagationStatus, previousPhase string, diverged, lagging, stale, missing []string) {
	if rec == nil || ps.Status.Phase == previousPhase {
		return
	}
	switch ps.Status.Phase {
	case "Diverged":
		rec.Eventf(ps, corev1.EventTypeWarning, "Diverged", "%d cluster(s) locally out of sync: %v", len(diverged), diverged)
	case "Stale":
		rec.Eventf(ps, corev1.EventTypeWarning, "StaleReports", "%d stale, %d missing agent report(s): stale=%v missing=%v", len(stale), len(missing), stale, missing)
	case "Propagating":
		rec.Eventf(ps, corev1.EventTypeNormal, "Propagating", "%d cluster(s) still catching up: %v", len(lagging), lagging)
	case "Synced":
		rec.Event(ps, corev1.EventTypeNormal, "Synced", "all target clusters converged on expected revision")
	}
}

func orDefault(v int32, def int32) int32 {
	if v == 0 {
		return def
	}
	return v
}

func (r *PropagationStatusReconciler) mapReportToPropagationStatus(ctx context.Context, obj client.Object) []ctrl.Request {
	rep, ok := obj.(*gitopsv1alpha1.ClusterAppReport)
	if !ok || rep.Spec.AppName == "" {
		return nil
	}
	return []ctrl.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: rep.Namespace,
			Name:      rep.Spec.AppName,
		},
	}}
}

func (r *PropagationStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Rule 5: Register Field Indexers during manager setup
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &gitopsv1alpha1.ClusterAppReport{}, "spec.appName", func(rawObj client.Object) []string {
		report, ok := rawObj.(*gitopsv1alpha1.ClusterAppReport)
		if !ok || report.Spec.AppName == "" {
			return nil
		}
		return []string{report.Spec.AppName}
	}); err != nil {
		return fmt.Errorf("indexing spec.appName: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&gitopsv1alpha1.PropagationStatus{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Watches(
			&gitopsv1alpha1.ClusterAppReport{},
			handler.EnqueueRequestsFromMapFunc(r.mapReportToPropagationStatus),
		).
		Complete(r)
}
