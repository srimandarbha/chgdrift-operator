package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
	"example.com/drift-operator/internal/kafka"
)

// ChangeWindowReconciler reconciles ChangeWindow objects.
// KafkaBridge is optional; when non-nil, reports are published to Kafka on
// phase transitions in addition to being logged.
type ChangeWindowReconciler struct {
	client.Client
	Recorder    record.EventRecorder
	KafkaBridge *kafka.KafkaBridge // nil-safe; omit to disable Kafka publishing
}

// +kubebuilder:rbac:groups=gitops.example.com,resources=changewindows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gitops.example.com,resources=changewindows/status,verbs=get;update;patch

func (r *ChangeWindowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var chg gitopsv1alpha1.ChangeWindow
	if err := r.Get(ctx, req.NamespacedName, &chg); err != nil {
		if apierrors.IsNotFound(err) {
			// Rule 2: NotFound - return empty Result without error
			return ctrl.Result{}, nil
		}
		// Rule 2: Transient error - retry with explicit backoff
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Rule 3: Deletion Check
	if !chg.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	original := chg.DeepCopy()
	now := time.Now()

	// 1. Wait until CHG maintenance window startTime starts
	if now.Before(chg.Spec.StartTime.Time) {
		waitDuration := chg.Spec.StartTime.Time.Sub(now)
		logger.Info("CHG maintenance window pending start", "chg", chg.Spec.CHGNumber, "startsIn", waitDuration.String())
		return ctrl.Result{RequeueAfter: waitDuration}, nil
	}

	if chg.Status.AppStates == nil {
		chg.Status.AppStates = make(map[string]gitopsv1alpha1.AppClusterStateMap)
	}
	if chg.Status.Actions == nil {
		chg.Status.Actions = make(map[string]gitopsv1alpha1.ActionRecord)
	}

	staleThreshold := time.Duration(orDefault(chg.Spec.StaleReportThresholdSeconds, 300)) * time.Second

	// 2. Fetch Latest PropagationStatus for every impacted Application
	for _, appName := range chg.Spec.ImpactedApps {
		var psList gitopsv1alpha1.PropagationStatusList
		if err := r.List(ctx, &psList, client.InNamespace(chg.Namespace), client.MatchingFields{"spec.appName": appName}, client.Limit(100)); err == nil && len(psList.Items) > 0 {
			ps := psList.Items[0]
			chg.Status.AppStates[appName] = gitopsv1alpha1.AppClusterStateMap{
				Phase:         ps.Status.Phase,
				ClusterStates: ps.Status.ClusterStates,
			}
			continue
		}

		var ps gitopsv1alpha1.PropagationStatus
		psName := types.NamespacedName{Namespace: chg.Namespace, Name: appName}
		if err := r.Get(ctx, psName, &ps); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("PropagationStatus not found for impacted app", "appName", appName)
				continue
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		chg.Status.AppStates[appName] = gitopsv1alpha1.AppClusterStateMap{
			Phase:         ps.Status.Phase,
			ClusterStates: ps.Status.ClusterStates,
		}
	}

	// 3. Maintenance Silence & Silence Classification (Kafka CHG JSON Driven)
	var silentClusters []gitopsv1alpha1.SilentClusterState
	for appName, appStateMap := range chg.Status.AppStates {
		for _, cs := range appStateMap.ClusterStates {
			silence := r.classifySilence(appName, cs, &chg, now, staleThreshold)
			if silence.State != "Reporting" {
				silentClusters = append(silentClusters, silence)
			}
		}
	}
	chg.Status.SilentClusters = silentClusters

	// 4. Action Execution (Parked Hard Refresh Action Skeleton)
	for appName, appStateMap := range chg.Status.AppStates {
		for _, cs := range appStateMap.ClusterStates {
			if cs.State == "Diverged" || cs.State == "OutOfSync" {
				r.runParkedHardRefreshAction(&chg, appName, cs, now)
			}
		}
	}

	// 5. Post-Validation Logic (all seven gates must pass for Passed=true)
	allConverged := true
	healthCheckPassed := true
	mcpUpdatedOnTime := true
	eventsClean := true
	objectsConverged := true
	dependenciesReady := true

	for _, appStateMap := range chg.Status.AppStates {
		for _, cs := range appStateMap.ClusterStates {
			if cs.State != "InSync" {
				allConverged = false
			}
			if cs.Health != "Healthy" && cs.Health != "" {
				healthCheckPassed = false
			}
			if cs.MCPStatus.UpdatingNodeCount > 0 || cs.MCPStatus.DegradedNodeCount > 0 ||
				cs.MCPStatus.Phase == "Updating" || cs.MCPStatus.Phase == "Degraded" {
				mcpUpdatedOnTime = false
			}
			if len(cs.RecentEvents) > 0 {
				eventsClean = false
			}
			for _, oc := range cs.ObjectChanges {
				if oc.ChangeType == "Failed" {
					objectsConverged = false
				}
			}
			for _, dep := range cs.Dependencies {
				if !dep.Ready {
					dependenciesReady = false
				}
			}
		}
	}

	issuesFound := r.buildIssuesList(&chg)
	noSilence := len(chg.Status.SilentClusters) == 0

	chg.Status.Validation = gitopsv1alpha1.ValidationResult{
		AllChangesApplied: allConverged,
		HealthCheckPassed: healthCheckPassed,
		MCPUpdatedOnTime:  mcpUpdatedOnTime,
		EventsClean:       eventsClean,
		ObjectsConverged:  objectsConverged,
		DependenciesReady: dependenciesReady,
		IssuesFound:       issuesFound,
		Passed: allConverged && healthCheckPassed && mcpUpdatedOnTime &&
			eventsClean && objectsConverged && dependenciesReady && noSilence,
	}

	previousPhase := chg.Status.Phase
	if chg.Status.Validation.Passed {
		chg.Status.Phase = "Validated"
		chg.Status.OverallStatus = "Good"
	} else if now.After(chg.Spec.EndTime.Time) {
		chg.Status.Phase = "ValidationFailed"
		chg.Status.OverallStatus = "Degraded"
	} else {
		chg.Status.Phase = "InProgress"
		chg.Status.OverallStatus = "InProgress"
	}

	// 6. Generate Kafka JSON Report on phase transition or 60s tick
	if chg.Status.Phase != previousPhase || now.Sub(chg.Status.LastReportedAt.Time) > 60*time.Second {
		reportPayload, err := r.BuildKafkaReportJSON(&chg, now)
		if err == nil {
			logger.Info("Kafka report compiled", "chg", chg.Spec.CHGNumber, "phase", chg.Status.Phase, "payloadSizeBytes", len(reportPayload))
			// Publish to Kafka when the bridge is configured.
			// Errors are logged but not returned; a Kafka outage must never
			// block status reconciliation or cause a requeue storm.
			if r.KafkaBridge != nil {
				if kerr := r.KafkaBridge.ProduceReport(ctx, chg.Spec.CHGNumber, reportPayload); kerr != nil {
					logger.Error(kerr, "failed to publish report to Kafka", "chg", chg.Spec.CHGNumber)
				}
			}
			chg.Status.LastReportedAt = metav1.NewTime(now)
		}
	}

	// Rule 4: Patch status independently using MergeFrom if status changed
	if !reflect.DeepEqual(original.Status, chg.Status) {
		if err := r.Status().Patch(ctx, &chg, client.MergeFrom(original)); err != nil {
			if apierrors.IsConflict(err) {
				// Rule 2: Conflict retry
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	if chg.Status.Phase == "InProgress" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *ChangeWindowReconciler) classifySilence(appName string, cs gitopsv1alpha1.ClusterRevisionState, chg *gitopsv1alpha1.ChangeWindow, now time.Time, staleThreshold time.Duration) gitopsv1alpha1.SilentClusterState {
	age := now.Sub(cs.ObservedAt.Time)
	if age <= staleThreshold {
		return gitopsv1alpha1.SilentClusterState{State: "Reporting"}
	}

	sawReportSinceChgStart := cs.SawReportSinceChgStart || cs.ObservedAt.After(chg.Spec.StartTime.Time) || cs.ObservedAt.Time.Equal(chg.Spec.StartTime.Time)
	if !sawReportSinceChgStart {
		return gitopsv1alpha1.SilentClusterState{
			App:              appName,
			Cluster:          cs.ClusterName,
			State:            "SilentBeforeChgStart",
			LastSeenAt:       cs.ObservedAt,
			SilentForSeconds: int64(age.Seconds()),
		}
	}

	return gitopsv1alpha1.SilentClusterState{
		App:              appName,
		Cluster:          cs.ClusterName,
		State:            "WentSilentDuringChg",
		LastSeenAt:       cs.ObservedAt,
		SilentForSeconds: int64(age.Seconds()),
	}
}

func (r *ChangeWindowReconciler) runParkedHardRefreshAction(chg *gitopsv1alpha1.ChangeWindow, appName string, cs gitopsv1alpha1.ClusterRevisionState, now time.Time) {
	key := fmt.Sprintf("%s/%s", appName, cs.ClusterName)
	action, exists := chg.Status.Actions[key]
	if !exists {
		action = gitopsv1alpha1.ActionRecord{
			App:         appName,
			Cluster:     cs.ClusterName,
			MaxAttempts: orDefault(chg.Spec.HardRefresh.MaxAttempts, 2),
			Result:      "Pending",
		}
	}

	if cs.State == "InSync" {
		if action.Attempts > 0 && action.Result != "Resolved" {
			action.Result = "Resolved"
		}
		chg.Status.Actions[key] = action
		return
	}

	maxAttempts := orDefault(chg.Spec.HardRefresh.MaxAttempts, 2)
	if action.Attempts >= maxAttempts {
		action.Result = "ExhaustedRetries"
		chg.Status.Actions[key] = action
		return
	}

	waitInterval := time.Duration(orDefault(chg.Spec.HardRefresh.WaitBetweenSeconds, 180)) * time.Second
	if !action.LastAttemptAt.IsZero() && now.Sub(action.LastAttemptAt.Time) < waitInterval {
		return
	}

	// Rule 4: Size Limit Discipline - Capped inline logs (max 20 lines / 2KB) to stay well under etcd limits (1.5MB)
	tailLogs := []string{
		fmt.Sprintf("%s - [SIMULATED] WARN: ImagePullBackOff on container %s-api", now.Format(time.RFC3339), appName),
		fmt.Sprintf("%s - [SIMULATED] ERROR: Connection timeout connecting to registry.internal", now.Format(time.RFC3339)),
	}
	if len(tailLogs) > 20 {
		tailLogs = tailLogs[len(tailLogs)-20:]
	}

	nexusBaseURL := chg.Spec.EvidenceRepoURL
	if nexusBaseURL == "" {
		nexusBaseURL = os.Getenv("NEXUS_RAW_REPO_URL")
	}
	if nexusBaseURL == "" {
		nexusBaseURL = "https://nexus.company.com:8081/repository/gitops-evidence"
	}

	logRef := fmt.Sprintf("%s/%s/%s-%s-attempt-%d.log", strings.TrimSuffix(nexusBaseURL, "/"), chg.Spec.CHGNumber, cs.ClusterName, appName, action.Attempts+1)

	action.Attempts++
	action.LastAttemptAt = metav1.NewTime(now)
	action.NextEligibleAt = metav1.NewTime(now.Add(waitInterval))
	action.Result = "Parked"

	action.History = append(action.History, gitopsv1alpha1.ActionAttemptHistory{
		Attempt:       action.Attempts,
		Type:          "HardRefresh",
		TriggeredAt:   metav1.NewTime(now),
		TriggerResult: "Parked (Execution disabled in operator config)",
		LogRef:        logRef,
		LogSummary:    fmt.Sprintf("SIMULATED (stub - live pod log query pending): ImagePullBackOff on %s in cluster %s", appName, cs.ClusterName),
		TailLogs:      tailLogs,
	})

	chg.Status.Actions[key] = action
}

func (r *ChangeWindowReconciler) buildIssuesList(chg *gitopsv1alpha1.ChangeWindow) []string {
	var issues []string
	for appName, appStateMap := range chg.Status.AppStates {
		for _, cs := range appStateMap.ClusterStates {
			// State / sync divergence
			if cs.State == "Diverged" || cs.State == "OutOfSync" {
				issues = append(issues, fmt.Sprintf("%s/%s: local drift (OutOfSync)", appName, cs.ClusterName))
			} else if cs.State == "Lagging" {
				issues = append(issues, fmt.Sprintf("%s/%s: observed revision %s trailing expected %s",
					appName, cs.ClusterName, cs.ObservedRevision, chg.Spec.ExpectedRevision))
			}
			// Health
			if cs.Health != "Healthy" && cs.Health != "" {
				issues = append(issues, fmt.Sprintf("%s/%s: post-health check failed (Health = %s)",
					appName, cs.ClusterName, cs.Health))
			}
			// MCP rollout
			if cs.MCPStatus.UpdatingNodeCount > 0 {
				issues = append(issues, fmt.Sprintf("%s/%s: MachineConfigPool %s still updating %d node(s)",
					appName, cs.ClusterName, cs.MCPStatus.Name, cs.MCPStatus.UpdatingNodeCount))
			}
			if cs.MCPStatus.DegradedNodeCount > 0 {
				issues = append(issues, fmt.Sprintf("%s/%s: MachineConfigPool %s has %d degraded node(s)",
					appName, cs.ClusterName, cs.MCPStatus.Name, cs.MCPStatus.DegradedNodeCount))
			}
			// Warning events
			for _, ev := range cs.RecentEvents {
				issues = append(issues, fmt.Sprintf("%s/%s: Warning event %s on %s (×%d): %s",
					appName, cs.ClusterName, ev.Reason, ev.InvolvedObject, ev.Count, ev.Message))
			}
			// Failed object changes
			for _, oc := range cs.ObjectChanges {
				if oc.ChangeType == "Failed" {
					issues = append(issues, fmt.Sprintf("%s/%s: ArgoCD resource %s/%s failed to sync",
						appName, cs.ClusterName, oc.Kind, oc.Name))
				}
			}
			// Unready dependencies
			for _, dep := range cs.Dependencies {
				if !dep.Ready {
					note := dep.Note
					if note != "" {
						note = " (" + note + ")"
					}
					issues = append(issues, fmt.Sprintf("%s/%s: dependency %s/%s not ready%s",
						appName, cs.ClusterName, dep.Kind, dep.Name, note))
				}
			}

		}
	}
	for _, s := range chg.Status.SilentClusters {
		if s.State == "WentSilentDuringChg" {
			issues = append(issues, fmt.Sprintf("%s/%s: agent stopped reporting after CHG start", s.App, s.Cluster))
		} else {
			issues = append(issues, fmt.Sprintf("%s/%s: agent was silent before CHG start", s.App, s.Cluster))
		}
	}
	return issues
}

func (r *ChangeWindowReconciler) BuildKafkaReportJSON(chg *gitopsv1alpha1.ChangeWindow, now time.Time) ([]byte, error) {
	reportMap := map[string]interface{}{
		"chgNumber":         chg.Spec.CHGNumber,
		"releaseTag":        chg.Spec.ReleaseTag,
		"expectedRevision":  chg.Spec.ExpectedRevision,
		"reportGeneratedAt": now.Format(time.RFC3339),
		"window": map[string]string{
			"start": chg.Spec.StartTime.Time.Format(time.RFC3339),
			"end":   chg.Spec.EndTime.Time.Format(time.RFC3339),
		},
		"phase":          chg.Status.Phase,
		"overallStatus":  chg.Status.OverallStatus,
		"rootApp":        chg.Spec.RootApp,
		"silentClusters": chg.Status.SilentClusters,
		"actionsApplied": chg.Status.Actions,
		"validation":     chg.Status.Validation,
	}
	return json.MarshalIndent(reportMap, "", "  ")
}

func (r *ChangeWindowReconciler) mapPropagationStatusToChangeWindow(ctx context.Context, obj client.Object) []ctrl.Request {
	ps, ok := obj.(*gitopsv1alpha1.PropagationStatus)
	if !ok || ps.Spec.AppName == "" {
		return nil
	}
	var chgList gitopsv1alpha1.ChangeWindowList
	if err := r.List(ctx, &chgList, client.InNamespace(ps.Namespace), client.MatchingFields{"spec.impactedApps": ps.Spec.AppName}, client.Limit(100)); err != nil {
		return nil
	}
	var reqs []ctrl.Request
	for _, chg := range chgList.Items {
		reqs = append(reqs, ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: chg.Namespace,
				Name:      chg.Name,
			},
		})
	}
	return reqs
}

func (r *ChangeWindowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &gitopsv1alpha1.ChangeWindow{}, "spec.impactedApps", func(rawObj client.Object) []string {
		chg, ok := rawObj.(*gitopsv1alpha1.ChangeWindow)
		if !ok {
			return nil
		}
		return chg.Spec.ImpactedApps
	}); err != nil {
		return fmt.Errorf("indexing spec.impactedApps: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&gitopsv1alpha1.ChangeWindow{}).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 5}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Watches(
			&gitopsv1alpha1.PropagationStatus{},
			handler.EnqueueRequestsFromMapFunc(r.mapPropagationStatusToChangeWindow),
		).
		Complete(r)
}
