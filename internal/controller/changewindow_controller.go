package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
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
		// Rule 2: Transient error - propagate error for exponential backoff & metrics
		return ctrl.Result{}, fmt.Errorf("failed to get ChangeWindow %s: %w", req.NamespacedName, err)
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
		chg.Status.Phase = "Pending"
		chg.Status.OverallStatus = "Pending"
		if !reflect.DeepEqual(original.Status, chg.Status) {
			_ = r.Status().Patch(ctx, &chg, client.MergeFrom(original))
		}
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
			return ctrl.Result{}, fmt.Errorf("failed to get PropagationStatus %s: %w", psName, err)
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

	// 4. Action Execution (Parked Hard Refresh Action)
	for appName, appStateMap := range chg.Status.AppStates {
		for _, cs := range appStateMap.ClusterStates {
			if cs.State == "Diverged" || cs.State == "OutOfSync" {
				r.runParkedHardRefreshAction(&chg, appName, cs, now)
			}
		}
	}

	// 5. Post-Validation Logic & Maintenance Pipeline (PreChecking -> InProgress -> Stabilizing -> Validated)
	validationRes, _ := r.evaluateGates(ctx, &chg, now)
	chg.Status.Validation = validationRes

	previousPhase := chg.Status.Phase
	var requeueAfter time.Duration

	// Capture baseline snapshot at window start for evidence model if not already set
	if chg.Status.Baseline == nil {
		chg.Status.Baseline = r.captureBaseline(&chg, now)
	}

	switch {
	case validationRes.Passed:
		stabSeconds := time.Duration(orDefault(chg.Spec.StabilizationPeriodSeconds, 0)) * time.Second
		if stabSeconds > 0 {
			if chg.Status.StabilizationStartedAt == nil {
				nowMeta := metav1.NewTime(now)
				chg.Status.StabilizationStartedAt = &nowMeta
			}
			stabEnd := chg.Status.StabilizationStartedAt.Time.Add(stabSeconds)
			if now.Before(stabEnd) {
				chg.Status.Phase = "Stabilizing"
				chg.Status.OverallStatus = "InProgress"
				requeueAfter = 15 * time.Second
			} else {
				chg.Status.Phase = "Validated"
				chg.Status.OverallStatus = "Good"
				requeueAfter = 60 * time.Second
			}
		} else {
			chg.Status.Phase = "Validated"
			chg.Status.OverallStatus = "Good"
			requeueAfter = 60 * time.Second
		}

	case now.After(chg.Spec.EndTime.Time):
		// Reset stabilization clock on regression/failure
		chg.Status.StabilizationStartedAt = nil
		chg.Status.Phase = "ValidationFailed"
		chg.Status.OverallStatus = "Degraded"
		requeueAfter = 60 * time.Second

	default:
		// Reset stabilization clock on regression
		chg.Status.StabilizationStartedAt = nil
		if previousPhase == "Pending" || previousPhase == "" {
			chg.Status.Phase = "PreChecking"
		} else {
			chg.Status.Phase = "InProgress"
		}
		chg.Status.OverallStatus = "InProgress"
		requeueAfter = 15 * time.Second
	}

	// 6. Record timeline entry on phase change
	validationChanged := !reflect.DeepEqual(original.Status.Validation, chg.Status.Validation)
	phaseChanged := chg.Status.Phase != previousPhase
	if phaseChanged {
		chg.Status.Timeline = append(chg.Status.Timeline, gitopsv1alpha1.TimelineEntry{
			Timestamp:   metav1.NewTime(now),
			Stage:       chg.Status.Phase,
			Category:    "Platform",
			Resource:    chg.Spec.CHGNumber,
			Event:       "PhaseTransition",
			Description: fmt.Sprintf("Maintenance window phase transitioned from '%s' to '%s'", previousPhase, chg.Status.Phase),
		})
		if len(chg.Status.Timeline) > 50 {
			chg.Status.Timeline = chg.Status.Timeline[len(chg.Status.Timeline)-50:]
		}
	}
	timeForHeartbeat := !chg.Status.LastReportedAt.IsZero() && now.Sub(chg.Status.LastReportedAt.Time) >= 15*time.Minute

	if phaseChanged || validationChanged || timeForHeartbeat {
		reportPayload, err := r.BuildKafkaReportJSON(&chg, now)
		if err == nil {
			logger.Info("Kafka report compiled", "chg", chg.Spec.CHGNumber, "phase", chg.Status.Phase, "payloadSizeBytes", len(reportPayload), "phaseChanged", phaseChanged, "validationChanged", validationChanged, "heartbeat", timeForHeartbeat)
			publishedSuccessfully := true
			if r.KafkaBridge != nil {
				if kerr := r.KafkaBridge.ProduceReport(ctx, chg.Spec.CHGNumber, reportPayload); kerr != nil {
					logger.Error(kerr, "failed to publish report to Kafka", "chg", chg.Spec.CHGNumber)
					publishedSuccessfully = false
				}
			}
			if publishedSuccessfully {
				chg.Status.LastReportedAt = metav1.NewTime(now)
			}
		}
	}

	// Rule 4: Patch status independently using MergeFrom if status changed
	if !reflect.DeepEqual(original.Status, chg.Status) {
		if err := r.Status().Patch(ctx, &chg, client.MergeFrom(original)); err != nil {
			if apierrors.IsConflict(err) {
				// Rule 2: Conflict retry
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("failed to patch ChangeWindow status %s: %w", req.NamespacedName, err)
		}
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *ChangeWindowReconciler) evaluateGates(ctx context.Context, chg *gitopsv1alpha1.ChangeWindow, now time.Time) (gitopsv1alpha1.ValidationResult, []string) {
	var issues []string

	gateClusterOps := gitopsv1alpha1.GateResult{
		Name:       "ClusterOperatorsHealthy",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "AllClusterOperatorsHealthy",
		Message:    "All OpenShift ClusterOperators report Available=True and Degraded=False",
		ObservedAt: metav1.NewTime(now),
	}

	gateAllChanges := gitopsv1alpha1.GateResult{
		Name:       "AllChangesApplied",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "AllClustersInSync",
		Message:    "All target cluster applications report InSync",
		ObservedAt: metav1.NewTime(now),
	}

	gateHealth := gitopsv1alpha1.GateResult{
		Name:       "HealthCheckPassed",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "AllWorkloadsHealthy",
		Message:    "All target cluster workloads report Healthy",
		ObservedAt: metav1.NewTime(now),
	}

	gateMCP := gitopsv1alpha1.GateResult{
		Name:       "MCPUpdatedOnTime",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "MachineConfigPoolConverged",
		Message:    "MachineConfigPool rollout updated and non-degraded",
		ObservedAt: metav1.NewTime(now),
	}

	gateEvents := gitopsv1alpha1.GateResult{
		Name:       "EventsClean",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "NoNewWarningEvents",
		Message:    "No unresolved warning events observed during window",
		ObservedAt: metav1.NewTime(now),
	}

	gateObjects := gitopsv1alpha1.GateResult{
		Name:       "ObjectsConverged",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "AllObjectsConverged",
		Message:    "All declared objects synced without failure",
		ObservedAt: metav1.NewTime(now),
	}

	gateDeps := gitopsv1alpha1.GateResult{
		Name:       "DependenciesReady",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "AllDependenciesReady",
		Message:    "All referenced external dependencies are ready",
		ObservedAt: metav1.NewTime(now),
	}

	gateVirt := gitopsv1alpha1.GateResult{
		Name:       "VirtImpactPassed",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "VirtualizationPlatformHealthy",
		Message:    "HyperConverged healthy and no stalled migrations",
		ObservedAt: metav1.NewTime(now),
	}

	gateClusterVersion := gitopsv1alpha1.GateResult{
		Name:       "ClusterVersionStable",
		Status:     gitopsv1alpha1.GateStatusUnknown,
		Reason:     "ClusterVersionNotReported",
		Message:    "ClusterVersion telemetry was not observed",
		ObservedAt: metav1.NewTime(now),
	}

	gatePlatformOps := gitopsv1alpha1.GateResult{
		Name:       "PlatformOperatorsDeployed",
		Status:     gitopsv1alpha1.GateStatusUnknown,
		Reason:     "PlatformOperatorsNotReported",
		Message:    "KubeVirt/CDI/SSP operator telemetry was not observed",
		ObservedAt: metav1.NewTime(now),
	}

	gateNodeMaint := gitopsv1alpha1.GateResult{
		Name:       "NoActiveNodeMaintenance",
		Status:     gitopsv1alpha1.GateStatusTrue,
		Reason:     "NoMaintenanceNodes",
		Message:    "No active NodeMaintenance objects detected",
		ObservedAt: metav1.NewTime(now),
	}

	if len(chg.Spec.ImpactedApps) == 0 {
		gateAllChanges.Status = gitopsv1alpha1.GateStatusUnknown
		gateAllChanges.Reason = "NoImpactedAppsConfigured"
		gateAllChanges.Message = "spec.impactedApps is empty; platform target scope missing"
		issues = append(issues, "spec.impactedApps is empty; validation cannot execute without scope")
	}

	totalClusterStates := 0
	observedAnyClusterOps := false
	observedAnyMCP := false
	observedAnyVirt := false

	isHub := r.isHubCluster(ctx)

	for appName, appStateMap := range chg.Status.AppStates {
		if len(appStateMap.ClusterStates) == 0 {
			if isHub {
				log.Log.Info("Hub management cluster has no workload propagation status; applying hub exclusion rule", "app", appName)
			} else {
				gateAllChanges.Status = gitopsv1alpha1.GateStatusUnknown
				gateAllChanges.Reason = "MissingAppPropagationStatus"
				gateAllChanges.Message = fmt.Sprintf("No cluster propagation status found for app %s", appName)
				issues = append(issues, fmt.Sprintf("%s: missing cluster propagation status", appName))
			}
		}

		for _, cs := range appStateMap.ClusterStates {
			totalClusterStates++

			if len(cs.PlatformObservation.ClusterOperators) > 0 {
				observedAnyClusterOps = true
				for _, co := range cs.PlatformObservation.ClusterOperators {
					if co.Degraded || !co.Available {
						gateClusterOps.Status = gitopsv1alpha1.GateStatusFalse
						gateClusterOps.Reason = "ClusterOperatorDegradedOrUnavailable"
						gateClusterOps.Message = fmt.Sprintf("ClusterOperator %s is degraded=%v, available=%v", co.Name, co.Degraded, co.Available)
						issues = append(issues, fmt.Sprintf("%s/%s: ClusterOperator %s degraded/unavailable", appName, cs.ClusterName, co.Name))
					}
				}
			}

			if cs.State != "InSync" {
				gateAllChanges.Status = gitopsv1alpha1.GateStatusFalse
				gateAllChanges.Reason = "ClusterNotInSync"
				gateAllChanges.Message = fmt.Sprintf("App %s on cluster %s is %s", appName, cs.ClusterName, cs.State)
				issues = append(issues, fmt.Sprintf("%s/%s: state is %s", appName, cs.ClusterName, cs.State))
			}

			if cs.Health != "Healthy" {
				if cs.Health == "" || cs.Health == "Unknown" {
					if gateHealth.Status != gitopsv1alpha1.GateStatusFalse {
						gateHealth.Status = gitopsv1alpha1.GateStatusUnknown
						gateHealth.Reason = "WorkloadHealthUnknown"
						gateHealth.Message = fmt.Sprintf("App %s on cluster %s health is unknown", appName, cs.ClusterName)
					}
				} else {
					gateHealth.Status = gitopsv1alpha1.GateStatusFalse
					gateHealth.Reason = "WorkloadUnhealthy"
					gateHealth.Message = fmt.Sprintf("App %s on cluster %s health is %s", appName, cs.ClusterName, cs.Health)
				}
				issues = append(issues, fmt.Sprintf("%s/%s: health is %s", appName, cs.ClusterName, cs.Health))
			}

			mcp := cs.MCPStatus
			if mcp.Name != "" || mcp.Phase != "" {
				observedAnyMCP = true
				if mcp.Phase == "Degraded" || mcp.DegradedNodeCount > 0 {
					gateMCP.Status = gitopsv1alpha1.GateStatusFalse
					gateMCP.Reason = "MCPDegraded"
					gateMCP.Message = fmt.Sprintf("MachineConfigPool %s has %d degraded nodes", mcp.Name, mcp.DegradedNodeCount)
					issues = append(issues, fmt.Sprintf("%s/%s: MCP %s degraded", appName, cs.ClusterName, mcp.Name))
				} else if mcp.Phase == "Updating" || mcp.UpdatingNodeCount > 0 || mcp.UnavailableNodeCount > 0 {
					gateMCP.Status = gitopsv1alpha1.GateStatusFalse
					gateMCP.Reason = "MCPUpdatingOrUnavailable"
					gateMCP.Message = fmt.Sprintf("MachineConfigPool %s updating=%d, unavailable=%d", mcp.Name, mcp.UpdatingNodeCount, mcp.UnavailableNodeCount)
					issues = append(issues, fmt.Sprintf("%s/%s: MCP %s updating/unavailable", appName, cs.ClusterName, mcp.Name))
				} else if mcp.MachineCount > 0 && mcp.ReadyMachineCount < mcp.MachineCount {
					gateMCP.Status = gitopsv1alpha1.GateStatusFalse
					gateMCP.Reason = "MCPNodesNotReady"
					gateMCP.Message = fmt.Sprintf("MachineConfigPool %s ready count %d < total %d", mcp.Name, mcp.ReadyMachineCount, mcp.MachineCount)
					issues = append(issues, fmt.Sprintf("%s/%s: MCP %s not all nodes ready", appName, cs.ClusterName, mcp.Name))
				} else if mcp.DesiredRenderedConfig != "" && mcp.CurrentRenderedConfig != "" && mcp.DesiredRenderedConfig != mcp.CurrentRenderedConfig {
					gateMCP.Status = gitopsv1alpha1.GateStatusFalse
					gateMCP.Reason = "MCPConfigMismatch"
					gateMCP.Message = fmt.Sprintf("MachineConfigPool %s current config %s does not match desired %s", mcp.Name, mcp.CurrentRenderedConfig, mcp.DesiredRenderedConfig)
					issues = append(issues, fmt.Sprintf("%s/%s: MCP %s config mismatch", appName, cs.ClusterName, mcp.Name))
				}
			}

			windowStart := chg.Spec.StartTime.Time
			for _, ev := range cs.RecentEvents {
				if ev.LastObservedAt.After(windowStart) || ev.LastObservedAt.Time.Equal(windowStart) {
					gateEvents.Status = gitopsv1alpha1.GateStatusFalse
					gateEvents.Reason = "WarningEventsObserved"
					gateEvents.Message = fmt.Sprintf("Warning event %s on %s: %s", ev.Reason, ev.InvolvedObject, ev.Message)
					issues = append(issues, fmt.Sprintf("%s/%s: Warning event %s on %s", appName, cs.ClusterName, ev.Reason, ev.InvolvedObject))
				}
			}

			for _, oc := range cs.ObjectChanges {
				ct := strings.ToLower(oc.ChangeType)
				if ct == "failed" || ct == "syncfailed" || ct == "error" || ct == "syncerror" || strings.HasPrefix(ct, "fail") {
					gateObjects.Status = gitopsv1alpha1.GateStatusFalse
					gateObjects.Reason = "ObjectSyncFailed"
					gateObjects.Message = fmt.Sprintf("Resource %s/%s failed sync (%s)", oc.Kind, oc.Name, oc.ChangeType)
					issues = append(issues, fmt.Sprintf("%s/%s: object %s/%s failed sync", appName, cs.ClusterName, oc.Kind, oc.Name))
				}
			}

			for _, dep := range cs.Dependencies {
				if !dep.Ready {
					gateDeps.Status = gitopsv1alpha1.GateStatusFalse
					gateDeps.Reason = "DependencyNotReady"
					gateDeps.Message = fmt.Sprintf("Dependency %s/%s not ready (%s)", dep.Kind, dep.Name, dep.Note)
					issues = append(issues, fmt.Sprintf("%s/%s: dependency %s/%s not ready", appName, cs.ClusterName, dep.Kind, dep.Name))
				}
			}

			virt := cs.VirtStatus
			if virt.HyperConvergedHealth != "" && virt.HyperConvergedHealth != "Unknown" {
				observedAnyVirt = true
			}
			if virt.HyperConvergedHealth == "Degraded" {
				gateVirt.Status = gitopsv1alpha1.GateStatusFalse
				gateVirt.Reason = "HyperConvergedDegraded"
				gateVirt.Message = "OpenShift Virtualization HyperConverged deployment is degraded"
				issues = append(issues, fmt.Sprintf("%s/%s: HyperConverged degraded", appName, cs.ClusterName))
			}
			if !virt.VirtHandlerReady {
				gateVirt.Status = gitopsv1alpha1.GateStatusFalse
				gateVirt.Reason = "VirtHandlerNotReady"
				gateVirt.Message = "virt-handler DaemonSet is not ready"
				issues = append(issues, fmt.Sprintf("%s/%s: virt-handler unready", appName, cs.ClusterName))
			}
			if virt.StalledMigrations > 0 {
				gateVirt.Status = gitopsv1alpha1.GateStatusFalse
				gateVirt.Reason = "StalledMigrations"
				gateVirt.Message = fmt.Sprintf("%d stalled VMI migrations observed", virt.StalledMigrations)
				issues = append(issues, fmt.Sprintf("%s/%s: %d stalled migrations", appName, cs.ClusterName, virt.StalledMigrations))
			}
			if virt.UnmigratableVMIs > 0 {
				gateVirt.Status = gitopsv1alpha1.GateStatusFalse
				gateVirt.Reason = "UnmigratableVMIs"
				gateVirt.Message = fmt.Sprintf("%d unmigratable VMIs observed on target nodes", virt.UnmigratableVMIs)
				issues = append(issues, fmt.Sprintf("%s/%s: %d unmigratable VMIs", appName, cs.ClusterName, virt.UnmigratableVMIs))
			}

			// Evaluate ClusterVersion gate from PlatformObservation
			cv := cs.PlatformObservation.ClusterVersion
			if cv.Version != "" || cv.DesiredVersion != "" {
				if cv.Progressing {
					gateClusterVersion.Status = gitopsv1alpha1.GateStatusFalse
					gateClusterVersion.Reason = "ClusterVersionProgressing"
					gateClusterVersion.Message = fmt.Sprintf("ClusterVersion is upgrading from %s to %s", cv.Version, cv.DesiredVersion)
					issues = append(issues, fmt.Sprintf("%s/%s: ClusterVersion upgrading", appName, cs.ClusterName))
				} else if cv.Available {
					gateClusterVersion.Status = gitopsv1alpha1.GateStatusTrue
					gateClusterVersion.Reason = "ClusterVersionStable"
					gateClusterVersion.Message = fmt.Sprintf("ClusterVersion %s is stable and available", cv.Version)
				} else {
					gateClusterVersion.Status = gitopsv1alpha1.GateStatusFalse
					gateClusterVersion.Reason = "ClusterVersionNotAvailable"
					gateClusterVersion.Message = "ClusterVersion is not Available"
					issues = append(issues, fmt.Sprintf("%s/%s: ClusterVersion not available", appName, cs.ClusterName))
				}
			}

			// Evaluate PlatformOperatorsDeployed gate
			kv := cs.PlatformObservation.KubeVirt
			cdi := cs.PlatformObservation.CDI
			ssp := cs.PlatformObservation.SSP
			if kv.Phase != "" && kv.Phase != "Unknown" {
				if kv.Ready && cdi.Ready && ssp.Ready {
					gatePlatformOps.Status = gitopsv1alpha1.GateStatusTrue
					gatePlatformOps.Reason = "AllPlatformOperatorsDeployed"
					gatePlatformOps.Message = "KubeVirt, CDI, and SSP operators are all Deployed"
				} else {
					gatePlatformOps.Status = gitopsv1alpha1.GateStatusFalse
					var notReady []string
					if !kv.Ready {
						notReady = append(notReady, fmt.Sprintf("KubeVirt(%s)", kv.Phase))
					}
					if !cdi.Ready {
						notReady = append(notReady, fmt.Sprintf("CDI(%s)", cdi.Phase))
					}
					if !ssp.Ready {
						notReady = append(notReady, fmt.Sprintf("SSP(%s)", ssp.Phase))
					}
					gatePlatformOps.Reason = "PlatformOperatorNotDeployed"
					gatePlatformOps.Message = fmt.Sprintf("Platform operators not deployed: %s", strings.Join(notReady, ", "))
					issues = append(issues, fmt.Sprintf("%s/%s: platform operators not deployed: %s", appName, cs.ClusterName, strings.Join(notReady, ", ")))
				}
			}

			// Evaluate NoActiveNodeMaintenance gate
			nm := cs.PlatformObservation.NodeMaintenance
			if nm.ActiveMaintenanceNodes > 0 {
				gateNodeMaint.Status = gitopsv1alpha1.GateStatusFalse
				gateNodeMaint.Reason = "ActiveNodeMaintenance"
				gateNodeMaint.Message = fmt.Sprintf("%d nodes under active maintenance", nm.ActiveMaintenanceNodes)
				issues = append(issues, fmt.Sprintf("%s/%s: %d nodes under maintenance", appName, cs.ClusterName, nm.ActiveMaintenanceNodes))
			}
		}
	}

	if !observedAnyClusterOps {
		gateClusterOps.Status = gitopsv1alpha1.GateStatusUnknown
		gateClusterOps.Reason = "ClusterOperatorsNotReported"
		gateClusterOps.Message = "OpenShift ClusterOperator telemetry was not observed for any cluster"
	}

	if !observedAnyMCP {
		gateMCP.Status = gitopsv1alpha1.GateStatusUnknown
		gateMCP.Reason = "MCPStatusNotReported"
		gateMCP.Message = "MachineConfigPool telemetry was not observed for any cluster"
	}

	if !observedAnyVirt {
		gateVirt.Status = gitopsv1alpha1.GateStatusUnknown
		gateVirt.Reason = "VirtStatusNotReported"
		gateVirt.Message = "Virtualization telemetry was not observed or HyperConverged status is unknown"
	}

	if totalClusterStates == 0 {
		gateClusterOps.Status = gitopsv1alpha1.GateStatusUnknown
		gateAllChanges.Status = gitopsv1alpha1.GateStatusUnknown
		gateAllChanges.Reason = "NoClusterStatesObserved"
		gateAllChanges.Message = "No per-cluster state reports were observed"
		gateHealth.Status = gitopsv1alpha1.GateStatusUnknown
		gateMCP.Status = gitopsv1alpha1.GateStatusUnknown
		gateEvents.Status = gitopsv1alpha1.GateStatusUnknown
		gateObjects.Status = gitopsv1alpha1.GateStatusUnknown
		gateDeps.Status = gitopsv1alpha1.GateStatusUnknown
		gateVirt.Status = gitopsv1alpha1.GateStatusUnknown
		issues = append(issues, "No cluster reports available to validate change")
	}

	for _, s := range chg.Status.SilentClusters {
		issues = append(issues, fmt.Sprintf("%s/%s: cluster reporting silent (%s)", s.App, s.Cluster, s.State))
	}
	preserveObservedAt := func(gate *gitopsv1alpha1.GateResult) {
		for _, prevGate := range chg.Status.Validation.GateResults {
			if prevGate.Name == gate.Name {
				if prevGate.Status == gate.Status && prevGate.Reason == gate.Reason && prevGate.Message == gate.Message && !prevGate.ObservedAt.IsZero() {
					gate.ObservedAt = prevGate.ObservedAt
				}
				break
			}
		}
	}

	preserveObservedAt(&gateClusterOps)
	preserveObservedAt(&gateAllChanges)
	preserveObservedAt(&gateHealth)
	preserveObservedAt(&gateMCP)
	preserveObservedAt(&gateEvents)
	preserveObservedAt(&gateObjects)
	preserveObservedAt(&gateDeps)
	preserveObservedAt(&gateVirt)
	preserveObservedAt(&gateClusterVersion)
	preserveObservedAt(&gatePlatformOps)
	preserveObservedAt(&gateNodeMaint)

	gateResults := []gitopsv1alpha1.GateResult{
		gateClusterVersion,
		gateClusterOps,
		gatePlatformOps,
		gateMCP,
		gateNodeMaint,
		gateAllChanges,
		gateHealth,
		gateEvents,
		gateObjects,
		gateDeps,
		gateVirt,
	}

	allTrue := true
	for _, g := range gateResults {
		if g.Status != gitopsv1alpha1.GateStatusTrue {
			allTrue = false
			break
		}
	}
	noSilence := len(chg.Status.SilentClusters) == 0

	passed := allTrue && noSilence && len(chg.Spec.ImpactedApps) > 0

	res := gitopsv1alpha1.ValidationResult{
		ClusterOperatorsHealthy: gateClusterOps.Status == gitopsv1alpha1.GateStatusTrue,
		AllChangesApplied:       gateAllChanges.Status == gitopsv1alpha1.GateStatusTrue,
		HealthCheckPassed:       gateHealth.Status == gitopsv1alpha1.GateStatusTrue,
		MCPUpdatedOnTime:        gateMCP.Status == gitopsv1alpha1.GateStatusTrue,
		EventsClean:             gateEvents.Status == gitopsv1alpha1.GateStatusTrue,
		ObjectsConverged:        gateObjects.Status == gitopsv1alpha1.GateStatusTrue,
		DependenciesReady:       gateDeps.Status == gitopsv1alpha1.GateStatusTrue,
		VirtImpactPassed:        gateVirt.Status == gitopsv1alpha1.GateStatusTrue,
		GateResults:             gateResults,
		IssuesFound:             issues,
		Passed:                  passed,
	}

	return res, issues
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

	action.Attempts++
	action.LastAttemptAt = metav1.NewTime(now)
	action.NextEligibleAt = metav1.NewTime(now.Add(waitInterval))
	action.Result = "Parked"

	action.History = append(action.History, gitopsv1alpha1.ActionAttemptHistory{
		Attempt:       action.Attempts,
		Type:          "HardRefresh",
		TriggeredAt:   metav1.NewTime(now),
		TriggerResult: "Parked (Execution disabled in operator config)",
		LogRef:        "", // Empty when Parked; no fake evidence URL generated
		LogSummary:    fmt.Sprintf("Parked hard refresh action evaluated for %s on cluster %s", appName, cs.ClusterName),
		TailLogs:      nil,
	})

	chg.Status.Actions[key] = action
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
		"baseline":       chg.Status.Baseline,
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
		Watches(
			&gitopsv1alpha1.PropagationStatus{},
			handler.EnqueueRequestsFromMapFunc(r.mapPropagationStatusToChangeWindow),
		).
		Complete(r)
}

func (r *ChangeWindowReconciler) isHubCluster(ctx context.Context) bool {
	return DetectClusterRole(ctx, r.Client)
}

// captureBaseline extracts a snapshot of platform state at the start of a maintenance window.
// This baseline enables Baseline → Observed → Evidence → Decision reasoning.
func (r *ChangeWindowReconciler) captureBaseline(chg *gitopsv1alpha1.ChangeWindow, now time.Time) *gitopsv1alpha1.BaselineSnapshot {
	baseline := &gitopsv1alpha1.BaselineSnapshot{
		CapturedAt: metav1.NewTime(now),
	}

	// Extract baseline from the first available cluster state's PlatformObservation
	for _, appStateMap := range chg.Status.AppStates {
		for _, cs := range appStateMap.ClusterStates {
			po := cs.PlatformObservation
			if po.ClusterVersion.Version != "" {
				baseline.ClusterVersion = po.ClusterVersion.Version
			}
			if po.KubeVirt.ObservedVersion != "" {
				baseline.KubeVirtVersion = po.KubeVirt.ObservedVersion
			}
			if po.CDI.ObservedVersion != "" {
				baseline.CDIVersion = po.CDI.ObservedVersion
			}
			if po.SSP.ObservedVersion != "" {
				baseline.SSPVersion = po.SSP.ObservedVersion
			}

			// Compute a digest of ClusterOperator states for drift detection
			if len(po.ClusterOperators) > 0 {
				var coNames []string
				for _, co := range po.ClusterOperators {
					coNames = append(coNames, fmt.Sprintf("%s=%s/%v/%v", co.Name, co.Version, co.Available, co.Degraded))
				}
				sort.Strings(coNames)
				digest := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(coNames, ","))))
				baseline.ClusterOperatorDigest = digest[:16] // truncated for readability
			}

			// Compute MCP hash
			if len(po.MachineConfigPools) > 0 {
				var mcpStates []string
				for _, mcp := range po.MachineConfigPools {
					mcpStates = append(mcpStates, fmt.Sprintf("%s=%s/%s", mcp.Name, mcp.Phase, mcp.CurrentRenderedConfig))
				}
				sort.Strings(mcpStates)
				digest := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(mcpStates, ","))))
				baseline.MachineConfigPoolHash = digest[:16]
			}

			// Take baseline from first cluster with data
			if baseline.ClusterVersion != "" || baseline.KubeVirtVersion != "" {
				return baseline
			}
		}
	}

	return baseline
}
