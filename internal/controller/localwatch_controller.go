package controller

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

const (
	// reportNamespace is the namespace where ClusterAppReports are written.
	// In an autonomous peer deployment this is typically the operator's own namespace.
	reportNamespace = "gitops-fleet"

	// maxWarningEvents is the maximum number of de-duplicated Warning events
	// stored per report. Etcd's 1.5 MB object limit is the hard ceiling.
	maxWarningEvents = 20

	// maxTailLines is the maximum number of pod log lines stored per report.
	maxTailLines int64 = 50
)

// LocalAppWatchReconciler runs on an autonomous peer cluster. It watches bare ConfigMaps
// that act as simple app descriptors (one per app), collects local platform telemetry and
// workload observability data, and writes a ClusterAppReport that the local or aggregated
// validation pipeline reads.
//
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=gitops.example.com,resources=clusterappreports,verbs=get;list;watch;create;update;patch
type LocalAppWatchReconciler struct {
	client.Client
	// ClusterName is the stable spoke identifier injected via CLUSTER_NAME env var.
	ClusterName string
	// Clientset enables access to the pod log streaming API (subresource).
	Clientset kubernetes.Interface
}

// AppDescriptor is the minimum data needed from the app descriptor ConfigMap.
type AppDescriptor struct {
	AppName    string
	Namespace  string // the app's workload namespace
	SyncStatus string // Synced | OutOfSync | Unknown — written by a sidecar/agent
	Health     string // Healthy | Progressing | Degraded | Unknown
	Revision   string // git commit SHA currently applied
}

// Reconcile is triggered by ConfigMaps labelled "gitops.example.com/app-descriptor=true"
// in any namespace on the spoke cluster.
func (r *LocalAppWatchReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Read the app descriptor ConfigMap.
	var cm corev1.ConfigMap
	if err := r.Get(ctx, req.NamespacedName, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get ConfigMap %s: %w", req.NamespacedName, err)
	}

	if cm.DeletionTimestamp != nil && !cm.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	desc := parseDescriptor(&cm)
	if desc.AppName == "" {
		logger.V(1).Info("ConfigMap missing gitops.example.com/app label; skipping",
			"configmap", req.NamespacedName)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Build the report name deterministically so every reconcile upserts the same object.
	reportName := fmt.Sprintf("%s-%s", r.ClusterName, desc.AppName)

	spec := gitopsv1alpha1.ClusterAppReportSpec{
		ClusterName:      r.ClusterName,
		AppName:          desc.AppName,
		AppNamespace:     desc.Namespace,
		ObservedRevision: desc.Revision,
		SyncStatus:       desc.SyncStatus,
		Health:           desc.Health,
		ObservedAt:       metav1.Now(),
	}

	// Collect richer diagnostics only when something looks wrong — steady-state
	// reports remain cheap (a single ConfigMap read + one CRD write).
	if spec.SyncStatus == "OutOfSync" || spec.Health == "Degraded" {
		spec.RecentEvents = r.collectWarningEvents(ctx, desc.Namespace)
		spec.TailLogs = r.collectPodLogs(ctx, desc.Namespace, desc.AppName)
		spec.ObjectChanges = r.objectChangesFromAnnotation(&cm)
		spec.Dependencies = r.checkDependencies(ctx, desc.Namespace, &cm)
	}

	// Determine target namespaces for virtualization check
	var targetNSList []string
	if rawNS := cm.Annotations["gitops.example.com/target-namespaces"]; rawNS != "" {
		for _, ns := range strings.Split(rawNS, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				targetNSList = append(targetNSList, ns)
			}
		}
	}
	if len(targetNSList) == 0 && desc.Namespace != "" {
		targetNSList = []string{desc.Namespace}
	}

	// MCP, Virt, ClusterOperators, and PlatformObservation status are collected on each reconcile.
	mcpPool := cm.Labels["gitops.example.com/mcp-pool"]
	spec.MCPStatus = r.readMachineConfigPool(ctx, mcpPool)
	virtHealth, virtWorkloads := r.readVirtualizationStatus(ctx, targetNSList)
	spec.VirtStatus = virtHealth

	clusterOps := r.readClusterOperators(ctx)

	// Platform-first dependency graph: read all platform resources
	allMCPs := r.readAllMachineConfigPools(ctx)
	if len(allMCPs) == 0 {
		allMCPs = []gitopsv1alpha1.MachineConfigPoolStatus{spec.MCPStatus}
	}

	platformObs := gitopsv1alpha1.PlatformObservationStatus{
		ClusterOperators:   clusterOps,
		MachineConfigPools: allMCPs,
		VirtHealth:         virtHealth,
		VirtWorkloads:      virtWorkloads,
		ClusterVersion:     r.readClusterVersion(ctx),
		KubeVirt:           r.readKubeVirtStatus(ctx),
		CDI:                r.readCDIStatus(ctx),
		SSP:                r.readSSPStatus(ctx),
		NodeMaintenance:    r.readNodeMaintenanceStatus(ctx),
		MigrationPolicies:  r.readMigrationPolicies(ctx),
		ObservedAt:         metav1.Now(),
	}
	platformObs.DependencyGraph = EvaluatePlatformDependencyGraph(platformObs)
	platformObs.TopologicalDAG = EvaluateTopologicalDAG(platformObs)
	platformObs.CorrelatedEvidence = r.correlateMultiSignalEvidence(ctx, desc.Namespace, spec.RecentEvents, spec.TailLogs)
	spec.PlatformObservation = platformObs

	if err := r.upsertReport(ctx, reportName, spec); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to upsert report %s: %w", reportName, err)
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// -------------------------------------------------------------------------
// Event collection
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) collectWarningEvents(ctx context.Context, namespace string) []gitopsv1alpha1.EventSummary {
	var eventList corev1.EventList
	if err := r.List(ctx, &eventList, client.InNamespace(namespace), client.Limit(100)); err != nil {
		return nil
	}

	type key struct{ Reason, Object string }
	grouped := make(map[key]*gitopsv1alpha1.EventSummary)

	for i := range eventList.Items {
		ev := &eventList.Items[i]
		if ev.Type != corev1.EventTypeWarning {
			continue
		}
		k := key{
			Reason: ev.Reason,
			Object: fmt.Sprintf("%s/%s", ev.InvolvedObject.Kind, ev.InvolvedObject.Name),
		}
		if existing, ok := grouped[k]; ok {
			if ev.Count > existing.Count {
				existing.Count = ev.Count
				existing.Message = ev.Message
				existing.LastObservedAt = ev.LastTimestamp
			}
		} else {
			grouped[k] = &gitopsv1alpha1.EventSummary{
				Reason:         ev.Reason,
				Message:        ev.Message,
				Count:          ev.Count,
				LastObservedAt: ev.LastTimestamp,
				InvolvedObject: k.Object,
			}
		}
	}

	results := make([]gitopsv1alpha1.EventSummary, 0, len(grouped))
	for _, v := range grouped {
		results = append(results, *v)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Count > results[j].Count
	})
	if len(results) > maxWarningEvents {
		results = results[:maxWarningEvents]
	}
	return results
}

// -------------------------------------------------------------------------
// Pod log tailing
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) collectPodLogs(ctx context.Context, namespace, appName string) []string {
	if r.Clientset == nil {
		return nil
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(namespace),
		client.MatchingLabels{"app": appName},
		client.Limit(100),
	); err != nil {
		return nil
	}

	if len(podList.Items) == 0 {
		_ = r.List(ctx, &podList,
			client.InNamespace(namespace),
			client.MatchingLabels{"app.kubernetes.io/name": appName},
			client.Limit(100),
		)
	}

	var lines []string
	for i := range podList.Items {
		pod := &podList.Items[i]
		if isPodReady(pod) {
			continue
		}
		for _, c := range pod.Spec.Containers {
			tailN := maxTailLines
			req := r.Clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
				Container: c.Name,
				TailLines: &tailN,
			})
			stream, err := req.Stream(ctx)
			if err != nil {
				continue
			}
			scanner := bufio.NewScanner(stream)
			prefix := fmt.Sprintf("[%s/%s] ", pod.Name, c.Name)
			for scanner.Scan() {
				lines = append(lines, prefix+scanner.Text())
				if int64(len(lines)) >= maxTailLines {
					_ = stream.Close()
					return lines
				}
			}
			_ = stream.Close()
		}
	}
	return lines
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------
// Object change extraction
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) objectChangesFromAnnotation(cm *corev1.ConfigMap) []gitopsv1alpha1.ObjectChangeSummary {
	const syncResourcesAnno = "gitops.example.com/last-sync-resources"
	raw, ok := cm.Annotations[syncResourcesAnno]
	if !ok || raw == "" {
		return nil
	}

	var results []gitopsv1alpha1.ObjectChangeSummary
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var kind, name, changeType, fieldsStr string

		if strings.Contains(line, "|") {
			parts := strings.Split(line, "|")
			if len(parts) >= 3 {
				kind, name, changeType = parts[0], parts[1], parts[2]
				if len(parts) >= 4 {
					fieldsStr = parts[3]
				}
			}
		} else if strings.Contains(line, "=") && strings.Contains(line, "/") {
			eqParts := strings.SplitN(line, "=", 2)
			knParts := strings.SplitN(eqParts[0], "/", 2)
			if len(knParts) == 2 {
				kind, name = knParts[0], knParts[1]
			}
			if len(eqParts) == 2 {
				ctParts := strings.SplitN(eqParts[1], ":", 2)
				changeType = ctParts[0]
				if len(ctParts) == 2 {
					fieldsStr = ctParts[1]
				}
			}
		}

		if kind != "" && name != "" {
			oc := gitopsv1alpha1.ObjectChangeSummary{
				Kind:       kind,
				Name:       name,
				ChangeType: changeType,
			}
			if fieldsStr != "" {
				oc.ChangedFields = strings.Split(fieldsStr, ",")
			}
			results = append(results, oc)
		}
	}
	return results
}

// -------------------------------------------------------------------------
// Dependency checking (Fail-closed)
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) checkDependencies(ctx context.Context, namespace string, cm *corev1.ConfigMap) []gitopsv1alpha1.DependencyRef {
	const depsAnnotation = "gitops.example.com/dependencies"
	raw, ok := cm.Annotations[depsAnnotation]
	if !ok || raw == "" {
		return nil
	}

	var results []gitopsv1alpha1.DependencyRef
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), "/", 2)
		if len(parts) != 2 {
			continue
		}
		kind, name := parts[0], parts[1]
		dep := gitopsv1alpha1.DependencyRef{Kind: kind, Name: name}

		switch kind {
		case "ConfigMap":
			var target corev1.ConfigMap
			dep.Ready = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &target) == nil
		case "Secret":
			var target corev1.Secret
			dep.Ready = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &target) == nil
		case "ServiceAccount":
			var target corev1.ServiceAccount
			dep.Ready = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &target) == nil
		case "DataVolume":
			dep.Ready = r.isDataVolumeReady(ctx, namespace, name)
		case "MachineConfig":
			dep.Ready = r.isMachineConfigReady(ctx, name)
		case "StorageClass":
			dep.Ready = r.isStorageClassReady(ctx, name)
		default:
			dep.Ready = false
			dep.Note = fmt.Sprintf("readiness check not implemented for kind %s", kind)
		}
		results = append(results, dep)
	}
	return results
}

func (r *LocalAppWatchReconciler) isMachineConfigReady(ctx context.Context, name string) bool {
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfig",
	})
	return r.Get(ctx, types.NamespacedName{Name: name}, mc) == nil
}

func (r *LocalAppWatchReconciler) isStorageClassReady(ctx context.Context, name string) bool {
	sc := &unstructured.Unstructured{}
	sc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "storage.k8s.io",
		Version: "v1",
		Kind:    "StorageClass",
	})
	return r.Get(ctx, types.NamespacedName{Name: name}, sc) == nil
}

func (r *LocalAppWatchReconciler) isDataVolumeReady(ctx context.Context, namespace, name string) bool {
	dv := &unstructured.Unstructured{}
	dv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cdi.kubevirt.io",
		Version: "v1beta1",
		Kind:    "DataVolume",
	})
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dv); err != nil {
		return false
	}
	phase, _, _ := unstructured.NestedString(dv.Object, "status", "phase")
	return phase == "Succeeded"
}

// -------------------------------------------------------------------------
// ClusterOperators, MachineConfigPool & Virtualization Status
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) readClusterOperators(ctx context.Context) []gitopsv1alpha1.ClusterOperatorStatus {
	var coList unstructured.UnstructuredList
	coList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ClusterOperator",
	})

	if err := r.List(ctx, &coList); err != nil {
		return nil
	}

	var results []gitopsv1alpha1.ClusterOperatorStatus
	for _, co := range coList.Items {
		name := co.GetName()
		status := gitopsv1alpha1.ClusterOperatorStatus{
			Name: name,
		}

		conditions, found, _ := unstructured.NestedSlice(co.Object, "status", "conditions")
		if found {
			for _, condRaw := range conditions {
				cond, ok := condRaw.(map[string]interface{})
				if !ok {
					continue
				}
				cType, _ := cond["type"].(string)
				cStatus, _ := cond["status"].(string)
				switch cType {
				case "Available":
					status.Available = cStatus == "True"
				case "Degraded":
					status.Degraded = cStatus == "True"
				case "Progressing":
					status.Progressing = cStatus == "True"
				}
			}
		}

		versions, found, _ := unstructured.NestedSlice(co.Object, "status", "versions")
		if found && len(versions) > 0 {
			if vMap, ok := versions[0].(map[string]interface{}); ok {
				ver, _ := vMap["version"].(string)
				status.Version = ver
			}
		}

		results = append(results, status)
	}
	return results
}

func (r *LocalAppWatchReconciler) readMachineConfigPool(ctx context.Context, preferredPool string) gitopsv1alpha1.MachineConfigPoolStatus {
	poolName := preferredPool
	if poolName == "" {
		for _, tryName := range []string{"virt-worker", "virt", "worker"} {
			mcp := &unstructured.Unstructured{}
			mcp.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "machineconfiguration.openshift.io",
				Version: "v1",
				Kind:    "MachineConfigPool",
			})
			if err := r.Get(ctx, types.NamespacedName{Name: tryName}, mcp); err == nil {
				poolName = tryName
				break
			}
		}
	}
	if poolName == "" {
		return gitopsv1alpha1.MachineConfigPoolStatus{
			Phase: "Unknown",
		}
	}

	mcp := &unstructured.Unstructured{}
	mcp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfigPool",
	})
	if err := r.Get(ctx, types.NamespacedName{Name: poolName}, mcp); err != nil {
		return gitopsv1alpha1.MachineConfigPoolStatus{
			Name:  poolName,
			Phase: "Unknown",
		}
	}

	machineCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "machineCount")
	readyCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "readyMachineCount")
	updatedCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "updatedMachineCount")
	unavailableCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "unavailableMachineCount")
	degradedCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "degradedMachineCount")
	currentConfig, _, _ := unstructured.NestedString(mcp.Object, "status", "configuration", "name")
	desiredConfig, _, _ := unstructured.NestedString(mcp.Object, "spec", "configuration", "name")

	updatingCount := machineCount - updatedCount
	if updatingCount < 0 {
		updatingCount = 0
	}

	phase := "Updated"
	if updatingCount > 0 || readyCount < machineCount || unavailableCount > 0 {
		phase = "Updating"
	}
	if degradedCount > 0 {
		phase = "Degraded"
	}

	return gitopsv1alpha1.MachineConfigPoolStatus{
		Name:                  poolName,
		MachineCount:          int32(machineCount),
		ReadyMachineCount:     int32(readyCount),
		UpdatedNodeCount:      int32(updatedCount),
		UpdatingNodeCount:     int32(updatingCount),
		UnavailableNodeCount:  int32(unavailableCount),
		DegradedNodeCount:     int32(degradedCount),
		CurrentRenderedConfig: currentConfig,
		DesiredRenderedConfig: desiredConfig,
		Phase:                 phase,
	}
}

func (r *LocalAppWatchReconciler) readVirtualizationStatus(ctx context.Context, targetNSList []string) (gitopsv1alpha1.VirtualizationImpactStatus, gitopsv1alpha1.VirtualizationWorkloadSummary) {
	health := gitopsv1alpha1.VirtualizationImpactStatus{
		HyperConvergedHealth: "Healthy",
		VirtHandlerReady:     true,
	}
	workloads := gitopsv1alpha1.VirtualizationWorkloadSummary{}

	// 1. Check HyperConverged status in openshift-cnv or fallback namespace
	hco := &unstructured.Unstructured{}
	hco.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "hco.kubevirt.io",
		Version: "v1beta1",
		Kind:    "HyperConverged",
	})
	foundHCO := false
	for _, ns := range []string{"openshift-cnv", "kubevirt-hyperconverged"} {
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: "kubevirt-hyperconverged"}, hco); err == nil {
			foundHCO = true
			conditions, found, _ := unstructured.NestedSlice(hco.Object, "status", "conditions")
			if found {
				for _, condRaw := range conditions {
					cond, ok := condRaw.(map[string]interface{})
					if !ok {
						continue
					}
					cType, _ := cond["type"].(string)
					cStatus, _ := cond["status"].(string)
					if cType == "Degraded" && cStatus == "True" {
						health.HyperConvergedHealth = "Degraded"
					}
				}
			}
			break
		}
	}
	if !foundHCO {
		health.HyperConvergedHealth = "Unknown"
	}

	// 2. Check virt-handler DaemonSet readiness
	var dsList unstructured.UnstructuredList
	dsList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "DaemonSet",
	})
	for _, ns := range []string{"openshift-cnv", "kubevirt"} {
		if err := r.List(ctx, &dsList, client.InNamespace(ns), client.MatchingLabels{"kubevirt.io": "virt-handler"}); err == nil && len(dsList.Items) > 0 {
			for _, ds := range dsList.Items {
				desired, _, _ := unstructured.NestedInt64(ds.Object, "status", "desiredNumberScheduled")
				ready, _, _ := unstructured.NestedInt64(ds.Object, "status", "numberReady")
				if desired > 0 && ready < desired {
					health.VirtHandlerReady = false
				}
			}
		}
	}

	if len(targetNSList) == 0 {
		targetNSList = []string{"default"}
	}

	for _, targetNS := range targetNSList {
		if targetNS == "" {
			continue
		}

		// 3. Query VirtualMachineInstanceMigration CRs to count active and stalled migrations
		var vmimList unstructured.UnstructuredList
		vmimList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "kubevirt.io",
			Version: "v1",
			Kind:    "VirtualMachineInstanceMigration",
		})
		if err := r.List(ctx, &vmimList, client.InNamespace(targetNS), client.Limit(100)); err == nil {
			for _, vmim := range vmimList.Items {
				phase, _, _ := unstructured.NestedString(vmim.Object, "status", "phase")
				switch phase {
				case "Scheduling", "Scheduled", "PreparingTarget", "TargetReady", "Running":
					health.ActiveMigrations++
					workloads.ActiveMigrations++
				}

				isStalled := false
				conditions, found, _ := unstructured.NestedSlice(vmim.Object, "status", "conditions")
				if found {
					for _, condRaw := range conditions {
						cond, ok := condRaw.(map[string]interface{})
						if !ok {
							continue
						}
						cType, _ := cond["type"].(string)
						cStatus, _ := cond["status"].(string)
						if cType == "Stalled" && cStatus == "True" {
							isStalled = true
						}
					}
				}
				if isStalled {
					health.StalledMigrations++
					workloads.StalledMigrations++
				}
			}
		}

		// 4. Query VirtualMachineInstance CRs to count unmigratable VMIs
		var vmiList unstructured.UnstructuredList
		vmiList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "kubevirt.io",
			Version: "v1",
			Kind:    "VirtualMachineInstance",
		})
		if err := r.List(ctx, &vmiList, client.InNamespace(targetNS), client.Limit(100)); err == nil {
			workloads.TotalVMIs += int32(len(vmiList.Items))
			for _, vmi := range vmiList.Items {
				phase, _, _ := unstructured.NestedString(vmi.Object, "status", "phase")
				if phase == "Running" {
					workloads.RunningVMIs++
				}
				isLiveMigratable := true
				conditions, found, _ := unstructured.NestedSlice(vmi.Object, "status", "conditions")
				if found {
					for _, condRaw := range conditions {
						cond, ok := condRaw.(map[string]interface{})
						if !ok {
							continue
						}
						cType, _ := cond["type"].(string)
						cStatus, _ := cond["status"].(string)
						if cType == "LiveMigratable" && cStatus == "False" {
							isLiveMigratable = false
							health.UnmigratableVMIs++
							workloads.UnmigratableVMIs++
						}
					}
				}
				if isLiveMigratable {
					workloads.LiveMigratableVMIs++
				}
			}
		}
	}

	return health, workloads
}

// -------------------------------------------------------------------------
// ClusterAppReport upsert
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) upsertReport(ctx context.Context, name string, spec gitopsv1alpha1.ClusterAppReportSpec) error {
	existing := &gitopsv1alpha1.ClusterAppReport{}
	err := r.Get(ctx, types.NamespacedName{Namespace: reportNamespace, Name: name}, existing)
	if apierrors.IsNotFound(err) {
		report := &gitopsv1alpha1.ClusterAppReport{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: reportNamespace,
				Labels: map[string]string{
					AppLabelKey: spec.AppName,
				},
			},
			Spec: spec,
		}
		return r.Create(ctx, report)
	}
	if err != nil {
		return err
	}

	updated := existing.DeepCopy()
	updated.Spec = spec
	return r.Patch(ctx, updated, client.MergeFrom(existing))
}

// -------------------------------------------------------------------------
// App descriptor parsing
// -------------------------------------------------------------------------

func parseDescriptor(cm *corev1.ConfigMap) AppDescriptor {
	desc := AppDescriptor{
		AppName:    cm.Labels[AppLabelKey],
		Namespace:  cm.Namespace,
		SyncStatus: cm.Data["syncStatus"],
		Health:     cm.Data["health"],
		Revision:   cm.Data["observedRevision"],
	}
	return desc
}

func (r *LocalAppWatchReconciler) correlateMultiSignalEvidence(ctx context.Context, namespace string, events []gitopsv1alpha1.EventSummary, logs []string) []gitopsv1alpha1.CorrelatedEvidence {
	var evidence []gitopsv1alpha1.CorrelatedEvidence

	for _, evt := range events {
		sev := gitopsv1alpha1.SeverityWarning
		if strings.Contains(evt.Reason, "Failed") || strings.Contains(evt.Reason, "Stalled") || strings.Contains(evt.Reason, "Error") {
			sev = gitopsv1alpha1.SeverityCritical
		}
		evidence = append(evidence, gitopsv1alpha1.CorrelatedEvidence{
			Timestamp: evt.LastObservedAt,
			Component: evt.InvolvedObject,
			ObjectID:  evt.InvolvedObject,
			EventType: evt.Reason,
			Message:   evt.Message,
			Severity:  sev,
			Source:    "K8sAPI",
		})
	}

	for _, logLine := range logs {
		if strings.Contains(logLine, "qemu monitor socket closed") || strings.Contains(logLine, "attachment failed") || strings.Contains(logLine, "domain-migrated") || strings.Contains(logLine, "ERROR") {
			evidence = append(evidence, gitopsv1alpha1.CorrelatedEvidence{
				Timestamp: metav1.Now(),
				Component: "virt-handler",
				ObjectID:  "pod/virt-handler",
				EventType: "LogSignal",
				Message:   logLine,
				Severity:  gitopsv1alpha1.SeverityCritical,
				Source:    "PodLog",
			})
		}
	}

	return evidence
}

// -------------------------------------------------------------------------
// Controller setup
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.ConfigMap{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			return obj.GetLabels()[AppLabelKey] != ""
		})).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 5}).
		Complete(r)
}
