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
	// In a spoke deployment this is typically the operator's own namespace.
	reportNamespace = "gitops-fleet"

	// maxWarningEvents is the maximum number of de-duplicated Warning events
	// stored per report. Etcd's 1.5 MB object limit is the hard ceiling.
	maxWarningEvents = 20

	// maxTailLines is the maximum number of pod log lines stored per report.
	maxTailLines int64 = 50
)

// LocalAppWatchReconciler runs on a spoke cluster. It watches bare ConfigMaps
// that act as simple app descriptors (one per app), collects local observability
// data, and writes a ClusterAppReport that the hub's PropagationStatusReconciler
// reads.
//
// Design note: rather than taking a hard dependency on ArgoCD or Flux Go modules
// (which would inflate go.mod significantly), this controller uses a plain
// corev1.ConfigMap as a lightweight "app descriptor" CRD substitute. The
// ConfigMap carries labels and annotations that identify the app name, expected
// revision, ArgoCD namespace, and ArgoCD Application name.  In a production
// deployment a separate side-car or the spoke's ArgoCD agent populates these
// ConfigMaps from Application.status fields.
//
// OpenShift Virtualization resources (VirtualMachine, VirtualMachineInstance,
// VirtualMachineInstanceMigration, DataVolume) are accessed via the dynamic
// unstructured client to avoid importing kubevirt.io/api, which brings in a
// very large dependency tree.
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
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
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

	// MCP status is cheap (a single unstructured Get/List).
	mcpPool := cm.Labels["gitops.example.com/mcp-pool"]
	spec.MCPStatus = r.readMachineConfigPool(ctx, mcpPool)

	if err := r.upsertReport(ctx, reportName, spec); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
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

	// Group by (Reason, InvolvedObject) and keep the most severe.
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

	// Sort by count descending and cap to maxWarningEvents.
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

	// Fallback to app.kubernetes.io/name selector if app: appName returned zero pods
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
		// Only tail logs for pods that are not fully Ready.
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
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------
// Object changes (from a ConfigMap annotation written by an ArgoCD post-sync hook)
// -------------------------------------------------------------------------

// objectChangesFromAnnotation reads a JSON annotation that an ArgoCD post-sync
// hook or a side-car writes to the descriptor ConfigMap.  The format matches
// ObjectChangeSummary so no additional parsing is needed in normal operation.
// When the annotation is absent (steady state), an empty slice is returned.
func (r *LocalAppWatchReconciler) objectChangesFromAnnotation(cm *corev1.ConfigMap) []gitopsv1alpha1.ObjectChangeSummary {
	const annotationKey = "gitops.example.com/last-sync-resources"
	raw, ok := cm.Annotations[annotationKey]
	if !ok || raw == "" {
		return nil
	}
	// Parse line format: "Kind/Name=changeType[:field1,field2]"
	var results []gitopsv1alpha1.ObjectChangeSummary
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		kindName := strings.SplitN(parts[0], "/", 2)
		if len(kindName) != 2 {
			continue
		}
		changeParts := strings.SplitN(parts[1], ":", 2)
		changeType := changeParts[0]
		var changedFields []string
		if len(changeParts) > 1 && changeParts[1] != "" {
			changedFields = strings.Split(changeParts[1], ",")
		}
		oc := gitopsv1alpha1.ObjectChangeSummary{
			Kind:          kindName[0],
			Name:          kindName[1],
			ChangeType:    changeType,
			ChangedFields: changedFields,
		}
		results = append(results, oc)
	}
	return results
}

// -------------------------------------------------------------------------
// Dependency checking
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) checkDependencies(ctx context.Context, namespace string, cm *corev1.ConfigMap) []gitopsv1alpha1.DependencyRef {
	const depsAnnotation = "gitops.example.com/dependencies"
	raw, ok := cm.Annotations[depsAnnotation]
	if !ok || raw == "" {
		return nil
	}

	// Format: "Kind/Name\nKind/Name\n…"
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
		case "DataVolume":
			dep.Ready = r.isDataVolumeReady(ctx, namespace, name)
		default:
			dep.Ready = true // Unknown kind; assume ready rather than false-alerting.
			dep.Note = fmt.Sprintf("readiness check not implemented for kind %s", kind)
		}
		results = append(results, dep)
	}
	return results
}

// isDataVolumeReady checks whether a CDI DataVolume's phase is "Succeeded".
// Uses the unstructured client to avoid a CDI Go module dependency.
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
// MachineConfigPool
// -------------------------------------------------------------------------

// readMachineConfigPool reads the MachineConfigPool status for preferredPool,
// "virt", or "worker" pool. Uses unstructured client to avoid importing MCO SDK.
func (r *LocalAppWatchReconciler) readMachineConfigPool(ctx context.Context, preferredPool string) gitopsv1alpha1.MachineConfigPoolStatus {
	poolsToTry := []string{}
	if preferredPool != "" {
		poolsToTry = append(poolsToTry, preferredPool)
	}
	poolsToTry = append(poolsToTry, "virt", "worker")

	mcp := &unstructured.Unstructured{}
	mcp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfigPool",
	})

	var targetPool string
	for _, poolName := range poolsToTry {
		if err := r.Get(ctx, types.NamespacedName{Name: poolName}, mcp); err == nil {
			targetPool = poolName
			break
		}
	}
	if targetPool == "" {
		return gitopsv1alpha1.MachineConfigPoolStatus{}
	}

	machineCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "machineCount")
	updatedCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "updatedMachineCount")
	updatingCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "unavailableMachineCount")
	degradedCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "degradedMachineCount")

	phase := "Updated"
	if updatingCount > 0 {
		phase = "Updating"
	}
	if degradedCount > 0 {
		phase = "Degraded"
	}

	return gitopsv1alpha1.MachineConfigPoolStatus{
		Name:              targetPool,
		MachineCount:      int32(machineCount),
		UpdatedNodeCount:  int32(updatedCount),
		UpdatingNodeCount: int32(updatingCount),
		DegradedNodeCount: int32(degradedCount),
		Phase:             phase,
	}
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

	// Update the existing report's spec with the latest snapshot.
	// Use DeepCopy to avoid mutating the cached object.
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

// -------------------------------------------------------------------------
// Controller setup
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Watch ConfigMaps labelled as app descriptors.
		For(&corev1.ConfigMap{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			return obj.GetLabels()[AppLabelKey] != ""
		})).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 5}).
		Complete(r)
}
