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
	"sigs.k8s.io/controller-runtime/pkg/log"

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
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines;virtualmachineinstances;virtualmachineinstancemigrations,verbs=get;list;watch
// +kubebuilder:rbac:groups=cdi.kubevirt.io,resources=datavolumes,verbs=get;list;watch
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
	IsVirtApp  bool   // true when this app manages VirtualMachine workloads
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

	if desc.IsVirtApp {
		spec.VMStatus = r.checkVMHealth(ctx, desc.Namespace, desc.AppName)
	}

	// MCP status is always cheap (a single unstructured Get).
	spec.MCPStatus = r.readMachineConfigPool(ctx)

	return ctrl.Result{RequeueAfter: 30 * time.Second}, r.upsertReport(ctx, reportName, spec)
}

// -------------------------------------------------------------------------
// Event collection
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) collectWarningEvents(ctx context.Context, namespace string) []gitopsv1alpha1.EventSummary {
	var eventList corev1.EventList
	if err := r.List(ctx, &eventList, client.InNamespace(namespace)); err != nil {
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
	); err != nil {
		return nil
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
	// Parse a simple line format: "Kind/Name=changeType[:field1,field2]"
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
		oc := gitopsv1alpha1.ObjectChangeSummary{
			Kind:       kindName[0],
			Name:       kindName[1],
			ChangeType: parts[1],
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
// OpenShift Virtualization health checks
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) checkVMHealth(ctx context.Context, namespace, appName string) []gitopsv1alpha1.VMHealthStatus {
	// List VirtualMachines in the namespace that belong to this app.
	// Uses the unstructured client — no kubevirt.io/api dependency.
	vmList := &unstructured.UnstructuredList{}
	vmList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kubevirt.io",
		Version: "v1",
		Kind:    "VirtualMachineList",
	})
	if err := r.List(ctx, vmList, client.InNamespace(namespace),
		client.MatchingLabels{"app": appName}); err != nil {
		return nil
	}

	var results []gitopsv1alpha1.VMHealthStatus
	for _, vmObj := range vmList.Items {
		vmName := vmObj.GetName()
		status := gitopsv1alpha1.VMHealthStatus{Name: vmName}

		// VirtualMachineInstance (may not exist if VM is stopped).
		vmi := &unstructured.Unstructured{}
		vmi.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "kubevirt.io",
			Version: "v1",
			Kind:    "VirtualMachineInstance",
		})
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: vmName}, vmi); err == nil {
			status.Ready = hasUnstructuredCondition(vmi, "Ready", "True")
			status.LiveMigratable = hasUnstructuredCondition(vmi, "LiveMigratable", "True")
		}

		// VM-level RestartRequired condition — true when a spec change is pending VMI restart.
		status.RestartRequired = hasUnstructuredCondition(&vmObj, "RestartRequired", "True")

		// DataVolume binding: check all volumes that reference a DataVolume.
		status.DataVolumesBound = r.allDataVolumesBound(ctx, namespace, &vmObj)

		// VirtualMachineSnapshot readiness check.
		status.SnapshotReady = r.isVMSnapshotReady(ctx, namespace, vmName)

		// In-flight live migration for this VMI.
		status.ActiveMigration = r.findActiveMigration(ctx, namespace, vmName)

		results = append(results, status)
	}
	return results
}

// isVMSnapshotReady checks whether any VirtualMachineSnapshot referencing this VM is readyToUse.
func (r *LocalAppWatchReconciler) isVMSnapshotReady(ctx context.Context, namespace, vmName string) bool {
	snapList := &unstructured.UnstructuredList{}
	snapList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "snapshot.kubevirt.io",
		Version: "v1alpha1",
		Kind:    "VirtualMachineSnapshotList",
	})
	if err := r.List(ctx, snapList, client.InNamespace(namespace)); err != nil {
		return true // Optional check; assume true if CRD is not present
	}
	for _, snap := range snapList.Items {
		specVM, _, _ := unstructured.NestedString(snap.Object, "spec", "source", "name")
		if specVM == vmName {
			readyToUse, _, _ := unstructured.NestedBool(snap.Object, "status", "readyToUse")
			return readyToUse
		}
	}
	return true
}

// hasUnstructuredCondition returns true when the object's status.conditions array
// contains a condition of the given type with the given status string.
func hasUnstructuredCondition(obj *unstructured.Unstructured, condType, condStatus string) bool {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, raw := range conditions {
		c, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if c["type"] == condType && c["status"] == condStatus {
			return true
		}
	}
	return false
}

// allDataVolumesBound checks that every DataVolume referenced by the VM's disk
// volumes is in the "Succeeded" phase.
func (r *LocalAppWatchReconciler) allDataVolumesBound(ctx context.Context, namespace string, vm *unstructured.Unstructured) bool {
	volumes, _, _ := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "volumes")
	for _, rawVol := range volumes {
		vol, ok := rawVol.(map[string]interface{})
		if !ok {
			continue
		}
		dvMap, _, _ := unstructured.NestedMap(vol, "dataVolume")
		if len(dvMap) == 0 {
			continue
		}
		dvName, _ := dvMap["name"].(string)
		if dvName == "" {
			continue
		}
		if !r.isDataVolumeReady(ctx, namespace, dvName) {
			return false
		}
	}
	return true
}

// findActiveMigration returns the name of any in-flight VMIM for the given VMI, or "".
func (r *LocalAppWatchReconciler) findActiveMigration(ctx context.Context, namespace, vmiName string) string {
	migList := &unstructured.UnstructuredList{}
	migList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kubevirt.io",
		Version: "v1",
		Kind:    "VirtualMachineInstanceMigrationList",
	})
	if err := r.List(ctx, migList, client.InNamespace(namespace)); err != nil {
		return ""
	}
	for _, mig := range migList.Items {
		specVMI, _, _ := unstructured.NestedString(mig.Object, "spec", "vmiName")
		if specVMI != vmiName {
			continue
		}
		phase, _, _ := unstructured.NestedString(mig.Object, "status", "phase")
		if phase != "Succeeded" && phase != "Failed" {
			return mig.GetName()
		}
	}
	return ""
}

// -------------------------------------------------------------------------
// MachineConfigPool
// -------------------------------------------------------------------------

// readMachineConfigPool reads the MachineConfigPool status for the worker pool
// (or a pool labelled gitops.example.com/mcp=true). Uses the unstructured
// client to avoid importing OpenShift's MCO SDK.
func (r *LocalAppWatchReconciler) readMachineConfigPool(ctx context.Context) gitopsv1alpha1.MachineConfigPoolStatus {
	mcp := &unstructured.Unstructured{}
	mcp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfigPool",
	})
	if err := r.Get(ctx, types.NamespacedName{Name: "worker"}, mcp); err != nil {
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
		Name:              "worker",
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
	return r.Update(ctx, updated)
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
	desc.IsVirtApp = cm.Labels["gitops.example.com/virt-app"] == "true"
	return desc
}

// -------------------------------------------------------------------------
// Controller setup
// -------------------------------------------------------------------------

func (r *LocalAppWatchReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Watch ConfigMaps labelled as app descriptors.
		For(&corev1.ConfigMap{}).
		Complete(r)
}
