package controller

import (
	"context"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

// -------------------------------------------------------------------------
// Platform-First Dependency Graph Validators
//
// Each function reads a specific OpenShift platform resource and returns a
// typed status struct. All reads go through the controller-runtime cache
// (r.Get / r.List) and gracefully degrade to "Unknown" or empty values
// when the target CRD does not exist on the cluster.
//
// Dependency chain validated:
//   MachineConfig → RenderedConfig → MachineConfigPool → Node →
//   MachineConfigDaemon → CRI-O → Kubelet → virt-handler → KubeVirt →
//   VMI scheduling → Storage → Network
// -------------------------------------------------------------------------

// +kubebuilder:rbac:groups=config.openshift.io,resources=clusterversions,verbs=get;list;watch
// +kubebuilder:rbac:groups=kubevirt.io,resources=kubevirts,verbs=get;list;watch
// +kubebuilder:rbac:groups=cdi.kubevirt.io,resources=cdis,verbs=get;list;watch
// +kubebuilder:rbac:groups=ssp.kubevirt.io,resources=ssps,verbs=get;list;watch
// +kubebuilder:rbac:groups=nodemaintenance.medik8s.io,resources=nodemaintenances,verbs=get;list;watch
// +kubebuilder:rbac:groups=migrations.kubevirt.io,resources=migrationpolicies,verbs=get;list;watch

// readClusterVersion inspects the singleton ClusterVersion "version" resource to determine
// if the OpenShift cluster is mid-upgrade or stable.
func (r *LocalAppWatchReconciler) readClusterVersion(ctx context.Context) gitopsv1alpha1.ClusterVersionStatus {
	cv := &unstructured.Unstructured{}
	cv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ClusterVersion",
	})

	if err := r.Get(ctx, types.NamespacedName{Name: "version"}, cv); err != nil {
		return gitopsv1alpha1.ClusterVersionStatus{}
	}

	result := gitopsv1alpha1.ClusterVersionStatus{}

	// Extract current version from status.history[0].version
	history, found, _ := unstructured.NestedSlice(cv.Object, "status", "history")
	if found && len(history) > 0 {
		if entry, ok := history[0].(map[string]interface{}); ok {
			result.Version, _ = entry["version"].(string)
		}
	}

	// Extract desired version from status.desired.version
	result.DesiredVersion, _, _ = unstructured.NestedString(cv.Object, "status", "desired", "version")

	// Extract channel from spec.channel
	result.Channel, _, _ = unstructured.NestedString(cv.Object, "spec", "channel")

	// Extract conditions
	conditions, found, _ := unstructured.NestedSlice(cv.Object, "status", "conditions")
	if found {
		for _, condRaw := range conditions {
			cond, ok := condRaw.(map[string]interface{})
			if !ok {
				continue
			}
			cType, _ := cond["type"].(string)
			cStatus, _ := cond["status"].(string)
			switch cType {
			case "Progressing":
				result.Progressing = cStatus == "True"
			case "Available":
				result.Available = cStatus == "True"
			}
		}
	}

	return result
}

// readKubeVirtStatus inspects the KubeVirt CR to determine operator readiness.
func (r *LocalAppWatchReconciler) readKubeVirtStatus(ctx context.Context) gitopsv1alpha1.KubeVirtOperatorStatus {
	result := gitopsv1alpha1.KubeVirtOperatorStatus{}

	// KubeVirt CR is typically in openshift-cnv or kubevirt namespace
	kv := &unstructured.Unstructured{}
	kv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kubevirt.io",
		Version: "v1",
		Kind:    "KubeVirt",
	})

	var found bool
	for _, ns := range []string{"openshift-cnv", "kubevirt"} {
		var kvList unstructured.UnstructuredList
		kvList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "kubevirt.io",
			Version: "v1",
			Kind:    "KubeVirt",
		})
		if err := r.List(ctx, &kvList, client.InNamespace(ns), client.Limit(1)); err == nil && len(kvList.Items) > 0 {
			kv = &kvList.Items[0]
			found = true
			break
		}
	}

	if !found {
		result.Phase = "Unknown"
		return result
	}

	result.Phase, _, _ = unstructured.NestedString(kv.Object, "status", "phase")
	result.TargetVersion, _, _ = unstructured.NestedString(kv.Object, "status", "targetKubeVirtVersion")
	result.ObservedVersion, _, _ = unstructured.NestedString(kv.Object, "status", "observedKubeVirtVersion")
	result.Ready = result.Phase == "Deployed"

	return result
}

// readCDIStatus inspects the CDI CR to determine operator readiness.
func (r *LocalAppWatchReconciler) readCDIStatus(ctx context.Context) gitopsv1alpha1.CDIOperatorStatus {
	result := gitopsv1alpha1.CDIOperatorStatus{}

	cdi := &unstructured.Unstructured{}
	cdi.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cdi.kubevirt.io",
		Version: "v1beta1",
		Kind:    "CDI",
	})

	var found bool
	for _, ns := range []string{"openshift-cnv", "cdi"} {
		var cdiList unstructured.UnstructuredList
		cdiList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "cdi.kubevirt.io",
			Version: "v1beta1",
			Kind:    "CDI",
		})
		if err := r.List(ctx, &cdiList, client.InNamespace(ns), client.Limit(1)); err == nil && len(cdiList.Items) > 0 {
			cdi = &cdiList.Items[0]
			found = true
			break
		}
	}

	if !found {
		result.Phase = "Unknown"
		return result
	}

	result.Phase, _, _ = unstructured.NestedString(cdi.Object, "status", "phase")
	result.TargetVersion, _, _ = unstructured.NestedString(cdi.Object, "status", "targetVersion")
	result.ObservedVersion, _, _ = unstructured.NestedString(cdi.Object, "status", "observedVersion")
	result.Ready = result.Phase == "Deployed"

	return result
}

// readSSPStatus inspects the SSP CR to determine operator readiness.
func (r *LocalAppWatchReconciler) readSSPStatus(ctx context.Context) gitopsv1alpha1.SSPOperatorStatus {
	result := gitopsv1alpha1.SSPOperatorStatus{}

	var found bool
	for _, ns := range []string{"openshift-cnv", "kubevirt-ssp"} {
		var sspList unstructured.UnstructuredList
		sspList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "ssp.kubevirt.io",
			Version: "v1beta2",
			Kind:    "SSP",
		})
		if err := r.List(ctx, &sspList, client.InNamespace(ns), client.Limit(1)); err == nil && len(sspList.Items) > 0 {
			ssp := &sspList.Items[0]
			result.Phase, _, _ = unstructured.NestedString(ssp.Object, "status", "phase")
			result.TargetVersion, _, _ = unstructured.NestedString(ssp.Object, "status", "targetVersion")
			result.ObservedVersion, _, _ = unstructured.NestedString(ssp.Object, "status", "observedVersion")
			result.Ready = result.Phase == "Deployed"
			found = true
			break
		}
	}

	if !found {
		result.Phase = "Unknown"
	}

	return result
}

// readNodeMaintenanceStatus counts active NodeMaintenance objects that indicate
// nodes are being drained for maintenance, which blocks VMI scheduling.
func (r *LocalAppWatchReconciler) readNodeMaintenanceStatus(ctx context.Context) gitopsv1alpha1.NodeMaintenanceStatus {
	result := gitopsv1alpha1.NodeMaintenanceStatus{}

	var nmList unstructured.UnstructuredList
	nmList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "nodemaintenance.medik8s.io",
		Version: "v1beta1",
		Kind:    "NodeMaintenance",
	})

	if err := r.List(ctx, &nmList, client.Limit(100)); err != nil {
		return result
	}

	for _, nm := range nmList.Items {
		phase, _, _ := unstructured.NestedString(nm.Object, "status", "phase")
		// Active maintenance is indicated by phase Running or empty (just created)
		if phase == "Running" || phase == "" {
			result.ActiveMaintenanceNodes++
			result.MaintenanceNodeNames = append(result.MaintenanceNodeNames, nm.GetName())
		}
	}

	return result
}

// readMigrationPolicies reads all MigrationPolicy objects to report bandwidth limits
// and auto-convergence configuration affecting live migrations during maintenance.
func (r *LocalAppWatchReconciler) readMigrationPolicies(ctx context.Context) []gitopsv1alpha1.MigrationPolicyStatus {
	var mpList unstructured.UnstructuredList
	mpList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "migrations.kubevirt.io",
		Version: "v1alpha1",
		Kind:    "MigrationPolicy",
	})

	if err := r.List(ctx, &mpList, client.Limit(100)); err != nil {
		return nil
	}

	var results []gitopsv1alpha1.MigrationPolicyStatus
	for _, mp := range mpList.Items {
		status := gitopsv1alpha1.MigrationPolicyStatus{
			Name: mp.GetName(),
		}

		bandwidth, _, _ := unstructured.NestedString(mp.Object, "spec", "bandwidthPerMigration")
		status.BandwidthPerMigration = bandwidth

		autoConverge, _, _ := unstructured.NestedBool(mp.Object, "spec", "allowAutoConverge")
		status.AllowAutoConverge = autoConverge

		results = append(results, status)
	}

	return results
}

// readAllMachineConfigPools reads ALL MachineConfigPool objects instead of just one.
// This provides complete visibility into worker, master, and virt-worker pool states.
func (r *LocalAppWatchReconciler) readAllMachineConfigPools(ctx context.Context) []gitopsv1alpha1.MachineConfigPoolStatus {
	var mcpList unstructured.UnstructuredList
	mcpList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfigPool",
	})

	if err := r.List(ctx, &mcpList, client.Limit(100)); err != nil {
		return nil
	}

	var results []gitopsv1alpha1.MachineConfigPoolStatus
	for _, mcp := range mcpList.Items {
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

		results = append(results, gitopsv1alpha1.MachineConfigPoolStatus{
			Name:                  mcp.GetName(),
			MachineCount:          int32(machineCount),
			ReadyMachineCount:     int32(readyCount),
			UpdatedNodeCount:      int32(updatedCount),
			UpdatingNodeCount:     int32(updatingCount),
			UnavailableNodeCount:  int32(unavailableCount),
			DegradedNodeCount:     int32(degradedCount),
			CurrentRenderedConfig: currentConfig,
			DesiredRenderedConfig: desiredConfig,
			Phase:                 phase,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})

	return results
}
