package controller

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

func TestReadClusterVersion_Progressing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gitopsv1alpha1.AddToScheme(scheme)

	cv := &unstructured.Unstructured{}
	cv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ClusterVersion",
	})
	cv.SetName("version")
	cv.Object["status"] = map[string]interface{}{
		"history": []interface{}{
			map[string]interface{}{
				"version": "4.14.1",
			},
		},
		"desired": map[string]interface{}{
			"version": "4.14.2",
		},
		"conditions": []interface{}{
			map[string]interface{}{"type": "Progressing", "status": "True"},
			map[string]interface{}{"type": "Available", "status": "True"},
		},
	}
	cv.Object["spec"] = map[string]interface{}{
		"channel": "stable-4.14",
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cv).Build()
	r := &LocalAppWatchReconciler{Client: client}

	status := r.readClusterVersion(context.Background())
	if !status.Progressing {
		t.Errorf("expected Progressing to be true")
	}
	if status.Version != "4.14.1" {
		t.Errorf("expected Version 4.14.1, got %s", status.Version)
	}
	if status.DesiredVersion != "4.14.2" {
		t.Errorf("expected DesiredVersion 4.14.2, got %s", status.DesiredVersion)
	}
	if status.Channel != "stable-4.14" {
		t.Errorf("expected Channel stable-4.14, got %s", status.Channel)
	}
}

func TestReadClusterVersion_Missing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &LocalAppWatchReconciler{Client: client}

	status := r.readClusterVersion(context.Background())
	if status.Progressing || status.Available || status.Version != "" {
		t.Errorf("expected empty ClusterVersionStatus for missing resource, got %+v", status)
	}
}

func TestReadKubeVirtStatus_Deployed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	kv := &unstructured.Unstructured{}
	kv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kubevirt.io",
		Version: "v1",
		Kind:    "KubeVirt",
	})
	kv.SetName("kubevirt-hyperconverged")
	kv.SetNamespace("openshift-cnv")
	kv.Object["status"] = map[string]interface{}{
		"phase":                   "Deployed",
		"targetKubeVirtVersion":   "v1.1.0",
		"observedKubeVirtVersion": "v1.1.0",
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(kv).Build()
	r := &LocalAppWatchReconciler{Client: client}

	status := r.readKubeVirtStatus(context.Background())
	if !status.Ready {
		t.Errorf("expected Ready to be true")
	}
	if status.Phase != "Deployed" {
		t.Errorf("expected Phase Deployed, got %s", status.Phase)
	}
}

func TestReadCDIStatus_Deployed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cdi := &unstructured.Unstructured{}
	cdi.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cdi.kubevirt.io",
		Version: "v1beta1",
		Kind:    "CDI",
	})
	cdi.SetName("cdi-kubevirt-hyperconverged")
	cdi.SetNamespace("openshift-cnv")
	cdi.Object["status"] = map[string]interface{}{
		"phase":           "Deployed",
		"targetVersion":   "v1.57.0",
		"observedVersion": "v1.57.0",
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cdi).Build()
	r := &LocalAppWatchReconciler{Client: client}

	status := r.readCDIStatus(context.Background())
	if !status.Ready {
		t.Errorf("expected Ready to be true")
	}
	if status.Phase != "Deployed" {
		t.Errorf("expected Phase Deployed, got %s", status.Phase)
	}
}

func TestReadSSPStatus_Deployed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	ssp := &unstructured.Unstructured{}
	ssp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "ssp.kubevirt.io",
		Version: "v1beta2",
		Kind:    "SSP",
	})
	ssp.SetName("ssp-kubevirt-hyperconverged")
	ssp.SetNamespace("openshift-cnv")
	ssp.Object["status"] = map[string]interface{}{
		"phase":           "Deployed",
		"targetVersion":   "v0.18.0",
		"observedVersion": "v0.18.0",
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ssp).Build()
	r := &LocalAppWatchReconciler{Client: client}

	status := r.readSSPStatus(context.Background())
	if !status.Ready {
		t.Errorf("expected Ready to be true")
	}
	if status.Phase != "Deployed" {
		t.Errorf("expected Phase Deployed, got %s", status.Phase)
	}
}

func TestReadNodeMaintenance_Active(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	nm := &unstructured.Unstructured{}
	nm.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "nodemaintenance.medik8s.io",
		Version: "v1beta1",
		Kind:    "NodeMaintenance",
	})
	nm.SetName("node-01-maint")
	nm.Object["status"] = map[string]interface{}{
		"phase": "Running",
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nm).Build()
	r := &LocalAppWatchReconciler{Client: client}

	status := r.readNodeMaintenanceStatus(context.Background())
	if status.ActiveMaintenanceNodes != 1 {
		t.Errorf("expected 1 active maintenance node, got %d", status.ActiveMaintenanceNodes)
	}
	if len(status.MaintenanceNodeNames) != 1 || status.MaintenanceNodeNames[0] != "node-01-maint" {
		t.Errorf("expected node-01-maint in MaintenanceNodeNames, got %v", status.MaintenanceNodeNames)
	}
}

func TestReadMigrationPolicies(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	mp := &unstructured.Unstructured{}
	mp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "migrations.kubevirt.io",
		Version: "v1alpha1",
		Kind:    "MigrationPolicy",
	})
	mp.SetName("fast-migration")
	mp.Object["spec"] = map[string]interface{}{
		"bandwidthPerMigration": "5Gi",
		"allowAutoConverge":     true,
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mp).Build()
	r := &LocalAppWatchReconciler{Client: client}

	policies := r.readMigrationPolicies(context.Background())
	if len(policies) != 1 {
		t.Fatalf("expected 1 migration policy, got %d", len(policies))
	}
	if policies[0].Name != "fast-migration" || policies[0].BandwidthPerMigration != "5Gi" || !policies[0].AllowAutoConverge {
		t.Errorf("unexpected migration policy content: %+v", policies[0])
	}
}

func TestReadAllMachineConfigPools(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	mcp1 := &unstructured.Unstructured{}
	mcp1.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfigPool",
	})
	mcp1.SetName("worker")
	mcp1.Object["status"] = map[string]interface{}{
		"machineCount":          int64(5),
		"readyMachineCount":     int64(5),
		"updatedMachineCount":   int64(5),
		"unavailableMachineCount": int64(0),
		"degradedMachineCount":  int64(0),
	}

	mcp2 := &unstructured.Unstructured{}
	mcp2.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfigPool",
	})
	mcp2.SetName("virt-worker")
	mcp2.Object["status"] = map[string]interface{}{
		"machineCount":          int64(3),
		"readyMachineCount":     int64(2),
		"updatedMachineCount":   int64(2),
		"unavailableMachineCount": int64(1),
		"degradedMachineCount":  int64(0),
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mcp1, mcp2).Build()
	r := &LocalAppWatchReconciler{Client: client}

	pools := r.readAllMachineConfigPools(context.Background())
	if len(pools) != 2 {
		t.Fatalf("expected 2 pools, got %d", len(pools))
	}
	if pools[0].Name != "virt-worker" || pools[0].Phase != "Updating" {
		t.Errorf("expected virt-worker pool phase Updating, got name=%s phase=%s", pools[0].Name, pools[0].Phase)
	}
	if pools[1].Name != "worker" || pools[1].Phase != "Updated" {
		t.Errorf("expected worker pool phase Updated, got name=%s phase=%s", pools[1].Name, pools[1].Phase)
	}
}

func TestMachineConfigAndStorageClassDependencies(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfig",
	})
	mc.SetName("99-virt-tuning")

	sc := &unstructured.Unstructured{}
	sc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "storage.k8s.io",
		Version: "v1",
		Kind:    "StorageClass",
	})
	sc.SetName("ocs-storagecluster-ceph-rbd")

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mc, sc).Build()
	r := &LocalAppWatchReconciler{Client: client}

	ctx := context.Background()
	if !r.isMachineConfigReady(ctx, "99-virt-tuning") {
		t.Errorf("expected MachineConfig to be ready")
	}
	if r.isMachineConfigReady(ctx, "nonexistent-mc") {
		t.Errorf("expected nonexistent MachineConfig to be unready")
	}

	if !r.isStorageClassReady(ctx, "ocs-storagecluster-ceph-rbd") {
		t.Errorf("expected StorageClass to be ready")
	}
	if r.isStorageClassReady(ctx, "nonexistent-sc") {
		t.Errorf("expected nonexistent StorageClass to be unready")
	}
}

func TestEvaluatePlatformDependencyGraph_UpstreamRootCause(t *testing.T) {
	obs := gitopsv1alpha1.PlatformObservationStatus{
		ClusterVersion: gitopsv1alpha1.ClusterVersionStatus{
			Version:        "4.14.0",
			DesiredVersion: "4.14.2",
			Progressing:    true,
			Available:      true,
		},
		MachineConfigPools: []gitopsv1alpha1.MachineConfigPoolStatus{
			{Name: "worker", Phase: "Updating"},
		},
		VirtHealth: gitopsv1alpha1.VirtualizationImpactStatus{
			StalledMigrations: 2,
		},
	}

	graph := EvaluatePlatformDependencyGraph(obs)
	if graph.Healthy {
		t.Errorf("expected graph to be unhealthy due to progressing ClusterVersion")
	}
	if graph.RootCauseResource != "ClusterVersion/version" {
		t.Errorf("expected root cause ClusterVersion/version, got %s", graph.RootCauseResource)
	}
	if len(graph.Nodes) != 6 {
		t.Fatalf("expected 6 causal stages, got %d", len(graph.Nodes))
	}
	if graph.Nodes[1].ImpactedBy != "ClusterVersion/version" {
		t.Errorf("expected stage 2 NodeConfig to be impacted by ClusterVersion/version, got %s", graph.Nodes[1].ImpactedBy)
	}
}

func TestEvaluatePlatformDependencyGraph_MCPRootCause(t *testing.T) {
	obs := gitopsv1alpha1.PlatformObservationStatus{
		ClusterVersion: gitopsv1alpha1.ClusterVersionStatus{
			Version:     "4.14.0",
			Progressing: false,
			Available:   true,
		},
		MachineConfigPools: []gitopsv1alpha1.MachineConfigPoolStatus{
			{Name: "virt-worker", Phase: "Updating"},
		},
		VirtHealth: gitopsv1alpha1.VirtualizationImpactStatus{
			StalledMigrations: 1,
		},
	}

	graph := EvaluatePlatformDependencyGraph(obs)
	if graph.Healthy {
		t.Errorf("expected graph to be unhealthy due to updating MCP")
	}
	if graph.RootCauseResource != "MachineConfigPool/virt-worker" {
		t.Errorf("expected root cause MachineConfigPool/virt-worker, got %s", graph.RootCauseResource)
	}
}

func TestEvaluateTopologicalDAG_ParentFailureHaltsChildren(t *testing.T) {
	obs := gitopsv1alpha1.PlatformObservationStatus{
		MachineConfigPools: []gitopsv1alpha1.MachineConfigPoolStatus{
			{Name: "worker", Phase: "Updating"},
		},
		VirtHealth: gitopsv1alpha1.VirtualizationImpactStatus{
			VirtHandlerReady:  false,
			StalledMigrations: 1,
		},
	}

	dag := EvaluateTopologicalDAG(obs)
	if !dag.Evaluated {
		t.Fatalf("expected DAG to be evaluated")
	}
	if dag.Healthy {
		t.Errorf("expected DAG to be unhealthy when parent MCP is updating")
	}
	if len(dag.Nodes) != 4 {
		t.Fatalf("expected 4 nodes in topological DAG, got %d", len(dag.Nodes))
	}
	if dag.Nodes[1].State != "Blocked" || dag.Nodes[1].BlockedBy != "mcp/worker" {
		t.Errorf("expected mcd/worker to be blocked by mcp/worker, got %+v", dag.Nodes[1])
	}
	if dag.Nodes[2].State != "Blocked" || dag.Nodes[2].BlockedBy != "mcd/worker" {
		t.Errorf("expected virt-handler to be blocked by mcd/worker, got %+v", dag.Nodes[2])
	}
}

func TestPredictMCPConvergence_VelocityAndStall(t *testing.T) {
	mcpUpdating := gitopsv1alpha1.MachineConfigPoolStatus{
		Name:               "worker",
		MachineCount:       10,
		UpdatedNodeCount:   5,
		UpdatingNodeCount:  5,
		DegradedNodeCount:  0,
		Phase:              "Updating",
	}

	pred := PredictMCPConvergence(mcpUpdating, 100) // 5 nodes updated in 100s -> 0.05 nodes/sec, ETA 100s for 5 nodes
	if pred.IsStalled {
		t.Errorf("expected pool rollout to be active, not stalled")
	}
	if pred.EstimatedETASeconds != 100 {
		t.Errorf("expected ETA 100s, got %d", pred.EstimatedETASeconds)
	}

	mcpDegraded := gitopsv1alpha1.MachineConfigPoolStatus{
		Name:               "worker",
		MachineCount:       10,
		UpdatedNodeCount:   3,
		UpdatingNodeCount:  7,
		DegradedNodeCount:  1,
		Phase:              "Degraded",
	}

	predDegraded := PredictMCPConvergence(mcpDegraded, 120)
	if !predDegraded.IsStalled {
		t.Errorf("expected degraded pool to be marked as stalled")
	}
}

func TestEvaluateClusterOperatorOrdering_UpstreamFailureImpactsDownstream(t *testing.T) {
	operators := []gitopsv1alpha1.ClusterOperatorStatus{
		{Name: "network", Available: true, Degraded: true},
		{Name: "kube-apiserver", Available: true, Degraded: false},
		{Name: "kubevirt", Available: true, Degraded: false},
	}

	res := EvaluateClusterOperatorOrdering(operators)
	if res.OrderedCheckPassed {
		t.Fatalf("expected ordering check to fail when network operator is degraded")
	}
	if res.FailedOperator != "network" {
		t.Errorf("expected failed operator to be network, got %s", res.FailedOperator)
	}
	if len(res.ImpactedOperators) < 2 {
		t.Errorf("expected downstream operators to be flagged as impacted, got %v", res.ImpactedOperators)
	}
}



