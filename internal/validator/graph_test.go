package validator

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEvaluateCausalChain_Healthy(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	g := NewTypedPlatformGraph()

	err := g.EvaluateCausalChain(context.Background(), client)
	if err != nil {
		t.Fatalf("expected healthy causal chain evaluation, got: %v", err)
	}
}

func TestEvaluateCausalChain_ParentMCPUpdatingBlocksChild(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	mcpWorker := &unstructured.Unstructured{}
	mcpWorker.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfigPool",
	})
	mcpWorker.SetName("worker")
	mcpWorker.Object["status"] = map[string]interface{}{
		"machineCount":         int64(5),
		"readyMachineCount":    int64(3),
		"degradedMachineCount": int64(0),
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mcpWorker).Build()
	g := NewTypedPlatformGraph()

	err := g.EvaluateCausalChain(context.Background(), client)
	if err == nil {
		t.Fatalf("expected causal chain evaluation to fail when MCP worker is updating")
	}
}

func TestTypedPlatformGraph_NodeSemantics(t *testing.T) {
	g := NewTypedPlatformGraph()
	if len(g.Nodes) < 13 {
		t.Fatalf("expected at least 13 semantic nodes in typed platform graph, got %d", len(g.Nodes))
	}

	mcpNode, ok := g.Nodes["mcp/worker"]
	if !ok || mcpNode.SemanticType != NodeTypeMCP {
		t.Fatalf("expected MCP worker node with semantic type %s, got %+v", NodeTypeMCP, mcpNode)
	}

	csiNode, ok := g.Nodes["csi/ceph-csi"]
	if !ok || csiNode.SemanticType != NodeTypeCSIDriver {
		t.Fatalf("expected CSI driver node with semantic type %s, got %+v", NodeTypeCSIDriver, csiNode)
	}

	ovnNode, ok := g.Nodes["ovn/ovn-kubernetes"]
	if !ok || ovnNode.SemanticType != NodeTypeOVNNetwork {
		t.Fatalf("expected OVN node with semantic type %s, got %+v", NodeTypeOVNNetwork, ovnNode)
	}

	vmimNode, ok := g.Nodes["vmim/active-migrations"]
	if !ok || vmimNode.SemanticType != NodeTypeVMIMigration {
		t.Fatalf("expected VMIM node with semantic type %s, got %+v", NodeTypeVMIMigration, vmimNode)
	}
}
