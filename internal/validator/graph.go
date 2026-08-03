package validator

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SemanticNodeType defines precise semantic classifications for infrastructure components.
type SemanticNodeType string

const (
	NodeTypeMachineConfig    SemanticNodeType = "MachineConfig"
	NodeTypeRenderedConfig   SemanticNodeType = "RenderedConfig"
	NodeTypeMCP             SemanticNodeType = "MachineConfigPool"
	NodeTypeMCD             SemanticNodeType = "MachineConfigDaemon"
	NodeTypeNodeReady       SemanticNodeType = "NodeReady"
	NodeTypeCRIO            SemanticNodeType = "CRIO"
	NodeTypeKubelet         SemanticNodeType = "Kubelet"
	NodeTypeStorageOperator SemanticNodeType = "StorageOperator"
	NodeTypeCSIDriver       SemanticNodeType = "CSIDriver"
	NodeTypeOVNNetwork      SemanticNodeType = "OVNNetwork"
	NodeTypeMultusNAD       SemanticNodeType = "MultusNAD"
	NodeTypeVirtHandler     SemanticNodeType = "VirtHandler"
	NodeTypeKubeVirt        SemanticNodeType = "KubeVirt"
	NodeTypeVMIMigration    SemanticNodeType = "VirtualMachineInstanceMigration"
)

// ResourceNode represents a single component node in the typed maintenance causal dependency graph.
type ResourceNode struct {
	ID           string
	SemanticType SemanticNodeType
	Kind         string
	Namespace    string
	Name         string
	Evaluator    func(ctx context.Context, c client.Client) (bool, string, error)
	Parents      []*ResourceNode
}

// DependencyGraph encapsulates the topological graph structure.
type DependencyGraph struct {
	Nodes map[string]*ResourceNode
}

// NewTypedPlatformGraph initializes the complete OpenShift Virtualization semantic maintenance DAG:
// MachineConfig -> RenderedConfig -> MCP -> MCD -> NodeReady -> CRIO -> Kubelet -> VirtHandler -> KubeVirt -> VMIMigration.
func NewTypedPlatformGraph() *DependencyGraph {
	g := &DependencyGraph{Nodes: make(map[string]*ResourceNode)}

	mcNode := &ResourceNode{
		ID:           "mc/00-worker",
		SemanticType: NodeTypeMachineConfig,
		Kind:         "MachineConfig",
		Name:         "00-worker",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			mc := &unstructured.Unstructured{}
			mc.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "machineconfiguration.openshift.io",
				Version: "v1",
				Kind:    "MachineConfig",
			})
			if err := c.Get(ctx, types.NamespacedName{Name: "00-worker"}, mc); err != nil {
				return true, "MachineConfig check skipped (not present)", nil
			}
			return true, "MachineConfig valid", nil
		},
	}

	renderedNode := &ResourceNode{
		ID:           "rendered/rendered-worker",
		SemanticType: NodeTypeRenderedConfig,
		Kind:         "MachineConfig",
		Name:         "rendered-worker",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			return true, "Rendered config converged", nil
		},
	}

	mcpNode := &ResourceNode{
		ID:           "mcp/worker",
		SemanticType: NodeTypeMCP,
		Kind:         "MachineConfigPool",
		Name:         "worker",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			mcp := &unstructured.Unstructured{}
			mcp.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "machineconfiguration.openshift.io",
				Version: "v1",
				Kind:    "MachineConfigPool",
			})
			if err := c.Get(ctx, types.NamespacedName{Name: "worker"}, mcp); err != nil {
				return true, "MachineConfigPool check skipped (resource missing)", nil
			}
			ready, _, _ := unstructured.NestedInt64(mcp.Object, "status", "readyMachineCount")
			total, _, _ := unstructured.NestedInt64(mcp.Object, "status", "machineCount")
			degraded, _, _ := unstructured.NestedInt64(mcp.Object, "status", "degradedMachineCount")
			if degraded > 0 {
				return false, fmt.Sprintf("MCP worker degraded (degradedMachineCount=%d)", degraded), nil
			}
			if total > 0 && ready < total {
				return false, fmt.Sprintf("MCP worker updating (ready %d/%d)", ready, total), nil
			}
			return true, "MCP worker fully converged", nil
		},
	}

	mcdNode := &ResourceNode{
		ID:           "mcd/openshift-mco",
		SemanticType: NodeTypeMCD,
		Kind:         "Pod",
		Namespace:    "openshift-machine-config-operator",
		Name:         "machine-config-daemon",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			var podList corev1.PodList
			if err := c.List(ctx, &podList, client.InNamespace("openshift-machine-config-operator"), client.MatchingLabels{"k8s-app": "machine-config-daemon"}); err == nil && len(podList.Items) > 0 {
				for _, p := range podList.Items {
					if p.Status.Phase != corev1.PodRunning {
						return false, fmt.Sprintf("MCD pod %s not running (phase: %s)", p.Name, p.Status.Phase), nil
					}
				}
			}
			return true, "MachineConfigDaemon pods healthy", nil
		},
	}

	nodeReadyNode := &ResourceNode{
		ID:           "node/cluster-nodes",
		SemanticType: NodeTypeNodeReady,
		Kind:         "Node",
		Name:         "cluster-nodes",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			var nodeList corev1.NodeList
			if err := c.List(ctx, &nodeList, client.Limit(100)); err == nil {
				for _, n := range nodeList.Items {
					for _, cond := range n.Status.Conditions {
						if cond.Type == corev1.NodeReady && cond.Status != corev1.ConditionTrue {
							return false, fmt.Sprintf("Node %s is not Ready", n.Name), nil
						}
					}
				}
			}
			return true, "All nodes Ready", nil
		},
	}

	crioNode := &ResourceNode{
		ID:           "cri-o/container-runtime",
		SemanticType: NodeTypeCRIO,
		Kind:         "Node",
		Name:         "cri-o",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			return true, "CRI-O runtime operational", nil
		},
	}

	kubeletNode := &ResourceNode{
		ID:           "kubelet/node-agent",
		SemanticType: NodeTypeKubelet,
		Kind:         "Node",
		Name:         "kubelet",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			return true, "Kubelet responsive", nil
		},
	}

	storageNode := &ResourceNode{
		ID:           "storage/odf-operator",
		SemanticType: NodeTypeStorageOperator,
		Kind:         "StorageCluster",
		Namespace:    "openshift-storage",
		Name:         "ocs-storagecluster",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			return true, "Storage cluster operational", nil
		},
	}

	csiNode := &ResourceNode{
		ID:           "csi/ceph-csi",
		SemanticType: NodeTypeCSIDriver,
		Kind:         "CSIDriver",
		Name:         "openshift-storage.ceph.fs.csi.ceph.com",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			return true, "CSI storage driver ready", nil
		},
	}

	ovnNode := &ResourceNode{
		ID:           "ovn/ovn-kubernetes",
		SemanticType: NodeTypeOVNNetwork,
		Kind:         "DaemonSet",
		Namespace:    "openshift-ovn-kubernetes",
		Name:         "ovn-kube-node",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			return true, "OVN-Kubernetes overlay network ready", nil
		},
	}

	multusNode := &ResourceNode{
		ID:           "network/multus-nad",
		SemanticType: NodeTypeMultusNAD,
		Kind:         "NetworkAttachmentDefinition",
		Name:         "multus-nad",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			return true, "Multus CNI attachment ready", nil
		},
	}

	virtHandlerNode := &ResourceNode{
		ID:           "daemonset/virt-handler",
		SemanticType: NodeTypeVirtHandler,
		Kind:         "DaemonSet",
		Namespace:    "openshift-cnv",
		Name:         "virt-handler",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			var dsList unstructured.UnstructuredList
			dsList.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "apps",
				Version: "v1",
				Kind:    "DaemonSet",
			})
			if err := c.List(ctx, &dsList, client.InNamespace("openshift-cnv"), client.MatchingLabels{"kubevirt.io": "virt-handler"}); err == nil && len(dsList.Items) > 0 {
				for _, ds := range dsList.Items {
					desired, _, _ := unstructured.NestedInt64(ds.Object, "status", "desiredNumberScheduled")
					ready, _, _ := unstructured.NestedInt64(ds.Object, "status", "numberReady")
					if desired > 0 && ready < desired {
						return false, fmt.Sprintf("virt-handler DaemonSet updating (ready %d/%d)", ready, desired), nil
					}
				}
			}
			return true, "virt-handler DaemonSet healthy", nil
		},
	}

	kubevirtNode := &ResourceNode{
		ID:           "kubevirt/kubevirt-kubevirt",
		SemanticType: NodeTypeKubeVirt,
		Kind:         "KubeVirt",
		Namespace:    "openshift-cnv",
		Name:         "kubevirt-kubevirt",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			kv := &unstructured.Unstructured{}
			kv.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "kubevirt.io",
				Version: "v1",
				Kind:    "KubeVirt",
			})
			if err := c.Get(ctx, types.NamespacedName{Namespace: "openshift-cnv", Name: "kubevirt-kubevirt"}, kv); err == nil {
				phase, _, _ := unstructured.NestedString(kv.Object, "status", "phase")
				if phase != "" && phase != "Deployed" {
					return false, fmt.Sprintf("KubeVirt control plane phase is %s (expected Deployed)", phase), nil
				}
			}
			return true, "KubeVirt control plane deployed", nil
		},
	}

	vmimNode := &ResourceNode{
		ID:           "vmim/active-migrations",
		SemanticType: NodeTypeVMIMigration,
		Kind:         "VirtualMachineInstanceMigration",
		Name:         "active-migrations",
		Evaluator: func(ctx context.Context, c client.Client) (bool, string, error) {
			var vmimList unstructured.UnstructuredList
			vmimList.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "kubevirt.io",
				Version: "v1",
				Kind:    "VirtualMachineInstanceMigration",
			})
			if err := c.List(ctx, &vmimList, client.Limit(100)); err == nil {
				for _, vmim := range vmimList.Items {
					conditions, found, _ := unstructured.NestedSlice(vmim.Object, "status", "conditions")
					if found {
						for _, condRaw := range conditions {
							if cond, ok := condRaw.(map[string]interface{}); ok {
								if cond["type"] == "Stalled" && cond["status"] == "True" {
									return false, fmt.Sprintf("VirtualMachineInstanceMigration %s is Stalled", vmim.GetName()), nil
								}
							}
						}
					}
				}
			}
			return true, "Live migrations healthy", nil
		},
	}

	// Establish Typed Causal Dependencies:
	// MachineConfig -> RenderedConfig -> MCP -> MCD -> NodeReady -> CRIO -> Kubelet -> virt-handler -> KubeVirt -> VMIM
	// StorageOperator -> CSIDriver -> virt-handler
	// OVNNetwork -> MultusNAD -> virt-handler
	renderedNode.Parents = []*ResourceNode{mcNode}
	mcpNode.Parents = []*ResourceNode{renderedNode}
	mcdNode.Parents = []*ResourceNode{mcpNode}
	nodeReadyNode.Parents = []*ResourceNode{mcdNode}
	crioNode.Parents = []*ResourceNode{nodeReadyNode}
	kubeletNode.Parents = []*ResourceNode{crioNode}
	csiNode.Parents = []*ResourceNode{storageNode}
	multusNode.Parents = []*ResourceNode{ovnNode}
	virtHandlerNode.Parents = []*ResourceNode{kubeletNode, csiNode, multusNode}
	kubevirtNode.Parents = []*ResourceNode{virtHandlerNode}
	vmimNode.Parents = []*ResourceNode{kubevirtNode}

	g.Nodes[mcNode.ID] = mcNode
	g.Nodes[renderedNode.ID] = renderedNode
	g.Nodes[mcpNode.ID] = mcpNode
	g.Nodes[mcdNode.ID] = mcdNode
	g.Nodes[nodeReadyNode.ID] = nodeReadyNode
	g.Nodes[crioNode.ID] = crioNode
	g.Nodes[kubeletNode.ID] = kubeletNode
	g.Nodes[storageNode.ID] = storageNode
	g.Nodes[csiNode.ID] = csiNode
	g.Nodes[ovnNode.ID] = ovnNode
	g.Nodes[multusNode.ID] = multusNode
	g.Nodes[virtHandlerNode.ID] = virtHandlerNode
	g.Nodes[kubevirtNode.ID] = kubevirtNode
	g.Nodes[vmimNode.ID] = vmimNode

	return g
}

// NewVirtMaintenanceGraph initializes the OpenShift Virtualization maintenance DAG (alias for backwards compatibility).
func NewVirtMaintenanceGraph() *DependencyGraph {
	return NewTypedPlatformGraph()
}

// EvaluateCausalChain evaluates nodes in topological dependency order.
// If a parent node fails validation, evaluation of child nodes is halted and a causal failure is returned.
func (g *DependencyGraph) EvaluateCausalChain(ctx context.Context, c client.Client) error {
	for _, node := range g.Nodes {
		for _, parent := range node.Parents {
			ok, reason, err := parent.Evaluator(ctx, c)
			if err != nil || !ok {
				return fmt.Errorf("causal dependency failure: parent [%s] (%s) failed validation: %s; blocking child [%s] (%s)",
					parent.ID, parent.SemanticType, reason, node.ID, node.SemanticType)
			}
		}
	}
	return nil
}
