package controller

import (
	"context"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DetectClusterRole checks Kubernetes API resources and namespaces to determine if the cluster is a Hub or Spoke.
// Hub Indicators:
//   1. MultiClusterHub resource exists (Group: operator.open-cluster-management.io)
//   2. ManagedCluster resources exist (Group: cluster.open-cluster-management.io)
//   3. Namespace 'open-cluster-management' exists
// Spoke Indicators:
//   1. Namespace 'open-cluster-management-agent' exists
//   2. Absence of MultiClusterHub resource
func DetectClusterRole(ctx context.Context, k8sClient client.Client) bool {
	// 1. Explicit Environment Override
	if role := os.Getenv("OPERATOR_ROLE"); strings.EqualFold(role, "hub") {
		return true
	} else if strings.EqualFold(role, "spoke") {
		return false
	}
	if isHubEnv := os.Getenv("IS_HUB_CLUSTER"); isHubEnv != "" {
		return strings.EqualFold(isHubEnv, "true") || isHubEnv == "1"
	}

	if k8sClient == nil {
		return checkEnvFallback()
	}

	// 2. Check for Spoke Agent Namespace ('open-cluster-management-agent')
	var spokeAgentNS corev1.Namespace
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "open-cluster-management-agent"}, &spokeAgentNS); err == nil {
		// Found spoke agent namespace -> This is a Spoke cluster
		return false
	}

	// 3. Check for MultiClusterHub CRD / Resource (ACM / MCE Hub indicator)
	var mchList unstructured.UnstructuredList
	mchList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operator.open-cluster-management.io",
		Version: "v1",
		Kind:    "MultiClusterHub",
	})
	if err := k8sClient.List(ctx, &mchList, client.Limit(1)); err == nil && len(mchList.Items) > 0 {
		// MultiClusterHub resource exists -> This is a Hub cluster
		return true
	}

	// 4. Check for ManagedCluster CRD / Resource (ACM / MCE Hub indicator)
	var mcList unstructured.UnstructuredList
	mcList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1",
		Kind:    "ManagedCluster",
	})
	if err := k8sClient.List(ctx, &mcList, client.Limit(1)); err == nil && len(mcList.Items) > 0 {
		// ManagedCluster list exists -> This is a Hub cluster managing spokes
		return true
	}

	// 5. Check for Hub Namespace ('open-cluster-management')
	var hubNS corev1.Namespace
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "open-cluster-management"}, &hubNS); err == nil {
		return true
	}

	return checkEnvFallback()
}

func checkEnvFallback() bool {
	cn := os.Getenv("CLUSTER_NAME")
	if cn == "" || strings.EqualFold(cn, "hub") || strings.HasPrefix(strings.ToLower(cn), "hub-") || strings.HasPrefix(strings.ToLower(cn), "mgmt-") {
		return true
	}
	return false
}
