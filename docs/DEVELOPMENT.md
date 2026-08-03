# Extensibility & Developer Guide

This guide explains how to add new APIs, CRDs, and Controllers to `drift-operator` while maintaining strict compliance with [AGENTS.md](../.agents/AGENTS.md).

---

## 4-Step Workflow for Adding New APIs & Controllers

### Step 1: Define API Structs & Register Scheme
1. Create `api/v1alpha1/<kind>_types.go` (or `api/v1beta1/` for new version groups):
   ```go
   package v1alpha1

   import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

   type MyNewKindSpec struct {
       // ...
   }

   type MyNewKindStatus struct {
       ObservedGeneration int64              `json:"observedGeneration,omitempty"`
       Conditions         []metav1.Condition `json:"conditions,omitempty"`
   }

   // +kubebuilder:object:root=true
   // +kubebuilder:subresource:status
   type MyNewKind struct {
       metav1.TypeMeta   `json:",inline"`
       metav1.ObjectMeta `json:"metadata,omitempty"`
       Spec   MyNewKindSpec   `json:"spec,omitempty"`
       Status MyNewKindStatus `json:"status,omitempty"`
   }

   // +kubebuilder:object:root=true
   type MyNewKindList struct {
       metav1.TypeMeta `json:",inline"`
       metav1.ListMeta `json:"metadata,omitempty"`
       Items           []MyNewKind `json:"items"`
   }
   ```
2. Register the type in `api/v1alpha1/groupversion_info.go`:
   ```go
   func init() {
       SchemeBuilder.Register(&MyNewKind{}, &MyNewKindList{})
   }
   ```

---

### Step 2: Implement the Reconciler
Create `internal/controller/<kind>_controller.go`:
```go
package controller

import (
    "context"
    "reflect"
    "time"
    apierrors "k8s.io/apimachinery/pkg/api/errors"
    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/controller-runtime/pkg/client"
    ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
    "sigs.k8s.io/controller-runtime/pkg/log"
    gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

type MyNewKindReconciler struct {
    client.Client
}

func (r *MyNewKindReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    logger := log.FromContext(ctx)
    var obj gitopsv1alpha1.MyNewKind
    if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
        if apierrors.IsNotFound(err) {
            return ctrl.Result{}, nil
        }
        return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
    }

    if !obj.DeletionTimestamp.IsZero() {
        return ctrl.Result{}, nil
    }

    original := obj.DeepCopy()

    // Always use client.Limit(100) on list queries
    var childList gitopsv1alpha1.ClusterAppReportList
    if err := r.List(ctx, &childList, client.InNamespace(obj.Namespace), client.Limit(100)); err != nil {
        return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
    }

    // Mutate obj.Status...

    // Rule 4: Patch status independently only if status has changed
    if !reflect.DeepEqual(original.Status, obj.Status) {
        if err := r.Status().Patch(ctx, &obj, client.MergeFrom(original)); err != nil {
            if apierrors.IsConflict(err) {
                return ctrl.Result{Requeue: true}, nil
            }
            return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
        }
    }

    return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *MyNewKindReconciler) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&gitopsv1alpha1.MyNewKind{}).
        WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 5}).
        Complete(r)
}
```

---

### Step 3: Register Controller in `controllers.go`
Open `internal/controller/controllers.go` and add the new controller to the slice:
```go
{
    name: "MyNewKind",
    controller: &MyNewKindReconciler{
        Client: mgr.GetClient(),
    },
},
```

---

### Step 4: Add CRD & RBAC Manifests
1. Add `config/crd/bases/gitops.example.com_mynewkinds.yaml` with the OpenAPI v3 validation schema.
2. Add RBAC rules to `config/rbac/role.yaml` following the principle of least privilege.
3. Register the resource in `PROJECT` metadata file.

---

## 5. Developer Guide: Extending Core Validation Engines

### 5.1 Adding Semantic Nodes to `TypedPlatformGraph` (`internal/validator/graph.go`)
To add a new component node to the causal dependency graph:
1. Define a new `SemanticNodeType` constant (e.g. `NodeTypeOVNNetwork`).
2. Create a `ResourceNode` struct with `SemanticType`, `Kind`, `Namespace`, `Name`, and an `Evaluator` function returning `(bool, string, error)`.
3. Establish parent-child dependency edges in `NewTypedPlatformGraph()` (e.g., `ovnNode.Parents = []*ResourceNode{crioNode}`).

### 5.2 Extending Cryptographic Evidence Models (`internal/validator/evidence.go`)
1. To modify evidence payload structures, update `ImmutableEvidenceSnapshot` or `SignedAuditReport`.
2. Always recalculate SHA-256 payload digests via `CalculateSHA256` and compute HMAC-SHA256 signatures via `SignHMAC256`.
3. Ensure `VerifyReportSignature` unit tests pass in `internal/validator/evidence_test.go`.

### 5.3 Executing Validation & Failure Injection Test Harnesses
```bash
# Run validator unit tests & cryptographic signature verification
go test -v ./internal/validator/...

# Run state machine controller tests
go test -v ./internal/controller/... -run TestChangeWindow

# Run synthetic failure injection suite (API throttling, Kafka outage, restart recovery)
go test -v ./internal/controller/... -run TestFailureInjection

# Run 10-goroutine parallel concurrency & scale benchmark suite
go test -v ./internal/controller/... -run "TestConcurrency|Benchmark"

# Run full project test suite
go test -v ./...
```
