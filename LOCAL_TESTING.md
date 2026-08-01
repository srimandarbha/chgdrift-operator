# Local Testing Guide (Docker & Kubernetes)

This guide provides step-by-step instructions for testing the **`drift-operator`** in a local Docker-based Kubernetes cluster using either **`kind` (Kubernetes in Docker)** or **`make run` (Local Out-of-Cluster Execution)**.

---

## Method 1: Local Testing in `kind` (Kubernetes in Docker)

This method tests the containerized operator inside a local Kubernetes cluster running on Docker.

### 1. Create a `kind` Cluster

```bash
kind create cluster --name drift-test
```

### 2. Build & Load Container Image

Build the local image and load it directly into `kind` (no container registry needed):

```bash
# 1. Build local container image
docker build -t drift-operator:latest .

# 2. Load image into kind cluster node
kind load docker-image drift-operator:latest --name drift-test
```

### 3. Deploy Manifests & CRDs

```bash
# 1. Create target namespace
kubectl create namespace gitops-fleet

# 2. Install CRDs (ClusterAppReport, PropagationStatus, ChangeWindow)
kubectl apply -f config/crd/bases/

# 3. Apply RBAC (ServiceAccount, Role, RoleBindings, Leader Election Leases)
kubectl apply -f config/rbac/

# 4. Deploy Operator Manager
kubectl apply -k config/manager/
```

---

## Method 2: Rapid Local Execution (`make run`)

If you want to run the Go binary directly on your machine without containerizing:

```bash
# 1. Ensure your KUBECONFIG points to your local Docker Desktop / Minikube cluster
kubectl config current-context

# 2. Install CRDs to cluster
make install

# 3. Run operator binary locally
make run
```

---

## Step 4: Apply Mock Sample CRs to Test Reconcilers

Create test custom resources in `gitops-fleet` namespace to trigger reconciliation:

### 1. Apply downstream agent report (`sample-report.yaml`)

```yaml
apiVersion: gitops.example.com/v1alpha1
kind: ClusterAppReport
metadata:
  name: us-east-01-svc-payments
  namespace: gitops-fleet
  labels:
    gitops.example.com/app: svc-payments
spec:
  clusterName: us-east-01
  appName: svc-payments
  observedRevision: a1b2c3d9
  syncStatus: Synced
  health: Healthy
  observedAt: "2026-08-01T12:00:00Z"
  mcpStatus:
    name: virt
    machineCount: 4
    updatedNodeCount: 4
    updatingNodeCount: 0
    degradedNodeCount: 0
    phase: Updated
```

```bash
kubectl apply -f sample-report.yaml
```

### 2. Apply central fleet status (`sample-propagation.yaml`)

```yaml
apiVersion: gitops.example.com/v1alpha1
kind: PropagationStatus
metadata:
  name: svc-payments
  namespace: gitops-fleet
spec:
  appName: svc-payments
  expectedRevision: a1b2c3d9
  targetClusters:
    - us-east-01
    - us-east-02
```

```bash
kubectl apply -f sample-propagation.yaml
```

### 3. Apply ChangeWindow maintenance window (`sample-changewindow.yaml`)

```yaml
apiVersion: gitops.example.com/v1alpha1
kind: ChangeWindow
metadata:
  name: chg0012345
  namespace: gitops-fleet
spec:
  chgNumber: CHG0012345
  releaseTag: v2.4.0
  expectedRevision: a1b2c3d9
  rootApp: platform-root
  impactedApps:
    - svc-payments
  startTime: "2026-08-01T10:00:00Z"
  endTime: "2026-08-01T14:00:00Z"
  staleReportThresholdSeconds: 300
```

```bash
kubectl apply -f sample-changewindow.yaml
```

---

## Step 5: Verify Reconciliation Results & Logs

### 1. Inspect `PropagationStatus`

```bash
kubectl get propagationstatus svc-payments -n gitops-fleet -o yaml
```
> **Expected Status Output**: `us-east-01` shows `InSync`, `us-east-02` shows in `missingClusters`, and `phase: Propagating`.

### 2. Inspect `ChangeWindow` Validation Report

```bash
kubectl get changewindow chg0012345 -n gitops-fleet -o yaml
```
> **Expected Validation Output**:
> ```yaml
> status:
>   phase: Validated
>   overallStatus: Good
>   validation:
>     allChangesApplied: false # us-east-02 missing
>     healthCheckPassed: true
>     mcpUpdatedOnTime: true
>     passed: false
> ```

### 3. Check Operator Logs

```bash
kubectl logs -f deployment/controller-manager -c manager -n gitops-fleet
```
