# Local Testing Guide for Docker Desktop Kubernetes

This guide details how to build, deploy, and test **`drift-operator`** locally on your laptop using **Docker Desktop Kubernetes** (Windows / macOS).

---

## Why Docker Desktop Makes Testing Easy

Docker Desktop shares its local image store directly with its built-in Kubernetes cluster. Any image you build using `docker build -t drift-operator:latest .` is **instantly available to Kubernetes** — no registry push, no `kind load`, and no external network calls required!

---

## Method 1: Containerized Local Testing in Docker Desktop

### Step 1: Verify Docker Desktop Kubernetes is Active

1. Open Docker Desktop Settings -> **Kubernetes** -> Ensure **Enable Kubernetes** is checked.
2. Set your `kubectl` context to Docker Desktop:

```cmd
kubectl config use-context docker-desktop
```

### Step 2: Build Image Locally

Build the container image. It will land directly in Docker Desktop's local engine:

```cmd
docker build -t drift-operator:latest .
```

### Step 3: Deploy CRDs, RBAC, and Operator

Deploy manifests into Docker Desktop Kubernetes:

```cmd
# 1. Create target namespace
kubectl create namespace gitops-fleet

# 2. Apply CRDs (ClusterAppReport, PropagationStatus, ChangeWindow)
kubectl apply -f config/crd/bases/

# 3. Apply RBAC (ServiceAccount, Role, RoleBindings, Leader Election Leases)
kubectl apply -f config/rbac/

# 4. Deploy Operator Manager
kubectl apply -k config/manager/
```

> **Note**: `config/manager/manager.yaml` uses `imagePullPolicy: IfNotPresent`, which forces Docker Desktop to use your local `drift-operator:latest` image directly!

---

## Method 2: Rapid Local Execution (`make run`)

If you want to run the Go binary directly on your laptop without building containers:

```cmd
# 1. Ensure KUBECONFIG points to Docker Desktop
kubectl config use-context docker-desktop

# 2. Install CRDs into Docker Desktop
make install

# 3. Run operator binary locally on your laptop
make run
```

---

## Step 4: Apply Sample Custom Resources to Test Reconcilers

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

```cmd
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

```cmd
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

```cmd
kubectl apply -f sample-changewindow.yaml
```

---

## Step 5: Verify Reconciliation Results & View Logs

```cmd
# 1. Inspect fleet propagation status
kubectl get propagationstatus svc-payments -n gitops-fleet -o yaml

# 2. Inspect ChangeWindow validation report
kubectl get changewindow chg0012345 -n gitops-fleet -o yaml

# 3. Stream live operator logs from Docker Desktop
kubectl logs -f deployment/controller-manager -c manager -n gitops-fleet
```

---

## Step 6: Local Kafka CHG Event Simulation

To test Kafka ingestion (`gitops.chg.events`) and emission (`gitops.change.validation`) without needing a fully deployed Central SRE Agent, refer to the step-by-step simulation guide:

- **[SIMULATE_CHG_TESTING.md](SIMULATE_CHG_TESTING.md)** — Step-by-step local Kafka event producer/consumer testing with CLI tools (`kcat`) and Python scripts.
