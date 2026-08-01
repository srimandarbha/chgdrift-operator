# Disconnected / Air-Gapped Baremetal OpenShift Deployment Guide

This guide provides step-by-step instructions for building, mirroring, and deploying the **Cross-Cluster GitOps Drift & CHG Correlation Operator** (`drift-operator`) to a **disconnected, air-gapped baremetal OpenShift Virtualization cluster** using an internal **Nexus Container Registry**.

---

## Architecture in Disconnected Baremetal Environment

In an air-gapped environment, nodes have zero internet access. All container images must be pulled from an internal Nexus registry (`nexus.company.com:8082`), and all Kafka event communications occur over internal TLS (`kafka.internal:9094`).

```
 [ Bastion Host / Build CI ]
             │
             ▼ (1. docker build & push)
 [ Internal Nexus Registry ] ◄── (nexus.company.com:8082)
             │
             ▼ (2. Air-gapped image pull via ServiceAccount Secret)
┌──────────────────────────────────────────────────────────────────────────┐
│ DISCONNECTED OPENSHIFT BAREMETAL CLUSTER (gitops-fleet)                  │
│                                                                          │
│  - Deployment (OpenShift restricted-v2 SCC compliant)                    │
│  - Internal Kafka mTLS Bridge (kafka.internal:9094)                      │
│  - Monitors OpenShift Virtualization MachineConfigPool (virt)           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Build Image & Push to Internal Nexus Registry

Run these steps on a bastion host or CI runner with network access to the internal Nexus registry:

```bash
# 1. Build multi-stage container image targeting Linux AMD64
docker build -t nexus.company.com:8082/gitops-fleet/drift-operator:v1.0.0 .

# 2. Log in to internal Nexus Container Registry
docker login nexus.company.com:8082 -u <NEXUS_USER> -p <NEXUS_PASSWORD>

# 3. Push container image to Nexus
docker push nexus.company.com:8082/gitops-fleet/drift-operator:v1.0.0
```

---

## Step 2: Configure Kustomize Deployment Image

Set the image URL to your internal Nexus registry in `config/manager/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - manager.yaml
images:
  - name: drift-operator
    newName: nexus.company.com:8082/gitops-fleet/drift-operator
    newTag: v1.0.0
```

---

## Step 3: Create Air-Gapped Secrets on OpenShift

Log in to your disconnected OpenShift cluster using `oc`:

```bash
# 1. Create target namespace
oc create namespace gitops-fleet

# 2. Create Image Pull Secret for Nexus Registry
oc create secret docker-registry nexus-pull-secret \
  --docker-server=nexus.company.com:8082 \
  --docker-username=<NEXUS_USER> \
  --docker-password=<NEXUS_PASSWORD> \
  -n gitops-fleet

# 3. Link Pull Secret to Operator ServiceAccount
oc secrets link drift-operator-controller-manager nexus-pull-secret --for=pull -n gitops-fleet

# 4. Create Kafka mTLS Certificate Secret (Internal Kafka)
oc create secret generic drift-operator-kafka-certs \
  --from-literal=KAFKA_BROKERS="kafka-broker1.internal:9094,kafka-broker2.internal:9094" \
  --from-literal=KAFKA_INGEST_TOPIC="gitops.chg.events" \
  --from-literal=KAFKA_EMIT_TOPIC="gitops.change.validation" \
  --from-literal=KAFKA_SERVER_NAME="kafka.internal" \
  --from-file=ca.crt=./ca.crt \
  --from-file=tls.crt=./tls.crt \
  --from-file=tls.key=./tls.key \
  -n gitops-fleet
```

---

## Step 4: Deploy CRDs, RBAC, and Manager

Apply the manifests to OpenShift:

```bash
# 1. Apply CRDs (ClusterAppReport, PropagationStatus, ChangeWindow)
oc apply -f config/crd/bases/

# 2. Apply RBAC (ServiceAccount, ClusterRole, RoleBindings, Leader Election Leases)
oc apply -f config/rbac/

# 3. Apply Manager Deployment (OpenShift restricted-v2 SCC Compliant)
oc apply -k config/manager/
```

---

## Step 5: OpenShift Security Context Constraint (SCC) Verification

The operator deployment in `config/manager/manager.yaml` strictly complies with OpenShift's **`restricted-v2` SCC**:

```yaml
spec:
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: manager
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop:
            - ALL
```

> **Note**: No fixed `runAsUser` is declared. OpenShift automatically allocates UIDs dynamically from the namespace's UID range (`openshift.io/sa.scc.uid-range`), satisfying OpenShift baremetal security policies.

---

## Step 6: Verify Deployment & Leader Election

```bash
# 1. Check Pod status
oc get pods -n gitops-fleet

# 2. Check Leader Election Lease acquisition
oc get leases -n gitops-fleet

# 3. Stream Operator Logs
oc logs -f deployment/controller-manager -c manager -n gitops-fleet
```
