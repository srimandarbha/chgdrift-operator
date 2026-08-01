# Disconnected Baremetal Deployment & Red Hat CoP GitOps Integration Guide

This guide provides an end-to-end operational workflow to **build, unit test, containerize, push to an internal Nexus Container Registry, store diagnostic evidence in a Nexus Raw Repository, and deploy `drift-operator`** across disconnected baremetal OpenShift clusters using the [Red Hat CoP GitOps Standards Repo Template](https://github.com/redhat-cop/gitops-standards-repo-template).

---

## Complete 6-Phase Operational Workflow

```
┌──────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: Build & Unit Test (Developer Machine / CI Runner)               │
│  - Run unit tests & code formatting (`make test`, `make fmt`)            │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: Container Build & Push to Internal Nexus Docker Registry        │
│  - `docker build -t nexus.company.com:8082/.../drift-operator:v1.0.0 .`  │
│  - `docker push nexus.company.com:8082/.../drift-operator:v1.0.0`        │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ PHASE 3: Configure Nexus Raw Repository for Diagnostic Log Evidence      │
│  - Host: `https://nexus.company.com:8081/repository/gitops-evidence/`    │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ PHASE 4: Integrate Manifests into Red Hat CoP GitOps Standards Repo      │
│  - Structure under `components/operators/drift-operator/`                │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ PHASE 5: Git Merge & Argo CD ApplicationSet Auto-Sync                     │
│  - Push commit to Red Hat CoP GitOps Repo                                │
│  - Argo CD syncs `bootstrap/` -> Deploys Operator to Hub Cluster         │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ PHASE 6: Verification & Health Checks                                    │
│  - Verify Pod, Leader Election Lease, & OpenShift `restricted-v2` SCC    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Build & Test locally

Run formatting, linting, and unit tests using the Makefile:

```bash
# 1. Format code
make fmt

# 2. Run vet checks
make vet

# 3. Run unit tests & coverage (uses fake-client unit test harness in internal/controller/suite_test.go)
make test

# 4. Build local binary
make build
```

---

## Phase 2: Build Container Image & Push to Internal Nexus

Run on a workstation or bastion host with access to the internal Nexus registry:

```bash
# 1. Build multi-stage container image for Linux AMD64
docker build -t nexus.company.com:8082/gitops-fleet/drift-operator:v1.0.0 .

# 2. Authenticate to internal Nexus container registry
docker login nexus.company.com:8082 -u <NEXUS_USER> -p <NEXUS_PASSWORD>

# 3. Push container image to Nexus
docker push nexus.company.com:8082/gitops-fleet/drift-operator:v1.0.0
```

---

## Phase 3: Configure Internal Nexus Raw Repository for Log Evidence

Instead of external S3 cloud storage, air-gapped baremetal deployments use an internal **Nexus Raw Repository** for storing 500-line pod diagnostic logs:

1. **Create Repository**: In Sonatype Nexus Repository Manager, create a **Raw (Hosted)** repository named `gitops-evidence`.
2. **Set Permissions**: Enable HTTP Basic Auth for `PUT` uploads by operators and **Anonymous Read** for browser log inspection by SREs.
3. **Log Storage URL Structure**:
   ```text
   https://nexus.company.com:8081/repository/gitops-evidence/{CHG_NUMBER}/{CLUSTER}-{APP}-attempt-{ATTEMPT}.log
   ```

---

## Phase 4: Structure Operator Manifests inside Red Hat CoP GitOps Repo

Inside your organizational clone of [redhat-cop/gitops-standards-repo-template](https://github.com/redhat-cop/gitops-standards-repo-template), structure the operator manifests under the `components/` directory:

```text
gitops-standards-repo/
├── bootstrap/
│   └── base/
│       └── root-applicationset.yaml          <── Seeding Argo CD ApplicationSet
├── components/
│   └── operators/
│       └── drift-operator/                  <── Add Operator Manifests Here
│           ├── base/
│           │   ├── crds/
│           │   │   ├── gitops.example.com_clusterappreports.yaml
│           │   │   ├── gitops.example.com_propagationstatuses.yaml
│           │   │   └── gitops.example.com_changewindows.yaml
│           │   ├── rbac/
│           │   │   ├── service_account.yaml
│           │   │   ├── role.yaml
│           │   │   ├── role_binding.yaml
│           │   │   ├── leader_election_role.yaml
│           │   │   └── leader_election_role_binding.yaml
│           │   ├── manager/
│           │   │   └── manager.yaml
│           │   └── kustomization.yaml
│           └── overlays/
│               └── prod/
│                   └── kustomization.yaml   <── Image override to Nexus URL
└── groups/
    └── prod/
        └── kustomization.yaml               <── Includes components/operators/drift-operator/overlays/prod
```

### `components/operators/drift-operator/overlays/prod/kustomization.yaml`

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: gitops-fleet
resources:
  - ../../base
images:
  - name: drift-operator
    newName: nexus.company.com:8082/gitops-fleet/drift-operator
    newTag: v1.0.0
```

---

## Phase 5: Commit to Git & Auto-Sync via Argo CD

Commit and push the component to your Red Hat CoP GitOps repository:

```bash
git add components/operators/drift-operator groups/prod/kustomization.yaml
git commit -m "feat(gitops): Add drift-operator component pointing to internal Nexus"
git push origin main
```

Argo CD automatically detects the commit on `main`, processes the `bootstrap/` ApplicationSet, creates the `gitops-fleet` namespace, applies the CRDs and RBAC, pulls `nexus.company.com:8082/gitops-fleet/drift-operator:v1.0.0`, and deploys the OpenShift `restricted-v2` SCC pod!

---

## Phase 6: Verification & Troubleshooting

Verify the deployment on your disconnected OpenShift cluster using `oc`:

```bash
# 1. Check Pod status in gitops-fleet namespace
oc get pods -n gitops-fleet

# 2. Verify OpenShift Security Context Constraint (SCC) allocation
oc describe pod -l control-plane=controller-manager -n gitops-fleet | grep -i scc

# 3. Check Leader Election Lease acquisition
oc get leases -n gitops-fleet

# 4. Stream Operator Logs
oc logs -f deployment/controller-manager -c manager -n gitops-fleet
```
