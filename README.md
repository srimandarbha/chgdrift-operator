# drift-operator

Custom Kubernetes Operator built with Kubebuilder (`example.com/drift-operator`) that correlates GitOps change propagation across 100+ clusters, tracks cross-cluster drift against CHG release tags, enforces maintenance silence, captures pod failure logs, and publishes LLM-understandable reports to Kafka.

## Directory Structure

```text
drift-operator/
├── .agents/
│   └── AGENTS.md                  # Operator Development Guidelines
├── api/
│   └── v1alpha1/                  # CRD Schemas (ClusterAppReport, PropagationStatus, ChangeWindow)
│       ├── groupversion_info.go
│       └── propagationstatus_types.go
├── config/                        # Kustomize manifests
│   ├── crd/bases/                 # CRD YAML definitions
│   ├── rbac/                      # Least-privilege ClusterRole & Bindings
│   └── prometheus/                # PrometheusRule alerting definitions
├── internal/
│   ├── controller/                # Idempotent Reconciler Implementations
│   │   ├── propagationstatus_controller.go
│   │   └── changewindow_controller.go
│   └── metrics/                   # Prometheus Metrics (State, Lag, Report Age)
│       └── metrics.go
├── main.go                        # Manager entrypoint (Leader Election, Probes, Indexers)
├── Makefile                       # Build/test automation
├── PROJECT                        # Kubebuilder metadata
└── go.mod                         # Go module definition
```

## Features

- **Three CRDs**:
  - `ClusterAppReport`: Written only by downstream cluster agents.
  - `PropagationStatus`: Managed by central operator to aggregate fleet status (`InSync`, `Propagating`, `Lagging`, `Diverged`, `Stale`, `Missing`).
  - `ChangeWindow`: Manages CHG maintenance windows, maintenance silence, log collection, and Kafka reporting.
- **Kafka CHG Maintenance Silence**: Ingests CHG JSON `startTime` and `endTime` to apply maintenance silence while classifying `WentSilentDuringChg` vs `SilentBeforeChgStart`.
- **Log-First Diagnostics**: Captures tail 500 lines from failing pods, capping inline JSON logs (max 20 lines / 2 KB) with S3 pointers (`logRef`).
- **OpenShift Virtualization Forward-Fix Paradigm**: Designed for forward-fix workflows (`v2.4.0` $\rightarrow$ `v2.4.1`) without attempting destructive rollbacks on VMs or hypervisor MachineConfigs.
- **Operator Development Guidelines (`.agents/AGENTS.md`)**: Fully compliant with level-based idempotency, status patching via `client.MergeFrom()`, list pagination, field indexers, and leader election.

## Documentation & Integration Guides

- **[KAFKA_INTEGRATION.md](KAFKA_INTEGRATION.md)** — Ingestion & emission Kafka topics, TLS/mTLS certificate (CN validation) setup, and JSON event schemas.
- **[DEVELOPMENT.md](DEVELOPMENT.md)** — Step-by-step 4-stage guide for adding new APIs, CRDs, and controllers.
- **[.agents/AGENTS.md](.agents/AGENTS.md)** — Production Operator Development Guidelines.

## Building & Running

```bash
# Format code
make fmt

# Run tests
make test

# Build manager binary
make build

# Run operator locally against active KUBECONFIG
make run
```
