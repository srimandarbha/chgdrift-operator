# Kafka Integration & Architecture Guide

This document details the topic architecture, security TLS/mTLS Certificate Authority (CA) & Common Name (CN) configuration, and JSON message payloads for the **Direct Kafka Bus Architecture (`Central Agent <-> Kafka <-> Spoke Operators`)**.

---

## 1. Direct Kafka Bus Architecture (No RHACM Telemetry Dependency)

In this architecture, **RHACM is not needed in the runtime telemetry loop**. Every cluster — including the RHACM management cluster itself — runs a local `drift-operator` and communicates directly over Kafka.

```
                            ┌─────────────────────────────────┐
                            │        CENTRAL SRE AGENT        │
                            │ (LangGraph + LLM + SQLite Cache)│
                            └────────────────┬────────────────┘
                                             │
                  ┌──────────────────────────┴──────────────────────────┐
                  │ Publishes Validation / Consumes Spoke Telemetry     │
                  ▼                                                     ▼
     [ Topic: gitops.change.validation ]                [ Topic: gitops.spoke.reports ]
                  │                                                     ▲
                  │                                                     │
                  │   ┌─────────────────────────────────────────────────┤
                  │   │ (Each Spoke Operator publishes local report)     │
                  ▼   │                                                 │
   ┌──────────────────┴─────┐         ┌────────────────────────┐       ┌┴───────────────────────┐
   │ SPOKE CLUSTER 1        │         │ SPOKE CLUSTER 2        │       │ RHACM CLUSTER          │
   │ [ drift-operator ]     │         │ [ drift-operator ]     │       │ [ drift-operator ]     │
   │ (Baremetal Virt)       │         │ (Baremetal Virt)       │       │ (Treated as Spoke #3)  │
   └────────────────────────┘         └────────────────────────┘       └────────────────────────┘
```

---

## 2. Topic Architecture: 3 Key Kafka Topics

| Topic Name | Direction | Publisher | Consumer | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **`gitops.chg.events`** | Ingestion | ServiceNow / CI/CD / GitHub Webhooks | Central SRE Agent | Triggers maintenance windows (`CHG0012345`) and release tag validation (`v2.4.0`). |
| **`gitops.spoke.reports`** | Telemetry | Spoke Operators (including RHACM Hub) | Central SRE Agent | Spoke operators stream local `ClusterAppReport` JSON snapshots (health, sync, MCP status, log snippets). |
| **`gitops.change.validation`** | Emission | Central SRE Agent | SRE ChatOps / Dashboards / ITSM | Central Agent publishes final LLM-synthesized validation reports. |

---

## 3. Defining Credentials & TLS / mTLS Certificates (CN Validation)

All spoke operators and the Central Agent secure Kafka connections using **mTLS (Mutual TLS)** with Common Name (CN) / SAN hostname verification.

### Kubernetes Secret Specification (`drift-operator-kafka-certs`)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: drift-operator-kafka-certs
  namespace: gitops-fleet
type: Opaque
stringData:
  KAFKA_BROKERS: "kafka-cluster-1.company.com:9094,kafka-cluster-2.company.com:9094"
  KAFKA_SECURITY_PROTOCOL: "SSL" # or SASL_SSL
  KAFKA_SASL_MECHANISM: "SCRAM-SHA-512"
  KAFKA_INGEST_TOPIC: "gitops.chg.events"
  KAFKA_SPOKE_REPORTS_TOPIC: "gitops.spoke.reports"
  KAFKA_EMIT_TOPIC: "gitops.change.validation"
  KAFKA_SERVER_NAME: "kafka-cluster.company.com" # Common Name (CN) / SAN Validation
data:
  ca.crt: <BASE64_ENCODED_CA_CERTIFICATE>
  tls.crt: <BASE64_ENCODED_CLIENT_CERTIFICATE>
  tls.key: <BASE64_ENCODED_CLIENT_PRIVATE_KEY>
```

---

## 4. Topic 1: CHG Maintenance Ingestion Payload (`gitops.chg.events`)

Published by ServiceNow or CI/CD to start a maintenance window:

```json
{
  "eventType": "CHG_INITIATED",
  "eventId": "evt-9a8b7c6d-5e4f-3a2b",
  "timestamp": "2026-08-10T01:55:00Z",
  "chgDetails": {
    "chgNumber": "CHG0012345",
    "requestedBy": "sre-deployer@company.com",
    "startTime": "2026-08-10T02:00:00Z",
    "endTime": "2026-08-10T04:00:00Z",
    "staleReportThresholdSeconds": 300
  },
  "gitDetails": {
    "repository": "https://github.com/my-org/gitops-standards-repo",
    "releaseTag": "v2.4.0",
    "expectedRevision": "a1b2c3d98f7e6c5b4a3f2e1d"
  },
  "blastRadius": {
    "rootApp": "platform-root",
    "impactedApps": ["svc-payments", "svc-networking"],
    "targetClusters": ["us-east-01", "us-east-02", "rhacm-hub-01"]
  }
}
```

### Consumer Error Handling & Retry Guarantees (`internal/kafka/kafka_bridge.go`)
- **At-Least-Once Delivery**: Upon receiving `CHG_INITIATED` events, `KafkaBridge.Start()` attempts to create the corresponding `ChangeWindow` custom resource.
- **Context-Aware Retry Loop**: If `kb.Client.Create` encounters a transient Kubernetes API error (e.g. API server downtime or webhooks unavailable), the consumer retries creation with a 2-second backoff while listening for `ctx.Done()`. Offsets are committed to Kafka **only after** creation succeeds (or if the resource already exists), ensuring zero event loss during cluster degradation.

---

## 5. Topic 2: Spoke Telemetry Payload (`gitops.spoke.reports`)

Published by **every spoke operator** (including the RHACM cluster):

```json
{
  "clusterName": "rhacm-hub-01",
  "appName": "svc-payments",
  "observedRevision": "a1b2c3d98f7e6c5b4a3f2e1d",
  "syncStatus": "Synced",
  "health": "Healthy",
  "mcpStatus": {
    "name": "virt",
    "machineCount": 16,
    "updatedNodeCount": 16,
    "updatingNodeCount": 0,
    "degradedNodeCount": 0,
    "phase": "Updated"
  },
  "observedAt": "2026-08-10T02:15:00Z",
  "tailLogs": []
}
```

---

## 6. Topic 3: Central LLM Validation Report Payload (`gitops.change.validation`)

Published by the **Central SRE Agent** after evaluating spoke reports:

```json
{
  "chgNumber": "CHG0012345",
  "releaseTag": "v2.4.0",
  "expectedRevision": "a1b2c3d98f7e6c5b4a3f2e1d",
  "reportGeneratedAt": "2026-08-10T04:00:00Z",
  "phase": "Validated",
  "overallStatus": "Good",
  "validation": {
    "allChangesApplied": true,
    "healthCheckPassed": true,
    "mcpUpdatedOnTime": true,
    "eventsClean": true,
    "objectsConverged": true,
    "dependenciesReady": true,
    "issuesFound": [],
    "passed": true
  }
}
```
