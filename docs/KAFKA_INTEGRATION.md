# Kafka Integration & Architecture Guide

This document details the topic architecture, security TLS/mTLS Certificate Authority (CA) & Common Name (CN) configuration, and JSON message payloads for the **Kafka Bus Integration (`ServiceNow / Central SRE Agent <-> Kafka <-> drift-operator`)**.

---

## 1. Direct Kafka Bus Architecture

The system uses **exactly two Kafka topics** for communication:

```
                            ┌─────────────────────────────────┐
                            │        CENTRAL SRE AGENT        │
                            │ (LangGraph + LLM + PostgreSQL)  │
                            └────────────────┬────────────────┘
                                             │
                  ┌──────────────────────────┴──────────────────────────┐
                  │ Publishes Maintenance Window / Consumes Validation  │
                  ▼                                                     ▲
     [ Ingestion: gitops.chg.events ]                   [ Emission: gitops.change.validation ]
                  │                                                     │
                  │ (Ingests CHG Window Events)                         │ (Emits Validation Reports)
                  ▼                                                     │
   ┌───────────────────────────────┐                                    │
   │ RHACM HUB / CENTRAL CLUSTER   ├────────────────────────────────────┘
   │ [ drift-operator ]            │
   │ (ChangeWindowReconciler)      │
   └───────────────▲───────────────┘
                   │
                   │ (Spokes write ClusterAppReport CRs directly to Hub)
    ┌──────────────┴──────────────┐
    │ SPOKE CLUSTER 1 / 2         │
    │ [ drift-operator ]          │
    │ (LocalAppWatchReconciler)   │
    └─────────────────────────────┘
```

---

## 2. Topic Architecture: 2 Key Kafka Topics

| Topic Name | Direction | Publisher | Consumer | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **`gitops.chg.events`** | Ingestion | ServiceNow / CI/CD / SRE Agent | `drift-operator` (`KafkaBridge`) | Triggers maintenance windows (`CHG0012345`) and creates `ChangeWindow` CRs. |
| **`gitops.change.validation`** | Emission | `drift-operator` (`ChangeWindowReconciler`) | Central SRE Agent / ChatOps / ITSM | Emits post-deployment LLM validation reports and 8-gate health assessments immediately on phase/state changes, with a 15-minute throttled heartbeat. |

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
  KAFKA_EMIT_TOPIC: "gitops.change.validation"
  KAFKA_SERVER_CN: "kafka-cluster.company.com" # Common Name (CN) / SAN Validation
  KAFKA_CA_FILE: "/etc/kafka/certs/ca.crt"
  KAFKA_CLIENT_CERT_FILE: "/etc/kafka/certs/tls.crt"
  KAFKA_CLIENT_KEY_FILE: "/etc/kafka/certs/tls.key"
data:
  ca.crt: <BASE64_ENCODED_CA_CERTIFICATE>
  tls.crt: <BASE64_ENCODED_CLIENT_CERTIFICATE>
  tls.key: <BASE64_ENCODED_CLIENT_PRIVATE_KEY>
```

### Container Volume Mounts (`config/manager/manager.yaml`)

The operator Deployment mounts the secret into `/etc/kafka/certs`:

```yaml
spec:
  containers:
    - name: manager
      envFrom:
        - secretRef:
            name: drift-operator-kafka-certs
      volumeMounts:
        - name: kafka-certs-vol
          mountPath: "/etc/kafka/certs"
          readOnly: true
  volumes:
    - name: kafka-certs-vol
      secret:
        secretName: drift-operator-kafka-certs
        items:
          - key: ca.crt
            path: ca.crt
          - key: tls.crt
            path: tls.crt
          - key: tls.key
            path: tls.key
```

### Environment Variable & Certificate Mapping

| Environment Variable | Source / Secret Key | Mount Path / Value | Description |
| :--- | :--- | :--- | :--- |
| **`KAFKA_BROKERS`** | Secret `stringData` | `kafka-1.co:9094,kafka-2.co:9094` | Comma-separated list of bootstrap brokers |
| **`KAFKA_INGEST_TOPIC`** | Secret `stringData` | `gitops.chg.events` | Ingestion topic for CHG initiation events |
| **`KAFKA_EMIT_TOPIC`** | Secret `stringData` | `gitops.change.validation` | Emission topic for validation reports |
| **`KAFKA_SERVER_CN`** | Secret `stringData` | `kafka-cluster.company.com` | Expected SAN/CN for TLS hostname verification (`InsecureSkipVerify: false`) |
| **`KAFKA_CA_FILE`** | Secret `data.ca.crt` | `/etc/kafka/certs/ca.crt` | Path to Root CA certificate PEM file |
| **`KAFKA_CLIENT_CERT_FILE`** | Secret `data.tls.crt` | `/etc/kafka/certs/tls.crt` | Path to Client mTLS certificate PEM file |
| **`KAFKA_CLIENT_KEY_FILE`** | Secret `data.tls.key` | `/etc/kafka/certs/tls.key` | Path to Client mTLS private key PEM file |

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

## 5. Topic 2: Central LLM Validation Report Payload (`gitops.change.validation`)

Published by `drift-operator` (`ChangeWindowReconciler`) after evaluating spoke cluster reports:

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
