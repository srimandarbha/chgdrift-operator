# Kafka Integration & Security Guide

This document details the Kafka event specifications, topic architecture, security TLS/mTLS Certificate Authority (CA) & Common Name (CN) configuration, and JSON message payloads for the **Cross-Cluster GitOps Drift & CHG Correlation Operator** (`drift-operator`).

---

## 1. Topic Architecture: Ingestion vs. Emission

The Operator uses **two separate Kafka topics** to isolate input events from validation reports and prevent recursive feedback loops:

```
[ Drift Detection Agent / ITSM ]
               │
               ▼ (Publishes CHG Initiated Event)
   [ Topic: gitops.chg.events ]  ◄─────── Ingestion Topic
               │
               ▼ (Consumes & Reconciles ChangeWindow CR)
     [ drift-operator ]
               │
               ▼ (Emits Validation Report)
 [ Topic: gitops.change.validation ] ◄─── Emission Topic (DIFFERENT TOPIC)
               │
               ▼ (Consumes LLM JSON Report)
[ SRE Agent / ChatOps / Dashboard ]
```

| Dimension | Ingestion Topic | Emission Topic |
| :--- | :--- | :--- |
| **Topic Name** | `gitops.chg.events` | `gitops.change.validation` |
| **Partition Key** | `CHG0012345` (CHG Number) | `CHG0012345` (CHG Number) |
| **Publisher** | Drift Detection Agent / CI/CD / ServiceNow | `drift-operator` Hub Controller |
| **Consumer** | `drift-operator` Hub Controller | LLM SRE Agent / ChatOps / Dashboards |
| **Purpose** | Triggers `ChangeWindow` CR creation & silence window | Reports fleet convergence, logs, & post-validation |

---

## 2. Defining Kafka Credentials & TLS / mTLS Certificates (CN Validation)

The Operator secures Kafka connections using **mTLS (Mutual TLS)** or **SASL_SSL**. Certificates and CN validation settings are injected via a Kubernetes Secret mounted into the Operator deployment.

### Kubernetes Secret Specification (`kafka-secret.yaml`)

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
  KAFKA_SERVER_NAME: "kafka-cluster.company.com" # Common Name (CN) / SAN Validation
data:
  ca.crt: <BASE64_ENCODED_CA_CERTIFICATE>
  tls.crt: <BASE64_ENCODED_CLIENT_CERTIFICATE>
  tls.key: <BASE64_ENCODED_CLIENT_PRIVATE_KEY>
```

### Operator Volume Mounts (`config/manager/manager.yaml` snippet)

```yaml
spec:
  containers:
    - name: manager
      image: drift-operator:latest
      envFrom:
        - secretRef:
            name: drift-operator-kafka-certs
      volumeMounts:
        - name: kafka-certs
          mountPath: /etc/kafka/certs
          readOnly: true
  volumes:
    - name: kafka-certs
      secret:
        secretName: drift-operator-kafka-certs
```

### Go TLS Config with CN Validation (`kafka_bridge.go` logic)

```go
func NewKafkaTLSConfig(caCertPath, clientCertPath, clientKeyPath, serverCN string) (*tls.Config, error) {
    caCert, err := os.ReadFile(caCertPath)
    if err != nil {
        return nil, fmt.Errorf("failed to read CA cert: %w", err)
    }
    caCertPool := x509.NewCertPool()
    caCertPool.AppendCertsFromPEM(caCert)

    cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
    if err != nil {
        return nil, fmt.Errorf("failed to load client cert key pair: %w", err)
    }

    return &tls.Config{
        Certificates:       []tls.Certificate{cert},
        RootCAs:            caCertPool,
        ServerName:         serverCN, // Common Name (CN) / SAN Hostname Verification
        InsecureSkipVerify: false,    // Enforce strict TLS CN verification
    }, nil
}
```

---

## 3. Ingestion Event Format (`gitops.chg.events`)

What the Operator **picks up / consumes** from Kafka:

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
    "expectedRevision": "a1b2c3d98f7e6c5b4a3f2e1d",
    "baselineRevision": "f9e8d7c6b5a43210fe987654",
    "changedFiles": [
      "components/networking/base/kustomization.yaml",
      "groups/prod/kustomization.yaml"
    ]
  },
  "blastRadius": {
    "rootApp": "platform-root",
    "impactedApps": [
      "svc-payments",
      "svc-networking"
    ],
    "targetClusters": [
      "us-east-01",
      "us-east-02",
      "eu-west-01"
    ]
  },
  "remediationPolicy": {
    "hardRefresh": {
      "maxAttempts": 2,
      "waitBetweenSeconds": 180,
      "actionExecutionMode": "Parked"
    }
  }
}
```

---

## 4. Emission Event Format (`gitops.change.validation`)

What the Operator **sends back / produces** to Kafka:

```json
{
  "chgNumber": "CHG0012345",
  "releaseTag": "v2.4.0",
  "expectedRevision": "a1b2c3d98f7e6c5b4a3f2e1d",
  "reportGeneratedAt": "2026-08-10T04:00:00Z",
  "window": {
    "start": "2026-08-10T02:00:00Z",
    "end": "2026-08-10T04:00:00Z"
  },
  "phase": "Validated",
  "overallStatus": "Good",
  "rootApp": "platform-root",
  "mcpStatus": {
    "name": "virt",
    "machineCount": 16,
    "updatedNodeCount": 16,
    "updatingNodeCount": 0,
    "degradedNodeCount": 0,
    "phase": "Updated"
  },
  "silentClusters": [],
  "actionsApplied": [],
  "validation": {
    "allChangesApplied": true,
    "healthCheckPassed": true,
    "mcpUpdatedOnTime": true,
    "issuesFound": [],
    "passed": true
  }
}
```
