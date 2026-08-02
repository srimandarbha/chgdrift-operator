# Local CHG Simulation & Operator Testing Guide

This guide details how to test **`drift-operator`** locally against your OpenShift lab cluster by **simulating Central Agent CHG initiation events over Kafka** without needing a fully deployed Central Agent.

---

## Architecture Flow for Local Simulation

```
 ┌──────────────────────────────────┐
 │  Local Simulator (CLI / Python)  │
 │  Publishes simulate_chg.json     │
 └────────────────┬─────────────────┘
                  │
                  ▼ (Topic 1: gitops.chg.events)
 ┌──────────────────────────────────┐
 │           KAFKA BUS              │
 └────────────────┬─────────────────┘
                  │
                  ▼ (Injected into Operator)
 ┌──────────────────────────────────┐
 │    Local Go Operator Manager     │  <== Runs locally via `make run`
 │  - Reads gitops.chg.events       │      against local OpenShift KUBECONFIG
 │  - Creates ChangeWindow CR       │
 │  - Inspects local OpenShift apps │
 │  - Evaluates 6 Validation Gates  │
 │  - Emits report to Kafka         │
 └────────────────┬─────────────────┘
                  │
                  ▼ (Topic 2: gitops.change.validation)
 ┌──────────────────────────────────┐
 │  Kafka Consumer (`kcat` / CLI)   │  <== You observe validation output
 └──────────────────────────────────┘
```

---

## Step 1: Configure Operator Local Environment Variables

Set environment variables on your terminal so `drift-operator` knows your local OpenShift `KUBECONFIG` and local Kafka broker endpoints:

```bash
# 1. Point to your local OpenShift lab cluster context
export KUBECONFIG=~/.kube/config
kubectl config set-context --current --namespace=gitops-fleet

# 2. Configure Kafka Brokers & Topic Names
export KAFKA_BROKERS="localhost:9092" # or OpenShift Strimzi Kafka bootstrap service
export KAFKA_INGEST_TOPIC="gitops.chg.events"
export KAFKA_EMIT_TOPIC="gitops.change.validation"
export OPERATOR_NAMESPACE="gitops-fleet"

# Optional: Disable TLS for local unencrypted testing (leave blank)
export KAFKA_CA_FILE=""
```

---

## Step 2: Start the Go Operator Manager Locally (`make run`)

1. Install CRDs into your local OpenShift lab cluster:
   ```bash
   make install
   ```
2. Start the operator manager locally:
   ```bash
   make run
   ```

You will see logs confirming Kafka bridge initialization:
```text
INFO Kafka bridge initialised and registered as leader-elected runnable {"ingestTopic": "gitops.chg.events", "emitTopic": "gitops.change.validation", "brokers": ["localhost:9092"]}
```

---

## Step 3: Prepare Simulated CHG Ingestion Payload (`simulate_chg.json`)

Create a local test payload file named `simulate_chg.json`:

```json
{
  "eventType": "CHG_INITIATED",
  "eventId": "evt-sim-1001",
  "timestamp": "2026-08-02T00:30:00Z",
  "chgDetails": {
    "chgNumber": "CHG-TEST-1001",
    "requestedBy": "lab-tester@company.com",
    "startTime": "2026-08-02T00:00:00Z",
    "endTime": "2026-08-02T02:00:00Z",
    "staleReportThresholdSeconds": 300
  },
  "gitDetails": {
    "releaseTag": "v2.4.0",
    "expectedRevision": "rev-100",
    "baselineRevision": "rev-099"
  },
  "blastRadius": {
    "rootApp": "platform-root",
    "impactedApps": ["payments-api"],
    "targetClusters": ["lab-cluster-01"]
  }
}
```

---

## Step 4: Publish Simulated Event to `gitops.chg.events`

Use one of the following methods to publish the payload:

### Option A: Using `kcat` / `kafkacat`
```bash
kcat -b localhost:9092 -t gitops.chg.events -P simulate_chg.json
```

### Option B: Using Kafka Console Producer CLI
```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic gitops.chg.events < simulate_chg.json
```

### Option C: Using Python (`simulate_chg.py`)
```python
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('simulate_chg.json') as f:
    payload = json.load(f)

producer.send('gitops.chg.events', payload)
producer.flush()
print("Simulated CHG event successfully published to gitops.chg.events!")
```

---

## Step 5: Verify Operator Reconciles `ChangeWindow` CR

Once published, check your running operator logs or inspect OpenShift:

1. Check operator logs:
   ```text
   INFO ChangeWindow created from Kafka event {"chg": "CHG-TEST-1001"}
   ```
2. Query the created custom resource:
   ```bash
   kubectl get changewindow chg-test-1001 -n gitops-fleet -o yaml
   ```

---

## Step 6: Listen for Emitted Validation Report on `gitops.change.validation`

In a separate terminal, observe the validation report emitted by `drift-operator`:

### Option A: Using `kcat`
```bash
kcat -b localhost:9092 -t gitops.change.validation -C -q
```

### Option B: Using Kafka Console Consumer CLI
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gitops.change.validation --from-beginning
```

### Expected Output Payload
```json
{
  "chgNumber": "CHG-TEST-1001",
  "releaseTag": "v2.4.0",
  "expectedRevision": "rev-100",
  "reportGeneratedAt": "2026-08-02T03:45:00Z",
  "phase": "Validated",
  "overallStatus": "Good",
  "validation": {
    "clusterOperatorsHealthy": true,
    "allChangesApplied": true,
    "healthCheckPassed": true,
    "mcpUpdatedOnTime": true,
    "eventsClean": true,
    "objectsConverged": true,
    "dependenciesReady": true,
    "virtImpactPassed": true,
    "gateResults": [
      { "name": "ClusterOperatorsHealthy", "status": "True", "reason": "AllClusterOperatorsHealthy" },
      { "name": "AllChangesApplied", "status": "True", "reason": "AllClustersInSync" },
      { "name": "HealthCheckPassed", "status": "True", "reason": "AllWorkloadsHealthy" },
      { "name": "MCPUpdatedOnTime", "status": "True", "reason": "MachineConfigPoolConverged" },
      { "name": "EventsClean", "status": "True", "reason": "NoNewWarningEvents" },
      { "name": "ObjectsConverged", "status": "True", "reason": "AllObjectsConverged" },
      { "name": "DependenciesReady", "status": "True", "reason": "AllDependenciesReady" },
      { "name": "VirtImpactPassed", "status": "True", "reason": "VirtualizationPlatformHealthy" }
    ],
    "issuesFound": [],
    "passed": true
  }
}
```
