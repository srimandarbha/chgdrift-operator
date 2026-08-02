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
 │    Local Go Operator Manager     │  <== Runs locally via `make run` or `go run main.go`
 │  - Reads gitops.chg.events       │      against local OpenShift KUBECONFIG
 │  - Creates ChangeWindow CR       │
 │  - Inspects local OpenShift apps │
 │  - Evaluates 8 Validation Gates  │
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

### Linux / macOS (Bash)
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

### Windows (PowerShell)
```powershell
# 1. Point to your local OpenShift lab cluster context
$env:KUBECONFIG="$HOME\.kube\config"
kubectl config set-context --current --namespace=gitops-fleet

# 2. Configure Kafka Brokers & Topic Names
$env:KAFKA_BROKERS="localhost:9092"
$env:KAFKA_INGEST_TOPIC="gitops.chg.events"
$env:KAFKA_EMIT_TOPIC="gitops.change.validation"
$env:OPERATOR_NAMESPACE="gitops-fleet"

# Optional: Disable TLS for local unencrypted testing
$env:KAFKA_CA_FILE=""
```

---

## Step 2: Start the Go Operator Manager Locally

### Linux / macOS (Bash)
```bash
# 1. Install CRDs into your local cluster
make install

# 2. Start the operator manager locally
make run
```

### Windows (PowerShell / Command Prompt)
```powershell
# 1. Apply CRD manifests into your local cluster
kubectl apply -k config/crd

# 2. Run the Go operator binary directly
go run main.go
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

### Option A: Using Python (`simulate_chg.py`) (Cross-Platform / Windows & Linux)
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

Run script:
```powershell
python simulate_chg.py
```

### Option B: Windows PowerShell via Docker Exec (If Kafka runs in Docker)
```powershell
Get-Content simulate_chg.json | docker exec -i kafka-container-name kafka-console-producer --bootstrap-server localhost:9092 --topic gitops.chg.events
```

### Option C: Linux / macOS / WSL (`kcat`)
```bash
kcat -b localhost:9092 -t gitops.chg.events -P simulate_chg.json
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

### Option A: Using Python (`consume_validation.py`) (Windows & Linux)
```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'gitops.change.validation',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for validation reports on gitops.change.validation...")
for message in consumer:
    print(json.dumps(message.value, indent=2))
```

Run consumer:
```powershell
python consume_validation.py
```

### Option B: Windows PowerShell via Docker Exec
```powershell
docker exec -it kafka-container-name kafka-console-consumer --bootstrap-server localhost:9092 --topic gitops.change.validation --from-beginning
```

### Option C: Linux / macOS / WSL (`kcat`)
```bash
kcat -b localhost:9092 -t gitops.change.validation -C -q
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
