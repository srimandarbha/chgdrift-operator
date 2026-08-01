# Autonomous Change-Aware SRE Agent Architecture & Design

This document details the complete technical specification, state management, LangGraph AI workflow, GitHub integration, and rationale for the **Edge SRE Agent** working alongside the `drift-operator`.

---

## 1. Purpose of the SRE Agent

The **SRE Agent** is a lightweight, event-driven edge daemon running on downstream/spoke clusters (or central edge nodes). Its primary goals are:

1. **Change Correlation**: Correlates live Git repository events (`main`/`sit` merges, release tags `v2.4.0`, PR file diffs) with local Kubernetes/OpenShift runtime state.
2. **Health & MCP Monitoring**: Continuously monitors Argo CD application health (`Healthy`, `Degraded`) and OpenShift `MachineConfigPool` (MCP) node rollout state (`Updating`, `Updated`).
3. **Log-First AI Diagnostics**: When a workload fails post-deployment, the Agent extracts tail pod logs, passes them to an LLM for Root Cause Analysis (RCA), and generates concise evidence.
4. **Hub Reporting**: Writes structured `ClusterAppReport` Custom Resources to the central Hub cluster for `drift-operator` aggregation.

---

## 2. Agent Inputs & Data Sources

| Input Source | Channel / API | Data Extracted |
| :--- | :--- | :--- |
| **GitHub Repository** | Webhooks (`push` on `main`/`sit`) & GitHub REST API | Merge commit SHA, Release tag, Merged PR numbers, PR titles, authors, and changed file paths. |
| **Local Kubernetes API** | `k8s.io/client-go` | Argo CD Application `syncStatus`, `health`, pod statuses, restart counts. |
| **OpenShift MCO API** | `machineconfiguration.openshift.io/v1` | `MachineConfigPool` (`worker`, `virt`) `machineCount`, `updatedNodeCount`, `updatingNodeCount`, `degradedNodeCount`. |
| **Pod Subresource API** | Core `v1` `/pods/{name}/log` | Tail 500 lines of container stderr/stdout on `CrashLoopBackOff` or `ImagePullBackOff`. |
| **Local Cache / DB** | Embedded SQLite / PebbleDB | Cached GitHub PR metadata, commit history, and LangGraph checkpoint states. |

---

## 3. Why LLM is Needed in the Agent

### Where LLM is Used
1. **Pod Log Summarization**: Synthesizing 500+ lines of raw container logs into a 2-line root cause (e.g. `"PostgreSQL authentication failed: password mismatch in secret db-credentials"`).
2. **PR Diff vs. Incident Symptom Correlation**: Comparing changed files in the merged PR (e.g. `config/db-schema.yaml`) with the runtime exception to prove causality.
3. **Structured RCA for Kafka Payload**: Producing clean, structured JSON summaries for SRE ChatOps and ticket creation.

### Why LLM is Essential Over Traditional Rules/Regex
* **Regex Brittleness**: Microservice error messages change across framework updates. Hardcoded regex cannot catch unexpected stack traces.
* **Payload Size Constraints**: Raw logs cannot be sent over Kafka to etcd (1.5 MB limit) without causing `RecordTooLargeException`. The LLM compresses 50 KB of log text into a high-density 500-byte diagnostic summary.

---

## 4. Why an Autonomous Agent Over Observability or Ansible Alone?

| System | Primary Role | Why It Cannot Replace the Agent |
| :--- | :--- | :--- |
| **Observability Alone** *(Splunk, Datadog, Prometheus)* | Passive metrics/log storage. | Observability tools are **passive sinks**. They store logs but do not know *which* Git PR or CHG maintenance window caused an anomaly, nor can they generate structured Kube CRDs or execute targeted GitOps refresh actions. |
| **Ansible Alone** *(AAP / AWX)* | Imperative playbook execution. | Ansible is **polling-based and imperative**. Running Ansible playbooks continuously against 100+ clusters wastes CPU and network bandwidth. The Agent is **event-driven and autonomous**, acting immediately when a Git PR lands. |
| **Autonomous SRE Agent** | Active event correlation & AI RCA. | Combines Git awareness + local runtime state + LLM RCA + CRD reporting in real-time. |

---

## 5. Embedded Database Requirement

### Does the Agent Require a Database? **YES (Lightweight Embedded DB)**
The Agent uses an embedded, zero-dependency key-value store (e.g., **SQLite** or **PebbleDB**):

* **Purpose 1: GitHub API Rate Limit Protection**: GitHub API enforces a 5,000 requests/hour limit. The DB caches PR metadata, commit SHAs, and release tag details locally.
* **Purpose 2: LangGraph State Persistence**: Persists graph execution state so if the Agent pod restarts mid-reconciliation, it resumes execution without re-fetching Git history or re-running LLM prompts.
* **Purpose 3: Alert Deduplication**: Stores last reported revision timestamps per app to prevent spamming duplicate reports.

---

## 6. GitHub API Integration: Fetching Tags & PRs

### Process Flow for Merges to `main` / `sit`
```
 [ GitHub Webhook: Push event to main/sit ]
                    │
                    ▼
  [ 1. Fetch Release Tag / Target SHA ]
  GET /repos/{owner}/{repo}/releases/tags/{tag}
                    │
                    ▼
  [ 2. Compare Commit Delta against Baseline ]
  GET /repos/{owner}/{repo}/compare/{baseline_sha}...{release_sha}
                    │
                    ▼
  [ 3. List Associated Merged PRs ]
  GET /repos/{owner}/{repo}/commits/{sha}/pulls
```

### Data Mapping Example
From GitHub API responses, the Agent extracts:
- `PR #142`: "Update payment gateway API client version to v2.4" (Author: `dev-user`, Files: `components/payments/kustomization.yaml`)
- `PR #145`: "Increase virt MachineConfig memory limits" (Author: `infra-user`, Files: `groups/virt/mcp.yaml`)

---

## 7. LangGraph Agent Workflow (Nodes, Edges, & States)

The Agent's internal decision engine is built using **LangGraph** (State Graph Architecture).

```
                      ┌────────────────────────┐
                      │      START NODE        │
                      └───────────┬────────────┘
                                  │
                                  ▼
                      ┌────────────────────────┐
                      │  FetchGitMetadataNode  │
                      └───────────┬────────────┘
                                  │
                                  ▼
                      ┌────────────────────────┐
                      │ AssessClusterHealthNode│
                      └───────────┬────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │ Conditional Edge: Health? │
                    └──────┬─────────────┬──────┘
             Healthy / InSync    Degraded / OutOfSync
                   │                     │
                   │                     ▼
                   │         ┌─────────────────────────┐
                   │         │ExtractDiagnosticLogsNode│
                   │         └───────────┬─────────────┘
                   │                     │
                   │                     ▼
                   │         ┌─────────────────────────┐
                   │         │LLMDiagnosticAnalyzerNode│
                   │         └───────────┬─────────────┘
                   │                     │
                   └──────────────┬──────┘
                                  │
                                  ▼
                      ┌────────────────────────┐
                      │EmitClusterAppReportNode│
                      └───────────┬────────────┘
                                  │
                                  ▼
                      ┌────────────────────────┐
                      │        END NODE        │
                      └────────────────────────┘
```

### LangGraph State Schema (`AgentState`)

```python
class AgentState(TypedDict):
    # Git Inputs
    git_repo: str
    target_branch: str          # e.g., "main" or "sit"
    release_tag: str            # e.g., "v2.4.0"
    expected_revision: str      # e.g., "a1b2c3d9"
    merged_prs: List[dict]      # List of PR numbers, titles, changed files

    # Cluster Runtime State
    cluster_name: str           # e.g., "us-east-01"
    app_name: str               # e.g., "svc-payments"
    sync_status: str            # "Synced" | "OutOfSync"
    health_status: str          # "Healthy" | "Progressing" | "Degraded"
    mcp_status: dict            # {updatingNodeCount: 0, degradedNodeCount: 0, phase: "Updated"}

    # Diagnostic & AI Fields
    failing_pods: List[str]
    raw_logs: List[str]         # Tail 500 lines
    llm_summary: str            # 2-line AI RCA summary
    log_ref: str                # S3 log URL pointer

    # Report Output
    report_created: bool
    error_message: str
```

### LangGraph Node Definitions

1. **`FetchGitMetadataNode`**:
   * **Role**: Triggered by GitHub `push` webhook or GitOps poll. Queries GitHub API (`/compare` and `/commits/{sha}/pulls`), populates `merged_prs` and `expected_revision`.
2. **`AssessClusterHealthNode`**:
   * **Role**: Queries local Kubernetes API for Argo CD app health (`syncStatus`, `healthStatus`) and OpenShift `MachineConfigPool` status (`mcp_status`).
3. **`ExtractDiagnosticLogsNode`**:
   * **Role**: Executed *only* if `health_status == "Degraded"`. Finds failing pods in `CrashLoopBackOff`, fetches tail 500 lines, uploads full log to S3 (`log_ref`), and truncates inline log snippet.
4. **`LLMDiagnosticAnalyzerNode`**:
   * **Role**: Executed *only* if `health_status == "Degraded"`. Passes `raw_logs` + `merged_prs` to LLM prompt:
     > *"Analyze these pod logs against merged PR #142 changes. Provide a 2-line root cause statement."*
5. **`EmitClusterAppReportNode`**:
   * **Role**: Constructs and writes the `ClusterAppReport` CRD to the central Hub cluster.

---

## 8. Agent Core Pseudocode

```python
import os
import json
import sqlite3
import requests
from langgraph.graph import StateGraph, END

# Initialize Embedded SQLite DB
db = sqlite3.connect("/var/lib/agent/cache.db")
db.execute("CREATE TABLE IF NOT EXISTS pr_cache (sha TEXT PRIMARY KEY, pr_json TEXT)")

def fetch_git_metadata_node(state: AgentState) -> AgentState:
    tag = state["release_tag"]
    # 1. Check local DB cache
    cursor = db.cursor()
    cursor.execute("SELECT pr_json FROM pr_cache WHERE sha=?", (tag,))
    row = cursor.fetchone()
    
    if row:
        state["merged_prs"] = json.loads(row[0])
    else:
        # 2. Call GitHub API
        headers = {"Authorization": f"token {os.getenv('GITHUB_TOKEN')}"}
        res = requests.get(f"https://api.github.com/repos/my-org/gitops-repo/commits/{tag}/pulls", headers=headers)
        prs = res.json()
        state["merged_prs"] = prs
        cursor.execute("INSERT OR REPLACE INTO pr_cache VALUES (?, ?)", (tag, json.dumps(prs)))
        db.commit()
    return state

def assess_cluster_health_node(state: AgentState) -> AgentState:
    # Query local Kube API for Argo CD App & OpenShift MCP
    app_info = k8s_client.get_argo_app(state["app_name"])
    mcp_info = k8s_client.get_mcp_status("virt")
    
    state["sync_status"] = app_info.status.sync.status          # Synced | OutOfSync
    state["health_status"] = app_info.status.health.status      # Healthy | Degraded
    state["mcp_status"] = mcp_info                              # {updating: 0, degraded: 0}
    return state

def route_health_check(state: AgentState) -> str:
    if state["health_status"] == "Degraded" or state["sync_status"] == "OutOfSync":
        return "diagnose"
    return "emit_report"

def extract_diagnostic_logs_node(state: AgentState) -> AgentState:
    pod = k8s_client.get_failing_pod(state["app_name"])
    raw_logs = k8s_client.get_pod_logs(pod.name, tail_lines=500)
    
    # Upload full log to S3 evidence bucket
    s3_url = s3_client.upload(f"logs/{state['app_name']}.log", raw_logs)
    state["log_ref"] = s3_url
    state["raw_logs"] = raw_logs[:20] # Keep max 20 inline lines
    return state

def llm_diagnostic_analyzer_node(state: AgentState) -> AgentState:
    prompt = f"""
    You are an expert SRE. Analyze the error logs below against PR changes:
    PR Changes: {json.dumps(state['merged_prs'])}
    Pod Error Logs: {state['raw_logs']}
    Provide a concise 2-sentence Root Cause Analysis (RCA).
    """
    summary = llm_client.generate(prompt)
    state["llm_summary"] = summary
    return state

def emit_cluster_app_report_node(state: AgentState) -> AgentState:
    report_crd = {
        "apiVersion": "gitops.example.com/v1alpha1",
        "kind": "ClusterAppReport",
        "metadata": {"name": f"{state['cluster_name']}-{state['app_name']}", "namespace": "gitops-fleet"},
        "spec": {
            "clusterName": state["cluster_name"],
            "appName": state["app_name"],
            "observedRevision": state["expected_revision"],
            "syncStatus": state["sync_status"],
            "health": state["health_status"],
            "mcpStatus": state["mcp_status"],
            "observedAt": now_iso()
        }
    }
    hub_k8s_client.apply(report_crd)
    state["report_created"] = True
    return state

# Construct LangGraph State Graph
workflow = StateGraph(AgentState)
workflow.add_node("fetch_git_metadata", fetch_git_metadata_node)
workflow.add_node("assess_cluster_health", assess_cluster_health_node)
workflow.add_node("extract_logs", extract_diagnostic_logs_node)
workflow.add_node("llm_analyzer", llm_diagnostic_analyzer_node)
workflow.add_node("emit_report", emit_cluster_app_report_node)

# Add Edges
workflow.set_entry_point("fetch_git_metadata")
workflow.add_edge("fetch_git_metadata", "assess_cluster_health")
workflow.add_conditional_edges(
    "assess_cluster_health",
    route_health_check,
    {
        "diagnose": "extract_logs",
        "emit_report": "emit_report"
    }
)
workflow.add_edge("extract_logs", "llm_analyzer")
workflow.add_edge("llm_analyzer", "emit_report")
workflow.add_edge("emit_report", END)

agent_app = workflow.compile()
```
