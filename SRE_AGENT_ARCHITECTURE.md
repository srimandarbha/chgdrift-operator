# Centralized SRE Agent & Spoke Operator Architecture

This document details the topology where the **SRE Agent runs centrally** on the Hub control plane (with LangGraph, GitHub API, LLM, and SQLite DB), while the **`drift-operator` runs on each spoke cluster** as a native Kubernetes Go operator.

---

## 1. System Topology Overview

```
                      ┌──────────────────────────────────────────────────────────┐
                      │              CENTRAL HUB CLUSTER / CONTROL PLANE         │
                      │                                                          │
                      │   [ Central SRE Agent ] (LangGraph + LLM + GitHub API)   │
                      │    - Receives GitHub Webhooks (`main`/`sit` merges)      │
                      │    - Consumes Kafka CHG Events (`CHG0012345`)           │
                      │    - Caches PRs & state in SQLite DB                     │
                      │    - Runs LangGraph AI RCA Engine                        │
                      └────────────────────────────┬─────────────────────────────┘
                                                   │
                ┌──────────────────────────────────┼──────────────────────────────────┐
                │ (Pushes ChangeWindow CRDs        │ (Relays ClusterAppReports        │
                │  via RHACM / GitOps)             │  & Diagnostic Logs back)         │
                ▼                                  ▼                                  ▼
┌───────────────────────────────┐  ┌───────────────────────────────┐  ┌───────────────────────────────┐
│ SPOKE CLUSTER 1 (Baremetal)   │  │ SPOKE CLUSTER 2 (Baremetal)   │  │ SPOKE CLUSTER N (Baremetal)   │
│                               │  │                               │  │                               │
│ [ drift-operator ] (Go)       │  │ [ drift-operator ] (Go)       │  │ [ drift-operator ] (Go)       │
│  - Inspects Argo CD Health    │  │  - Inspects Argo CD Health    │  │  - Inspects Argo CD Health    │
│  - Monitors OpenShift MCP     │  │  - Monitors OpenShift MCP     │  │  - Monitors OpenShift MCP     │
│  - Tails Pod Logs on Failure  │  │  - Tails Pod Logs on Failure  │  │  - Tails Pod Logs on Failure  │
└───────────────────────────────┘  └───────────────────────────────┘  └───────────────────────────────┘
```

---

## 2. Why This Topology (Central Agent + Spoke Operators) Is Ideal

1. **Security & Credential Isolation**: GitHub API tokens, LLM API keys, and S3 storage keys stay **securely on the Central Hub**. Spoke clusters do not need access to GitHub tokens or LLM credentials.
2. **Native Edge Performance**: The Go `drift-operator` runs natively on each spoke cluster with zero-latency access to the local Kubernetes API, OpenShift MCO API (`machineconfiguration.openshift.io/v1`), and pod log subresources.
3. **Heavy AI Workload Offloading**: The Python/LangGraph AI engine and SQLite database run centrally, keeping spoke cluster resource footprints minimal (100 MB RAM).

---

## 3. Role of the Central SRE Agent (Hub)

The **Central SRE Agent** is the brain of the system running on the Hub cluster:

* **Inputs**:
  * **GitHub Webhooks**: Receives `push` events on `main`/`sit` branches when PRs are merged.
  * **GitHub REST API**: Queries release tags (`v2.4.0`), commit SHAs, PR numbers, titles, authors, and changed file paths (`components/...`).
  * **Kafka Topic (`gitops.chg.events`)**: Ingests CHG maintenance windows (`CHG0012345`).
* **Embedded SQLite Database**: Caches GitHub PR metadata to avoid hitting GitHub API rate limits (5,000 req/hr) and checkpoints LangGraph graph states.
* **LangGraph AI RCA Engine**: When a spoke cluster reports `Degraded` health, the Central Agent pulls the tail pod logs, executes the LangGraph workflow, runs LLM Root Cause Analysis against the merged PR diffs, and synthesizes the final report.

---

## 4. Role of the Local Spoke Operator (`drift-operator`)

The **Local Operator** runs natively on every downstream spoke cluster:

* **Inputs**: Local Kubernetes API (`Argo CD` status) and OpenShift MCO API (`MachineConfigPool`).
* **Tasks**:
  1. Reconciles local `ClusterAppReport` objects with `observedRevision`, `syncStatus`, `health`, and `mcpStatus` (`worker`, `virt`).
  2. Detects local pod failures (`CrashLoopBackOff`, `ImagePullBackOff`).
  3. Tails 500 lines of pod logs and writes a local `ClusterAppReport` resource.
  4. Relays status back to the Hub via RHACM (Klusterlet) or direct status sync.

---

## 5. Central LangGraph Workflow (Nodes, Edges, & States)

The **Central Agent** runs the following **LangGraph State Graph**:

```
                    ┌────────────────────────┐
                    │  FetchGitMetadataNode  │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │  CollectSpokeStateNode │
                    └───────────┬────────────┘
                                │
                  ┌─────────────┴─────────────┐
                  │ Conditional Edge: Health? │
                  └──────┬─────────────┬──────┘
           Healthy / InSync    Degraded / OutOfSync
                 │                     │
                 │                     ▼
                 │         ┌─────────────────────────┐
                 │         │ FetchDiagnosticLogsNode │
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
                    │ EmitValidationReportNode│
                    └────────────────────────┘
```

### Central LangGraph State Schema (`CentralAgentState`)

```python
class CentralAgentState(TypedDict):
    # Git & CHG Metadata
    chg_number: str             # e.g., "CHG0012345"
    git_repo: str
    target_branch: str          # e.g., "main" or "sit"
    release_tag: str            # e.g., "v2.4.0"
    expected_revision: str      # e.g., "a1b2c3d9"
    merged_prs: List[dict]      # List of PR titles, authors, changed files

    # Spoke Cluster Statuses (Collected from 100+ Spoke Operators)
    spoke_reports: List[dict]   # List of ClusterAppReport status objects
    failing_clusters: List[str] # Clusters reporting Degraded or OutOfSync

    # Diagnostic & AI Fields
    raw_pod_logs: Dict[str, str] # {cluster_app: "tail log string"}
    llm_rca_summary: str        # 2-line AI RCA summary
    log_s3_url: str             # S3 log URL pointer

    # Report Output
    report_emitted: bool
```

### LangGraph Nodes Explained

1. **`FetchGitMetadataNode`**:
   * Triggered by GitHub `push` webhook on `main`/`sit`. Queries GitHub API (`/releases/tags/{tag}` and `/commits/{sha}/pulls`), populates `merged_prs` and caches in SQLite DB.
2. **`CollectSpokeStateNode`**:
   * Reads `ClusterAppReport` CRDs sent from all Spoke Operators. Checks `syncStatus`, `health`, and OpenShift `mcpStatus`.
3. **`FetchDiagnosticLogsNode`**:
   * Triggered *only* if any spoke cluster reports `Degraded` or `OutOfSync`. Fetches tail pod logs captured by the Spoke Operator.
4. **`LLMDiagnosticAnalyzerNode`**:
   * Triggered *only* if failures exist. Passes pod logs + merged PR file diffs to LLM prompt:
     > *"Analyze these pod logs against merged PR #142 changes. Provide a 2-line root cause statement."*
5. **`EmitValidationReportNode`**:
   * Publishes the final consolidated validation report to Kafka (`gitops.change.validation`).

---

## 6. Central Agent Core Pseudocode

```python
import os
import json
import sqlite3
import requests
from langgraph.graph import StateGraph, END

# Initialize Central SQLite Cache
db = sqlite3.connect("/var/lib/central-agent/cache.db")
db.execute("CREATE TABLE IF NOT EXISTS pr_cache (tag TEXT PRIMARY KEY, pr_json TEXT)")

def fetch_git_metadata_node(state: CentralAgentState) -> CentralAgentState:
    tag = state["release_tag"]
    cursor = db.cursor()
    cursor.execute("SELECT pr_json FROM pr_cache WHERE tag=?", (tag,))
    row = cursor.fetchone()
    
    if row:
        state["merged_prs"] = json.loads(row[0])
    else:
        headers = {"Authorization": f"token {os.getenv('GITHUB_TOKEN')}"}
        res = requests.get(f"https://api.github.com/repos/my-org/gitops-repo/commits/{tag}/pulls", headers=headers)
        prs = res.json()
        state["merged_prs"] = prs
        cursor.execute("INSERT OR REPLACE INTO pr_cache VALUES (?, ?)", (tag, json.dumps(prs)))
        db.commit()
    return state

def collect_spoke_state_node(state: CentralAgentState) -> CentralAgentState:
    # Read ClusterAppReports from all spoke operators via Hub Kube API
    reports = hub_k8s_client.list_cluster_app_reports()
    state["spoke_reports"] = reports
    
    failing = []
    for r in reports:
        if r["spec"]["health"] == "Degraded" or r["spec"]["syncStatus"] == "OutOfSync":
            failing.append(r["spec"]["clusterName"])
    state["failing_clusters"] = failing
    return state

def route_health_check(state: CentralAgentState) -> str:
    if len(state["failing_clusters"]) > 0:
        return "diagnose"
    return "emit_report"

def fetch_diagnostic_logs_node(state: CentralAgentState) -> CentralAgentState:
    # Extract logs captured by spoke operators
    logs = {}
    for r in state["spoke_reports"]:
        if r["spec"]["clusterName"] in state["failing_clusters"]:
            logs[r["spec"]["clusterName"]] = r["spec"].get("tailLogs", [])
    state["raw_pod_logs"] = logs
    return state

def llm_diagnostic_analyzer_node(state: CentralAgentState) -> CentralAgentState:
    prompt = f"""
    You are an expert SRE. Analyze the error logs from failing spoke clusters against merged PR changes:
    PR Changes: {json.dumps(state['merged_prs'])}
    Spoke Pod Logs: {json.dumps(state['raw_pod_logs'])}
    Provide a concise 2-sentence Root Cause Analysis (RCA).
    """
    summary = llm_client.generate(prompt)
    state["llm_rca_summary"] = summary
    return state

def emit_validation_report_node(state: CentralAgentState) -> CentralAgentState:
    report_payload = {
        "chgNumber": state["chg_number"],
        "releaseTag": state["release_tag"],
        "failingClusters": state["failing_clusters"],
        "rcaSummary": state.get("llm_rca_summary", "All checks passed"),
        "status": "Validated" if len(state["failing_clusters"]) == 0 else "ValidationFailed"
    }
    kafka_producer.send("gitops.change.validation", json.dumps(report_payload))
    state["report_emitted"] = True
    return state

# Construct Central LangGraph State Graph
workflow = StateGraph(CentralAgentState)
workflow.add_node("fetch_git_metadata", fetch_git_metadata_node)
workflow.add_node("collect_spoke_state", collect_spoke_state_node)
workflow.add_node("fetch_logs", fetch_diagnostic_logs_node)
workflow.add_node("llm_analyzer", llm_diagnostic_analyzer_node)
workflow.add_node("emit_report", emit_validation_report_node)

# Add Edges
workflow.set_entry_point("fetch_git_metadata")
workflow.add_edge("fetch_git_metadata", "collect_spoke_state")
workflow.add_conditional_edges(
    "collect_spoke_state",
    route_health_check,
    {
        "diagnose": "fetch_logs",
        "emit_report": "emit_report"
    }
)
workflow.add_edge("fetch_logs", "llm_analyzer")
workflow.add_edge("llm_analyzer", "emit_report")
workflow.add_edge("emit_report", END)

central_agent_app = workflow.compile()
```
