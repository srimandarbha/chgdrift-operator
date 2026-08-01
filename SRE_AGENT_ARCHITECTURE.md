# Standalone External SRE Agent & Spoke Operator Architecture

This document details the architecture where the **Central SRE Agent runs on a dedicated external SRE Platform / Management Node**, completely decoupled from the OpenShift RHACM Hub cluster.

---

## 1. System Topology Overview (External Agent Topology)

```
                       ┌─────────────────────────────────────────────────────────────┐
                       │  DEDICATED EXTERNAL SRE PLATFORM (Standalone VM / App Node) │
                       │                                                             │
                       │     [ Central SRE Agent ] (Python / LangGraph AI Engine)    │
                       │      - Receives GitHub Webhooks (`main`/`sit` merges)       │
                       │      - Interacts with GitHub REST API (PRs, Tags, Diffs)    │
                       │      - Consumes Kafka CHG Events (`CHG0012345`)              │
                       │      - Stores PR & State Cache in Embedded SQLite DB        │
                       │      - Executes LangGraph LLM Root Cause Analysis           │
                       │      - Commits Log Evidence to `gitops-evidence-repo`       │
                       │      - Posts Adaptive Cards directly to MS Teams            │
                       └──────────────────────────────┬──────────────────────────────┘
                                                      │
                                                      │ (Communicates via Shared Kafka Bus)
                                                      ▼
                       ┌─────────────────────────────────────────────────────────────┐
                       │                   SHARED KAFKA BUS                          │
                       │  - Ingestion: gitops.chg.events                             │
                       │  - Spoke Telemetry: gitops.spoke.reports                     │
                       │  - Emission: gitops.change.validation                       │
                       └──────────────────────────────┬──────────────────────────────┘
                                                      │
                    ┌─────────────────────────────────┼─────────────────────────────────┐
                    ▼                                 ▼                                 ▼
   ┌────────────────────────────────┐┌────────────────────────────────┐┌────────────────────────────────┐
   │ SPOKE CLUSTER 1 (Baremetal)    ││ SPOKE CLUSTER 2 (Baremetal)    ││ RHACM HUB CLUSTER              │
   │ [ drift-operator ] (Go)        ││ [ drift-operator ] (Go)        ││ [ drift-operator ] (Go)        │
   │  - Inspects Argo CD Health     ││  - Inspects Argo CD Health     ││  - Inspects Local Workloads    │
   │  - Monitors OpenShift MCP      ││  - Monitors OpenShift MCP      ││  (Treated as just Spoke #3)    │
   └────────────────────────────────┘└────────────────────────────────┘└────────────────────────────────┘
```

---

## 2. Benefits of Hosting Agent Externally (Not on RHACM Hub)

1. **Zero Dependency on RHACM Hub Availability**: The SRE Agent runs on a dedicated management VM or application node. Upgrading, rebooting, or failing over the RHACM Hub cluster has zero impact on the SRE Agent.
2. **Centralized Credentials & Zero Secrets on Kubernetes**:
   * The SRE Agent holds the GitHub Token, LLM API Key, and MS Teams Webhook URL on its external node.
   * Neither RHACM nor spoke clusters require GitHub tokens or LLM credentials.
3. **No Heavy Python / LangGraph Workload on OpenShift**:
   * Keeps Python runtime dependencies, LangGraph AI engine, and SQLite databases off OpenShift cluster nodes, preserving cluster CPU/memory quotas.

---

## 3. Communication Channels

1. **GitHub API / Webhooks**:
   * GitHub triggers a `push` webhook to the External SRE Agent on `main`/`sit` merges.
   * The Agent queries GitHub REST API (`/compare` and `/commits/{sha}/pulls`) to extract PR titles, authors, and file diffs.
2. **Kafka Bus**:
   * **`gitops.chg.events`**: ServiceNow / CI-CD sends maintenance window events to the Agent.
   * **`gitops.spoke.reports`**: Spoke Operators (`drift-operator` on Spoke 1, Spoke 2, RHACM) stream local `ClusterAppReport` status snapshots.
3. **Log Evidence Upload (GitHub REST API)**:
   * The External Agent commits diagnostic log files directly to `https://github.com/my-org/gitops-evidence-repo` using its local GitHub token.
4. **Microsoft Teams Webhooks**:
   * The External Agent posts color-coded MS Teams Adaptive Cards directly to `#sre-alerts`.

---

## 4. External Agent LangGraph Workflow & Pseudocode

The **External SRE Agent** executes the following Python LangGraph workflow:

```python
import os
import json
import sqlite3
import requests
from langgraph.graph import StateGraph, END

# Initialize Embedded SQLite Cache on External VM
db = sqlite3.connect("/var/lib/sre-agent/cache.db")
db.execute("CREATE TABLE IF NOT EXISTS pr_cache (tag TEXT PRIMARY KEY, pr_json TEXT)")

def fetch_git_metadata_node(state: dict) -> dict:
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

def collect_spoke_telemetry_node(state: dict) -> dict:
    # Read telemetry messages received from Kafka topic gitops.spoke.reports
    reports = kafka_consumer.read_spoke_reports(state["chg_number"])
    state["spoke_reports"] = reports
    
    failing = []
    for r in reports:
        if r["health"] == "Degraded" or r["syncStatus"] == "OutOfSync":
            failing.append(r["clusterName"])
    state["failing_clusters"] = failing
    return state

def route_health_check(state: dict) -> str:
    if len(state["failing_clusters"]) > 0:
        return "diagnose"
    return "send_teams_alert"

def llm_diagnostic_analyzer_node(state: dict) -> dict:
    prompt = f"""
    You are an expert SRE. Analyze the error logs from failing spoke clusters against merged PR changes:
    PR Changes: {json.dumps(state['merged_prs'])}
    Spoke Pod Logs: {json.dumps(state['spoke_reports'])}
    Provide a concise 2-sentence Root Cause Analysis (RCA).
    """
    summary = llm_client.generate(prompt)
    state["llm_rca_summary"] = summary
    return state

def commit_github_evidence_node(state: dict) -> dict:
    # External Agent commits log file to GitHub evidence repo
    file_path = f"evidence/{state['chg_number']}/failure-summary.log"
    web_url = github_client.commit_file(
        repo="gitops-evidence-repo",
        path=file_path,
        content=json.dumps(state["spoke_reports"]),
        message=f"docs(evidence): Diagnostic evidence for {state['chg_number']}"
    )
    state["evidence_url"] = web_url
    return state

def send_teams_alert_node(state: dict) -> dict:
    card_payload = build_teams_adaptive_card(
        chg_number=state["chg_number"],
        release_tag=state["release_tag"],
        rca_summary=state.get("llm_rca_summary", "All checks passed"),
        evidence_url=state.get("evidence_url", "")
    )
    requests.post(os.getenv("TEAMS_WEBHOOK_URL"), json=card_payload)
    state["alert_sent"] = True
    return state

# Build External Agent LangGraph Workflow
workflow = StateGraph(dict)
workflow.add_node("fetch_git_metadata", fetch_git_metadata_node)
workflow.add_node("collect_spoke_telemetry", collect_spoke_telemetry_node)
workflow.add_node("llm_analyzer", llm_diagnostic_analyzer_node)
workflow.add_node("commit_evidence", commit_github_evidence_node)
workflow.add_node("send_teams_alert", send_teams_alert_node)

workflow.set_entry_point("fetch_git_metadata")
workflow.add_edge("fetch_git_metadata", "collect_spoke_telemetry")
workflow.add_conditional_edges(
    "collect_spoke_telemetry",
    route_health_check,
    {
        "diagnose": "llm_analyzer",
        "send_teams_alert": "send_teams_alert"
    }
)
workflow.add_edge("llm_analyzer", "commit_evidence")
workflow.add_edge("commit_evidence", "send_teams_alert")
workflow.add_edge("send_teams_alert", END)

external_agent_app = workflow.compile()
```
