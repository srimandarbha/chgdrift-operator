# Microsoft Teams Integration Guide (Adaptive Cards)

This document details how to route post-deployment validation reports and LLM Root Cause Analysis (RCA) directly into a **Microsoft Teams Channel** via an **Incoming Webhook** using **Adaptive Cards**, eliminating or supplementing Kafka emission topics.

---

## 1. Architecture Overview

```
 ┌──────────────────────────────────────────────────────────┐
 │                  CENTRAL SRE AGENT                      │
 │        (LangGraph + LLM Root Cause Analysis)             │
 └────────────────────────────┬─────────────────────────────┘
                              │
                              ▼ (HTTP POST JSON Adaptive Card)
   [ Microsoft Teams Webhook: https://company.webhook.office.com/... ]
                              │
                              ▼
            ┌───────────────────────────────────┐
            │   MS Teams Channel: #sre-alerts   │
            │   - Color-Coded Status Badge      │
            │   - 6 Health Check Results        │
            │   - 2-Sentence LLM AI Summary     │
            │   - Action Buttons (Logs & CHG)   │
            └───────────────────────────────────┘
```

---

## 2. Setting Up Microsoft Teams Webhook

1. Open your Microsoft Teams Channel (e.g., `#sre-alerts` or `#gitops-deployments`).
2. Click **`...` (More Options)** -> **Connectors** (or **Workflows**).
3. Search for **Incoming Webhook** -> Click **Add** -> Name it `SRE Drift Bot`.
4. Copy the generated Webhook URL:
   `https://company.webhook.office.com/webhookb2/abc123xyz...`

---

## 3. Microsoft Teams Adaptive Card Payload Schema

When a ChangeWindow completes validation or fails, the Central SRE Agent formats and HTTP POSTs the following **Adaptive Card v1.4 JSON** to the Teams Webhook:

```json
{
  "type": "message",
  "attachments": [
    {
      "contentType": "application/vnd.microsoft.card.adaptive",
      "contentUrl": null,
      "content": {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
          {
            "type": "TextBlock",
            "size": "Large",
            "weight": "Bolder",
            "text": "🚨 CHG Validation Failed: CHG0012345 (Release v2.4.0)",
            "color": "Attention"
          },
          {
            "type": "FactSet",
            "facts": [
              { "title": "CHG Number:", "value": "CHG0012345" },
              { "title": "Git Release Tag:", "value": "v2.4.0" },
              { "title": "Maintenance Window:", "value": "02:00 UTC - 04:00 UTC" },
              { "title": "Overall Status:", "value": "Degraded" }
            ]
          },
          {
            "type": "TextBlock",
            "weight": "Bolder",
            "text": "🔍 Post-Deployment Health Checks:"
          },
          {
            "type": "TextBlock",
            "text": "❌ Git Revision Sync: Failed (Cluster us-east-02 lagging)\n✅ App Health: Healthy\n❌ MachineConfigPool (virt): Updating 2 nodes past window end\n✅ Agent Silence Check: Passed (No dark clusters)"
          },
          {
            "type": "TextBlock",
            "weight": "Bolder",
            "text": "🤖 AI Root Cause Analysis (LLM Summary):"
          },
          {
            "type": "TextBlock",
            "text": "PR #142 updated memory limits in components/payments/kustomization.yaml. Node worker-03 in us-east-02 failed to drain due to PDB violation on container svc-payments-api.",
            "wrap": true
          }
        ],
        "actions": [
          {
            "type": "Action.OpenUrl",
            "title": "View Full Diagnostic Log (Nexus Raw)",
            "url": "https://nexus.company.com:8081/repository/gitops-evidence/CHG0012345/us-east-02-svc-payments-attempt-1.log"
          },
          {
            "type": "Action.OpenUrl",
            "title": "Open ServiceNow CHG Ticket",
            "url": "https://service-now.company.com/nav_to.do?uri=change_request.do?sysparm_query=number=CHG0012345"
          }
        ]
      }
    }
  ]
}
```

---

## 4. Python Implementation Snippet (Central SRE Agent)

```python
import requests
import json

def send_teams_notification(webhook_url: str, card_payload: dict):
    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, data=json.dumps(card_payload), headers=headers)
    if response.status_code == 200:
        print("Successfully delivered validation report to MS Teams!")
    else:
        print(f"Failed to post to Teams: {response.status_code} - {response.text}")
```
