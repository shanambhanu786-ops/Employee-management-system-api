"""EventBridge-scheduled Lambda: runs the CRM SLA escalation sweep every 5 minutes (replaces a cron box)."""
import json
import os
import urllib.request


def handler(event, context):
    headers = {"Content-Type": "application/json"}
    if os.environ.get("CRM_API_KEY"):
        headers["X-API-Key"] = os.environ["CRM_API_KEY"]
    req = urllib.request.Request(f"{os.environ['CRM_URL'].rstrip('/')}/crm/sla/escalate",
                                 data=b"{}", headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        body = json.loads(r.read())
    print(f"escalated {body.get('count', 0)} case(s)")
    return {"count": body.get("count", 0)}
