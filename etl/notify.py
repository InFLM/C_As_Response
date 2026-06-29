"""Microsoft Teams notifications for the ETL pipeline.

Posts an Adaptive Card to a Teams *Workflow* incoming webhook (the modern replacement
for the retired "Incoming Webhook" connector). Configuration is a single env var:

    TEAMS_WEBHOOK_URL   the webhook URL from the Teams Workflow trigger

How to get the URL (no admin/app password needed):
    Teams -> the target channel -> ... -> Workflows -> template
    "Post to a channel when a webhook request is received" -> create.
    It generates an HTTP POST URL; paste it into .env.test as TEAMS_WEBHOOK_URL.

If TEAMS_WEBHOOK_URL is unset, send_notification() is a no-op (just prints a notice),
and any HTTP error is caught and logged, so notifications can never break the pipeline.
"""
import json
import os
import urllib.request


def _post(url: str, payload: dict) -> int:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status


def send_notification(subject: str, body: str) -> None:
    url = os.getenv("TEAMS_WEBHOOK_URL")
    if not url:
        print("Teams not configured (TEAMS_WEBHOOK_URL unset); skipping notification.")
        return

    # Keep the card reasonable: cap long bodies (e.g. tracebacks) to the last 4000 chars.
    if len(body) > 4000:
        body = "...(truncated)...\n" + body[-4000:]

    # One bold heading TextBlock, then one monospace TextBlock per line so newlines
    # (especially in tracebacks) render as actual line breaks in Teams.
    blocks = [{
        "type": "TextBlock",
        "text": subject,
        "weight": "Bolder",
        "size": "Medium",
        "wrap": True,
    }]
    for line in (body.splitlines() or [body]):
        blocks.append({
            "type": "TextBlock",
            "text": line if line else " ",
            "wrap": True,
            "spacing": "None",
            "fontType": "Monospace",
        })

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.4",
                "body": blocks,
            },
        }],
    }

    try:
        status = _post(url, card)
        print(f"Teams notification posted (HTTP {status}).")
    except Exception as exc:
        # Never let a notification failure mask the pipeline's real result.
        print(f"WARNING: failed to post Teams notification: {exc}")
