import os
import json
import base64
import logging
from typing import Any, Dict

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
ROUTER_APPROVAL_URL = os.environ.get("ROUTER_APPROVAL_URL", "").rstrip("/")


def post_to_slack_blocks(payload: Dict[str, Any]) -> None:
    if not SLACK_WEBHOOK_URL:
        raise RuntimeError("SLACK_WEBHOOK_URL is not configured")

    resp = requests.post(
        SLACK_WEBHOOK_URL,
        json=payload,
        timeout=30,
    )

    if resp.status_code >= 300:
        raise RuntimeError(f"Slack webhook failed: {resp.status_code} {resp.text}")


def build_slack_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    source = payload.get("source", "unknown")
    event_type = payload.get("event_type", "unknown_event")
    run_id = payload.get("run_id", "n/a")
    period = payload.get("period", "n/a")
    run_type = payload.get("run_type", "n/a")
    reasons = payload.get("reasons", [])

    reason_text = ", ".join(reasons) if reasons else "none"

    action_value_approve = json.dumps({
        "run_id": run_id,
        "decision": "approved"
    })

    action_value_deny = json.dumps({
        "run_id": run_id,
        "decision": "denied"
    })

    return {
        "text": f"Finance Close Approval Required for {run_id}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*Finance Close Approval Required*\n"
                        f"*Source:* {source}\n"
                        f"*Event Type:* {event_type}\n"
                        f"*Run ID:* `{run_id}`\n"
                        f"*Period:* {period}\n"
                        f"*Run Type:* {run_type}\n"
                        f"*Reasons:* {reason_text}"
                    )
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Approve ✅"
                        },
                        "style": "primary",
                        "action_id": "approve_finance_close",
                        "value": action_value_approve
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Deny ❌"
                        },
                        "style": "danger",
                        "action_id": "deny_finance_close",
                        "value": action_value_deny
                    }
                ]
            }
        ]
    }


def send_router_approval(run_id: str, decision: str, reviewer_id: str) -> Dict[str, Any]:
    if not ROUTER_APPROVAL_URL:
        raise RuntimeError("ROUTER_APPROVAL_URL is not configured")

    body = {
        "run_id": run_id,
        "decision": decision,
        "reviewer_id": reviewer_id
    }

    resp = requests.post(
        f"{ROUTER_APPROVAL_URL}/approval-decision",
        json=body,
        timeout=30,
    )

    if resp.status_code >= 300:
        raise RuntimeError(f"Router approval call failed: {resp.status_code} {resp.text}")

    return resp.json()


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/notify")
def notify():
    try:
        payload = request.get_json(silent=True) or {}
        logger.info("Received /notify payload: %s", payload)

        slack_payload = build_slack_message(payload)
        post_to_slack_blocks(slack_payload)

        return jsonify({"status": "sent"}), 200

    except Exception as exc:
        logger.exception("Failed to send Slack message via /notify")
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.post("/pubsub/push")
def pubsub_push():
    try:
        envelope = request.get_json(silent=True) or {}
        logger.info("Received /pubsub/push envelope: %s", envelope)

        if "message" not in envelope:
            return jsonify({"status": "error", "detail": "Invalid Pub/Sub envelope"}), 400

        pubsub_message = envelope["message"]
        data_b64 = pubsub_message.get("data", "")

        if not data_b64:
            return jsonify({"status": "error", "detail": "Missing message.data"}), 400

        decoded = base64.b64decode(data_b64).decode("utf-8")
        payload = json.loads(decoded)

        slack_payload = build_slack_message(payload)
        post_to_slack_blocks(slack_payload)

        return jsonify({"status": "sent"}), 200

    except Exception as exc:
        logger.exception("Failed to process Pub/Sub push")
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.post("/slack/actions")
def slack_actions():
    """
    Slack sends form-encoded payload=... for button clicks.
    """
    try:
        raw_payload = request.form.get("payload")
        if not raw_payload:
            return jsonify({"status": "error", "detail": "Missing Slack payload"}), 400

        payload = json.loads(raw_payload)
        logger.info("Received Slack action payload: %s", payload)

        user = payload.get("user", {})
        reviewer_id = user.get("username") or user.get("id") or "slack_user"

        actions = payload.get("actions", [])
        if not actions:
            return jsonify({"status": "error", "detail": "No actions found"}), 400

        action = actions[0]
        action_id = action.get("action_id")
        value = action.get("value", "{}")
        action_data = json.loads(value)

        run_id = action_data["run_id"]
        decision = action_data["decision"]

        router_result = send_router_approval(
            run_id=run_id,
            decision=decision,
            reviewer_id=reviewer_id
        )

        response_text = (
            f"Decision recorded.\n"
            f"Run ID: {run_id}\n"
            f"Decision: {decision}\n"
            f"Router status: {router_result.get('status')}"
        )

        return jsonify({
            "text": response_text,
            "replace_original": False
        }), 200

    except Exception as exc:
        logger.exception("Failed to process Slack action")
        return jsonify({
            "text": f"Approval action failed: {str(exc)}",
            "replace_original": False
        }), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)