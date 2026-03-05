"""
Insight Harbor — Notification Activity
========================================
Sends pipeline completion notifications via Teams webhook.
"""

from __future__ import annotations

import logging

import httpx

from pipeline.shared.config import config
from pipeline.shared.models import NotificationPayload

logger = logging.getLogger("ih.activity.notify")


def notify_completion(input_data: dict) -> dict:
    """Send pipeline completion notification to Teams webhook.

    Input:
        {
            "run_id": "...",
            "status": "completed|partial_success|failed",
            "partitions_processed": 16,
            "records_ingested": 12000,
            "records_transformed": 11800,
            "duration_minutes": 4.5,
            "errors": ["Partition 3 failed after 5 retries"],
        }

    Returns:
        {"sent": true, "webhook_status": 200}
    """
    if not config.TEAMS_WEBHOOK_URL:
        logger.info("No Teams webhook configured — skipping notification")
        return {"sent": False, "webhook_status": 0}

    payload = NotificationPayload(**input_data)

    # Build Adaptive Card for Teams
    status_emoji = {
        "completed": "✅",
        "partial_success": "⚠️",
        "failed": "❌",
    }.get(payload.status, "ℹ️")

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"{status_emoji} Insight Harbor Pipeline — {payload.status.replace('_', ' ').title()}",
                            "weight": "bolder",
                            "size": "medium",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Run ID", "value": payload.run_id},
                                {"title": "Status", "value": payload.status},
                                {"title": "Partitions", "value": str(payload.partitions_processed)},
                                {"title": "Records Ingested", "value": f"{payload.records_ingested:,}"},
                                {"title": "Records Transformed", "value": f"{payload.records_transformed:,}"},
                                {"title": "Duration", "value": f"{payload.duration_minutes:.1f} min"},
                            ],
                        },
                    ],
                },
            }
        ],
    }

    # Add errors section if any
    if payload.errors:
        card["attachments"][0]["content"]["body"].append(
            {
                "type": "TextBlock",
                "text": "**Errors:**\n" + "\n".join(f"- {e}" for e in payload.errors[:5]),
                "wrap": True,
                "color": "attention",
            }
        )

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(config.TEAMS_WEBHOOK_URL, json=card)
            logger.info("Teams notification sent: %d", response.status_code)
            return {"sent": True, "webhook_status": response.status_code}
    except Exception as exc:
        logger.warning("Failed to send Teams notification: %s", exc)
        return {"sent": False, "webhook_status": 0, "error": str(exc)}
