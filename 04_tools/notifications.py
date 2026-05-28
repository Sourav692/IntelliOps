# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — Notification Helpers
# MAGIC
# MAGIC Send alerts to Slack and Microsoft Teams via incoming webhooks.

# COMMAND ----------

import requests
import json

# COMMAND ----------

def send_slack_notification(webhook_url, title, message, color="#FF0000", fields=None):
    """
    Send a Slack notification via incoming webhook.

    Args:
        webhook_url: Slack incoming webhook URL
        title: Alert title
        message: Alert body text
        color: Sidebar color hex (default red for alerts)
        fields: Optional list of {"title": ..., "value": ..., "short": bool}
    """
    if not webhook_url:
        print("  ⏭ Slack notification skipped — no webhook URL configured.")
        return False

    attachment = {
        "color": color,
        "title": f"🤖 IntelliOps: {title}",
        "text": message,
        "footer": "IntelliOps V1 | AI-Powered DataOps",
        "ts": int(__import__("time").time()),
    }
    if fields:
        attachment["fields"] = fields

    payload = {"attachments": [attachment]}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print(f"  ✔ Slack notification sent: {title}")
            return True
        else:
            print(f"  ❌ Slack error {resp.status_code}: {resp.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Slack request failed: {e}")
        return False

# COMMAND ----------

def send_teams_notification(webhook_url, title, message, color="FF0000", facts=None):
    """
    Send a Microsoft Teams notification via incoming webhook.

    Args:
        webhook_url: Teams incoming webhook URL
        title: Card title
        message: Card body text
        color: Theme color hex (no #)
        facts: Optional list of {"name": ..., "value": ...}
    """
    if not webhook_url:
        print("  ⏭ Teams notification skipped — no webhook URL configured.")
        return False

    section = {
        "activityTitle": f"🤖 IntelliOps: {title}",
        "text": message,
    }
    if facts:
        section["facts"] = facts

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": f"IntelliOps: {title}",
        "sections": [section],
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print(f"  ✔ Teams notification sent: {title}")
            return True
        else:
            print(f"  ❌ Teams error {resp.status_code}: {resp.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Teams request failed: {e}")
        return False

# COMMAND ----------

def notify(title, message, severity="warning", details=None):
    """
    Send notification to all configured channels.

    Args:
        title: Alert title
        message: Alert body
        severity: "info" (green), "warning" (yellow), "critical" (red)
        details: Optional dict of key-value pairs for structured fields
    """
    color_map = {"info": "#36A64F", "warning": "#FFA500", "critical": "#FF0000"}
    color = color_map.get(severity, "#FFA500")

    fields = [{"title": k, "value": str(v), "short": True} for k, v in (details or {}).items()]
    facts = [{"name": k, "value": str(v)} for k, v in (details or {}).items()]

    results = []
    if NOTIFICATION_ENABLED:
        if SLACK_WEBHOOK_URL:
            results.append(send_slack_notification(SLACK_WEBHOOK_URL, title, message, color, fields))
        if TEAMS_WEBHOOK_URL:
            results.append(send_teams_notification(TEAMS_WEBHOOK_URL, title, message, color.lstrip("#"), facts))
        if not SLACK_WEBHOOK_URL and not TEAMS_WEBHOOK_URL:
            print(f"  ⏭ No notification channels configured. Message: [{severity.upper()}] {title} — {message}")
    else:
        print(f"  ⏭ Notifications disabled. Message: [{severity.upper()}] {title} — {message}")

    return any(results) if results else False
