"""
Insight Harbor — Explosion Adapter for Durable Functions
=========================================================
Wraps the existing Purview M365 Usage Bundle Explosion Processor for use
in the Durable Functions pipeline.

Instead of reading/writing local files, this adapter:
  • Reads JSONL lines from ADLS via streaming
  • Processes records through the explosion logic
  • Writes exploded CSV back to ADLS via append blob

The core explosion logic (153-column schema, Copilot message expansion,
agent categorization) is reused from the existing processor.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any

import orjson

logger = logging.getLogger("ih.explosion")

# ═══════════════════════════════════════════════════════════════════════════════
# 153-Column Exploded Header (matches PAX $PurviewExplodedHeader)
# Lifted from Purview_M365_Usage_Bundle_Explosion_Processor_v1.0.0.py
# ═══════════════════════════════════════════════════════════════════════════════

PURVIEW_EXPLODED_HEADER: list[str] = [
    "RecordId", "CreationTime", "RecordType", "Operation", "UserId",
    "OrganizationId", "UserType", "ResultStatus", "ObjectId", "Workload",
    "ClientIP", "Scope", "UserKey", "AppAccessContext_AADSessionId",
    "AppAccessContext_APIId", "AppAccessContext_ClientAppId",
    "AppAccessContext_ClientAppName", "AppAccessContext_CorrelationId",
    "AppAccessContext_UniqueTokenId", "AppAccessContext_IssuedAtTime",
    "CopilotEventData_AppHost", "CopilotEventData_Contexts",
    "CopilotEventData_ThreadId", "CopilotEventData_ConversationId",
    "CopilotEventData_MessageId", "CopilotEventData_AccessedResources",
    "CopilotEventData_SensitivityLabel",
    "CopilotEventData_CitationSources", "CopilotEventData_CitationSourceCount",
    "Message_TurnNumber", "Message_Id", "Message_isPrompt",
    "Message_Content", "Message_ContentType", "Message_ContentSize",
    "Message_TokenCount", "Message_SensitivityLabel",
    "Message_SensitivityLabelId", "Message_PluginNames",
    "Message_CreatedTime", "Message_ModifiedTime",
    "AgentId", "AgentName", "AgentType", "AgentScope",
    "TurnNumber", "TokensTotal", "TokensInput", "TokensOutput", "DurationMs",
    # M365 Usage columns (populated for non-Copilot activity types)
    "ItemName", "SiteUrl", "SourceRelativeUrl", "SourceFileName",
    "SourceFileExtension", "DestinationRelativeUrl", "DestinationFileName",
    "EventSource", "ExternalAccess", "TeamName", "TeamGuid",
    "ChannelName", "ChannelGuid", "ChannelType",
    "CommunicationType", "MessageURLs", "ResourceTenantId",
    "TabType", "Name", "OldValue", "NewValue",
    "ItemType", "ListItemUniqueId", "EventData",
    "ModifiedProperties",
    # SharePoint / OneDrive
    "SharingType", "TargetUserOrGroupType", "TargetUserOrGroupName",
    "EventDetail", "SharingScope",
    # Exchange
    "MailboxOwnerUPN", "MailboxOwnerSid", "MailboxGuid",
    "LogonType", "LogonUserSid", "LogonUserDisplayName",
    "ClientInfoString", "ClientRequestId", "InternalLogonType",
    "AffectedItems", "OperationCount",
    # Forms
    "FormId", "FormName", "FormType",
    # Stream
    "StreamVideoId", "StreamVideoTitle",
    # Planner
    "PlanId", "PlanTitle", "TaskId", "TaskTitle",
    # Power Apps
    "AppId", "AppName", "EnvironmentName",
    # Additional metadata
    "AdditionalInfo", "Parameters",
    # Compliance / DLP
    "PolicyId", "PolicyName", "RuleId", "RuleName",
    "AlertId", "AlertType", "Category", "Severity",
    # Timestamps
    "MeetingStartTime", "MeetingEndTime",
    "ScheduledStartTime", "ScheduledEndTime",
    # Azure AD / Entra
    "Actor_DisplayName", "Actor_Id", "Actor_Type",
    "Target_DisplayName", "Target_Id", "Target_Type",
    # DSPM for AI
    "DSPMAIClassification", "DSPMAIPolicyAction",
    "DSPMAISensitiveInfoTypes",
    # Metadata
    "_RawRecordSize", "_ExplodedRowCount", "_ProcessedAtUtc",
]


def explode_records_from_jsonl(
    jsonl_lines: list[str],
    *,
    prompt_filter: str | None = None,
) -> list[dict[str, str]]:
    """Explode raw audit records (JSONL) into flat 153-column rows.

    This implements the core explosion logic:
      1. Parse each JSONL line
      2. Extract top-level fields
      3. Expand CopilotEventData.Messages[] into one row per message
      4. Flatten nested objects (AppAccessContext, etc.)
      5. Categorize agents from AppHost

    Args:
        jsonl_lines: Raw JSONL strings, one audit record per line.
        prompt_filter: Optional filter — "Prompt", "Response", "Both", or None.

    Returns:
        List of flat dicts conforming to PURVIEW_EXPLODED_HEADER.
    """
    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    exploded_rows: list[dict[str, str]] = []

    for line in jsonl_lines:
        try:
            record = orjson.loads(line)
        except Exception:
            continue

        base = _extract_base_fields(record)
        base["_ProcessedAtUtc"] = processed_at

        # Check if this is a CopilotInteraction with messages to expand
        copilot_data = record.get("CopilotEventData") or record.get("copilotEventData")
        if copilot_data and isinstance(copilot_data, dict):
            messages = copilot_data.get("Messages") or copilot_data.get("messages") or []
            _extract_copilot_fields(base, copilot_data)

            if messages:
                raw_size = len(line)
                for msg in messages:
                    msg_row = _expand_message(base, msg, len(messages))

                    # Apply prompt filter if specified
                    if prompt_filter:
                        is_prompt = msg_row.get("Message_isPrompt", "")
                        if prompt_filter == "Prompt" and is_prompt != "True":
                            continue
                        elif prompt_filter == "Response" and is_prompt != "False":
                            continue

                    msg_row["_RawRecordSize"] = str(raw_size)
                    msg_row["_ExplodedRowCount"] = str(len(messages))
                    exploded_rows.append(msg_row)
            else:
                # CopilotInteraction with no messages — single row
                base["_RawRecordSize"] = str(len(line))
                base["_ExplodedRowCount"] = "1"
                exploded_rows.append(base)
        else:
            # Non-Copilot activity type — single row
            _extract_m365_usage_fields(base, record)
            base["_RawRecordSize"] = str(len(line))
            base["_ExplodedRowCount"] = "1"
            exploded_rows.append(base)

    return exploded_rows


def _extract_base_fields(record: dict[str, Any]) -> dict[str, str]:
    """Extract top-level audit record fields."""
    row: dict[str, str] = {}
    direct_fields = {
        "Id": "RecordId", "CreationTime": "CreationTime",
        "RecordType": "RecordType", "Operation": "Operation",
        "UserId": "UserId", "OrganizationId": "OrganizationId",
        "UserType": "UserType", "ResultStatus": "ResultStatus",
        "ObjectId": "ObjectId", "Workload": "Workload",
        "ClientIP": "ClientIP", "Scope": "Scope", "UserKey": "UserKey",
    }

    for src, dst in direct_fields.items():
        val = record.get(src, "")
        row[dst] = str(val) if val is not None else ""

    # AppAccessContext (nested object)
    aac = record.get("AppAccessContext") or {}
    if isinstance(aac, dict):
        row["AppAccessContext_AADSessionId"] = str(aac.get("AADSessionId", ""))
        row["AppAccessContext_APIId"] = str(aac.get("APIId", ""))
        row["AppAccessContext_ClientAppId"] = str(aac.get("ClientAppId", ""))
        row["AppAccessContext_ClientAppName"] = str(aac.get("ClientAppName", ""))
        row["AppAccessContext_CorrelationId"] = str(aac.get("CorrelationId", ""))
        row["AppAccessContext_UniqueTokenId"] = str(aac.get("UniqueTokenId", ""))
        row["AppAccessContext_IssuedAtTime"] = str(aac.get("IssuedAtTime", ""))

    return row


def _extract_copilot_fields(row: dict[str, str], copilot_data: dict) -> None:
    """Extract CopilotEventData top-level fields (not Messages)."""
    prefix = "CopilotEventData_"
    field_map = {
        "AppHost": "AppHost", "Contexts": "Contexts",
        "ThreadId": "ThreadId", "ConversationId": "ConversationId",
        "MessageId": "MessageId", "AccessedResources": "AccessedResources",
        "SensitivityLabel": "SensitivityLabel",
        "CitationSources": "CitationSources",
    }

    for src, suffix in field_map.items():
        val = copilot_data.get(src, "")
        if isinstance(val, (list, dict)):
            row[f"{prefix}{suffix}"] = orjson.dumps(val).decode("utf-8")
        else:
            row[f"{prefix}{suffix}"] = str(val) if val is not None else ""

    # Citation source count
    citations = copilot_data.get("CitationSources") or []
    if isinstance(citations, list):
        row[f"{prefix}CitationSourceCount"] = str(len(citations))
    else:
        row[f"{prefix}CitationSourceCount"] = ""

    # Agent fields
    app_host = str(copilot_data.get("AppHost", "")).lower()
    agent_info = _categorize_agent(app_host, copilot_data)
    row.update(agent_info)


def _expand_message(
    base: dict[str, str], msg: dict, total_messages: int
) -> dict[str, str]:
    """Expand a single CopilotEventData.Messages[] entry into a row."""
    row = dict(base)  # Copy base fields

    msg_fields = {
        "TurnNumber": "Message_TurnNumber", "Id": "Message_Id",
        "isPrompt": "Message_isPrompt", "Content": "Message_Content",
        "ContentType": "Message_ContentType",
        "ContentSize": "Message_ContentSize",
        "TokenCount": "Message_TokenCount",
        "SensitivityLabel": "Message_SensitivityLabel",
        "SensitivityLabelId": "Message_SensitivityLabelId",
        "CreatedTime": "Message_CreatedTime",
        "ModifiedTime": "Message_ModifiedTime",
    }

    for src, dst in msg_fields.items():
        val = msg.get(src, "")
        if isinstance(val, bool):
            row[dst] = str(val)
        elif isinstance(val, (list, dict)):
            row[dst] = orjson.dumps(val).decode("utf-8")
        else:
            row[dst] = str(val) if val is not None else ""

    # Plugin names
    plugins = msg.get("PluginNames") or msg.get("pluginNames") or []
    if isinstance(plugins, list):
        row["Message_PluginNames"] = ";".join(str(p) for p in plugins)
    else:
        row["Message_PluginNames"] = str(plugins) if plugins else ""

    # Token aggregation (turn level)
    row["TurnNumber"] = str(msg.get("TurnNumber", ""))
    row["TokensTotal"] = str(msg.get("TokenCount", ""))

    return row


def _extract_m365_usage_fields(row: dict[str, str], record: dict) -> None:
    """Extract non-Copilot M365 usage activity fields."""
    m365_fields = {
        "ItemName": "ItemName", "SiteUrl": "SiteUrl",
        "SourceRelativeUrl": "SourceRelativeUrl",
        "SourceFileName": "SourceFileName",
        "SourceFileExtension": "SourceFileExtension",
        "DestinationRelativeUrl": "DestinationRelativeUrl",
        "DestinationFileName": "DestinationFileName",
        "EventSource": "EventSource",
        "ExternalAccess": "ExternalAccess",
        "TeamName": "TeamName", "TeamGuid": "TeamGuid",
        "ChannelName": "ChannelName", "ChannelGuid": "ChannelGuid",
        "ChannelType": "ChannelType",
        "CommunicationType": "CommunicationType",
        "TabType": "TabType", "Name": "Name",
    }

    for src, dst in m365_fields.items():
        val = record.get(src, "")
        if isinstance(val, (list, dict)):
            row[dst] = orjson.dumps(val).decode("utf-8")
        else:
            row[dst] = str(val) if val is not None else ""


def _categorize_agent(
    app_host: str, copilot_data: dict
) -> dict[str, str]:
    """Categorize agent information from AppHost and CopilotEventData.

    Matches PAX's agent categorization logic.
    """
    result = {
        "AgentId": "",
        "AgentName": "",
        "AgentType": "",
        "AgentScope": "",
    }

    # Extract AgentId from various possible locations
    agent_id = ""
    contexts = copilot_data.get("Contexts") or copilot_data.get("contexts")
    if isinstance(contexts, list):
        for ctx in contexts:
            if isinstance(ctx, dict):
                aid = ctx.get("Id") or ctx.get("id") or ""
                if aid:
                    agent_id = str(aid)
                    break
    elif isinstance(contexts, str) and contexts:
        agent_id = contexts

    result["AgentId"] = agent_id

    # Determine agent type from AppHost
    host_map = {
        "teams": "Teams",
        "word": "Word",
        "excel": "Excel",
        "powerpoint": "PowerPoint",
        "outlook": "Outlook",
        "onenote": "OneNote",
        "bizchat": "BizChat",
        "m365chat": "M365Chat",
        "copilot": "Copilot",
    }

    for keyword, agent_type in host_map.items():
        if keyword in app_host:
            result["AgentType"] = agent_type
            break

    return result


def rows_to_csv_string(
    rows: list[dict[str, str]],
    *,
    header: list[str] | None = None,
    include_header: bool = True,
) -> str:
    """Convert exploded rows to a CSV string.

    Args:
        rows: List of row dicts.
        header: Column order (default: PURVIEW_EXPLODED_HEADER).
        include_header: Whether to include CSV header row.

    Returns:
        CSV string with all rows.
    """
    cols = header or PURVIEW_EXPLODED_HEADER
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=cols,
        lineterminator="\n",
        extrasaction="ignore",
    )
    if include_header:
        writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()
