"""
Insight Harbor — Pipeline Constants
====================================
Activity bundle definitions, record type mappings, service operation maps,
and Copilot SKU references ported from PAX v1.10.7.

Reference: docs/PAX_Purview_Audit_Report.md
"""

from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════════════════
# M365 Usage Activity Bundle (matches PAX $m365UsageActivityBundle)
# ~120 activity types across 12 workload categories
# ═══════════════════════════════════════════════════════════════════════════════

M365_USAGE_ACTIVITY_BUNDLE: list[str] = [
    # ── Exchange (8) ──
    "MailItemsAccessed", "Send", "MailboxLogin",
    "SearchQueryInitiatedExchange", "MoveToDeletedItems",
    "SoftDelete", "HardDelete", "UpdateInboxRules",

    # ── SharePoint / OneDrive Files (11) ──
    "FileAccessed", "FileModified", "FileUploaded",
    "FileDownloaded", "FileDeleted", "FileRenamed",
    "FileMoved", "FileCopied", "FileCheckedOut",
    "FileCheckedIn", "FileRecycled",

    # ── SharePoint / OneDrive Sharing (8) ──
    "SharingSet", "SharingInvitationCreated", "SharingInvitationAccepted",
    "AnonymousLinkCreated", "CompanyLinkCreated",
    "SecureLinkCreated", "SharingRevoked", "SharingInheritanceBroken",

    # ── SharePoint Groups (2) ──
    "AddedToGroup", "RemovedFromGroup",

    # ── Teams Management (17+) ──
    "TeamCreated", "TeamDeleted", "TeamSettingChanged",
    "MemberAdded", "MemberRemoved", "MemberRoleChanged",
    "ChannelAdded", "ChannelDeleted", "ChannelSettingChanged",
    "TabAdded", "TabRemoved", "TabUpdated",
    "ConnectorAdded", "ConnectorRemoved", "ConnectorUpdated",
    "BotAddedToTeam", "BotRemovedFromTeam",

    # ── Teams Chat (13+) ──
    "ChatCreated", "ChatUpdated", "ChatRetrieved",
    "MessageSent", "MessageUpdated", "MessageDeleted",
    "MessageRead", "MessageHostedContentRead",
    "SubscribedToMessages", "UnsubscribedFromMessages",
    "MessageCreatedNotification", "MessageDeletedNotification",
    "MessageUpdatedNotification",

    # ── Teams Meetings (14+) ──
    "MeetingStarted", "MeetingEnded", "MeetingJoined",
    "MeetingLeft", "MeetingParticipantDetail",
    "MeetingDetail", "MeetingPolicyUpdated",
    "MeetingRecordingStarted", "MeetingRecordingStopped",
    "MeetingTranscriptionStarted", "MeetingTranscriptionStopped",
    "MeetingRegistrationCreated", "MeetingRegistrationUpdated",
    "MeetingRegistered",

    # ── Teams Apps (5+) ──
    "AppInstalled", "AppUpgraded", "AppUninstalled",
    "AppPermissionGranted", "AppPermissionRevoked",

    # ── Office Apps (5) ──
    "FileAccessedExtended", "FileSyncUploadFull",
    "FileSyncDownloadFull", "FileModifiedExtended",
    "SearchQueryInitiatedSharePoint",

    # ── Microsoft Forms (8) ──
    "FormCreated", "FormUpdated", "FormDeleted",
    "FormViewed", "FormResponseCreated", "FormResponseUpdated",
    "FormResponseDeleted", "FormSummaryViewed",

    # ── Microsoft Stream (4) ──
    "StreamVideoCreated", "StreamVideoUpdated",
    "StreamVideoDeleted", "StreamVideoViewed",

    # ── Planner (8) ──
    "PlanCreated", "PlanModified", "PlanDeleted",
    "PlanCopied", "PlanRead",
    "TaskCreated", "TaskModified", "TaskDeleted",

    # ── Power Apps (5) ──
    "LaunchPowerApp", "PowerAppPermissionEdited",
    "AppLaunched", "GatewayResourceRequestAction",
    "EnvironmentPropertyChange",
]

# CopilotInteraction base type — always included unless ExcludeCopilotInteraction
COPILOT_BASE_ACTIVITY_TYPE = "CopilotInteraction"

# ═══════════════════════════════════════════════════════════════════════════════
# M365 Usage Record Type Bundle (matches PAX $m365UsageRecordBundle)
# 14 record types
# ═══════════════════════════════════════════════════════════════════════════════

M365_USAGE_RECORD_BUNDLE: list[str] = [
    "ExchangeAdmin", "ExchangeItem", "ExchangeMailbox",
    "SharePointFileOperation", "SharePointSharingOperation",
    "SharePoint", "OneDrive",
    "MicrosoftTeams",
    "OfficeNative",
    "MicrosoftForms",
    "MicrosoftStream",
    "PlannerPlan", "PlannerTask",
    "PowerAppsApp",
]

# ═══════════════════════════════════════════════════════════════════════════════
# DSPM for AI Activity Types (matches PAX -IncludeDSPMForAI)
# ═══════════════════════════════════════════════════════════════════════════════

DSPM_AI_ACTIVITY_TYPES: list[str] = [
    "AICompliancePolicyEvent",
    "DlpSensitiveInformationTypeCmdletRecord",
    "SecurityComplianceAlerts",
]

# ═══════════════════════════════════════════════════════════════════════════════
# Record Type → Workload Mapping (matches PAX $recordTypeWorkloadMap)
# Used for service-based query splitting
# ═══════════════════════════════════════════════════════════════════════════════

RECORD_TYPE_WORKLOAD_MAP: dict[str, list[str]] = {
    "ExchangeAdmin": ["Exchange"],
    "ExchangeItem": ["Exchange"],
    "ExchangeMailbox": ["Exchange"],
    "SharePointFileOperation": ["SharePoint", "OneDrive"],
    "SharePointSharingOperation": ["SharePoint", "OneDrive"],
    "SharePoint": ["SharePoint"],
    "OneDrive": ["OneDrive"],
    "MicrosoftTeams": ["MicrosoftTeams"],
    "OfficeNative": [],         # Cross-workload — no service filter
    "MicrosoftForms": [],       # Cross-workload
    "MicrosoftStream": [],      # Cross-workload
    "PlannerPlan": [],          # Cross-workload
    "PlannerTask": [],          # Cross-workload
    "PowerAppsApp": [],         # Cross-workload
}

# ═══════════════════════════════════════════════════════════════════════════════
# Service → Operation Mapping (matches PAX $serviceOperationMap)
# Used to align operationFilters per workload pass
# ═══════════════════════════════════════════════════════════════════════════════

SERVICE_OPERATION_MAP: dict[str, list[str]] = {
    "Exchange": [
        "MailItemsAccessed", "Send", "MailboxLogin",
        "SearchQueryInitiatedExchange", "MoveToDeletedItems",
        "SoftDelete", "HardDelete", "UpdateInboxRules",
    ],
    "SharePoint": [
        "FileAccessed", "FileModified", "FileUploaded",
        "FileDownloaded", "FileDeleted", "FileRenamed",
        "FileMoved", "FileCopied", "FileCheckedOut",
        "FileCheckedIn", "FileRecycled",
        "SharingSet", "SharingInvitationCreated",
        "SharingInvitationAccepted", "AnonymousLinkCreated",
        "CompanyLinkCreated", "SecureLinkCreated",
        "SharingRevoked", "SharingInheritanceBroken",
        "AddedToGroup", "RemovedFromGroup",
        "SearchQueryInitiatedSharePoint",
    ],
    "OneDrive": [
        "FileAccessed", "FileModified", "FileUploaded",
        "FileDownloaded", "FileDeleted", "FileRenamed",
        "FileMoved", "FileCopied", "FileCheckedOut",
        "FileCheckedIn", "FileRecycled",
        "SharingSet", "SharingInvitationCreated",
        "SharingInvitationAccepted", "AnonymousLinkCreated",
        "CompanyLinkCreated", "SecureLinkCreated",
        "SharingRevoked", "SharingInheritanceBroken",
    ],
    "MicrosoftTeams": [
        "TeamCreated", "TeamDeleted", "TeamSettingChanged",
        "MemberAdded", "MemberRemoved", "MemberRoleChanged",
        "ChannelAdded", "ChannelDeleted", "ChannelSettingChanged",
        "TabAdded", "TabRemoved", "TabUpdated",
        "ConnectorAdded", "ConnectorRemoved", "ConnectorUpdated",
        "BotAddedToTeam", "BotRemovedFromTeam",
        "ChatCreated", "ChatUpdated", "ChatRetrieved",
        "MessageSent", "MessageUpdated", "MessageDeleted",
        "MessageRead", "MessageHostedContentRead",
        "MeetingStarted", "MeetingEnded", "MeetingJoined",
        "MeetingLeft", "MeetingParticipantDetail",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# Copilot SKU IDs (matches PAX $script:CopilotSkuIds) — reference only.
# The Python transform uses keyword matching ("copilot" substring) which is
# MORE robust, catching new SKUs automatically.
# ═══════════════════════════════════════════════════════════════════════════════

COPILOT_SKU_IDS: list[str] = [
    "a55e26b6-c4c0-4667-bf8c-0f6e8ec4f1c1",  # Microsoft 365 Copilot
    "639dec6b-bb19-468b-871c-c5c441c4b0cb",  # Microsoft Copilot Studio
    "e946c3cd-ed45-4810-a223-d5f2e1a8076e",  # Copilot for Microsoft 365
    "cfb76ac1-a8da-4643-a65b-92c8751b8d39",  # Copilot for Microsoft 365 (alt)
    "d56f3deb-4993-4b11-aaab-4c84fb4ef9b1",  # Copilot for Microsoft 365 Enterprise
    "b4135cb0-3ced-4243-a038-7c3d4fbae1e8",  # Microsoft 365 Copilot for Finance
    "8c6fbc8c-3ec2-4657-ba29-1522c0370de2",  # Microsoft 365 Copilot for Sales
    "91484d0c-d3db-40aa-a026-be5c8c72ae6d",  # Microsoft 365 Copilot for Service
    "b39b5fa0-e5b2-46d4-8da8-76d800748c11",  # Microsoft 365 Copilot (GCC)
    "7f12cfab-27f5-4877-a5ee-c1e489f06b0f",  # Microsoft Copilot Studio (viral)
]

# ═══════════════════════════════════════════════════════════════════════════════
# Copilot keyword detection (used by Entra transform — superior to fixed IDs)
# ═══════════════════════════════════════════════════════════════════════════════

COPILOT_KEYWORDS: list[str] = ["copilot"]

# License tier priority rules (first match wins)
LICENSE_TIER_RULES: list[tuple[str, str]] = [
    ("copilot",           "Copilot"),
    ("spe_e5",            "E5"),
    ("enterprisepremium", "E5"),
    ("spe_e3",            "E3"),
    ("spe_f1",            "F1/F3"),
    ("spe_f3",            "F1/F3"),
]

# ═══════════════════════════════════════════════════════════════════════════════
# Exit code semantics (matches PAX exit codes)
# ═══════════════════════════════════════════════════════════════════════════════

EXIT_CODE_MAP: dict[str, int] = {
    "completed": 0,           # All partitions completed
    "partial_success": 10,    # Some partitions failed after retries
    "circuit_breaker": 20,    # Circuit breaker tripped
    "failed": 99,             # Total failure
}

# ═══════════════════════════════════════════════════════════════════════════════
# Graph API Version Negotiation
# ═══════════════════════════════════════════════════════════════════════════════

GRAPH_API_VERSIONS: list[str] = ["v1.0", "beta"]

# ═══════════════════════════════════════════════════════════════════════════════
# Entra User Select Fields (for Graph /users endpoint)
# Matches PAX -OnlyUserInfo output columns
# ═══════════════════════════════════════════════════════════════════════════════

ENTRA_USER_SELECT_FIELDS: list[str] = [
    "id",
    "userPrincipalName",
    "displayName",
    "mail",
    "jobTitle",
    "department",
    "employeeType",
    "employeeId",
    "employeeHireDate",
    "officeLocation",
    "city",
    "state",
    "country",
    "postalCode",
    "companyName",
    "employeeOrgData",
    "usageLocation",
    "accountEnabled",
    "userType",
    "createdDateTime",
    "assignedLicenses",
]

# Entra Silver output columns
ENTRA_SILVER_COLUMNS: list[str] = [
    "UserPrincipalName",
    "DisplayName",
    "EntraObjectId",
    "Email",
    "JobTitle",
    "Department",
    "EmployeeType",
    "EmployeeId",
    "HireDate",
    "OfficeLocation",
    "City",
    "State",
    "Country",
    "PostalCode",
    "CompanyName",
    "Division",
    "CostCenter",
    "UsageLocation",
    "AccountEnabled",
    "UserType",
    "AccountCreatedDate",
    "ManagerDisplayName",
    "ManagerUPN",
    "ManagerJobTitle",
    "AssignedLicenses",
    "HasLicense",
    "HasCopilotLicense",
    "LicenseTier",
    "_SnapshotDate",
    "_LoadedAtUtc",
]

# Entra source-to-silver column mapping (case-insensitive matching)
ENTRA_SOURCE_TO_SILVER: dict[str, str] = {
    "userprincipalname":          "UserPrincipalName",
    "displayname":                "DisplayName",
    "id":                         "EntraObjectId",
    "email":                      "Email",
    "mail":                       "Email",
    "jobtitle":                   "JobTitle",
    "department":                 "Department",
    "employeetype":               "EmployeeType",
    "employeeid":                 "EmployeeId",
    "employeehiredate":           "HireDate",
    "officelocation":             "OfficeLocation",
    "city":                       "City",
    "state":                      "State",
    "country":                    "Country",
    "postalcode":                 "PostalCode",
    "companyname":                "CompanyName",
    "employeeorgdata_division":   "Division",
    "employeeorgdata_costcenter": "CostCenter",
    "usagelocation":              "UsageLocation",
    "accountenabled":             "AccountEnabled",
    "usertype":                   "UserType",
    "createddatetime":            "AccountCreatedDate",
    "manager_displayname":        "ManagerDisplayName",
    "manager_userprincipalname":  "ManagerUPN",
    "manager_jobtitle":           "ManagerJobTitle",
    "assignedlicenses":           "AssignedLicenses",
    "haslicense":                 "HasLicense",
}

# ═══════════════════════════════════════════════════════════════════════════════
# Purview Silver — Computed Columns / Enrichment Columns
# ═══════════════════════════════════════════════════════════════════════════════

PURVIEW_COMPUTED_COLUMNS: list[str] = [
    "UsageDate",
    "UsageHour",
    "PromptType",
    "IsAgent",
    "_SourceFile",
    "_LoadedAtUtc",
]

ENTRA_ENRICHMENT_COLUMNS: list[str] = [
    "Department",
    "JobTitle",
    "Country",
    "City",
    "ManagerDisplayName",
    "Division",
    "CostCenter",
    "HasCopilotLicense",
    "LicenseTier",
    "CompanyName",
]

# Numeric columns — cast to int if present
PURVIEW_INT_COLUMNS: set[str] = {
    "TurnNumber", "TokensTotal", "TokensInput", "TokensOutput", "DurationMs"
}

# Deduplication composite key columns
DEDUP_KEY_COLS: tuple[str, ...] = ("RecordId", "Message_Id")

# Silver filenames
SILVER_COPILOT_USAGE_FILENAME = "silver_copilot_usage.csv"
SILVER_ENTRA_USERS_FILENAME = "silver_entra_users.csv"
