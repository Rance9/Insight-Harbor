# Silver Schema: `silver_entra_users`

**Source:** `bronze/entra-users/EntraUsers_MAClicensing_*.csv` (37–47 column output from PAX `-OnlyUserInfo`)
**Output:** `silver/entra-users/silver_entra_users.csv` (full replace per run — point-in-time snapshot)
**Produced by:** `transform/bronze_to_silver_entra.py`

This is the **user dimension table** for Insight Harbor. It is joined to `silver_copilot_usage` on
`UserPrincipalName = UserId` to enrich usage records with organizational context (department, manager,
region, job title) and licensing status. It enables the **Adoption by Department**, **Adoption Rate**,
and **Licensed vs. Active** metrics.

> **Note on source casing:** The PAX Entra CSV uses mixed casing that differs from Graph API property
> names (e.g., `DisplayName` not `displayName`, `Email` not `mail`, `Country` not `country`).
> The Bronze-to-Silver transform handles both casings via case-insensitive column matching.

---

## Schema Definition

| # | Column Name | Data Type | Nullable | Source Column (as-is from CSV) | Description |
|---|---|---|---|---|---|
| 1 | `UserPrincipalName` | string | No | `userPrincipalName` | UPN — primary key and join key to `silver_copilot_usage.UserId`. |
| 2 | `DisplayName` | string | No | `DisplayName` | User's full display name. |
| 3 | `EntraObjectId` | string | No | `id` | Entra ID (GUID). Stable identifier across UPN changes. |
| 4 | `Email` | string | Yes | `Email` | Primary email address (PAX outputs `Email`, not Graph `mail`). |
| 5 | `JobTitle` | string | Yes | `JobTitle` | Job title. Key dimension for role-based adoption analysis. |
| 6 | `Department` | string | Yes | `department` | Department. Primary organizational dimension for dashboards. |
| 7 | `EmployeeType` | string | Yes | `employeeType` | Employment classification (e.g., `Full-Time`, `Contractor`). |
| 8 | `EmployeeId` | string | Yes | `employeeId` | HR employee identifier. |
| 9 | `HireDate` | datetime | Yes | `employeeHireDate` | Employee hire date (ISO 8601). New-hire adoption tracking. |
| 10 | `OfficeLocation` | string | Yes | `officeLocation` | Physical office location name. |
| 11 | `City` | string | Yes | `city` | City. |
| 12 | `State` | string | Yes | `state` | State or province. |
| 13 | `Country` | string | Yes | `Country` | Country. Primary geographic segmentation column. |
| 14 | `PostalCode` | string | Yes | `postalCode` | Postal/ZIP code. |
| 15 | `CompanyName` | string | Yes | `companyName` | Company name (useful in multi-subsidiary tenants). |
| 16 | `Division` | string | Yes | `employeeOrgData_division` | Organizational division (from `employeeOrgData`). |
| 17 | `CostCenter` | string | Yes | `employeeOrgData_costCenter` | Cost center (from `employeeOrgData`). |
| 18 | `UsageLocation` | string | Yes | `usageLocation` | ISO alpha-2 country code for license assignment. |
| 19 | `AccountEnabled` | boolean | No | `accountEnabled` | Whether the account is active. |
| 20 | `UserType` | string | Yes | `userType` | `"Member"` or `"Guest"`. |
| 21 | `AccountCreatedDate` | datetime | Yes | `createdDateTime` | Account creation timestamp. |
| 22 | `ManagerDisplayName` | string | Yes | `manager_displayName` | Direct manager's display name. |
| 23 | `ManagerUPN` | string | Yes | `manager_userPrincipalName` | Direct manager's UPN. Org-hierarchy rollups. |
| 24 | `ManagerJobTitle` | string | Yes | `manager_jobTitle` | Direct manager's job title. |
| 25 | `AssignedLicenses` | string | Yes | `assignedLicenses` | All assigned license SKU IDs (semicolon-delimited, e.g., `SPE_E5`). |
| 26 | `HasLicense` | boolean | No | `HasLicense` | True if user has **any** license assigned. |
| 27 | `HasCopilotLicense` | boolean | No | *Computed* | True if `AssignedLicenses` contains a Copilot SKU (see below). |
| 28 | `LicenseTier` | string | No | *Computed* | Simplified license category for dashboard grouping. |
| 29 | `_SnapshotDate` | date | No | *Computed* | Date this snapshot was taken (from filename timestamp). |
| 30 | `_LoadedAtUtc` | datetime | No | *Computed* | UTC timestamp when this row was processed into Silver. |

---

## Computed Column Definitions

| Column | Logic |
|---|---|
| `HasCopilotLicense` | `True` if `AssignedLicenses` contains any Copilot SKU (case-insensitive substring match for `copilot`); otherwise `False`. See **Copilot SKU Identifiers** below. |
| `LicenseTier` | Derived from `AssignedLicenses` (first match wins, priority order): `"Copilot"` if `HasCopilotLicense`; `"E5"` if contains `SPE_E5`; `"E3"` if contains `SPE_E3`; `"F1/F3"` if contains `SPE_F1`; `"Other Licensed"` if `HasLicense == True`; `"Unlicensed"` if `HasLicense == False`. |
| `_SnapshotDate` | Parsed from source filename: `EntraUsers_MAClicensing_20260303_120000.csv` → `2026-03-03` |
| `_LoadedAtUtc` | `datetime.now(UTC)` at time of Silver transform run |

---

## Copilot SKU Identifiers

The following SKU part-numbers indicate a Microsoft 365 Copilot license:

| SKU Part Number | Description |
|---|---|
| `Microsoft_365_Copilot` | Copilot for Microsoft 365 (standalone add-on) |
| `MICROSOFT_365_COPILOT` | Same, alternate casing |
| `Microsoft_Copilot_Studio` | Copilot Studio license |

> **Note:** SKU identifiers evolve over time. The transform uses case-insensitive substring matching
> for `copilot` against `AssignedLicenses` to future-proof detection.

---

## Source Column Case Normalization

PAX Entra CSV uses mixed casing that differs from Graph API property names. The transform
performs case-insensitive column lookup and maps to the Silver column names above.

| Source Column (as-is from CSV) | Silver Column | Notes |
|---|---|---|
| `userPrincipalName` | `UserPrincipalName` | Consistent camelCase |
| `DisplayName` | `DisplayName` | PascalCase in source |
| `id` | `EntraObjectId` | Renamed for clarity |
| `Email` | `Email` | Source uses `Email`, not Graph `mail` |
| `JobTitle` | `JobTitle` | PascalCase in source |
| `department` | `Department` | camelCase in source |
| `Country` | `Country` | PascalCase in source |
| `accountEnabled` | `AccountEnabled` | camelCase in source |
| `HasLicense` | `HasLicense` | PascalCase in source |

---

## Columns Intentionally Dropped from PAX Output

### Base Columns (always present in PAX output)

| Dropped Column | Reason |
|---|---|
| `givenName`, `surname` | Redundant with `DisplayName` |
| `preferredLanguage` | Not relevant to Copilot adoption analytics |
| `onPremisesSyncEnabled` | AD sync metadata — not an analytics dimension |
| `onPremisesImmutableId` | AD sync metadata |
| `externalUserState` | Guest user state — filterable via `UserType` instead |
| `proxyAddresses_Primary` | Email routing detail — `Email` column is sufficient |
| `proxyAddresses_Count` | Low analytics value |
| `proxyAddresses_All` | PII-heavy, low analytics value |
| `manager_id` | GUID — `ManagerUPN` and `ManagerDisplayName` are more useful |
| `manager_mail` | Redundant with `ManagerUPN` for join purposes |

### Tenant-Specific Extension Columns (present in some tenants with HR integrations)

These columns appear in the PAX output when tenants have HR systems or custom attributes integrated.
They are **dropped by default** since they overlap with standard columns and are often empty:

| Dropped Column | Reason |
|---|---|
| `ManagerID` | Duplicate of `manager_id` (GUID) |
| `BusinessAreaLabel` | Overlaps with `Division`; org-specific |
| `CountryofEmployment` | Redundant with `Country` |
| `CompanyCodeLabel` | Redundant with `CompanyName` |
| `CostCentreLabel` | Redundant with `CostCenter` |
| `UserName` | Duplicate of `DisplayName` |
| `EffectiveDate` | HR-specific; often empty |
| `FunctionType` | HR-specific; often empty |
| `BusinessAreaCode` | HR-specific; often empty |
| `OrgLevel_3Label` | HR-specific; often empty |

> **Note:** The transform silently ignores any unexpected columns, so new extension columns
> added by future PAX versions or HR integrations won't cause errors.

---

## Join Specification

```
silver_copilot_usage  LEFT JOIN  silver_entra_users
    ON  silver_copilot_usage.UserId = silver_entra_users.UserPrincipalName
```

**Direction:** LEFT JOIN from usage → users. Usage rows without a matching user retain null
enrichment fields. This handles service accounts, deleted users, or users not yet synced.

**Enrichment columns added to `silver_copilot_usage`:**
- `Department`
- `JobTitle`
- `Country`
- `City`
- `ManagerDisplayName`
- `Division`
- `CostCenter`
- `HasCopilotLicense`
- `LicenseTier`
- `CompanyName`

---

## Key Metrics Enabled by This Table

| Metric | Formula |
|---|---|
| **Licensed Users** | `COUNTROWS(FILTER(silver_entra_users, HasCopilotLicense = TRUE))` |
| **Adoption Rate** | `Active Users with Copilot Activity / Licensed Users` |
| **Adoption by Department** | `DISTINCTCOUNT(UserId)` grouped by `Department` |
| **Adoption by Region** | `DISTINCTCOUNT(UserId)` grouped by `Country` or `City` |
| **Manager Adoption View** | Usage aggregated by `ManagerDisplayName` |
| **New Hire Ramp** | Activity filtered by `EmployeeHireDate` within last 90 days |

---

## Refresh Cadence

Entra user data is a **point-in-time snapshot** — unlike audit data which is time-range based.
Recommended refresh: **weekly** (or on-demand after org changes like reorgs, license waves).

Each refresh **fully replaces** `silver/entra-users/silver_entra_users.csv` in ADLS.
No append logic — the latest snapshot is always the current truth.
