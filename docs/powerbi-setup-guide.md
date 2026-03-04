# Insight Harbor — Power BI Setup & DAX Measures Guide

## Overview

This guide walks through connecting Power BI Desktop to the Insight Harbor ADLS Gen2 Silver layer, building the semantic model, and implementing all key DAX measures for M365 Copilot usage analytics.

**Prerequisites:**
- Power BI Desktop (latest)
- At least one full pipeline run completed (Silver layer has data in `silver/copilot-usage/`)
- Azure Storage account key for `ihstoragepoc01` (see [Retrieving the Storage Key](#retrieving-the-storage-key))

---

## 1. Data Connection

### Option A — Direct Import from ADLS Gen2 (Recommended for PoC)

1. **Get Data → Azure → Azure Blob Storage**
2. Account name: `ihstoragepoc01`
3. Authentication: **Account Key**
4. Navigate to: **insight-harbor → silver → copilot-usage**
5. Select all `*.csv` files matching `silver_copilot_usage_*.csv`
6. Transform: **Combine Files** (Power Query auto-detects schema from headers)
7. Verify all 36+ columns are present (see Silver schema below)
8. Click **Close & Apply**

> **Why Import, not DirectQuery?** The Silver CSVs use Cool tier ADLS storage which has higher per-operation costs at query time. Daily import keeps the report fast and costs near zero.

### Retrieving the Storage Key

```powershell
az storage account keys list `
  --account-name ihstoragepoc01 `
  --resource-group insight-harbor-rg `
  --query "[0].value" -o tsv
```

Store the key in: `config\insight-harbor-config.json → adls.accountKey`

---

## 2. Power Query / M Transformations

After combining the Silver files, apply these transforms in Power Query:

```m
// In the Power Query editor (Home > Advanced Editor), ensure these steps exist:

// 1. Parse date columns as Date/DateTime (example for UsageDate):
#"Changed Type" = Table.TransformColumnTypes(
    Source,
    {
        {"UsageDate",           type date},
        {"UsageHour",           Int64.Type},
        {"_LoadedAtUtc",        type datetimezone},
        {"IsAgent",             type logical}
    }
)
```

**Column types to confirm in Power Query:**

| Column | Power Query Type |
|---|---|
| `UsageDate` | Date |
| `UsageHour` | Whole Number |
| `CreationTime` | DateTime |
| `RecordType` | Whole Number |
| `IsAgent` | True/False |
| `_LoadedAtUtc` | DateTime |
| All other text fields | Text |

---

## 3. Date Table (Required for time intelligence)

Create a new calculated table in the model:

```dax
DateTable = 
ADDCOLUMNS(
    CALENDAR(
        DATE(2024, 1, 1),
        TODAY()
    ),
    "Year",             YEAR([Date]),
    "MonthNum",         MONTH([Date]),
    "MonthName",        FORMAT([Date], "MMMM"),
    "MonthShort",       FORMAT([Date], "MMM"),
    "Quarter",          "Q" & QUARTER([Date]),
    "YearMonth",        FORMAT([Date], "YYYY-MM"),
    "YearQuarter",      FORMAT([Date], "YYYY") & " Q" & QUARTER([Date]),
    "WeekNum",          WEEKNUM([Date]),
    "DayOfWeek",        WEEKDAY([Date], 2),
    "DayName",          FORMAT([Date], "dddd"),
    "IsWeekend",        IF(WEEKDAY([Date], 2) >= 6, TRUE(), FALSE()),
    "IsToday",          IF([Date] = TODAY(), TRUE(), FALSE())
)
```

**Mark as Date Table:** Right-click `DateTable` → Mark as date table → Date column.

**Create relationship:** `DateTable[Date]` → `silver_copilot_usage[UsageDate]` (many-to-one)

---

## 4. Core DAX Measures

Create a dedicated **Measures** table (Home → Enter Data → name it `_Measures`, 1 blank column).

### 4.1 Volume Metrics

```dax
// Total audit records in scope
Total Records = 
COUNTROWS(silver_copilot_usage)

// Unique active Copilot users
Active Users = 
DISTINCTCOUNT(silver_copilot_usage[UserId])

// Total prompts sent by users
Total Prompts = 
CALCULATE(
    COUNTROWS(silver_copilot_usage),
    silver_copilot_usage[PromptType] = "Prompt"
)

// Total Copilot responses
Total Responses = 
CALCULATE(
    COUNTROWS(silver_copilot_usage),
    silver_copilot_usage[PromptType] = "Response"
)

// Prompts per active user (utilization intensity)
Prompts Per User = 
DIVIDE([Total Prompts], [Active Users], 0)

// Days in period (for rate calculations)
Days In Period = 
DATEDIFF(
    MIN(silver_copilot_usage[UsageDate]),
    MAX(silver_copilot_usage[UsageDate]),
    DAY
) + 1

// Average daily prompts
Avg Daily Prompts = 
DIVIDE([Total Prompts], [Days In Period], 0)
```

### 4.2 User Adoption

```dax
// Total users in dataset (denominator for adoption %)
Total Users In Dataset = 
CALCULATE(
    DISTINCTCOUNT(silver_copilot_usage[UserId]),
    ALL(silver_copilot_usage)
)

// % of users who have used Copilot at all
Adoption Rate = 
DIVIDE([Active Users], [Total Users In Dataset], 0)

// DAU/MAU ratio (engagement depth — higher is better)
// Requires current month filter context for MAU
MAU = 
CALCULATE(
    DISTINCTCOUNT(silver_copilot_usage[UserId]),
    DATESINPERIOD(DateTable[Date], LASTDATE(DateTable[Date]), -30, DAY)
)

DAU = 
CALCULATE(
    DISTINCTCOUNT(silver_copilot_usage[UserId]),
    DATESINPERIOD(DateTable[Date], LASTDATE(DateTable[Date]), -1, DAY)
)

DAU MAU Ratio = 
DIVIDE([DAU], [MAU], 0)

// New users this period (first prompt vs prior period)
New Users = 
VAR CurrentPeriodUsers = VALUES(silver_copilot_usage[UserId])
VAR PriorPeriodStart   = MINX(ALL(DateTable[Date]), DateTable[Date])
VAR CurrentPeriodStart = MIN(silver_copilot_usage[UsageDate])
RETURN
CALCULATE(
    DISTINCTCOUNT(silver_copilot_usage[UserId]),
    FILTER(
        silver_copilot_usage,
        NOT(silver_copilot_usage[UserId] IN
            CALCULATETABLE(
                VALUES(silver_copilot_usage[UserId]),
                silver_copilot_usage[UsageDate] < CurrentPeriodStart
            )
        )
    )
)
```

### 4.3 Agent & Scenario Analysis

```dax
// Interactions with an Agent (IsAgent = TRUE)
Agent Interactions = 
CALCULATE(
    COUNTROWS(silver_copilot_usage),
    silver_copilot_usage[IsAgent] = TRUE()
)

// % of all interactions that used an agent
Agent Usage Rate = 
DIVIDE([Agent Interactions], [Total Records], 0)

// Distinct agents in use
Active Agents = 
CALCULATE(
    DISTINCTCOUNT(silver_copilot_usage[AgentName]),
    silver_copilot_usage[IsAgent] = TRUE()
)

// Top workload by prompt count (useful for tooltip/card)
Top Workload = 
CALCULATE(
    FIRSTNONBLANK(silver_copilot_usage[Workload], 1),
    TOPN(1,
        SUMMARIZE(silver_copilot_usage, silver_copilot_usage[Workload]),
        [Total Prompts]
    )
)
```

### 4.4 Time Intelligence

```dax
// Month-over-month prompt growth
MoM Growth = 
VAR CurrentMonthPrompts = [Total Prompts]
VAR PriorMonthPrompts = 
    CALCULATE(
        [Total Prompts],
        DATEADD(DateTable[Date], -1, MONTH)
    )
RETURN
DIVIDE(CurrentMonthPrompts - PriorMonthPrompts, PriorMonthPrompts, BLANK())

// Rolling 7-day prompts
Prompts 7D = 
CALCULATE(
    [Total Prompts],
    DATESINPERIOD(DateTable[Date], LASTDATE(DateTable[Date]), -7, DAY)
)

// Rolling 30-day prompts
Prompts 30D = 
CALCULATE(
    [Total Prompts],
    DATESINPERIOD(DateTable[Date], LASTDATE(DateTable[Date]), -30, DAY)
)

// Year-to-date prompts
Prompts YTD = 
CALCULATE([Total Prompts], DATESYTD(DateTable[Date]))

// Same period last year
Prompts SPLY = 
CALCULATE([Total Prompts], SAMEPERIODLASTYEAR(DateTable[Date]))
```

### 4.5 Department & Segment Analysis

```dax
// % of department's users who are active Copilot users
Dept Adoption = 
DIVIDE(
    CALCULATE([Active Users]),
    CALCULATE([Active Users], ALL(silver_copilot_usage[Department])),
    0
)

// Department with highest prompt volume
Top Department = 
CALCULATE(
    FIRSTNONBLANK(silver_copilot_usage[Department], 1),
    TOPN(1,
        SUMMARIZE(silver_copilot_usage, silver_copilot_usage[Department]),
        [Total Prompts]
    )
)
```

### 4.6 Data Freshness

```dax
// Latest data loaded into Silver layer
Data As Of = 
MAX(silver_copilot_usage[_LoadedAtUtc])

// Days since last load (staleness indicator)
Days Since Load = 
DATEDIFF([Data As Of], NOW(), DAY)

// Label: "Fresh" / "Stale" for a card visual
Data Status = 
IF([Days Since Load] <= 1, "Fresh", "Stale (" & [Days Since Load] & " days)")
```

---

## 5. Recommended Visuals & Layout

### Page 1 — Executive Summary

| Visual | Field(s) | Measure(s) |
|---|---|---|
| KPI Card | — | Total Prompts |
| KPI Card | — | Active Users |
| KPI Card | — | Adoption Rate (%) |
| KPI Card | — | Prompts Per User |
| Line Chart | DateTable[YearMonth] | Total Prompts, Total Responses |
| Bar Chart | Workload | Total Prompts |
| Donut | PromptType | Total Records |
| Data Status card | — | Data Status, Data As Of |

### Page 2 — User Adoption

| Visual | Field(s) | Measure(s) |
|---|---|---|
| Area Chart | DateTable[Date] | Active Users, MAU |
| Bar Chart | Department | Active Users, Adoption Rate |
| Scatter | Department X: Adoption Rate Y: Prompts Per User | — |
| Table | Country, Department | Active Users, Total Prompts |
| KPI Card | — | New Users, DAU MAU Ratio |

### Page 3 — Agent & Scenario Intelligence

| Visual | Field(s) | Measure(s) |
|---|---|---|
| KPI Card | — | Agent Usage Rate, Active Agents |
| Bar Chart | AgentName | Total Prompts, Agent Interactions |
| Table | Workload, IsAgent | Total Prompts, Active Users |
| Timeline | DateTable[Date] | Agent Interactions vs Total Prompts |

### Page 4 — Trends

| Visual | Field(s) | Measure(s) |
|---|---|---|
| Line Chart | DateTable[YearMonth] | MoM Growth |
| Ribbon Chart | DateTable[YearMonth] | Total Prompts (by Department) |
| Bar Chart | Hour | Total Prompts (usage by hour of day) |
| Heatmap (Table) | DayName × MonthShort | Total Prompts |

---

## 6. Slicers (Add to All Pages)

- **Date Range** — DateTable[Date] (relative date, e.g., Last 30 days)
- **Department** — silver_copilot_usage[Department]
- **Workload** — silver_copilot_usage[Workload]
- **Country** — silver_copilot_usage[Country]
- **IsAgent** — silver_copilot_usage[IsAgent] (Yes/No toggle)

---

## 7. Scheduled Refresh Setup

After publishing to Power BI Service:

1. **Dataset Settings → Data source credentials**
   - Source: Azure Blob Storage
   - Auth: **Account Key** → paste the `ihstoragepoc01` key
2. **Scheduled Refresh**
   - Enable: On
   - Frequency: **Daily**
   - Time: 09:00 UTC (one hour after Automation runs health check)
3. **Email Notifications**: add your email for refresh failure alerts

---

## 8. Row-Level Security (Optional, PoC skip)

If sharing with department managers who should only see their own department:

```dax
// RLS role: "Department Filter"
[Department] = USERPRINCIPALNAME()
```

Map each Power BI Pro user to their department in Manage Roles. Requires the `Department` column to be populated in Silver (it is, sourced from Entra enrichment).

---

## 9. Export for Dashboard (Phase 7)

To feed the HTML dashboard without Power BI, export Silver data as JSON:

```python
# Add to transform/bronze_to_silver_purview.py or run standalone
import pandas as pd, json

df = pd.read_csv("ingestion/output/silver_copilot_usage_latest.csv")
summary = {
    "totalPrompts":   int(df[df.PromptType == "Prompt"].shape[0]),
    "activeUsers":    int(df.UserId.nunique()),
    "topWorkload":    df.groupby("Workload").size().idxmax(),
    "dataAsOf":       df["_LoadedAtUtc"].max(),
    "byDepartment":   df.groupby("Department").UserId.nunique().to_dict(),
    "byWorkload":     df.groupby("Workload").size().to_dict(),
    "dailyTrend":     df.groupby("UsageDate").size().to_dict()
}
with open("dashboard/html/data.json", "w") as f:
    json.dump(summary, f, indent=2, default=str)
```

This `data.json` is what the Phase 7 Azure Functions API will serve to the HTML dashboard.
