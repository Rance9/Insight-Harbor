# Insight Harbor — Claude Code MCP Workflow

## Overview

This document explains how to use **Claude Code CLI** with the **Model Context Protocol (MCP)** filesystem server to iterate rapidly on the HTML dashboard without manually editing code.

Claude Code reads the project files directly via MCP, understands the codebase, and generates precise targeted edits based on natural-language instructions. This removes the "copy-paste code" bottleneck and lets you describe the visual change you want in plain English.

---

## 1. Prerequisites

### Install Claude Code CLI

```bash
npm install -g @anthropic-ai/claude-code
```

Authenticate:
```bash
claude login
```

> Claude Code requires an Anthropic API key (set via environment variable or interactive login).

### Verify MCP filesystem server

The filesystem MCP server is built into Claude Code. When you run `claude` from the project root, it automatically gets read/write access to all files in the current directory.

---

## 2. Starting a Session

Always run Claude Code from the Insight Harbor project root:

```cmd
cd "C:\Users\bmiddendorf\OneDrive - Microsoft\Documents\Copilot Analytics Team\Aggregated Copilot Analytics\Insight Harbor"
claude
```

Claude Code will:
1. Read the project structure
2. Index dashboard files for context
3. Open an interactive REPL where you describe what to build or change

---

## 3. Prompt Templates for Dashboard Enhancements

Use these as starting points. Copy into the Claude Code REPL and adjust specifics.

---

### Prompt 0 — Project Context (Run First)

```
Read the following files to understand the Insight Harbor dashboard:
- dashboard/html/index.html
- docs/powerbi-setup-guide.md (the "Export for Dashboard" section)
- transform/schema/silver_purview_schema.md (the Silver schema columns)

Confirm you understand:
1. The dashboard uses Chart.js and reads from either a local data.json or the Azure Functions API
2. The Silver layer has 36 columns including UsageDate, UsageHour, PromptType, IsAgent, Department, Workload, Country, AgentName
3. The API endpoints are /api/summary, /api/trend, /api/department, /api/workload
```

---

### Prompt 1 — Add an Hourly Heatmap

```
In dashboard/html/index.html, add a new chart section below the department table.
The chart should be a heatmap showing prompt volume by hour of day vs day of week.

Requirements:
- Use Chart.js matrix plugin (add CDN link if needed)
- X-axis: hours 0-23
- Y-axis: Mon-Sun
- Color intensity: prompt count (darker = more prompts)
- Data source: when DATA_SOURCE is "api", fetch from /api/workload and compute
  hour/day from the trend data; when "file", read hourly data from data.json if present
- Keep the existing dark theme (--bg, --surface, --accent CSS variables)
- Add a chart card with title "Prompt Activity Heatmap"
- Put the chart in a new full-width row below the existing chart-grid-3 section
```

---

### Prompt 2 — Add Department Adoption Gauge Cards

```
In dashboard/html/index.html, add per-department adoption gauge (circular progress rings)
as a new section between the KPI grid and the trend chart.

Requirements:
- One SVG ring per department (top 6 by user count from /api/department)
- Each ring shows % of department users who have used Copilot at least once
- If total users per department is unknown, display user count instead of %
- Color: green > 50%, amber 20-50%, red < 20%
- Layout: horizontal flex, wraps on mobile
- SVG ring animation on load (strokeDasharray transition)
- Keep same dark card design as existing .kpi-card elements
- Do NOT remove or modify the existing KPI grid
```

---

### Prompt 3 — Teams Workload Deep Dive Page

```
In dashboard/html/index.html, add a second tab/page called "Teams Deep Dive".

The dashboard should become tab-aware:
- Tab 1 = "Overview" (all existing content, unchanged)  
- Tab 2 = "Teams Deep Dive" (new content)

Tab nav:
- Horizontal pill-style tabs in the header area
- Active tab has --accent background
- Inactive tabs are transparent with border

Teams Deep Dive content:
- KPI cards: Teams prompts, Teams users, Avg prompts/user in Teams
- Meeting vs chat breakdown (doughnut - filter Workload = "MicrosoftTeams")
- Hour-of-day bar chart for Teams activity (peak hours analysis)
- A "fun fact" card that highlights the busiest Teams hour in natural language:
  e.g. "Peak Teams Copilot usage is 10 AM with 142 prompts/day."

All data should come from filtering the existing API responses, 
not new API endpoints.
```

---

### Prompt 4 — Mobile Responsive Overhaul

```
Review dashboard/html/index.html and make it fully mobile-responsive at 375px width.

Focus areas:
1. KPI grid: reduce to 2 columns at < 600px, 1 column at < 380px
2. Chart cards: chart height should be 160px at < 600px (Chart.js maintains aspect ratio)
3. Header: at < 600px, stack logo and metadata vertically
4. Department table: at < 600px, hide the bar-cell visualization, only show name + number
5. Font sizes: kpi-value should be 1.5rem at < 600px
6. Test with CSS media queries; no JavaScript needed
7. Do NOT change the desktop layout
```

---

### Prompt 5 — Export to PNG Button

```
In dashboard/html/index.html, add an "Export PNG" button in the header area 
(right side, next to the status badge).

Requirements:
- Use html2canvas library (add CDN: https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js)
- When clicked, captures the <main> element as PNG
- Filename: "insight-harbor-<YYYY-MM-DD>.png"
- Button style: matches header aesthetics (var(--surface2) background, var(--border) border)
- Show a brief "Downloading..." text state while capturing
- Works with the current Chart.js charts rendered in canvases
```

---

### Prompt 6 — Wire Up Live API Mode

```
Review dashboard/html/index.html and update the CONFIG block and data-fetching code 
so that switching CONFIG.DATA_SOURCE from "file" to "api" works seamlessly.

Current gaps to fix:
1. When DATA_SOURCE is "api", the department chart needs to use /api/department
   which returns { departments: [...] } vs the file format which has byDepartment as 
   an object — normalize both into the same internal format before rendering
2. When DATA_SOURCE is "api", the workload data comes from /api/workload which
   returns records + pct; the file format has byWorkload as { workloadName: count }
   — normalize both to { workload, records, pct }
3. Add a visible banner at the top of main when DATA_SOURCE is "api" and the
   API is unreachable (CORS error or 5xx) with a "Switch to file mode" helper message
4. Display the API_BASE_URL in the footer when DATA_SOURCE is "api"
```

---

## 4. Iterative Improvement Workflow

Recommended cadence for dashboard sprints:

```
1. Generate synthetic data:
   .\scripts\generate-synthetic.ps1 -RowCount 1000

2. Export to data.json:
   python -c "
   import pandas as pd, json
   df = pd.read_csv('ingestion/output/<latest_csv>')
   # (from powerbi-setup-guide.md export snippet)
   "

3. Open dashboard in browser:
   start dashboard\html\index.html

4. Open Claude Code:
   claude

5. Run Prompt 0 (context), then paste the enhancement prompt you want
   
6. Review the edit in VS Code diff view

7. Hard-refresh the browser (Ctrl+Shift+R) and review the result

8. Accept or ask Claude to refine:
   "Looks good but the bar heights are too small on mobile — can you increase the min-height to 120px?"
```

---

## 5. Tips for Best Results

| Tip | Details |
|---|---|
| **Be specific about selectors** | Reference exact IDs/classes from the HTML (`#trendChart`, `.kpi-grid`) so Claude targets the right element |
| **Specify the constraint** | "Do NOT modify the existing KPI cards" prevents unintended changes |
| **Request incremental diffs** | "Add only what's needed — don't rewrite the whole file" keeps changes reviewable |
| **Test with synthetic data first** | Run `generate-synthetic.ps1` before testing new chart types |
| **Version with git** | `git add -A && git commit -m "dashboard: add heatmap"` after each accepted change |
| **Describe the data shape** | Tell Claude if a field might be null (e.g., `Department` can be empty string) |
| **Request mobile breakpoints explicitly** | Claude won't add responsive CSS unless asked |

---

## 6. MCP Server Configuration (Advanced)

If you want to give Claude Code access to the ADLS contents directly (e.g., to inspect Silver CSV schema):

Create `.mcp.json` in the project root:

```json
{
  "mcpServers": {
    "filesystem": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-filesystem", "."],
      "description": "Insight Harbor project files"
    }
  }
}
```

Then run `claude --mcp-config .mcp.json` to enable. Claude can then read any project file on request, including Silver CSVs if they exist locally in `ingestion/output/`.

---

## 7. Phase 7 Deploy — After Quota Increase

Once Dynamic VM quota in East US 2 is > 0, deploy Phase 7:

```powershell
# Deploy Functions + Static Web App
az deployment group create `
  --resource-group insight-harbor-rg `
  --template-file infrastructure\main-dashboard.bicep `
  --parameters @infrastructure\parameters-dashboard.json

# Publish Functions app
cd dashboard\api
func azure functionapp publish ih-api-poc01

# Deploy Static Web App
# (GitHub Actions workflow file auto-created by Static Web Apps)
# Or manually: az staticwebapp upload ...
```

After deploy, change `dashboard/html/index.html` line:
```js
DATA_SOURCE: "api",           // ← change from "file" to "api"
API_BASE_URL: "https://ih-api-poc01.azurewebsites.net/api",
```

Re-run Claude Code **Prompt 6** to fix any data-format normalization issues.
