"""
Insight Harbor — Dashboard Data Exporter
Reads Silver CSV (or synthetic test CSV) and produces dashboard/html/data.json
for the HTML dashboard in file mode (DATA_SOURCE = "file").

Usage:
    python scripts/export-dashboard-data.py --input ingestion/output/synthetic_*.csv
    python scripts/export-dashboard-data.py --input ingestion/output/silver_*.csv

    The glob pattern selects the most recent matching file.
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone

# ── Dependency check ────────────────────────────────────────────────────────
try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is required. Install with: pip install pandas")
    sys.exit(1)


def parse_args():
    p = argparse.ArgumentParser(description="Export Silver CSV to dashboard data.json")
    p.add_argument("--input",  default="ingestion/output/silver_copilot_usage_*.csv",
                   help="Glob pattern for input CSV(s). Most recent match is used.")
    p.add_argument("--output", default="dashboard/html/data.json",
                   help="Output path for data.json")
    p.add_argument("--dry-run", action="store_true",
                   help="Print output JSON without writing")
    return p.parse_args()


def find_latest(pattern: str) -> str:
    matches = glob.glob(pattern)
    if not matches:
        return ""
    return max(matches, key=os.path.getmtime)


def export_data(input_path: str, output_path: str, dry_run: bool):
    print(f"Reading: {input_path}")
    df = pd.read_csv(input_path, low_memory=False)
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")

    # Detect column presence
    has_usage_date  = "UsageDate" in df.columns
    has_prompt_type = "PromptType" in df.columns
    has_is_agent    = "IsAgent" in df.columns
    has_department  = "Department" in df.columns
    has_workload    = "Workload" in df.columns
    has_user_id     = "UserId" in df.columns
    has_loaded_at   = "_LoadedAtUtc" in df.columns

    # Normalise date
    if has_usage_date:
        df["UsageDate"] = pd.to_datetime(df["UsageDate"], errors="coerce")

    # Prompt rows — count "Prompt" type; fall back to "Interaction" if no Prompt/Response split
    if has_prompt_type:
        prompts_df = df[df["PromptType"] == "Prompt"]
        if prompts_df.empty and "Interaction" in df["PromptType"].values:
            # Synthetic or unsplit data — count all interactions as prompts
            prompts_df = df[df["PromptType"] == "Interaction"]
    else:
        prompts_df = df

    total_prompts  = int(len(prompts_df))
    active_users   = int(df["UserId"].nunique()) if has_user_id else 0
    total_records  = int(len(df))
    agent_rate     = float(df["IsAgent"].sum() / len(df)) if has_is_agent and len(df) > 0 else 0.0
    top_workload   = df["Workload"].value_counts().idxmax() if has_workload and not df["Workload"].dropna().empty else None
    data_as_of     = str(df["_LoadedAtUtc"].max()) if has_loaded_at else (
                     str(df["UsageDate"].max()) if has_usage_date else None)

    # Daily trend (last 90 days)
    daily_trend: dict = {}
    if has_usage_date:
        daily = (
            prompts_df
            .dropna(subset=["UsageDate"])
            .groupby(prompts_df["UsageDate"].dt.date)
            .size()
        )
        daily_trend = {str(k): int(v) for k, v in daily.items()}

    # By department
    by_department: dict = {}
    if has_department and has_user_id:
        by_department = df.groupby("Department")["UserId"].nunique().to_dict()
        by_department = {str(k): int(v) for k, v in by_department.items() if k and str(k) != "nan"}

    # By workload
    by_workload: dict = {}
    if has_workload:
        by_workload = df["Workload"].value_counts().to_dict()
        by_workload = {str(k): int(v) for k, v in by_workload.items() if k and str(k) != "nan"}

    output = {
        "totalRecords":   total_records,
        "totalPrompts":   total_prompts,
        "activeUsers":    active_users,
        "topWorkload":    top_workload,
        "agentUsageRate": round(agent_rate, 4),
        "dataAsOf":       data_as_of,
        "generatedAt":    datetime.now(timezone.utc).isoformat(),
        "sourceFile":     os.path.basename(input_path),
        "dailyTrend":     daily_trend,
        "byDepartment":   by_department,
        "byWorkload":     by_workload,
    }

    json_str = json.dumps(output, indent=2, default=str)

    if dry_run:
        print("\n— DRY RUN — data.json would contain:\n")
        print(json_str[:2000])
        print("...")
    else:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(json_str)
        size = os.path.getsize(output_path)
        print(f"  Written: {output_path} ({size:,} bytes)")
        print(f"  totalPrompts={total_prompts:,}  activeUsers={active_users:,}  "
              f"topWorkload={top_workload}  agentRate={agent_rate:.1%}")
        print(f"\n  Open dashboard/html/index.html in your browser to view.")


def main():
    args = parse_args()
    input_path = find_latest(args.input)
    if not input_path:
        print(f"ERROR: No file found matching: {args.input}")
        sys.exit(1)
    export_data(input_path, args.output, args.dry_run)


if __name__ == "__main__":
    main()
