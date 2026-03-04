#!/usr/bin/env python3
"""
Insight Harbor — Bronze to Silver Transform: Entra Users
=========================================================
Reads the PAX Entra Users CSV (`EntraUsers_MAClicensing_*.csv`), normalizes columns
to the Silver schema (transform/schema/silver_entra_users_schema.md), computes
derived license fields, and writes the clean Silver dimension table to ADLS Gen2
(or locally for testing).

This is a FULL-REPLACE operation — each run overwrites the previous Silver Entra
file. Unlike the append-based Purview usage transform, Entra user data is a
point-in-time dimension snapshot.

Pipeline position:
    PAX -OnlyUserInfo → [THIS SCRIPT] → Silver Entra Users → JOIN in Purview transform

Usage:
    python bronze_to_silver_entra.py --input <entra_csv> --config <config_json> [options]

Examples:
    # Manual test run (local output only)
    python transform/bronze_to_silver_entra.py \\
        --input ingestion/output/EntraUsers_MAClicensing_20260303_081043.csv \\
        --dry-run

    # Full run with ADLS write
    python transform/bronze_to_silver_entra.py \\
        --input ingestion/output/EntraUsers_MAClicensing_20260303_081043.csv \\
        --config config/insight-harbor-config.json

Requirements:
    pip install azure-storage-blob

Python 3.9+
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_VERSION = "1.0.0"
DEFAULT_CONFIG_PATH = "config/insight-harbor-config.json"

# Silver schema: ordered output columns matching silver_entra_users_schema.md
SILVER_COLUMNS: list[str] = [
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

# Map source CSV column names (case-insensitive) to Silver column names.
# Key = lowercase source column name, Value = Silver column name.
# If a source column has multiple possible casing variants, list them all.
SOURCE_TO_SILVER: dict[str, str] = {
    "userprincipalname":          "UserPrincipalName",
    "displayname":                "DisplayName",
    "id":                         "EntraObjectId",
    "email":                      "Email",
    "mail":                       "Email",           # Graph API name fallback
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

# Copilot SKU detection: case-insensitive substring match
COPILOT_KEYWORDS = ["copilot"]

# License tier priority (first match wins)
LICENSE_TIER_RULES: list[tuple[str, str]] = [
    ("copilot",           "Copilot"),        # HasCopilotLicense = True
    ("spe_e5",            "E5"),
    ("enterprisepremium", "E5"),             # Office 365 E5
    ("spe_e3",            "E3"),
    ("spe_f1",            "F1/F3"),
    ("spe_f3",            "F1/F3"),
]

SILVER_BLOB_NAME = "silver_entra_users.csv"


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _resolve_keyvault_refs(cfg: dict) -> dict:
    """
    Scan config dict for values prefixed with '@KeyVault:' and resolve them
    via Azure Key Vault. Uses azure-keyvault-secrets SDK if available,
    otherwise falls back to 'az keyvault secret show' CLI.
    Requires 'keyVault.vaultName' in config.
    """
    vault_name = cfg.get("keyVault", {}).get("vaultName", "")
    if not vault_name:
        return cfg  # No vault configured — return as-is

    # Collect all @KeyVault: references
    refs: list[tuple[list[str], str]] = []  # (path_to_key, secret_name)

    def _scan(obj, path):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _scan(v, path + [k])
        elif isinstance(obj, str) and obj.startswith("@KeyVault:"):
            refs.append((path, obj[len("@KeyVault:"):]))

    _scan(cfg, [])
    if not refs:
        return cfg

    # Try SDK first, then CLI fallback
    resolved: dict[str, str] = {}
    try:
        from azure.identity import DefaultAzureCredential  # type: ignore
        from azure.keyvault.secrets import SecretClient  # type: ignore
        cred = DefaultAzureCredential()
        client = SecretClient(vault_url=f"https://{vault_name}.vault.azure.net", credential=cred)
        for _, secret_name in refs:
            if secret_name not in resolved:
                resolved[secret_name] = client.get_secret(secret_name).value
                print(f"  Key Vault (SDK): resolved {secret_name}")
    except Exception:
        # Fallback to Azure CLI
        import subprocess
        for _, secret_name in refs:
            if secret_name not in resolved:
                try:
                    result = subprocess.run(
                        ["az", "keyvault", "secret", "show",
                         "--vault-name", vault_name,
                         "--name", secret_name,
                         "--query", "value", "-o", "tsv"],
                        capture_output=True, text=True, check=True
                    )
                    resolved[secret_name] = result.stdout.strip()
                    print(f"  Key Vault (CLI): resolved {secret_name}")
                except Exception as e:
                    print(f"WARNING: Could not resolve Key Vault secret '{secret_name}': {e}",
                          file=sys.stderr)

    # Apply resolved values back into config
    for path, secret_name in refs:
        if secret_name in resolved:
            obj = cfg
            for key in path[:-1]:
                obj = obj[key]
            obj[path[-1]] = resolved[secret_name]

    return cfg


def load_config(config_path: str) -> dict:
    abs_path = os.path.abspath(config_path)
    if not os.path.isfile(abs_path):
        print(f"ERROR: Config file not found: {abs_path}", file=sys.stderr)
        sys.exit(1)
    try:
        with open(abs_path, encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Config file is not valid JSON: {e}", file=sys.stderr)
        sys.exit(1)
    return _resolve_keyvault_refs(cfg)


def build_column_map(csv_columns: list[str]) -> dict[str, str]:
    """
    Build a mapping from actual CSV column names to Silver column names.
    Uses case-insensitive matching against SOURCE_TO_SILVER.
    Returns: dict[actual_csv_column_name, silver_column_name]
    """
    col_map: dict[str, str] = {}
    for csv_col in csv_columns:
        key = csv_col.strip().lower()
        if key in SOURCE_TO_SILVER:
            col_map[csv_col] = SOURCE_TO_SILVER[key]
    return col_map


def parse_bool(val: str) -> str:
    """Normalize boolean-like strings to 'TRUE' or 'FALSE' for Power BI."""
    v = val.strip().upper() if val else ""
    return "TRUE" if v in ("TRUE", "1", "YES") else "FALSE"


def has_copilot_license(assigned_licenses: str) -> bool:
    """Check if any Copilot SKU is present in the assigned licenses string."""
    if not assigned_licenses:
        return False
    lower = assigned_licenses.lower()
    return any(kw in lower for kw in COPILOT_KEYWORDS)


def compute_license_tier(assigned_licenses: str, has_license: bool, has_copilot: bool) -> str:
    """Determine simplified license tier for dashboard grouping."""
    if not has_license:
        return "Unlicensed"
    if has_copilot:
        return "Copilot"

    lower = (assigned_licenses or "").lower()
    for pattern, tier in LICENSE_TIER_RULES:
        if pattern in lower:
            return tier
    return "Other Licensed"


def parse_snapshot_date(filename: str) -> str:
    """
    Extract date from filename pattern: EntraUsers_MAClicensing_YYYYMMDD_HHMMSS.csv
    Returns ISO date string or empty string.
    """
    match = re.search(r"(\d{8})(?:_\d{6})?\.csv$", filename, re.IGNORECASE)
    if match:
        raw = match.group(1)
        try:
            return datetime.strptime(raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ─── ADLS helpers ────────────────────────────────────────────────────────────

def get_adls_client(cfg: dict):
    """Create and return a BlobServiceClient. Returns None if dependencies missing."""
    try:
        from azure.storage.blob import BlobServiceClient  # type: ignore
    except ImportError:
        print("WARNING: azure-storage-blob not installed. ADLS operations disabled.", file=sys.stderr)
        return None

    adls = cfg.get("adls", {})
    account_name = adls.get("storageAccountName", "")
    account_key = adls.get("storageAccountKey", "")

    if not account_name or not account_key:
        print("WARNING: ADLS storageAccountName or storageAccountKey missing in config.", file=sys.stderr)
        return None

    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=account_key)


def upload_csv_to_adls(client, container: str, blob_path: str,
                       rows: list[dict], columns: list[str]) -> bool:
    """Upload a list of row dicts as a CSV to ADLS. Returns True on success."""
    try:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        content = output.getvalue().encode("utf-8")

        blob_client = client.get_blob_client(container=container, blob=blob_path)
        blob_client.upload_blob(content, overwrite=True)
        print(f"  ADLS upload SUCCESS -> {container}/{blob_path}")
        return True
    except Exception as exc:
        print(f"  WARNING: ADLS upload FAILED: {exc}", file=sys.stderr)
        return False


# ─── Transform core ──────────────────────────────────────────────────────────

def transform_row(raw_row: dict, col_map: dict[str, str],
                  snapshot_date: str, loaded_at: str) -> dict | None:
    """
    Map one Entra CSV row to a Silver row.
    Returns None if the row is missing a UserPrincipalName (invalid).
    """
    # Build a Silver row by mapping source columns
    silver_row: dict[str, str] = {col: "" for col in SILVER_COLUMNS}

    for csv_col, silver_col in col_map.items():
        val = raw_row.get(csv_col, "").strip()
        silver_row[silver_col] = val

    # Validate required field
    upn = silver_row.get("UserPrincipalName", "").strip()
    if not upn:
        return None

    # Normalize booleans
    silver_row["AccountEnabled"] = parse_bool(silver_row.get("AccountEnabled", ""))
    has_lic = parse_bool(silver_row.get("HasLicense", ""))
    silver_row["HasLicense"] = has_lic

    # Compute derived license fields
    assigned = silver_row.get("AssignedLicenses", "")
    is_copilot = has_copilot_license(assigned)
    is_licensed = has_lic == "TRUE"

    silver_row["HasCopilotLicense"] = "TRUE" if is_copilot else "FALSE"
    silver_row["LicenseTier"] = compute_license_tier(assigned, is_licensed, is_copilot)

    # Metadata columns
    silver_row["_SnapshotDate"] = snapshot_date
    silver_row["_LoadedAtUtc"] = loaded_at

    return silver_row


def run_transform(args: argparse.Namespace) -> int:
    """Main transform logic. Returns exit code (0 = success)."""
    run_start = datetime.now(timezone.utc)
    loaded_at = run_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Load config ──────────────────────────────────────────────────────────
    cfg = load_config(args.config)

    adls_cfg = cfg.get("adls", {})
    container = adls_cfg.get("containerName", "insight-harbor")
    silver_path_prefix = adls_cfg.get("paths", {}).get("silverEntraUsers", "silver/entra-users")
    silver_blob_path = f"{silver_path_prefix}/{SILVER_BLOB_NAME}"
    output_destination = cfg.get("pax", {}).get("outputDestination", "Local")
    ih_version = cfg.get("solution", {}).get("version", "unknown")

    input_path = os.path.abspath(args.input)
    if not os.path.isfile(input_path):
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        return 1

    source_file = Path(input_path).name
    snapshot_date = parse_snapshot_date(source_file)

    # Local output path
    local_output_dir = os.path.dirname(input_path)
    local_silver_path = os.path.join(local_output_dir, SILVER_BLOB_NAME)

    print(f"\nInsight Harbor — Bronze to Silver Entra Transform v{SCRIPT_VERSION}")
    print(f"  Input:          {input_path}")
    print(f"  Silver:         {silver_blob_path}")
    print(f"  Snapshot date:  {snapshot_date}")
    print(f"  Destination:    {output_destination}")
    print(f"  Dry run:        {args.dry_run}")
    print()

    # ── Read & transform ─────────────────────────────────────────────────────
    print(f"Reading Entra Users CSV: {source_file}")
    silver_rows: list[dict] = []
    skipped_no_upn = 0
    error_count = 0
    seen_upns: set[str] = set()
    dupe_count = 0

    try:
        with open(input_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            csv_columns = reader.fieldnames or []

            # Build column mapping
            col_map = build_column_map(csv_columns)
            mapped_silver_cols = set(col_map.values())
            missing = {"UserPrincipalName", "DisplayName", "Department"} - mapped_silver_cols
            if missing:
                print(f"  WARNING: Key columns not found in source: {missing}", file=sys.stderr)

            print(f"  Source columns:  {len(csv_columns)}")
            print(f"  Mapped to Silver: {len(col_map)}")
            unmapped = [c for c in csv_columns if c not in col_map]
            if unmapped:
                print(f"  Dropped columns: {len(unmapped)} ({', '.join(unmapped[:10])}{'...' if len(unmapped) > 10 else ''})")

            for raw_row in reader:
                try:
                    silver_row = transform_row(raw_row, col_map, snapshot_date, loaded_at)
                    if silver_row is None:
                        skipped_no_upn += 1
                        continue

                    upn = silver_row["UserPrincipalName"].lower()
                    if upn in seen_upns:
                        dupe_count += 1
                        continue
                    seen_upns.add(upn)

                    silver_rows.append(silver_row)
                except Exception as exc:
                    error_count += 1
                    if error_count <= 5:
                        print(f"  WARNING: Row transform error: {exc}", file=sys.stderr)

    except Exception as exc:
        print(f"ERROR: Failed to read input CSV: {exc}", file=sys.stderr)
        return 1

    total_input = len(silver_rows) + skipped_no_upn + dupe_count
    print(f"\n  Input records:     {total_input:,}")
    print(f"  Silver rows:       {len(silver_rows):,}")
    print(f"  Skipped (no UPN):  {skipped_no_upn:,}")
    print(f"  Skipped (dupe):    {dupe_count:,}")
    print(f"  Errors:            {error_count:,}")

    if not silver_rows:
        print("\nNo rows to write.")
        return 0

    # ── Compute stats ────────────────────────────────────────────────────────
    licensed = sum(1 for r in silver_rows if r.get("HasLicense") == "TRUE")
    copilot = sum(1 for r in silver_rows if r.get("HasCopilotLicense") == "TRUE")
    depts = {}
    for r in silver_rows:
        d = r.get("Department", "") or "(blank)"
        depts[d] = depts.get(d, 0) + 1

    print(f"\n  Licensed users:    {licensed:,}")
    print(f"  Copilot licensed:  {copilot:,}")
    print(f"  Unlicensed:        {len(silver_rows) - licensed:,}")
    print(f"  Departments:       {len(depts)}")
    for dept, cnt in sorted(depts.items(), key=lambda x: -x[1])[:8]:
        print(f"    {dept}: {cnt}")

    # ── License tier breakdown ───────────────────────────────────────────────
    tiers: dict[str, int] = {}
    for r in silver_rows:
        t = r.get("LicenseTier", "Unknown")
        tiers[t] = tiers.get(t, 0) + 1
    print(f"\n  License tiers:")
    for tier, cnt in sorted(tiers.items(), key=lambda x: -x[1]):
        print(f"    {tier}: {cnt}")

    # ── Write local Silver CSV ────────────────────────────────────────────────
    if not args.dry_run:
        try:
            with open(local_silver_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=SILVER_COLUMNS,
                                        lineterminator="\n", extrasaction="ignore")
                writer.writeheader()
                writer.writerows(silver_rows)
            print(f"\n  Local Silver written -> {local_silver_path}")
        except Exception as exc:
            print(f"ERROR: Failed to write local Silver CSV: {exc}", file=sys.stderr)
            return 1
    else:
        print(f"\n  [DRY RUN] Would write {len(silver_rows):,} rows to {local_silver_path}")

    # ── Upload to ADLS ────────────────────────────────────────────────────────
    upload_success: bool | None = None
    if not args.dry_run and output_destination == "ADLS":
        adls_client = get_adls_client(cfg)
        if adls_client:
            print("Uploading Silver Entra CSV to ADLS...")
            upload_success = upload_csv_to_adls(
                adls_client, container, silver_blob_path, silver_rows, SILVER_COLUMNS
            )

    # ── Cleanup local artifacts after successful ADLS upload ──────────────────
    # NOTE: silver_entra_users.csv is also used as a local input for Entra
    # enrichment in the downstream purview transform. The orchestrator handles
    # final cleanup of all intermediate files after Stage 3 completes.
    # We only clean the metadata JSON here since it served its purpose.
    if upload_success:
        pass  # Local silver CSV preserved for downstream transform enrichment

    # ── Write transform metadata ──────────────────────────────────────────────
    _write_metadata(run_start, input_path, source_file, silver_blob_path,
                    total_input, len(silver_rows), skipped_no_upn, dupe_count, error_count,
                    args, output_destination, ih_version, upload_success, snapshot_date,
                    licensed, copilot)

    run_end = datetime.now(timezone.utc)
    elapsed = (run_end - run_start).total_seconds()
    print(f"\nTransform complete: {len(silver_rows):,} Silver rows in {elapsed:.2f}s")

    return 1 if error_count > 0 else 0


def _write_metadata(run_start: datetime, input_path: str, source_file: str,
                    silver_blob_path: str, input_records: int, silver_rows: int,
                    skipped_no_upn: int, skipped_dupe: int, errors: int,
                    args: argparse.Namespace, output_destination: str,
                    ih_version: str, upload_success: bool | None,
                    snapshot_date: str, licensed: int, copilot: int) -> None:
    """Write transform_metadata.json alongside the input file."""
    run_end = datetime.now(timezone.utc)
    stem = Path(input_path).stem
    parent = Path(input_path).parent
    meta_path = str(parent / f"{stem}_entra_transform_metadata.json")
    metadata = {
        "step":                 "bronze_to_silver_entra",
        "scriptVersion":        SCRIPT_VERSION,
        "insightHarborVersion": ih_version,
        "runStartUtc":          run_start.isoformat(),
        "runEndUtc":            run_end.isoformat(),
        "elapsedSeconds":       round((run_end - run_start).total_seconds(), 2),
        "inputFile":            input_path,
        "sourceFile":           source_file,
        "snapshotDate":         snapshot_date,
        "inputRecords":         input_records,
        "silverRows":           silver_rows,
        "skippedNoUpn":         skipped_no_upn,
        "skippedDupe":          skipped_dupe,
        "errors":               errors,
        "licensedUsers":        licensed,
        "copilotLicensedUsers": copilot,
        "silverBlobPath":       silver_blob_path,
        "outputDestination":    output_destination,
        "uploadSuccess":        upload_success,
        "dryRun":               args.dry_run,
    }
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"  Metadata written -> {meta_path}")
        # Cleanup metadata file after successful ADLS upload
        if upload_success:
            try:
                os.remove(meta_path)
                print(f"  Cleaned up: {meta_path}")
            except OSError:
                pass
    except Exception as exc:
        print(f"  WARNING: Could not write transform metadata: {exc}", file=sys.stderr)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Insight Harbor — Bronze to Silver Entra Transform v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the PAX Entra Users CSV (EntraUsers_MAClicensing_*.csv).",
    )
    parser.add_argument(
        "--config", "-c",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to insight-harbor-config.json. Default: {DEFAULT_CONFIG_PATH}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Process and report but do not write any output files or upload to ADLS.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {SCRIPT_VERSION}",
    )

    args = parser.parse_args()
    sys.exit(run_transform(args))


if __name__ == "__main__":
    main()
