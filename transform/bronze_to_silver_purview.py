#!/usr/bin/env python3
"""
Insight Harbor — Bronze to Silver Transform: Copilot Usage
==========================================================
Reads exploded Bronze Purview CSV files, applies the Silver schema defined in
transform/schema/silver_purview_schema.md, computes derived columns, deduplicates
records, and writes the clean Silver layer CSV to ADLS Gen2 (or locally for testing).

Pipeline position:
    PAX Script (RAW) → Python Explosion Processor → [THIS SCRIPT] → Power BI / HTML Dashboard

Usage:
    python bronze_to_silver_purview.py --input <exploded_csv> --config <config_json> [options]

Examples:
    # Manual test run (local output only)
    python transform/bronze_to_silver_purview.py \\
        --input ingestion/output/PAX_Purview_20260302_Exploded.csv \\
        --dry-run

    # Full run with ADLS write
    python transform/bronze_to_silver_purview.py \\
        --input ingestion/output/PAX_Purview_20260302_Exploded.csv \\
        --config config/insight-harbor-config.json

    # Reprocess with overwrite (no dedup against existing Silver)
    python transform/bronze_to_silver_purview.py \\
        --input ingestion/output/PAX_Purview_20260302_Exploded.csv \\
        --overwrite

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
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_VERSION = "1.2.0"
DEFAULT_CONFIG_PATH = "config/insight-harbor-config.json"

# ── Column pass-through strategy ──────────────────────────────────────────
# ALL Bronze columns are preserved in the Silver output (no trimming).
# Computed and Entra-enrichment columns are added on top of whatever the
# exploded Bronze CSV already provides.  The dynamic header is built at
# runtime from the actual input file, so any new columns the explosion
# processor adds in the future flow through automatically.

# Columns the transform adds / overwrites on every row
COMPUTED_COLUMNS: list[str] = [
    "UsageDate",
    "UsageHour",
    "PromptType",
    "IsAgent",
    "_SourceFile",
    "_LoadedAtUtc",
]

# Columns enriched from Entra dimension table
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

# Numeric columns — cast to int if present (empty string otherwise)
INT_COLUMNS = {"TurnNumber", "TokensTotal", "TokensInput", "TokensOutput", "DurationMs"}

# Deduplication composite key
DEDUP_KEY_COLS = ("RecordId", "Message_Id")

# Default Silver output filename in ADLS
SILVER_BLOB_NAME = "silver_copilot_usage.csv"


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


def parse_creation_time(raw: str) -> datetime | None:
    """Parse ISO 8601 / common Purview datetime strings to a UTC datetime."""
    if not raw:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def compute_prompt_type(is_prompt_raw: str) -> str:
    """Map Message_isPrompt value to human-readable PromptType."""
    val = is_prompt_raw.strip().upper() if is_prompt_raw else ""
    if val in ("TRUE", "1", "YES"):
        return "Prompt"
    if val in ("FALSE", "0", "NO"):
        return "Response"
    return "Interaction"


def compute_is_agent(agent_id: str) -> str:
    """Return 'TRUE' or 'FALSE' string for Power BI compatibility."""
    return "TRUE" if (agent_id and agent_id.strip()) else "FALSE"


def safe_int(val: str) -> str:
    """Cast to integer if parseable, else return empty string."""
    if not val or not val.strip():
        return ""
    try:
        return str(int(float(val.strip())))
    except (ValueError, TypeError):
        return ""


def make_dedup_key(row: dict) -> tuple:
    """Build the deduplication composite key for a row."""
    return (
        row.get("RecordId", "").strip(),
        row.get("Message_Id", "").strip(),
    )


# ─── ADLS helpers ────────────────────────────────────────────────────────────

def get_adls_client(cfg: dict):
    """Create and return a BlobServiceClient. Returns None if dependencies missing."""
    try:
        from azure.storage.blob import BlobServiceClient  # type: ignore
    except ImportError:
        print("WARNING: azure-storage-blob not installed. ADLS operations disabled.", file=sys.stderr)
        print("  Install with: pip install azure-storage-blob", file=sys.stderr)
        return None

    adls = cfg.get("adls", {})
    account_name = adls.get("storageAccountName", "")
    account_key   = adls.get("storageAccountKey", "")

    if not account_name or not account_key:
        print("WARNING: ADLS storageAccountName or storageAccountKey missing in config.", file=sys.stderr)
        return None

    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=account_key)


def download_existing_silver(client, container: str, silver_path: str) -> set[tuple]:
    """
    Download the current Silver CSV from ADLS and return a set of existing dedup keys.
    Returns an empty set if the file doesn't exist yet (first run).
    """
    existing_keys: set[tuple] = set()
    try:
        blob_client = client.get_blob_client(container=container, blob=silver_path)
        data = blob_client.download_blob().readall().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(data))
        for row in reader:
            existing_keys.add((
                row.get("RecordId", "").strip(),
                row.get("Message_Id", "").strip(),
            ))
        print(f"  Existing Silver rows: {len(existing_keys):,} (dedup baseline loaded)")
    except Exception:
        print("  No existing Silver file found in ADLS — this will be the first write.")
    return existing_keys


def upload_csv_to_adls(client, container: str, blob_path: str, rows: list[dict], columns: list[str]) -> bool:
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


# ─── Entra enrichment ────────────────────────────────────────────────────────

def load_entra_lookup(entra_path: str | None, adls_client=None,
                      container: str = "", adls_blob: str = "") -> dict[str, dict]:
    """
    Load the Silver Entra users CSV into a lookup dict keyed by lowercase UPN.
    Tries local path first, then ADLS if available.
    Returns: dict[upn_lower] → {Department, JobTitle, Country, ...}
    """
    entra_lookup: dict[str, dict] = {}

    # Try local path first
    if entra_path and os.path.isfile(entra_path):
        print(f"  Loading Entra dimension (local): {entra_path}")
        try:
            with open(entra_path, encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    upn = row.get("UserPrincipalName", "").strip().lower()
                    if upn:
                        entra_lookup[upn] = {col: row.get(col, "") for col in ENTRA_ENRICHMENT_COLUMNS}
            print(f"  Entra users loaded: {len(entra_lookup):,}")
            return entra_lookup
        except Exception as exc:
            print(f"  WARNING: Failed to load local Entra file: {exc}", file=sys.stderr)

    # Try ADLS
    if adls_client and adls_blob:
        print(f"  Loading Entra dimension (ADLS): {container}/{adls_blob}")
        try:
            blob_client = adls_client.get_blob_client(container=container, blob=adls_blob)
            data = blob_client.download_blob().readall().decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(data))
            for row in reader:
                upn = row.get("UserPrincipalName", "").strip().lower()
                if upn:
                    entra_lookup[upn] = {col: row.get(col, "") for col in ENTRA_ENRICHMENT_COLUMNS}
            print(f"  Entra users loaded: {len(entra_lookup):,}")
        except Exception:
            print("  No Entra Silver file found — enrichment columns will be empty.")

    if not entra_lookup:
        print("  No Entra data available — enrichment columns will be empty.")

    return entra_lookup


def enrich_row_with_entra(silver_row: dict, entra_lookup: dict[str, dict]) -> None:
    """Add Entra enrichment columns to a Silver row (in-place)."""
    user_id = silver_row.get("UserId", "").strip().lower()
    entra = entra_lookup.get(user_id)
    if entra:
        for col in ENTRA_ENRICHMENT_COLUMNS:
            silver_row[col] = entra.get(col, "")
    else:
        for col in ENTRA_ENRICHMENT_COLUMNS:
            silver_row[col] = ""


# ─── Transform core ──────────────────────────────────────────────────────────

def transform_row(raw_row: dict, source_file: str, loaded_at: str) -> dict | None:
    """
    Map one exploded Bronze row to a Silver row.
    ALL Bronze columns are preserved (pass-through). Computed columns are
    added / overwritten on top.
    Returns None if the row is invalid (missing required fields).
    """
    record_id = raw_row.get("RecordId", "").strip()
    if not record_id:
        return None  # Drop rows with no RecordId

    # Start with a full copy of every Bronze column
    silver_row: dict = {k: (v.strip() if isinstance(v, str) else v) for k, v in raw_row.items()}

    # ── Computed / overwritten columns ───────────────────────────────────
    creation_time_raw = raw_row.get("CreationTime", "").strip()
    dt = parse_creation_time(creation_time_raw)
    silver_row["UsageDate"] = dt.strftime("%Y-%m-%d") if dt else ""
    silver_row["UsageHour"] = str(dt.hour) if dt else ""

    agent_id = raw_row.get("AgentId", "").strip()
    is_prompt_raw = raw_row.get("Message_isPrompt", "").strip()

    silver_row["PromptType"] = compute_prompt_type(is_prompt_raw)
    silver_row["IsAgent"] = compute_is_agent(agent_id)

    # Cast numeric columns to int where possible
    for col in INT_COLUMNS:
        if col in silver_row:
            silver_row[col] = safe_int(silver_row[col])

    # Metadata columns
    silver_row["_SourceFile"] = source_file
    silver_row["_LoadedAtUtc"] = loaded_at

    return silver_row


def run_transform(args: argparse.Namespace) -> int:
    """
    Main transform logic.
    Returns exit code (0 = success, 1 = fatal error).
    """
    run_start = datetime.now(timezone.utc)
    loaded_at = run_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Load config ──────────────────────────────────────────────────────────
    cfg = load_config(args.config)

    adls_cfg = cfg.get("adls", {})
    container = adls_cfg.get("containerName", "insight-harbor")
    silver_path_prefix = adls_cfg.get("paths", {}).get("silverCopilotUsage", "silver/copilot-usage")
    silver_blob_path = f"{silver_path_prefix}/{SILVER_BLOB_NAME}"
    output_destination = cfg.get("pax", {}).get("outputDestination", "Local")
    ih_version = cfg.get("solution", {}).get("version", "unknown")

    input_path = os.path.abspath(args.input)
    if not os.path.isfile(input_path):
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        return 1

    source_file = Path(input_path).name

    # Local output path (always written regardless of ADLS)
    local_output_dir = os.path.dirname(input_path)
    local_silver_path = os.path.join(local_output_dir, SILVER_BLOB_NAME)

    print(f"\nInsight Harbor — Bronze to Silver Transform v{SCRIPT_VERSION}")
    print(f"  Input:       {input_path}")
    print(f"  Silver:      {silver_blob_path}")
    print(f"  Destination: {output_destination}")
    print(f"  Dry run:     {args.dry_run}")
    print(f"  Overwrite:   {args.overwrite}")
    print()

    # ── ADLS client & existing keys ──────────────────────────────────────────
    adls_client = None
    existing_keys: set[tuple] = set()

    if not args.dry_run and output_destination == "ADLS":
        adls_client = get_adls_client(cfg)
        if adls_client and not args.overwrite:
            existing_keys = download_existing_silver(adls_client, container, silver_blob_path)

    # ── Load Entra dimension for enrichment ──────────────────────────────────
    entra_path_prefix = adls_cfg.get("paths", {}).get("silverEntraUsers", "silver/entra-users")
    entra_blob_path = f"{entra_path_prefix}/silver_entra_users.csv"
    entra_local = getattr(args, "entra_local", None)
    entra_lookup = load_entra_lookup(
        entra_path=entra_local,
        adls_client=adls_client,
        container=container,
        adls_blob=entra_blob_path,
    )

    # ── Read & transform Bronze rows ─────────────────────────────────────────
    print(f"Reading Bronze exploded CSV: {source_file}")
    new_rows: list[dict] = []
    skipped_no_id = 0
    skipped_dedup = 0
    error_count = 0
    bronze_columns: list[str] = []

    try:
        with open(input_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            bronze_columns = list(reader.fieldnames or [])
            # Warn if expected key columns are missing
            for col in ("RecordId", "CreationTime", "UserId", "Operation"):
                if col not in bronze_columns:
                    print(f"  WARNING: Expected column '{col}' not found in input. Schema mismatch?", file=sys.stderr)

            print(f"  Bronze columns:   {len(bronze_columns):,} (all will be preserved)")

            for raw_row in reader:
                try:
                    silver_row = transform_row(raw_row, source_file, loaded_at)
                    if silver_row is None:
                        skipped_no_id += 1
                        continue

                    dedup_key = make_dedup_key(silver_row)
                    if dedup_key in existing_keys:
                        skipped_dedup += 1
                        continue

                    enrich_row_with_entra(silver_row, entra_lookup)
                    existing_keys.add(dedup_key)
                    new_rows.append(silver_row)
                except Exception as exc:
                    error_count += 1
                    if error_count <= 5:  # Log first 5 errors only
                        print(f"  WARNING: Row transform error: {exc}", file=sys.stderr)

    except Exception as exc:
        print(f"ERROR: Failed to read input CSV: {exc}", file=sys.stderr)
        return 1

    # ── Build dynamic Silver column list ─────────────────────────────────────
    # Order: all Bronze columns first, then computed columns not already present,
    # then Entra enrichment columns not already present.
    silver_columns: list[str] = list(bronze_columns)
    for col in COMPUTED_COLUMNS + ENTRA_ENRICHMENT_COLUMNS:
        if col not in silver_columns:
            silver_columns.append(col)

    input_records = len(new_rows) + skipped_no_id + skipped_dedup
    print(f"  Input records:    {input_records:,}")
    print(f"  New Silver rows:  {len(new_rows):,}")
    print(f"  Silver columns:   {len(silver_columns):,}")
    print(f"  Skipped (no ID):  {skipped_no_id:,}")
    print(f"  Skipped (dedup):  {skipped_dedup:,}")
    print(f"  Errors:           {error_count:,}")

    # Enrichment stats
    if entra_lookup and new_rows:
        enriched = sum(1 for r in new_rows if r.get("Department", "") != "")
        print(f"  Entra-enriched:   {enriched:,} / {len(new_rows):,} ({100*enriched/len(new_rows):.1f}%)")

    if not new_rows:
        print("\nNo new rows to write. Silver layer is already up to date.")
        _write_metadata(run_start, input_path, source_file, silver_blob_path,
                        input_records, 0, skipped_no_id, skipped_dedup, error_count,
                    args, output_destination, ih_version, upload_success=None,
                    entra_users_loaded=len(entra_lookup),
                    silver_column_count=len(silver_columns))
    # ── Write local Silver CSV ────────────────────────────────────────────────
    if not args.dry_run:
        try:
            with open(local_silver_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=silver_columns, lineterminator="\n", extrasaction="ignore")
                writer.writeheader()
                writer.writerows(new_rows)
            print(f"\n  Local Silver written -> {local_silver_path}")
        except Exception as exc:
            print(f"ERROR: Failed to write local Silver CSV: {exc}", file=sys.stderr)
            return 1
    else:
        print(f"\n  [DRY RUN] Would write {len(new_rows):,} rows to {local_silver_path}")

    # ── Upload to ADLS ────────────────────────────────────────────────────────
    upload_success: bool | None = None
    if not args.dry_run and output_destination == "ADLS" and adls_client:
        print("Uploading Silver CSV to ADLS...")
        upload_success = upload_csv_to_adls(
            adls_client, container, silver_blob_path, new_rows, silver_columns
        )
    elif args.dry_run:
        print(f"  [DRY RUN] Would upload {len(new_rows):,} rows to ADLS: {container}/{silver_blob_path}")

    # ── Cleanup local artifacts after successful ADLS upload ──────────────────
    if upload_success:
        for cleanup_path in [local_silver_path]:
            try:
                if os.path.isfile(cleanup_path):
                    os.remove(cleanup_path)
                    print(f"  Cleaned up: {cleanup_path}")
            except OSError as exc:
                print(f"  WARNING: Could not remove {cleanup_path}: {exc}", file=sys.stderr)

    # ── Write transform metadata ──────────────────────────────────────────────
    _write_metadata(run_start, input_path, source_file, silver_blob_path,
                    input_records, len(new_rows), skipped_no_id, skipped_dedup, error_count,
                    args, output_destination, ih_version, upload_success,
                    entra_users_loaded=len(entra_lookup),
                    silver_column_count=len(silver_columns))

    run_end = datetime.now(timezone.utc)
    elapsed = (run_end - run_start).total_seconds()
    print(f"\nTransform complete: {len(new_rows):,} new Silver rows in {elapsed:.2f}s")

    return 1 if error_count > 0 else 0


def _write_metadata(run_start: datetime, input_path: str, source_file: str,
                    silver_blob_path: str, input_records: int, new_rows: int,
                    skipped_no_id: int, skipped_dedup: int, errors: int,
                    args: argparse.Namespace, output_destination: str,
                    ih_version: str, upload_success: bool | None,
                    entra_users_loaded: int = 0,
                    silver_column_count: int = 0) -> None:
    """Write transform_metadata.json alongside the input file. Cleaned up after ADLS success."""
    run_end = datetime.now(timezone.utc)
    stem = Path(input_path).stem
    parent = Path(input_path).parent
    meta_path = str(parent / f"{stem}_transform_metadata.json")
    metadata = {
        "step":                 "bronze_to_silver",
        "scriptVersion":        SCRIPT_VERSION,
        "insightHarborVersion": ih_version,
        "runStartUtc":          run_start.isoformat(),
        "runEndUtc":            run_end.isoformat(),
        "elapsedSeconds":       round((run_end - run_start).total_seconds(), 2),
        "inputFile":            input_path,
        "sourceFile":           source_file,
        "inputRecords":         input_records,
        "newSilverRows":        new_rows,
        "silverColumnCount":    silver_column_count,
        "skippedNoId":          skipped_no_id,
        "skippedDedup":         skipped_dedup,
        "errors":               errors,
        "silverBlobPath":       silver_blob_path,
        "outputDestination":    output_destination,
        "uploadSuccess":        upload_success,
        "entraUsersLoaded":    entra_users_loaded,
        "dryRun":               args.dry_run,
        "overwrite":            args.overwrite,
    }
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"  Metadata written -> {meta_path}")
        # Cleanup metadata file after successful ADLS upload (data is in the lake)
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
        description=f"Insight Harbor — Bronze to Silver Transform v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the exploded Bronze CSV (output of pipeline_explode.py).",
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
        "--overwrite",
        action="store_true",
        default=False,
        help="Skip dedup check against existing Silver data and overwrite ADLS Silver file.",
    )
    parser.add_argument(
        "--entra-local",
        default=None,
        help="Path to a local Silver Entra users CSV for enrichment (skips ADLS download).",
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
