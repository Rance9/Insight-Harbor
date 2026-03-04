#!/usr/bin/env python3
"""
Insight Harbor — Pipeline Explosion Wrapper
============================================
Orchestrates the Purview M365 Usage Bundle Explosion Processor as part of the
Insight Harbor ingestion pipeline.

Responsibilities:
  1. Read insight-harbor-config.json for ADLS settings and paths.
  2. Accept the path to a RAW PAX Purview CSV (unexploded, with AuditData JSON column).
  3. Call the explosion processor to produce a 153-column flat CSV (~50x faster than PAX built-in).
  4. Upload the exploded CSV to ADLS Gen2 under bronze/exploded/YYYY/MM/DD/.
  5. Write an explosion_metadata.json recording run details, row counts, and ADLS paths.

Usage:
    python pipeline_explode.py --input <raw_csv_path> --config <config_json_path> [options]

Examples:
    # Minimal — uses default config location
    python transform/explosion/pipeline_explode.py \\
        --input ingestion/output/PAX_Purview_20260302_020000.csv

    # Explicit config path
    python transform/explosion/pipeline_explode.py \\
        --input ingestion/output/PAX_Purview_20260302_020000.csv \\
        --config config/insight-harbor-config.json

    # Dry run (no ADLS upload, local output only)
    python transform/explosion/pipeline_explode.py \\
        --input ingestion/output/PAX_Purview_20260302_020000.csv \\
        --dry-run

Requirements:
    pip install azure-storage-blob  (for ADLS upload)
    orjson is optional but recommended for ~5-10x faster JSON parsing in the explosion step:
    pip install orjson

Python 3.9+
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_VERSION = "1.0.0"
DEFAULT_CONFIG_PATH = "config/insight-harbor-config.json"
PROCESSOR_FILENAME = "Purview_M365_Usage_Bundle_Explosion_Processor_v1.0.0.py"


# ─── Helpers ─────────────────────────────────────────────────────────────────

def load_config(config_path: str) -> dict:
    """Load and validate the Insight Harbor config file."""
    abs_path = os.path.abspath(config_path)
    if not os.path.isfile(abs_path):
        print(f"ERROR: Config file not found: {abs_path}", file=sys.stderr)
        print("  Copy config/insight-harbor-config.template.json to config/insight-harbor-config.json and fill in your values.", file=sys.stderr)
        sys.exit(1)
    try:
        with open(abs_path, encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Config file is not valid JSON: {e}", file=sys.stderr)
        sys.exit(1)
    return cfg


def load_explosion_module(script_dir: str) -> Any:
    """
    Dynamically load the explosion processor as a module from the same directory.
    This avoids subprocess overhead and allows direct function calls.
    """
    processor_path = os.path.join(script_dir, PROCESSOR_FILENAME)
    if not os.path.isfile(processor_path):
        print(f"ERROR: Explosion processor not found: {processor_path}", file=sys.stderr)
        print(f"  Expected: {PROCESSOR_FILENAME} in the same directory as this script.", file=sys.stderr)
        sys.exit(1)
    spec = importlib.util.spec_from_file_location("explosion_processor", processor_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Register in sys.modules so multiprocessing workers can pickle/import it
    sys.modules["explosion_processor"] = module
    return module


def upload_to_adls(local_path: str, cfg: dict, blob_path: str) -> bool:
    """
    Upload a local file to ADLS Gen2 using azure-storage-blob.
    Returns True on success, False on failure.
    """
    try:
        from azure.storage.blob import BlobServiceClient  # type: ignore
    except ImportError:
        print("WARNING: azure-storage-blob not installed. ADLS upload skipped.", file=sys.stderr)
        print("  Install with: pip install azure-storage-blob", file=sys.stderr)
        return False

    adls_cfg = cfg.get("adls", {})
    account_name = adls_cfg.get("storageAccountName", "")
    account_key   = adls_cfg.get("storageAccountKey", "")
    container     = adls_cfg.get("containerName", "insight-harbor")

    if not account_name or not account_key:
        print("WARNING: ADLS storageAccountName or storageAccountKey missing in config. Upload skipped.", file=sys.stderr)
        return False

    account_url = f"https://{account_name}.blob.core.windows.net"
    try:
        client = BlobServiceClient(account_url=account_url, credential=account_key)
        blob_client = client.get_blob_client(container=container, blob=blob_path)
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"  ADLS upload SUCCESS → {container}/{blob_path}")
        return True
    except Exception as exc:
        print(f"  WARNING: ADLS upload FAILED: {exc}", file=sys.stderr)
        print("  Local file is preserved. Continuing without cloud upload.")
        return False


def write_explosion_metadata(meta: dict, output_csv_path: str) -> None:
    """Write a companion explosion_metadata.json next to the output CSV."""
    stem = Path(output_csv_path).stem
    parent = Path(output_csv_path).parent
    meta_path = str(parent / f"{stem}_explosion_metadata.json")
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, default=str)
        print(f"  Metadata written → {meta_path}")
    except Exception as exc:
        print(f"  WARNING: Could not write explosion metadata: {exc}", file=sys.stderr)


# ─── Main pipeline step ──────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> int:
    """
    Main pipeline step: explode → upload → metadata.
    Returns exit code (0 = success, 1 = errors).
    """
    run_start = datetime.now(timezone.utc)

    # ── Load config ──────────────────────────────────────────────────────────
    cfg = load_config(args.config)

    # ── Resolve paths ────────────────────────────────────────────────────────
    input_path = os.path.abspath(args.input)
    if not os.path.isfile(input_path):
        print(f"ERROR: Input CSV not found: {input_path}", file=sys.stderr)
        return 1

    # Determine output path
    if args.output:
        output_path = os.path.abspath(args.output)
    else:
        stem = Path(input_path).stem
        parent = Path(input_path).parent
        output_path = str(parent / f"{stem}_Exploded.csv")

    output_destination = cfg.get("pax", {}).get("outputDestination", "Local")
    ih_version = cfg.get("solution", {}).get("version", "unknown")

    print(f"\nInsight Harbor — Explosion Pipeline Step v{SCRIPT_VERSION}")
    print(f"  Input:       {input_path}")
    print(f"  Output:      {output_path}")
    print(f"  Destination: {output_destination}")
    print(f"  Dry run:     {args.dry_run}")
    print()

    # ── Load explosion processor ─────────────────────────────────────────────
    script_dir = os.path.dirname(os.path.abspath(__file__))
    processor = load_explosion_module(script_dir)

    # ── Run explosion ────────────────────────────────────────────────────────
    print("Running explosion processor...")
    # Force workers=1 when loaded as a wrapper — multiprocessing can't pickle
    # functions from dynamically-imported modules on Windows (spawn method).
    workers = args.workers if args.workers > 0 else 1

    stats = processor.run_explosion(
        input_csv=input_path,
        output_csv=output_path,
        prompt_filter=args.prompt_filter,
        workers=workers,
        chunk_size=args.chunk_size,
        quiet=args.quiet,
    )

    run_end = datetime.now(timezone.utc)
    elapsed = (run_end - run_start).total_seconds()
    had_errors = stats.get("errors", 0) > 0

    # ── Build ADLS blob path ─────────────────────────────────────────────────
    run_date = run_start.strftime("%Y/%m/%d")
    output_filename = Path(output_path).name
    adls_blob_path = (
        f"{cfg.get('adls', {}).get('paths', {}).get('bronzeExploded', 'bronze/exploded')}"
        f"/{run_date}/{output_filename}"
    )

    # ── Upload to ADLS ───────────────────────────────────────────────────────
    upload_success: bool | None = None
    if not args.dry_run and output_destination == "ADLS":
        print("Uploading exploded CSV to ADLS...")
        upload_success = upload_to_adls(output_path, cfg, adls_blob_path)
    elif args.dry_run:
        print(f"  [DRY RUN] Would upload to ADLS: {adls_blob_path}")
    else:
        print("  Output destination is 'Local' — skipping ADLS upload.")

    # ── Write metadata ────────────────────────────────────────────────────────
    metadata = {
        "step":                    "explosion",
        "wrapperVersion":          SCRIPT_VERSION,
        "processorVersion":        getattr(processor, "SCRIPT_VERSION", "unknown"),
        "insightHarborVersion":    ih_version,
        "runStartUtc":             run_start.isoformat(),
        "runEndUtc":               run_end.isoformat(),
        "elapsedSeconds":          round(elapsed, 2),
        "inputFile":               input_path,
        "outputFile":              output_path,
        "inputRecords":            stats.get("input_records", 0),
        "outputRows":              stats.get("output_rows", 0),
        "errors":                  stats.get("errors", 0),
        "expansionRatio":          round(stats.get("output_rows", 0) / max(stats.get("input_records", 1), 1), 2),
        "promptFilter":            args.prompt_filter,
        "workers":                 workers,
        "outputDestination":       output_destination,
        "adlsBlobPath":            adls_blob_path if output_destination == "ADLS" else None,
        "uploadSuccess":           upload_success,
        "dryRun":                  args.dry_run,
    }
    write_explosion_metadata(metadata, output_path)

    # ── Cleanup local metadata after successful ADLS upload ─────────────────
    # NOTE: The exploded CSV is NOT cleaned up here because it is needed as
    # input for the downstream bronze_to_silver transform. The orchestrator
    # (run-pipeline-local.ps1) handles final cleanup of all intermediate files.
    if upload_success:
        stem = Path(output_path).stem
        parent = Path(output_path).parent
        meta_path = str(parent / f"{stem}_explosion_metadata.json")
        try:
            if os.path.isfile(meta_path):
                os.remove(meta_path)
                print(f"  Cleaned up: {meta_path}")
        except OSError as exc:
            print(f"  WARNING: Could not remove {meta_path}: {exc}", file=sys.stderr)

    print()
    print(f"Explosion complete: {stats.get('input_records', 0):,} records → {stats.get('output_rows', 0):,} rows in {elapsed:.2f}s")
    if had_errors:
        print(f"WARNING: {stats.get('errors', 0)} record(s) failed to process.", file=sys.stderr)

    return 1 if had_errors else 0


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Insight Harbor — Explosion Pipeline Step v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  python pipeline_explode.py --input ingestion/output/PAX_Purview_20260302.csv
  python pipeline_explode.py --input ingestion/output/PAX_Purview_20260302.csv --dry-run
  python pipeline_explode.py --input ingestion/output/PAX_Purview_20260302.csv --workers 4
""",
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the RAW PAX Purview CSV (unexploded, must contain AuditData column).",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Path for the output exploded CSV. Default: <input_stem>_Exploded.csv in same folder.",
    )
    parser.add_argument(
        "--config", "-c",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to insight-harbor-config.json. Default: {DEFAULT_CONFIG_PATH}",
    )
    parser.add_argument(
        "--prompt-filter",
        choices=["Prompt", "Response", "Both", "Null"],
        default=None,
        help="Filter Copilot messages by prompt/response type (optional).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of parallel workers. Default: 0 (auto). Use 1 to disable parallelism.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of CSV rows per processing chunk. Default: 5000.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run explosion locally but skip ADLS upload.",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        default=False,
        help="Suppress explosion processor progress output.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {SCRIPT_VERSION}",
    )

    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
