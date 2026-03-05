import azure.functions as func
import json
import logging
import os
import io
from datetime import datetime, timezone
from typing import Any

# Configure Azure Monitor OpenTelemetry BEFORE importing instrumented libraries
from azure.monitor.opentelemetry import configure_azure_monitor
try:
    configure_azure_monitor(
        logger_name="insight_harbor",
        enable_live_metrics=True,
    )
except Exception as _ai_err:
    logging.warning("App Insights OpenTelemetry init skipped: %s", _ai_err)

import pandas as pd
import jwt                                         # PyJWT
import requests as http_requests                   # avoid collision with func types
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

logger = logging.getLogger("insight_harbor")

# ─────────────────────────────────────────────────────────────────────────────
# Auth Config — Entra ID JWT validation
# ─────────────────────────────────────────────────────────────────────────────
AUTH_ENABLED       = os.environ.get("IH_AUTH_ENABLED", "true").lower() == "true"
AUTH_TENANT_ID     = os.environ.get("IH_AUTH_TENANT_ID",  "579e8f66-10ec-4646-a923-b9dc013cc0a7")
AUTH_CLIENT_ID     = os.environ.get("IH_AUTH_CLIENT_ID",  "e571ba41-17da-4e7b-85fb-bc6d832d4f78")
AUTH_ISSUER        = f"https://login.microsoftonline.com/{AUTH_TENANT_ID}/v2.0"
OIDC_CONFIG_URL    = f"https://login.microsoftonline.com/{AUTH_TENANT_ID}/v2.0/.well-known/openid-configuration"

# Module-level cache for JWKS signing keys (avoids re-downloading on every request)
_jwks_cache: dict[str, Any] = {}
_jwks_cache_ts: datetime | None = None
JWKS_CACHE_TTL = 3600  # 1 hour


def _get_signing_keys() -> dict[str, jwt.algorithms.RSAAlgorithm]:
    """Download Microsoft's JWKS signing keys and cache them."""
    global _jwks_cache, _jwks_cache_ts

    now = datetime.now(timezone.utc)
    if _jwks_cache_ts and (now - _jwks_cache_ts).total_seconds() < JWKS_CACHE_TTL and _jwks_cache:
        return _jwks_cache

    try:
        oidc_config = http_requests.get(OIDC_CONFIG_URL, timeout=10).json()
        jwks_uri = oidc_config["jwks_uri"]
        jwks_data = http_requests.get(jwks_uri, timeout=10).json()

        signing_keys = {}
        for key_data in jwks_data.get("keys", []):
            kid = key_data.get("kid")
            if kid:
                signing_keys[kid] = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))

        _jwks_cache = signing_keys
        _jwks_cache_ts = now
        logger.info("Refreshed JWKS signing keys (%d keys)", len(signing_keys))
        return signing_keys

    except Exception as exc:
        logger.error("Failed to fetch JWKS keys: %s", exc)
        return _jwks_cache  # Return stale cache if available


def _validate_token(req: func.HttpRequest) -> dict | None:
    """
    Validate the Bearer token from the Authorization header.
    Returns decoded claims on success, None on failure.
    """
    if not AUTH_ENABLED:
        return {"auth_disabled": True}

    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]  # Strip "Bearer "

    try:
        # Get the token header to find which key was used to sign
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
        if not kid:
            logger.warning("Token missing 'kid' header")
            return None

        signing_keys = _get_signing_keys()
        public_key = signing_keys.get(kid)
        if not public_key:
            logger.warning("Unknown signing key kid=%s", kid)
            return None

        # Validate the token
        claims = jwt.decode(
            token,
            key=public_key,
            algorithms=["RS256"],
            audience=AUTH_CLIENT_ID,
            issuer=AUTH_ISSUER,
            options={"require": ["exp", "iss", "aud", "sub"]}
        )

        logger.info("Authenticated request from: %s", claims.get("preferred_username", claims.get("sub")))
        return claims

    except jwt.ExpiredSignatureError:
        logger.warning("Token expired")
        return None
    except jwt.InvalidAudienceError:
        logger.warning("Token audience mismatch")
        return None
    except jwt.InvalidIssuerError:
        logger.warning("Token issuer mismatch")
        return None
    except jwt.InvalidTokenError as exc:
        logger.warning("Token validation failed: %s", exc)
        return None


def _auth_error(req: func.HttpRequest | None = None) -> func.HttpResponse:
    """Return a 401 Unauthorized response with CORS headers."""
    origin = (req.headers.get("Origin", "") if req else "")
    cors_origin = origin if origin in ALLOWED_ORIGINS else ALLOWED_ORIGINS[0]
    return func.HttpResponse(
        body=json.dumps({"error": "Unauthorized. Provide a valid Bearer token."}),
        status_code=401,
        mimetype="application/json",
        headers={
            "Access-Control-Allow-Origin": cors_origin,
            "Vary": "Origin",
            "WWW-Authenticate": 'Bearer realm="Insight Harbor API"'
        }
    )


def _cors_preflight(req: func.HttpRequest) -> func.HttpResponse | None:
    """Handle CORS preflight OPTIONS request. Returns response if OPTIONS, else None."""
    if req.method == "OPTIONS":
        origin = req.headers.get("Origin", "")
        cors_origin = origin if origin in ALLOWED_ORIGINS else ALLOWED_ORIGINS[0]
        return func.HttpResponse(
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": cors_origin,
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Authorization, Content-Type",
                "Access-Control-Max-Age": "86400",
                "Vary": "Origin"
            }
        )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Config from environment variables (set in Function App settings or local.settings.json)
# ─────────────────────────────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.environ.get("IH_ADLS_ACCOUNT_NAME", "ihstoragepoc01")
ADLS_ACCOUNT_KEY  = os.environ.get("IH_ADLS_ACCOUNT_KEY",  "")   # From Key Vault in production
ADLS_CONTAINER    = os.environ.get("IH_ADLS_CONTAINER",    "insight-harbor")
SILVER_PREFIX     = "silver/copilot-usage/"

# Allowed CORS origins (SWA domain + localhost for dev)
ALLOWED_ORIGINS = [
    "https://lemon-mud-0e797b310.6.azurestaticapps.net",
    "https://ih.data-analytics.tech",
    "http://localhost:4280",   # SWA CLI dev server
    "http://127.0.0.1:4280",
]

# In-process cache — avoid re-downloading blob on every request during same invocation
_cache: dict[str, Any] = {}
_cache_ts: datetime | None = None
CACHE_TTL_SECONDS = 300  # 5 minutes


def _get_blob_client() -> ContainerClient:
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={ADLS_ACCOUNT_NAME};"
        f"AccountKey={ADLS_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    svc = BlobServiceClient.from_connection_string(conn_str)
    return svc.get_container_client(ADLS_CONTAINER)


def _load_silver_df() -> pd.DataFrame:
    """Download all Silver CSVs from ADLS and concatenate into a single DataFrame."""
    global _cache, _cache_ts

    now = datetime.now(timezone.utc)
    if _cache_ts and (now - _cache_ts).total_seconds() < CACHE_TTL_SECONDS and "df" in _cache:
        logger.info("Returning cached Silver DataFrame (age: %.0fs)", (now - _cache_ts).total_seconds())
        return _cache["df"]

    logger.info("Loading Silver data from ADLS: %s/%s%s", ADLS_ACCOUNT_NAME, ADLS_CONTAINER, SILVER_PREFIX)
    container = _get_blob_client()

    blobs = list(container.list_blobs(name_starts_with=SILVER_PREFIX))
    csv_blobs = [b for b in blobs if b.name.endswith(".csv")]

    if not csv_blobs:
        logger.warning("No Silver CSV files found at %s", SILVER_PREFIX)
        return pd.DataFrame()

    frames = []
    for blob in csv_blobs:
        data = container.download_blob(blob.name).readall()
        df_chunk = pd.read_csv(io.BytesIO(data), low_memory=False)
        frames.append(df_chunk)
        logger.info("  Loaded blob: %s (%d rows)", blob.name, len(df_chunk))

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # Type coercions
    if "UsageDate" in df.columns:
        df["UsageDate"] = pd.to_datetime(df["UsageDate"], errors="coerce")
    if "IsAgent" in df.columns:
        df["IsAgent"] = df["IsAgent"].astype(bool, errors="ignore")

    _cache["df"] = df
    _cache_ts = now
    logger.info("Loaded %d total Silver rows from %d blob(s).", len(df), len(csv_blobs))
    return df


def _json_response(data: Any, status_code: int = 200, req: func.HttpRequest | None = None) -> func.HttpResponse:
    origin = (req.headers.get("Origin", "") if req else "")
    cors_origin = origin if origin in ALLOWED_ORIGINS else ALLOWED_ORIGINS[0]
    return func.HttpResponse(
        body=json.dumps(data, default=str),
        status_code=status_code,
        mimetype="application/json",
        headers={
            "Access-Control-Allow-Origin": cors_origin,
            "Vary": "Origin",
            "Cache-Control": "public, max-age=300"
        }
    )


def _error_response(message: str, status_code: int = 500, req: func.HttpRequest | None = None) -> func.HttpResponse:
    return _json_response({"error": message}, status_code, req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/summary
# Returns top-level KPIs for the dashboard header cards
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="summary", methods=["GET", "OPTIONS"])
def get_summary(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    try:
        df = _load_silver_df()
        if df.empty:
            return _json_response({"error": "No data available yet. Run the pipeline first."}, 204, req)

        prompts_df = df[df["PromptType"] == "Prompt"] if "PromptType" in df.columns else df

        top_workload = None
        if "Workload" in df.columns and not df["Workload"].dropna().empty:
            top_workload = df["Workload"].value_counts().idxmax()

        data_as_of = None
        if "_LoadedAtUtc" in df.columns:
            data_as_of = str(df["_LoadedAtUtc"].max())
        elif "UsageDate" in df.columns:
            data_as_of = str(df["UsageDate"].max())

        summary = {
            "totalRecords":      int(len(df)),
            "totalPrompts":      int(len(prompts_df)),
            "activeUsers":       int(df["UserId"].nunique()) if "UserId" in df.columns else 0,
            "topWorkload":       top_workload,
            "dataAsOf":          data_as_of,
            "agentUsageRate":    round(
                float(df["IsAgent"].sum() / len(df)) if "IsAgent" in df.columns and len(df) > 0 else 0,
                4
            ),
            "generatedAt":       datetime.now(timezone.utc).isoformat()
        }

        # Adoption rate: Copilot-licensed users with at least one prompt / total Copilot-licensed
        if "HasCopilotLicense" in df.columns and "UserId" in df.columns:
            copilot_users = df[df["HasCopilotLicense"].astype(str).str.upper() == "TRUE"]["UserId"].unique()
            active_copilot = len(set(copilot_users) & set(
                prompts_df["UserId"].unique() if "UserId" in prompts_df.columns else []
            ))
            total_copilot = len(copilot_users)
            summary["copilotLicensed"] = total_copilot
            summary["copilotActive"] = active_copilot
            summary["adoptionRate"] = round(active_copilot / total_copilot, 4) if total_copilot > 0 else 0
        else:
            summary["copilotLicensed"] = 0
            summary["copilotActive"] = 0
            summary["adoptionRate"] = 0

        return _json_response(summary, req=req)

    except Exception as exc:
        logger.exception("Error in /api/summary")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/trend?days=30
# Returns daily prompt counts for the trend line chart
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="trend", methods=["GET", "OPTIONS"])
def get_trend(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    try:
        days = int(req.params.get("days", 30))
        days = max(7, min(365, days))   # Clamp to sane range

        df = _load_silver_df()
        if df.empty or "UsageDate" not in df.columns:
            return _json_response({"trend": []}, req=req)

        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
        df_filtered = df[df["UsageDate"] >= cutoff.tz_localize(None)]

        if "PromptType" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["PromptType"] == "Prompt"]

        daily = (
            df_filtered
            .groupby(df_filtered["UsageDate"].dt.date)
            .agg(
                prompts=("UserId", "count"),
                users=("UserId", "nunique")
            )
            .reset_index()
            .rename(columns={"UsageDate": "date"})
            .sort_values("date")
        )

        trend = [
            {"date": str(row["date"]), "prompts": int(row["prompts"]), "users": int(row["users"])}
            for _, row in daily.iterrows()
        ]

        return _json_response({"days": days, "trend": trend}, req=req)

    except Exception as exc:
        logger.exception("Error in /api/trend")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/department
# Returns per-department breakdown of users and prompts
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="department", methods=["GET", "OPTIONS"])
def get_department(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    try:
        df = _load_silver_df()
        if df.empty or "Department" not in df.columns:
            return _json_response({"departments": []}, req=req)

        if "PromptType" in df.columns:
            prompts_df = df[df["PromptType"] == "Prompt"]
        else:
            prompts_df = df

        dept = (
            prompts_df
            .groupby("Department")
            .agg(
                prompts=("UserId", "count"),
                users=("UserId", "nunique")
            )
            .reset_index()
            .sort_values("prompts", ascending=False)
        )

        departments = [
            {
                "department": row["Department"] or "Unknown",
                "prompts":    int(row["prompts"]),
                "users":      int(row["users"]),
                "promptsPerUser": round(float(row["prompts"]) / float(row["users"]), 1) if row["users"] > 0 else 0
            }
            for _, row in dept.iterrows()
        ]

        return _json_response({"departments": departments}, req=req)

    except Exception as exc:
        logger.exception("Error in /api/department")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/workload
# Returns per-workload breakdown (Teams, Word, Excel, etc.)
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="workload", methods=["GET", "OPTIONS"])
def get_workload(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    try:
        df = _load_silver_df()
        if df.empty or "Workload" not in df.columns:
            return _json_response({"workloads": []}, req=req)

        wl = (
            df
            .groupby("Workload")
            .agg(
                records=("UserId", "count"),
                users=("UserId", "nunique")
            )
            .reset_index()
            .sort_values("records", ascending=False)
        )

        total = int(wl["records"].sum())
        workloads = [
            {
                "workload": row["Workload"] or "Unknown",
                "records":  int(row["records"]),
                "users":    int(row["users"]),
                "pct":      round(float(row["records"]) / total * 100, 1) if total > 0 else 0
            }
            for _, row in wl.iterrows()
        ]

        return _json_response({"workloads": workloads}, req=req)

    except Exception as exc:
        logger.exception("Error in /api/workload")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/licensing
# Returns license tier distribution and Copilot adoption metrics
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="licensing", methods=["GET", "OPTIONS"])
def get_licensing(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    try:
        df = _load_silver_df()
        if df.empty or "LicenseTier" not in df.columns:
            return _json_response({"tiers": [], "adoptionRate": 0}, req=req)

        # Per-tier breakdown: unique users per license tier
        tier_users = (
            df.groupby("LicenseTier")["UserId"]
            .nunique()
            .reset_index()
            .rename(columns={"UserId": "users"})
            .sort_values("users", ascending=False)
        )

        tiers = [
            {"tier": row["LicenseTier"] or "Unknown", "users": int(row["users"])}
            for _, row in tier_users.iterrows()
        ]

        # Adoption rate
        adoption_rate = 0
        copilot_licensed = 0
        copilot_active = 0
        if "HasCopilotLicense" in df.columns and "PromptType" in df.columns:
            prompts_df = df[df["PromptType"] == "Prompt"]
            copilot_rows = df[df["HasCopilotLicense"].astype(str).str.upper() == "TRUE"]
            copilot_licensed = int(copilot_rows["UserId"].nunique())
            copilot_active = int(len(
                set(copilot_rows["UserId"].unique()) &
                set(prompts_df["UserId"].unique())
            ))
            adoption_rate = round(copilot_active / copilot_licensed, 4) if copilot_licensed > 0 else 0

        return _json_response({
            "tiers": tiers,
            "copilotLicensed": copilot_licensed,
            "copilotActive": copilot_active,
            "adoptionRate": adoption_rate,
        }, req=req)

    except Exception as exc:
        logger.exception("Error in /api/licensing")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hourly
# Returns hour-of-day × day-of-week matrix for the heatmap
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="hourly", methods=["GET", "OPTIONS"])
def get_hourly(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    try:
        df = _load_silver_df()
        if df.empty or "UsageDate" not in df.columns:
            return _json_response({"cells": []}, req=req)

        # Ensure UsageDate is datetime
        df["UsageDate"] = pd.to_datetime(df["UsageDate"], errors="coerce")

        # Extract day of week (0=Mon, 6=Sun) and hour
        df["_dow"] = df["UsageDate"].dt.dayofweek
        if "UsageHour" in df.columns:
            df["_hour"] = pd.to_numeric(df["UsageHour"], errors="coerce").fillna(0).astype(int)
        else:
            df["_hour"] = df["UsageDate"].dt.hour

        # Filter to prompts only if PromptType exists
        work_df = df[df["PromptType"] == "Prompt"] if "PromptType" in df.columns else df

        # Group by day-of-week × hour
        matrix = (
            work_df
            .groupby(["_dow", "_hour"])
            .size()
            .reset_index(name="count")
        )

        cells = [
            {"dow": int(row["_dow"]), "hour": int(row["_hour"]), "count": int(row["count"])}
            for _, row in matrix.iterrows()
        ]

        return _json_response({"cells": cells}, req=req)

    except Exception as exc:
        logger.exception("Error in /api/hourly")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/health
# Simple liveness check — returns 200 if Function App is running
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="health", methods=["GET"])
def get_health(req: func.HttpRequest) -> func.HttpResponse:
    return _json_response({
        "status": "ok",
        "service": "insight-harbor-api",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }, req=req)


# ═══════════════════════════════════════════════════════════════════════════════
# Admin API — /api/admin/*
# ═══════════════════════════════════════════════════════════════════════════════
# These endpoints are gated by membership in the InsightHarbor-Admins
# Entra security group (checked via the "groups" claim in the JWT).

ADMIN_GROUP_ID = os.environ.get(
    "IH_ADMIN_GROUP_ID",
    ""  # Set to the Object ID of the InsightHarbor-Admins security group
)

PIPELINE_STATE_PREFIX = "pipeline/state/"
PIPELINE_HISTORY_PREFIX = "pipeline/history/"


def _is_admin(claims: dict) -> bool:
    """Check whether the user's JWT contains the admin group claim."""
    if not ADMIN_GROUP_ID:
        # No group configured — allow any authenticated user (PoC mode)
        return True
    groups = claims.get("groups", [])
    return ADMIN_GROUP_ID in groups


def _admin_auth_error(req: func.HttpRequest) -> func.HttpResponse:
    """Return 403 Forbidden for non-admin users."""
    return _json_response({"error": "Forbidden. Admin role required."}, 403, req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/admin/connectors
# Returns status and configuration of all registered data connectors
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="admin/connectors", methods=["GET", "OPTIONS"])
def admin_connectors(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    if not _is_admin(claims):
        return _admin_auth_error(req)

    try:
        # Import connector registry (lives in the pipeline package)
        import sys
        pipeline_root = os.path.join(os.path.dirname(__file__), "..", "..", "pipeline")
        if pipeline_root not in sys.path:
            sys.path.insert(0, pipeline_root)

        from shared.connectors.registry import ConnectorRegistry
        ConnectorRegistry.reset()
        registry = ConnectorRegistry.instance()

        data = {
            "connectors": registry.list_all(),
            "enabled": registry.list_enabled(),
            "validation": registry.validate_all(),
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }

        ConnectorRegistry.reset()
        return _json_response(data, req=req)

    except Exception as exc:
        logger.exception("Error in /api/admin/connectors")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/admin/runs?limit=10
# Returns recent pipeline run history from ADLS state files
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="admin/runs", methods=["GET", "OPTIONS"])
def admin_runs(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    if not _is_admin(claims):
        return _admin_auth_error(req)

    try:
        limit = int(req.params.get("limit", 10))
        limit = max(1, min(50, limit))

        container = _get_blob_client()

        # Enumerate run state files
        runs: list[dict] = []
        try:
            blobs = list(container.list_blobs(name_starts_with=PIPELINE_HISTORY_PREFIX))
            json_blobs = sorted(
                [b for b in blobs if b.name.endswith(".json")],
                key=lambda b: b.last_modified,
                reverse=True,
            )

            for blob in json_blobs[:limit]:
                try:
                    data = container.download_blob(blob.name).readall()
                    run_info = json.loads(data)
                    run_info["_blob"] = blob.name
                    runs.append(run_info)
                except Exception:
                    runs.append({"_blob": blob.name, "error": "parse_failed"})

        except ResourceNotFoundError:
            pass

        # Also include current run state if available
        current_state: dict | None = None
        try:
            state_blobs = list(container.list_blobs(name_starts_with=PIPELINE_STATE_PREFIX))
            latest = sorted(
                [b for b in state_blobs if b.name.endswith(".json")],
                key=lambda b: b.last_modified,
                reverse=True,
            )
            if latest:
                data = container.download_blob(latest[0].name).readall()
                current_state = json.loads(data)
        except (ResourceNotFoundError, Exception):
            pass

        return _json_response({
            "runs": runs,
            "currentState": current_state,
            "total": len(runs),
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }, req=req)

    except Exception as exc:
        logger.exception("Error in /api/admin/runs")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/admin/health
# Extended health check — includes storage connectivity and connector status
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="admin/health", methods=["GET", "OPTIONS"])
def admin_health(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)
    if not _is_admin(claims):
        return _admin_auth_error(req)

    try:
        checks: dict[str, dict] = {}

        # Storage connectivity
        try:
            container = _get_blob_client()
            _ = list(container.list_blobs(name_starts_with="silver/", results_per_page=1))
            checks["adls"] = {"status": "ok", "account": ADLS_ACCOUNT_NAME}
        except Exception as exc:
            checks["adls"] = {"status": "error", "detail": str(exc)}

        # Silver data freshness
        try:
            df = _load_silver_df()
            if df.empty:
                checks["silver_data"] = {"status": "empty", "rows": 0}
            else:
                latest = None
                if "_LoadedAtUtc" in df.columns:
                    latest = str(df["_LoadedAtUtc"].max())
                checks["silver_data"] = {
                    "status": "ok",
                    "rows": int(len(df)),
                    "columns": int(len(df.columns)),
                    "latestLoad": latest,
                }
        except Exception as exc:
            checks["silver_data"] = {"status": "error", "detail": str(exc)}

        overall = "ok" if all(c["status"] == "ok" for c in checks.values()) else "degraded"

        return _json_response({
            "status": overall,
            "service": "insight-harbor-api",
            "version": "1.0.0",
            "checks": checks,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, req=req)

    except Exception as exc:
        logger.exception("Error in /api/admin/health")
        return _error_response(str(exc), req=req)


# ═════════════════════════════════════════════════════════════════════════════
# Semantic Query Engine: /api/schema  &  /api/query
# ═════════════════════════════════════════════════════════════════════════════

# Lazy-initialised singletons (heavy imports deferred to first call)
_schema_catalog = None
_query_generator = None


def _get_schema_catalog():
    """Lazy-load the SchemaCatalog singleton."""
    global _schema_catalog
    if _schema_catalog is None:
        from shared.query.schema_catalog import SchemaCatalog
        _schema_catalog = SchemaCatalog()
    return _schema_catalog


def _get_query_generator():
    """Lazy-load the QueryGenerator singleton."""
    global _query_generator
    if _query_generator is None:
        from shared.query.query_generator import QueryGenerator
        _query_generator = QueryGenerator(_get_schema_catalog())
    return _query_generator


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/schema[?dataset=<name>]
# Returns schema metadata for all datasets, or details for one.
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="schema", methods=["GET", "OPTIONS"])
def get_schema(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)

    try:
        catalog = _get_schema_catalog()
        dataset = req.params.get("dataset", "")

        if dataset:
            schema = catalog.get_schema(dataset)
            if schema is None:
                return _error_response(
                    f"Unknown dataset '{dataset}'. Available: {', '.join(catalog.list_datasets())}",
                    status=404, req=req,
                )
            return _json_response({
                "dataset": dataset,
                "display_name": catalog.get_display_name(dataset),
                "description": catalog.get_description(dataset),
                "grain": catalog.get_grain(dataset),
                "silver_path": catalog.get_silver_path(dataset),
                "columns": catalog.get_columns(dataset),
                "queryable_columns": catalog.get_queryable_columns(dataset),
                "aliases": schema.get("aliases", {}),
                "generatedAt": datetime.now(timezone.utc).isoformat(),
            }, req=req)
        else:
            return _json_response({
                "datasets": catalog.to_summary(),
                "total": len(catalog.list_datasets()),
                "generatedAt": datetime.now(timezone.utc).isoformat(),
            }, req=req)

    except Exception as exc:
        logger.exception("Error in /api/schema")
        return _error_response(str(exc), req=req)


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/query
# Execute a structured query DSL against Silver data.
# Body: { "dataset": "...", "filters": [...], "group_by": [...], ... }
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="query", methods=["POST", "OPTIONS"])
def run_query(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)

    try:
        body = req.get_json()
    except Exception:
        return _error_response("Request body must be valid JSON", status=400, req=req)

    try:
        generator = _get_query_generator()
        plan = generator.generate(body)
    except Exception as exc:
        return _error_response(f"Query validation failed: {exc}", status=400, req=req)

    try:
        from shared.query.query_executor import QueryExecutor

        # Build a lightweight ADLS reader from the existing blob client
        container = _get_blob_client()

        class _AdlsReader:
            """Thin adapter so QueryExecutor can call download_text()."""
            def download_text(self, blob_path: str) -> str:
                return container.download_blob(blob_path).readall().decode("utf-8")

        executor = QueryExecutor(_AdlsReader())
        result = executor.execute(plan)

        return _json_response(result, req=req)

    except Exception as exc:
        logger.exception("Error executing query")
        return _error_response(f"Query execution failed: {exc}", req=req)


# Lazy singletons for viz + narrative
_viz_recommender = None
_narrative_generator = None


def _get_viz_recommender():
    global _viz_recommender
    if _viz_recommender is None:
        from shared.query.viz_recommender import VizRecommender
        _viz_recommender = VizRecommender(_get_schema_catalog())
    return _viz_recommender


def _get_narrative_generator():
    global _narrative_generator
    if _narrative_generator is None:
        from shared.query.narrative import NarrativeGenerator
        _narrative_generator = NarrativeGenerator(_get_schema_catalog())
    return _narrative_generator


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/visualize
# Run query + return result WITH visualization recommendation & narrative.
# Body: same DSL as /api/query, with optional "include_narrative": true
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="visualize", methods=["POST", "OPTIONS"])
def visualize(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)

    try:
        body = req.get_json()
    except Exception:
        return _error_response("Request body must be valid JSON", status=400, req=req)

    include_narrative = body.pop("include_narrative", True)
    include_alternatives = body.pop("include_alternatives", False)

    # 1. Validate query
    try:
        generator = _get_query_generator()
        plan = generator.generate(body)
    except Exception as exc:
        return _error_response(f"Query validation failed: {exc}", status=400, req=req)

    # 2. Execute query
    try:
        from shared.query.query_executor import QueryExecutor
        container = _get_blob_client()

        class _AdlsReader:
            def download_text(self, blob_path: str) -> str:
                return container.download_blob(blob_path).readall().decode("utf-8")

        executor = QueryExecutor(_AdlsReader())
        result = executor.execute(plan)
    except Exception as exc:
        logger.exception("Error executing query for /api/visualize")
        return _error_response(f"Query execution failed: {exc}", req=req)

    # 3. Recommend visualization
    try:
        recommender = _get_viz_recommender()
        if include_alternatives:
            recs = recommender.recommend_multiple(plan, result)
            result["visualization"] = recs[0].to_dict()
            result["alternatives"] = [r.to_dict() for r in recs[1:]]
        else:
            rec = recommender.recommend(plan, result)
            result["visualization"] = rec.to_dict()
    except Exception as exc:
        logger.warning("Viz recommendation failed: %s", exc)
        result["visualization"] = {"chart_type": "table", "title": "Data",
                                   "description": "Fallback table view"}

    # 4. Generate narrative (optional)
    if include_narrative:
        try:
            narrator = _get_narrative_generator()
            result["narrative"] = narrator.generate(plan, result)
        except Exception as exc:
            logger.warning("Narrative generation failed: %s", exc)
            result["narrative"] = {"summary": "", "insights": [], "methodology": ""}

    return _json_response(result, req=req)


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/narrative
# Generate a narrative summary for a previously-executed query result.
# Body: { "plan": {...}, "result": {...} }
# ─────────────────────────────────────────────────────────────────────────────
@app.route(route="narrative", methods=["POST", "OPTIONS"])
def get_narrative(req: func.HttpRequest) -> func.HttpResponse:
    preflight = _cors_preflight(req)
    if preflight:
        return preflight
    claims = _validate_token(req)
    if claims is None:
        return _auth_error(req)

    try:
        body = req.get_json()
    except Exception:
        return _error_response("Request body must be valid JSON", status=400, req=req)

    plan_dict = body.get("plan")
    result_data = body.get("result")
    if not plan_dict or not result_data:
        return _error_response("Body must include 'plan' and 'result' keys", status=400, req=req)

    try:
        from shared.query.query_generator import QueryPlan
        plan = QueryPlan(
            dataset=plan_dict.get("dataset", ""),
            silver_path=plan_dict.get("silver_path", ""),
            filters=plan_dict.get("filters", []),
            group_by=plan_dict.get("group_by", []),
            aggregations=plan_dict.get("aggregations", []),
            sort_by=plan_dict.get("sort_by", []),
            limit=plan_dict.get("limit", 1000),
            columns=plan_dict.get("columns"),
        )
        narrator = _get_narrative_generator()
        narrative = narrator.generate(plan, result_data)
        return _json_response(narrative, req=req)
    except Exception as exc:
        logger.exception("Error in /api/narrative")
        return _error_response(str(exc), req=req)
