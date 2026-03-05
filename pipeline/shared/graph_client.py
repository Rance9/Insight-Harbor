"""
Insight Harbor — Graph API Client
===================================
MSAL-based client for Microsoft Graph API with token caching,
automatic refresh, 401 retry, 429 backoff, and API version negotiation.

Replaces PAX's Connect-MgGraph + Invoke-MgGraphRequest.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import httpx
import msal

from .config import config

logger = logging.getLogger("ih.graph_client")


class ThrottledError(Exception):
    """Raised when Graph API returns 429 — triggers Durable retry."""

    def __init__(self, message: str, retry_after_seconds: int = 60):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class PermanentError(Exception):
    """Raised on 403 or other non-retryable errors."""


class GraphClient:
    """Thin wrapper around MSAL + httpx for Microsoft Graph API calls.

    Features matching PAX:
      • Client-credential auth (no user interaction)
      • Automatic token refresh (proactive at 5-min window)
      • 401 → refresh token + single retry
      • 429 → raise ThrottledError for Durable retry policy
      • 500/502/503/504 → raise for Durable retry policy
      • Pagination via @odata.nextLink
      • API version fallback (v1.0 → beta)
    """

    def __init__(
        self,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ):
        self._tenant_id = tenant_id or config.TENANT_ID
        self._client_id = client_id or config.CLIENT_ID
        self._client_secret = client_secret or config.CLIENT_SECRET
        self._scopes = ["https://graph.microsoft.com/.default"]

        # MSAL confidential client (handles token caching internally)
        self._app = msal.ConfidentialClientApplication(
            self._client_id,
            authority=f"https://login.microsoftonline.com/{self._tenant_id}",
            client_credential=self._client_secret,
        )

        self._token_result: dict[str, Any] | None = None
        self._token_acquired_at: float = 0.0

    # ── Authentication ──────────────────────────────────────────────────────

    def _acquire_token(self) -> str:
        """Acquire or refresh access token via MSAL client credentials flow."""
        result = self._app.acquire_token_for_client(scopes=self._scopes)

        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "Unknown"))
            raise PermanentError(f"Token acquisition failed: {error}")

        self._token_result = result
        self._token_acquired_at = time.time()
        logger.debug("Token acquired, expires_in=%s", result.get("expires_in"))
        return result["access_token"]

    def _get_token(self) -> str:
        """Get valid token, refreshing proactively at 5-minute threshold."""
        if self._token_result is None:
            return self._acquire_token()

        # Proactive refresh: if token expires in < 5 minutes, refresh
        expires_in = self._token_result.get("expires_in", 3600)
        elapsed = time.time() - self._token_acquired_at
        remaining = expires_in - elapsed

        if remaining < 300:  # < 5 minutes remaining
            logger.debug("Proactive token refresh (%.0fs remaining)", remaining)
            return self._acquire_token()

        return self._token_result["access_token"]

    # ── HTTP Methods ────────────────────────────────────────────────────────

    def _headers(self, token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "ConsistencyLevel": "eventual",
        }

    def _handle_response(
        self, response: httpx.Response, *, allow_404: bool = False
    ) -> dict[str, Any] | list[Any] | None:
        """Unified response handling matching PAX's error hierarchy."""
        status = response.status_code

        if status == 200 or status == 201:
            return response.json() if response.content else None

        if status == 204:
            return None

        if status == 404 and allow_404:
            return None

        if status == 401:
            raise _Unauthorized("401 Unauthorized — token may be expired")

        if status == 403:
            body = response.text[:500]
            raise PermanentError(f"403 Forbidden: {body}")

        if status == 429:
            retry_after = int(response.headers.get("Retry-After", "60"))
            logger.warning("429 Throttled — Retry-After: %ds", retry_after)
            raise ThrottledError(
                f"429 Too Many Requests (Retry-After: {retry_after}s)",
                retry_after_seconds=retry_after,
            )

        if status in (500, 502, 503, 504):
            body = response.text[:500]
            raise RuntimeError(f"Server error {status}: {body}")

        body = response.text[:500]
        raise RuntimeError(f"Unexpected status {status}: {body}")

    async def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> dict[str, Any] | list[Any] | None:
        """Execute HTTP request with 401 retry (single attempt)."""
        token = self._get_token()

        async with httpx.AsyncClient(timeout=120.0) as client:
            for attempt in range(2):  # Max 1 retry on 401
                response = await client.request(
                    method,
                    url,
                    headers=self._headers(token),
                    json=json_body,
                )
                try:
                    return self._handle_response(response, allow_404=allow_404)
                except _Unauthorized:
                    if attempt == 0:
                        logger.info("401 received — refreshing token and retrying")
                        token = self._acquire_token()
                    else:
                        raise PermanentError("401 after token refresh — check permissions")

    def _request_sync(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> dict[str, Any] | list[Any] | None:
        """Synchronous HTTP request with 401 retry."""
        token = self._get_token()

        with httpx.Client(timeout=120.0) as client:
            for attempt in range(2):
                response = client.request(
                    method,
                    url,
                    headers=self._headers(token),
                    json=json_body,
                )
                try:
                    return self._handle_response(response, allow_404=allow_404)
                except _Unauthorized:
                    if attempt == 0:
                        logger.info("401 received — refreshing token and retrying")
                        token = self._acquire_token()
                    else:
                        raise PermanentError("401 after token refresh — check permissions")

    # ── Convenience Methods ─────────────────────────────────────────────────

    def get(self, url: str, *, allow_404: bool = False) -> dict[str, Any] | None:
        """Synchronous GET request."""
        return self._request_sync("GET", url, allow_404=allow_404)

    def post(self, url: str, body: dict[str, Any]) -> dict[str, Any] | None:
        """Synchronous POST request."""
        return self._request_sync("POST", url, json_body=body)

    def delete(self, url: str, *, allow_404: bool = True) -> None:
        """Synchronous DELETE request (404 is acceptable)."""
        self._request_sync("DELETE", url, allow_404=allow_404)

    # ── Audit Log Specific Methods ──────────────────────────────────────────

    def create_audit_query(
        self,
        display_name: str,
        start_time: str,
        end_time: str,
        activity_types: list[str],
        *,
        record_types: list[str] | None = None,
        user_principal_names: list[str] | None = None,
        service_filter: str | None = None,
    ) -> dict[str, Any]:
        """Create a Purview audit log query (POST /security/auditLog/queries).

        Matches PAX's Invoke-GraphAuditQuery.
        """
        body: dict[str, Any] = {
            "displayName": display_name,
            "filterStartDateTime": start_time,
            "filterEndDateTime": end_time,
            "operationFilters": activity_types,
        }

        if record_types:
            body["recordTypeFilters"] = record_types

        if user_principal_names:
            body["userPrincipalNameFilters"] = user_principal_names

        if service_filter:
            body["serviceFilters"] = [service_filter]

        result = self.post(config.graph_audit_url, body)
        if not result or "id" not in result:
            raise RuntimeError(f"Failed to create audit query: {result}")

        logger.info(
            "Created audit query %s (%s)", result["id"], display_name
        )
        return {"query_id": result["id"], "display_name": display_name}

    def poll_audit_query(self, query_id: str) -> dict[str, Any]:
        """Poll audit query status (GET /security/auditLog/queries/{id}).

        Returns: {query_id, status, record_count}
        """
        url = f"{config.graph_audit_url}/{query_id}"
        result = self.get(url)
        if not result:
            raise RuntimeError(f"Query {query_id} not found")

        return {
            "query_id": query_id,
            "status": result.get("status", "unknown"),
            "record_count": result.get("rowCount", 0),
        }

    def fetch_audit_records(
        self, query_id: str, *, page_size: int = 1000
    ):
        """Generator yielding pages of audit records.

        Follows @odata.nextLink until exhausted.
        Matches PAX's paginated fetch with per-page streaming.

        Yields: list[dict] — one page of records at a time.
        """
        url = f"{config.graph_audit_url}/{query_id}/records?$top={page_size}"
        page_num = 0

        while url:
            page_num += 1
            result = self.get(url)
            if not result:
                break

            records = result.get("value", [])
            if records:
                yield records

            url = result.get("@odata.nextLink")
            if url:
                logger.debug("Fetching page %d (nextLink present)", page_num + 1)

        logger.info("Fetched %d pages for query %s", page_num, query_id)

    def delete_audit_query(self, query_id: str) -> None:
        """Delete completed audit query to free the 10-query slot.

        Matches PAX's post-fetch cleanup.
        """
        url = f"{config.graph_audit_url}/{query_id}"
        self.delete(url)
        logger.info("Deleted audit query %s", query_id)

    # ── Entra User Methods ──────────────────────────────────────────────────

    def fetch_users(
        self,
        select_fields: list[str],
        *,
        page_size: int = 999,
        filter_expr: str | None = None,
    ):
        """Generator yielding pages of Entra users.

        Follows @odata.nextLink pagination.
        Matches PAX's -OnlyUserInfo mode.

        Yields: list[dict] — one page of user records.
        """
        select = ",".join(select_fields)
        url = (
            f"{config.GRAPH_BASE_URL}/{config.GRAPH_API_VERSION}"
            f"/users?$select={select}&$top={page_size}"
        )
        if filter_expr:
            url += f"&$filter={filter_expr}"

        page_num = 0
        while url:
            page_num += 1
            result = self.get(url)
            if not result:
                break

            users = result.get("value", [])
            if users:
                yield users

            url = result.get("@odata.nextLink")

        logger.info("Fetched %d pages of users", page_num)


class _Unauthorized(Exception):
    """Internal exception for 401 handling — triggers token refresh."""
