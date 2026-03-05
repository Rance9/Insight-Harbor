"""
Insight Harbor — ADLS Gen2 Client
==================================
Blob storage helper for reading / writing data to Azure Data Lake Storage Gen2.
Supports streaming reads, append blobs, and JSON state files.

Uses DefaultAzureCredential for managed-identity auth in Azure,
falling back to environment variables for local development.
"""

from __future__ import annotations

import io
import json
import logging
from typing import Any, Generator, Optional

import orjson
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    ContainerClient,
    ContentSettings,
)

from .config import config

logger = logging.getLogger("ih.adls_client")


class ADLSClient:
    """Thin wrapper around Azure Blob SDK for pipeline I/O.

    All methods use streaming where possible to stay within
    Consumption plan memory limits (~1.5 GB).
    """

    def __init__(
        self,
        account_name: str | None = None,
        container_name: str | None = None,
    ):
        self._account_name = account_name or config.ADLS_ACCOUNT_NAME
        self._container = container_name or config.ADLS_CONTAINER
        self._account_url = f"https://{self._account_name}.blob.core.windows.net"

        # DefaultAzureCredential works with:
        #  - Managed Identity (in Azure)
        #  - Azure CLI / VS Code credentials (local dev)
        #  - Environment variables (CI/CD)
        self._credential = DefaultAzureCredential()

        self._service_client: BlobServiceClient | None = None
        self._container_client: ContainerClient | None = None

    # ── Client Initialization ───────────────────────────────────────────────

    def _get_service_client(self) -> BlobServiceClient:
        if self._service_client is None:
            self._service_client = BlobServiceClient(
                account_url=self._account_url,
                credential=self._credential,
            )
        return self._service_client

    def _get_container_client(self) -> ContainerClient:
        if self._container_client is None:
            self._container_client = (
                self._get_service_client().get_container_client(self._container)
            )
        return self._container_client

    def _get_blob_client(self, blob_path: str) -> BlobClient:
        return self._get_container_client().get_blob_client(blob_path)

    def _get_append_blob_client(self, blob_path: str) -> BlobClient:
        """Get a BlobClient for append-blob operations."""
        return BlobClient(
            account_url=self._account_url,
            container_name=self._container,
            blob_name=blob_path,
            credential=self._credential,
        )

    # ── Read Operations ─────────────────────────────────────────────────────

    def blob_exists(self, blob_path: str) -> bool:
        """Check if a blob exists."""
        client = self._get_blob_client(blob_path)
        try:
            client.get_blob_properties()
            return True
        except Exception:
            return False

    def download_text(self, blob_path: str) -> str:
        """Download blob as UTF-8 text. Best for small files (< 100 MB)."""
        client = self._get_blob_client(blob_path)
        data = client.download_blob().readall()
        return data.decode("utf-8")

    def download_json(self, blob_path: str) -> Any:
        """Download and parse a JSON blob."""
        text = self.download_text(blob_path)
        return orjson.loads(text)

    def download_stream(
        self, blob_path: str, chunk_size: int | None = None
    ) -> Generator[bytes, None, None]:
        """Stream blob content in chunks. Memory-safe for large files.

        Args:
            blob_path: Path within the container.
            chunk_size: Bytes per chunk (default: config.STREAM_CHUNK_SIZE_BYTES).

        Yields:
            bytes — one chunk at a time.
        """
        chunk_size = chunk_size or config.STREAM_CHUNK_SIZE_BYTES
        client = self._get_blob_client(blob_path)
        stream = client.download_blob()

        for chunk in stream.chunks():
            # The SDK may return chunks larger than requested; we re-chunk
            buf = chunk
            while len(buf) > chunk_size:
                yield buf[:chunk_size]
                buf = buf[chunk_size:]
            if buf:
                yield buf

    def download_lines(
        self, blob_path: str, *, encoding: str = "utf-8"
    ) -> Generator[str, None, None]:
        """Stream blob content line by line. Memory-safe for JSONL files.

        Yields:
            str — one line at a time (stripped of newline).
        """
        buffer = ""
        for chunk in self.download_stream(blob_path):
            buffer += chunk.decode(encoding)
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if line:
                    yield line
        # Yield any remaining content
        if buffer.strip():
            yield buffer.strip()

    def list_blobs(
        self, prefix: str, *, max_results: int | None = None
    ) -> list[str]:
        """List blob names under a prefix."""
        container = self._get_container_client()
        blobs = container.list_blobs(name_starts_with=prefix)
        result = []
        for blob in blobs:
            result.append(blob.name)
            if max_results and len(result) >= max_results:
                break
        return result

    # ── Write Operations ────────────────────────────────────────────────────

    def upload_text(
        self,
        blob_path: str,
        content: str,
        *,
        content_type: str = "text/plain",
        overwrite: bool = True,
    ) -> None:
        """Upload text content as a blob."""
        client = self._get_blob_client(blob_path)
        client.upload_blob(
            content.encode("utf-8"),
            overwrite=overwrite,
            content_settings=ContentSettings(content_type=content_type),
        )
        logger.debug("Uploaded %s (%d bytes)", blob_path, len(content))

    def upload_json(
        self, blob_path: str, data: Any, *, overwrite: bool = True
    ) -> None:
        """Upload data as a JSON blob (using orjson for speed)."""
        encoded = orjson.dumps(data, option=orjson.OPT_INDENT_2).decode("utf-8")
        self.upload_text(blob_path, encoded, content_type="application/json",
                         overwrite=overwrite)

    def upload_csv(
        self,
        blob_path: str,
        content: str,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload CSV content as a blob."""
        self.upload_text(blob_path, content, content_type="text/csv",
                         overwrite=overwrite)

    # ── Append Blob Operations (streaming writes) ───────────────────────────

    def create_append_blob(
        self,
        blob_path: str,
        *,
        content_type: str = "application/x-ndjson",
    ) -> BlobClient:
        """Create an empty append blob for streaming writes.

        Append blobs support up to 195 GB and 50,000 appends.
        Each append can be up to 4 MB.
        """
        client = self._get_append_blob_client(blob_path)
        try:
            client.create_append_blob(
                content_settings=ContentSettings(content_type=content_type)
            )
            logger.debug("Created append blob: %s", blob_path)
        except Exception as e:
            if "BlobAlreadyExists" in str(e):
                logger.debug("Append blob already exists: %s", blob_path)
            else:
                raise
        return client

    def append_to_blob(
        self,
        blob_path: str,
        data: str | bytes,
    ) -> None:
        """Append data to an existing append blob.

        Handles chunking for data > 4 MB (append blob per-block limit).
        """
        client = self._get_append_blob_client(blob_path)
        if isinstance(data, str):
            data = data.encode("utf-8")

        # Append blob has a 4 MB per-block limit
        MAX_BLOCK = 4 * 1024 * 1024
        offset = 0
        while offset < len(data):
            chunk = data[offset : offset + MAX_BLOCK]
            client.append_block(chunk)
            offset += len(chunk)

    def append_jsonl(
        self,
        blob_path: str,
        records: list[dict[str, Any]],
    ) -> None:
        """Append records as JSONL (one JSON object per line) to append blob.

        Matches PAX's per-page JSONL flush pattern.
        """
        if not records:
            return

        lines = []
        for record in records:
            lines.append(orjson.dumps(record).decode("utf-8"))
        payload = "\n".join(lines) + "\n"
        self.append_to_blob(blob_path, payload)

    # ── State Management ────────────────────────────────────────────────────

    def load_run_state(self, run_date: str) -> dict[str, Any] | None:
        """Load the latest run state for a given date from ADLS.

        Checks `pipeline/state/run_{date}_*.json` for incomplete runs.
        """
        prefix = f"{config.PIPELINE_STATE_PREFIX}/run_{run_date}"
        blobs = self.list_blobs(prefix)

        if not blobs:
            return None

        # Get the most recent state file
        latest = sorted(blobs)[-1]
        logger.info("Found previous run state: %s", latest)

        try:
            return self.download_json(latest)
        except Exception as e:
            logger.warning("Failed to load run state %s: %s", latest, e)
            return None

    def save_run_state(self, run_id: str, state: dict[str, Any]) -> str:
        """Save run state to ADLS for cross-run resume."""
        blob_path = f"{config.PIPELINE_STATE_PREFIX}/{run_id}.json"
        self.upload_json(blob_path, state)
        logger.info("Saved run state to %s", blob_path)
        return blob_path

    def save_run_metadata(self, run_id: str, metadata: dict[str, Any]) -> str:
        """Save run metadata / metrics to ADLS history."""
        blob_path = f"{config.PIPELINE_HISTORY_PREFIX}/{run_id}_metadata.json"
        self.upload_json(blob_path, metadata)
        logger.info("Saved run metadata to %s", blob_path)
        return blob_path

    # ── Cleanup ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the underlying service client."""
        if self._service_client:
            self._service_client.close()
            self._service_client = None
            self._container_client = None
