"""Native-async client for the private Unix-socket transfer service."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from opai_oci_transfer.errors import extract_http_error_detail, sanitize_error_detail
from opai_oci_transfer.models import (
    Descriptor,
    ImageSnapshot,
    ManifestRecord,
    OCITransferError,
)

PROTOCOL_VERSION = 1
_MAX_RESPONSE = 2 * 1024 * 1024
_MAX_EVENT = 256 * 1024


class AsyncOCIClient:
    """Protocol client whose Unix socket is attached privately by ``CopyManager``."""

    def __init__(self, *, timeout: float = 30.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.timeout = timeout
        self._socket_path: Path | None = None
        self._http: httpx.AsyncClient | None = None
        self._closed = False

    def _attach(self, socket_path: Path) -> None:
        self._socket_path = socket_path
        self._closed = False
        self._http = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(socket_path)),
            base_url="http://localhost",
            timeout=self.timeout,
            follow_redirects=False,
        )

    async def aclose(self) -> None:
        self._closed = True
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self) -> AsyncOCIClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    def _transport(self) -> httpx.AsyncClient:
        if self._closed:
            raise RuntimeError("client is closed")
        if self._http is None or self._socket_path is None:
            raise RuntimeError("client is not attached to a CopyManager")
        return self._http

    async def health(self) -> dict[str, Any]:
        response = await self._request("GET", "/v1/health")
        if response.status_code != 200:
            raise OCITransferError("transfer service is not ready", code="protocol_error")
        value = _json(response.content)
        if value.get("protocol_version") != PROTOCOL_VERSION or not value.get("ready"):
            raise OCITransferError("incompatible transfer service", code="protocol_error")
        return value

    async def plan(self, payload: dict[str, Any]) -> ImageSnapshot:
        response = await self._request("POST", "/v1/plan", payload)
        if response.status_code != 200:
            raise _service_error(response.content, response.status_code)
        value = _json(response.content)
        if value.get("protocol_version") != PROTOCOL_VERSION:
            raise OCITransferError("incompatible plan response", code="protocol_error")
        try:
            manifests = tuple(ManifestRecord(**item) for item in value.get("manifests", []))
            blobs = tuple(Descriptor(**item) for item in value.get("blobs", []))
            snapshot = ImageSnapshot.create(
                source=payload["source"],
                resolved_source=value["resolved_source"],
                destination=payload["destination"],
                root_digest=value["root_digest"],
                root_media_type=value["root_media_type"],
                root_size=value["root_size"],
                platforms=tuple(payload.get("platforms", [])),
                manifests=manifests,
                blobs=blobs,
                copy_referrers=payload["copy_referrers"],
                copy_digest_tags=payload["copy_digest_tags"],
                replacement_policy=payload["replacement_policy"],
            )
            if supplied := value.get("snapshot_digest"):
                if supplied != snapshot.digest:
                    raise OCITransferError("invalid snapshot digest", code="protocol_error")
            return snapshot
        except OCITransferError:
            raise
        except (KeyError, TypeError, ValueError):
            raise OCITransferError("malformed plan response", code="protocol_error") from None

    async def copy(
        self, operation_id: str, payload: dict[str, Any]
    ) -> AsyncIterator[dict[str, Any]]:
        body = _encode(payload)
        try:
            async with self._transport().stream(
                "POST",
                f"/v1/copies/{quote(operation_id, safe='')}",
                content=body,
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status_code != 200:
                    data = await response.aread()
                    raise _service_error(data[:_MAX_RESPONSE], response.status_code)
                pending = b""
                async for chunk in response.aiter_bytes():
                    pending += chunk
                    if len(pending) > _MAX_EVENT and b"\n" not in pending:
                        raise OCITransferError("oversized service event", code="protocol_error")
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        if len(line) > _MAX_EVENT:
                            raise OCITransferError("oversized service event", code="protocol_error")
                        event = _event(line)
                        yield event
                        if event.get("type") in {"completed", "failed"}:
                            return
                if pending:
                    event = _event(pending)
                    yield event
                    if event.get("type") in {"completed", "failed"}:
                        return
                raise OCITransferError("copy stream ended without result", code="protocol_error")
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            detail = sanitize_error_detail(exc)
            raise OCITransferError(
                f"transfer service unavailable ({type(exc).__name__}): {detail}",
                code="registry_unavailable",
                retryable=True,
            ) from None

    async def cancel(self, operation_id: str) -> None:
        response = await self._request("DELETE", f"/v1/copies/{quote(operation_id, safe='')}")
        if response.status_code not in {200, 202, 204, 404}:
            raise _service_error(response.content, response.status_code)

    async def _request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> httpx.Response:
        body = None if payload is None else _encode(payload)
        try:
            response = await self._transport().request(
                method,
                path,
                content=body,
                headers={"Content-Type": "application/json"} if body is not None else None,
            )
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            detail = sanitize_error_detail(exc)
            raise OCITransferError(
                f"transfer service unavailable ({type(exc).__name__}): {detail}",
                code="registry_unavailable",
                retryable=True,
            ) from None
        if len(response.content) > _MAX_RESPONSE:
            raise OCITransferError("oversized service response", code="protocol_error")
        return response


def _encode(payload: dict[str, Any]) -> bytes:
    body = json.dumps(payload, separators=(",", ":")).encode()
    if len(body) > _MAX_RESPONSE:
        raise ValueError("request is too large")
    return body


def _event(line: bytes) -> dict[str, Any]:
    value = _json(line)
    if value.get("protocol_version") != PROTOCOL_VERSION:
        raise OCITransferError("incompatible service event", code="protocol_error")
    return value


def _json(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(data)
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise OCITransferError("malformed service response", code="protocol_error") from None


def _service_error(body: bytes, status: int) -> OCITransferError:
    try:
        value = _json(body)
    except OCITransferError:
        detail = extract_http_error_detail(body)
        message = f"transfer service returned HTTP {status}"
        if detail:
            message = f"{message}: {detail}"
        return OCITransferError(message, code="protocol_error")
    detail = sanitize_error_detail(value.get("message", "transfer failed"))
    return OCITransferError(
        f"transfer service returned HTTP {status}: {detail}",
        code=str(value.get("code", "transfer_failed")),
        retryable=bool(value.get("retryable", False)),
        retry_after=value.get("retry_after"),
    )
