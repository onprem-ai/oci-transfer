"""Durable asynchronous OCI copy manager."""

from __future__ import annotations

import asyncio
import builtins
import inspect
import json
import os
import secrets
import shutil
import tempfile
import uuid
from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import Any

from opai_oci_transfer.client import AsyncOCIClient
from opai_oci_transfer.credentials import AnonymousCredentialProvider, CredentialProvider
from opai_oci_transfer.models import (
    BlobProgress,
    CopyError,
    CopyJob,
    CredentialRequest,
    JobState,
    OCITransferError,
    PruneResult,
    RegistryCredentials,
    ReplacementPolicy,
    TerminalState,
    normalize_platforms,
    parse_reference,
    validate_provider_id,
)
from opai_oci_transfer.store import TERMINAL, SQLiteQueueStore, safe_error

UpdateCallback = Callable[[CopyJob], Awaitable[None]]


class CopyManager:
    def __init__(
        self,
        database_path: Path,
        client: AsyncOCIClient,
        *,
        workers: int = 1,
        requests_per_registry: int = 3,
        source_credentials: CredentialProvider | None = None,
        destination_credentials: CredentialProvider | None = None,
        credential_providers: Iterable[CredentialProvider] = (),
        insecure_registries: Iterable[str] = (),
        registry_ca_files: dict[str, Path] | None = None,
        initial_backoff: float = 0.5,
        max_backoff: float = 60.0,
        no_progress_timeout: float = 3600.0,
        overall_timeout: float = 0.0,
        integrity_retries: int = 2,
        poll_interval: float = 0.2,
        lease_seconds: int = 60,
        progress_interval: float = 0.2,
    ) -> None:
        if workers < 1 or requests_per_registry < 1:
            raise ValueError("concurrency limits must be positive")
        if initial_backoff <= 0 or max_backoff < initial_backoff:
            raise ValueError("invalid backoff")
        if no_progress_timeout <= 0 or overall_timeout < 0:
            raise ValueError("invalid timeout")
        if (
            integrity_retries < 0
            or poll_interval <= 0
            or lease_seconds < 30
            or progress_interval <= 0
        ):
            raise ValueError("invalid manager configuration")
        self.client, self.workers, self.requests_per_registry = (
            client,
            workers,
            requests_per_registry,
        )
        self.initial_backoff, self.max_backoff = initial_backoff, max_backoff
        self.no_progress_timeout, self.overall_timeout = no_progress_timeout, overall_timeout
        self.integrity_retries, self.poll_interval, self.lease_seconds = (
            integrity_retries,
            poll_interval,
            lease_seconds,
        )
        self.progress_interval = progress_interval
        self.store = SQLiteQueueStore(database_path)
        anonymous = AnonymousCredentialProvider()
        source = source_credentials or anonymous
        destination = destination_credentials or anonymous
        self._providers: dict[str, CredentialProvider] = {}
        for provider in (source, destination, *credential_providers):
            if provider.id not in self._providers:
                self.register_provider(provider)
        self._source_default, self._destination_default = source.id, destination.id
        self._insecure = tuple(sorted({host.lower() for host in insecure_registries}))
        self._ca_files = {
            host.lower(): Path(path).expanduser().resolve()
            for host, path in (registry_ca_files or {}).items()
        }
        self._worker_id = f"{os.getpid()}-{uuid.uuid4().hex}"
        self._tasks: list[asyncio.Task[None]] = []
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._closing = False
        self._process: asyncio.subprocess.Process | None = None
        self._runtime: Path | None = None
        self._operations: dict[str, str] = {}

    def register_provider(self, provider: CredentialProvider) -> None:
        provider_id = validate_provider_id(provider.id)
        if not inspect.iscoroutinefunction(provider.get_credentials):
            raise TypeError("credential provider must define async get_credentials")
        existing = self._providers.get(provider_id)
        if existing is not None and existing is not provider:
            raise ValueError(f"duplicate provider id: {provider_id}")
        self._providers[provider_id] = provider

    async def start(self) -> None:
        if self._tasks:
            return
        self._closing = False
        self._stop.clear()
        await self._start_service()
        self._tasks = [
            asyncio.create_task(self._worker(i), name=f"oci-copy-{i}") for i in range(self.workers)
        ]

    async def close(self) -> None:
        self._closing = True
        self._stop.set()
        self._wake.set()
        shutdown_errors: list[OCITransferError] = []
        for operation in tuple(self._operations.values()):
            try:
                await self.client.cancel(operation)
            except OCITransferError as exc:
                # Complete all local cleanup, then preserve the actionable
                # service failure for the caller of close().
                shutdown_errors.append(exc)
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await asyncio.to_thread(self.store.release, self._worker_id)
        # Close the UDS connection pool before terminating its server.
        await self.client.aclose()
        if self._process is not None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.communicate(), 5)
            except TimeoutError:
                self._process.kill()
                await self._process.communicate()
            self._process = None
        if self._runtime is not None:
            await asyncio.to_thread(shutil.rmtree, self._runtime, True)
            self._runtime = None
        # Allow a manager to be restarted after close; _attach creates a fresh pool.
        self.client._closed = False
        if shutdown_errors:
            details = "; ".join(safe_error(error) for error in shutdown_errors)
            raise OCITransferError(
                f"failed to cancel active operations during shutdown: {details}",
                code="protocol_error",
            )

    async def __aenter__(self) -> CopyManager:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def enqueue(
        self,
        source: str,
        destination: str,
        *,
        platforms: str | tuple[str, ...] | None = "all",
        source_credentials: CredentialProvider | None = None,
        destination_credentials: CredentialProvider | None = None,
        copy_referrers: bool = True,
        copy_digest_tags: bool = True,
        replacement_policy: ReplacementPolicy = "no_clobber",
    ) -> CopyJob:
        parse_reference(source)
        parse_reference(destination)
        selected = normalize_platforms(platforms)
        if replacement_policy not in {"no_clobber", "overwrite"}:
            raise ValueError("invalid replacement policy")
        for provider in (source_credentials, destination_credentials):
            if provider is not None:
                self.register_provider(provider)
        source_id = source_credentials.id if source_credentials else self._source_default
        destination_id = (
            destination_credentials.id if destination_credentials else self._destination_default
        )
        job = await asyncio.to_thread(
            self.store.enqueue,
            {
                "source": source,
                "destination": destination,
                "platforms_json": json.dumps(selected),
                "source_provider_id": source_id,
                "destination_provider_id": destination_id,
                "copy_referrers": int(copy_referrers),
                "copy_digest_tags": int(copy_digest_tags),
                "replacement_policy": replacement_policy,
            },
        )
        self._wake.set()
        return job

    async def get(self, job_id: str) -> CopyJob:
        return await asyncio.to_thread(self.store.get, job_id)

    async def list(
        self, *, limit: int = 100, state: JobState | None = None
    ) -> builtins.list[CopyJob]:
        return await asyncio.to_thread(self.store.list, limit, state)

    async def errors(self, job_id: str) -> builtins.list[CopyError]:
        return await asyncio.to_thread(self.store.errors, job_id)

    async def blobs(self, job_id: str) -> builtins.list[BlobProgress]:
        return await asyncio.to_thread(self.store.blobs, job_id)

    async def cancel(self, job_id: str) -> CopyJob:
        await asyncio.to_thread(self.store.cancel, job_id)
        operation = self._operations.get(job_id)
        if operation:
            await self.client.cancel(operation)
        self._wake.set()
        return await self.get(job_id)

    async def retry(self, job_id: str) -> CopyJob:
        job = await asyncio.to_thread(self.store.retry, job_id)
        self._wake.set()
        return job

    async def dismiss(self, job_id: str) -> None:
        """Delete a terminal copy job and its durable history."""
        await asyncio.to_thread(self.store.dismiss, job_id)

    async def wait(
        self,
        job_id: str,
        *,
        on_update: UpdateCallback | None = None,
        poll_interval: float | None = None,
    ) -> CopyJob:
        if on_update is not None and not inspect.iscoroutinefunction(on_update):
            raise TypeError("on_update must be an async callable")
        interval, previous = poll_interval or self.poll_interval, None
        if interval <= 0:
            raise ValueError("poll_interval must be positive")
        while True:
            job = await self.get(job_id)
            if job != previous and on_update is not None:
                await on_update(job)
            if job.state in TERMINAL:
                return job
            previous = job
            await asyncio.sleep(interval)

    async def prune(
        self,
        *,
        states: Iterable[TerminalState] | None = None,
        older_than: datetime | str | None = None,
        dry_run: bool = False,
    ) -> PruneResult:
        state_tuple = tuple(states) if states is not None else None
        older = older_than.isoformat() if isinstance(older_than, datetime) else older_than
        return await asyncio.to_thread(self.store.prune, state_tuple, older, dry_run)

    async def _start_service(self) -> None:
        override = os.environ.get("OPAI_OCI_TRANSFER_TEST_BINARY")
        binary = (
            Path(override)
            if override
            else Path(str(files("opai_oci_transfer").joinpath("_bin/opai-oci-transferd")))
        )
        if not binary.is_file():
            raise RuntimeError("bundled opai-oci-transferd binary is unavailable")
        self._runtime = Path(await asyncio.to_thread(tempfile.mkdtemp, prefix="opai-oci-transfer-"))
        await asyncio.to_thread(self._runtime.chmod, 0o700)
        socket = self._runtime / "service.sock"
        args = [
            os.fspath(binary),
            "--socket",
            os.fspath(socket),
            "--max-operations",
            str(self.workers),
            "--requests-per-registry",
            str(self.requests_per_registry),
        ]
        for host in self._insecure:
            args.extend(("--insecure-registry", host))
        for host, path in self._ca_files.items():
            args.extend(("--registry-ca", f"{host}={path}"))
        self._process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        self.client._attach(socket)
        deadline = asyncio.get_running_loop().time() + 10
        while asyncio.get_running_loop().time() < deadline:
            if self._process.returncode is not None:
                raise RuntimeError("transfer service exited during startup")
            try:
                await self.client.health()
                return
            except OCITransferError:
                await asyncio.sleep(0.05)
        raise RuntimeError("transfer service readiness timed out")

    async def _worker(self, index: int) -> None:
        worker = f"{self._worker_id}-{index}"
        while not self._stop.is_set():
            claim = await asyncio.to_thread(self.store.claim, worker, self.lease_seconds)
            if claim is None:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), self.poll_interval)
                except TimeoutError:
                    pass
                continue
            job, token = claim
            try:
                await self._execute(job, token)
            except asyncio.CancelledError:
                await asyncio.to_thread(self.store.release_claim, job.id, token)
                raise
            except Exception as exc:
                detail = safe_error(exc)
                await self._record_failure(
                    job.id,
                    token,
                    OCITransferError(
                        f"Copy failed ({type(exc).__name__}): {detail}",
                        code="transfer_failed",
                    ),
                )

    async def _credentials(
        self, details: dict[str, Any], side: str, refresh: bool = False
    ) -> RegistryCredentials | None:
        provider = self._providers.get(details[f"{side}_provider_id"])
        if provider is None:
            raise OCITransferError(
                "credential provider is not registered", code="credential_provider_unavailable"
            )
        registry, repository = parse_reference(
            details["source" if side == "source" else "destination"]
        )
        return await provider.get_credentials(
            CredentialRequest(registry, repository, "pull" if side == "source" else "push"),
            refresh=refresh,
        )

    async def _execute(self, job: CopyJob, token: str) -> None:
        heartbeat = asyncio.create_task(self._heartbeat(job.id, token))
        try:
            details = await asyncio.to_thread(self.store.details, job.id)
            source_creds = await self._credentials(details, "source")
            destination_creds = await self._credentials(details, "destination")
            common = {
                "destination": details["destination"],
                "platforms": json.loads(details["platforms_json"]),
                "copy_referrers": bool(details["copy_referrers"]),
                "copy_digest_tags": bool(details["copy_digest_tags"]),
                "replacement_policy": details["replacement_policy"],
                "source_credentials": _credential_dict(source_creds),
                "destination_credentials": _credential_dict(destination_creds),
            }
            snapshot = await asyncio.to_thread(self.store.snapshot, job.id)
            if snapshot is None:
                planned = await self.client.plan(
                    {"protocol_version": 1, "source": details["source"], **common}
                )
                if not await asyncio.to_thread(self.store.save_snapshot, job.id, token, planned):
                    return
                snapshot = json.loads(json.dumps(planned, default=lambda value: value.__dict__))
            operation = secrets.token_urlsafe(24)
            self._operations[job.id] = operation
            terminal = False
            async for event in self.client.copy(
                operation,
                {
                    "protocol_version": 1,
                    "snapshot": snapshot,
                    "source_credentials": common["source_credentials"],
                    "destination_credentials": common["destination_credentials"],
                },
            ):
                if await asyncio.to_thread(self.store.cancellation_requested, job.id, token):
                    await self.client.cancel(operation)
                    await asyncio.to_thread(self.store.finish, job.id, token, "cancelled")
                    return
                kind = event.get("type")
                if kind in {"phase", "progress"}:
                    if not await asyncio.to_thread(self.store.progress, job.id, token, event):
                        return
                elif kind == "failed":
                    terminal = True
                    raise OCITransferError(
                        event.get("message", "transfer failed"),
                        code=event.get("code", "transfer_failed"),
                        retryable=bool(event.get("retryable")),
                        retry_after=event.get("retry_after"),
                    )
                elif kind == "completed":
                    terminal = True
            if not terminal:
                raise OCITransferError("copy ended without result", code="protocol_error")
            await asyncio.to_thread(self.store.progress, job.id, token, {"phase": "verifying"})
            await asyncio.to_thread(self.store.finish, job.id, token, "completed")
        except OCITransferError as exc:
            if await asyncio.to_thread(self.store.cancellation_requested, job.id, token):
                await asyncio.to_thread(self.store.finish, job.id, token, "cancelled")
            else:
                if exc.code == "authentication_failed":
                    # Refresh both independently scoped capabilities. A provider
                    # may return the same value when it cannot refresh.
                    await self._credentials(details, "source", refresh=True)
                    await self._credentials(details, "destination", refresh=True)
                    exc = OCITransferError(
                        str(exc), code=exc.code, retryable=True, retry_after=exc.retry_after
                    )
                await self._record_failure(job.id, token, exc)
        finally:
            self._operations.pop(job.id, None)
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _record_failure(self, job_id: str, token: str, exc: OCITransferError) -> None:
        details = await asyncio.to_thread(self.store.details, job_id)
        retryable = exc.retryable
        original_detail = safe_error(exc)
        integrity_failure = exc.code == "digest_mismatch"
        if integrity_failure and details["integrity_failures"] >= self.integrity_retries:
            detail = original_detail
            retryable = False
            exc = OCITransferError(
                f"Copy failed repeated integrity verification: {detail}",
                code="integrity_retries_exhausted",
            )
        last = datetime.fromisoformat(details["last_progress_at"])
        if (
            retryable
            and (datetime.now(last.tzinfo) - last).total_seconds() >= self.no_progress_timeout
        ):
            detail = original_detail
            retryable, exc = (
                False,
                OCITransferError(
                    f"Copy made no progress before the configured timeout: {detail}",
                    code="no_progress_timeout",
                ),
            )
        if self.overall_timeout and details["started_at"]:
            started = datetime.fromisoformat(details["started_at"])
            if (datetime.now(started.tzinfo) - started).total_seconds() >= self.overall_timeout:
                detail = original_detail
                retryable, exc = (
                    False,
                    OCITransferError(
                        f"Copy exceeded the configured overall timeout: {detail}",
                        code="overall_timeout",
                    ),
                )
        if exc.retry_after is not None:
            delay = min(self.max_backoff, max(0.0, exc.retry_after))
        else:
            cap = min(
                self.max_backoff,
                self.initial_backoff * 2 ** min(details["consecutive_failures"], 16),
            )
            delay = secrets.randbelow(max(1, int(cap * 1000) + 1)) / 1000
        await asyncio.to_thread(
            self.store.fail_or_retry,
            job_id,
            token,
            code=exc.code,
            message=str(exc),
            retryable=retryable,
            delay=delay,
            integrity_failure=integrity_failure,
        )
        if retryable:
            self._wake.set()

    async def _heartbeat(self, job_id: str, token: str) -> None:
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            if not await asyncio.to_thread(self.store.heartbeat, job_id, token, self.lease_seconds):
                return


def _credential_dict(value: RegistryCredentials | None) -> dict[str, Any] | None:
    return (
        None
        if value is None
        else {
            "username": value.username,
            "password": value.password,
            "expires_at": value.expires_at,
        }
    )
