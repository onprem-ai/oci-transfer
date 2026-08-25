import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import opai_oci_transfer.store as store_module
from opai_oci_transfer import AsyncOCIClient, CopyManager, OCITransferError
from opai_oci_transfer.credentials import OnPremLicenseProvider
from opai_oci_transfer.models import (
    CopyJob,
    Descriptor,
    ImageSnapshot,
    JobNotFoundError,
    normalize_platforms,
    parse_reference,
    validate_digest,
    validate_provider_id,
)
from opai_oci_transfer.store import SQLiteQueueStore


def test_model_edge_cases() -> None:
    with pytest.raises(ValueError):
        Descriptor("bad", "x", 1)
    with pytest.raises(ValueError):
        Descriptor("sha256:" + "a" * 64, "x", -1)
    unknown = ImageSnapshot.create(
        source="a.example/x:v1",
        resolved_source="a.example/x@sha256:" + "a" * 64,
        destination="b.example/x:v1",
        root_digest="sha256:" + "a" * 64,
        root_media_type="m",
        root_size=1,
        blobs=(Descriptor("sha256:" + "b" * 64, "x", None),),
    )
    assert unknown.expected_bytes is None
    base = dict(
        id="x",
        source="a",
        resolved_source=None,
        destination="b",
        state="queued",
        completed_bytes=0,
        expected_bytes=None,
        network_bytes=0,
        completed_blobs=0,
        total_blobs=None,
        bytes_per_second=None,
        run_count=0,
        consecutive_failures=0,
        next_retry_at=None,
        last_progress_at="x",
        snapshot_digest=None,
        error_code=None,
        error_message=None,
        created_at="x",
        updated_at="x",
        started_at=None,
        completed_at=None,
        worker_id=None,
        lease_expires_at=None,
        heartbeat_at=None,
    )
    assert CopyJob(**base).percent is None
    assert CopyJob(**(base | {"expected_bytes": 0})).percent == 0
    assert CopyJob(**(base | {"expected_bytes": 0, "state": "completed"})).percent == 100
    assert CopyJob(**(base | {"expected_bytes": 2, "completed_bytes": 3})).percent == 100
    assert CopyJob(**base).to_dict()["id"] == "x"
    for bad in ("", "bad id"):
        with pytest.raises(ValueError):
            validate_provider_id(bad)
    for bad in ("bad", "example.com/x", "example.com/x@sha256:no", "example.com:99999/x:v"):
        with pytest.raises(ValueError):
            parse_reference(bad)
    with pytest.raises(ValueError):
        validate_digest("sha256:abc")
    for bad in ((), ("linux/amd64", "linux/amd64"), ("BAD",)):
        with pytest.raises(ValueError):
            normalize_platforms(bad)


@pytest.mark.asyncio
async def test_onprem_bad_response_and_owned_lifecycle() -> None:
    async def key() -> str:
        return "value"

    provider = OnPremLicenseProvider(
        "https://license.example",
        key,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))),
    )
    with pytest.raises(OCITransferError) as caught:
        await provider.get_credentials(
            __import__("opai_oci_transfer").CredentialRequest("r", "x", "pull")
        )
    assert caught.value.retryable
    assert "HTTP 500" in str(caught.value)
    await provider.aclose()
    owned = OnPremLicenseProvider("https://license.example", key)
    async with owned as entered:
        assert entered is owned


def values() -> dict[str, object]:
    return {
        "source": "a.example/x:v1",
        "destination": "b.example/x:v1",
        "platforms_json": "[]",
        "source_provider_id": "anonymous",
        "destination_provider_id": "anonymous",
        "copy_referrers": 1,
        "copy_digest_tags": 1,
        "replacement_policy": "no_clobber",
    }


def snap() -> ImageSnapshot:
    return ImageSnapshot.create(
        source="a.example/x:v1",
        resolved_source="a.example/x@sha256:" + "a" * 64,
        destination="b.example/x:v1",
        root_digest="sha256:" + "a" * 64,
        root_media_type="m",
        root_size=1,
        blobs=(Descriptor("sha256:" + "b" * 64, "x", None),),
    )


def test_store_additional_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = SQLiteQueueStore(tmp_path / "q.sqlite")
    assert db.claim("w", 60) is None
    job = db.enqueue(values())
    claimed, token = db.claim("w", 60) or (None, None)
    assert claimed and token
    assert db.snapshot(job.id) is None
    assert not db.save_snapshot(job.id, "bad", snap())
    assert db.save_snapshot(job.id, token, snap())
    assert db.snapshot(job.id) is not None
    assert not db.progress(job.id, token, {"digest": "sha256:" + "c" * 64})
    assert not db.finish(job.id, "bad", "completed")
    assert not db.cancellation_requested(job.id, token)
    assert db.cancellation_requested(job.id, "bad")
    with pytest.raises(JobNotFoundError):
        db.details("bad")
    monkeypatch.setattr(store_module.SQLiteQueueStore, "_connect", lambda self: MagicMock())
    with pytest.raises(RuntimeError):
        SQLiteQueueStore(tmp_path / "other")


@pytest.mark.asyncio
async def test_manager_timeout_failure_paths(tmp_path: Path) -> None:
    manager = CopyManager(tmp_path / "q.sqlite", AsyncOCIClient(), overall_timeout=0.001)
    job = await manager.enqueue("a.example/x:v1", "b.example/x:v1")
    claimed, token = await asyncio.to_thread(manager.store.claim, "w", 60) or (None, None)
    assert claimed and token
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    with manager.store._connect() as db:
        db.execute(
            "UPDATE copy_jobs SET last_progress_at=?,started_at=? WHERE id=?", (old, old, job.id)
        )
    await manager._record_failure(job.id, token, OCITransferError("temp", retryable=True))
    assert (await manager.get(job.id)).error_code == "overall_timeout"


@pytest.mark.asyncio
async def test_old_permanent_failure_is_not_mislabeled_no_progress(tmp_path: Path) -> None:
    manager = CopyManager(tmp_path / "permanent.sqlite", AsyncOCIClient())
    job = await manager.enqueue("a.example/x:v1", "b.example/x:v1")
    claimed, token = await asyncio.to_thread(manager.store.claim, "w", 60) or (None, None)
    assert claimed and token
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    with manager.store._connect() as db:
        db.execute("UPDATE copy_jobs SET last_progress_at=? WHERE id=?", (old, job.id))
    await manager._record_failure(
        job.id,
        token,
        OCITransferError("manifest has no config", code="manifest_invalid", retryable=False),
    )
    result = await manager.get(job.id)
    assert result.error_code == "manifest_invalid"
    assert result.error_message == "manifest has no config"


@pytest.mark.asyncio
async def test_manager_active_cancel(tmp_path: Path) -> None:
    manager = CopyManager(tmp_path / "q.sqlite", AsyncOCIClient())
    job = await manager.enqueue("a.example/x:v1", "b.example/x:v1")
    await asyncio.to_thread(manager.store.claim, "w", 60)
    manager._operations[job.id] = "operation"
    manager.client.cancel = AsyncMock(side_effect=OCITransferError("gone"))  # type: ignore[method-assign]
    with pytest.raises(OCITransferError, match="gone"):
        await manager.cancel(job.id)
    assert (await manager.get(job.id)).state == "cancel_requested"
