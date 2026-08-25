import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from opai_oci_transfer import AsyncOCIClient, CopyManager, OCITransferError
from opai_oci_transfer.models import Descriptor, ImageSnapshot, JobConflictError


class FakeClient(AsyncOCIClient):
    def __init__(self, *, failure: OCITransferError | None = None) -> None:
        super().__init__()
        self.failure = failure
        self.cancelled: list[str] = []

    async def plan(self, payload: dict[str, Any]) -> ImageSnapshot:
        return ImageSnapshot.create(
            source=payload["source"],
            resolved_source=payload["source"].split(":v1")[0] + "@sha256:" + "a" * 64,
            destination=payload["destination"],
            root_digest="sha256:" + "a" * 64,
            root_media_type="manifest",
            root_size=10,
            blobs=(Descriptor("sha256:" + "b" * 64, "layer", 5),),
        )

    async def copy(
        self, operation_id: str, payload: dict[str, Any]
    ) -> AsyncIterator[dict[str, Any]]:
        del operation_id, payload
        if self.failure:
            raise self.failure
        yield {"type": "phase", "phase": "copying"}
        yield {
            "type": "progress",
            "digest": "sha256:" + "b" * 64,
            "offset": 5,
            "network_bytes": 5,
            "disposition": "completed",
        }
        yield {"type": "phase", "phase": "publishing"}
        yield {"type": "completed"}

    async def cancel(self, operation_id: str) -> None:
        self.cancelled.append(operation_id)


async def manager(tmp_path: Path, client: AsyncOCIClient, **kwargs: Any) -> CopyManager:
    value = CopyManager(tmp_path / "q.sqlite", client, poll_interval=0.001, **kwargs)
    value._start_service = AsyncMock()  # type: ignore[method-assign]
    return value


@pytest.mark.asyncio
async def test_worker_completes_and_wait_updates(tmp_path: Path) -> None:
    value = await manager(tmp_path, FakeClient())
    seen: list[str] = []

    async def update(job: Any) -> None:
        seen.append(job.state)

    job = await value.enqueue("a.example/team/app:v1", "b.example/team/app:v1")
    async with value:
        result = await asyncio.wait_for(value.wait(job.id, on_update=update), 2)
        await value.start()
    assert result.state == "completed"
    assert result.snapshot_digest and result.completed_bytes == 5
    assert result.percent == 100
    assert seen[-1] == "completed"


@pytest.mark.asyncio
async def test_worker_records_permanent_failure(tmp_path: Path) -> None:
    client = FakeClient(failure=OCITransferError("bad token=secret", code="authorization_denied"))
    value = await manager(tmp_path, client)
    job = await value.enqueue("a.example/team/app:v1", "b.example/team/app:v1")
    await value.start()
    result = await asyncio.wait_for(value.wait(job.id), 2)
    await value.close()
    assert result.state == "failed" and result.error_code == "authorization_denied"
    assert "secret" not in (result.error_message or "")


@pytest.mark.asyncio
async def test_worker_retries_transient_failure(tmp_path: Path) -> None:
    client = FakeClient(failure=OCITransferError("temporary", code="network_error", retryable=True))
    value = await manager(tmp_path, client, initial_backoff=0.001, max_backoff=0.001)
    job = await value.enqueue("a.example/team/app:v1", "b.example/team/app:v1")
    await value.start()
    for _ in range(200):
        errors = await value.errors(job.id)
        if errors:
            break
        await asyncio.sleep(0.001)
    await value.close()
    assert (await value.get(job.id)).state in {"retry_wait", "copying"}
    assert errors[0].retryable


@pytest.mark.asyncio
async def test_validation_callbacks_and_conflicts(tmp_path: Path) -> None:
    value = await manager(tmp_path, FakeClient())
    with pytest.raises(ValueError):
        await value.enqueue("short:v1", "b.example/x:v1")
    with pytest.raises(ValueError):
        await value.enqueue("a.example/x:v1", "b.example/x:v1", replacement_policy="bad")  # type: ignore[arg-type]
    job = await value.enqueue("a.example/x:v1", "b.example/x:v1")
    with pytest.raises(JobConflictError):
        await value.enqueue("a.example/x:v2", "b.example/x:v1")
    with pytest.raises(TypeError):
        await value.wait(job.id, on_update=lambda _: None)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await value.wait(job.id, poll_interval=-1)
    await value.cancel(job.id)


def test_invalid_manager_configuration(tmp_path: Path) -> None:
    for kwargs in (
        {"workers": 0},
        {"requests_per_registry": 0},
        {"initial_backoff": 0},
        {"initial_backoff": 2, "max_backoff": 1},
        {"no_progress_timeout": 0},
        {"overall_timeout": -1},
        {"integrity_retries": -1},
        {"poll_interval": 0},
        {"lease_seconds": 1},
        {"progress_interval": 0},
    ):
        with pytest.raises(ValueError):
            CopyManager(
                tmp_path / (str(len(kwargs)) + str(kwargs) + ".sqlite"), FakeClient(), **kwargs
            )
