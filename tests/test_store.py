from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from opai_oci_transfer.models import Descriptor, ImageSnapshot, JobConflictError, JobNotFoundError
from opai_oci_transfer.store import SQLiteQueueStore, safe_error


def values(source: str = "a.example/x:v1") -> dict[str, object]:
    return {
        "source": source,
        "destination": "b.example/x:v1",
        "platforms_json": "[]",
        "source_provider_id": "anonymous",
        "destination_provider_id": "anonymous",
        "copy_referrers": 1,
        "copy_digest_tags": 1,
        "replacement_policy": "no_clobber",
    }


def snapshot() -> ImageSnapshot:
    return ImageSnapshot.create(
        source="a.example/x:v1",
        resolved_source="a.example/x@sha256:" + "a" * 64,
        destination="b.example/x:v1",
        root_digest="sha256:" + "a" * 64,
        root_media_type="manifest",
        root_size=10,
        blobs=(
            Descriptor("sha256:" + "b" * 64, "layer", 10),
            Descriptor("sha256:" + "c" * 64, "layer", 5),
        ),
    )


def store(tmp_path: Path) -> SQLiteQueueStore:
    return SQLiteQueueStore(tmp_path / "queue.sqlite")


def test_enqueue_conflict_listing_and_missing(tmp_path: Path) -> None:
    db = store(tmp_path)
    first = db.enqueue(values())
    assert db.enqueue(values()).id == first.id
    with pytest.raises(JobConflictError):
        db.enqueue(values("a.example/x:v2"))
    assert db.list(1, "queued") == [first]
    with pytest.raises(ValueError):
        db.list(0, None)
    with pytest.raises(JobNotFoundError):
        db.get("missing")


def test_claim_snapshot_progress_finish(tmp_path: Path) -> None:
    db = store(tmp_path)
    job = db.enqueue(values())
    claimed, token = db.claim("worker", 60) or (None, None)
    assert claimed is not None and claimed.state == "planning" and token
    assert db.heartbeat(job.id, token, 60)
    assert not db.heartbeat(job.id, "bad", 60)
    snap = snapshot()
    assert db.save_snapshot(job.id, token, snap)
    assert not db.save_snapshot(job.id, "bad", snap)
    digest = snap.blobs[0].digest
    assert db.progress(job.id, token, {"digest": digest, "offset": 4, "network_bytes": 4})
    assert db.progress(job.id, token, {"digest": digest, "offset": 2, "network_bytes": 2})
    assert db.progress(job.id, token, {"digest": digest, "disposition": "reused"})
    assert db.progress(job.id, token, {"phase": "publishing"})
    assert db.progress(job.id, token, {"phase": "unknown"})
    current = db.get(job.id)
    assert current.completed_bytes == 10 and current.network_bytes == 4
    assert db.blobs(job.id)[0].disposition == "reused"
    assert db.finish(job.id, token, "completed")
    assert db.get(job.id).state == "completed"


def test_failure_retry_cancel_release_and_prune(tmp_path: Path) -> None:
    db = store(tmp_path)
    job = db.enqueue(values())
    _, token = db.claim("worker", 60) or (None, None)
    assert token
    assert db.fail_or_retry(
        job.id,
        token,
        code="network_error",
        message="Bearer abc https://bad",
        retryable=True,
        delay=0,
    )
    assert db.get(job.id).state == "retry_wait"
    assert "abc" not in db.errors(job.id)[0].message
    claimed, token = db.claim("worker", 60) or (None, None)
    assert claimed and token
    assert db.fail_or_retry(job.id, token, code="bad", message="bad", retryable=False, delay=None)
    assert db.retry(job.id).state == "queued"
    assert db.cancel(job.id).state == "cancelled"
    with pytest.raises(JobConflictError):
        db.retry(job.id)
    preview = db.prune(("cancelled",), None, True)
    assert preview.jobs == 1 and preview.errors == 2
    assert db.prune(None, (datetime.now(UTC) + timedelta(seconds=1)).isoformat(), False).jobs == 1
    with pytest.raises(ValueError):
        db.prune(None, None, False)
    with pytest.raises(ValueError):
        db.prune(("queued",), None, False)  # type: ignore[arg-type]


def test_release_and_safe_error(tmp_path: Path) -> None:
    db = store(tmp_path)
    job = db.enqueue(values())
    db.claim("worker", 60)
    db.release("worker")
    assert db.get(job.id).state == "queued"
    assert "secret" not in safe_error("https://host/path token=secret")
