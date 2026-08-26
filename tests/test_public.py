import asyncio
from pathlib import Path

import pytest

from opai_oci_transfer import (
    AsyncOCIClient,
    CopyManager,
    ImageSnapshot,
    RegistryCredentials,
    StaticCredentialProvider,
)
from opai_oci_transfer.models import Descriptor, normalize_platforms, parse_reference


def test_snapshot_is_deterministic() -> None:
    kwargs = dict(
        source="a.example/team/app:v1",
        resolved_source="a.example/team/app@sha256:" + "a" * 64,
        destination="b.example/team/app:v1",
        root_digest="sha256:" + "a" * 64,
        root_media_type="application/vnd.oci.image.manifest.v1+json",
        root_size=10,
        blobs=(Descriptor("sha256:" + "b" * 64, "x", 4),),
    )
    assert ImageSnapshot.create(**kwargs).digest == ImageSnapshot.create(**kwargs).digest
    assert ImageSnapshot.create(**kwargs).expected_bytes == 4


def test_reference_and_platform_validation() -> None:
    assert parse_reference("registry.example/team/app:v1") == ("registry.example", "team/app")
    assert parse_reference(
        "registry.example/team/app:v1@sha256:" + "a" * 64
    ) == ("registry.example", "team/app")
    assert normalize_platforms(("linux/arm64", "linux/amd64")) == (
        "linux/amd64",
        "linux/arm64",
    )
    with pytest.raises(ValueError):
        parse_reference("ubuntu:latest")
    with pytest.raises(ValueError):
        parse_reference("registry.example/team/app")


@pytest.mark.asyncio
async def test_queue_lifecycle(tmp_path: Path) -> None:
    manager = CopyManager(tmp_path / "queue.sqlite", AsyncOCIClient())
    job = await manager.enqueue("a.example/team/app:v1", "b.example/team/app:v1")
    same = await manager.enqueue("a.example/team/app:v1", "b.example/team/app:v1")
    assert same.id == job.id
    assert (await manager.cancel(job.id)).state == "cancelled"
    assert (await manager.wait(job.id)).state == "cancelled"
    assert (await manager.prune(states=("cancelled",), dry_run=True)).jobs == 1
    await manager.dismiss(job.id)
    with pytest.raises(KeyError):
        await manager.get(job.id)


def test_static_provider_is_async() -> None:
    provider = StaticCredentialProvider("test-provider", RegistryCredentials("u", "p"))
    assert asyncio.iscoroutinefunction(provider.get_credentials)
