from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from opai_oci_transfer.client import AsyncOCIClient, _event, _json, _service_error
from opai_oci_transfer.models import OCITransferError


class AsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk


@pytest.mark.asyncio
async def test_health_and_plan() -> None:
    responses = [
        httpx.Response(200, json={"ready": True, "protocol_version": 1}),
        httpx.Response(
            200,
            json={
                "protocol_version": 1,
                "resolved_source": "a.example/x@sha256:" + "a" * 64,
                "root_digest": "sha256:" + "a" * 64,
                "root_media_type": "manifest",
                "root_size": 5,
                "manifests": [],
                "blobs": [{"digest": "sha256:" + "b" * 64, "media_type": "layer", "size": 2}],
            },
        ),
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    client = AsyncOCIClient()
    client._socket_path = Path("fake")
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://x")
    assert (await client.health())["ready"]
    snapshot = await client.plan(
        {
            "source": "a.example/x:v1",
            "destination": "b.example/x:v1",
            "platforms": [],
            "copy_referrers": True,
            "copy_digest_tags": True,
            "replacement_policy": "no_clobber",
        }
    )
    assert snapshot.expected_bytes == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_streaming_copy_and_cancel() -> None:
    lines = [
        b'{"protocol_version":1,"type":"progress","digest":"sha256:x"}\n',
        b'{"protocol_version":1,"type":"completed"}\n',
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, stream=AsyncStream(lines))

    client = AsyncOCIClient()
    client._socket_path = Path("fake")
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://x")
    events = [event async for event in client.copy("op", {})]
    assert [event["type"] for event in events] == ["progress", "completed"]
    await client.cancel("op")
    await client.aclose()


@pytest.mark.asyncio
async def test_protocol_failures(tmp_path: Path) -> None:
    client = AsyncOCIClient(timeout=0.01)
    with pytest.raises(RuntimeError, match="attached"):
        await client.health()
    client._attach(tmp_path / "missing")
    with pytest.raises(OCITransferError) as caught:
        await client.health()
    assert caught.value.retryable
    await client.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await client.health()
    with pytest.raises(OCITransferError):
        _json(b"[]")
    with pytest.raises(OCITransferError):
        _event(b'{"protocol_version":2}')
    error = _service_error(b'{"code":"rate_limited","message":"later","retryable":true}', 429)
    assert error.code == "rate_limited" and error.retryable
    fallback = _service_error(b"bad", 500)
    assert fallback.code == "protocol_error"


def test_encode_size_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    import opai_oci_transfer.client as module

    monkeypatch.setattr(module, "_MAX_RESPONSE", 2)
    with pytest.raises(ValueError):
        module._encode({"too": "large"})
