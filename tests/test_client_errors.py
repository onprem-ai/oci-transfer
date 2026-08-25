from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

import opai_oci_transfer.client as module
from opai_oci_transfer.client import AsyncOCIClient
from opai_oci_transfer.models import OCITransferError


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk


def client(handler: httpx.AsyncBaseTransport) -> AsyncOCIClient:
    value = AsyncOCIClient()
    value._socket_path = Path("fake")
    value._http = httpx.AsyncClient(transport=handler, base_url="http://x")
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(500),
        httpx.Response(200, json={"ready": False, "protocol_version": 1}),
        httpx.Response(200, json={"ready": True, "protocol_version": 2}),
    ],
)
async def test_bad_health(response: httpx.Response) -> None:
    value = client(httpx.MockTransport(lambda request: response))
    with pytest.raises(OCITransferError):
        await value.health()
    await value.aclose()


@pytest.mark.asyncio
async def test_bad_plan_responses() -> None:
    responses = [
        httpx.Response(400, json={"code": "invalid_reference", "message": "bad"}),
        httpx.Response(200, json={"protocol_version": 2}),
        httpx.Response(200, json={"protocol_version": 1}),
    ]
    value = client(httpx.MockTransport(lambda request: responses.pop(0)))
    payload = {
        "source": "a.example/x:v1",
        "destination": "b.example/x:v1",
        "copy_referrers": True,
        "copy_digest_tags": True,
        "replacement_policy": "no_clobber",
    }
    for code in ("invalid_reference", "protocol_error", "protocol_error"):
        with pytest.raises(OCITransferError) as caught:
            await value.plan(payload)
        assert caught.value.code == code
    await value.aclose()


@pytest.mark.asyncio
async def test_copy_response_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        httpx.Response(500, content=b"bad"),
        httpx.Response(200, stream=Stream([b'{"protocol_version":2,"type":"completed"}\n'])),
        httpx.Response(200, stream=Stream([b'{"protocol_version":1,"type":"warning"}\n'])),
        httpx.Response(200, stream=Stream([b"x" * 20])),
    ]
    value = client(httpx.MockTransport(lambda request: responses.pop(0)))
    with pytest.raises(OCITransferError):
        _ = [item async for item in value.copy("op", {})]
    with pytest.raises(OCITransferError):
        _ = [item async for item in value.copy("op", {})]
    with pytest.raises(OCITransferError, match="without result"):
        _ = [item async for item in value.copy("op", {})]
    monkeypatch.setattr(module, "_MAX_EVENT", 10)
    with pytest.raises(OCITransferError, match="oversized"):
        _ = [item async for item in value.copy("op", {})]
    await value.aclose()


@pytest.mark.asyncio
async def test_cancel_failure_and_large_response(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [httpx.Response(500, json={"code": "bad"}), httpx.Response(200, content=b"123")]
    value = client(httpx.MockTransport(lambda request: responses.pop(0)))
    with pytest.raises(OCITransferError):
        await value.cancel("op")
    monkeypatch.setattr(module, "_MAX_RESPONSE", 2)
    with pytest.raises(OCITransferError, match="oversized"):
        await value.health()
    await value.aclose()
