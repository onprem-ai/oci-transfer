import asyncio
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from opai_oci_transfer import (
    AsyncOCIClient,
    CopyManager,
    OCITransferError,
    RegistryCredentials,
    StaticCredentialProvider,
)
from opai_oci_transfer.manager import _credential_dict


def test_provider_registration_and_credential_dict(tmp_path: Path) -> None:
    first = StaticCredentialProvider("same", RegistryCredentials("u", "p"))
    manager = CopyManager(tmp_path / "q.sqlite", AsyncOCIClient(), source_credentials=first)
    manager.register_provider(first)
    with pytest.raises(ValueError, match="duplicate"):
        manager.register_provider(StaticCredentialProvider("same", RegistryCredentials("x", "y")))
    sync = MagicMock(id="sync", get_credentials=lambda request, refresh=False: None)
    with pytest.raises(TypeError):
        manager.register_provider(sync)
    assert _credential_dict(None) is None
    result = _credential_dict(RegistryCredentials("username", "credential-value"))
    assert result is not None and result["username"] == "username"


@pytest.mark.asyncio
async def test_credentials_missing_after_restart(tmp_path: Path) -> None:
    provider = StaticCredentialProvider("ephemeral", RegistryCredentials("u", "p"))
    manager = CopyManager(tmp_path / "q.sqlite", AsyncOCIClient())
    job = await manager.enqueue("a.example/x:v1", "b.example/x:v1", source_credentials=provider)
    details = await asyncio.to_thread(manager.store.details, job.id)
    manager._providers.pop("ephemeral")
    with pytest.raises(OCITransferError) as caught:
        await manager._credentials(details, "source")
    assert caught.value.code == "credential_provider_unavailable"


@pytest.mark.asyncio
async def test_start_missing_binary_and_context_methods(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPAI_OCI_TRANSFER_TEST_BINARY", str(tmp_path / "missing"))
    manager = CopyManager(tmp_path / "q.sqlite", AsyncOCIClient())
    with pytest.raises(RuntimeError, match="unavailable"):
        await manager.start()
    manager._start_service = AsyncMock()  # type: ignore[method-assign]
    entered = await manager.__aenter__()
    assert entered is manager
    await manager.__aexit__()


@pytest.mark.asyncio
async def test_close_preserves_service_cancel_failure(tmp_path: Path) -> None:
    manager = CopyManager(tmp_path / "close.sqlite", AsyncOCIClient())
    manager._operations["job"] = "operation"
    manager.client.cancel = AsyncMock(side_effect=OCITransferError("socket disappeared"))  # type: ignore[method-assign]
    with pytest.raises(OCITransferError, match="socket disappeared"):
        await manager.close()


@pytest.mark.asyncio
async def test_start_real_fake_service_and_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "service"
    script.write_text(
        """#!/usr/bin/env python3
import asyncio,json,sys
sock=sys.argv[sys.argv.index('--socket')+1]
async def h(r,w):
 await r.readuntil(b'\\r\\n\\r\\n')
 b=json.dumps({'ready':True,'protocol_version':1}).encode()
 w.write(b'HTTP/1.1 200 OK\\r\\nContent-Length: '+str(len(b)).encode()+b'\\r\\n\\r\\n'+b)
 await w.drain(); w.close()
async def main():
 s=await asyncio.start_unix_server(h,sock)
 await s.serve_forever()
asyncio.run(main())
"""
    )
    script.chmod(0o700)
    monkeypatch.setenv("OPAI_OCI_TRANSFER_TEST_BINARY", os.fspath(script))
    client = AsyncOCIClient()
    manager = CopyManager(tmp_path / "q.sqlite", client)
    await manager.start()
    assert manager._process is not None
    await manager.close()
    assert manager._process is None
