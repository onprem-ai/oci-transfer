import asyncio

import httpx
import pytest

from opai_oci_transfer import (
    AnonymousCredentialProvider,
    CredentialRequest,
    OCITransferError,
    OnPremLicenseProvider,
    RegistryCredentials,
    StaticCredentialProvider,
)


@pytest.mark.asyncio
async def test_builtin_providers() -> None:
    request = CredentialRequest("r.example", "team/app", "pull")
    assert await AnonymousCredentialProvider().get_credentials(request) is None
    credentials = RegistryCredentials("u", "p")
    assert (
        await StaticCredentialProvider("static", credentials).get_credentials(request)
        == credentials
    )
    with pytest.raises(ValueError):
        StaticCredentialProvider("secret id", credentials)


@pytest.mark.asyncio
async def test_onprem_provider() -> None:
    async def license_key() -> str:
        return "secret"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer secret"
        return httpx.Response(
            200,
            json={
                "package_registry": {
                    "host": "r.example",
                    "repository": "team",
                    "credentials": {"username": "u", "password": "p"},
                }
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OnPremLicenseProvider("https://license.example", license_key, http_client=http)
    result = await provider.get_credentials(CredentialRequest("r.example", "team/app", "pull"))
    assert result == RegistryCredentials("u", "p")
    with pytest.raises(OCITransferError) as denied:
        await provider.get_credentials(CredentialRequest("r.example", "team/app", "push"))
    assert denied.value.code == "authorization_denied"
    with pytest.raises(OCITransferError):
        await provider.get_credentials(CredentialRequest("other.example", "team/app", "pull"))
    await provider.aclose()
    await http.aclose()


def test_onprem_rejects_sync_callback() -> None:
    with pytest.raises(TypeError):
        OnPremLicenseProvider("https://license.example", lambda: "key")  # type: ignore[arg-type,return-value]
    assert asyncio.iscoroutinefunction(AnonymousCredentialProvider().get_credentials)
