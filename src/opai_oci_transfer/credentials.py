"""Async-only registry credential providers."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

import httpx

from opai_oci_transfer.errors import extract_http_error_detail, sanitize_error_detail
from opai_oci_transfer.models import (
    CredentialRequest,
    OCITransferError,
    RegistryCredentials,
    validate_provider_id,
)


@runtime_checkable
class CredentialProvider(Protocol):
    id: str

    async def get_credentials(
        self, request: CredentialRequest, *, refresh: bool = False
    ) -> RegistryCredentials | None: ...


class AnonymousCredentialProvider:
    id = "anonymous"

    async def get_credentials(self, request: CredentialRequest, *, refresh: bool = False) -> None:
        del request, refresh
        return None


class StaticCredentialProvider:
    def __init__(self, provider_id: str, credentials: RegistryCredentials) -> None:
        self.id = validate_provider_id(provider_id)
        self._credentials = credentials

    async def get_credentials(
        self, request: CredentialRequest, *, refresh: bool = False
    ) -> RegistryCredentials:
        del request, refresh
        return self._credentials


LicenseProvider = Callable[[], Awaitable[str]]


class OnPremLicenseProvider:
    """Adapt a License Server entitlement into pull-only registry credentials."""

    def __init__(
        self,
        api_url: str,
        license_provider: LicenseProvider,
        *,
        provider_id: str = "onprem-license",
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
    ) -> None:
        if not inspect.iscoroutinefunction(license_provider):
            raise TypeError("license_provider must be an async callable")
        self.id = validate_provider_id(provider_id)
        self._api_url = api_url.rstrip("/")
        self._license_provider = license_provider
        self._client = http_client or httpx.AsyncClient(timeout=timeout, follow_redirects=False)
        self._owns_client = http_client is None

    async def get_credentials(
        self, request: CredentialRequest, *, refresh: bool = False
    ) -> RegistryCredentials:
        del refresh
        if request.operation != "pull":
            raise OCITransferError("license credentials are pull-only", code="authorization_denied")
        key = await self._license_provider()
        try:
            response = await self._client.get(
                f"{self._api_url}/v1/entitlement",
                headers={"Authorization": f"Bearer {key}"},
            )
            if response.status_code >= 400:
                message = f"License Server returned HTTP {response.status_code}"
                if detail := extract_http_error_detail(response.content):
                    message = f"{message}: {detail}"
                raise OCITransferError(
                    message,
                    code="credential_provider_unavailable",
                    retryable=response.status_code in {408, 429, 500, 502, 503, 504},
                )
            value = response.json()
            registry = value["package_registry"]
            credentials = registry["credentials"]
            host = str(registry["host"]).lower()
            repository = str(registry["repository"]).strip("/")
            if host != request.registry.lower() or not (
                request.repository == repository or request.repository.startswith(repository + "/")
            ):
                raise OCITransferError(
                    "entitlement does not authorize the requested source",
                    code="authorization_denied",
                )
            return RegistryCredentials(
                username=str(credentials["username"]),
                password=str(credentials.get("password", credentials.get("session", ""))),
                expires_at=credentials.get("expires_at"),
            )
        except OCITransferError:
            raise
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            detail = sanitize_error_detail(exc)
            raise OCITransferError(
                f"credential provider unavailable ({type(exc).__name__}): {detail}",
                code="credential_provider_unavailable",
                retryable=isinstance(exc, httpx.TransportError),
            ) from None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> OnPremLicenseProvider:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()
