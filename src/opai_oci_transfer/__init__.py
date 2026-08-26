"""Asynchronous OCI registry transfer public API."""

from opai_oci_transfer.client import AsyncOCIClient
from opai_oci_transfer.credentials import (
    AnonymousCredentialProvider,
    CredentialProvider,
    LicenseProvider,
    OnPremLicenseProvider,
    StaticCredentialProvider,
)
from opai_oci_transfer.manager import CopyManager, UpdateCallback
from opai_oci_transfer.models import (
    BlobProgress,
    CopyError,
    CopyJob,
    CredentialRequest,
    Descriptor,
    ImageSnapshot,
    JobConflictError,
    JobNotFoundError,
    ManifestRecord,
    OCITransferError,
    PruneResult,
    RegistryCredentials,
)

__version__ = "0.2.0"
__all__ = [
    "AnonymousCredentialProvider",
    "AsyncOCIClient",
    "BlobProgress",
    "CopyError",
    "CopyJob",
    "CopyManager",
    "CredentialProvider",
    "CredentialRequest",
    "Descriptor",
    "ImageSnapshot",
    "JobConflictError",
    "JobNotFoundError",
    "LicenseProvider",
    "ManifestRecord",
    "OCITransferError",
    "OnPremLicenseProvider",
    "PruneResult",
    "RegistryCredentials",
    "StaticCredentialProvider",
    "UpdateCallback",
]
