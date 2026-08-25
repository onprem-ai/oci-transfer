"""Immutable public records and OCI reference/policy validation."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Literal

JobState = Literal[
    "queued",
    "planning",
    "copying",
    "retry_wait",
    "publishing",
    "verifying",
    "completed",
    "failed",
    "cancel_requested",
    "cancelled",
]
BlobDisposition = Literal["pending", "transferring", "reused", "mounted", "completed", "failed"]
ReplacementPolicy = Literal["no_clobber", "overwrite"]
PlatformMode = Literal["all", "selected"]
TerminalState = Literal["completed", "failed", "cancelled"]

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_PROVIDER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
# Deliberately requires registry/repository and a tag or digest. Go validates again.
_REFERENCE = re.compile(
    r"^(?P<registry>localhost|(?:[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)(?::[0-9]{1,5})?)"
    r"/(?P<repository>[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*)"
    r"(?:(?P<tag>:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})|@(?P<digest>sha256:[0-9a-f]{64}))$"
)


class OCITransferError(RuntimeError):
    """A sanitized transfer failure with a stable machine-readable code."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "transfer_failed",
        retryable: bool = False,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.retry_after = retry_after


class JobNotFoundError(KeyError):
    """A requested copy job does not exist."""


class JobConflictError(RuntimeError):
    """An operation conflicts with durable queue state."""


@dataclass(frozen=True)
class RegistryCredentials:
    username: str
    password: str
    expires_at: str | None = None


@dataclass(frozen=True)
class CredentialRequest:
    registry: str
    repository: str
    operation: Literal["pull", "push"]


@dataclass(frozen=True)
class Descriptor:
    digest: str
    media_type: str
    size: int | None

    def __post_init__(self) -> None:
        validate_digest(self.digest)
        if self.size is not None and self.size < 0:
            raise ValueError("descriptor size must be nonnegative")


@dataclass(frozen=True)
class ManifestRecord:
    digest: str
    media_type: str
    size: int
    kind: str
    subject_digest: str | None = None
    platform: str | None = None


@dataclass(frozen=True)
class ImageSnapshot:
    source: str
    resolved_source: str
    destination: str
    root_digest: str
    root_media_type: str
    root_size: int
    platforms: tuple[str, ...]
    manifests: tuple[ManifestRecord, ...]
    blobs: tuple[Descriptor, ...]
    copy_referrers: bool
    copy_digest_tags: bool
    replacement_policy: ReplacementPolicy
    digest: str

    @classmethod
    def create(
        cls,
        *,
        source: str,
        resolved_source: str,
        destination: str,
        root_digest: str,
        root_media_type: str,
        root_size: int,
        platforms: tuple[str, ...] = (),
        manifests: tuple[ManifestRecord, ...] = (),
        blobs: tuple[Descriptor, ...] = (),
        copy_referrers: bool = True,
        copy_digest_tags: bool = True,
        replacement_policy: ReplacementPolicy = "no_clobber",
    ) -> ImageSnapshot:
        canonical = {
            "source": source,
            "resolved_source": resolved_source,
            "destination": destination,
            "root_digest": root_digest,
            "root_media_type": root_media_type,
            "root_size": root_size,
            "platforms": list(platforms),
            "manifests": [asdict(item) for item in manifests],
            "blobs": [asdict(item) for item in blobs],
            "copy_referrers": copy_referrers,
            "copy_digest_tags": copy_digest_tags,
            "replacement_policy": replacement_policy,
        }
        digest = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        return cls(
            source=source,
            resolved_source=resolved_source,
            destination=destination,
            root_digest=root_digest,
            root_media_type=root_media_type,
            root_size=root_size,
            platforms=platforms,
            manifests=manifests,
            blobs=blobs,
            copy_referrers=copy_referrers,
            copy_digest_tags=copy_digest_tags,
            replacement_policy=replacement_policy,
            digest=digest,
        )

    @property
    def expected_bytes(self) -> int | None:
        if any(item.size is None for item in self.blobs):
            return None
        return sum(item.size or 0 for item in self.blobs)


@dataclass(frozen=True)
class CopyJob:
    id: str
    source: str
    resolved_source: str | None
    destination: str
    state: JobState
    completed_bytes: int
    expected_bytes: int | None
    network_bytes: int
    completed_blobs: int
    total_blobs: int | None
    bytes_per_second: int | None
    run_count: int
    consecutive_failures: int
    next_retry_at: str | None
    last_progress_at: str
    snapshot_digest: str | None
    error_code: str | None
    error_message: str | None
    created_at: str
    updated_at: str
    started_at: str | None
    completed_at: str | None
    worker_id: str | None
    lease_expires_at: str | None
    heartbeat_at: str | None

    @property
    def percent(self) -> float | None:
        if self.expected_bytes is None:
            return None
        if self.expected_bytes == 0:
            return 100.0 if self.state == "completed" else 0.0
        return min(100.0, 100.0 * self.completed_bytes / self.expected_bytes)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BlobProgress:
    digest: str
    media_type: str
    size: int | None
    offset: int
    disposition: BlobDisposition
    created_at: str
    updated_at: str
    completed_at: str | None


@dataclass(frozen=True)
class CopyError:
    id: int
    job_id: str
    occurred_at: str
    error_code: str
    message: str
    retryable: bool
    http_status: int | None
    retry_after: float | None


@dataclass(frozen=True)
class PruneResult:
    job_ids: tuple[str, ...]
    jobs: int
    errors: int
    blobs: int
    snapshots: int
    dry_run: bool


def validate_digest(value: str) -> str:
    if not _DIGEST.fullmatch(value):
        raise ValueError("invalid OCI SHA-256 digest")
    return value


def validate_provider_id(value: str) -> str:
    if not _PROVIDER_ID.fullmatch(value):
        raise ValueError("provider id must be a non-secret identifier")
    return value


def parse_reference(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or len(value) > 512 or any(ord(c) < 33 for c in value):
        raise ValueError("invalid fully qualified OCI reference")
    match = _REFERENCE.fullmatch(value)
    if not match:
        raise ValueError("OCI reference must be fully qualified and include a tag or sha256 digest")
    port = match.group("registry").rpartition(":")[2]
    if port.isdigit() and not 1 <= int(port) <= 65535:
        raise ValueError("invalid registry port")
    return match.group("registry").lower(), match.group("repository")


def normalize_platforms(platforms: str | tuple[str, ...] | None) -> tuple[str, ...]:
    if platforms is None or platforms == "all":
        return ()
    values = (platforms,) if isinstance(platforms, str) else tuple(platforms)
    pattern = re.compile(r"^[a-z0-9]+/[a-z0-9_]+(?:/[A-Za-z0-9_.-]+)?$")
    if (
        not values
        or len(values) != len(set(values))
        or any(not pattern.fullmatch(v) for v in values)
    ):
        raise ValueError("platforms must be 'all' or unique os/architecture[/variant] values")
    return tuple(sorted(values))
