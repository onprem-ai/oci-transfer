# OCI Transfer Requirements

Status: implementation-ready initial specification.

## 1. Product purpose

`oci-transfer` provides an asynchronous Python API for durable, observable OCI
image copies between registries.

The core operation is:

```text
one source image reference -> one destination image reference
```

Each queue job represents exactly one source/destination image-reference pair.
Repository-wide copies, tag batches, and multiple source references in one job
are out of scope. Applications enqueue multiple jobs when copying multiple
images.

The transfer is daemonless: it does not require Docker, Podman, or containerd,
and it does not stage complete images in a local container image store.

The initial transfer engine is the Go library
`github.com/regclient/regclient`. It is wrapped by private Go code and is not
exposed directly as part of the Python API.

There is no public command-line interface in the initial product.

## 2. Repository and distribution

- Local repository name: `oci-transfer`.
- Python distribution name: `opai-oci-transfer`.
- Python import package: `opai_oci_transfer`.
- Private Go service executable name: `opai-oci-transferd`.
- Initial supported platforms are Linux x86_64 and Linux ARM64.
- Platform-specific Python wheels bundle a prebuilt Go service binary; normal
  users do not need a Go toolchain.
- The Python package resolves the bundled binary automatically as a package
  resource. No public binary-path configuration is required. A clearly private,
  test-only environment override may select a development binary.
- Go source is maintained in the repository, but generated binaries are not
  committed to version control.
- Source-distribution installation is unsupported initially unless a suitable
  prebuilt binary is available; building from source requires the documented Go
  toolchain.
- Publishing, releases, and deployment require separate explicit approval.

## 3. Public execution model

The public Python API is asynchronous only.

- Every public operation that can perform network, filesystem, database, or
  subprocess work is `async def`.
- Public callbacks and providers are async-only and explicitly typed as
  `Callable[..., Awaitable[T]]`.
- Blocking filesystem and SQLite work must not block the caller's event loop.
- The Python client provides deterministic async cleanup through `aclose()`
  and/or async context management.
- No synchronous client facade or automatic sync/async detection is provided.

## 4. Public Python API

The API intentionally follows the durable job model of `opai-models` while
using OCI-specific terminology.

```python
client = AsyncOCIClient(...)
manager = CopyManager(
    database_path=...,
    client=client,
)

async with manager:
    job = await manager.enqueue(
        source="registry-a.example.com/team/image:v1",
        destination="registry-b.example.com/team/image:v1",
    )
    completed = await manager.wait(job.id)
```

Primary public objects:

```text
AsyncOCIClient
CopyManager
CopyJob
ImageSnapshot
ManifestRecord
BlobProgress
CopyError
RegistryCredentials
CredentialRequest
CredentialProvider
AnonymousCredentialProvider
StaticCredentialProvider
OnPremLicenseProvider
```

Public records are frozen dataclasses using JSON-serializable scalar values,
UTC ISO-8601 timestamps, and tuples for collections. `CopyJob` contains `id`,
`source`, `resolved_source`, `destination`, `state`, `completed_bytes`,
`expected_bytes`, `network_bytes`, `completed_blobs`, `total_blobs`,
`bytes_per_second`, `run_count`, `consecutive_failures`, `snapshot_digest`,
`error_code`, `error_message`, and lifecycle timestamps. `ImageSnapshot`
contains the canonical references, root digest/media type, platform selection,
manifest records, unique blob descriptors, referrer and digest-tag policies,
and its deterministic digest. `BlobProgress` identifies a digest, media type,
size, high-water offset, disposition (`pending`, `transferring`, `reused`,
`mounted`, `completed`, or `failed`), and timestamps. `CopyError` mirrors the
model downloader's sanitized append-only error record.

Queue methods mirror the model downloader. `enqueue()` performs local reference
and conflict validation, persists a `queued` job, wakes workers, and returns
without contacting either registry. A background worker later claims the job,
enters `planning`, obtains credentials, resolves and pins the source digest, and
persists the immutable OCI snapshot before copying content.

```python
await manager.start()
await manager.close()
await manager.enqueue(source, destination, ...)
await manager.get(job_id)
await manager.list(...)
await manager.errors(job_id)
await manager.cancel(job_id)
await manager.retry(job_id)
await manager.wait(job_id, on_update=callback)
await manager.prune(...)
```

`wait()` polls durable SQLite state, invokes the async `on_update` callback when
the immutable `CopyJob` record changes, awaits each invocation, and returns at a
terminal state. No separate async event-iterator API is included initially.

Completed, failed, and cancelled jobs and their error history are retained by
default. `prune()` explicitly removes eligible terminal jobs and dependent
records using filters such as terminal state and age; it never removes active
jobs. Pruning deletes matching records by default (`dry_run=False`); callers may
set `dry_run=True` to preview the result. An unfiltered prune is rejected to
prevent accidental deletion. There is no automatic deletion by default. `prune()` requires at least one of
`states` or `older_than`, accepts only terminal states, and returns immutable
counts and job IDs describing the records deleted or that would be deleted.

## 5. Component boundaries

### Python layer

The Python layer owns:

- the public asynchronous API;
- durable queue persistence;
- job scheduling and concurrency limits;
- worker leases and fencing;
- retry scheduling and error history;
- progress aggregation and the `wait(..., on_update=...)` callback boundary;
- credential-provider invocation;
- supervision of the private Go component.

### Go layer

The Go layer owns:

- parsing and validating OCI references;
- registry protocol operations;
- authentication using credentials supplied for an operation;
- manifest and index traversal;
- direct registry-to-registry blob transfer;
- blob reuse and cross-repository mount attempts;
- digest verification performed by regclient;
- per-blob progress reporting;
- cancellation through Go contexts;
- returning structured, sanitized results and errors.

### Transfer engine

The initial engine is pinned to a reviewed version of
`github.com/regclient/regclient`. The wrapper must isolate its API so the engine
can be upgraded or replaced without changing the public Python API.

A proof of concept must validate progress, cancellation, multi-platform copies,
existing-blob reuse, authentication, and retries before the dependency choice
is finalized.

## 6. Python-to-Go boundary

The Go component is an implementation detail, not a user-facing CLI.

The boundary is a private HTTP/1.1 service over a Unix-domain socket with JSON
requests and NDJSON streaming responses. Protocol version `1` is carried in the
URL (`/v1/...`) and every streamed event includes `protocol_version: 1`. The
Python client uses native asynchronous socket/HTTP I/O. Windows is not initially
supported.

Each `CopyManager` owns one long-lived bundled Go service process that serves
all of that manager's jobs. `CopyManager` automatically starts, monitors, and
stops it. `start()` waits for protocol-compatible readiness, detects unexpected
process exits, and applies bounded restart behavior; `close()` stops the process
deterministically. The long-lived process reuses registry connections and
shares per-registry throttles across jobs. Users do not install, build,
configure, or operate the Go component separately. Connecting to an externally
managed service may be added later but is not part of the initial public
contract.

The protocol provides:

- `GET /v1/health`, returning readiness, protocol version, service version, and
  regclient version;
- `POST /v1/plan`, resolving a source and returning an immutable copy plan;
- `POST /v1/copies/{operation_id}`, accepting the persisted plan, destination,
  policies, and ephemeral credentials, and streaming ordered NDJSON events
  until a terminal result;
- `DELETE /v1/copies/{operation_id}`, cancelling the matching active operation;
- structured `phase`, `artifact`, `progress`, `warning`, `completed`, and
  `failed` events.

`operation_id` is an opaque random identifier and is not a credential. The
service rejects duplicate active IDs, unknown protocol fields that alter
semantics, oversized requests, malformed JSON, and more simultaneous operations
than the Python manager's worker configuration. Request bodies and event lines
are bounded. Socket writes apply backpressure; coalescible progress updates may
be replaced by newer offsets for the same blob, but phase, warning, and terminal
events are never dropped. HTTP access logs and request-body logging are disabled.

## 7. Copy identity and immutable source

A source and destination are explicit fully qualified references. Any supported
source registry may be paired with any supported destination registry, including
anonymous public registries and authenticated private registries. Before
transfer, a tag-only source is resolved to a digest. Tag-plus-digest references
are also supported: the digest is authoritative, the source tag is retained as
metadata without being trusted for resolution, and both original strings remain
in durable job metadata. All retries copy the same resolved source content by
digest.

Destination tags, digest-only references, and tag-plus-digest references are
supported. For a combined destination, content is copied and verified by digest,
then the retained tag is published and verified to resolve to that same digest. A digest
in `destination` is itself explicit; no redundant opt-in flag is required. The
wrapper validates engine and registry support before transfer and returns a
stable unsupported-operation error rather than silently creating a tag or
changing the requested reference.

A tag changing after enqueue must not silently change the job's source. During
planning, a tag-only source is resolved and pinned to its root digest. A combined
source is read directly by its supplied digest; its tag is descriptive and does
not participate in content selection. All subsequent reads and retries use that
digest rather than resolving the tag again. If the registry cannot serve the
pinned graph consistently or required content changes or disappears, the job
fails with an integrity or registry error. The copy preserves the source root
manifest/index digest exactly by
default. If the
destination cannot accept the original manifest or media type without
conversion, the job fails with a stable unsupported-operation error. Silent
manifest conversion is forbidden; an explicit conversion feature may be added
later.

Destination replacement uses an explicit policy. The default is `no_clobber`:
if the destination tag already resolves to a different digest, the job fails
without changing it. If it already resolves to the planned source digest, the
operation succeeds idempotently after verifying the requested content and
referrers according to policy. Callers may explicitly request `overwrite`.

The snapshot includes:

- canonical source reference and resolved digest reference;
- root manifest/index digest, media type, and byte size;
- destination reference;
- normalized platform selection;
- every selected manifest descriptor;
- every unique config/layer blob descriptor, deduplicated by digest;
- every discovered OCI referrer and recognized digest tag;
- total blob count and total known compressed bytes;
- referrer, digest-tag, and replacement policies;
- deterministic snapshot SHA-256.

The snapshot digest is SHA-256 over canonical JSON containing the immutable
fields above, excluding credentials, timestamps, job IDs, destination state,
and progress. OCI descriptors require a nonnegative size. If a registry omits a
usable blob size, planning records it as unknown, `expected_bytes` and percentage
are `None`, and blob-count progress remains available; the copy is not rejected
solely for an unknown size.

## 8. Progress semantics

regclient's per-item callback is an input to progress aggregation, not the
public status contract.

Job status exposes:

- state;
- completed bytes;
- expected bytes when known;
- percentage when computable;
- completed blobs and total blobs;
- current transfer rate;
- run count;
- created, started, updated, and completed timestamps.

Progress rules:

- Blob identity is digest-based.
- Duplicate blobs are counted once in the logical transfer plan.
- Progress uses each blob's high-water offset so retries do not move aggregate
  progress backward.
- Blobs already present or mounted at the destination become logically
  complete without claiming those bytes traversed the network.
- Logical completion bytes and actual network bytes must be distinct metrics.
- The API returns `None`/`null` for percentage when a reliable denominator is
  unavailable.
- Manifest publication is represented as a phase even though its bytes may be
  excluded from the percentage denominator.
- Progress event frequency is bounded and configurable.
- Public progress callbacks are async-only, awaited on the event-loop thread,
  and delivered in order. In `wait()`, each callback completes before status
  polling continues; it does not pause the independent background transfer. A
  callback that performs blocking work must offload it explicitly.
- Planning recursively walks the selected image/index and referrer graphs,
  rejects cycles or configured depth/count/manifest-size limits, and
  deduplicates descriptors globally by digest.
- For complete multi-platform copies, all selected child manifests contribute
  their unique blobs to one denominator. Platform filtering happens during
  planning, before totals are finalized.
- Referrer payload blobs contribute to total/completed blobs and bytes. Root,
  child, and referrer manifest bytes are tracked as records and phases but are
  excluded from byte percentage because they are small and not represented
  consistently by transfer callbacks.
- A skipped or mounted blob contributes its full descriptor size to logical
  `completed_bytes`; it contributes zero to `network_bytes`. A transferred blob
  contributes its monotonic high-water offset to both metrics for the current
  run. `bytes_per_second` is based on network-byte deltas, not logical reuse.
- A known-size job reports `percent = min(100, 100 * completed_bytes /
  expected_bytes)`. A zero-blob graph reports 100 only after destination
  verification. Unknown-size jobs report `percent=None`.

## 9. Concurrency

The system supports multiple queued image-copy jobs.

It must provide bounded, configurable limits for:

- concurrent jobs per manager;
- concurrent requests per registry host within one Go service process.

As with the model downloader, `workers` is a per-manager limit. Multiple manager
processes sharing one database may therefore run the sum of their configured
workers. Cross-process global concurrency limiting is not claimed.

The public configuration uses these names and semantics:

```python
CopyManager(
    workers=2,                 # copy two images concurrently
    requests_per_registry=3,   # allow three concurrent requests per registry
)
```

The default is `workers=1` and `requests_per_registry=3`. regclient `ImageCopy`
starts concurrent goroutines for an image's config, layers, and child manifests.
It does not expose a per-copy blob-worker count. Instead, each registry host has
a `config.Host.ReqConcurrent` request throttle (default `3`), configured through
`regclient.WithConfigHost` or `WithConfigHostDefault`. A blob copy acquires the
source and destination host throttles together, so active transfers are bounded
by both registries' shared host limits, including across concurrent jobs using
the same regclient instance.

The wrapper exposes this accurately as `requests_per_registry`, defaulting to
regclient's value of `3`; it does not claim to offer `blobs_per_job`. Each blob
is normally one streaming transfer. Chunked upload controls request/upload chunk
size and retry behavior, not parallel byte-range streams within one blob.

## 10. Durable queue

The only supported queue backend is SQLite on local storage. `CopyManager`
accepts the complete SQLite database file path through `database_path`; it does
not accept a directory, connection, engine, session, or partially configured
database object. PostgreSQL and other queue backends are out of scope.

Job states:

```text
queued
planning
copying
retry_wait
publishing
verifying
completed
failed
cancel_requested
cancelled
```

Enqueue is idempotent for equivalent active work. Because enqueue performs no
network I/O, equivalence initially uses the canonical source reference,
destination reference, platform selection, provider IDs, referrer/digest-tag
policies, and destination replacement policy. Equivalent callers attach to the
same active job. Once planned, the resolved digest is also part of the durable
identity. Any other active job targeting the same destination reference is
rejected. A new enqueue never trusts a previously
completed job blindly: it creates a new validation run and re-resolves the
source, then verifies the destination digest and required referrers. If
everything already matches, the new job completes without transferring blobs;
otherwise normal replacement policy applies.

The persistence model must include:

- copy jobs;
- immutable source snapshots;
- per-manifest and per-blob state;
- append-only sanitized error history;
- worker identity, lease expiry, heartbeat, and fencing token;
- retry counters and next-attempt time;
- aggregate progress and rate.

Failed and cancelled jobs retain their immutable snapshots and per-blob state
until explicitly pruned. This supports debugging and safe retry by preserving
the exact source digest, selected platforms and manifests, expected blob sizes,
referrer expectations, and terminal progress. Snapshots and blob records must
never contain credentials, tokens, authorization headers, or registry challenge
responses.

SQLite must use WAL mode, `synchronous=FULL`, a bounded busy timeout, short
claim transactions, leases, and fencing-token checks. It must not be placed on
network storage.

## 11. Recovery and idempotency

Execution is at least once, not exactly once.

After the transfer call succeeds, the worker resolves the destination reference
and confirms that its root manifest/index digest exactly matches the pinned
source digest. It also verifies requested referrer and digest-tag preservation
according to policy. A job is marked `completed` only after these checks pass.

`retry(job_id)` always retries the original immutable snapshot and pinned source
digest; it never resolves the original source tag to newer content. To copy a
new value of a mutable tag, the caller creates a new job with `enqueue()`.

After interruption, a worker retries the copy against the pinned source digest.
Recovery generally relies on the destination registry recognizing or mounting
already-present blobs. Byte-range continuation of an interrupted upload is not
guaranteed unless validated for the relevant registry and regclient path.

The API and documentation must distinguish:

- durable queue/job recovery;
- completed-blob reuse;
- resumable partial-blob upload.

Only guarantees verified by integration tests may be advertised.

## 12. Retry and cancellation

Retry behavior follows the model downloader's strategy:

- Regclient owns bounded retries for individual registry requests; the wrapper
  configures that facility rather than nesting another large per-request retry
  loop around it.
- The Python manager exclusively owns durable job-level retry classification,
  scheduling, backoff state, and error history. The Go service returns a
  structured sanitized outcome and never maintains a durable retry queue.
- A transient operation failure moves the durable job to `retry_wait`; there is
  no fixed lifetime job-attempt limit while verified progress continues.
- Job retries use exponential full-jitter backoff with defaults
  `initial_backoff=0.5` seconds and `max_backoff=60.0` seconds.
- `Retry-After` is honored within configured bounds when regclient exposes it.
- The no-progress clock is reset whenever a blob's high-water byte offset
  increases or a blob becomes complete/reused/mounted. The default
  `no_progress_timeout` is `3600.0` seconds.
- `overall_timeout=0.0` means unlimited total job duration; a positive value is
  an optional hard deadline.
- Authentication failures trigger credential refresh when the provider can
  refresh, before final classification.
- Digest/integrity failures use a separate bounded retry budget, defaulting to
  `integrity_retries=2`.
- Digest mismatch, invalid manifests, unsupported media types, denied access,
  and source mutation receive stable error codes.
- Cancellation is cooperative through Python worker cancellation and Go
  context cancellation.
- `manager.close()` gracefully stops active work, terminates the owned Go
  process, releases leases promptly, and leaves affected jobs resumable; it does
  not mark them cancelled.
- Process shutdown or unexpected process death likewise does not represent user
  cancellation; expired leases make interrupted jobs eligible for recovery.
- Only explicit `cancel(job_id)` transitions active work through
  `cancel_requested` to `cancelled`.

The Go adapter maps regclient and HTTP failures into stable categories rather
than exposing raw error strings. Retryable categories are `network_error`,
`timeout`, `rate_limited`, `registry_unavailable`, and `temporary_registry_error`.
Permanent categories include `invalid_reference`, `authentication_failed`,
`authorization_denied`, `destination_conflict`, `manifest_invalid`,
`digest_mismatch`, `source_changed`, `unsupported_operation`,
`credential_provider_unavailable`, and `protocol_error`. HTTP 408, 429, 500,
502, 503, and 504 are retryable; other 4xx responses are permanent except that
401/403 first permit credential refresh. Unknown errors fail safely as
`transfer_failed` unless explicitly classified by tests. Raw registry response
bodies and URLs are never persisted.

## 13. Credentials

The transfer core is independent of OnPrem AI licensing.

Credentials use one opinionated async-only provider contract, separately for
source and destination. `CopyManager` accepts default source and destination
providers, and `enqueue()` may override either provider for that job. Providers
and their returned credentials are runtime-only capabilities: neither provider
objects nor credentials are persisted. A per-job override has a non-secret,
stable provider ID that is persisted with the job. After restart, that exact
provider ID must be registered again. A missing provider never silently falls
back to a manager default: the job fails loudly with a stable
`credential_provider_unavailable` error before contacting either registry.

The provider is called with the registry, repository, and operation (`pull` or
`push`) and returns immutable registry credentials or `None` for anonymous
access. Synchronous callbacks and automatic sync/async detection are not
supported.

A built-in `AnonymousCredentialProvider` returns `None`. A convenience
`StaticCredentialProvider` holds caller-supplied credentials in memory while
implementing the same async contract. Custom providers may retrieve secrets from
environment-backed configuration, a secret manager, or another service. This
permits arbitrary public-to-private, private-to-public, and private-to-private
copies without special cases in the transfer API.

The initial release does not automatically read Docker/Podman authentication
files or invoke external credential helpers. Credentials come only from the
explicit providers configured by the application, keeping authentication
predictable and avoiding hidden blocking subprocesses.

An optional built-in OnPrem AI adapter is layered on the generic provider
interface and supports the existing License Server contract. The transfer core
does not know what a license key is. The adapter calls `GET /v1/entitlement`
with `Authorization: Bearer <license-key>` and uses `package_registry.host`,
`package_registry.repository`, and its short-lived `credentials`. The returned
username/session are then supplied to the registry, which performs the standard
OCI Distribution bearer-token challenge against `GET /v1/registry/token`.
Initially this adapter provides source credentials only because license-derived
credentials are pull-only. Destination pushes use a separate generic credential
provider. The provider interfaces remain direction-aware so a future licensed
publisher flow can be added without changing the copy API.

Requirements:

- credentials are requested only after a worker claims a job and are sent to
  the long-lived Go process for that operation over the permission-restricted
  Unix socket;
- the Go process retains credentials only in memory for the operation lifetime,
  removes operation references on completion/cancellation, and does not place
  them in process arguments or environment variables;
- credentials are requested when needed and refreshed after expiry or an
  eligible authentication failure;
- credentials, authorization headers, and tokens are never persisted;
- secrets are never included in logs, progress events, errors, or traces;
- source and destination credential scopes remain separate;
- credential callbacks are awaited on the Python event-loop thread;
- transport from Python to Go must not expose credentials to unrelated local
  users.

The provider contract is:

```python
class CredentialProvider(Protocol):
    id: str

    async def get_credentials(
        self, request: CredentialRequest, *, refresh: bool = False
    ) -> RegistryCredentials | None: ...
```

`CredentialRequest` contains only `registry`, `repository`, and `operation`.
`RegistryCredentials` contains `username`, `password`, and optional `expires_at`.
Provider IDs are validated non-secret identifiers. The OnPrem adapter accepts
`api_url`, an async `license_provider`, and an optional reusable
`httpx.AsyncClient`; it implements the `/v1/entitlement` contract documented by
the License Server and validates that the returned host/repository authorizes
the requested source. It never forwards a license key to the registry.

## 14. Multi-platform images and OCI referrers

The API must explicitly select one of:

- copy the complete image/index;
- copy one requested platform;
- copy an explicit platform set.

The default is to copy the complete image index and all referenced platforms,
preserving the source image. Callers may explicitly request one platform or an
explicit platform set.

OCI referrers associated with every copied manifest are copied by default,
including signatures, SBOMs, and attestations. Their graphs are traversed safely,
deduplicated by digest, included in planning and status, and published only after
the content they reference is available at the destination. Callers may
explicitly disable referrer copying.

Digest-tag conventions (for example, tags used as a fallback by some signing
tools when the Referrers API is unavailable) are copied by default alongside
OCI referrers. Their discovery is bounded to regclient's digest-tag convention
for each copied manifest; the implementation must not perform an unbounded or
unrelated tag copy. Callers may explicitly disable digest-tag copying.

## 15. Security

- HTTPS is required by default. Plain HTTP requires explicit opt-in for each
  specific registry host; there is no global insecure switch. Loopback test
  registries may be opted in through the same per-registry mechanism.
- Source and destination references are validated before persistence or use.
- Custom CA certificates are supported per registry in the initial release and
  are configured independently from plain-HTTP opt-in. Mutual TLS client
  certificates are deferred until a concrete requirement exists.
- Credentials and bearer challenges must be protected against redirect-based
  exfiltration.
- Errors are sanitized and length-bounded before persistence or API exposure.
- Queue files and local sockets use restrictive permissions.
- The service authenticates or restricts local clients through filesystem
  permissions; remote TCP is out of scope until separately designed with TLS
  and authorization.
- Dependency versions are pinned and monitored for vulnerabilities.

## 16. Testing and release gates

Required automated coverage includes:

- direct registry-to-registry copy;
- Docker and OCI manifests;
- complete and selected-platform index copies;
- parallel layer transfer;
- aggregate progress monotonicity and final completion;
- already-present and mounted blobs;
- source tag mutation after snapshot;
- separate source/destination authentication;
- expiring credential refresh;
- cancellation and process-kill recovery;
- transient retry scheduling;
- destination rejection and digest failures;
- queue claim exclusion, lease reclaim, and fencing;
- multi-process queue workers;
- absence of secrets in logs, database rows, events, and errors;
- protocol compatibility and malformed-message handling;
- common target registry implementations used in production.

The Go code must pass `gofmt`, `go vet`, `go test`, `go test -race`, and
`govulncheck`. The Python code must pass Ruff, mypy strict checking, pytest,
and at least 90% branch coverage. Lock files are committed and CI builds both
Linux x86_64 and Linux ARM64 wheels. Release tests install each wheel without a
Go toolchain, launch the bundled service, and run a registry-to-registry smoke
test. An sdist may be published only when its documented Go build path is tested.

## 17. Initial exclusions

Unless added explicitly, the first version excludes:

- a public CLI;
- container execution;
- image building;
- a Docker/Podman/containerd daemon dependency;
- arbitrary remote TCP access to the private Go service;
- cross-host queue workers;
- guaranteed partial-blob resume;
- image mutation.
