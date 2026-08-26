# OCI Transfer

![Tests](https://img.shields.io/endpoint?style=for-the-badge&url=https://gist.githubusercontent.com/tomas-polach/a86fb4ad5e2ecb21c0a53493264f8e4c/raw/oci-transfer-tests-status.json)
![Coverage](https://img.shields.io/endpoint?style=for-the-badge&url=https://gist.githubusercontent.com/tomas-polach/a86fb4ad5e2ecb21c0a53493264f8e4c/raw/oci-transfer-coverage.json)

Durable, observable, daemonless OCI image copies between registries. The public API is async-only; a private bundled Go service performs registry operations with regclient.

```python
from pathlib import Path
from opai_oci_transfer import AsyncOCIClient, CopyManager

client = AsyncOCIClient()
manager = CopyManager(Path("copies.sqlite"), client)
async with manager:
    job = await manager.enqueue(
        "registry-a.example/team/image:v1@sha256:<64-hex-digest>",
        "registry-b.example/team/image:v1@sha256:<64-hex-digest>",
    )
    completed = await manager.wait(job.id)
    # Optional explicit removal of terminal durable history.
    await manager.dismiss(completed.id)
await client.aclose()
```

`enqueue` only validates and persists local state. References may be tag-only, digest-only, or `tag@digest`. For a combined reference, the digest is authoritative while the tag is retained as descriptive metadata for destination publication and discovery. Workers persist a deterministic snapshot, copy immutable content, and verify both the destination digest and tag. SQLite must be on local storage. Credentials are provided through explicit async providers and are never persisted.

The wheel must contain `opai_oci_transfer/_bin/opai-oci-transferd`. For development tests only, `OPAI_OCI_TRANSFER_TEST_BINARY` may point to a locally built service.

For web services, create one manager per application process in the application lifespan. See [`examples/fastapi_singleton.py`](examples/fastapi_singleton.py).

See [docs/requirements.md](docs/requirements.md) for the complete contract.
