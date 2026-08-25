# opai-oci-transfer

Durable, observable, daemonless OCI image copies between registries. The public API is async-only; a private bundled Go service performs registry operations with regclient.

```python
from pathlib import Path
from opai_oci_transfer import AsyncOCIClient, CopyManager

client = AsyncOCIClient()
manager = CopyManager(Path("copies.sqlite"), client)
async with manager:
    job = await manager.enqueue(
        "registry-a.example/team/image:v1",
        "registry-b.example/team/image:v1",
    )
    completed = await manager.wait(job.id)
await client.aclose()
```

`enqueue` only validates and persists local state. Workers resolve the source tag to an immutable digest, persist a deterministic snapshot, copy its content, and verify publication. SQLite must be on local storage. Credentials are provided through explicit async providers and are never persisted.

The wheel must contain `opai_oci_transfer/_bin/opai-oci-transferd`. For development tests only, `OPAI_OCI_TRANSFER_TEST_BINARY` may point to a locally built service.

See [docs/requirements.md](docs/requirements.md) for the complete contract.
