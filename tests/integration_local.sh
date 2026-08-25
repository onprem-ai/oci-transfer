#!/bin/sh
# Runs a real registry-to-registry copy against a disposable local registry.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
name=oci-transfer-test-registry
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
docker run -d --rm --name "$name" -p 127.0.0.1:5001:5000 registry:2 >/dev/null
docker pull busybox:1.36.1 >/dev/null
docker tag busybox:1.36.1 localhost:5001/source/busybox:v1
docker push localhost:5001/source/busybox:v1 >/dev/null
mkdir -p /tmp/oci-transfer-bin
docker run --rm -v "$root:/src" -v /tmp/oci-transfer-bin:/out -w /src/go golang:1.25.13-bookworm \
  bash -c 'export PATH=/usr/local/go/bin:$PATH; CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/opai-oci-transferd ./cmd/opai-oci-transferd'
OPAI_OCI_TRANSFER_TEST_BINARY=/tmp/oci-transfer-bin/opai-oci-transferd uv run python - <<'PY'
import asyncio, tempfile
from pathlib import Path
from opai_oci_transfer import AsyncOCIClient, CopyManager
async def main():
    client = AsyncOCIClient(timeout=30)
    manager = CopyManager(Path(tempfile.mkdtemp()) / "queue.sqlite", client, insecure_registries=["localhost:5001"])
    async with manager:
        job = await manager.enqueue("localhost:5001/source/busybox:v1", "localhost:5001/target/busybox:v1", copy_referrers=False, copy_digest_tags=False)
        result = await asyncio.wait_for(manager.wait(job.id), 60)
        assert result.state == "completed", (result, await manager.errors(job.id))
    await client.aclose()
asyncio.run(main())
PY
