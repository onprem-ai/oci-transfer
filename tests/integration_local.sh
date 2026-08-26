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
docker pull alpine:3.20 >/dev/null
docker tag busybox:1.36.1 localhost:5001/source/busybox:v1
docker push localhost:5001/source/busybox:v1 >/dev/null
docker tag alpine:3.20 localhost:5001/target/busybox:copied
docker push localhost:5001/target/busybox:copied >/dev/null
source_digest=$(curl -fsSI \
  -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
  http://localhost:5001/v2/source/busybox/manifests/v1 | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}')
export SOURCE_DIGEST="$source_digest"
mkdir -p /tmp/oci-transfer-bin
docker run --rm -v "$root:/src" -v /tmp/oci-transfer-bin:/out -w /src/go golang:1.25.13-bookworm \
  bash -c 'export PATH=/usr/local/go/bin:$PATH; CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/opai-oci-transferd ./cmd/opai-oci-transferd'
OPAI_OCI_TRANSFER_TEST_BINARY=/tmp/oci-transfer-bin/opai-oci-transferd uv run python - <<'PY'
import asyncio, os, tempfile
from pathlib import Path
from opai_oci_transfer import AsyncOCIClient, CopyManager
async def main():
    client = AsyncOCIClient(timeout=30)
    manager = CopyManager(Path(tempfile.mkdtemp()) / "queue.sqlite", client, insecure_registries=["localhost:5001"])
    digest = os.environ["SOURCE_DIGEST"]
    # The descriptive source tag deliberately does not exist. A combined
    # reference must resolve exclusively by its authoritative digest.
    source = f"localhost:5001/source/busybox:descriptive-label@{digest}"
    destination = f"localhost:5001/target/busybox:copied@{digest}"
    async with manager:
        conflict_job = await manager.enqueue(
            source,
            destination,
            copy_referrers=False,
            copy_digest_tags=False,
        )
        conflict = await asyncio.wait_for(manager.wait(conflict_job.id), 60)
        assert conflict.state == "failed", (conflict, await manager.errors(conflict_job.id))
        assert conflict.error_code == "destination_conflict"

        job = await manager.enqueue(
            source,
            destination,
            copy_referrers=False,
            copy_digest_tags=False,
            replacement_policy="overwrite",
        )
        result = await asyncio.wait_for(manager.wait(job.id), 60)
        assert result.state == "completed", (result, await manager.errors(job.id))
        assert result.source == source and result.destination == destination
    await client.aclose()
asyncio.run(main())
PY
actual_tag_digest=$(curl -fsSI -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
  http://localhost:5001/v2/target/busybox/manifests/copied | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}')
actual_digest=$(curl -fsSI -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
  "http://localhost:5001/v2/target/busybox/manifests/$SOURCE_DIGEST" | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}')
[ "$actual_tag_digest" = "$SOURCE_DIGEST" ] && [ "$actual_digest" = "$SOURCE_DIGEST" ] || {
  echo "destination publication mismatch: tag=$actual_tag_digest digest=$actual_digest expected=$SOURCE_DIGEST" >&2
  exit 1
}
