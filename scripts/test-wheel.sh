#!/bin/sh
set -eu
wheel=$1
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
case "$wheel" in /*) ;; *) wheel="$root/$wheel" ;; esac
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
uv venv --python 3.11 "$tmp/venv" >/dev/null
uv pip install --python "$tmp/venv/bin/python" "$wheel" >/dev/null
"$tmp/venv/bin/python" - <<'PY'
import asyncio
import os
import stat
import tempfile
from importlib.resources import files
from pathlib import Path

from opai_oci_transfer import AsyncOCIClient, CopyManager

binary = Path(str(files("opai_oci_transfer").joinpath("_bin/opai-oci-transferd")))
assert binary.is_file()
assert binary.stat().st_mode & stat.S_IXUSR
assert binary.read_bytes()[:4] == b"\x7fELF"

async def main() -> None:
    client = AsyncOCIClient()
    manager = CopyManager(Path(tempfile.mkdtemp()) / "queue.sqlite", client)
    try:
        await manager._start_service()
        health = await client.health()
        assert health["protocol_version"] == 1 and health["ready"]
    finally:
        await manager.close()

asyncio.run(main())
PY
