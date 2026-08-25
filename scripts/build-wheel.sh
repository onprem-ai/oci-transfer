#!/bin/sh
set -eu
arch=${1:-$(uname -m)}
case "$arch" in
  x86_64|amd64) goarch=amd64; platform=manylinux_2_28_x86_64 ;;
  aarch64|arm64) goarch=arm64; platform=manylinux_2_28_aarch64 ;;
  *) echo "unsupported architecture: $arch" >&2; exit 2 ;;
esac
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
binary="$root/src/opai_oci_transfer/_bin/opai-oci-transferd"
temporary="$root/.wheel-bin"
cleanup() {
  rm -rf "$temporary"
  rm -f "$binary"
}
trap cleanup EXIT
mkdir -p "$(dirname "$binary")" "$root/dist"
rm -rf "$temporary"
docker build --build-arg TARGETARCH="$goarch" --output "type=local,dest=$temporary" "$root"
cp "$temporary/opai-oci-transferd" "$binary"
chmod 0755 "$binary"
OPAI_OCI_WHEEL_PLATFORM="$platform" uv build --wheel --out-dir "$root/dist"
