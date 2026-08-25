#!/bin/sh
set -eu
cd "$(dirname "$0")/../go"
: "${GOOS:=linux}"
: "${GOARCH:=$(go env GOARCH)}"
CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" go build -buildvcs=false -trimpath \
  -ldflags='-s -w' -o ../src/opai_oci_transfer/_bin/opai-oci-transferd \
  ./cmd/opai-oci-transferd
