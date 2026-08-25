# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25.13
FROM golang:${GO_VERSION}-bookworm AS service
WORKDIR /src
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH:-amd64} \
    go build -buildvcs=false -trimpath -ldflags='-s -w' \
    -o /out/opai-oci-transferd ./cmd/opai-oci-transferd

FROM scratch AS binary
COPY --from=service /out/opai-oci-transferd /opai-oci-transferd
