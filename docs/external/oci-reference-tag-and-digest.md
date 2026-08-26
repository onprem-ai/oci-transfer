# OCI references with a tag and digest

Versions reviewed:

- OCI Distribution Specification 1.1.1
- regclient 0.11.5

Sources:

- https://github.com/opencontainers/distribution-spec/blob/v1.1.1/spec.md
- https://github.com/opencontainers/distribution-spec/issues/610
- https://github.com/regclient/regclient/blob/v0.11.5/types/ref/ref.go

Relevant behavior:

- The registry HTTP API manifest endpoint accepts one `{reference}` value,
  which is either a tag or a digest. It does not receive a combined
  `tag@digest` value.
- Client-facing tooling commonly accepts
  `registry/repository:tag@sha256:<digest>` and decomposes it before making
  registry requests.
- regclient 0.11.5 parses and retains both `Tag` and `Digest` in `ref.Ref`.
- `Ref.SetDigest()` returns a digest-only reference and clears the tag.
- `Ref.SetTag()` returns a tag-only reference and clears the digest.
- `Ref.CommonName()` serializes both fields when both are present.
- `opai-oci-transfer` therefore treats a supplied digest as authoritative for
  source reads and integrity verification, while retaining a supplied
  destination tag for publication and discoverability.
