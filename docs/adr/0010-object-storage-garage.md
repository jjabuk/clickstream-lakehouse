# ADR 0010: Garage instead of MinIO as the local S3 object store

- **Status:** accepted
- **Date:** 2026-11-16

## Context

The lakehouse needs an S3-compatible object store running in `docker compose`: it holds the
Iceberg data and metadata files and the raw JSONL batches. The original plan used MinIO.

On 2026-11-16 the `minio/minio` repository is archived (last push April 2026). The images
checked were not available: `minio/minio` on Docker Hub (access denied) and the
`quay.io/minio/minio` tag matching the last GitHub release (no such manifest). A pinned,
reproducible image is a hard requirement for a public repository, so MinIO is out.

## Decision

Use **Garage** (Deuxfleurs; latest release at the time of writing: v2.5.0, 2026-10-24, image
`dxflrs/garage`, exact tag pinned in compose). It is a small, actively maintained single binary
with an S3 API that covers what Iceberg needs.

## Alternatives considered

- **SeaweedFS.** Actively developed, S3 gateway included. Not chosen: more moving parts (master,
  volume, filer) for a single-node setup, and Lakekeeper's documentation lists it among stores
  that do not verify every part of a signed request, which matters for remote signing.
- **RustFS.** Closest to MinIO in API and console, but a young project.
- **MinIO built from source or an old image from another registry.** Unmaintained and not
  reproducible from a clean machine.

## Consequences

- **No STS, so no vended credentials.** Lakekeeper falls back to remote signing, which it
  documents as safe on Garage because Garage checks the whole signed request. PyIceberg supports
  remote signing; DuckDB does not, so DuckDB uses its own S3 key (ADR 0002, ADR 0003).
- **No bucket versioning** (and no `ListObjectVersions`). Iceberg does not need it: snapshots and
  branches live in table metadata, not in the object store.
- **Cluster layout must be initialised.** Even a single node needs `garage layout assign` and
  `apply` before it serves requests, then a bucket and an access key. This is a one-shot init
  step in compose, wrapped in a `make` target.
- **Path-style addressing and a fixed region** (set in Garage's config) must be used by every
  client: PyIceberg, DuckDB and, later, Kafka Connect and Spark.
- The S3 API is the contract, so replacing the store later (including with real S3 in stage 2) is
  configuration only.
