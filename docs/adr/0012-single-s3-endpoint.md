# ADR 0012: One S3 endpoint URL valid for every client

- **Status:** accepted
- **Date:** 2026-11-16

## Context

Lakekeeper stores one `endpoint` per warehouse. It uses it for its own S3 calls and returns the
same value to every client in the table config (`s3.endpoint`). In a real deployment this is a
single URL of a managed object store, reachable from everywhere.

Locally there are two worlds: the compose network, where Garage is `garage:3900`, and the host,
where PyIceberg, DuckDB and dbt run and where Garage is published on `127.0.0.1:3900`. With the
endpoint set to `http://garage:3900` the catalog and table creation worked, but a host client
failed to write data (`Could not connect to the endpoint URL: http://garage:3900/...`).
A client-side `s3.endpoint` override had no effect: the value from the server wins in PyIceberg.

## Decision

Treat the endpoint as one URL that must resolve identically for every client, and make the
local setup satisfy that instead of working around it: the warehouse endpoint is
`http://garage:3900`, and the host resolves `garage` through a one-time `/etc/hosts` entry
(`127.0.0.1 garage`). `make hosts-check` verifies it and prints the command when it is missing.

The endpoint is the single configuration knob (`S3_ENDPOINT`): moving to a cloud object store means
changing that value and the credentials, nothing in ingest, dbt or the catalog setup.

## Alternatives considered

- **Run every client inside the compose network.** Satisfies the principle, but ingest, dbt,
  DuckDB and Evidence would all run in containers; host-side `uv run` and fast iteration are lost.
- **`garage.localtest.me` with `extra_hosts` for the container.** Needs no sudo, but depends on a
  third-party public DNS name and an internet connection.
- **`network_mode: service:garage` for Lakekeeper with endpoint `localhost:3900`.** Works offline
  without host changes, but couples two services' lifecycles and hides Lakekeeper's ports under the
  Garage service.
- **`host.docker.internal`.** Resolves in containers on Docker Desktop but not on the host.

## Consequences

- One-time manual step per machine (sudo). CI appends the same line to `/etc/hosts` in the workflow.
- Behaviour matches production: nothing in the code knows the store is local.
- On Linux the same entry works; no Docker Desktop specifics are involved.
