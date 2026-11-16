#!/usr/bin/env bash
# One-shot, idempotent Garage setup: cluster layout, bucket and access key.
# Run from the repo root through `make storage-init`; credentials come from .env.
set -euo pipefail

COMPOSE=(docker compose -f infra/docker-compose.yml --env-file .env)
garage() { "${COMPOSE[@]}" exec -T garage /garage "$@"; }

set -a
# shellcheck disable=SC1091
source .env
set +a

# 1. Layout: a node without a role cannot store data. Assign it once.
if garage status | grep -q "NO ROLE ASSIGNED"; then
  node_id="$(garage node id -q | cut -d@ -f1)"
  garage layout assign -z dc1 -c 5GB "$node_id"
  garage layout apply --version 1
fi

# 2. Bucket.
garage bucket info "$S3_BUCKET" >/dev/null 2>&1 || garage bucket create "$S3_BUCKET"

# 3. Fixed access key (so .env is the single source of truth), with full access to the bucket.
garage key info "$S3_ACCESS_KEY_ID" >/dev/null 2>&1 ||
  garage key import --yes -n lakehouse "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY"
garage bucket allow --read --write --owner "$S3_BUCKET" --key "$S3_ACCESS_KEY_ID"

echo "Garage ready: bucket '$S3_BUCKET', key '$S3_ACCESS_KEY_ID'."
