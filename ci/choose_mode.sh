#!/usr/bin/env bash
# Decide how a pull request is built (ADR 0015). Prints `slim` or `full` and the reason, and
# writes `mode=...` to $GITHUB_OUTPUT when it runs in GitHub Actions.
#
#   slim: build state:modified+ only, defer everything else to the production state of `main`.
#   full: replay the CI days from an empty warehouse and verify incremental == full refresh.
#
# Usage: ci/choose_mode.sh STATE_DIR BASE_REF   (e.g. data/ci/state origin/main)
set -euo pipefail

state_dir=$1
base_ref=$2

decide() {
  echo "mode=$1 ($2)"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then echo "mode=$1" >>"$GITHUB_OUTPUT"; fi
  exit 0
}

# No production state to defer to: the first PR, or the last main artifact has expired.
[[ -f "$state_dir/manifest.json" && -f "$state_dir/warehouse.duckdb" ]] ||
  decide full "no production state in $state_dir"

# The production warehouse was built from main's data and tool versions. If the PR changes the
# generator, the loader, the stack, the CI days (Makefile, ci/) or pinned versions, deferred tables
# and rebuilt models would no longer come from the same inputs.
data_paths=(generator ingest infra ci Makefile uv.lock dbt/package-lock.yml)
if ! git diff --quiet "$base_ref"...HEAD -- "${data_paths[@]}"; then
  decide full "the PR changes data or versions: $(git diff --name-only "$base_ref"...HEAD -- "${data_paths[@]}" | tr '\n' ' ')"
fi

# A slim build creates modified incremental models from scratch, so it never exercises the
# incremental path. If any incremental model is affected, replay and compare instead.
affected=$(uv run dbt ls --quiet --project-dir dbt --profiles-dir dbt --target ci \
  --select "state:modified+,config.materialized:incremental" --state "$state_dir" \
  --resource-type model --output name)
if [[ -n "$affected" ]]; then
  decide full "incremental models affected: $(echo "$affected" | tr '\n' ' ')"
fi

decide slim "no incremental model affected"
