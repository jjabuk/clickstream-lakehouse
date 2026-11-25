# ADR 0023: The semantic layer lives in dbt (MetricFlow), and Evidence is tested against it

- **Status:** accepted
- **Date:** 2026-11-23
- **Versions:** dbt-core 1.12.7 (MetricFlow 0.214 is its dependency), dbt-metricflow 0.16.0
  (the `mf` CLI, dev dependency). The latest semantic-layer YAML spec, which dbt 1.12 and v2
  both read.

## Context

Before this decision, the metrics existed only in Evidence (`evidence/metrics/*.yaml`, ADR 0018).
Any other consumer would have had to redefine them: Feast in week 6, a notebook, or the
Databricks build in stage 2. Building the semantic layer also exposed a real inconsistency.
`fct_sessions.reached_cart` counted any visit to the cart page (an open funnel).
`fct_funnel_daily` counted the cart only after a product view (a closed funnel). Two marts gave
two answers to "how many sessions reached the cart".

## Decision

**One funnel definition.** The closed funnel steps of `int_sessions__funnel_steps` are now
columns of `fct_sessions` (`reached_product`, `reached_cart`, `reached_checkout`).
`fct_funnel_daily` aggregates them instead of joining the intermediate model again.

**Semantic models on the marts** (latest spec, in `models/marts/_marts.yml`):

| Semantic model | Entities | Dimensions | Simple metrics |
|---|---|---|---|
| `fct_sessions` (main) | session (primary), visitor, user | started at (hour), device, country, campaign, entry page, bounce | sessions, visitors, bounced, product, cart and checkout sessions, page views, average duration |
| `fct_page_views` | page view (primary), session, visitor, user | occurred at (hour), page type | page views |
| `fct_visitor_conversion_daily` | visitor day (primary), visitor | product viewed at (hour), device, window complete | product viewer days, converted viewer days |
| `fct_traffic_daily` | traffic day | day, is_bot | events, bot events |
| `dim_user` | user (primary) | plan, country | none |

- **`dim_date` is the time spine.**
- **Ratio metrics** live in `models/marts/_metrics.yml`: bounce rate, the three step rates,
  cart to checkout, pages per session and bot share. Each is a ratio of two sums, so it is
  correct at any grain.
- **The SCD2 dimension joins without validity windows.** Facts carry `user_key`, the version that
  was valid when the session happened. So `user__plan` is the plan at that time, and MetricFlow
  needs no SCD2 config.
- **Visitor conversion across sessions** is a fact table, not a MetricFlow conversion metric.
  Session rates miss a visitor who views a product today and buys three days later.
  - `fct_visitor_conversion_daily` has one row per visitor and day with a product view. It flags
    whether a checkout followed within 7 days of the first product view that day, in any
    session.
  - `visitor_product_to_checkout_7d` is the ratio of its two counts.
  - **Right censoring.** For the last 7 days of data the window is still open, and those days
    can still convert. `is_window_complete` marks the closed rows, and the dashboard reads only
    those. Without it, the latest week always looks like a drop: 20% open against 26% closed in
    the dev data.
  - A unit test covers the window edges: a checkout before the product view, one after 7 days,
    and an open window.
- **Exposures** for the four Evidence pages, and `access: public` on the marts. With that, the
  evaluator requires every mart column to be described, and they now are.

**Evidence stays a second definition.** Evidence has no MetricFlow connector, so its metric
YAML restates the same ratios over the same marts.

> **Update (ADR 0024):** the `mf` CLI, `make metrics-validate` and `make metrics-query`, and the test
> that compared Evidence's metrics with `mf query` were removed to keep the project simple. `mf`
> needs dbt-core 1.x in an environment of its own next to dbt v2, and v2's `dbt sl` needs a dbt
> platform account. The semantic models and metrics stay in the YAML as the definitions; dbt v2
> parses them but validates them only with a platform account.
>
> **Update (2026-11-25):** the comparison is back without MetricFlow. `ci/check_metrics.py`
> compiles each dbt metric from the YAML to daily SQL on the DuckDB warehouse (a simple metric is
> its aggregation by the UTC date of the model's `agg_time_dimension`, a ratio is a ratio of two
> of them per day) and each Evidence metric to daily SQL on the Postgres copy, as Evidence runs
> it. Every Evidence metric must have a dbt metric of the same name, and the two must agree on
> every day (`make metrics-check`; `make ci-metrics` in CI after `ci-evidence`). It supports what
> the project uses: simple and ratio metrics, and a filter on a dimension of the same model.

## Alternatives considered

- **Metrics only in Evidence.** It has no duplication. But the definitions would be locked in one
  BI tool, with no lineage and no tests, and every other consumer would recompute them.
- **Rates stored in the marts.** They are wrong as soon as anyone sums or averages them across
  days or devices. Counts stay in the marts, and ratios are computed at query time.
- **Semantic models on `fct_funnel_daily`.** That model is pre-aggregated and has no entities, so
  it could not join to users or count visitors. The session is the right grain.
- **The legacy spec (`semantic_models:` and `measures:`).** dbt v2 and the latest spec replace
  it. Writing the new spec now saves a migration (ADR 0019).
- **LookML, Cube or Omni as the semantic layer.** They belong to the BI tool or are a service of
  their own. Keeping the layer in dbt keeps it next to the models it describes, and it travels to
  Databricks in stage 2.

## Consequences

- **MetricFlow's conversion metric was tried and dropped.** It needs base and conversion
  events that are a filtered set of rows. In MetricFlow 0.214 that does not work:
  - a filtered simple metric compiles to a SUM, which conversion metrics refuse;
  - filters on the inputs of a conversion metric either fail to resolve or, when both inputs
    are the same metric, collapse into one filter;
  - the workaround, unfiltered `count_distinct(case ...)` inputs, made every page view a base
    event. The result equalled the naive same-day ratio of checkout viewers to product viewers.
    `mf query --explain` showed it, after `mf validate-configs` had passed.

  The lesson: a passing validation does not mean a metric is right. The fact table states the
  definition in SQL, a unit test pins it down, and every consumer reads the same counts.
- **The time dimensions have hour granularity while the spine is daily.** MetricFlow warns about
  this. Queries by hour work, but `join_to_timespine` at hour grain would need an hourly spine.
- **`dbt parse` validates the semantic manifest,** so slim CI (`make check`) catches broken
  definitions without the warehouse. The data checks run only in the full replay.
