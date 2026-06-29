# Option 2 — Live data-validation mission control (handoff brief)

Use this to kick off a **separate task**. Open a new chat, connect this folder
(`matillion_replacement_starter`), and say: *"Pick up the Option 2 work described in
OPTION_2_VALIDATION_BRIEF.md."* Everything needed for context is below.

---

## Objective

Build **data-validation pipelines** that reconcile clients' PointClickCare (PCC) data as it
moves through the stack, and surface the results in a **live mission-control dashboard**:

```
PointClickCare  →  SQL Server (client DB)  →  Snowflake RAW  →  dbt transforms (marts)
```

At each hop we want to know, per client and per run: did the data arrive complete and
unchanged, and do the transformed outputs hold their integrity?

This is the "live" sibling of Option 1. Option 1 (`dashboard.py` + `dashboard.html`) already
ships: it reads `logs/pipeline_*.log` and shows run status/rows/dbt results. Option 2 reads
**data** (counts, hashes, test results in Snowflake) rather than logs.

---

## Two layers — keep them separate

### Layer A — validation logic (the important half; lives in the pipeline)

Reconciliation + tests, not a dashboard. Recommended free/OSS stack:

- **Reconciliation / audit tables.** At each hop, write a row count and a hash of key
  columns into an `AUDIT_*` table in Snowflake, tagged with `run_id`, `client`, `stage`,
  `captured_at`. A reconciliation model then compares hop-to-hop and flags drift
  (e.g. SQL Server pulled 51,004 rows but Snowflake RAW has 50,998 → 6 missing for client X).
- **dbt tests** for in-warehouse integrity: `unique`, `not_null`, `relationships`,
  `accepted_values`.
- **dbt packages** (free): `dbt_utils` (`equal_rowcount`, `fewer_rows_than`,
  `recency`) and `dbt_expectations` (Great Expectations-style column checks).
- **Elementary** (free, open-source dbt package): captures test results + volume/freshness
  anomalies and generates a hostable HTML observability report. **Strong candidate to start
  with** — it delivers most of Option 2 out of the box, no dashboard to hand-build.

### Layer B — viewing layer (Option 2 proper)

A dashboard that queries the `AUDIT_*` / Elementary tables **live** in Snowflake and shows,
per client: source vs landed vs loaded vs transformed counts, % match, failing tests, and
drift over time. Two build paths:

1. **Self-hosted on the Azure VM** (matches the "free / self-host" constraint): extend
   `dashboard.py` into a version that runs SQL against Snowflake (reusing
   `etl/snowflake_helpers.py`) and renders the same self-contained HTML. No new accounts.
2. **In-app live artifact**: a Cowork artifact that calls a **Snowflake connector** and
   refreshes on open. Needs the Snowflake connector enabled (see below).

---

## The hard part: the PCC source-of-truth hop

The SQL Server → Snowflake → dbt hops are easy to reconcile (both are queryable). The
**PointClickCare → SQL Server** hop is the real design problem:

- There is **no PointClickCare MCP connector**, and PCC does not expose an arbitrary-query
  API. An independent PCC count usually comes from PCC's **Data Extract / scheduled report
  feed** (or the SQL mirror already being pulled).
- **Decide early what the authoritative PCC number is** that you reconcile against. If the
  SQL Server DB *is* the only feed from PCC, then "PCC vs SQL Server" can't be independently
  verified and the first real checkpoint is SQL Server → Snowflake. Clarify this per client.

---

## Connectors (from registry search)

- **Snowflake** (Cortex) — exists, **not yet connected**. Needed only for the in-app live
  artifact path (Layer B path 2). The self-hosted path doesn't need it.
- **PointClickCare** — **no connector exists.** PCC data must come via extract/report feed.
- **Monte Carlo** — exists, but it's a paid enterprise data-observability platform. Listed
  for awareness; **Elementary is the free self-hosted equivalent** and fits the constraints.

---

## Constraints carried over from this project

- Free / open-source where possible; self-hosted on the existing **Azure VM**.
- Reuse existing code: `etl/snowflake_helpers.py`, `etl/sqlserver_helpers.py`, the
  `dbt_ciena/` project (models `stg_as_response`, `fct_as_response`).
- **Security:** `.env.example` in this repo contains real-looking SQL Server + Snowflake
  credentials — rotate them; keep real secrets only in `.env` (chmod 600).

---

## Suggested first steps for the new task

1. Confirm the authoritative PCC source per client (extract feed vs SQL mirror).
2. Stand up **Elementary** in `dbt_ciena/` and generate its report — see how far it gets you
   before building anything custom.
3. Define the `AUDIT_*` table schema and a reconciliation model for one client/table
   (`as_response`) end to end as the reference implementation.
4. Decide Layer B path: extend `dashboard.py` to query Snowflake, or build the in-app
   Snowflake artifact.
5. Generalize across clients (parameterize by client/schema).

## Open questions to resolve

- One Snowflake DB/schema per client, or shared with a `client` column?
- Reconciliation granularity: table-level counts only, or column-level hashes / row-level?
- How are mismatches alerted — extend the existing Teams notification (`etl/notify.py`),
  Elementary's alerts, or dashboard-only?
- Run cadence for validation: same nightly window as the ETL, or separate?
