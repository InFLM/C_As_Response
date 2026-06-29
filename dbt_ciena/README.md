# dbt_ciena

The transformation layer of the Ciena pipeline. Python lands raw data into Snowflake;
dbt turns that raw table into the cleaned, deduplicated history table.

## The idea in one line

You write a `SELECT` in a `.sql` file (a *model*); dbt wraps it in the
`CREATE VIEW` / `CREATE TABLE` / `MERGE` boilerplate and runs it against Snowflake.

## What's here

| Path | What it is |
|------|------------|
| `dbt_project.yml` | Project settings. Names the project, points at `models/`, and sets defaults: `staging/` builds as **views**, `marts/` builds as **incremental tables**, in schema `PCC`. |
| `profiles.yml` | The Snowflake connection (account, user, key-pair auth, warehouse). Kept separate from code; usually git-ignored. `.example` / `.test` are templates. |
| `models/staging/sources.yml` | Declares the raw inputs (`RAW_AS_RESPONSE_C`, loaded by the Python ETL) so SQL can reference them with `{{ source(...) }}`. |
| `models/staging/stg_as_response.sql` | **Staging** model. Light cleanup over the raw table + dedup to one row per `assess_response_id`. Built as a **view**. |
| `models/marts/fct_as_response.sql` | **Mart** / final table. The incremental **MERGE** on `assess_response_id` — the replacement for the Matillion "Combine" job. |
| `models/marts/schema.yml` | Tests + docs. Enforces `assess_response_id` is `unique`/`not_null` and `revision_date` is `not_null`. |
| `macros/` | Reusable SQL snippets. Empty for now. |
| `target/` | **Generated**, not source. The compiled SQL dbt sent to Snowflake + internal metadata. Safe to delete; rebuilt on each run. |
| `logs/dbt.log` | Run logs. Also regenerated. |

## How a run flows

`dbt build` reads `sources.yml` → builds `stg_as_response` (view) →
builds `fct_as_response` (merge) → runs the `schema.yml` tests.
The order is worked out automatically from the `{{ ref() }}` / `{{ source() }}` links.

## Common commands (run from this folder)

```bash
dbt debug                                   # check the Snowflake connection
dbt compile --select stg_as_response        # write resolved SQL to target/ without running
dbt build  --select stg_as_response fct_as_response             # build models + tests
dbt build  --select stg_as_response fct_as_response --full-refresh   # rebuild the table from scratch
```

## Conventions

- **Do not double-quote column names** in the model SQL. Snowflake folds bare identifiers
  to UPPERCASE, which keeps the columns, the `unique_key` config, and the tests all
  agreeing. Quoting locks in exact case and causes "invalid identifier" errors.
- The normal nightly run is driven by `etl/run_ciena_as_response.py`, which loads the raw
  table and then calls `dbt build` automatically.
