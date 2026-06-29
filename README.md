# Matillion Replacement Starter: Python + dbt + Snowflake

This is a starter implementation based on the Matillion YAML files for the Ciena `AS_Response` pipeline.

## Ciena AS_Response pipeline process

Process:
1. Pull data from ciena.dbo.as_response in SQL Server where the revision date is in the last 15 days. This is transferred to snowflake, ciena.pcc.as_response_c
2. Pull all data from snowflake tbl ciena.pcc.stage_as_response joined to ciena.pcc.as_response_c. Only grab the assess_response_ids that are null from ciena.pcc.as_response_c. union to the data that was transferred to ciena.pcc.as_response_c. Like merge-upsert (?). This is transferred to ciena.pcc.as_response in snowflake, production tbl.
3. Pull all data from ciena.pcc.as_response in snowflake into ciena.pcc.stage_as_response. This is for next time's step 2

## What this replaces

The Matillion flow you shared is approximately:

```text
SQL Server Ciena.dbo.as_response, last 15 days
    -> Snowflake CIENA.PCC.as_response_c
    -> Snowflake transformation combines stage_as_response + as_response_c
    -> final CIENA.PCC.as_response
    -> stage_as_response is rebuilt from CIENA.PCC.as_response
```

This starter converts that into:

```text
SQL Server
    -> Python extract
    -> Parquet landing files
    -> Snowflake raw/staging table
    -> dbt incremental model
    -> final reporting table
```

## Where to run dbt

Run dbt from the same place you run the Python project, usually VS Code connected to WSL.

Example:

```bash
cd ~/Desktop/matillion_replacement_starter/dbt_ciena
dbt debug
dbt run --select stg_as_response fct_as_response
```

You can run Python from the project root:

```bash
cd ~/Desktop/matillion_replacement_starter
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env
python etl/run_ciena_as_response.py
```

## Recommended dev flow in VS Code

1. Open this folder in VS Code.
2. Use the WSL terminal.
3. Create/activate the virtual environment.
4. Fill out `.env`.
5. Run the Python extractor.
6. Run dbt from `dbt_ciena/`.
7. Once tested, schedule `etl/run_ciena_as_response.py` and `dbt run` with SQL Server Agent or Windows Task Scheduler.

## Important security note

Your Matillion export appears to contain database credentials in plain text. Rotate those credentials or at least treat the export files as sensitive.

## Files

```text
etl/run_ciena_as_response.py       Python extraction + Snowflake load starter
etl/snowflake_helpers.py           Snowflake connection/loading helpers
etl/sqlserver_helpers.py           SQL Server extraction helpers
dbt_ciena/dbt_project.yml          dbt project config
dbt_ciena/profiles.yml.example     dbt profile template
dbt_ciena/models/staging           source + staging models
dbt_ciena/models/marts             final incremental model
sql_agent/example_sql_agent_step.sql Example job step command
```
