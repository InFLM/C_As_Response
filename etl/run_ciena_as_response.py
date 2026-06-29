"""Extract dbo.as_response for the recent revision_date window in a single pass,
then load to Snowflake and run dbt.

The source has no index on revision_date, so a date filter forces a full scan of
~2.4B rows. A full scan is unavoidable (the only indexes are the clustered
(assess_id, question_key) and a nonclustered PK on assess_response_id, neither of
which is ordered by revision time), so we do ONE scan rather than many: a single
server-side streaming cursor writes each chunk to its own Parquet file as it
arrives. If the connection drops, the whole extract is retried from scratch (there
is no cheap way to resume an unindexed scan partway through). See
extract_query_to_parquet in sqlserver_helpers.
"""
import base64
import os
import shutil
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

import socket
import traceback

from etl.sqlserver_helpers import extract_query_to_parquet, get_window_start
from etl.snowflake_helpers import load_parquet_folder_to_table
from etl.notify import send_notification

# Repo layout: this file is at <repo>/etl/run_ciena_as_response.py
REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = REPO_ROOT / "dbt_ciena"


def build_create_table_sql(raw_table: str) -> str:
    """DDL to bootstrap the raw landing table on the first run.

    Column names are UPPERCASE to match what stg_as_response selects
    ("ASSESS_RESPONSE_ID", etc.); the COPY uses MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    so the lowercase Parquet column names still line up. Types mirror the source
    dbo.as_response. Note: ACKNOWLEDGED is assumed boolean (SQL Server bit) -- if the
    source column is numeric, change it to NUMBER(38,0).
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        "ASSESS_RESPONSE_ID"        NUMBER(38,0),
        "ASSESS_ID"                 NUMBER(38,0),
        "QUESTION_KEY"              VARCHAR,
        "ITEM_VALUE"                VARCHAR,
        "ACKNOWLEDGED"              VARCHAR,
        "CREATED_DATE"              TIMESTAMP_NTZ,
        "POSITION_DESC"             VARCHAR,
        "REVISION_DATE"             TIMESTAMP_NTZ,
        "REVISION_BY"               VARCHAR,
        "LONG_USERNAME"             VARCHAR,
        "REVISION_DATE_UTC_OFFSET"  VARCHAR
    )
    """


def build_query(window_start: str) -> str:
    # Match Matillion's predicate EXACTLY so both jobs cover the same rows:
    #   cast(revision_date as date) >= <server-computed 15-day boundary>, no upper bound.
    # `window_start` is a 'YYYY-MM-DD' string resolved once per run from the server's
    # getdate() (see get_window_start), so the timezone/"today" reference matches Matillion.
    # NOLOCK avoids blocking the read-only source; OPTION (RECOMPILE) lets the optimizer use
    # the literal date. (No index on revision_date, so the cast costs nothing extra here.)
    return f"""
    SELECT
        assess_response_id,
        assess_id,
        question_key,
        item_value,
        acknowledged,
        created_date,
        position_desc,
        revision_date,
        revision_by,
        long_username,
        CONVERT(varchar(33), revision_date_utc_offset, 127) AS [revision_date_utc_offset]
    FROM dbo.as_response WITH (NOLOCK)
    WHERE CAST(revision_date AS date) >= '{window_start}'
    OPTION (RECOMPILE)
    """


def run_dbt() -> None:
    """Run the dbt transform that turns the freshly-loaded raw table into the
    final history table (stg_as_response -> fct_as_response).

    `dbt build` runs the models AND their tests (the unique/not_null checks on
    assess_response_id), so a duplicate slipping through fails loudly here.
    The dbt Snowflake profile reads SNOWFLAKE_* via env_var(); those were placed
    in os.environ by load_dotenv() above and are inherited by this subprocess.
    """
    if os.getenv("RUN_DBT", "true").lower() in ("0", "false", "no"):
        print("RUN_DBT disabled; skipping dbt. Run manually with: "
              f"cd {DBT_DIR} && dbt build --select stg_as_response fct_as_response")
        return

    dbt_exe = shutil.which("dbt")
    if dbt_exe is None:
        raise RuntimeError(
            "dbt not found on PATH. Activate the venv (it provides dbt-core/dbt-snowflake) "
            "or set RUN_DBT=false to skip the transform step."
        )

    # Bridge key-pair auth: our SNOWFLAKE_PRIVATE_KEY is base64-encoded PEM, but
    # dbt-snowflake's `private_key` field wants raw PEM (or base64 DER). Decode it once
    # into SNOWFLAKE_PRIVATE_KEY_PEM, which profiles.yml reads via env_var().
    env = os.environ.copy()
    b64_key = env.get("SNOWFLAKE_PRIVATE_KEY")
    if b64_key and not env.get("SNOWFLAKE_PRIVATE_KEY_PEM"):
        env["SNOWFLAKE_PRIVATE_KEY_PEM"] = base64.b64decode(b64_key).decode("utf-8")

    cmd = [
        dbt_exe, "build",
        "--select", "stg_as_response", "fct_as_response",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
    ]
    print(f"Running dbt: {' '.join(cmd)}")
    # check=True -> a failing model or test aborts the pipeline with a non-zero exit.
    subprocess.run(cmd, cwd=str(DBT_DIR), check=True, env=env)
    print("dbt build complete.")


def main() -> str:
    # Load .env if present; fall back to .env.test (where the key-pair creds live) for
    # any vars not already set. Real shell/system env always takes precedence.
    load_dotenv()
    load_dotenv(REPO_ROOT / ".env.test", override=False)

    lookback_days = int(os.getenv("AS_RESPONSE_LOOKBACK_DAYS", "15"))
    landing_root = Path(os.getenv("LANDING_DIR", "./landing"))
    # Must match the dbt source (ciena_pcc_raw.RAW_AS_RESPONSE_C in sources.yml).
    raw_table = os.getenv("RAW_AS_RESPONSE_TABLE", "CIENA.PCC.RAW_AS_RESPONSE_C")
    # Resume an aborted run by setting EXTRACT_RUN_ID to an existing run_id folder;
    # otherwise a fresh timestamped folder is used.
    run_id = os.getenv("EXTRACT_RUN_ID") or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_folder = landing_root / "ciena" / "as_response" / f"run_id={run_id}"

    # Resolve the 15-day window start FROM THE SERVER (getdate()), exactly like Matillion,
    # so the two jobs cover the same window regardless of client timezone. Override with
    # WINDOW_START_DATE=YYYY-MM-DD to pin an explicit boundary (use the same value in both
    # tools when reconciling them against one fixed window).
    window_start = os.getenv("WINDOW_START_DATE") or get_window_start(lookback_days)

    # No index on revision_date means the date filter scans all ~2.4B rows. We do this in
    # a single streaming pass and write each chunk to its own Parquet file as it arrives.
    print(
        f"Extracting dbo.as_response for cast(revision_date as date) >= {window_start} "
        f"(server-derived {lookback_days}-day window, matches Matillion) -> {output_folder}"
    )

    rows = extract_query_to_parquet(
        query=build_query(window_start),
        output_path=str(output_folder),
        chunksize=100_000,   # fewer round-trips over the fragile link than 5,000
        max_retries=5,
    )
    print(f"Extract complete: {rows:,} rows")

    if rows == 0:
        print("No rows extracted; skipping Snowflake load and dbt.")
        return (
            f"No rows extracted for revision_date >= {window_start}. "
            "Snowflake load and dbt were skipped."
        )

    print(f"Loading Parquet files to Snowflake table {raw_table} (truncate + load)...")
    # create_table_sql -> bootstrap the table on first run (no-op afterwards).
    # truncate_target=True -> raw table holds exactly this pull, so the dbt MERGE
    # downstream never sees duplicate assess_response_id rows.
    load_parquet_folder_to_table(
        str(output_folder),
        target_table=raw_table,
        truncate_target=True,
        create_table_sql=build_create_table_sql(raw_table),
    )
    print("Snowflake load complete.")

    # Chain the transform: raw -> staging -> incremental merge into history.
    run_dbt()
    print("Pipeline complete.")

    return (
        f"Extracted {rows:,} rows for cast(revision_date as date) >= {window_start}.\n"
        f"Loaded into {raw_table} and ran dbt build (stg_as_response, fct_as_response)."
    )


if __name__ == "__main__":
    # Load creds up front so the failure email can still send if main() raises early.
    load_dotenv()
    load_dotenv(REPO_ROOT / ".env.test", override=False)
    host = socket.gethostname()
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        summary = main()
        send_notification(
            subject="✅ Ciena ETL — SUCCESS",
            body=f"Host:    {host}\nStarted: {started}\n\n{summary}",
        )
    except Exception:
        tb = traceback.format_exc()
        send_notification(
            subject="❌ Ciena ETL — FAILED",
            body=f"Host:    {host}\nStarted: {started}\n\n{tb}",
        )
        # Re-raise so the exit code is non-zero and the log/scheduler show the failure.
        raise
