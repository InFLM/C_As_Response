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

from etl.sqlserver_helpers import (
    extract_chunked_to_parquet,
    get_key_bounds,
    get_window_start,
)
from etl.snowflake_helpers import prepare_raw_table, load_parquet_files_append
from etl.notify import send_notification
from etl.reconcile import reconcile_run

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


def build_chunk_query(assess_id_lo: int, assess_id_hi: int, window_start: str) -> str:
    """One chunk of the extract: the revision_date window restricted to a half-open
    assess_id range [lo, hi).

    assess_id is the leading column of the table's CLUSTERED index, so
    `assess_id >= lo AND assess_id < hi` is a bounded clustered range scan -- a small slice
    of the table instead of one giant full scan that a log restore can kill. The
    revision_date predicate matches build_query exactly.
    """
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
    WHERE assess_id >= {assess_id_lo} AND assess_id < {assess_id_hi}
      AND CAST(revision_date AS date) >= '{window_start}'
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

    # Keep dbt's incremental window in lockstep with the extract window: a widened lookback
    # (for self-healing) must not leave freshly-extracted rows unmerged.
    lookback_days = int(os.getenv("AS_RESPONSE_LOOKBACK_DAYS", "25"))
    cmd = [
        dbt_exe, "build",
        "--select", "stg_as_response", "fct_as_response",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--vars", f'{{"as_response_lookback_days": {lookback_days}}}',
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

    # Widened from 15 -> 25 days for self-healing: with no index the scan cost is identical
    # (full scan either way), so the extra overlap only adds a little transfer/merge but gives
    # any row a NOLOCK read missed many more chances to be re-pulled on a later run. The MERGE
    # is idempotent, so the extra overlap can never duplicate. dbt's window is kept in sync
    # via --vars in run_dbt().
    lookback_days = int(os.getenv("AS_RESPONSE_LOOKBACK_DAYS", "25"))
    landing_root = Path(os.getenv("LANDING_DIR", "./landing"))
    # Must match the dbt source (ciena_pcc_raw.RAW_AS_RESPONSE_C in sources.yml).
    raw_table = os.getenv("RAW_AS_RESPONSE_TABLE", "CIENA.PCC.RAW_AS_RESPONSE_C")
    # Resume an aborted run by setting EXTRACT_RUN_ID to an existing run_id folder;
    # otherwise a fresh timestamped folder is used.
    run_id = os.getenv("EXTRACT_RUN_ID") or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_folder = landing_root / "ciena" / "as_response" / f"run_id={run_id}"

    # Disk hygiene: each run writes ~GBs of parquet, so leftover folders from prior runs
    # (especially failed ones, which don't reach the post-load cleanup) will fill a small VM
    # disk over weeks. Before writing anything, remove every sibling run folder except this
    # run's own (which may be a resume target via EXTRACT_RUN_ID). Successful runs also
    # self-clean after the load below.
    runs_parent = output_folder.parent
    if runs_parent.exists():
        for d in runs_parent.iterdir():
            if d.is_dir() and d != output_folder:
                shutil.rmtree(d, ignore_errors=True)

    # Resolve the 15-day window start FROM THE SERVER (getdate()), exactly like Matillion,
    # so the two jobs cover the same window regardless of client timezone. Override with
    # WINDOW_START_DATE=YYYY-MM-DD to pin an explicit boundary (use the same value in both
    # tools when reconciling them against one fixed window).
    window_start = os.getenv("WINDOW_START_DATE") or get_window_start(lookback_days)

    # No index on revision_date means the date filter scans all ~2.4B rows -- one long,
    # uninterruptible pass that the log-shipping standby kills whenever it restores a log.
    # The table IS clustered on assess_id, so we walk that key in resumable ranges: each chunk
    # is a bounded range scan that fits between restores and is retried on its own if dropped.
    # Tune the chunk count with EXTRACT_CHUNKS; resume an aborted run with EXTRACT_RUN_ID.
    n_chunks = int(os.getenv("EXTRACT_CHUNKS", "300"))
    key_lo, key_hi = get_key_bounds("assess_id", "dbo.as_response")

    print(
        f"Extracting dbo.as_response for cast(revision_date as date) >= {window_start} "
        f"(server-derived {lookback_days}-day window) across assess_id [{key_lo}, {key_hi}] "
        f"in ~{n_chunks} chunk(s) -> {output_folder}"
    )

    # STREAMING load: prepare the raw table ONCE, then load+delete each chunk as it is
    # extracted (via the chunk_sink below). This keeps at most ~one chunk of Parquet on the VM
    # disk at a time, so a wide window (e.g. a 90-day backfill) can't exhaust the small OS disk.
    # Truncate only on a FRESH run -- a resume (EXTRACT_RUN_ID) must keep already-loaded chunks.
    manifest_path = output_folder / "_manifest.json"
    is_resume = manifest_path.exists()
    print(
        f"Preparing {raw_table} for streaming load "
        f"({'RESUME -- no truncate' if is_resume else 'fresh -- truncate'})."
    )
    prepare_raw_table(
        target_table=raw_table,
        create_table_sql=build_create_table_sql(raw_table),
        truncate=not is_resume,
    )

    def _chunk_sink(files: list[str]) -> None:
        # Append this chunk to the already-prepared raw table; the extractor deletes the
        # local files right after this returns.
        load_parquet_files_append(files, target_table=raw_table)

    rows = extract_chunked_to_parquet(
        build_chunk_query=lambda lo, hi: build_chunk_query(lo, hi, window_start),
        output_path=str(output_folder),
        key_lo=key_lo,
        key_hi=key_hi,
        n_chunks=n_chunks,
        chunksize=100_000,   # fewer round-trips over the fragile link than 5,000
        max_retries=5,
        chunk_sink=_chunk_sink,
    )
    print(f"Extract + streaming load complete: {rows:,} rows into {raw_table}")

    if rows == 0:
        print("No rows extracted; skipping dbt.")
        return (
            f"No rows extracted for revision_date >= {window_start}. "
            f"{raw_table} was truncated to empty and dbt was skipped."
        )

    # All chunks have already been streamed to Snowflake and their local Parquet deleted.
    # Remove the (now effectively empty) run folder/manifest unless KEEP_LANDING is set.
    if os.getenv("KEEP_LANDING", "").lower() not in ("1", "true", "yes"):
        shutil.rmtree(output_folder, ignore_errors=True)
        print(f"Cleaned up landing folder: {output_folder}")

    # Chain the transform: raw -> staging -> incremental merge into history.
    run_dbt()
    print("Pipeline complete.")

    # Reconcile: raw-vs-final per-day + load integrity, written to an AUDIT table with a Teams
    # alert on real drift. Wrapped so a reconciliation error never fails an otherwise-good run
    # (the data is already loaded); you'd just re-run, which is safe (idempotent).
    final_table = os.getenv("FINAL_AS_RESPONSE_TABLE", '"as_response"')
    tolerance = int(os.getenv("RECON_TOLERANCE", "0"))
    try:
        _, recon_summary = reconcile_run(
            run_id=run_id,
            window_start=window_start,
            extracted_rows=rows,
            raw_table=raw_table,
            final_table=final_table,
            tolerance=tolerance,
        )
    except Exception as exc:   # noqa: BLE001 -- reconciliation must never fail the load
        recon_summary = f"Reconciliation check errored (non-fatal): {exc}"
    print(recon_summary)

    return (
        f"Extracted {rows:,} rows for cast(revision_date as date) >= {window_start}.\n"
        f"Loaded into {raw_table} and ran dbt build (stg_as_response, fct_as_response).\n"
        f"{recon_summary}"
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
