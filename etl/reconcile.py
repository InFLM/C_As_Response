"""Post-run reconciliation checks: catch missing/duplicated data and record an audit trail.

Two cheap checks run after every merge -- both entirely in Snowflake against tables already
loaded this run, so they add NO extra scan of the fragile SQL Server source:

  1. LOAD integrity   -- rows the extract wrote  vs  COUNT(*) that landed in the raw table.
                         Catches a partial/failed COPY.
  2. MERGE integrity  -- per revision-day, COUNT in the raw landing table vs the final table.
                         `final < raw` for a day means the merge LOST rows (a real problem);
                         `final > raw` is informational (source delete, or a row the extract
                         missed this run that was captured before).

Results are written to an AUDIT table (one row per check), and a Teams alert fires only on
genuine drift (a load mismatch, or a day where the final table is short). Because the pipeline
is idempotent (truncate-load raw + keyed MERGE), the response to drift is simply: re-run.

A source-vs-Snowflake check would need another full scan of dbo.as_response, so it is NOT done
here every run; run that separately/periodically if you want end-to-end source assurance.
"""
import os
from datetime import datetime

from etl.snowflake_helpers import connect_snowflake
from etl.notify import send_notification

AUDIT_TABLE = os.getenv("AUDIT_TABLE", "AUDIT_AS_RESPONSE")


def _audit_ddl(table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        RUN_ID        VARCHAR,
        CHECK_TYPE    VARCHAR,
        REVISION_DAY  DATE,
        RAW_COUNT     NUMBER,
        FINAL_COUNT   NUMBER,
        DIFF          NUMBER,
        CHECKED_AT    TIMESTAMP_NTZ
    )
    """


def reconcile_run(
    run_id: str,
    window_start: str,
    extracted_rows: int,
    raw_table: str,
    final_table: str,
    tolerance: int = 0,
) -> tuple[bool, str]:
    """Reconcile the raw landing table against the final merged table for this run.

    raw_table / final_table are the identifiers as used elsewhere in the pipeline (e.g.
    '"as_response_c"' / '"as_response"'); they resolve against the connection's schema.
    Returns (drift_found, human_summary). Never raises for "drift" -- only for real errors.
    """
    checked_at = datetime.now()
    issues: list[str] = []
    audit_rows: list[tuple] = []

    with connect_snowflake() as conn:
        cur = conn.cursor()
        cur.execute(_audit_ddl(AUDIT_TABLE))

        # 1) LOAD integrity: did every extracted row land in the raw table?
        cur.execute(f"SELECT COUNT(*) FROM {raw_table}")
        raw_total = int(cur.fetchone()[0])
        load_diff = raw_total - extracted_rows
        audit_rows.append(
            (run_id, "LOAD", None, extracted_rows, raw_total, load_diff, checked_at)
        )
        if load_diff != 0:
            issues.append(
                f"LOAD mismatch: extracted {extracted_rows:,} but {raw_total:,} landed "
                f"(diff {load_diff:+,})."
            )

        # 2) MERGE integrity: per-day raw vs final within the window.
        cur.execute(
            f'SELECT CAST("revision_date" AS date) AS d, COUNT(*) '
            f"FROM {raw_table} "
            f'WHERE CAST("revision_date" AS date) >= %s GROUP BY 1',
            (window_start,),
        )
        raw_by_day = {r[0]: int(r[1]) for r in cur.fetchall()}

        cur.execute(
            f'SELECT CAST("revision_date" AS date) AS d, COUNT(*) '
            f"FROM {final_table} "
            f'WHERE CAST("revision_date" AS date) >= %s GROUP BY 1',
            (window_start,),
        )
        final_by_day = {r[0]: int(r[1]) for r in cur.fetchall()}

        for d in sorted(set(raw_by_day) | set(final_by_day)):
            rc = raw_by_day.get(d, 0)
            fc = final_by_day.get(d, 0)
            diff = fc - rc
            audit_rows.append((run_id, "MERGE_BY_DAY", d, rc, fc, diff, checked_at))
            # Only a SHORT final table is a real loss; final > raw is informational.
            if diff < -tolerance:
                issues.append(f"{d}: final {fc:,} < raw {rc:,} (merge short by {-diff:,}).")

        cur.executemany(
            f"INSERT INTO {AUDIT_TABLE} "
            "(RUN_ID, CHECK_TYPE, REVISION_DAY, RAW_COUNT, FINAL_COUNT, DIFF, CHECKED_AT) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            audit_rows,
        )

    drift = bool(issues)
    if drift:
        body = (
            f"Run {run_id} (window revision_date >= {window_start})\n"
            f"{len(issues)} issue(s):\n- " + "\n- ".join(issues[:30])
            + "\n\nPipeline is idempotent -- re-running the extract+merge is safe and will "
            "converge. Audit detail in " + AUDIT_TABLE + "."
        )
        send_notification(subject="⚠️ Ciena ETL — reconciliation DRIFT", body=body)
        summary = f"Reconciliation DRIFT: {len(issues)} issue(s) (see {AUDIT_TABLE})."
    else:
        summary = (
            f"Reconciliation OK: {raw_total:,} rows landed = {extracted_rows:,} extracted; "
            f"per-day raw vs final match within tolerance {tolerance} for "
            f"revision_date >= {window_start}. Audit written to {AUDIT_TABLE}."
        )
    return drift, summary
