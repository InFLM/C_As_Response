import glob
import json
import math
import os
import shutil
import urllib.parse
from dataclasses import dataclass

import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlServerConfig:
    driver: str
    server: str
    database: str
    user: str
    password: str
    trust_cert: str = "yes"

    @classmethod
    def from_env(cls) -> "SqlServerConfig":
        return cls(
            driver=os.environ["SQLSERVER_DRIVER"],
            server=os.environ["SQLSERVER_SERVER"],
            database=os.environ["SQLSERVER_DATABASE"],
            user=os.environ["SQLSERVER_USER"],
            password=os.environ["SQLSERVER_PASSWORD"],
            trust_cert=os.getenv("SQLSERVER_TRUST_CERT", "yes"),
        )

    def connection_string(self) -> str:
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "Encrypt=yes;"
            f"TrustServerCertificate={self.trust_cert};"
            "Connection Timeout=60;"
            "ApplicationIntent=ReadOnly;"
        )


def connect_sqlserver(config: SqlServerConfig | None = None) -> pyodbc.Connection:
    # config = config or SqlServerConfig.from_env()
    # return pyodbc.connect(config.connection_string())
    config = config or SqlServerConfig.from_env()

    conn = pyodbc.connect(
        config.connection_string() + "Connection Timeout=60;",
        timeout=60,
    )

    # Query timeout in seconds. 0 = no timeout.
    conn.timeout = 0

    return conn


def get_window_start(lookback_days: int) -> str:
    """Return the extract window's start date AS COMPUTED BY THE SQL SERVER ('YYYY-MM-DD').

    Matillion filters with `cast(revision_date as date) >= dateadd(day, -N,
    cast(getdate() as date))`, where getdate() is the *server's* clock and timezone.
    Computing the boundary on the client (date.today()) can land on a different calendar day
    when the server and client timezones differ -- which is why the two jobs disagreed on
    row counts. We ask the server for the exact same value so our window matches Matillion's,
    and we resolve it ONCE per run so every query uses one fixed boundary even if the run
    crosses midnight.
    """
    # int() guards against injection; CONVERT(...,23) yields ISO yyyy-mm-dd.
    sql = (
        f"SELECT CONVERT(varchar(10), "
        f"DATEADD(day, -{int(lookback_days)}, CAST(GETDATE() AS date)), 23)"
    )
    with connect_sqlserver() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
    if not row or not row[0]:
        raise ValueError("Could not compute window start from server GETDATE().")
    return str(row[0])


def extract_query_to_parquet(
    query: str,
    output_path: str,
    chunksize: int = 100_000,
    max_retries: int = 5,
    retry_backoff_seconds: float = 10.0,
) -> int:
    """Stream a SQL Server query to Parquet in a single pass.

    The source table is read-only and has no indexes, so any predicate on revision_date
    forces a FULL TABLE SCAN. The right move is therefore ONE scan, not one per day:
    we open a single server-side streaming cursor and write each chunk to its own Parquet
    file as it arrives, so the result set is never fully held in memory.

    Resilience: the upstream connection is fragile. Because there is no index, there is no
    cheap way to resume a scan partway through, so if the connection drops we retry the
    WHOLE extract from scratch with exponential-ish backoff. The output folder is cleared
    at the start of every attempt, so a failed/partial scan can never leave behind
    duplicate or half-written part files.

    Returns total rows written.
    """
    attempt = 0
    while True:
        attempt += 1

        # Clean slate each attempt: a dropped scan must not leave partial/duplicate files.
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path, exist_ok=True)

        total_rows = 0
        try:
            with connect_sqlserver() as conn:
                for i, chunk in enumerate(
                    pd.read_sql(query, conn, chunksize=chunksize), start=1
                ):
                    file_path = os.path.join(output_path, f"part-{i:05d}.parquet")
                    chunk.to_parquet(file_path, index=False, engine="pyarrow")
                    total_rows += len(chunk)
                    logger.info(
                        "wrote %s rows (running total %s) -> %s",
                        f"{len(chunk):,}",
                        f"{total_rows:,}",
                        file_path,
                    )
            return total_rows

        # pd.read_sql wraps pyodbc failures in pandas.errors.DatabaseError, so catch BOTH --
        # otherwise a mid-query drop (SQLExecDirectW/SQLFetch) escapes the retry entirely.
        except (pyodbc.Error, pd.errors.DatabaseError) as exc:
            if attempt >= max_retries:
                logger.error("Extract failed after %s attempt(s): %s", attempt, exc)
                raise
            wait = retry_backoff_seconds * attempt
            logger.warning(
                "Connection dropped on attempt %s/%s (%s). Retrying in %.0fs...",
                attempt,
                max_retries,
                exc,
                wait,
            )
            time.sleep(wait)


def get_key_bounds(key_col: str, table: str) -> tuple[int, int]:
    """Return (min, max) of an indexed key column -- cheaply.

    dbo.as_response is clustered on (assess_id, question_key), so MIN/MAX of assess_id is
    satisfied from the first/last pages of the clustered index (an index seek), NOT a full
    scan. Used only to compute chunk boundaries; the revision_date filtering happens per chunk.
    """
    sql = f"SELECT MIN({key_col}) AS lo, MAX({key_col}) AS hi FROM {table} WITH (NOLOCK)"
    with connect_sqlserver() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
    if not row or row[0] is None:
        raise ValueError(f"No rows / NULL bounds for {key_col} in {table}")
    return int(row[0]), int(row[1])


def extract_chunked_to_parquet(
    build_chunk_query,
    output_path: str,
    key_lo: int,
    key_hi: int,
    n_chunks: int = 300,
    chunksize: int = 100_000,
    max_retries: int = 5,
    retry_backoff_seconds: float = 10.0,
    resume: bool = True,
    chunk_sink=None,
) -> int:
    """Extract a revision_date window in resumable chunks along the clustered assess_id key.

    chunk_sink: optional callable(list_of_parquet_paths) invoked after each chunk is written.
        When provided, the pipeline STREAMS: each chunk's Parquet is handed to the sink (e.g.
        loaded to Snowflake) and then DELETED locally before the next chunk starts, so peak
        disk usage is ~one chunk instead of the whole window. The sink runs inside the chunk's
        retry/manifest guard, so a chunk is marked complete only after it both extracted AND
        sank successfully; a crash in between re-does the chunk on resume (safe/idempotent).
        When None, every chunk's Parquet is kept on disk (original behavior).

    Why (vs. single-pass extract_query_to_parquet): the source is a log-shipping standby that
    drops every session each time it restores a log, so one long full scan reliably gets
    killed mid-read. The table IS clustered on assess_id, so each chunk
    (WHERE assess_id >= lo AND assess_id < hi AND <revision_date filter>) is a bounded range
    scan that finishes between restores. A dropped chunk is retried on its own -- catching
    both pyodbc errors AND the pandas.errors.DatabaseError that pd.read_sql wraps them in --
    and progress is checkpointed to <output_path>/_manifest.json, so an aborted run resumes
    (re-run with EXTRACT_RUN_ID set to the same run_id). Tune piece size with n_chunks.

    build_chunk_query: callable(lo, hi) -> SQL for the half-open [lo, hi) assess_id range.
    """
    os.makedirs(output_path, exist_ok=True)
    manifest_path = os.path.join(output_path, "_manifest.json")

    completed: dict[str, int] = {}
    total_rows = 0
    if resume and os.path.exists(manifest_path):
        with open(manifest_path) as fh:
            state = json.load(fh)
        completed = state.get("completed", {})
        total_rows = state.get("total_rows", 0)
        logger.info(
            "Resuming: %s chunk(s) done, %s rows so far.", len(completed), f"{total_rows:,}"
        )
    else:
        for f in glob.glob(os.path.join(output_path, "*.parquet")):
            os.remove(f)

    step = max(1, math.ceil((key_hi - key_lo + 1) / max(1, n_chunks)))
    ranges = []
    start = key_lo
    while start <= key_hi:
        ranges.append((start, start + step))   # half-open; final end may overshoot (ok)
        start += step
    logger.info(
        "Extracting in %s chunk(s) of ~%s assess_id-units over [%s, %s].",
        len(ranges), f"{step:,}", key_lo, key_hi,
    )

    def _save_manifest() -> None:
        tmp = manifest_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({"completed": completed, "total_rows": total_rows}, fh)
        os.replace(tmp, manifest_path)   # atomic: a crash can't corrupt the manifest

    for idx, (clo, chi) in enumerate(ranges, start=1):
        key = f"{clo}-{chi}"
        if key in completed:
            continue
        attempt = 0
        while True:
            attempt += 1
            # Clear partial files from a previous failed attempt of THIS chunk only.
            for f in glob.glob(os.path.join(output_path, f"part-{clo}-{chi}-*.parquet")):
                os.remove(f)
            try:
                rows_this = 0
                chunk_files: list[str] = []
                with connect_sqlserver() as conn:
                    for j, chunk in enumerate(
                        pd.read_sql(build_chunk_query(clo, chi), conn, chunksize=chunksize),
                        start=1,
                    ):
                        fp = os.path.join(output_path, f"part-{clo}-{chi}-{j:03d}.parquet")
                        chunk.to_parquet(fp, index=False, engine="pyarrow")
                        chunk_files.append(fp)
                        rows_this += len(chunk)
                # Stream this chunk to its destination and free the disk before the next chunk,
                # so peak local usage stays at ~one chunk. Done INSIDE the try so a sink failure
                # leaves the chunk un-marked and it re-runs on retry/resume (idempotent).
                if chunk_sink is not None:
                    chunk_sink(chunk_files)
                    for fp in chunk_files:
                        try:
                            os.remove(fp)
                        except OSError:
                            pass
                completed[key] = rows_this
                total_rows += rows_this
                _save_manifest()
                logger.info(
                    "chunk %s/%s assess_id [%s, %s): %s rows (running total %s)",
                    idx, len(ranges), clo, chi, f"{rows_this:,}", f"{total_rows:,}",
                )
                break
            except (pyodbc.Error, pd.errors.DatabaseError) as exc:
                if attempt >= max_retries:
                    logger.error(
                        "Chunk assess_id [%s, %s) failed after %s attempt(s): %s",
                        clo, chi, attempt, exc,
                    )
                    raise
                wait = retry_backoff_seconds * attempt
                logger.warning(
                    "chunk [%s, %s) dropped on attempt %s/%s (%s). Retrying in %.0fs...",
                    clo, chi, attempt, max_retries, exc, wait,
                )
                time.sleep(wait)

    return total_rows
