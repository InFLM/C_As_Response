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

        except (pyodbc.OperationalError, pyodbc.Error) as exc:
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
