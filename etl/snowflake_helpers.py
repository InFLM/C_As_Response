import base64
import os
from dataclasses import dataclass
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    warehouse: str
    database: str
    schema: str
    password: str | None = None
    private_key: str | None = None
    authenticator: str | None = None
    role: str | None = None

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.getenv("SNOWFLAKE_PASSWORD") or None,
            private_key=os.getenv("SNOWFLAKE_PRIVATE_KEY") or None,
            authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR") or None,
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
            role=os.getenv("SNOWFLAKE_ROLE") or None,
        )


def _load_private_key(private_key_b64: str):
    private_key_bytes = base64.b64decode(private_key_b64)

    return serialization.load_pem_private_key(
        private_key_bytes,
        password=None,
    )


def connect_snowflake(config: SnowflakeConfig | None = None):
    config = config or SnowflakeConfig.from_env()

    connect_args = {
        "account": config.account,
        "user": config.user,
        "warehouse": config.warehouse,
        "database": config.database,
        "schema": config.schema,
    }

    if config.role:
        connect_args["role"] = config.role

    # Preferred for automated ETL: key-pair auth
    if config.private_key:
        connect_args["private_key"] = _load_private_key(config.private_key)

    # Manual fallback: SSO/browser auth
    elif config.authenticator:
        connect_args["authenticator"] = config.authenticator

    # Last fallback: password auth
    elif config.password:
        connect_args["password"] = config.password

    else:
        raise ValueError(
            "No Snowflake authentication method found. Set one of: "
            "SNOWFLAKE_PRIVATE_KEY, SNOWFLAKE_AUTHENTICATOR, or SNOWFLAKE_PASSWORD."
        )

    return snowflake.connector.connect(**connect_args)


def load_parquet_folder_to_table(
    folder_path: str,
    target_table: str,
    stage_name: str = "PY_ETL_STAGE",
    file_format_name: str = "PY_PARQUET_FORMAT",
    truncate_target: bool = False,
    create_table_sql: str | None = None,
) -> None:
    """Upload local Parquet files to an internal Snowflake stage and COPY into a table.

    truncate_target:
        When True, the target table is TRUNCATEd before the COPY so the load is
        idempotent -- the table ends up holding exactly the current extract, never an
        accumulation of overlapping runs. This mirrors the Matillion `as_response_c`
        landing table, which Matillion replace-loads each run; the downstream dbt
        incremental model then MERGEs that clean window into the history table on
        assess_response_id. Leave False for append-style loads.
    create_table_sql:
        Optional DDL run before anything else. Use a `CREATE TABLE IF NOT EXISTS ...`
        so the first run can bootstrap the target table (COPY does not create it).
        Idempotent and harmless on every subsequent run.
    """
    folder = Path(folder_path)
    # parquet_files = sorted(folder.glob("*.parquet"))
    parquet_files = sorted(folder.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {folder_path}")

    with connect_snowflake() as conn:
        cur = conn.cursor()
        try:
            if create_table_sql:
                # Bootstrap the target table if it does not exist yet.
                cur.execute(create_table_sql)

            cur.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
            cur.execute(
                f"CREATE FILE FORMAT IF NOT EXISTS {file_format_name} TYPE = PARQUET"
            )
            cur.execute(f"REMOVE @{stage_name}")

            if truncate_target:
                # Replace the landing table contents so re-runs can't duplicate rows.
                cur.execute(f"TRUNCATE TABLE IF EXISTS {target_table}")

            for file in parquet_files:
                cur.execute(
                    f"PUT 'file://{file.resolve()}' @{stage_name} "
                    "AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
                )

            cur.execute(
                f"""
                COPY INTO {target_table}
                FROM @{stage_name}
                FILE_FORMAT = (FORMAT_NAME = {file_format_name})
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = FALSE
                """
            )
        finally:
            cur.close()


#---------------------------------------------

# import os
# from dataclasses import dataclass
# from pathlib import Path

# import snowflake.connector


# @dataclass(frozen=True)
# class SnowflakeConfig:
#     account: str
#     user: str
#     password: str
#     warehouse: str
#     database: str
#     schema: str
#     role: str | None = None

#     @classmethod
#     def from_env(cls) -> "SnowflakeConfig":
#         return cls(
#             account=os.environ["SNOWFLAKE_ACCOUNT"],
#             user=os.environ["SNOWFLAKE_USER"],
#             password=os.environ["SNOWFLAKE_PASSWORD"],
#             warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
#             database=os.environ["SNOWFLAKE_DATABASE"],
#             schema=os.environ["SNOWFLAKE_SCHEMA"],
#             role=os.getenv("SNOWFLAKE_ROLE") or None,
#         )


# def connect_snowflake(config: SnowflakeConfig | None = None):
#     config = config or SnowflakeConfig.from_env()
#     # If SSO authenticator is set, use it instead of password
#     auth = os.getenv("SNOWFLAKE_AUTHENTICATOR")
#     connect_args = {
#         "account": config.account,
#         "user": config.user,
#         "role": config.role,
#         "warehouse": config.warehouse,
#         "database": config.database,
#         "schema": config.schema,
#     }
#     if auth:
#         connect_args["authenticator"] = auth
#     else:
#         connect_args["password"] = config.password
    
#     return snowflake.connector.connect(**connect_args)



# def load_parquet_folder_to_table(
#     folder_path: str,
#     target_table: str,
#     stage_name: str = "PY_ETL_STAGE",
#     file_format_name: str = "PY_PARQUET_FORMAT",
# ) -> None:
#     """Upload local Parquet files to an internal Snowflake stage and COPY into a table.

#     This is a starter approach. For production, prefer Azure Blob/S3 external stages
#     so app servers do not upload huge files through the Snowflake connection.
#     """
#     folder = Path(folder_path)
#     parquet_files = sorted(folder.glob("*.parquet"))
#     if not parquet_files:
#         raise FileNotFoundError(f"No Parquet files found in {folder_path}")

#     with connect_snowflake() as conn:
#         cur = conn.cursor()
#         try:
#             cur.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
#             cur.execute(
#                 f"CREATE FILE FORMAT IF NOT EXISTS {file_format_name} TYPE = PARQUET"
#             )
#             cur.execute(f"REMOVE @{stage_name}")

#             for file in parquet_files:
#                 cur.execute(
#                     f"PUT 'file://{file.resolve()}' @{stage_name} "
#                     "AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
#                 )

#             # Assumes target_table already exists with matching columns.
#             # For production, generate or manage DDL explicitly.
#             cur.execute(
#                 f"""
#                 COPY INTO {target_table}
#                 FROM @{stage_name}
#                 FILE_FORMAT = (FORMAT_NAME = {file_format_name})
#                 MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
#                 PURGE = FALSE
#                 """
#             )
#         finally:
#             cur.close()
