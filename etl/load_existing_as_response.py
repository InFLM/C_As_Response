from dotenv import load_dotenv

from etl.snowflake_helpers import load_parquet_folder_to_table

load_dotenv(".env.test")

load_parquet_folder_to_table(
    folder_path="landing/ciena/as_response/run_id=20260610_123059",
    target_table="CIENA.PCC.test_RAW_AS_RESPONSE_C",
)

print("Snowflake load complete.")