-- Bare (unquoted) identifiers throughout: Snowflake folds them to UPPERCASE, which
-- matches the raw table's UPPERCASE columns and keeps the column names consistent with
-- the unique_key config and the schema.yml tests downstream. Do NOT double-quote these.
select
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
    revision_date_utc_offset
from {{ source('ciena_pcc_raw', 'RAW_AS_RESPONSE_C') }}

-- Belt-and-suspenders: guarantee one row per assess_response_id (latest revision wins)
-- so the downstream incremental MERGE can never hit a nondeterministic-merge error,
-- even if the raw table somehow contains duplicates.
qualify row_number() over (
    partition by assess_response_id
    order by revision_date desc
) = 1
