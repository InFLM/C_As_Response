-- Bare (unquoted) identifiers throughout: Snowflake folds them to UPPERCASE, which
-- matches the raw table's UPPERCASE columns and keeps the column names consistent with
-- the unique_key config and the schema.yml tests downstream. Do NOT double-quote these.
select
    "assess_response_id",
    "assess_id",
    "question_key",
    "item_value",
    "acknowledged",
    "created_date",
    "position_desc",
    "revision_date",
    "revision_by",
    "long_username",
    "revision_date_utc_offset"
from {{ source('ciena_pcc_raw', 'as_response_c') }}

-- Collapse to ONE row per natural key (assess_id, question_key), latest revision wins, to
-- match the source's unique constraint and feed the natural-key MERGE. `nulls last` so a
-- NULL revision_date never beats a real one (Snowflake orders NULLs FIRST on DESC by default).
qualify row_number() over (
    partition by "assess_id", "question_key"
    order by "revision_date" desc nulls last
) = 1
