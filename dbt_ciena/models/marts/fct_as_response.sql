{{ config(
    materialized='incremental',
    unique_key='assess_response_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

-- Bare (unquoted) identifiers: must match the unique_key ('assess_response_id') so the
-- incremental MERGE join resolves correctly. Do NOT double-quote these.
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
from {{ ref('stg_as_response') }}

{% if is_incremental() %}
-- Reprocess the whole extracted window each run (Matillion re-merged all of
-- as_response_c). The raw/stg table only holds the rolling lookback window anyway,
-- so this MERGEs every freshly pulled row -- including late-arriving edits whose
-- revision_date is older than the last 2 days -- on assess_response_id.
where revision_date >= dateadd(
    day, -{{ var('as_response_lookback_days', 15) }}, current_date()
)
{% endif %}
