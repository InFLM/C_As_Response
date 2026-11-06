{{ config(
    materialized='incremental',
    unique_key=['"assess_id"', '"question_key"'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    alias='as_response'
) }}

-- MERGE grain: the source is UNIQUE on (assess_id, question_key), and PCC assigns a NEW
-- assess_response_id when a response is revised. Keying the merge on assess_response_id
-- therefore leaves the OLD row behind on every revision -> duplicate rows per
-- (assess_id, question_key). We upsert on the natural key so a revision UPDATES the existing
-- row in place (and its assess_response_id changes to the new one), matching the source.
-- on_schema_change is 'append_new_columns' (not sync_all_columns) so a schema drift can never
-- silently DROP a column from the prod table.

-- Bare (unquoted) identifiers: must match the unique_key ('assess_response_id') so the
-- incremental MERGE join resolves correctly. Do NOT double-quote these.
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
from {{ ref('stg_as_response') }}

{% if is_incremental() %}
-- Reprocess the whole extracted window each run (Matillion re-merged all of
-- as_response_c). The raw/stg table only holds the rolling lookback window anyway,
-- so this MERGEs every freshly pulled row -- including late-arriving edits whose
-- revision_date is older than the last 2 days -- on assess_response_id.
where "revision_date" >= dateadd(
    day, -{{ var('as_response_lookback_days', 15) }}, current_date()
)
{% endif %}
