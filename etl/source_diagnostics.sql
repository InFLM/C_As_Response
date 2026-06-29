/* ============================================================================
   source_diagnostics.sql
   Read-only checks against the Ciena source (dbo.as_response) to decide whether
   an efficient incremental watermark is possible, or whether we're stuck with a
   full resumable scan.

   Context (from diagnosing run_id=20260626_102749):
     * Table is clustered on (assess_id, question_key); assess_id is NOT ordered
       by time, so chunking by it = full-table I/O every run.
     * assess_response_id IS a monotonic identity, but ~11.9% of changes are
       in-place edits to OLD rows (revision_date moves, PK does not), so a
       max(assess_response_id) watermark would silently miss ~1 in 8 changes.
     * What we actually need: a column that is BOTH indexed AND ordered by
       *change* time. These queries look for one, and check if CDC/Change
       Tracking can give us that.

   All statements are READ-ONLY. Safe to run on the log-shipping standby.
   Run them one block at a time. SET NOCOUNT ON keeps output clean.
   ============================================================================ */
SET NOCOUNT ON;

/* ----------------------------------------------------------------------------
   1. Every index on dbo.as_response, with key columns in order.
      Confirms the clustered key and tells us whether assess_response_id (or
      revision_date) has its OWN usable index.
      WHAT TO LOOK FOR:
        * A row where index_type = 'CLUSTERED' -> its key_columns should be
          'assess_id, question_key' (confirms the chunking key).
        * Any index whose leading key_column is 'assess_response_id'
          -> chunking/watermarking by it could seek (still beware key lookups).
        * Any index whose leading key_column is 'revision_date'
          -> if this exists, the whole problem is solved: filter is seekable.
   ---------------------------------------------------------------------------- */
SELECT
    i.name                              AS index_name,
    i.type_desc                         AS index_type,
    i.is_unique,
    i.is_primary_key,
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        JOIN sys.columns c
          ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE ic.object_id = i.object_id
          AND ic.index_id  = i.index_id
          AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS key_columns,
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        JOIN sys.columns c
          ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE ic.object_id = i.object_id
          AND ic.index_id  = i.index_id
          AND ic.is_included_column = 1
        ORDER BY ic.index_column_id
        FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS included_columns
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID('dbo.as_response')
  AND i.type > 0                       -- skip the heap placeholder row
ORDER BY i.is_primary_key DESC, i.index_id;


/* ----------------------------------------------------------------------------
   2. Column-level facts: is assess_response_id an IDENTITY, and is there a
      rowversion/timestamp column (the textbook indexed+monotonic change marker)?
      WHAT TO LOOK FOR:
        * is_identity = 1 on assess_response_id -> confirms monotonic insert id.
        * Any row with data_type = 'timestamp' (a.k.a. rowversion). If one
          exists AND it's indexed (check it against query 1), that's your
          watermark for BOTH inserts and updates -- the ideal outcome.
   ---------------------------------------------------------------------------- */
SELECT
    c.name                AS column_name,
    t.name                AS data_type,
    c.is_identity,
    c.is_nullable,
    c.max_length
FROM sys.columns c
JOIN sys.types  t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.as_response')
ORDER BY c.column_id;


/* ----------------------------------------------------------------------------
   3. Is Change Data Capture (CDC) enabled on this database / table?
      CDC captures inserts AND updates AND deletes into indexed change tables,
      which is exactly what a correct incremental needs.
      WHAT TO LOOK FOR:
        * is_cdc_enabled = 1 on the database, and a row in the table list.
        * If is_cdc_enabled = 0, CDC is not on. It can only be enabled on the
          PRIMARY (read-write) server, not this standby -- so this tells you
          whether to ask the DBA / source team to turn it on upstream.
   ---------------------------------------------------------------------------- */
SELECT name AS database_name, is_cdc_enabled
FROM sys.databases
WHERE database_id = DB_ID();

-- CDC-tracked tables (returns rows only if CDC is enabled):
IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'change_tables' AND schema_id = SCHEMA_ID('cdc'))
    SELECT  source_schema  = OBJECT_SCHEMA_NAME(ct.source_object_id),
            source_table   = OBJECT_NAME(ct.source_object_id),
            capture_instance,
            ct.start_lsn
    FROM cdc.change_tables ct;
ELSE
    SELECT 'CDC not enabled on this database' AS cdc_status;


/* ----------------------------------------------------------------------------
   4. Is Change Tracking (CT) enabled? (Lighter-weight than CDC: it records
      WHICH rows changed since a version, by PK -- enough to drive an efficient
      pull when combined with the clustered key.)
      WHAT TO LOOK FOR:
        * A row in the first result -> CT is on for the database.
        * A row in the second result for as_response -> CT is on for the table.
        * Empty -> not enabled (again, must be enabled on the primary, not here).
   ---------------------------------------------------------------------------- */
SELECT  DB_NAME(database_id) AS database_name,
        is_auto_cleanup_on,
        retention_period,
        retention_period_units_desc
FROM sys.change_tracking_databases
WHERE database_id = DB_ID();

SELECT  OBJECT_SCHEMA_NAME(object_id) AS schema_name,
        OBJECT_NAME(object_id)        AS table_name,
        is_track_columns_updated_on
FROM sys.change_tracking_tables
WHERE object_id = OBJECT_ID('dbo.as_response');


/* ----------------------------------------------------------------------------
   5. Sanity-check the edit rate the standby itself reports (mirrors the 11.9%
      we measured in the landing files). Confirms how many recent changes are
      edits to OLD rows -- i.e. how much a max(assess_response_id) watermark
      would MISS. Bounded by assess_id so it stays a clustered range scan and
      doesn't trigger a full table scan just to answer the question.
      Adjust the assess_id range to a small slice you know has recent activity.
      WHAT TO LOOK FOR:
        * edited_old_rows / total_recent  near 0  -> table is ~append-only;
          a max-id watermark would actually be safe.
        * non-trivial ratio (we saw ~12%) -> watermark on id is unsafe; you
          need CDC/CT or a full scan.
   ---------------------------------------------------------------------------- */
SELECT
    COUNT(*)                                                   AS total_recent,
    SUM(CASE WHEN revision_date > DATEADD(day, 1, created_date)
             THEN 1 ELSE 0 END)                               AS edited_old_rows
FROM dbo.as_response WITH (NOLOCK)
WHERE assess_id >= 0 AND assess_id < 200000          -- <-- small clustered slice
  AND revision_date >= DATEADD(day, -15, CAST(GETDATE() AS date))
OPTION (RECOMPILE);
