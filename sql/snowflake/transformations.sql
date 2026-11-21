-- ============================================================================
-- SkyPoints Snowflake Staging Load & Country Split Transformation Logic
-- Implements:
-- 0. Session Batch ID for Idempotent Multi-Run Tracking
-- 1. Quarantine malformed rows to RAW_DLQ_QUARANTINE (Idempotent MERGE)
-- 2. Load staging from landing with exact calendar Age, Stale_Member, and Batch ID
-- 3. "Latest record wins" deduplication and country routing for ALL 5 countries
-- 4. Semi-structured JSON flattening into STG_REDEMPTIONS_FLATTENED
-- NOTE: Step 5 (Analytical Mart Build) and Step 6 (Finalize Batch Tracking)
-- have been separated into sql/snowflake/03_mart_and_finalize.sql so country
-- routing merges complete before mart calculation.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 0: Initialize Session Batch ID for Idempotent Execution
-- ----------------------------------------------------------------------------
SET run_batch_id = UUID_STRING();

-- STG_MEMBER_PROFILE is per-batch; truncate before loading the incoming batch
TRUNCATE TABLE STG_MEMBER_PROFILE;

-- Claim unassigned landing rows for this batch run to protect against in-flight arrivals
-- Clean up any unfinalized DLQ records from prior interrupted runs so the retry starts cleanly
DELETE FROM RAW_DLQ_QUARANTINE
WHERE BATCH_ID IN (
    SELECT DISTINCT BATCH_ID FROM RAW_MEMBER_LANDING WHERE PROCESSED_AT IS NULL AND BATCH_ID IS NOT NULL AND BATCH_ID <> $run_batch_id
    UNION
    SELECT DISTINCT BATCH_ID FROM RAW_REDEMPTIONS_LANDING WHERE PROCESSED_AT IS NULL AND BATCH_ID IS NOT NULL AND BATCH_ID <> $run_batch_id
);

-- Re-claim all unfinalized landing rows (protects against in-flight arrivals and resumes interrupted runs)
UPDATE RAW_MEMBER_LANDING
SET BATCH_ID = $run_batch_id
WHERE PROCESSED_AT IS NULL;

UPDATE RAW_REDEMPTIONS_LANDING
SET BATCH_ID = $run_batch_id
WHERE PROCESSED_AT IS NULL;

-- ----------------------------------------------------------------------------
-- STEP 1: Quarantine Bad Landing Records into RAW_DLQ_QUARANTINE
-- Sanitizes empty strings '', 'NULL', and 'NONE' before checking constraints.
-- Validates mandatory fields, calendar dates, domain rules, and canonical country.
-- Plain INSERT with BATCH_ID and RAW_RECORD_HASH; landing PROCESSED_AT guarantees idempotency.
-- Guarded with NOT EXISTS on (BATCH_ID, RAW_RECORD_HASH) to prevent duplicates on mid-run retries.
-- ----------------------------------------------------------------------------
INSERT INTO RAW_DLQ_QUARANTINE (
    RECORD_ID,
    RAW_RECORD,
    RAW_RECORD_HASH,
    ERROR_REASON,
    SOURCE_FEED,
    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    BATCH_ID,
    QUARANTINED_AT
)
WITH SanitizedLanding AS (
    SELECT
        RECORD_TYPE,
        COALESCE(CASE WHEN NULLIF(TRIM(SOURCE_SYSTEM), '') IS NULL OR UPPER(TRIM(SOURCE_SYSTEM)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(SOURCE_SYSTEM) END, 'FLAT_FILE') AS SOURCE_SYSTEM,
        CASE WHEN NULLIF(TRIM(MEMBER_ID), '') IS NULL OR UPPER(TRIM(MEMBER_ID)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(MEMBER_ID) END AS MEMBER_ID,
        CASE WHEN NULLIF(TRIM(MEMBER_NAME), '') IS NULL OR UPPER(TRIM(MEMBER_NAME)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(MEMBER_NAME) END AS MEMBER_NAME,
        CASE WHEN NULLIF(TRIM(ENROLLMENT_DATE), '') IS NULL OR UPPER(TRIM(ENROLLMENT_DATE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(ENROLLMENT_DATE) END AS ENROLLMENT_DATE,
        CASE WHEN NULLIF(TRIM(LAST_FLIGHT_DATE), '') IS NULL OR UPPER(TRIM(LAST_FLIGHT_DATE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(LAST_FLIGHT_DATE) END AS LAST_FLIGHT_DATE,
        CASE WHEN NULLIF(TRIM(TIER_CODE), '') IS NULL OR UPPER(TRIM(TIER_CODE)) IN ('NULL', 'NONE') THEN NULL ELSE UPPER(TRIM(TIER_CODE)) END AS TIER_CODE,
        CASE WHEN NULLIF(TRIM(AGENT_NAME), '') IS NULL OR UPPER(TRIM(AGENT_NAME)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(AGENT_NAME) END AS AGENT_NAME,
        CASE WHEN NULLIF(TRIM(STATE), '') IS NULL OR UPPER(TRIM(STATE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(STATE) END AS STATE,
        CASE WHEN NULLIF(TRIM(COUNTRY), '') IS NULL OR UPPER(TRIM(COUNTRY)) IN ('NULL', 'NONE') THEN NULL ELSE UPPER(TRIM(COUNTRY)) END AS COUNTRY,
        CASE WHEN NULLIF(TRIM(POST_CODE), '') IS NULL OR UPPER(TRIM(POST_CODE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(POST_CODE) END AS POST_CODE,
        CASE WHEN NULLIF(TRIM(DOB), '') IS NULL OR UPPER(TRIM(DOB)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(DOB) END AS DOB,
        CASE WHEN NULLIF(TRIM(IS_ACTIVE), '') IS NULL OR UPPER(TRIM(IS_ACTIVE)) IN ('NULL', 'NONE') THEN NULL ELSE UPPER(TRIM(IS_ACTIVE)) END AS IS_ACTIVE,
        SOURCE_FILE_NAME,
        INGESTED_AT
    FROM RAW_MEMBER_LANDING
    WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL
)
SELECT
    COALESCE(s.MEMBER_ID, 'UNKNOWN_ID') AS RECORD_ID,
    CONCAT_WS('|', s.RECORD_TYPE, s.SOURCE_SYSTEM, s.MEMBER_NAME, s.MEMBER_ID, s.ENROLLMENT_DATE, s.LAST_FLIGHT_DATE, s.TIER_CODE, s.AGENT_NAME, s.STATE, s.COUNTRY, s.DOB, s.IS_ACTIVE) AS RAW_RECORD,
    SHA2(CONCAT_WS('|', s.RECORD_TYPE, s.SOURCE_SYSTEM, s.MEMBER_NAME, s.MEMBER_ID, s.ENROLLMENT_DATE, s.LAST_FLIGHT_DATE, s.TIER_CODE, s.AGENT_NAME, s.STATE, s.COUNTRY, s.DOB, s.IS_ACTIVE), 256) AS RAW_RECORD_HASH,
    CASE
        WHEN s.MEMBER_ID IS NULL THEN 'Missing mandatory Member_Id'
        WHEN s.MEMBER_NAME IS NULL THEN 'Missing mandatory Member_Name'
        WHEN s.ENROLLMENT_DATE IS NULL THEN 'Missing mandatory Enrollment_Date'
        WHEN TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD') IS NULL THEN 'Invalid calendar Enrollment_Date'
        WHEN s.LAST_FLIGHT_DATE IS NOT NULL AND TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD') IS NULL THEN 'Invalid Last_Flight_Date: unparseable calendar date'
        WHEN s.DOB IS NOT NULL AND TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY') IS NULL THEN 'Invalid DOB: unparseable calendar date'
        WHEN LENGTH(s.MEMBER_ID) > 18 THEN 'Member_Id exceeds 18 characters'
        WHEN s.COUNTRY IS NULL THEN 'Unmapped country: Country is required and cannot be empty'
        WHEN LENGTH(s.COUNTRY) > 5 THEN 'Country exceeds 5 characters'
        WHEN m.CANONICAL_CODE IS NULL THEN 'Unmapped country'
        WHEN s.STATE IS NOT NULL AND LENGTH(s.STATE) > 5 THEN 'State exceeds 5 characters'
        WHEN s.POST_CODE IS NOT NULL AND (TRY_TO_NUMBER(s.POST_CODE) IS NULL OR LENGTH(s.POST_CODE) > 5) THEN 'Invalid Post_Code: must be numeric up to 5 digits'
        WHEN s.TIER_CODE IS NOT NULL AND s.TIER_CODE NOT IN ('PLT', 'GLD', 'SLV') THEN 'Invalid domain value: Tier_Code not in (PLT, GLD, SLV)'
        WHEN s.IS_ACTIVE IS NOT NULL AND s.IS_ACTIVE NOT IN ('A', 'I') THEN 'Invalid domain value: Is_Active must be in (A, I)'
        ELSE 'Validation constraint failure'
    END AS ERROR_REASON,
    s.SOURCE_FILE_NAME AS SOURCE_FEED,
    s.SOURCE_SYSTEM AS SOURCE_SYSTEM,
    s.SOURCE_FILE_NAME AS SOURCE_FILE_NAME,
    $run_batch_id AS BATCH_ID,
    CURRENT_TIMESTAMP() AS QUARANTINED_AT
FROM SanitizedLanding s
LEFT JOIN REF_COUNTRY_MAP m ON s.COUNTRY = m.COUNTRY_CODE
WHERE s.RECORD_TYPE = 'D'
  AND (
      s.MEMBER_ID IS NULL
      OR s.MEMBER_NAME IS NULL
      OR s.ENROLLMENT_DATE IS NULL
      OR TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD') IS NULL
      OR (s.LAST_FLIGHT_DATE IS NOT NULL AND TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD') IS NULL)
      OR (s.DOB IS NOT NULL AND TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY') IS NULL)
      OR LENGTH(s.MEMBER_ID) > 18
      OR s.COUNTRY IS NULL
      OR LENGTH(s.COUNTRY) > 5
      OR m.CANONICAL_CODE IS NULL
      OR (s.STATE IS NOT NULL AND LENGTH(s.STATE) > 5)
      OR (s.POST_CODE IS NOT NULL AND (TRY_TO_NUMBER(s.POST_CODE) IS NULL OR LENGTH(s.POST_CODE) > 5))
      OR (s.TIER_CODE IS NOT NULL AND s.TIER_CODE NOT IN ('PLT', 'GLD', 'SLV'))
      OR (s.IS_ACTIVE IS NOT NULL AND s.IS_ACTIVE NOT IN ('A', 'I'))
  )
  AND NOT EXISTS (
      SELECT 1 FROM RAW_DLQ_QUARANTINE q
      WHERE q.BATCH_ID = $run_batch_id
        AND q.RAW_RECORD_HASH = SHA2(CONCAT_WS('|', s.RECORD_TYPE, s.SOURCE_SYSTEM, s.MEMBER_NAME, s.MEMBER_ID, s.ENROLLMENT_DATE, s.LAST_FLIGHT_DATE, s.TIER_CODE, s.AGENT_NAME, s.STATE, s.COUNTRY, s.DOB, s.IS_ACTIVE), 256)
  );


-- ----------------------------------------------------------------------------
-- STEP 2: Populate STG_MEMBER_PROFILE with Exact Calendar Age & Stale_Member
-- Stores canonical country code (e.g. AUS -> AU, INDIA -> IND) and batch metadata
-- ----------------------------------------------------------------------------
INSERT INTO STG_MEMBER_PROFILE (
    STG_ROW_ID,
    BATCH_ID,
    SOURCE_SYSTEM,
    MEMBER_ID,
    MEMBER_NAME,
    ENROLLMENT_DATE,
    LAST_FLIGHT_DATE,
    TIER_CODE,
    AGENT_NAME,
    STATE,
    COUNTRY,
    POST_CODE,
    DOB,
    IS_ACTIVE,
    AGE,
    STALE_MEMBER,
    SOURCE_FILE_NAME,
    EXTRACT_DATE,
    LINE_NO,
    INGESTED_AT
)
WITH SanitizedLanding AS (
    SELECT
        RECORD_TYPE,
        COALESCE(CASE WHEN NULLIF(TRIM(SOURCE_SYSTEM), '') IS NULL OR UPPER(TRIM(SOURCE_SYSTEM)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(SOURCE_SYSTEM) END, 'FLAT_FILE') AS SOURCE_SYSTEM,
        CASE WHEN NULLIF(TRIM(MEMBER_ID), '') IS NULL OR UPPER(TRIM(MEMBER_ID)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(MEMBER_ID) END AS MEMBER_ID,
        CASE WHEN NULLIF(TRIM(MEMBER_NAME), '') IS NULL OR UPPER(TRIM(MEMBER_NAME)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(MEMBER_NAME) END AS MEMBER_NAME,
        CASE WHEN NULLIF(TRIM(ENROLLMENT_DATE), '') IS NULL OR UPPER(TRIM(ENROLLMENT_DATE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(ENROLLMENT_DATE) END AS ENROLLMENT_DATE,
        CASE WHEN NULLIF(TRIM(LAST_FLIGHT_DATE), '') IS NULL OR UPPER(TRIM(LAST_FLIGHT_DATE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(LAST_FLIGHT_DATE) END AS LAST_FLIGHT_DATE,
        CASE WHEN NULLIF(TRIM(TIER_CODE), '') IS NULL OR UPPER(TRIM(TIER_CODE)) IN ('NULL', 'NONE') THEN NULL ELSE UPPER(TRIM(TIER_CODE)) END AS TIER_CODE,
        CASE WHEN NULLIF(TRIM(AGENT_NAME), '') IS NULL OR UPPER(TRIM(AGENT_NAME)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(AGENT_NAME) END AS AGENT_NAME,
        CASE WHEN NULLIF(TRIM(STATE), '') IS NULL OR UPPER(TRIM(STATE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(STATE) END AS STATE,
        CASE WHEN NULLIF(TRIM(COUNTRY), '') IS NULL OR UPPER(TRIM(COUNTRY)) IN ('NULL', 'NONE') THEN NULL ELSE UPPER(TRIM(COUNTRY)) END AS COUNTRY,
        CASE WHEN NULLIF(TRIM(POST_CODE), '') IS NULL OR UPPER(TRIM(POST_CODE)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(POST_CODE) END AS POST_CODE,
        CASE WHEN NULLIF(TRIM(DOB), '') IS NULL OR UPPER(TRIM(DOB)) IN ('NULL', 'NONE') THEN NULL ELSE TRIM(DOB) END AS DOB,
        CASE WHEN NULLIF(TRIM(IS_ACTIVE), '') IS NULL OR UPPER(TRIM(IS_ACTIVE)) IN ('NULL', 'NONE') THEN NULL ELSE UPPER(TRIM(IS_ACTIVE)) END AS IS_ACTIVE,
        SOURCE_FILE_NAME,
        COALESCE(EXTRACT_DATE, TRY_TO_DATE(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}'), 'YYYYMMDD'), INGESTED_AT::DATE, CURRENT_DATE()) AS EXTRACT_DATE,
        LINE_NO,
        INGESTED_AT
    FROM RAW_MEMBER_LANDING
    WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL
)
SELECT
    UUID_STRING() AS STG_ROW_ID,
    $run_batch_id AS BATCH_ID,
    s.SOURCE_SYSTEM,
    s.MEMBER_ID,
    s.MEMBER_NAME,
    TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD') AS ENROLLMENT_DATE,
    TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD') AS LAST_FLIGHT_DATE,
    s.TIER_CODE,
    s.AGENT_NAME,
    s.STATE,
    m.CANONICAL_CODE AS COUNTRY,
    TRY_TO_NUMBER(s.POST_CODE) AS POST_CODE,
    TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY') AS DOB,
    COALESCE(s.IS_ACTIVE, 'A') AS IS_ACTIVE,
    -- Derived 1: Exact Calendar Age (avoids /365.25 off-by-one errors)
    CASE
        WHEN TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY') IS NULL THEN NULL
        ELSE DATEDIFF(year, TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY'), CURRENT_DATE()) - 
             CASE 
                WHEN (MONTH(CURRENT_DATE()) < MONTH(TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY'))) 
                  OR (MONTH(CURRENT_DATE()) = MONTH(TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY')) 
                      AND DAY(CURRENT_DATE()) < DAY(TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY'))) 
                THEN 1 ELSE 0 
             END
    END AS AGE,
    -- Derived 2: Stale_Member flag where days since Flight_Date > 90
    CASE 
        WHEN s.LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD'), CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    s.SOURCE_FILE_NAME,
    s.EXTRACT_DATE,
    s.LINE_NO,
    CURRENT_TIMESTAMP() AS INGESTED_AT
FROM SanitizedLanding s
JOIN REF_COUNTRY_MAP m ON s.COUNTRY = m.COUNTRY_CODE
WHERE s.RECORD_TYPE = 'D'
  AND s.MEMBER_ID IS NOT NULL
  AND s.MEMBER_NAME IS NOT NULL
  AND TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD') IS NOT NULL
  AND (s.LAST_FLIGHT_DATE IS NULL OR TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD') IS NOT NULL)
  AND (s.DOB IS NULL OR TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY') IS NOT NULL)
  AND LENGTH(s.MEMBER_ID) <= 18
  AND s.COUNTRY IS NOT NULL
  AND LENGTH(s.COUNTRY) <= 5
  AND m.CANONICAL_CODE IS NOT NULL
  AND (s.STATE IS NULL OR LENGTH(s.STATE) <= 5)
  AND (s.POST_CODE IS NULL OR (TRY_TO_NUMBER(s.POST_CODE) IS NOT NULL AND LENGTH(s.POST_CODE) <= 5))
  AND (s.TIER_CODE IS NULL OR s.TIER_CODE IN ('PLT', 'GLD', 'SLV'))
  AND (s.IS_ACTIVE IS NULL OR s.IS_ACTIVE IN ('A', 'I'));


-- ----------------------------------------------------------------------------
-- STEP 2B: Collision Resolution (Renames & Key Collisions)
-- Detects (SOURCE_SYSTEM, MEMBER_ID) groups with differing names INSIDE the
-- current batch (BATCH_ID = $run_batch_id). A rename across distinct batches
-- is treated as a normal update without DLQ rejection.
-- Identifies the row with the latest EXTRACT_DATE DESC, then
-- COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE) DESC, tie-breaking on INGESTED_AT DESC,
-- LINE_NO DESC, and idempotently quarantines older conflicting rows to RAW_DLQ_QUARANTINE.
-- NOTE: We do not run a DELETE on STG_MEMBER_PROFILE here because:
--   1) Matching on (SOURCE_SYSTEM, MEMBER_ID, INGESTED_AT) deletes BOTH the winner
--      and loser since Snowflake's CURRENT_TIMESTAMP() is identical within a batch.
--   2) DEDUPED_LATEST_MEMBERS in Step 3 already filters by RN = 1, ensuring only the
--      latest winning record propagates forward into the country tables.
-- ----------------------------------------------------------------------------
MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    WITH CurrentBatchMembers AS (
        SELECT *
        FROM STG_MEMBER_PROFILE
        WHERE BATCH_ID = $run_batch_id
    ),
    MultiNameGroups AS (
        SELECT SOURCE_SYSTEM, MEMBER_ID
        FROM CurrentBatchMembers
        GROUP BY SOURCE_SYSTEM, MEMBER_ID
        HAVING COUNT(DISTINCT LOWER(TRIM(MEMBER_NAME))) > 1
    ),
    RankedCollisions AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.SOURCE_SYSTEM, s.MEMBER_ID
                ORDER BY s.EXTRACT_DATE DESC,
                         COALESCE(s.LAST_FLIGHT_DATE, s.ENROLLMENT_DATE) DESC,
                         COALESCE(s.SOURCE_FILE_NAME, '') DESC,
                         COALESCE(s.LINE_NO, 0) DESC
            ) AS RN
        FROM CurrentBatchMembers s
        JOIN MultiNameGroups g ON s.SOURCE_SYSTEM = g.SOURCE_SYSTEM AND s.MEMBER_ID = g.MEMBER_ID
    )
    SELECT
        MEMBER_ID AS RECORD_ID,
        CONCAT_WS('|', SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE), 256) AS RAW_RECORD_HASH,
        'Key collision: superseded by newer record' AS ERROR_REASON,
        'STG_MEMBER_PROFILE' AS SOURCE_FEED,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM RankedCollisions
    WHERE RN > 1
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
AND target.RAW_RECORD_HASH = src.RAW_RECORD_HASH
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, RAW_RECORD_HASH, ERROR_REASON, SOURCE_FEED, SOURCE_SYSTEM, SOURCE_FILE_NAME, BATCH_ID, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.RAW_RECORD_HASH, src.ERROR_REASON, src.SOURCE_FEED, src.SOURCE_SYSTEM, src.SOURCE_FILE_NAME, src.BATCH_ID, CURRENT_TIMESTAMP());


-- ----------------------------------------------------------------------------
-- STEP 3: "Latest Record Wins" Transformation & Full Per-Country Split
-- Covers all 5 target countries: IND, USA, CAN, PHIL, AU
-- Prioritizes EXTRACT_DATE DESC, then COALESCE(flight, enrollment) DESC,
-- tie-breaking with LINE_NO DESC for deterministic resolution.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE DEDUPED_LATEST_MEMBERS AS
WITH RankedMembers AS (
    SELECT
        SOURCE_SYSTEM,
        MEMBER_ID,
        MEMBER_NAME,
        ENROLLMENT_DATE,
        LAST_FLIGHT_DATE,
        TIER_CODE,
        AGENT_NAME,
        STATE,
        COUNTRY,
        POST_CODE,
        DOB,
        IS_ACTIVE,
        AGE,
        STALE_MEMBER,
        SOURCE_FILE_NAME,
        EXTRACT_DATE,
        LINE_NO,
        INGESTED_AT,
        STG_ROW_ID,
        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, MEMBER_ID 
            ORDER BY EXTRACT_DATE DESC,
                     COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE) DESC,
                     COALESCE(SOURCE_FILE_NAME, '') DESC,
                     COALESCE(LINE_NO, 0) DESC
        ) AS RN
    FROM STG_MEMBER_PROFILE
    WHERE BATCH_ID = $run_batch_id
)
SELECT
    SOURCE_SYSTEM,
    MEMBER_ID,
    MEMBER_NAME,
    ENROLLMENT_DATE,
    LAST_FLIGHT_DATE,
    TIER_CODE,
    AGENT_NAME,
    STATE,
    COUNTRY,
    POST_CODE,
    DOB,
    IS_ACTIVE,
    AGE,
    STALE_MEMBER,
    SOURCE_FILE_NAME,
    EXTRACT_DATE,
    LINE_NO,
    INGESTED_AT,
    STG_ROW_ID
FROM RankedMembers
WHERE RN = 1;

-- ----------------------------------------------------------------------------
-- STEP 3B: Country-Specific Routing & Migration via Static MERGEs
-- ----------------------------------------------------------------------------
-- NOTE: To eliminate Snowflake Scripting syntax risks and dynamic IDENTIFIER(:1) execution,
-- static MERGE and DELETE statements for all country tables are generated directly from
-- CANONICAL_TABLE_MAP in src/country_router.py into:
--   sql/snowflake/generated_country_merges.sql
--
-- In SnowSQL CLI, execute:
-- !source sql/snowflake/generated_country_merges.sql
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- STEP 3C: Audit Out-of-Order / Superseded Records into RAW_DLQ_QUARANTINE
-- Captures late-arriving older records that are blocked from overwriting newer core records
-- ----------------------------------------------------------------------------
MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        src.MEMBER_ID AS RECORD_ID,
        CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_DATE, src.LAST_FLIGHT_DATE, src.LINE_NO) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_DATE, src.LAST_FLIGHT_DATE, src.LINE_NO), 256) AS RAW_RECORD_HASH,
        'Superseded by existing newer record in core table' AS ERROR_REASON,
        'DEDUPED_LATEST_MEMBERS' AS SOURCE_FEED,
        src.SOURCE_SYSTEM,
        src.SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM DEDUPED_LATEST_MEMBERS src
    JOIN MEMBERS_ALL m ON src.SOURCE_SYSTEM = m.SOURCE_SYSTEM AND src.MEMBER_ID = m.MEMBER_ID
    WHERE (
        m.EXTRACT_DATE > src.EXTRACT_DATE
        OR (
            m.EXTRACT_DATE = src.EXTRACT_DATE
            AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
        )
        OR (
            m.EXTRACT_DATE = src.EXTRACT_DATE
            AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) = COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
            AND (
                COALESCE(m.SOURCE_FILE_NAME, '') > COALESCE(src.SOURCE_FILE_NAME, '')
                OR (
                    COALESCE(m.SOURCE_FILE_NAME, '') = COALESCE(src.SOURCE_FILE_NAME, '')
                    AND COALESCE(m.LINE_NO, 0) > COALESCE(src.LINE_NO, 0)
                )
            )
        )
    )
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
AND target.RAW_RECORD_HASH = src.RAW_RECORD_HASH
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, RAW_RECORD_HASH, ERROR_REASON, SOURCE_FEED, SOURCE_SYSTEM, SOURCE_FILE_NAME, BATCH_ID, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.RAW_RECORD_HASH, src.ERROR_REASON, src.SOURCE_FEED, src.SOURCE_SYSTEM, src.SOURCE_FILE_NAME, src.BATCH_ID, CURRENT_TIMESTAMP());


-- ----------------------------------------------------------------------------
-- STEP 4: Parse Semi-Structured JSON Feed & Flatten into Queryable Table
-- ----------------------------------------------------------------------------

-- 1. Quarantine malformed or negative transactions to RAW_DLQ_QUARANTINE
-- Plain INSERT with BATCH_ID and RAW_RECORD_HASH; landing PROCESSED_AT ensures idempotency.
INSERT INTO RAW_DLQ_QUARANTINE (
    RECORD_ID,
    RAW_RECORD,
    RAW_RECORD_HASH,
    ERROR_REASON,
    SOURCE_FEED,
    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    BATCH_ID,
    QUARANTINED_AT
)
SELECT
    COALESCE(r.value:txn_id::STRING, 'UNKNOWN_TXN') AS RECORD_ID,
    r.value::STRING AS RAW_RECORD,
    SHA2(r.value::STRING, 256) AS RAW_RECORD_HASH,
    CASE
        WHEN raw.FEED_PAYLOAD:feed_date IS NULL
             OR NULLIF(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), '') IS NULL
             OR TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') IS NULL
        THEN 'Invalid feed_date in redemption feed'
        WHEN raw.FEED_PAYLOAD:member_id IS NULL OR NULLIF(TRIM(raw.FEED_PAYLOAD:member_id::STRING), '') IS NULL THEN 'Missing mandatory member_id in redemption feed'
        WHEN r.value:txn_id IS NULL OR NULLIF(TRIM(r.value:txn_id::STRING), '') IS NULL THEN 'Missing mandatory transaction txn_id'
        WHEN r.value:txn_date IS NULL OR NULLIF(TRIM(r.value:txn_date::STRING), '') IS NULL THEN 'Missing mandatory transaction txn_date'
        WHEN TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') IS NULL THEN 'Invalid txn_date: unparseable calendar date'
        WHEN r.value:miles_redeemed IS NULL THEN 'Missing mandatory miles_redeemed'
        WHEN TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NULL THEN 'Invalid miles_redeemed data type: non-numeric'
        WHEN TRY_TO_NUMBER(r.value:miles_redeemed::STRING) < 0 THEN 'Invalid miles_redeemed: value cannot be negative'
        WHEN r.value:partner IS NULL OR NULLIF(TRIM(r.value:partner::STRING), '') IS NULL THEN 'Missing mandatory partner airline name'
        WHEN r.value:status IS NULL OR NULLIF(TRIM(r.value:status::STRING), '') IS NULL THEN 'Missing mandatory transaction status'
        ELSE 'Redemption validation failure'
    END AS ERROR_REASON,
    'RAW_REDEMPTIONS_LANDING' AS SOURCE_FEED,
    'PARTNER_JSON' AS SOURCE_SYSTEM,
    COALESCE(raw.FEED_DATE, 'redemptions_feed') AS SOURCE_FILE_NAME,
    $run_batch_id AS BATCH_ID,
    CURRENT_TIMESTAMP() AS QUARANTINED_AT
FROM RAW_REDEMPTIONS_LANDING raw,
LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r
WHERE raw.BATCH_ID = $run_batch_id AND raw.PROCESSED_AT IS NULL
  AND (
      raw.FEED_PAYLOAD:feed_date IS NULL
      OR NULLIF(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), '') IS NULL
      OR TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') IS NULL
      OR raw.FEED_PAYLOAD:member_id IS NULL OR NULLIF(TRIM(raw.FEED_PAYLOAD:member_id::STRING), '') IS NULL
      OR r.value:txn_id IS NULL OR NULLIF(TRIM(r.value:txn_id::STRING), '') IS NULL
      OR r.value:txn_date IS NULL OR NULLIF(TRIM(r.value:txn_date::STRING), '') IS NULL
      OR TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') IS NULL
      OR r.value:miles_redeemed IS NULL
      OR TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NULL
      OR TRY_TO_NUMBER(r.value:miles_redeemed::STRING) < 0
      OR r.value:partner IS NULL OR NULLIF(TRIM(r.value:partner::STRING), '') IS NULL
      OR r.value:status IS NULL OR NULLIF(TRIM(r.value:status::STRING), '') IS NULL
  )
  AND NOT EXISTS (
      SELECT 1 FROM RAW_DLQ_QUARANTINE q
      WHERE q.BATCH_ID = $run_batch_id
        AND q.RAW_RECORD_HASH = SHA2(r.value::STRING, 256)
  );

-- 2. Idempotent MERGE into STG_REDEMPTIONS_FLATTENED on TXN_ID
-- Note: Snowflake does not enforce PRIMARY KEY constraints; MERGE is essential for idempotency across reruns.
MERGE INTO STG_REDEMPTIONS_FLATTENED target
USING (
    SELECT
        r.value:txn_id::STRING AS TXN_ID,
        TRIM(raw.FEED_PAYLOAD:member_id::STRING) AS MEMBER_ID,
        TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') AS FEED_DATE,
        TRY_TO_DATE(TRIM(r.value:txn_date::STRING), 'YYYYMMDD') AS TXN_DATE,
        TRIM(r.value:partner::STRING) AS PARTNER,
        TRY_TO_NUMBER(r.value:miles_redeemed::STRING) AS MILES_REDEEMED,
        UPPER(TRIM(r.value:status::STRING)) AS STATUS,
        $run_batch_id AS BATCH_ID
    FROM RAW_REDEMPTIONS_LANDING raw,
    LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r
    WHERE raw.BATCH_ID = $run_batch_id AND raw.PROCESSED_AT IS NULL
      AND raw.FEED_PAYLOAD:feed_date IS NOT NULL
      AND TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') IS NOT NULL
      AND raw.FEED_PAYLOAD:member_id IS NOT NULL AND NULLIF(TRIM(raw.FEED_PAYLOAD:member_id::STRING), '') IS NOT NULL
      AND r.value:txn_id IS NOT NULL AND NULLIF(TRIM(r.value:txn_id::STRING), '') IS NOT NULL
      AND r.value:txn_date IS NOT NULL AND TRY_TO_DATE(TRIM(r.value:txn_date::STRING), 'YYYYMMDD') IS NOT NULL
      AND r.value:miles_redeemed IS NOT NULL AND TRY_TO_NUMBER(r.value:miles_redeemed::STRING) >= 0
      AND r.value:partner IS NOT NULL AND NULLIF(TRIM(r.value:partner::STRING), '') IS NOT NULL
      AND r.value:status IS NOT NULL AND NULLIF(TRIM(r.value:status::STRING), '') IS NOT NULL
) src
ON target.TXN_ID = src.TXN_ID
WHEN MATCHED THEN
    UPDATE SET
        target.MEMBER_ID = src.MEMBER_ID,
        target.FEED_DATE = src.FEED_DATE,
        target.TXN_DATE = src.TXN_DATE,
        target.PARTNER = src.PARTNER,
        target.MILES_REDEEMED = src.MILES_REDEEMED,
        target.STATUS = src.STATUS,
        target.BATCH_ID = src.BATCH_ID,
        target.INGESTED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (TXN_ID, MEMBER_ID, FEED_DATE, TXN_DATE, PARTNER, MILES_REDEEMED, STATUS, BATCH_ID, INGESTED_AT)
    VALUES (src.TXN_ID, src.MEMBER_ID, src.FEED_DATE, src.TXN_DATE, src.PARTNER, src.MILES_REDEEMED, src.STATUS, src.BATCH_ID, CURRENT_TIMESTAMP());

