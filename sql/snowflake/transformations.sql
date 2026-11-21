-- ============================================================================
-- SkyPoints Snowflake Staging Load & Country Split Transformation Logic
-- Implements:
-- 1. Quarantine malformed rows to RAW_DLQ_QUARANTINE
-- 2. Load staging from landing with exact calendar Age and Stale_Member (> 90 days)
-- 3. "Latest record wins" deduplication and country routing for ALL 5 countries
-- 4. Semi-structured JSON flattening & Loyalty 360 mart (COMPLETED miles only)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Quarantine Bad Landing Records into RAW_DLQ_QUARANTINE
-- Sanitizes empty strings '', 'NULL', and 'NONE' before checking constraints.
-- Validates mandatory fields, calendar dates, domain rules, and canonical country.
-- ----------------------------------------------------------------------------
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
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
)
SELECT
    COALESCE(s.MEMBER_ID, 'UNKNOWN_ID'),
    CONCAT_WS('|', s.RECORD_TYPE, s.SOURCE_SYSTEM, s.MEMBER_NAME, s.MEMBER_ID, s.ENROLLMENT_DATE, s.LAST_FLIGHT_DATE, s.TIER_CODE, s.AGENT_NAME, s.STATE, s.COUNTRY, s.DOB, s.IS_ACTIVE),
    CASE
        WHEN s.MEMBER_ID IS NULL THEN 'Missing mandatory Member_Id'
        WHEN s.MEMBER_NAME IS NULL THEN 'Missing mandatory Member_Name'
        WHEN s.ENROLLMENT_DATE IS NULL THEN 'Missing mandatory Enrollment_Date'
        WHEN TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD') IS NULL THEN 'Invalid calendar Enrollment_Date'
        WHEN s.LAST_FLIGHT_DATE IS NOT NULL AND TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD') IS NULL THEN 'Invalid Last_Flight_Date: unparseable calendar date'
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
    s.SOURCE_FILE_NAME,
    CURRENT_TIMESTAMP()
FROM SanitizedLanding s
LEFT JOIN REF_COUNTRY_MAP m ON s.COUNTRY = m.COUNTRY_CODE
WHERE s.RECORD_TYPE = 'D'
  AND (
      s.MEMBER_ID IS NULL
      OR s.MEMBER_NAME IS NULL
      OR s.ENROLLMENT_DATE IS NULL
      OR TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD') IS NULL
      OR (s.LAST_FLIGHT_DATE IS NOT NULL AND TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD') IS NULL)
      OR LENGTH(s.MEMBER_ID) > 18
      OR s.COUNTRY IS NULL
      OR LENGTH(s.COUNTRY) > 5
      OR m.CANONICAL_CODE IS NULL
      OR (s.STATE IS NOT NULL AND LENGTH(s.STATE) > 5)
      OR (s.POST_CODE IS NOT NULL AND (TRY_TO_NUMBER(s.POST_CODE) IS NULL OR LENGTH(s.POST_CODE) > 5))
      OR (s.TIER_CODE IS NOT NULL AND s.TIER_CODE NOT IN ('PLT', 'GLD', 'SLV'))
      OR (s.IS_ACTIVE IS NOT NULL AND s.IS_ACTIVE NOT IN ('A', 'I'))
  );


-- ----------------------------------------------------------------------------
-- STEP 2: Populate STG_MEMBER_PROFILE with Exact Calendar Age & Stale_Member
-- Stores canonical country code (e.g. AUS -> AU, INDIA -> IND)
-- ----------------------------------------------------------------------------
INSERT INTO STG_MEMBER_PROFILE (
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
        INGESTED_AT
    FROM RAW_MEMBER_LANDING
)
SELECT
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
    CURRENT_TIMESTAMP() AS INGESTED_AT
FROM SanitizedLanding s
JOIN REF_COUNTRY_MAP m ON s.COUNTRY = m.COUNTRY_CODE
WHERE s.RECORD_TYPE = 'D'
  AND s.MEMBER_ID IS NOT NULL
  AND s.MEMBER_NAME IS NOT NULL
  AND TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD') IS NOT NULL
  AND (s.LAST_FLIGHT_DATE IS NULL OR TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD') IS NOT NULL)
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
-- Detects (SOURCE_SYSTEM, MEMBER_ID) groups with differing names.
-- Keeps the row with the latest COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE),
-- quarantines older conflicting rows to RAW_DLQ_QUARANTINE, and removes them from staging.
-- ----------------------------------------------------------------------------
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
WITH MultiNameGroups AS (
    SELECT SOURCE_SYSTEM, MEMBER_ID
    FROM STG_MEMBER_PROFILE
    GROUP BY SOURCE_SYSTEM, MEMBER_ID
    HAVING COUNT(DISTINCT LOWER(TRIM(MEMBER_NAME))) > 1
),
RankedCollisions AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.SOURCE_SYSTEM, s.MEMBER_ID
            ORDER BY COALESCE(s.LAST_FLIGHT_DATE, s.ENROLLMENT_DATE) DESC, s.INGESTED_AT DESC
        ) AS RN
    FROM STG_MEMBER_PROFILE s
    JOIN MultiNameGroups g ON s.SOURCE_SYSTEM = g.SOURCE_SYSTEM AND s.MEMBER_ID = g.MEMBER_ID
)
SELECT
    MEMBER_ID,
    CONCAT_WS('|', SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE),
    'Key collision: superseded by newer record',
    'STG_MEMBER_PROFILE',
    CURRENT_TIMESTAMP()
FROM RankedCollisions
WHERE RN > 1;

DELETE FROM STG_MEMBER_PROFILE target
WHERE EXISTS (
    SELECT 1
    FROM (
        SELECT
            s.SOURCE_SYSTEM,
            s.MEMBER_ID,
            s.INGESTED_AT,
            ROW_NUMBER() OVER (
                PARTITION BY s.SOURCE_SYSTEM, s.MEMBER_ID
                ORDER BY COALESCE(s.LAST_FLIGHT_DATE, s.ENROLLMENT_DATE) DESC, s.INGESTED_AT DESC
            ) AS RN
        FROM STG_MEMBER_PROFILE s
        JOIN (
            SELECT SOURCE_SYSTEM, MEMBER_ID
            FROM STG_MEMBER_PROFILE
            GROUP BY SOURCE_SYSTEM, MEMBER_ID
            HAVING COUNT(DISTINCT LOWER(TRIM(MEMBER_NAME))) > 1
        ) g ON s.SOURCE_SYSTEM = g.SOURCE_SYSTEM AND s.MEMBER_ID = g.MEMBER_ID
    ) r
    WHERE target.SOURCE_SYSTEM = r.SOURCE_SYSTEM
      AND target.MEMBER_ID = r.MEMBER_ID
      AND target.INGESTED_AT = r.INGESTED_AT
      AND r.RN > 1
);


-- ----------------------------------------------------------------------------
-- STEP 3: "Latest Record Wins" Transformation & Full Per-Country Split
-- Covers all 5 target countries: IND, USA, CAN, PHIL, AU
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
        DOB,
        IS_ACTIVE,
        AGE,
        STALE_MEMBER,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, MEMBER_ID 
            ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE) DESC, INGESTED_AT DESC
        ) AS RN
    FROM STG_MEMBER_PROFILE
)
SELECT * EXCLUDE (RN)
FROM RankedMembers
WHERE RN = 1;

-- ----------------------------------------------------------------------------
-- STEP 3: Dynamic Country Routing & Migration via Snowflake Scripting
-- Note: This block uses Snowflake Scripting (EXECUTE IMMEDIATE/IDENTIFIER) and
-- is designed as a production runbook; it was not executed against a live Snowflake account.
-- ----------------------------------------------------------------------------

-- 1. Quarantine members with unmapped or NULL country (leave existing country rows untouched)
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
SELECT
    d.MEMBER_ID,
    CONCAT_WS('|', d.MEMBER_ID, d.MEMBER_NAME, COALESCE(d.COUNTRY, 'NULL_COUNTRY'), d.ENROLLMENT_DATE),
    'Unmapped or missing country',
    'STG_MEMBER_PROFILE',
    CURRENT_TIMESTAMP()
FROM DEDUPED_LATEST_MEMBERS d
LEFT JOIN REF_COUNTRY_MAP m ON UPPER(TRIM(d.COUNTRY)) = m.COUNTRY_CODE
WHERE d.COUNTRY IS NULL OR m.TARGET_TABLE IS NULL;

-- ----------------------------------------------------------------------------
-- STEP 3B: Static Country MERGE & Migration DELETE Routing
-- ----------------------------------------------------------------------------
-- NOTE: To eliminate Snowflake Scripting syntax risks and dynamic IDENTIFIER(:1) execution,
-- static MERGE and DELETE statements for all country tables are generated directly from
-- CANONICAL_TABLE_MAP in src/country_router.py into:
--   sql/snowflake/generated_country_merges.sql
--
-- Run the generated country merges file right after this step:
-- !include sql/snowflake/generated_country_merges.sql
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- STEP 4: Parse Semi-Structured JSON Feed & Flatten into Queryable Table
-- ----------------------------------------------------------------------------

-- 1. Quarantine malformed or negative transactions to RAW_DLQ_QUARANTINE
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
SELECT
    COALESCE(r.value:txn_id::STRING, 'UNKNOWN_TXN'),
    r.value::STRING,
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
        WHEN r.value:miles_redeemed::INTEGER < 0 THEN 'Invalid miles_redeemed: value cannot be negative'
        WHEN r.value:partner IS NULL OR NULLIF(TRIM(r.value:partner::STRING), '') IS NULL THEN 'Missing mandatory partner airline name'
        WHEN r.value:status IS NULL OR NULLIF(TRIM(r.value:status::STRING), '') IS NULL THEN 'Missing mandatory transaction status'
        ELSE 'Redemption validation failure'
    END AS ERROR_REASON,
    'RAW_REDEMPTIONS_LANDING',
    CURRENT_TIMESTAMP()
FROM RAW_REDEMPTIONS_LANDING raw,
LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r
WHERE raw.FEED_PAYLOAD:feed_date IS NULL
   OR NULLIF(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), '') IS NULL
   OR TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') IS NULL
   OR raw.FEED_PAYLOAD:member_id IS NULL OR NULLIF(TRIM(raw.FEED_PAYLOAD:member_id::STRING), '') IS NULL
   OR r.value:txn_id IS NULL OR NULLIF(TRIM(r.value:txn_id::STRING), '') IS NULL
   OR r.value:txn_date IS NULL OR NULLIF(TRIM(r.value:txn_date::STRING), '') IS NULL
   OR TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') IS NULL
   OR r.value:miles_redeemed IS NULL
   OR TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NULL
   OR r.value:miles_redeemed::INTEGER < 0
   OR r.value:partner IS NULL OR NULLIF(TRIM(r.value:partner::STRING), '') IS NULL
   OR r.value:status IS NULL OR NULLIF(TRIM(r.value:status::STRING), '') IS NULL;

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
        r.value:miles_redeemed::INTEGER AS MILES_REDEEMED,
        UPPER(TRIM(r.value:status::STRING)) AS STATUS
    FROM RAW_REDEMPTIONS_LANDING raw,
    LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r
    WHERE raw.FEED_PAYLOAD:feed_date IS NOT NULL
      AND TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') IS NOT NULL
      AND raw.FEED_PAYLOAD:member_id IS NOT NULL AND NULLIF(TRIM(raw.FEED_PAYLOAD:member_id::STRING), '') IS NOT NULL
      AND r.value:txn_id IS NOT NULL AND NULLIF(TRIM(r.value:txn_id::STRING), '') IS NOT NULL
      AND r.value:txn_date IS NOT NULL AND TRY_TO_DATE(TRIM(r.value:txn_date::STRING), 'YYYYMMDD') IS NOT NULL
      AND r.value:miles_redeemed IS NOT NULL AND TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NOT NULL AND r.value:miles_redeemed::INTEGER >= 0
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
        target.INGESTED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (TXN_ID, MEMBER_ID, FEED_DATE, TXN_DATE, PARTNER, MILES_REDEEMED, STATUS, INGESTED_AT)
    VALUES (src.TXN_ID, src.MEMBER_ID, src.FEED_DATE, src.TXN_DATE, src.PARTNER, src.MILES_REDEEMED, src.STATUS, CURRENT_TIMESTAMP());


-- ----------------------------------------------------------------------------
-- STEP 5: Loyalty 360 Analytical Mart (Sums Only COMPLETED Miles)
-- ----------------------------------------------------------------------------

-- 1. Quarantine ambiguous redemptions whose MEMBER_ID spans multiple SOURCE_SYSTEMs
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
SELECT
    r.TXN_ID,
    CONCAT_WS('|', r.TXN_ID, r.MEMBER_ID, r.FEED_DATE, r.TXN_DATE, r.PARTNER, r.MILES_REDEEMED, r.STATUS),
    'Ambiguous member_id across source systems',
    'STG_REDEMPTIONS_FLATTENED',
    CURRENT_TIMESTAMP()
FROM STG_REDEMPTIONS_FLATTENED r
WHERE r.MEMBER_ID IN (
    SELECT MEMBER_ID
    FROM DEDUPED_LATEST_MEMBERS
    GROUP BY MEMBER_ID
    HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
);

DELETE FROM STG_REDEMPTIONS_FLATTENED
WHERE MEMBER_ID IN (
    SELECT MEMBER_ID
    FROM DEDUPED_LATEST_MEMBERS
    GROUP BY MEMBER_ID
    HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
);

-- 2. Build or Refresh MEMBER_LOYALTY_360 Mart
CREATE OR REPLACE TABLE MEMBER_LOYALTY_360 AS
WITH RedemptionSummary AS (
    SELECT
        MEMBER_ID,
        COUNT(DISTINCT TXN_ID) AS TOTAL_REDEMPTIONS,
        -- Sum only COMPLETED miles in total redeemed miles
        COALESCE(SUM(CASE WHEN STATUS = 'COMPLETED' THEN MILES_REDEEMED ELSE 0 END), 0) AS TOTAL_MILES_REDEEMED,
        COALESCE(SUM(CASE WHEN STATUS = 'PENDING' THEN MILES_REDEEMED ELSE 0 END), 0) AS PENDING_MILES_REDEEMED,
        MAX(TXN_DATE) AS LAST_REDEMPTION_DATE,
        MODE(PARTNER) AS TOP_PARTNER
    FROM STG_REDEMPTIONS_FLATTENED
    GROUP BY MEMBER_ID
)
SELECT
    m.SOURCE_SYSTEM,
    m.MEMBER_ID,
    m.MEMBER_NAME,
    m.COUNTRY,
    m.TIER_CODE,
    m.ENROLLMENT_DATE,
    m.LAST_FLIGHT_DATE,
    m.AGE,
    m.STALE_MEMBER,
    COALESCE(r.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
    COALESCE(r.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
    COALESCE(r.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
    r.LAST_REDEMPTION_DATE,
    COALESCE(r.TOP_PARTNER, 'None') AS TOP_PARTNER,
    CURRENT_TIMESTAMP() AS LAST_SYNC_AT
FROM DEDUPED_LATEST_MEMBERS m
LEFT JOIN RedemptionSummary r ON m.MEMBER_ID = r.MEMBER_ID;
