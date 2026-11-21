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
-- ----------------------------------------------------------------------------
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
SELECT
    COALESCE(MEMBER_ID, 'UNKNOWN_ID'),
    CONCAT_WS('|', RECORD_TYPE, MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE),
    CASE
        WHEN TRIM(MEMBER_ID) IS NULL OR TRIM(MEMBER_ID) = '' THEN 'Missing mandatory Member_Id'
        WHEN TRIM(MEMBER_NAME) IS NULL OR TRIM(MEMBER_NAME) = '' THEN 'Missing mandatory Member_Name'
        WHEN TRIM(ENROLLMENT_DATE) IS NULL OR TRIM(ENROLLMENT_DATE) = '' THEN 'Missing mandatory Enrollment_Date'
        WHEN TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYYMMDD') IS NULL THEN 'Invalid calendar Enrollment_Date'
        WHEN TRIM(LAST_FLIGHT_DATE) IS NOT NULL AND TRIM(LAST_FLIGHT_DATE) <> '' AND TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD') IS NULL THEN 'Invalid Last_Flight_Date: unparseable calendar date'
        WHEN LENGTH(TRIM(MEMBER_ID)) > 18 THEN 'Member_Id exceeds 18 characters'
        WHEN LENGTH(TRIM(COUNTRY)) > 5 THEN 'Country exceeds 5 characters'
        WHEN LENGTH(TRIM(STATE)) > 5 THEN 'State exceeds 5 characters'
        WHEN POST_CODE IS NOT NULL AND TRIM(POST_CODE) <> '' AND (TRY_TO_NUMBER(TRIM(POST_CODE)) IS NULL OR LENGTH(TRIM(POST_CODE)) > 5) THEN 'Invalid Post_Code: must be numeric up to 5 digits'
        WHEN TRIM(TIER_CODE) IS NOT NULL AND UPPER(TRIM(TIER_CODE)) NOT IN ('PLT', 'GLD', 'SLV') THEN 'Invalid domain value: Tier_Code not in (PLT, GLD, SLV)'
        WHEN TRIM(IS_ACTIVE) IS NOT NULL AND UPPER(TRIM(IS_ACTIVE)) NOT IN ('A', 'I') THEN 'Invalid domain value: Is_Active must be in (A, I)'
        ELSE 'Validation constraint failure'
    END AS ERROR_REASON,
    SOURCE_FILE_NAME,
    CURRENT_TIMESTAMP()
FROM RAW_MEMBER_LANDING
WHERE RECORD_TYPE = 'D'
  AND (
      TRIM(MEMBER_ID) IS NULL OR TRIM(MEMBER_ID) = ''
      OR TRIM(MEMBER_NAME) IS NULL OR TRIM(MEMBER_NAME) = ''
      OR TRIM(ENROLLMENT_DATE) IS NULL OR TRIM(ENROLLMENT_DATE) = ''
      OR TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYYMMDD') IS NULL
      OR (TRIM(LAST_FLIGHT_DATE) IS NOT NULL AND TRIM(LAST_FLIGHT_DATE) <> '' AND TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD') IS NULL)
      OR LENGTH(TRIM(MEMBER_ID)) > 18
      OR (COUNTRY IS NOT NULL AND LENGTH(TRIM(COUNTRY)) > 5)
      OR (STATE IS NOT NULL AND LENGTH(TRIM(STATE)) > 5)
      OR (POST_CODE IS NOT NULL AND TRIM(POST_CODE) <> '' AND (TRY_TO_NUMBER(TRIM(POST_CODE)) IS NULL OR LENGTH(TRIM(POST_CODE)) > 5))
      OR (TRIM(TIER_CODE) IS NOT NULL AND UPPER(TRIM(TIER_CODE)) NOT IN ('PLT', 'GLD', 'SLV'))
      OR (TRIM(IS_ACTIVE) IS NOT NULL AND UPPER(TRIM(IS_ACTIVE)) NOT IN ('A', 'I'))
  );


-- ----------------------------------------------------------------------------
-- STEP 2: Populate STG_MEMBER_PROFILE with Exact Calendar Age & Stale_Member
-- ----------------------------------------------------------------------------
INSERT INTO STG_MEMBER_PROFILE (
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
SELECT
    TRIM(MEMBER_ID) AS MEMBER_ID,
    TRIM(MEMBER_NAME) AS MEMBER_NAME,
    TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYYMMDD') AS ENROLLMENT_DATE,
    TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD') AS LAST_FLIGHT_DATE,
    UPPER(TRIM(TIER_CODE)) AS TIER_CODE,
    TRIM(AGENT_NAME) AS AGENT_NAME,
    TRIM(STATE) AS STATE,
    UPPER(TRIM(COUNTRY)) AS COUNTRY,
    TRY_TO_NUMBER(TRIM(POST_CODE)) AS POST_CODE,
    TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY') AS DOB,
    COALESCE(UPPER(TRIM(IS_ACTIVE)), 'A') AS IS_ACTIVE,
    -- Derived 1: Exact Calendar Age (avoids /365.25 off-by-one errors)
    CASE
        WHEN TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY') IS NULL THEN NULL
        ELSE DATEDIFF(year, TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY'), CURRENT_DATE()) - 
             CASE 
                WHEN (MONTH(CURRENT_DATE()) < MONTH(TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY'))) 
                  OR (MONTH(CURRENT_DATE()) = MONTH(TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY')) 
                      AND DAY(CURRENT_DATE()) < DAY(TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY'))) 
                THEN 1 ELSE 0 
             END
    END AS AGE,
    -- Derived 2: Stale_Member flag where days since Flight_Date > 90
    CASE 
        WHEN LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD'), CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    CURRENT_TIMESTAMP() AS INGESTED_AT
FROM RAW_MEMBER_LANDING
WHERE RECORD_TYPE = 'D'
  AND TRIM(MEMBER_ID) IS NOT NULL AND TRIM(MEMBER_ID) <> ''
  AND TRIM(MEMBER_NAME) IS NOT NULL AND TRIM(MEMBER_NAME) <> ''
  AND TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYYMMDD') IS NOT NULL
  AND (TRIM(LAST_FLIGHT_DATE) IS NULL OR TRIM(LAST_FLIGHT_DATE) = '' OR TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD') IS NOT NULL)
  AND LENGTH(TRIM(MEMBER_ID)) <= 18
  AND (COUNTRY IS NULL OR LENGTH(TRIM(COUNTRY)) <= 5)
  AND (STATE IS NULL OR LENGTH(TRIM(STATE)) <= 5)
  AND (POST_CODE IS NULL OR TRIM(POST_CODE) = '' OR (TRY_TO_NUMBER(TRIM(POST_CODE)) IS NOT NULL AND LENGTH(TRIM(POST_CODE)) <= 5))
  AND (TIER_CODE IS NULL OR UPPER(TRIM(TIER_CODE)) IN ('PLT', 'GLD', 'SLV'))
  AND (IS_ACTIVE IS NULL OR UPPER(TRIM(IS_ACTIVE)) IN ('A', 'I'));


-- ----------------------------------------------------------------------------
-- STEP 3: "Latest Record Wins" Transformation & Full Per-Country Split
-- Covers all 5 target countries: IND, USA, CAN, PHIL, AU
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE DEDUPED_LATEST_MEMBERS AS
WITH RankedMembers AS (
    SELECT
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
            PARTITION BY MEMBER_ID 
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

-- 2. Snowflake Scripting: Loop over distinct mapped target tables
EXECUTE IMMEDIATE $$
DECLARE
    c1 CURSOR FOR SELECT DISTINCT TARGET_TABLE FROM REF_COUNTRY_MAP;
    v_tbl VARCHAR;
BEGIN
    FOR record IN c1 DO
        v_tbl := record.TARGET_TABLE;

        -- A. Idempotent MERGE into specific target table updating all columns identically
        EXECUTE IMMEDIATE '
            MERGE INTO IDENTIFIER(:1) target
            USING (
                SELECT d.*
                FROM DEDUPED_LATEST_MEMBERS d
                JOIN REF_COUNTRY_MAP m ON UPPER(TRIM(d.COUNTRY)) = m.COUNTRY_CODE
                WHERE m.TARGET_TABLE = :1
            ) src
            ON target.MEMBER_ID = src.MEMBER_ID
            WHEN MATCHED THEN
                UPDATE SET
                    target.MEMBER_NAME = src.MEMBER_NAME,
                    target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
                    target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
                    target.TIER_CODE = src.TIER_CODE,
                    target.AGENT_NAME = src.AGENT_NAME,
                    target.STATE = src.STATE,
                    target.COUNTRY = src.COUNTRY,
                    target.DOB = src.DOB,
                    target.IS_ACTIVE = src.IS_ACTIVE,
                    target.AGE = src.AGE,
                    target.STALE_MEMBER = src.STALE_MEMBER,
                    target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER, LAST_UPDATED_AT)
                VALUES (src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP())'
            USING (v_tbl);

        -- B. Join-based anti-check DELETE: Removes members who moved to a DIFFERENT mapped country.
        -- Handles NULL country correctly (no NOT IN NULL traps) and never deletes unmapped countries.
        EXECUTE IMMEDIATE '
            DELETE FROM IDENTIFIER(:1) target
            WHERE EXISTS (
                SELECT 1
                FROM DEDUPED_LATEST_MEMBERS d
                JOIN REF_COUNTRY_MAP m ON UPPER(TRIM(d.COUNTRY)) = m.COUNTRY_CODE
                WHERE d.MEMBER_ID = target.MEMBER_ID
                  AND m.TARGET_TABLE <> :1
            )'
            USING (v_tbl);

    END FOR;
    RETURN 'SUCCESS';
END;
$$;


-- ----------------------------------------------------------------------------
-- STEP 4: Parse Semi-Structured JSON Feed & Flatten into Queryable Table
-- ----------------------------------------------------------------------------

-- 1. Quarantine malformed or negative transactions to RAW_DLQ_QUARANTINE
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
SELECT
    COALESCE(r.value:txn_id::STRING, 'UNKNOWN_TXN'),
    r.value::STRING,
    CASE
        WHEN raw.FEED_PAYLOAD:member_id IS NULL OR TRIM(raw.FEED_PAYLOAD:member_id::STRING) = '' THEN 'Missing mandatory member_id in redemption feed'
        WHEN r.value:txn_id IS NULL OR TRIM(r.value:txn_id::STRING) = '' THEN 'Missing mandatory transaction txn_id'
        WHEN r.value:txn_date IS NULL OR TRIM(r.value:txn_date::STRING) = '' THEN 'Missing mandatory transaction txn_date'
        WHEN TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') IS NULL THEN 'Invalid txn_date: unparseable calendar date'
        WHEN r.value:miles_redeemed IS NULL THEN 'Missing mandatory miles_redeemed'
        WHEN TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NULL THEN 'Invalid miles_redeemed data type: non-numeric'
        WHEN r.value:miles_redeemed::INTEGER < 0 THEN 'Invalid miles_redeemed: value cannot be negative'
        WHEN r.value:partner IS NULL OR TRIM(r.value:partner::STRING) = '' THEN 'Missing mandatory partner airline name'
        WHEN r.value:status IS NULL OR TRIM(r.value:status::STRING) = '' THEN 'Missing mandatory transaction status'
        ELSE 'Redemption validation failure'
    END AS ERROR_REASON,
    'RAW_REDEMPTIONS_LANDING',
    CURRENT_TIMESTAMP()
FROM RAW_REDEMPTIONS_LANDING raw,
LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r
WHERE raw.FEED_PAYLOAD:member_id IS NULL OR TRIM(raw.FEED_PAYLOAD:member_id::STRING) = ''
   OR r.value:txn_id IS NULL OR TRIM(r.value:txn_id::STRING) = ''
   OR r.value:txn_date IS NULL OR TRIM(r.value:txn_date::STRING) = ''
   OR TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') IS NULL
   OR r.value:miles_redeemed IS NULL
   OR TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NULL
   OR r.value:miles_redeemed::INTEGER < 0
   OR r.value:partner IS NULL OR TRIM(r.value:partner::STRING) = ''
   OR r.value:status IS NULL OR TRIM(r.value:status::STRING) = '';

-- 2. Idempotent MERGE into STG_REDEMPTIONS_FLATTENED on TXN_ID
-- Note: Snowflake does not enforce PRIMARY KEY constraints; MERGE is essential for idempotency across reruns.
MERGE INTO STG_REDEMPTIONS_FLATTENED target
USING (
    SELECT
        r.value:txn_id::STRING AS TXN_ID,
        raw.FEED_PAYLOAD:member_id::STRING AS MEMBER_ID,
        COALESCE(TRY_TO_DATE(raw.FEED_PAYLOAD:feed_date::STRING, 'YYYYMMDD'), CURRENT_DATE()) AS FEED_DATE,
        TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') AS TXN_DATE,
        TRIM(r.value:partner::STRING) AS PARTNER,
        r.value:miles_redeemed::INTEGER AS MILES_REDEEMED,
        UPPER(TRIM(r.value:status::STRING)) AS STATUS
    FROM RAW_REDEMPTIONS_LANDING raw,
    LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r
    WHERE raw.FEED_PAYLOAD:member_id IS NOT NULL AND TRIM(raw.FEED_PAYLOAD:member_id::STRING) <> ''
      AND r.value:txn_id IS NOT NULL AND TRIM(r.value:txn_id::STRING) <> ''
      AND r.value:txn_date IS NOT NULL AND TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') IS NOT NULL
      AND r.value:miles_redeemed IS NOT NULL AND r.value:miles_redeemed::INTEGER >= 0
      AND r.value:partner IS NOT NULL AND TRIM(r.value:partner::STRING) <> ''
      AND r.value:status IS NOT NULL AND TRIM(r.value:status::STRING) <> ''
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
