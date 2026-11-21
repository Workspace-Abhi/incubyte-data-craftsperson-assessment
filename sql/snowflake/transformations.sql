-- ============================================================================
-- SkyPoints Snowflake Staging Load & Country Split Transformation Logic
-- Implements:
-- 1. Load staging from landing with Age and Stale_Member (> 90 days)
-- 2. "Latest record wins" deduplication and country routing
-- 3. Semi-structured JSON flattening & join
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Populate STG_MEMBER_PROFILE with Derived Columns
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
    DOB,
    IS_ACTIVE,
    AGE,
    STALE_MEMBER,
    INGESTED_AT
)
SELECT
    TRIM(MEMBER_ID) AS MEMBER_ID,
    TRIM(MEMBER_NAME) AS MEMBER_NAME,
    TO_DATE(ENROLLMENT_DATE, 'YYYYMMDD') AS ENROLLMENT_DATE,
    TRY_TO_DATE(LAST_FLIGHT_DATE, 'YYYYMMDD') AS LAST_FLIGHT_DATE,
    TRIM(TIER_CODE) AS TIER_CODE,
    TRIM(AGENT_NAME) AS AGENT_NAME,
    TRIM(STATE) AS STATE,
    UPPER(TRIM(COUNTRY)) AS COUNTRY,
    -- Handle DOB variations (e.g. 8-digit vs 7-digit dropped leading zero)
    TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY') AS DOB,
    COALESCE(UPPER(TRIM(IS_ACTIVE)), 'A') AS IS_ACTIVE,
    -- Derived 1: Age computed from DOB
    FLOOR(DATEDIFF(day, TRY_TO_DATE(LPAD(TRIM(DOB), 8, '0'), 'DDMMYYYY'), CURRENT_DATE()) / 365.25) AS AGE,
    -- Derived 2: Stale_Member flag where days since Flight_Date > 90
    CASE 
        WHEN LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, TRY_TO_DATE(LAST_FLIGHT_DATE, 'YYYYMMDD'), CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    CURRENT_TIMESTAMP() AS INGESTED_AT
FROM RAW_MEMBER_LANDING
WHERE RECORD_TYPE = 'D'
  AND MEMBER_ID IS NOT NULL
  AND MEMBER_NAME IS NOT NULL
  AND ENROLLMENT_DATE IS NOT NULL;


-- ----------------------------------------------------------------------------
-- STEP 2: "Latest Record Wins" Transformation & Per-Country Split
-- When a member moves countries, the latest record wins.
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

-- MERGE into TABLE_INDIA
MERGE INTO TABLE_INDIA target
USING (SELECT * FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY = 'IND') src
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
    VALUES (src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP());

-- Remove from TABLE_INDIA if member moved to another country
DELETE FROM TABLE_INDIA
WHERE MEMBER_ID IN (
    SELECT MEMBER_ID FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY <> 'IND'
);

-- MERGE into TABLE_USA
MERGE INTO TABLE_USA target
USING (SELECT * FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY = 'USA') src
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
    VALUES (src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP());

-- Remove from TABLE_USA if member moved to another country
DELETE FROM TABLE_USA
WHERE MEMBER_ID IN (
    SELECT MEMBER_ID FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY <> 'USA'
);


-- ----------------------------------------------------------------------------
-- STEP 3: Parse Semi-Structured JSON Feed & Flatten into Queryable Table
-- ----------------------------------------------------------------------------
INSERT INTO STG_REDEMPTIONS_FLATTENED (
    TXN_ID,
    MEMBER_ID,
    FEED_DATE,
    TXN_DATE,
    PARTNER,
    MILES_REDEEMED,
    STATUS,
    INGESTED_AT
)
SELECT
    r.value:txn_id::STRING AS TXN_ID,
    raw.FEED_PAYLOAD:member_id::STRING AS MEMBER_ID,
    TO_DATE(raw.FEED_PAYLOAD:feed_date::STRING, 'YYYYMMDD') AS FEED_DATE,
    TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD') AS TXN_DATE,
    r.value:partner::STRING AS PARTNER,
    r.value:miles_redeemed::INTEGER AS MILES_REDEEMED,
    r.value:status::STRING AS STATUS,
    CURRENT_TIMESTAMP() AS INGESTED_AT
FROM RAW_REDEMPTIONS_LANDING raw,
LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r;


-- ----------------------------------------------------------------------------
-- STEP 4: Loyalty 360 Analytical Mart (Join Profile + Redemptions)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE MEMBER_LOYALTY_360 AS
WITH RedemptionSummary AS (
    SELECT
        MEMBER_ID,
        COUNT(DISTINCT TXN_ID) AS TOTAL_REDEMPTIONS,
        COALESCE(SUM(MILES_REDEEMED), 0) AS TOTAL_MILES_REDEEMED,
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
    r.LAST_REDEMPTION_DATE,
    r.TOP_PARTNER,
    CURRENT_TIMESTAMP() AS LAST_SYNC_AT
FROM DEDUPED_LATEST_MEMBERS m
LEFT JOIN RedemptionSummary r ON m.MEMBER_ID = r.MEMBER_ID;
