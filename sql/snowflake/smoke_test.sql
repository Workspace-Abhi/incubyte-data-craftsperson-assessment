-- ============================================================================
-- Snowflake End-to-End Smoke Test Script
-- Schema: SKYPOINTS_SMOKE_TEST (Transient schema for isolated verification)
--
-- This script validates the SkyPoints pipeline logic on a live Snowflake account:
-- 1. Sets up temporary schema and all tables/views.
-- 2. Seeds reference data and initial country table state.
-- 3. Ingests hand-crafted edge-case fixtures covering:
--    - Member moving country (old table DELETE, new table INSERT)
--    - Same ID in two source systems (both survive independently)
--    - Rename collision (older name quarantined, newer name kept)
--    - Empty-string tier code (treated as NULL, not rejected)
--    - Unmapped country (quarantined at staging, not silently dropped)
--    - Bad feed_date in redemption (quarantined into DLQ)
--    - Duplicate txn_id replay (idempotent MERGE, no double counting)
-- 4. Runs full transformation pipeline.
-- 5. Evaluates PASS/FAIL assertions in a single result table.
-- ============================================================================

-- Step 0: Isolated Environment Setup
CREATE OR REPLACE TRANSIENT SCHEMA SKYPOINTS_SMOKE_TEST;
USE SCHEMA SKYPOINTS_SMOKE_TEST;

-- ----------------------------------------------------------------------------
-- 1. DDL: Create Tables and Views
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_MEMBER_LANDING (
    RECORD_TYPE          VARCHAR(1),
    SOURCE_SYSTEM        VARCHAR(30)      DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50),
    MEMBER_NAME          VARCHAR(255),
    ENROLLMENT_DATE      VARCHAR(50),
    LAST_FLIGHT_DATE     VARCHAR(50),
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    DOB                  VARCHAR(50),
    IS_ACTIVE            VARCHAR(10),
    POST_CODE            VARCHAR(50),
    SOURCE_FILE_NAME     VARCHAR(255),
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_DLQ_QUARANTINE (
    RECORD_ID            VARCHAR(50),
    RAW_RECORD           VARCHAR,
    ERROR_REASON         VARCHAR(500),
    SOURCE_FEED          VARCHAR(255),
    QUARANTINED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STG_MEMBER_PROFILE (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    POST_CODE            NUMBER(5, 0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1)       DEFAULT 'A',
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    SOURCE_FILE_NAME     VARCHAR(255),
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_MEMBER PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID) RELY
);

CREATE OR REPLACE TABLE TABLE_INDIA (
    SOURCE_SYSTEM VARCHAR(30) NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID VARCHAR(50) NOT NULL,
    MEMBER_NAME VARCHAR(255) NOT NULL,
    ENROLLMENT_DATE DATE NOT NULL,
    LAST_FLIGHT_DATE DATE,
    TIER_CODE VARCHAR(10),
    AGENT_NAME VARCHAR(255),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50) DEFAULT 'IND',
    DOB DATE,
    IS_ACTIVE VARCHAR(1),
    AGE INTEGER,
    STALE_MEMBER VARCHAR(1),
    LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
);

CREATE OR REPLACE TABLE TABLE_USA (
    SOURCE_SYSTEM VARCHAR(30) NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID VARCHAR(50) NOT NULL,
    MEMBER_NAME VARCHAR(255) NOT NULL,
    ENROLLMENT_DATE DATE NOT NULL,
    LAST_FLIGHT_DATE DATE,
    TIER_CODE VARCHAR(10),
    AGENT_NAME VARCHAR(255),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50) DEFAULT 'USA',
    DOB DATE,
    IS_ACTIVE VARCHAR(1),
    AGE INTEGER,
    STALE_MEMBER VARCHAR(1),
    LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
);

CREATE OR REPLACE TABLE TABLE_CAN (
    SOURCE_SYSTEM VARCHAR(30) NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID VARCHAR(50) NOT NULL,
    MEMBER_NAME VARCHAR(255) NOT NULL,
    ENROLLMENT_DATE DATE NOT NULL,
    LAST_FLIGHT_DATE DATE,
    TIER_CODE VARCHAR(10),
    AGENT_NAME VARCHAR(255),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50) DEFAULT 'CAN',
    DOB DATE,
    IS_ACTIVE VARCHAR(1),
    AGE INTEGER,
    STALE_MEMBER VARCHAR(1),
    LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
);

CREATE OR REPLACE TABLE TABLE_PHIL (
    SOURCE_SYSTEM VARCHAR(30) NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID VARCHAR(50) NOT NULL,
    MEMBER_NAME VARCHAR(255) NOT NULL,
    ENROLLMENT_DATE DATE NOT NULL,
    LAST_FLIGHT_DATE DATE,
    TIER_CODE VARCHAR(10),
    AGENT_NAME VARCHAR(255),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50) DEFAULT 'PHIL',
    DOB DATE,
    IS_ACTIVE VARCHAR(1),
    AGE INTEGER,
    STALE_MEMBER VARCHAR(1),
    LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
);

CREATE OR REPLACE TABLE TABLE_AU (
    SOURCE_SYSTEM VARCHAR(30) NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID VARCHAR(50) NOT NULL,
    MEMBER_NAME VARCHAR(255) NOT NULL,
    ENROLLMENT_DATE DATE NOT NULL,
    LAST_FLIGHT_DATE DATE,
    TIER_CODE VARCHAR(10),
    AGENT_NAME VARCHAR(255),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50) DEFAULT 'AU',
    DOB DATE,
    IS_ACTIVE VARCHAR(1),
    AGE INTEGER,
    STALE_MEMBER VARCHAR(1),
    LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
);

CREATE OR REPLACE TABLE REF_COUNTRY_MAP (
    COUNTRY_CODE         VARCHAR(10)      PRIMARY KEY,
    CANONICAL_CODE       VARCHAR(10)      NOT NULL,
    TARGET_TABLE         VARCHAR(50)      NOT NULL
);

INSERT INTO REF_COUNTRY_MAP (COUNTRY_CODE, CANONICAL_CODE, TARGET_TABLE) VALUES
('IND', 'IND', 'TABLE_INDIA'),
('INDIA', 'IND', 'TABLE_INDIA'),
('USA', 'USA', 'TABLE_USA'),
('CAN', 'CAN', 'TABLE_CAN'),
('CANADA', 'CAN', 'TABLE_CAN'),
('PHIL', 'PHIL', 'TABLE_PHIL'),
('PH', 'PHIL', 'TABLE_PHIL'),
('AU', 'AU', 'TABLE_AU'),
('AUS', 'AU', 'TABLE_AU');

CREATE OR REPLACE VIEW MEMBERS_ALL AS
SELECT * FROM TABLE_INDIA
UNION ALL
SELECT * FROM TABLE_USA
UNION ALL
SELECT * FROM TABLE_CAN
UNION ALL
SELECT * FROM TABLE_PHIL
UNION ALL
SELECT * FROM TABLE_AU;

CREATE OR REPLACE TABLE RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STG_REDEMPTIONS_FLATTENED (
    TXN_ID               VARCHAR(50)      PRIMARY KEY,
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    FEED_DATE            DATE             NOT NULL,
    TXN_DATE             DATE             NOT NULL,
    PARTNER              VARCHAR(100)     NOT NULL,
    MILES_REDEEMED       INTEGER          NOT NULL,
    STATUS               VARCHAR(50)      NOT NULL,
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE MEMBER_LOYALTY_360 (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255),
    COUNTRY              VARCHAR(50),
    TIER_CODE            VARCHAR(10),
    ENROLLMENT_DATE      DATE,
    LAST_FLIGHT_DATE     DATE,
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    TOTAL_REDEMPTIONS    INTEGER          DEFAULT 0,
    TOTAL_MILES_REDEEMED INTEGER          DEFAULT 0,
    PENDING_MILES_REDEEMED INTEGER        DEFAULT 0,
    LAST_REDEMPTION_DATE DATE,
    TOP_PARTNER          VARCHAR(100),
    LAST_SYNC_AT         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
);

-- ----------------------------------------------------------------------------
-- 2. SEED INITIAL HISTORICAL STATE
-- ----------------------------------------------------------------------------
-- Pre-existing member in TABLE_USA who will move to TABLE_INDIA in today's batch
INSERT INTO TABLE_USA (
    SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER
) VALUES (
    'FLAT_FILE', 'MOVE_101', 'David Miller', DATE '2020-01-01', DATE '2021-05-01',
    'GLD', 'AgentA', 'CA', 'USA', DATE '1985-05-03', 'A', 38, 'Y'
);

-- ----------------------------------------------------------------------------
-- 3. LOAD TEST FIXTURES INTO RAW LANDING
-- ----------------------------------------------------------------------------
INSERT INTO RAW_MEMBER_LANDING (
    RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE, SOURCE_FILE_NAME
) VALUES
-- Fixture 1: Member MOVE_101 moved to IND with newer flight date (2023-01-15)
('D', 'FLAT_FILE', 'MOVE_101', 'David Miller', '20200101', '20230115', 'GLD', 'AgentA', 'MH', 'IND', '03051985', 'A', '40001', 'today_batch.dat'),

-- Fixture 2: Same ID 'DUP_1' across two distinct source systems
('D', 'IND_SYS', 'DUP_1', 'Vikas Patel', '20220101', '20230615', 'SLV', 'AgentB', 'DL', 'IND', '15081995', 'A', '11001', 'IND.csv'),
('D', 'USA_SYS', 'DUP_1', 'Sam Wilson', '20220101', '20230720', 'PLT', 'AgentC', 'NY', 'USA', '20101990', 'A', '10001', 'USA.csv'),

-- Fixture 3: Rename collision for 'REN_202' (Older name Alice Brown vs Newer name Alice Green)
('D', 'FLAT_FILE', 'REN_202', 'Alice Brown', '20200101', '20210601', 'GLD', 'AgentD', 'VIC', 'AU', '12121992', 'A', '3000', 'older_event.dat'),
('D', 'FLAT_FILE', 'REN_202', 'Alice Green', '20200101', '20230815', 'GLD', 'AgentD', 'VIC', 'AU', '12121992', 'A', '3000', 'newer_event.dat'),

-- Fixture 4: Empty-string Tier Code (should be treated as NULL and valid, not quarantined)
('D', 'FLAT_FILE', 'EMPTY_TIER_1', 'Carlos Ray', '20220101', '20230101', '   ', 'AgentE', 'NCR', 'PHIL', '01011990', 'A', '1200', 'ph_batch.dat'),

-- Fixture 5: Unmapped country 'GERMANY' (should be quarantined at staging)
('D', 'FLAT_FILE', 'BAD_CTRY_1', 'Hans Gruber', '20220101', '20230101', 'GLD', 'AgentF', 'BER', 'GERMANY', '01011990', 'A', '10115', 'eu_batch.dat');

-- Semi-structured Redemption Fixtures
INSERT INTO RAW_REDEMPTIONS_LANDING (FEED_PAYLOAD, FEED_DATE)
SELECT PARSE_JSON('{
    "member_id": "MOVE_101",
    "feed_date": "20240115",
    "redemptions": [
        {"txn_id": "TXN_REPLAY_1", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 5000, "status": "COMPLETED"},
        {"txn_id": "TXN_REPLAY_1", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 5000, "status": "COMPLETED"}
    ]
}'), '20240115';

-- Bad redemption feed fixture (unparseable feed_date)
INSERT INTO RAW_REDEMPTIONS_LANDING (FEED_PAYLOAD, FEED_DATE)
SELECT PARSE_JSON('{
    "member_id": "DUP_1",
    "feed_date": "BAD_DATE_FORMAT",
    "redemptions": [
        {"txn_id": "TXN_BAD_FEED", "txn_date": "20240110", "partner": "SkyPoints", "miles_redeemed": 1000, "status": "COMPLETED"}
    ]
}'), 'BAD_DATE_FORMAT';

-- ----------------------------------------------------------------------------
-- 4. EXECUTE TRANSFORMATION LOGIC
-- ----------------------------------------------------------------------------

-- A. Quarantine malformed landing rows
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
SELECT
    MEMBER_ID,
    CONCAT_WS('|', RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE),
    CASE
        WHEN RECORD_TYPE <> 'D' THEN 'Invalid Record_Type: expected ''D'''
        WHEN MEMBER_ID IS NULL OR NULLIF(TRIM(MEMBER_ID), '') IS NULL THEN 'Missing mandatory Member_Id'
        WHEN LENGTH(TRIM(MEMBER_ID)) > 18 THEN 'Field length exceeded: Member_Id exceeds 18 chars'
        WHEN MEMBER_NAME IS NULL OR NULLIF(TRIM(MEMBER_NAME), '') IS NULL THEN 'Missing mandatory Member_Name'
        WHEN ENROLLMENT_DATE IS NULL OR TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYYMMDD') IS NULL THEN 'Invalid Enrollment_Date'
        WHEN LAST_FLIGHT_DATE IS NOT NULL AND NULLIF(TRIM(LAST_FLIGHT_DATE), '') IS NOT NULL AND TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD') IS NULL THEN 'Invalid Last_Flight_Date: unparseable calendar date'
        WHEN COUNTRY IS NULL OR NULLIF(TRIM(COUNTRY), '') IS NULL THEN 'Unmapped country: Country is required and cannot be empty'
        WHEN LENGTH(TRIM(COUNTRY)) > 5 THEN 'Field length exceeded: Country exceeds 5 characters'
        WHEN m.CANONICAL_CODE IS NULL THEN 'Unmapped country'
        WHEN STATE IS NOT NULL AND NULLIF(TRIM(STATE), '') IS NOT NULL AND LENGTH(TRIM(STATE)) > 5 THEN 'Field length exceeded: State exceeds 5 characters'
        WHEN POST_CODE IS NOT NULL AND NULLIF(TRIM(POST_CODE), '') IS NOT NULL AND (TRY_TO_NUMBER(TRIM(POST_CODE)) IS NULL OR LENGTH(TRIM(POST_CODE)) > 5) THEN 'Invalid Post_Code: must be numeric'
        WHEN TIER_CODE IS NOT NULL AND NULLIF(TRIM(TIER_CODE), '') IS NOT NULL AND UPPER(TRIM(TIER_CODE)) NOT IN ('PLT', 'GLD', 'SLV') THEN 'Invalid domain value: Tier_Code not in (PLT, GLD, SLV)'
        WHEN IS_ACTIVE IS NOT NULL AND NULLIF(TRIM(IS_ACTIVE), '') IS NOT NULL AND UPPER(TRIM(IS_ACTIVE)) NOT IN ('A', 'I') THEN 'Invalid domain value: Is_Active must be in (A, I)'
        ELSE 'Unknown validation failure'
    END,
    'RAW_MEMBER_LANDING',
    CURRENT_TIMESTAMP()
FROM RAW_MEMBER_LANDING l
LEFT JOIN REF_COUNTRY_MAP m ON UPPER(TRIM(l.COUNTRY)) = m.COUNTRY_CODE
WHERE l.RECORD_TYPE <> 'D'
   OR l.MEMBER_ID IS NULL OR NULLIF(TRIM(l.MEMBER_ID), '') IS NULL OR LENGTH(TRIM(l.MEMBER_ID)) > 18
   OR l.MEMBER_NAME IS NULL OR NULLIF(TRIM(l.MEMBER_NAME), '') IS NULL
   OR l.ENROLLMENT_DATE IS NULL OR TRY_TO_DATE(TRIM(l.ENROLLMENT_DATE), 'YYYYMMDD') IS NULL
   OR (l.LAST_FLIGHT_DATE IS NOT NULL AND NULLIF(TRIM(l.LAST_FLIGHT_DATE), '') IS NOT NULL AND TRY_TO_DATE(TRIM(l.LAST_FLIGHT_DATE), 'YYYYMMDD') IS NULL)
   OR l.COUNTRY IS NULL OR NULLIF(TRIM(l.COUNTRY), '') IS NULL OR LENGTH(TRIM(l.COUNTRY)) > 5 OR m.CANONICAL_CODE IS NULL
   OR (l.STATE IS NOT NULL AND NULLIF(TRIM(l.STATE), '') IS NOT NULL AND LENGTH(TRIM(l.STATE)) > 5)
   OR (l.POST_CODE IS NOT NULL AND NULLIF(TRIM(l.POST_CODE), '') IS NOT NULL AND (TRY_TO_NUMBER(TRIM(l.POST_CODE)) IS NULL OR LENGTH(TRIM(l.POST_CODE)) > 5))
   OR (l.TIER_CODE IS NOT NULL AND NULLIF(TRIM(l.TIER_CODE), '') IS NOT NULL AND UPPER(TRIM(l.TIER_CODE)) NOT IN ('PLT', 'GLD', 'SLV'))
   OR (l.IS_ACTIVE IS NOT NULL AND NULLIF(TRIM(l.IS_ACTIVE), '') IS NOT NULL AND UPPER(TRIM(l.IS_ACTIVE)) NOT IN ('A', 'I'));

-- B. Staging Insert
INSERT INTO STG_MEMBER_PROFILE (
    SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE,
    AGE, STALE_MEMBER, SOURCE_FILE_NAME, INGESTED_AT
)
WITH SanitizedLanding AS (
    SELECT
        RECORD_TYPE,
        COALESCE(NULLIF(TRIM(SOURCE_SYSTEM), ''), 'FLAT_FILE') AS SOURCE_SYSTEM,
        NULLIF(TRIM(MEMBER_ID), '') AS MEMBER_ID,
        NULLIF(TRIM(MEMBER_NAME), '') AS MEMBER_NAME,
        NULLIF(TRIM(ENROLLMENT_DATE), '') AS ENROLLMENT_DATE,
        NULLIF(TRIM(LAST_FLIGHT_DATE), '') AS LAST_FLIGHT_DATE,
        NULLIF(TRIM(TIER_CODE), '') AS TIER_CODE,
        NULLIF(TRIM(AGENT_NAME), '') AS AGENT_NAME,
        NULLIF(TRIM(STATE), '') AS STATE,
        NULLIF(TRIM(COUNTRY), '') AS COUNTRY,
        NULLIF(TRIM(POST_CODE), '') AS POST_CODE,
        NULLIF(TRIM(DOB), '') AS DOB,
        NULLIF(TRIM(IS_ACTIVE), '') AS IS_ACTIVE,
        SOURCE_FILE_NAME,
        INGESTED_AT
    FROM RAW_MEMBER_LANDING
)
SELECT
    s.SOURCE_SYSTEM,
    s.MEMBER_ID,
    s.MEMBER_NAME,
    TRY_TO_DATE(s.ENROLLMENT_DATE, 'YYYYMMDD'),
    TRY_TO_DATE(s.LAST_FLIGHT_DATE, 'YYYYMMDD'),
    s.TIER_CODE,
    s.AGENT_NAME,
    s.STATE,
    m.CANONICAL_CODE,
    TRY_TO_NUMBER(s.POST_CODE),
    TRY_TO_DATE(LPAD(s.DOB, 8, '0'), 'DDMMYYYY'),
    COALESCE(s.IS_ACTIVE, 'A'),
    35 AS AGE,
    'N' AS STALE_MEMBER,
    s.SOURCE_FILE_NAME,
    CURRENT_TIMESTAMP()
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

-- C. Collision Resolution
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
            ORDER BY COALESCE(s.LAST_FLIGHT_DATE, s.ENROLLMENT_DATE) DESC, s.INGESTED_AT DESC, s.SOURCE_FILE_NAME
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

-- D. Deduplication Window
CREATE OR REPLACE TEMPORARY TABLE DEDUPED_LATEST_MEMBERS AS
WITH RankedMembers AS (
    SELECT
        SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
        TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER,
        SOURCE_FILE_NAME, INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, MEMBER_ID 
            ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE) DESC, INGESTED_AT DESC, SOURCE_FILE_NAME
        ) AS RN
    FROM STG_MEMBER_PROFILE
)
SELECT * EXCLUDE (RN)
FROM RankedMembers
WHERE RN = 1;

-- E. Country Merges and Deletes
-- AU
MERGE INTO TABLE_AU target
USING (SELECT * FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY = 'AU') src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN UPDATE SET target.MEMBER_NAME = src.MEMBER_NAME, target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER, LAST_UPDATED_AT)
VALUES (src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP());
DELETE FROM TABLE_AU target WHERE EXISTS (SELECT 1 FROM DEDUPED_LATEST_MEMBERS d WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM AND d.MEMBER_ID = target.MEMBER_ID AND d.COUNTRY <> 'AU');

-- CAN
MERGE INTO TABLE_CAN target
USING (SELECT * FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY = 'CAN') src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN UPDATE SET target.MEMBER_NAME = src.MEMBER_NAME, target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER, LAST_UPDATED_AT)
VALUES (src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP());
DELETE FROM TABLE_CAN target WHERE EXISTS (SELECT 1 FROM DEDUPED_LATEST_MEMBERS d WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM AND d.MEMBER_ID = target.MEMBER_ID AND d.COUNTRY <> 'CAN');

-- IND
MERGE INTO TABLE_INDIA target
USING (SELECT * FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY = 'IND') src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN UPDATE SET target.MEMBER_NAME = src.MEMBER_NAME, target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER, LAST_UPDATED_AT)
VALUES (src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP());
DELETE FROM TABLE_INDIA target WHERE EXISTS (SELECT 1 FROM DEDUPED_LATEST_MEMBERS d WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM AND d.MEMBER_ID = target.MEMBER_ID AND d.COUNTRY <> 'IND');

-- PHIL
MERGE INTO TABLE_PHIL target
USING (SELECT * FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY = 'PHIL') src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN UPDATE SET target.MEMBER_NAME = src.MEMBER_NAME, target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER, LAST_UPDATED_AT)
VALUES (src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP());
DELETE FROM TABLE_PHIL target WHERE EXISTS (SELECT 1 FROM DEDUPED_LATEST_MEMBERS d WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM AND d.MEMBER_ID = target.MEMBER_ID AND d.COUNTRY <> 'PHIL');

-- USA
MERGE INTO TABLE_USA target
USING (SELECT * FROM DEDUPED_LATEST_MEMBERS WHERE COUNTRY = 'USA') src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN UPDATE SET target.MEMBER_NAME = src.MEMBER_NAME, target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER, LAST_UPDATED_AT)
VALUES (src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.DOB, src.IS_ACTIVE, src.AGE, src.STALE_MEMBER, CURRENT_TIMESTAMP());
DELETE FROM TABLE_USA target WHERE EXISTS (SELECT 1 FROM DEDUPED_LATEST_MEMBERS d WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM AND d.MEMBER_ID = target.MEMBER_ID AND d.COUNTRY <> 'USA');

-- F. Redemptions Quarantine & MERGE
INSERT INTO RAW_DLQ_QUARANTINE (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
SELECT
    COALESCE(r.value:txn_id::STRING, 'MISSING_TXN_ID'),
    TO_VARCHAR(r.value),
    CASE
        WHEN raw.FEED_PAYLOAD:feed_date IS NULL OR TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') IS NULL THEN 'Invalid feed_date in redemption feed'
        WHEN raw.FEED_PAYLOAD:member_id IS NULL OR NULLIF(TRIM(raw.FEED_PAYLOAD:member_id::STRING), '') IS NULL THEN 'Missing mandatory member_id in redemption feed'
        WHEN r.value:txn_id IS NULL OR NULLIF(TRIM(r.value:txn_id::STRING), '') IS NULL THEN 'Missing mandatory transaction txn_id'
        WHEN r.value:txn_date IS NULL OR TRY_TO_DATE(TRIM(r.value:txn_date::STRING), 'YYYYMMDD') IS NULL THEN 'Missing mandatory transaction txn_date'
        WHEN r.value:miles_redeemed IS NULL THEN 'Missing mandatory miles_redeemed'
        WHEN TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NULL THEN 'Invalid miles_redeemed data type'
        WHEN r.value:miles_redeemed::INTEGER < 0 THEN 'Invalid miles_redeemed: value cannot be negative'
        WHEN r.value:partner IS NULL OR NULLIF(TRIM(r.value:partner::STRING), '') IS NULL THEN 'Missing mandatory partner airline name'
        WHEN r.value:status IS NULL OR NULLIF(TRIM(r.value:status::STRING), '') IS NULL THEN 'Missing mandatory transaction status'
        ELSE 'Unknown redemption validation failure'
    END,
    'RAW_REDEMPTIONS_LANDING',
    CURRENT_TIMESTAMP()
FROM RAW_REDEMPTIONS_LANDING raw,
LATERAL FLATTEN(input => raw.FEED_PAYLOAD:redemptions) r
WHERE raw.FEED_PAYLOAD:feed_date IS NULL
   OR TRY_TO_DATE(TRIM(raw.FEED_PAYLOAD:feed_date::STRING), 'YYYYMMDD') IS NULL
   OR raw.FEED_PAYLOAD:member_id IS NULL OR NULLIF(TRIM(raw.FEED_PAYLOAD:member_id::STRING), '') IS NULL
   OR r.value:txn_id IS NULL OR NULLIF(TRIM(r.value:txn_id::STRING), '') IS NULL
   OR r.value:txn_date IS NULL OR TRY_TO_DATE(TRIM(r.value:txn_date::STRING), 'YYYYMMDD') IS NULL
   OR r.value:miles_redeemed IS NULL OR TRY_TO_NUMBER(r.value:miles_redeemed::STRING) IS NULL OR r.value:miles_redeemed::INTEGER < 0
   OR r.value:partner IS NULL OR NULLIF(TRIM(r.value:partner::STRING), '') IS NULL
   OR r.value:status IS NULL OR NULLIF(TRIM(r.value:status::STRING), '') IS NULL;

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

-- G. Loyalty 360 Mart MERGE
MERGE INTO MEMBER_LOYALTY_360 target
USING (
    WITH RedemptionSummary AS (
        SELECT
            MEMBER_ID,
            COUNT(DISTINCT TXN_ID) AS TOTAL_REDEMPTIONS,
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
        COALESCE(r.TOP_PARTNER, 'None') AS TOP_PARTNER
    FROM MEMBERS_ALL m
    LEFT JOIN RedemptionSummary r ON m.MEMBER_ID = r.MEMBER_ID
    WHERE EXISTS (
        SELECT 1 FROM DEDUPED_LATEST_MEMBERS d
        WHERE d.SOURCE_SYSTEM = m.SOURCE_SYSTEM AND d.MEMBER_ID = m.MEMBER_ID
    )
    OR EXISTS (
        SELECT 1 FROM STG_REDEMPTIONS_FLATTENED red
        WHERE red.MEMBER_ID = m.MEMBER_ID
    )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.COUNTRY = src.COUNTRY,
        target.TIER_CODE = src.TIER_CODE,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.TOTAL_REDEMPTIONS = src.TOTAL_REDEMPTIONS,
        target.TOTAL_MILES_REDEEMED = src.TOTAL_MILES_REDEEMED,
        target.PENDING_MILES_REDEEMED = src.PENDING_MILES_REDEEMED,
        target.LAST_REDEMPTION_DATE = src.LAST_REDEMPTION_DATE,
        target.TOP_PARTNER = src.TOP_PARTNER,
        target.LAST_SYNC_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, COUNTRY, TIER_CODE,
        ENROLLMENT_DATE, LAST_FLIGHT_DATE, AGE, STALE_MEMBER,
        TOTAL_REDEMPTIONS, TOTAL_MILES_REDEEMED, PENDING_MILES_REDEEMED,
        LAST_REDEMPTION_DATE, TOP_PARTNER, LAST_SYNC_AT
    )
    VALUES (
        src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.COUNTRY, src.TIER_CODE,
        src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE, src.AGE, src.STALE_MEMBER,
        src.TOTAL_REDEMPTIONS, src.TOTAL_MILES_REDEEMED, src.PENDING_MILES_REDEEMED,
        src.LAST_REDEMPTION_DATE, src.TOP_PARTNER, CURRENT_TIMESTAMP()
    );

-- ----------------------------------------------------------------------------
-- 5. FINAL SMOKE TEST ASSERTIONS (PASS/FAIL)
-- ----------------------------------------------------------------------------
WITH AssertionResults AS (
    -- Test 1A: Member moving country deleted from old table (TABLE_USA)
    SELECT
        1 AS test_id,
        'Member moving country: deleted from old TABLE_USA' AS test_name,
        '0' AS expected_value,
        TO_VARCHAR(COUNT(*)) AS actual_value
    FROM TABLE_USA
    WHERE MEMBER_ID = 'MOVE_101'

    UNION ALL

    -- Test 1B: Member moving country inserted into new table (TABLE_INDIA)
    SELECT
        2 AS test_id,
        'Member moving country: inserted into new TABLE_INDIA' AS test_name,
        '1' AS expected_value,
        TO_VARCHAR(COUNT(*)) AS actual_value
    FROM TABLE_INDIA
    WHERE MEMBER_ID = 'MOVE_101'

    UNION ALL

    -- Test 2: Same ID across two source systems both survive in their country tables
    SELECT
        3 AS test_id,
        'Composite key: Same ID in IND_SYS and USA_SYS both survive' AS test_name,
        '2' AS expected_value,
        TO_VARCHAR(
            (SELECT COUNT(*) FROM TABLE_INDIA WHERE SOURCE_SYSTEM = 'IND_SYS' AND MEMBER_ID = 'DUP_1') +
            (SELECT COUNT(*) FROM TABLE_USA WHERE SOURCE_SYSTEM = 'USA_SYS' AND MEMBER_ID = 'DUP_1')
        ) AS actual_value

    UNION ALL

    -- Test 3A: Rename collision older name quarantined
    SELECT
        4 AS test_id,
        'Rename collision: older name quarantined in DLQ' AS test_name,
        '1' AS expected_value,
        TO_VARCHAR(COUNT(*)) AS actual_value
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'REN_202'
      AND ERROR_REASON = 'Key collision: superseded by newer record'

    UNION ALL

    -- Test 3B: Rename collision newer name kept in target table
    SELECT
        5 AS test_id,
        'Rename collision: newer name kept in target table' AS test_name,
        'Alice Green' AS expected_value,
        MEMBER_NAME AS actual_value
    FROM TABLE_AU
    WHERE MEMBER_ID = 'REN_202'

    UNION ALL

    -- Test 4: Empty-string Tier Code treated as NULL and staged successfully
    SELECT
        6 AS test_id,
        'Empty-string tier code: treated as null, not quarantined' AS test_name,
        '1' AS expected_value,
        TO_VARCHAR(COUNT(*)) AS actual_value
    FROM TABLE_PHIL
    WHERE MEMBER_ID = 'EMPTY_TIER_1'
      AND TIER_CODE IS NULL

    UNION ALL

    -- Test 5: Unmapped country quarantined at staging
    SELECT
        7 AS test_id,
        'Unmapped country: quarantined in DLQ at staging' AS test_name,
        '1' AS expected_value,
        TO_VARCHAR(COUNT(*)) AS actual_value
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'BAD_CTRY_1'
      AND ERROR_REASON = 'Unmapped country'

    UNION ALL

    -- Test 6: Bad feed_date in redemption quarantined
    SELECT
        8 AS test_id,
        'Bad redemption feed_date: quarantined into DLQ' AS test_name,
        '1' AS expected_value,
        TO_VARCHAR(COUNT(*)) AS actual_value
    FROM RAW_DLQ_QUARANTINE
    WHERE SOURCE_FEED = 'RAW_REDEMPTIONS_LANDING'
      AND ERROR_REASON = 'Invalid feed_date in redemption feed'

    UNION ALL

    -- Test 7: Duplicate txn_id replay is idempotent (miles not doubled)
    SELECT
        9 AS test_id,
        'Duplicate txn_id replay: idempotent merge (miles not doubled)' AS test_name,
        '5000' AS expected_value,
        TO_VARCHAR(COALESCE(TOTAL_MILES_REDEEMED, 0)) AS actual_value
    FROM MEMBER_LOYALTY_360
    WHERE MEMBER_ID = 'MOVE_101'
)
SELECT
    test_id,
    test_name,
    expected_value,
    actual_value,
    CASE WHEN expected_value = actual_value THEN 'PASS' ELSE 'FAIL' END AS status
FROM AssertionResults
ORDER BY test_id;
