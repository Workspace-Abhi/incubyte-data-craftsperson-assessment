-- ============================================================================
-- Snowflake End-to-End Smoke Test Script
-- Schema: SKYPOINTS_SMOKE_TEST (Transient schema for isolated verification)
--
-- This script runs the REAL production scripts via SnowSQL !source directives:
-- 1. Sets up temporary schema SKYPOINTS_SMOKE_TEST.
-- 2. Executes production DDL via !source sql/snowflake/ddl.sql.
-- 3. Seeds initial state and loads hand-crafted edge-case fixtures:
--    - Member moving country (old table DELETE, new table INSERT)
--    - Same raw ID in two source systems (both survive independently)
--    - Rename collision in ONE insert statement (winner kept, loser quarantined)
--    - Empty-string tier (treated as NULL, loaded into country table)
--    - Unmapped country (quarantined to DLQ)
--    - Null country (quarantined to DLQ)
--    - Bad feed_date in redemption feed (quarantined to DLQ)
--    - Duplicate txn_id replay (idempotent, miles not doubled)
--    - Ambiguous member_id redemption across multiple sources (quarantined)
-- 4. Executes production transformations via !source sql/snowflake/transformations.sql.
-- 5. Executes production country merges via !source sql/snowflake/generated_country_merges.sql.
-- 6. Returns a single result table evaluating PASS/FAIL across all scenarios.
-- ============================================================================

-- Step 1: Create isolated temporary schema and set as current
CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
USE DATABASE SKYPOINTS_DB;
CREATE OR REPLACE TRANSIENT SCHEMA SKYPOINTS_SMOKE_TEST;
USE SCHEMA SKYPOINTS_SMOKE_TEST;

-- Step 2: Execute production DDL
-- ============================================================================
-- SkyPoints Airline Loyalty Program - Snowflake DDL Architecture
-- Scalable for billions of daily records across flat files and JSON feeds
-- ============================================================================

-- Database and Schema Setup
CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.CORE;
USE SCHEMA SKYPOINTS_DB.CORE;

-- ----------------------------------------------------------------------------
-- 1. RAW LANDING TABLE: Member Profile Flat File
-- Ingests raw pipe-delimited data as VARCHAR to guarantee zero load dropouts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_MEMBER_LANDING (
    RECORD_TYPE          VARCHAR(10),     -- 'H' for Header, 'D' for Detail
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_NAME          VARCHAR(255),
    MEMBER_ID            VARCHAR(50),
    ENROLLMENT_DATE      VARCHAR(50),
    LAST_FLIGHT_DATE     VARCHAR(50),
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    DOB                  VARCHAR(50),
    IS_ACTIVE            VARCHAR(10),
    POST_CODE            VARCHAR(50),
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dead Letter Queue (DLQ) table for quarantined malformed rows
CREATE TABLE IF NOT EXISTS RAW_DLQ_QUARANTINE (
    RECORD_ID            VARCHAR(50),
    RAW_RECORD           VARCHAR,
    RAW_RECORD_HASH      VARCHAR(64),
    ERROR_REASON         VARCHAR(500),
    SOURCE_FEED          VARCHAR(255),
    SOURCE_SYSTEM        VARCHAR(30),
    SOURCE_FILE_NAME     VARCHAR(255),
    BATCH_ID             VARCHAR(50),
    QUARANTINED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. STAGING TABLE: Member Profile with Derived Columns
-- Strongly-typed, normalized dates, plus Age & Stale_Member (> 90 days)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STG_MEMBER_PROFILE (
    STG_ROW_ID          VARCHAR          DEFAULT UUID_STRING(),
    BATCH_ID             VARCHAR(50),
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    POST_CODE            NUMBER(5, 0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1)       DEFAULT 'A',
    AGE                  INTEGER,         -- Derived: Exact calendar age
    STALE_MEMBER         VARCHAR(1),      -- Derived: 'Y' if days since Flight_Date > 90 else 'N'
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
    -- NOTE: Primary key constraints are removed from STG_MEMBER_PROFILE because staging
    -- legitimately holds multiple records per key during ingestion prior to deduplication.
    -- Snowflake does not enforce PRIMARY KEY constraints on country tables or marts.
)
CLUSTER BY (COUNTRY, ENROLLMENT_DATE);

-- ----------------------------------------------------------------------------
-- 3. COUNTRY-SPECIFIC TARGET TABLES
-- Dedicated physical country tables clustered on MEMBER_ID
-- NOTE: Primary key declarations are informational metadata; Snowflake does not
-- enforce primary key uniqueness. Pipeline deduplication and MERGE logic enforce uniqueness.
-- ----------------------------------------------------------------------------

-- Table: India
CREATE TABLE IF NOT EXISTS TABLE_INDIA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'IND',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: USA
CREATE TABLE IF NOT EXISTS TABLE_USA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'USA',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: Australia
CREATE TABLE IF NOT EXISTS TABLE_AU (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'AU',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- ----------------------------------------------------------------------------
-- 4. SEMI-STRUCTURED REDEMPTION TABLES & MART TABLES
-- ----------------------------------------------------------------------------

-- Landing table for raw JSON documents
CREATE TABLE IF NOT EXISTS RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Flattened relational redemptions table
CREATE TABLE IF NOT EXISTS STG_REDEMPTIONS_FLATTENED (
    TXN_ID               VARCHAR(50)      PRIMARY KEY,
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    FEED_DATE            DATE             NOT NULL,
    TXN_DATE             DATE             NOT NULL,
    PARTNER              VARCHAR(100)     NOT NULL,
    MILES_REDEEMED       INTEGER          NOT NULL,
    STATUS               VARCHAR(50)      NOT NULL,
    IS_AMBIGUOUS         BOOLEAN          DEFAULT FALSE,
    BATCH_ID             VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID, TXN_DATE);

-- Analytical mart table
CREATE TABLE IF NOT EXISTS MEMBER_LOYALTY_360 (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255),
    COUNTRY              VARCHAR(50),
    TIER_CODE            VARCHAR(50),
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
-- 5. SCHEMA EVOLUTION & MIGRATION STATEMENTS (FOR PRE-EXISTING SCHEMAS)
-- Safe, idempotent column additions executed BEFORE views are created/updated
-- so existing tables have all required columns before view compilation.
-- ----------------------------------------------------------------------------
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING MODIFY COLUMN TIER_CODE VARCHAR(50);

ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;

ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS RAW_RECORD_HASH VARCHAR(64);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_SYSTEM VARCHAR(30);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS STG_ROW_ID VARCHAR;

ALTER TABLE STG_REDEMPTIONS_FLATTENED ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

-- ----------------------------------------------------------------------------
-- 6. REFERENCE TABLES: Country Table Mapping
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REF_COUNTRY_MAP (
    COUNTRY_CODE         VARCHAR(10)      PRIMARY KEY,
    CANONICAL_CODE       VARCHAR(10)      NOT NULL,
    TARGET_TABLE         VARCHAR(50)      NOT NULL
);

-- Seed Country Mappings (IND/INDIA -> TABLE_INDIA, USA -> TABLE_USA, AU/AUS -> TABLE_AU)
MERGE INTO REF_COUNTRY_MAP target
USING (
    SELECT 'IND' AS code, 'IND' AS canonical, 'TABLE_INDIA' AS tbl UNION ALL
    SELECT 'INDIA', 'IND', 'TABLE_INDIA' UNION ALL
    SELECT 'USA', 'USA', 'TABLE_USA' UNION ALL
    SELECT 'AU', 'AU', 'TABLE_AU' UNION ALL
    SELECT 'AUS', 'AU', 'TABLE_AU'
) src
ON target.COUNTRY_CODE = src.code
WHEN MATCHED THEN
    UPDATE SET target.CANONICAL_CODE = src.canonical, target.TARGET_TABLE = src.tbl
WHEN NOT MATCHED THEN
    INSERT (COUNTRY_CODE, CANONICAL_CODE, TARGET_TABLE) VALUES (src.code, src.canonical, src.tbl);

-- ----------------------------------------------------------------------------
-- 7. UNIFIED COUNTRY MEMBERS VIEW
-- Explicit column list projected across all country tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL AS
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_INDIA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_USA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_AU;

-- ----------------------------------------------------------------------------
-- 8. DYNAMIC UNIFIED MEMBERS VIEW (Query-Time Recalculation)
-- Dynamically recomputes AGE and STALE_MEMBER at query time relative to CURRENT_DATE()
-- while preserving static stored attributes in the country tables.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL_CURRENT AS
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
    -- Dynamically recomputed exact calendar age at query time
    CASE
        WHEN DOB IS NULL THEN NULL
        ELSE DATEDIFF(year, DOB, CURRENT_DATE()) - 
             CASE 
                WHEN (MONTH(CURRENT_DATE()) < MONTH(DOB)) 
                  OR (MONTH(CURRENT_DATE()) = MONTH(DOB) 
                      AND DAY(CURRENT_DATE()) < DAY(DOB)) 
                THEN 1 ELSE 0 
             END
    END AS AGE,
    -- Dynamically recomputed stale member flag at query time (>90 days since flight)
    CASE 
        WHEN LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, LAST_FLIGHT_DATE, CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    EXTRACT_TS,
    SOURCE_FILE_NAME,
    LINE_NO,
    LAST_UPDATED_AT
FROM MEMBERS_ALL;

-- ----------------------------------------------------------------------------
-- 9. DYNAMIC ANALYTICAL MART: Member Loyalty 360 Current View
-- Reads stored redemption totals from MEMBER_LOYALTY_360 (the mart), which are
-- refreshed each run by the pipeline, and recomputes only Age and Stale_Member
-- dynamically relative to CURRENT_DATE() using MEMBERS_ALL_CURRENT.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBER_LOYALTY_360_CURRENT AS
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
    COALESCE(l.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
    COALESCE(l.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
    COALESCE(l.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
    l.LAST_REDEMPTION_DATE,
    COALESCE(l.TOP_PARTNER, 'None') AS TOP_PARTNER,
    CURRENT_TIMESTAMP() AS LAST_SYNC_AT
FROM MEMBERS_ALL_CURRENT m
LEFT JOIN MEMBER_LOYALTY_360 l
    ON m.SOURCE_SYSTEM = l.SOURCE_SYSTEM AND m.MEMBER_ID = l.MEMBER_ID;


-- Step 3: Seed initial historical state & load fixtures into landing
-- Pre-existing historical member in TABLE_USA who moves to India in today's batch.
-- Seeded with higher flight date (2023-08-01) but older extract date (2023-01-01).
INSERT INTO TABLE_USA (
    SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS
) VALUES (
    'FLAT_FILE', 'MOVE_101', 'David Miller', DATE '2020-01-01', DATE '2023-08-01',
    'GLD', 'AgentA', 'CA', 'USA', 90210, DATE '1985-05-03', 'A', 38, 'Y', '2023-01-01 00:00:00'::TIMESTAMP_NTZ
);

-- Interrupted batch fixture: simulates an interrupted run that claimed rows with an old BATCH_ID but failed before PROCESSED_AT was stamped
INSERT INTO RAW_MEMBER_LANDING (
    RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE, SOURCE_FILE_NAME, LINE_NO, BATCH_ID
) VALUES
('D', 'FLAT_FILE', 'INTERRUPT_1', 'Orphan Recovered', '20220101', '20230101', 'GLD', 'AgentInt', 'CA', 'USA', '01011990', 'A', '90210', 'batch_int.dat', 1, 'ORPHAN_BATCH_999');

-- Member landing fixtures covering all test scenarios (loaded in a single batch)
INSERT INTO RAW_MEMBER_LANDING (
    RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE, SOURCE_FILE_NAME, LINE_NO
) VALUES
-- 1. Member MOVE_101 moving country to IND with lower flight date (20210501) but newer extract date (20240115)
('D', 'FLAT_FILE', 'MOVE_101', 'David Miller', '20200101', '20210501', 'GLD', 'AgentA', 'MH', 'IND', '03051985', 'A', '40001', 'today_batch_20240115.dat', 1),

-- 2. Same raw ID 'DUP_1' across two distinct source systems
('D', 'IND_SYS', 'DUP_1', 'Vikas Patel', '20220101', '20230615', 'SLV', 'AgentB', 'DL', 'IND', '15081995', 'A', '11001', 'IND.csv', 1),
('D', 'USA_SYS', 'DUP_1', 'Sam Wilson', '20220101', '20230720', 'PLT', 'AgentC', 'NY', 'USA', '20101990', 'A', '10001', 'USA.csv', 1),

-- 3. Rename collision loaded in ONE insert statement (identical INGESTED_AT)
('D', 'FLAT_FILE', 'REN_202', 'Alice Brown', '20200101', '20210601', 'GLD', 'AgentD', 'VIC', 'AU', '12121992', 'A', '3000', 'older_event.dat', 1),
('D', 'FLAT_FILE', 'REN_202', 'Alice Green', '20200101', '20230815', 'GLD', 'AgentD', 'VIC', 'AU', '12121992', 'A', '3000', 'newer_event.dat', 2),

-- 4. Empty-string Tier Code (should be treated as NULL and valid, not rejected)
('D', 'FLAT_FILE', 'EMPTY_TIER_1', 'Carlos Ray', '20220101', '20230101', '   ', 'AgentE', 'VIC', 'AU', '01011990', 'A', '3000', 'au_batch.dat', 1),

-- 5. Unmapped country 'GERMANY' (quarantined to DLQ)
('D', 'FLAT_FILE', 'BAD_CTRY_1', 'Hans Gruber', '20220101', '20230101', 'GLD', 'AgentF', 'BER', 'GERMANY', '01011990', 'A', '10115', 'eu_batch.dat', 1),

-- 6. Null country (quarantined to DLQ)
('D', 'FLAT_FILE', 'NULL_CTRY_1', 'Jane Doe', '20220101', '20230101', 'GLD', 'AgentG', 'CA', NULL, '01011990', 'A', '90210', 'us_batch.dat', 1),

-- 7. Two distinct rows with missing member_id in the same batch (both must be quarantined, not suppressed)
('D', 'FLAT_FILE', NULL, 'Missing ID Alpha', '20220101', '20230101', 'GLD', 'AgentH', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat', 1),
('D', 'FLAT_FILE', NULL, 'Missing ID Beta', '20220101', '20230101', 'GLD', 'AgentH', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat', 2),

-- 8. Same ID in two distinct source systems both bad (both must be quarantined)
('D', 'IND_SYS', 'BAD_DUP_1', 'Bad Ind User', '20220101', '20230101', 'GLD', 'AgentI', 'BER', 'GERMANY', '01011990', 'A', '11001', 'IND_bad.csv', 1),
('D', 'USA_SYS', 'BAD_DUP_1', 'Bad Usa User', '20220101', '20230101', 'GLD', 'AgentI', 'BER', 'GERMANY', '01011990', 'A', '10001', 'USA_bad.csv', 1),

-- 9. Bad row arriving in Batch 1 (will be repeated in Batch 2)
('D', 'FLAT_FILE', 'REPEAT_BAD_1', 'Repeat Bad Person', '20220101', '20230101', 'INVALID_TIER', 'AgentJ', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat', 1),

-- 10. Member BATCH_REN_1 original name in Batch 1 (will be renamed in Batch 2)
('D', 'FLAT_FILE', 'BATCH_REN_1', 'Original Name', '20200101', '20210101', 'GLD', 'AgentK', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat', 1),

-- 11. Member RESOLVE_101 ambiguous across two sources in Batch 1 (will be resolved in Batch 2)
('D', 'IND_SYS', 'RESOLVE_101', 'Rahul Ind', '20200101', '20210101', 'GLD', 'AgentL', 'MH', 'IND', '01011990', 'A', '40001', 'ind_1.dat', 1),
('D', 'USA_SYS', 'RESOLVE_101', 'Sam Usa', '20200101', '20210101', 'GLD', 'AgentL', 'CA', 'USA', '01011990', 'A', '90210', 'usa_1.dat', 1),

-- 12. Invalid DOB (unparseable calendar date, should be quarantined to DLQ)
('D', 'FLAT_FILE', 'BAD_DOB_1', 'Bad DOB User', '20220101', '20230101', 'GLD', 'AgentM', 'CA', 'USA', '99999999', 'A', '90210', 'batch_1.dat', 1),

-- 13. Same-day cross-country move in Batch 1 (arrives in USA with morning file zzz_20240115_090000.dat)
('D', 'FLAT_FILE', 'SAME_DAY_MOVE_1', 'Same Day User', '20220101', '20230101', 'GLD', 'AgentSD', 'CA', 'USA', '01011990', 'A', '90210', 'zzz_20240115_090000.dat', 10),

-- 14. Same-country tie fixture in Batch 1 (USA, timestamp 20240115_120000)
('D', 'FLAT_FILE', 'SAME_CTRY_TIE_1', 'Tie Initial', '20220101', '20230101', 'GLD', 'AgentCT', 'CA', 'USA', '01011990', 'A', '90210', 'tie_20240115_120000.dat', 20);

-- Redemption fixtures
-- Non-numeric miles_redeemed fixture: 'abc' quarantined without aborting
INSERT INTO RAW_REDEMPTIONS_LANDING (FEED_PAYLOAD, FEED_DATE)
SELECT PARSE_JSON('{
    "member_id": "MOVE_101",
    "feed_date": "20240115",
    "redemptions": [
        {"txn_id": "TXN_NON_NUM_1", "txn_date": "20240110", "partner": "SkyPoints", "miles_redeemed": "abc", "status": "COMPLETED"}
    ]
}'), '20240115';

-- Replay fixture: identical TXN_REPLAY_1 sent twice
INSERT INTO RAW_REDEMPTIONS_LANDING (FEED_PAYLOAD, FEED_DATE)
SELECT PARSE_JSON('{
    "member_id": "MOVE_101",
    "feed_date": "20240115",
    "redemptions": [
        {"txn_id": "TXN_REPLAY_1", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 5000, "status": "COMPLETED"},
        {"txn_id": "TXN_REPLAY_1", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 5000, "status": "COMPLETED"}
    ]
}'), '20240115';

-- Bad feed_date fixture
INSERT INTO RAW_REDEMPTIONS_LANDING (FEED_PAYLOAD, FEED_DATE)
SELECT PARSE_JSON('{
    "member_id": "MOVE_101",
    "feed_date": "BAD_DATE_FORMAT",
    "redemptions": [
        {"txn_id": "TXN_BAD_FEED", "txn_date": "20240110", "partner": "SkyPoints", "miles_redeemed": 1000, "status": "COMPLETED"}
    ]
}'), 'BAD_DATE_FORMAT';

-- Ambiguous member_id redemption: DUP_1 exists in both IND_SYS and USA_SYS
INSERT INTO RAW_REDEMPTIONS_LANDING (FEED_PAYLOAD, FEED_DATE)
SELECT PARSE_JSON('{
    "member_id": "DUP_1",
    "feed_date": "20240115",
    "redemptions": [
        {"txn_id": "TXN_AMBIG_1", "txn_date": "20240112", "partner": "SkyPoints", "miles_redeemed": 2500, "status": "COMPLETED"}
    ]
}'), '20240115';

-- Ambiguity round-trip fixture: RESOLVE_101 initially ambiguous, resolved in Batch 2
INSERT INTO RAW_REDEMPTIONS_LANDING (FEED_PAYLOAD, FEED_DATE)
SELECT PARSE_JSON('{
    "member_id": "RESOLVE_101",
    "feed_date": "20240115",
    "redemptions": [
        {"txn_id": "TXN_RESOLVE_1", "txn_date": "20240110", "partner": "SkyPoints", "miles_redeemed": 3000, "status": "COMPLETED"}
    ]
}'), '20240115';

-- Step 4: Run production pipeline via run_all.sql (Run 1: Processes Initial Batches)
-- ==================== INLINED 01_DDL.SQL ====================

-- ============================================================================
-- SkyPoints Airline Loyalty Program - Snowflake DDL Architecture
-- Scalable for billions of daily records across flat files and JSON feeds
-- ============================================================================

-- Database and Schema Setup
CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.CORE;
USE SCHEMA SKYPOINTS_DB.CORE;

-- ----------------------------------------------------------------------------
-- 1. RAW LANDING TABLE: Member Profile Flat File
-- Ingests raw pipe-delimited data as VARCHAR to guarantee zero load dropouts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_MEMBER_LANDING (
    RECORD_TYPE          VARCHAR(10),     -- 'H' for Header, 'D' for Detail
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_NAME          VARCHAR(255),
    MEMBER_ID            VARCHAR(50),
    ENROLLMENT_DATE      VARCHAR(50),
    LAST_FLIGHT_DATE     VARCHAR(50),
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    DOB                  VARCHAR(50),
    IS_ACTIVE            VARCHAR(10),
    POST_CODE            VARCHAR(50),
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dead Letter Queue (DLQ) table for quarantined malformed rows
CREATE TABLE IF NOT EXISTS RAW_DLQ_QUARANTINE (
    RECORD_ID            VARCHAR(50),
    RAW_RECORD           VARCHAR,
    RAW_RECORD_HASH      VARCHAR(64),
    ERROR_REASON         VARCHAR(500),
    SOURCE_FEED          VARCHAR(255),
    SOURCE_SYSTEM        VARCHAR(30),
    SOURCE_FILE_NAME     VARCHAR(255),
    BATCH_ID             VARCHAR(50),
    QUARANTINED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. STAGING TABLE: Member Profile with Derived Columns
-- Strongly-typed, normalized dates, plus Age & Stale_Member (> 90 days)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STG_MEMBER_PROFILE (
    STG_ROW_ID          VARCHAR          DEFAULT UUID_STRING(),
    BATCH_ID             VARCHAR(50),
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    POST_CODE            NUMBER(5, 0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1)       DEFAULT 'A',
    AGE                  INTEGER,         -- Derived: Exact calendar age
    STALE_MEMBER         VARCHAR(1),      -- Derived: 'Y' if days since Flight_Date > 90 else 'N'
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
    -- NOTE: Primary key constraints are removed from STG_MEMBER_PROFILE because staging
    -- legitimately holds multiple records per key during ingestion prior to deduplication.
    -- Snowflake does not enforce PRIMARY KEY constraints on country tables or marts.
)
CLUSTER BY (COUNTRY, ENROLLMENT_DATE);

-- ----------------------------------------------------------------------------
-- 3. COUNTRY-SPECIFIC TARGET TABLES
-- Dedicated physical country tables clustered on MEMBER_ID
-- NOTE: Primary key declarations are informational metadata; Snowflake does not
-- enforce primary key uniqueness. Pipeline deduplication and MERGE logic enforce uniqueness.
-- ----------------------------------------------------------------------------

-- Table: India
CREATE TABLE IF NOT EXISTS TABLE_INDIA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'IND',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: USA
CREATE TABLE IF NOT EXISTS TABLE_USA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'USA',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: Australia
CREATE TABLE IF NOT EXISTS TABLE_AU (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'AU',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- ----------------------------------------------------------------------------
-- 4. SEMI-STRUCTURED REDEMPTION TABLES & MART TABLES
-- ----------------------------------------------------------------------------

-- Landing table for raw JSON documents
CREATE TABLE IF NOT EXISTS RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Flattened relational redemptions table
CREATE TABLE IF NOT EXISTS STG_REDEMPTIONS_FLATTENED (
    TXN_ID               VARCHAR(50)      PRIMARY KEY,
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    FEED_DATE            DATE             NOT NULL,
    TXN_DATE             DATE             NOT NULL,
    PARTNER              VARCHAR(100)     NOT NULL,
    MILES_REDEEMED       INTEGER          NOT NULL,
    STATUS               VARCHAR(50)      NOT NULL,
    IS_AMBIGUOUS         BOOLEAN          DEFAULT FALSE,
    BATCH_ID             VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID, TXN_DATE);

-- Analytical mart table
CREATE TABLE IF NOT EXISTS MEMBER_LOYALTY_360 (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255),
    COUNTRY              VARCHAR(50),
    TIER_CODE            VARCHAR(50),
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
-- 5. SCHEMA EVOLUTION & MIGRATION STATEMENTS (FOR PRE-EXISTING SCHEMAS)
-- Safe, idempotent column additions executed BEFORE views are created/updated
-- so existing tables have all required columns before view compilation.
-- ----------------------------------------------------------------------------
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING MODIFY COLUMN TIER_CODE VARCHAR(50);

ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;

ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS RAW_RECORD_HASH VARCHAR(64);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_SYSTEM VARCHAR(30);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS STG_ROW_ID VARCHAR;

ALTER TABLE STG_REDEMPTIONS_FLATTENED ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

-- ----------------------------------------------------------------------------
-- 6. REFERENCE TABLES: Country Table Mapping
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REF_COUNTRY_MAP (
    COUNTRY_CODE         VARCHAR(10)      PRIMARY KEY,
    CANONICAL_CODE       VARCHAR(10)      NOT NULL,
    TARGET_TABLE         VARCHAR(50)      NOT NULL
);

-- Seed Country Mappings (IND/INDIA -> TABLE_INDIA, USA -> TABLE_USA, AU/AUS -> TABLE_AU)
MERGE INTO REF_COUNTRY_MAP target
USING (
    SELECT 'IND' AS code, 'IND' AS canonical, 'TABLE_INDIA' AS tbl UNION ALL
    SELECT 'INDIA', 'IND', 'TABLE_INDIA' UNION ALL
    SELECT 'USA', 'USA', 'TABLE_USA' UNION ALL
    SELECT 'AU', 'AU', 'TABLE_AU' UNION ALL
    SELECT 'AUS', 'AU', 'TABLE_AU'
) src
ON target.COUNTRY_CODE = src.code
WHEN MATCHED THEN
    UPDATE SET target.CANONICAL_CODE = src.canonical, target.TARGET_TABLE = src.tbl
WHEN NOT MATCHED THEN
    INSERT (COUNTRY_CODE, CANONICAL_CODE, TARGET_TABLE) VALUES (src.code, src.canonical, src.tbl);

-- ----------------------------------------------------------------------------
-- 7. UNIFIED COUNTRY MEMBERS VIEW
-- Explicit column list projected across all country tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL AS
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_INDIA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_USA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_AU;

-- ----------------------------------------------------------------------------
-- 8. DYNAMIC UNIFIED MEMBERS VIEW (Query-Time Recalculation)
-- Dynamically recomputes AGE and STALE_MEMBER at query time relative to CURRENT_DATE()
-- while preserving static stored attributes in the country tables.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL_CURRENT AS
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
    -- Dynamically recomputed exact calendar age at query time
    CASE
        WHEN DOB IS NULL THEN NULL
        ELSE DATEDIFF(year, DOB, CURRENT_DATE()) - 
             CASE 
                WHEN (MONTH(CURRENT_DATE()) < MONTH(DOB)) 
                  OR (MONTH(CURRENT_DATE()) = MONTH(DOB) 
                      AND DAY(CURRENT_DATE()) < DAY(DOB)) 
                THEN 1 ELSE 0 
             END
    END AS AGE,
    -- Dynamically recomputed stale member flag at query time (>90 days since flight)
    CASE 
        WHEN LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, LAST_FLIGHT_DATE, CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    EXTRACT_TS,
    SOURCE_FILE_NAME,
    LINE_NO,
    LAST_UPDATED_AT
FROM MEMBERS_ALL;

-- ----------------------------------------------------------------------------
-- 9. DYNAMIC ANALYTICAL MART: Member Loyalty 360 Current View
-- Reads stored redemption totals from MEMBER_LOYALTY_360 (the mart), which are
-- refreshed each run by the pipeline, and recomputes only Age and Stale_Member
-- dynamically relative to CURRENT_DATE() using MEMBERS_ALL_CURRENT.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBER_LOYALTY_360_CURRENT AS
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
    COALESCE(l.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
    COALESCE(l.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
    COALESCE(l.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
    l.LAST_REDEMPTION_DATE,
    COALESCE(l.TOP_PARTNER, 'None') AS TOP_PARTNER,
    CURRENT_TIMESTAMP() AS LAST_SYNC_AT
FROM MEMBERS_ALL_CURRENT m
LEFT JOIN MEMBER_LOYALTY_360 l
    ON m.SOURCE_SYSTEM = l.SOURCE_SYSTEM AND m.MEMBER_ID = l.MEMBER_ID;


-- ==================== INLINED 03_TRANSFORMATIONS.SQL ====================

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
SET run_batch_id = (SELECT UUID_STRING());

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
        WHEN m.CANONICAL_CODE IS NULL THEN 'Unmapped country'
        WHEN LENGTH(s.COUNTRY) > 5 THEN 'Country exceeds 5 characters'
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
    EXTRACT_TS,
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
        COALESCE(
            EXTRACT_TS,
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}[_ -]?[0-9]{6}'), 'YYYYMMDD_HH24MISS'),
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}'), 'YYYYMMDD'),
            INGESTED_AT,
            CURRENT_TIMESTAMP()
        ) AS EXTRACT_TS,
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
    s.EXTRACT_TS,
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
                ORDER BY s.EXTRACT_TS DESC,
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
        EXTRACT_TS,
        LINE_NO,
        INGESTED_AT,
        STG_ROW_ID,
        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, MEMBER_ID 
            ORDER BY EXTRACT_TS DESC,
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
    EXTRACT_TS,
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
--   sql/snowflake/04_country_merges.generated.sql
--
-- In SnowSQL CLI, execute:
-- !source sql/snowflake/04_country_merges.generated.sql
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- STEP 3C: Audit Out-of-Order / Superseded Records into RAW_DLQ_QUARANTINE
-- Captures late-arriving older records that are blocked from overwriting newer core records
-- ----------------------------------------------------------------------------
MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        src.MEMBER_ID AS RECORD_ID,
        CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO), 256) AS RAW_RECORD_HASH,
        'Superseded by existing newer record in core table' AS ERROR_REASON,
        'DEDUPED_LATEST_MEMBERS' AS SOURCE_FEED,
        src.SOURCE_SYSTEM,
        src.SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM DEDUPED_LATEST_MEMBERS src
    JOIN MEMBERS_ALL m ON src.SOURCE_SYSTEM = m.SOURCE_SYSTEM AND src.MEMBER_ID = m.MEMBER_ID
    WHERE (
        m.EXTRACT_TS > src.EXTRACT_TS
        OR (
            m.EXTRACT_TS = src.EXTRACT_TS
            AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.value:txn_id::STRING ORDER BY raw.INGESTED_AT DESC) = 1
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



-- ==================== INLINED 04_COUNTRY_MERGES.SQL ====================

-- ============================================================================
-- Static Country MERGE & Migration DELETE Statements for Snowflake
-- Generated automatically by scripts/generate_country_merges.py
-- Single source of truth: CANONICAL_TABLE_MAP in src/country_router.py
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Target Country: AU -> TABLE_AU
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_AU target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'AU'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_AU if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_AU target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'AU'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: IND -> TABLE_INDIA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_INDIA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'IND'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_INDIA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_INDIA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'IND'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: USA -> TABLE_USA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_USA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'USA'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_USA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_USA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'USA'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);



-- ==================== INLINED 05_MART_AND_FINALIZE.SQL ====================

-- ============================================================================
-- SkyPoints Step 3: Analytical Mart Build & Finalize Landing Batch Tracking
-- NOTE: run_batch_id is a session variable initialized in transformations.sql.
-- All four scripts must execute in the same SnowSQL session in order.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 5: Loyalty 360 Analytical Mart (Sums Only COMPLETED Miles)
-- ----------------------------------------------------------------------------

-- Snapshot pre-update status (column renamed so it can never clash with the live table)
CREATE OR REPLACE TEMPORARY TABLE PRE_UPDATE_REDEMPTION_STATUS AS
SELECT TXN_ID, IS_AMBIGUOUS AS PREV_IS_AMBIGUOUS
FROM STG_REDEMPTIONS_FLATTENED;

-- Member IDs that appear in more than one source system
CREATE OR REPLACE TEMPORARY TABLE AMBIGUOUS_MEMBER_IDS AS
SELECT m.MEMBER_ID
FROM MEMBERS_ALL_CURRENT m
GROUP BY m.MEMBER_ID
HAVING COUNT(DISTINCT m.SOURCE_SYSTEM) > 1;

-- Flag redemptions (no correlated view subquery)
UPDATE STG_REDEMPTIONS_FLATTENED AS red
SET IS_AMBIGUOUS = IFF(
    red.MEMBER_ID IN (SELECT a.MEMBER_ID FROM AMBIGUOUS_MEMBER_IDS a),
    TRUE,
    FALSE
);

-- Members whose ambiguity flipped in this run
CREATE OR REPLACE TEMPORARY TABLE AMBIGUITY_CHANGED_MEMBERS AS
SELECT DISTINCT cur.MEMBER_ID
FROM STG_REDEMPTIONS_FLATTENED cur
JOIN PRE_UPDATE_REDEMPTION_STATUS prev ON cur.TXN_ID = prev.TXN_ID
WHERE cur.IS_AMBIGUOUS <> prev.PREV_IS_AMBIGUOUS;

MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        red.TXN_ID AS RECORD_ID,
        CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS), 256) AS RAW_RECORD_HASH,
        'Ambiguous member_id across source systems' AS ERROR_REASON,
        'STG_REDEMPTIONS_FLATTENED' AS SOURCE_FEED,
        'PARTNER_JSON' AS SOURCE_SYSTEM,
        'stg_redemptions' AS SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM STG_REDEMPTIONS_FLATTENED red
    WHERE red.IS_AMBIGUOUS = TRUE
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
AND target.RAW_RECORD_HASH = src.RAW_RECORD_HASH
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, RAW_RECORD_HASH, ERROR_REASON, SOURCE_FEED, SOURCE_SYSTEM, SOURCE_FILE_NAME, BATCH_ID, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.RAW_RECORD_HASH, src.ERROR_REASON, src.SOURCE_FEED, src.SOURCE_SYSTEM, src.SOURCE_FILE_NAME, src.BATCH_ID, CURRENT_TIMESTAMP());

-- Remove resolved redemptions from DLQ when ambiguity disappears
DELETE FROM RAW_DLQ_QUARANTINE
WHERE SOURCE_FEED = 'STG_REDEMPTIONS_FLATTENED'
  AND ERROR_REASON = 'Ambiguous member_id across source systems'
  AND RECORD_ID IN (
      SELECT red.TXN_ID FROM STG_REDEMPTIONS_FLATTENED red WHERE red.IS_AMBIGUOUS = FALSE
  );

-- 2. Build or Refresh MEMBER_LOYALTY_360 Mart (Idempotent MERGE for affected members)
-- Keyed on (SOURCE_SYSTEM, MEMBER_ID) from MEMBERS_ALL_CURRENT; touches only members affected by this batch,
-- including members whose redemptions were previously ambiguous and are now resolved.
-- Excludes IS_AMBIGUOUS = TRUE transactions from analytical calculations.
-- Breaks MODE(PARTNER) ties deterministically by highest count then alphabetical.
MERGE INTO MEMBER_LOYALTY_360 target
USING (
    WITH TouchedMembers AS (
        -- 1. Members touched in current batch
        SELECT SOURCE_SYSTEM, MEMBER_ID
        FROM DEDUPED_LATEST_MEMBERS
        UNION
        -- 2. Members with incoming redemptions in current batch
        SELECT m.SOURCE_SYSTEM, red.MEMBER_ID
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN MEMBERS_ALL_CURRENT m ON red.MEMBER_ID = m.MEMBER_ID
        WHERE red.BATCH_ID = $run_batch_id
        UNION
        -- 3. Members whose redemptions status flipped (became ambiguous or became resolved)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        JOIN AMBIGUITY_CHANGED_MEMBERS acm ON m.MEMBER_ID = acm.MEMBER_ID
        UNION
        -- 4. Members with redemptions whose member profile in MEMBERS_ALL spans multiple source systems (ambiguity detected)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        WHERE m.MEMBER_ID IN (
            SELECT MEMBER_ID FROM MEMBERS_ALL_CURRENT GROUP BY MEMBER_ID HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
        )
    ),
    PartnerCounts AS (
        SELECT
            red.MEMBER_ID,
            red.PARTNER,
            COUNT(*) AS CNT,
            ROW_NUMBER() OVER (PARTITION BY red.MEMBER_ID ORDER BY COUNT(*) DESC, red.PARTNER ASC) AS RN
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE AND red.PARTNER IS NOT NULL
        GROUP BY red.MEMBER_ID, red.PARTNER
    ),
    TopPartner AS (
        SELECT MEMBER_ID, PARTNER AS TOP_PARTNER
        FROM PartnerCounts
        WHERE RN = 1
    ),
    RedemptionSummary AS (
        SELECT
            red.MEMBER_ID,
            COUNT(DISTINCT red.TXN_ID) AS TOTAL_REDEMPTIONS,
            COALESCE(SUM(CASE WHEN red.STATUS = 'COMPLETED' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS TOTAL_MILES_REDEEMED,
            COALESCE(SUM(CASE WHEN red.STATUS = 'PENDING' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS PENDING_MILES_REDEEMED,
            MAX(red.TXN_DATE) AS LAST_REDEMPTION_DATE
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE
        GROUP BY red.MEMBER_ID
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
        COALESCE(rs.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
        COALESCE(rs.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
        COALESCE(rs.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
        rs.LAST_REDEMPTION_DATE,
        COALESCE(tp.TOP_PARTNER, 'None') AS TOP_PARTNER
    FROM MEMBERS_ALL_CURRENT m
    JOIN TouchedMembers tm ON m.SOURCE_SYSTEM = tm.SOURCE_SYSTEM AND m.MEMBER_ID = tm.MEMBER_ID
    LEFT JOIN RedemptionSummary rs ON m.MEMBER_ID = rs.MEMBER_ID
    LEFT JOIN TopPartner tp ON m.MEMBER_ID = tp.MEMBER_ID
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

-- Remove members from mart that no longer exist in unified current members view (e.g. cross-country moves or hard deletes)
DELETE FROM MEMBER_LOYALTY_360
WHERE (SOURCE_SYSTEM, MEMBER_ID) NOT IN (
    SELECT SOURCE_SYSTEM, MEMBER_ID FROM MEMBERS_ALL_CURRENT
);


-- ----------------------------------------------------------------------------
-- STEP 6: Mark Processed Records in Landing Tables for Batch Tracking
-- Stamps ONLY the records claimed and processed by this specific batch
-- ----------------------------------------------------------------------------
UPDATE RAW_MEMBER_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;

UPDATE RAW_REDEMPTIONS_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;


-- Step 4B: Load Batch 2 arriving in a separate batch
-- Includes:
-- 1. Identical malformed record to test multi-batch reject preservation
-- 2. Rename across batches: BATCH_REN_1 renamed to 'Updated Name' with newer flight date
-- 3. Ambiguity resolution: remove spurious secondary source for RESOLVE_101 so ambiguity disappears
INSERT INTO RAW_MEMBER_LANDING (
    RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE, SOURCE_FILE_NAME, LINE_NO
) VALUES
('D', 'FLAT_FILE', 'REPEAT_BAD_1', 'Repeat Bad Person', '20220101', '20230101', 'INVALID_TIER', 'AgentJ', 'CA', 'USA', '01011990', 'A', '90210', 'batch_2.dat', 1),
('D', 'FLAT_FILE', 'BATCH_REN_1', 'Updated Name', '20200101', '20230601', 'GLD', 'AgentK', 'CA', 'USA', '01011990', 'A', '90210', 'batch_2.dat', 2),
('D', 'FLAT_FILE', 'MOVE_101', 'David Miller Older', '20200101', '20210101', 'GLD', 'AgentA', 'MH', 'IND', '03051985', 'A', '40001', 'batch_old_20200101.dat', 3),
-- Same-day correction with earlier-sorting file name aaa_20240115_140000.dat (14:00 > 09:00) moving country from USA to IND
('D', 'FLAT_FILE', 'SAME_DAY_MOVE_1', 'Same Day User', '20220101', '20230101', 'GLD', 'AgentSD', 'MH', 'IND', '01011990', 'A', '40001', 'aaa_20240115_140000.dat', 10),
-- Same-country identical timestamp re-send in Batch 2 (later load wins on full tie)
('D', 'FLAT_FILE', 'SAME_CTRY_TIE_1', 'Tie Winner Later Load', '20220101', '20230101', 'GLD', 'AgentCT', 'CA', 'USA', '01011990', 'A', '90210', 'tie_20240115_120000.dat', 5);

-- Resolve ambiguity for RESOLVE_101 before Batch 2 execution
DELETE FROM STG_MEMBER_PROFILE WHERE SOURCE_SYSTEM = 'USA_SYS' AND MEMBER_ID = 'RESOLVE_101';
DELETE FROM TABLE_USA WHERE SOURCE_SYSTEM = 'USA_SYS' AND MEMBER_ID = 'RESOLVE_101';

-- Step 4C: Run production pipeline for Batch 2 via run_all.sql (Run 2: Processes Batch 2)
-- ==================== INLINED 01_DDL.SQL ====================

-- ============================================================================
-- SkyPoints Airline Loyalty Program - Snowflake DDL Architecture
-- Scalable for billions of daily records across flat files and JSON feeds
-- ============================================================================

-- Database and Schema Setup
CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.CORE;
USE SCHEMA SKYPOINTS_DB.CORE;

-- ----------------------------------------------------------------------------
-- 1. RAW LANDING TABLE: Member Profile Flat File
-- Ingests raw pipe-delimited data as VARCHAR to guarantee zero load dropouts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_MEMBER_LANDING (
    RECORD_TYPE          VARCHAR(10),     -- 'H' for Header, 'D' for Detail
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_NAME          VARCHAR(255),
    MEMBER_ID            VARCHAR(50),
    ENROLLMENT_DATE      VARCHAR(50),
    LAST_FLIGHT_DATE     VARCHAR(50),
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    DOB                  VARCHAR(50),
    IS_ACTIVE            VARCHAR(10),
    POST_CODE            VARCHAR(50),
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dead Letter Queue (DLQ) table for quarantined malformed rows
CREATE TABLE IF NOT EXISTS RAW_DLQ_QUARANTINE (
    RECORD_ID            VARCHAR(50),
    RAW_RECORD           VARCHAR,
    RAW_RECORD_HASH      VARCHAR(64),
    ERROR_REASON         VARCHAR(500),
    SOURCE_FEED          VARCHAR(255),
    SOURCE_SYSTEM        VARCHAR(30),
    SOURCE_FILE_NAME     VARCHAR(255),
    BATCH_ID             VARCHAR(50),
    QUARANTINED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. STAGING TABLE: Member Profile with Derived Columns
-- Strongly-typed, normalized dates, plus Age & Stale_Member (> 90 days)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STG_MEMBER_PROFILE (
    STG_ROW_ID          VARCHAR          DEFAULT UUID_STRING(),
    BATCH_ID             VARCHAR(50),
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    POST_CODE            NUMBER(5, 0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1)       DEFAULT 'A',
    AGE                  INTEGER,         -- Derived: Exact calendar age
    STALE_MEMBER         VARCHAR(1),      -- Derived: 'Y' if days since Flight_Date > 90 else 'N'
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
    -- NOTE: Primary key constraints are removed from STG_MEMBER_PROFILE because staging
    -- legitimately holds multiple records per key during ingestion prior to deduplication.
    -- Snowflake does not enforce PRIMARY KEY constraints on country tables or marts.
)
CLUSTER BY (COUNTRY, ENROLLMENT_DATE);

-- ----------------------------------------------------------------------------
-- 3. COUNTRY-SPECIFIC TARGET TABLES
-- Dedicated physical country tables clustered on MEMBER_ID
-- NOTE: Primary key declarations are informational metadata; Snowflake does not
-- enforce primary key uniqueness. Pipeline deduplication and MERGE logic enforce uniqueness.
-- ----------------------------------------------------------------------------

-- Table: India
CREATE TABLE IF NOT EXISTS TABLE_INDIA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'IND',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: USA
CREATE TABLE IF NOT EXISTS TABLE_USA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'USA',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: Australia
CREATE TABLE IF NOT EXISTS TABLE_AU (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'AU',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- ----------------------------------------------------------------------------
-- 4. SEMI-STRUCTURED REDEMPTION TABLES & MART TABLES
-- ----------------------------------------------------------------------------

-- Landing table for raw JSON documents
CREATE TABLE IF NOT EXISTS RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Flattened relational redemptions table
CREATE TABLE IF NOT EXISTS STG_REDEMPTIONS_FLATTENED (
    TXN_ID               VARCHAR(50)      PRIMARY KEY,
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    FEED_DATE            DATE             NOT NULL,
    TXN_DATE             DATE             NOT NULL,
    PARTNER              VARCHAR(100)     NOT NULL,
    MILES_REDEEMED       INTEGER          NOT NULL,
    STATUS               VARCHAR(50)      NOT NULL,
    IS_AMBIGUOUS         BOOLEAN          DEFAULT FALSE,
    BATCH_ID             VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID, TXN_DATE);

-- Analytical mart table
CREATE TABLE IF NOT EXISTS MEMBER_LOYALTY_360 (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255),
    COUNTRY              VARCHAR(50),
    TIER_CODE            VARCHAR(50),
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
-- 5. SCHEMA EVOLUTION & MIGRATION STATEMENTS (FOR PRE-EXISTING SCHEMAS)
-- Safe, idempotent column additions executed BEFORE views are created/updated
-- so existing tables have all required columns before view compilation.
-- ----------------------------------------------------------------------------
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING MODIFY COLUMN TIER_CODE VARCHAR(50);

ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;

ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS RAW_RECORD_HASH VARCHAR(64);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_SYSTEM VARCHAR(30);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS STG_ROW_ID VARCHAR;

ALTER TABLE STG_REDEMPTIONS_FLATTENED ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

-- ----------------------------------------------------------------------------
-- 6. REFERENCE TABLES: Country Table Mapping
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REF_COUNTRY_MAP (
    COUNTRY_CODE         VARCHAR(10)      PRIMARY KEY,
    CANONICAL_CODE       VARCHAR(10)      NOT NULL,
    TARGET_TABLE         VARCHAR(50)      NOT NULL
);

-- Seed Country Mappings (IND/INDIA -> TABLE_INDIA, USA -> TABLE_USA, AU/AUS -> TABLE_AU)
MERGE INTO REF_COUNTRY_MAP target
USING (
    SELECT 'IND' AS code, 'IND' AS canonical, 'TABLE_INDIA' AS tbl UNION ALL
    SELECT 'INDIA', 'IND', 'TABLE_INDIA' UNION ALL
    SELECT 'USA', 'USA', 'TABLE_USA' UNION ALL
    SELECT 'AU', 'AU', 'TABLE_AU' UNION ALL
    SELECT 'AUS', 'AU', 'TABLE_AU'
) src
ON target.COUNTRY_CODE = src.code
WHEN MATCHED THEN
    UPDATE SET target.CANONICAL_CODE = src.canonical, target.TARGET_TABLE = src.tbl
WHEN NOT MATCHED THEN
    INSERT (COUNTRY_CODE, CANONICAL_CODE, TARGET_TABLE) VALUES (src.code, src.canonical, src.tbl);

-- ----------------------------------------------------------------------------
-- 7. UNIFIED COUNTRY MEMBERS VIEW
-- Explicit column list projected across all country tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL AS
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_INDIA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_USA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_AU;

-- ----------------------------------------------------------------------------
-- 8. DYNAMIC UNIFIED MEMBERS VIEW (Query-Time Recalculation)
-- Dynamically recomputes AGE and STALE_MEMBER at query time relative to CURRENT_DATE()
-- while preserving static stored attributes in the country tables.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL_CURRENT AS
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
    -- Dynamically recomputed exact calendar age at query time
    CASE
        WHEN DOB IS NULL THEN NULL
        ELSE DATEDIFF(year, DOB, CURRENT_DATE()) - 
             CASE 
                WHEN (MONTH(CURRENT_DATE()) < MONTH(DOB)) 
                  OR (MONTH(CURRENT_DATE()) = MONTH(DOB) 
                      AND DAY(CURRENT_DATE()) < DAY(DOB)) 
                THEN 1 ELSE 0 
             END
    END AS AGE,
    -- Dynamically recomputed stale member flag at query time (>90 days since flight)
    CASE 
        WHEN LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, LAST_FLIGHT_DATE, CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    EXTRACT_TS,
    SOURCE_FILE_NAME,
    LINE_NO,
    LAST_UPDATED_AT
FROM MEMBERS_ALL;

-- ----------------------------------------------------------------------------
-- 9. DYNAMIC ANALYTICAL MART: Member Loyalty 360 Current View
-- Reads stored redemption totals from MEMBER_LOYALTY_360 (the mart), which are
-- refreshed each run by the pipeline, and recomputes only Age and Stale_Member
-- dynamically relative to CURRENT_DATE() using MEMBERS_ALL_CURRENT.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBER_LOYALTY_360_CURRENT AS
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
    COALESCE(l.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
    COALESCE(l.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
    COALESCE(l.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
    l.LAST_REDEMPTION_DATE,
    COALESCE(l.TOP_PARTNER, 'None') AS TOP_PARTNER,
    CURRENT_TIMESTAMP() AS LAST_SYNC_AT
FROM MEMBERS_ALL_CURRENT m
LEFT JOIN MEMBER_LOYALTY_360 l
    ON m.SOURCE_SYSTEM = l.SOURCE_SYSTEM AND m.MEMBER_ID = l.MEMBER_ID;


-- ==================== INLINED 03_TRANSFORMATIONS.SQL ====================

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
SET run_batch_id = (SELECT UUID_STRING());

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
        WHEN m.CANONICAL_CODE IS NULL THEN 'Unmapped country'
        WHEN LENGTH(s.COUNTRY) > 5 THEN 'Country exceeds 5 characters'
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
    EXTRACT_TS,
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
        COALESCE(
            EXTRACT_TS,
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}[_ -]?[0-9]{6}'), 'YYYYMMDD_HH24MISS'),
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}'), 'YYYYMMDD'),
            INGESTED_AT,
            CURRENT_TIMESTAMP()
        ) AS EXTRACT_TS,
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
    s.EXTRACT_TS,
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
                ORDER BY s.EXTRACT_TS DESC,
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
        EXTRACT_TS,
        LINE_NO,
        INGESTED_AT,
        STG_ROW_ID,
        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, MEMBER_ID 
            ORDER BY EXTRACT_TS DESC,
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
    EXTRACT_TS,
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
--   sql/snowflake/04_country_merges.generated.sql
--
-- In SnowSQL CLI, execute:
-- !source sql/snowflake/04_country_merges.generated.sql
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- STEP 3C: Audit Out-of-Order / Superseded Records into RAW_DLQ_QUARANTINE
-- Captures late-arriving older records that are blocked from overwriting newer core records
-- ----------------------------------------------------------------------------
MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        src.MEMBER_ID AS RECORD_ID,
        CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO), 256) AS RAW_RECORD_HASH,
        'Superseded by existing newer record in core table' AS ERROR_REASON,
        'DEDUPED_LATEST_MEMBERS' AS SOURCE_FEED,
        src.SOURCE_SYSTEM,
        src.SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM DEDUPED_LATEST_MEMBERS src
    JOIN MEMBERS_ALL m ON src.SOURCE_SYSTEM = m.SOURCE_SYSTEM AND src.MEMBER_ID = m.MEMBER_ID
    WHERE (
        m.EXTRACT_TS > src.EXTRACT_TS
        OR (
            m.EXTRACT_TS = src.EXTRACT_TS
            AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.value:txn_id::STRING ORDER BY raw.INGESTED_AT DESC) = 1
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



-- ==================== INLINED 04_COUNTRY_MERGES.SQL ====================

-- ============================================================================
-- Static Country MERGE & Migration DELETE Statements for Snowflake
-- Generated automatically by scripts/generate_country_merges.py
-- Single source of truth: CANONICAL_TABLE_MAP in src/country_router.py
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Target Country: AU -> TABLE_AU
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_AU target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'AU'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_AU if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_AU target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'AU'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: IND -> TABLE_INDIA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_INDIA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'IND'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_INDIA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_INDIA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'IND'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: USA -> TABLE_USA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_USA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'USA'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_USA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_USA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'USA'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);



-- ==================== INLINED 05_MART_AND_FINALIZE.SQL ====================

-- ============================================================================
-- SkyPoints Step 3: Analytical Mart Build & Finalize Landing Batch Tracking
-- NOTE: run_batch_id is a session variable initialized in transformations.sql.
-- All four scripts must execute in the same SnowSQL session in order.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 5: Loyalty 360 Analytical Mart (Sums Only COMPLETED Miles)
-- ----------------------------------------------------------------------------

-- Snapshot pre-update status (column renamed so it can never clash with the live table)
CREATE OR REPLACE TEMPORARY TABLE PRE_UPDATE_REDEMPTION_STATUS AS
SELECT TXN_ID, IS_AMBIGUOUS AS PREV_IS_AMBIGUOUS
FROM STG_REDEMPTIONS_FLATTENED;

-- Member IDs that appear in more than one source system
CREATE OR REPLACE TEMPORARY TABLE AMBIGUOUS_MEMBER_IDS AS
SELECT m.MEMBER_ID
FROM MEMBERS_ALL_CURRENT m
GROUP BY m.MEMBER_ID
HAVING COUNT(DISTINCT m.SOURCE_SYSTEM) > 1;

-- Flag redemptions (no correlated view subquery)
UPDATE STG_REDEMPTIONS_FLATTENED AS red
SET IS_AMBIGUOUS = IFF(
    red.MEMBER_ID IN (SELECT a.MEMBER_ID FROM AMBIGUOUS_MEMBER_IDS a),
    TRUE,
    FALSE
);

-- Members whose ambiguity flipped in this run
CREATE OR REPLACE TEMPORARY TABLE AMBIGUITY_CHANGED_MEMBERS AS
SELECT DISTINCT cur.MEMBER_ID
FROM STG_REDEMPTIONS_FLATTENED cur
JOIN PRE_UPDATE_REDEMPTION_STATUS prev ON cur.TXN_ID = prev.TXN_ID
WHERE cur.IS_AMBIGUOUS <> prev.PREV_IS_AMBIGUOUS;

MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        red.TXN_ID AS RECORD_ID,
        CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS), 256) AS RAW_RECORD_HASH,
        'Ambiguous member_id across source systems' AS ERROR_REASON,
        'STG_REDEMPTIONS_FLATTENED' AS SOURCE_FEED,
        'PARTNER_JSON' AS SOURCE_SYSTEM,
        'stg_redemptions' AS SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM STG_REDEMPTIONS_FLATTENED red
    WHERE red.IS_AMBIGUOUS = TRUE
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
AND target.RAW_RECORD_HASH = src.RAW_RECORD_HASH
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, RAW_RECORD_HASH, ERROR_REASON, SOURCE_FEED, SOURCE_SYSTEM, SOURCE_FILE_NAME, BATCH_ID, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.RAW_RECORD_HASH, src.ERROR_REASON, src.SOURCE_FEED, src.SOURCE_SYSTEM, src.SOURCE_FILE_NAME, src.BATCH_ID, CURRENT_TIMESTAMP());

-- Remove resolved redemptions from DLQ when ambiguity disappears
DELETE FROM RAW_DLQ_QUARANTINE
WHERE SOURCE_FEED = 'STG_REDEMPTIONS_FLATTENED'
  AND ERROR_REASON = 'Ambiguous member_id across source systems'
  AND RECORD_ID IN (
      SELECT red.TXN_ID FROM STG_REDEMPTIONS_FLATTENED red WHERE red.IS_AMBIGUOUS = FALSE
  );

-- 2. Build or Refresh MEMBER_LOYALTY_360 Mart (Idempotent MERGE for affected members)
-- Keyed on (SOURCE_SYSTEM, MEMBER_ID) from MEMBERS_ALL_CURRENT; touches only members affected by this batch,
-- including members whose redemptions were previously ambiguous and are now resolved.
-- Excludes IS_AMBIGUOUS = TRUE transactions from analytical calculations.
-- Breaks MODE(PARTNER) ties deterministically by highest count then alphabetical.
MERGE INTO MEMBER_LOYALTY_360 target
USING (
    WITH TouchedMembers AS (
        -- 1. Members touched in current batch
        SELECT SOURCE_SYSTEM, MEMBER_ID
        FROM DEDUPED_LATEST_MEMBERS
        UNION
        -- 2. Members with incoming redemptions in current batch
        SELECT m.SOURCE_SYSTEM, red.MEMBER_ID
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN MEMBERS_ALL_CURRENT m ON red.MEMBER_ID = m.MEMBER_ID
        WHERE red.BATCH_ID = $run_batch_id
        UNION
        -- 3. Members whose redemptions status flipped (became ambiguous or became resolved)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        JOIN AMBIGUITY_CHANGED_MEMBERS acm ON m.MEMBER_ID = acm.MEMBER_ID
        UNION
        -- 4. Members with redemptions whose member profile in MEMBERS_ALL spans multiple source systems (ambiguity detected)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        WHERE m.MEMBER_ID IN (
            SELECT MEMBER_ID FROM MEMBERS_ALL_CURRENT GROUP BY MEMBER_ID HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
        )
    ),
    PartnerCounts AS (
        SELECT
            red.MEMBER_ID,
            red.PARTNER,
            COUNT(*) AS CNT,
            ROW_NUMBER() OVER (PARTITION BY red.MEMBER_ID ORDER BY COUNT(*) DESC, red.PARTNER ASC) AS RN
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE AND red.PARTNER IS NOT NULL
        GROUP BY red.MEMBER_ID, red.PARTNER
    ),
    TopPartner AS (
        SELECT MEMBER_ID, PARTNER AS TOP_PARTNER
        FROM PartnerCounts
        WHERE RN = 1
    ),
    RedemptionSummary AS (
        SELECT
            red.MEMBER_ID,
            COUNT(DISTINCT red.TXN_ID) AS TOTAL_REDEMPTIONS,
            COALESCE(SUM(CASE WHEN red.STATUS = 'COMPLETED' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS TOTAL_MILES_REDEEMED,
            COALESCE(SUM(CASE WHEN red.STATUS = 'PENDING' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS PENDING_MILES_REDEEMED,
            MAX(red.TXN_DATE) AS LAST_REDEMPTION_DATE
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE
        GROUP BY red.MEMBER_ID
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
        COALESCE(rs.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
        COALESCE(rs.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
        COALESCE(rs.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
        rs.LAST_REDEMPTION_DATE,
        COALESCE(tp.TOP_PARTNER, 'None') AS TOP_PARTNER
    FROM MEMBERS_ALL_CURRENT m
    JOIN TouchedMembers tm ON m.SOURCE_SYSTEM = tm.SOURCE_SYSTEM AND m.MEMBER_ID = tm.MEMBER_ID
    LEFT JOIN RedemptionSummary rs ON m.MEMBER_ID = rs.MEMBER_ID
    LEFT JOIN TopPartner tp ON m.MEMBER_ID = tp.MEMBER_ID
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

-- Remove members from mart that no longer exist in unified current members view (e.g. cross-country moves or hard deletes)
DELETE FROM MEMBER_LOYALTY_360
WHERE (SOURCE_SYSTEM, MEMBER_ID) NOT IN (
    SELECT SOURCE_SYSTEM, MEMBER_ID FROM MEMBERS_ALL_CURRENT
);


-- ----------------------------------------------------------------------------
-- STEP 6: Mark Processed Records in Landing Tables for Batch Tracking
-- Stamps ONLY the records claimed and processed by this specific batch
-- ----------------------------------------------------------------------------
UPDATE RAW_MEMBER_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;

UPDATE RAW_REDEMPTIONS_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;


-- Step 5: Capture snapshot of mart and table counts BEFORE rerun into temporary table
CREATE OR REPLACE TEMPORARY TABLE PRE_RERUN_SNAPSHOT AS
SELECT
    0 AS STG_MEMBER_COUNT, -- Staging is per-batch (P6); empty rerun batch produces 0 staging rows
    (SELECT COUNT(*) FROM RAW_DLQ_QUARANTINE) AS DLQ_COUNT,
    (SELECT COUNT(*) FROM STG_REDEMPTIONS_FLATTENED) AS STG_REDEMP_COUNT,
    (SELECT COUNT(*) FROM MEMBER_LOYALTY_360) AS MART_COUNT,
    (SELECT COUNT(*) FROM MEMBER_LOYALTY_360 WHERE MEMBER_ID = 'MOVE_101' AND COUNTRY = 'IND') AS MOVE_101_COUNTRY_CORRECT,
    (SELECT TO_VARCHAR(COALESCE(TOTAL_MILES_REDEEMED, 0)) FROM MEMBER_LOYALTY_360 WHERE MEMBER_ID = 'MOVE_101') AS MOVE_101_COMPLETED_MILES,
    (SELECT TO_VARCHAR(COALESCE(PENDING_MILES_REDEEMED, 0)) FROM MEMBER_LOYALTY_360 WHERE MEMBER_ID = 'MOVE_101') AS MOVE_101_PENDING_MILES;

-- Step 5B: Execute Idempotent Rerun via run_all.sql (Run 3: Empty idempotent rerun)
-- ==================== INLINED 01_DDL.SQL ====================

-- ============================================================================
-- SkyPoints Airline Loyalty Program - Snowflake DDL Architecture
-- Scalable for billions of daily records across flat files and JSON feeds
-- ============================================================================

-- Database and Schema Setup
CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.CORE;
USE SCHEMA SKYPOINTS_DB.CORE;

-- ----------------------------------------------------------------------------
-- 1. RAW LANDING TABLE: Member Profile Flat File
-- Ingests raw pipe-delimited data as VARCHAR to guarantee zero load dropouts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_MEMBER_LANDING (
    RECORD_TYPE          VARCHAR(10),     -- 'H' for Header, 'D' for Detail
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_NAME          VARCHAR(255),
    MEMBER_ID            VARCHAR(50),
    ENROLLMENT_DATE      VARCHAR(50),
    LAST_FLIGHT_DATE     VARCHAR(50),
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    DOB                  VARCHAR(50),
    IS_ACTIVE            VARCHAR(10),
    POST_CODE            VARCHAR(50),
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dead Letter Queue (DLQ) table for quarantined malformed rows
CREATE TABLE IF NOT EXISTS RAW_DLQ_QUARANTINE (
    RECORD_ID            VARCHAR(50),
    RAW_RECORD           VARCHAR,
    RAW_RECORD_HASH      VARCHAR(64),
    ERROR_REASON         VARCHAR(500),
    SOURCE_FEED          VARCHAR(255),
    SOURCE_SYSTEM        VARCHAR(30),
    SOURCE_FILE_NAME     VARCHAR(255),
    BATCH_ID             VARCHAR(50),
    QUARANTINED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. STAGING TABLE: Member Profile with Derived Columns
-- Strongly-typed, normalized dates, plus Age & Stale_Member (> 90 days)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STG_MEMBER_PROFILE (
    STG_ROW_ID          VARCHAR          DEFAULT UUID_STRING(),
    BATCH_ID             VARCHAR(50),
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    POST_CODE            NUMBER(5, 0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1)       DEFAULT 'A',
    AGE                  INTEGER,         -- Derived: Exact calendar age
    STALE_MEMBER         VARCHAR(1),      -- Derived: 'Y' if days since Flight_Date > 90 else 'N'
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
    -- NOTE: Primary key constraints are removed from STG_MEMBER_PROFILE because staging
    -- legitimately holds multiple records per key during ingestion prior to deduplication.
    -- Snowflake does not enforce PRIMARY KEY constraints on country tables or marts.
)
CLUSTER BY (COUNTRY, ENROLLMENT_DATE);

-- ----------------------------------------------------------------------------
-- 3. COUNTRY-SPECIFIC TARGET TABLES
-- Dedicated physical country tables clustered on MEMBER_ID
-- NOTE: Primary key declarations are informational metadata; Snowflake does not
-- enforce primary key uniqueness. Pipeline deduplication and MERGE logic enforce uniqueness.
-- ----------------------------------------------------------------------------

-- Table: India
CREATE TABLE IF NOT EXISTS TABLE_INDIA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'IND',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: USA
CREATE TABLE IF NOT EXISTS TABLE_USA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'USA',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: Australia
CREATE TABLE IF NOT EXISTS TABLE_AU (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'AU',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- ----------------------------------------------------------------------------
-- 4. SEMI-STRUCTURED REDEMPTION TABLES & MART TABLES
-- ----------------------------------------------------------------------------

-- Landing table for raw JSON documents
CREATE TABLE IF NOT EXISTS RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Flattened relational redemptions table
CREATE TABLE IF NOT EXISTS STG_REDEMPTIONS_FLATTENED (
    TXN_ID               VARCHAR(50)      PRIMARY KEY,
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    FEED_DATE            DATE             NOT NULL,
    TXN_DATE             DATE             NOT NULL,
    PARTNER              VARCHAR(100)     NOT NULL,
    MILES_REDEEMED       INTEGER          NOT NULL,
    STATUS               VARCHAR(50)      NOT NULL,
    IS_AMBIGUOUS         BOOLEAN          DEFAULT FALSE,
    BATCH_ID             VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID, TXN_DATE);

-- Analytical mart table
CREATE TABLE IF NOT EXISTS MEMBER_LOYALTY_360 (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255),
    COUNTRY              VARCHAR(50),
    TIER_CODE            VARCHAR(50),
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
-- 5. SCHEMA EVOLUTION & MIGRATION STATEMENTS (FOR PRE-EXISTING SCHEMAS)
-- Safe, idempotent column additions executed BEFORE views are created/updated
-- so existing tables have all required columns before view compilation.
-- ----------------------------------------------------------------------------
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING MODIFY COLUMN TIER_CODE VARCHAR(50);

ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;

ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS RAW_RECORD_HASH VARCHAR(64);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_SYSTEM VARCHAR(30);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS STG_ROW_ID VARCHAR;

ALTER TABLE STG_REDEMPTIONS_FLATTENED ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

-- ----------------------------------------------------------------------------
-- 6. REFERENCE TABLES: Country Table Mapping
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REF_COUNTRY_MAP (
    COUNTRY_CODE         VARCHAR(10)      PRIMARY KEY,
    CANONICAL_CODE       VARCHAR(10)      NOT NULL,
    TARGET_TABLE         VARCHAR(50)      NOT NULL
);

-- Seed Country Mappings (IND/INDIA -> TABLE_INDIA, USA -> TABLE_USA, AU/AUS -> TABLE_AU)
MERGE INTO REF_COUNTRY_MAP target
USING (
    SELECT 'IND' AS code, 'IND' AS canonical, 'TABLE_INDIA' AS tbl UNION ALL
    SELECT 'INDIA', 'IND', 'TABLE_INDIA' UNION ALL
    SELECT 'USA', 'USA', 'TABLE_USA' UNION ALL
    SELECT 'AU', 'AU', 'TABLE_AU' UNION ALL
    SELECT 'AUS', 'AU', 'TABLE_AU'
) src
ON target.COUNTRY_CODE = src.code
WHEN MATCHED THEN
    UPDATE SET target.CANONICAL_CODE = src.canonical, target.TARGET_TABLE = src.tbl
WHEN NOT MATCHED THEN
    INSERT (COUNTRY_CODE, CANONICAL_CODE, TARGET_TABLE) VALUES (src.code, src.canonical, src.tbl);

-- ----------------------------------------------------------------------------
-- 7. UNIFIED COUNTRY MEMBERS VIEW
-- Explicit column list projected across all country tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL AS
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_INDIA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_USA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_AU;

-- ----------------------------------------------------------------------------
-- 8. DYNAMIC UNIFIED MEMBERS VIEW (Query-Time Recalculation)
-- Dynamically recomputes AGE and STALE_MEMBER at query time relative to CURRENT_DATE()
-- while preserving static stored attributes in the country tables.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL_CURRENT AS
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
    -- Dynamically recomputed exact calendar age at query time
    CASE
        WHEN DOB IS NULL THEN NULL
        ELSE DATEDIFF(year, DOB, CURRENT_DATE()) - 
             CASE 
                WHEN (MONTH(CURRENT_DATE()) < MONTH(DOB)) 
                  OR (MONTH(CURRENT_DATE()) = MONTH(DOB) 
                      AND DAY(CURRENT_DATE()) < DAY(DOB)) 
                THEN 1 ELSE 0 
             END
    END AS AGE,
    -- Dynamically recomputed stale member flag at query time (>90 days since flight)
    CASE 
        WHEN LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, LAST_FLIGHT_DATE, CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    EXTRACT_TS,
    SOURCE_FILE_NAME,
    LINE_NO,
    LAST_UPDATED_AT
FROM MEMBERS_ALL;

-- ----------------------------------------------------------------------------
-- 9. DYNAMIC ANALYTICAL MART: Member Loyalty 360 Current View
-- Reads stored redemption totals from MEMBER_LOYALTY_360 (the mart), which are
-- refreshed each run by the pipeline, and recomputes only Age and Stale_Member
-- dynamically relative to CURRENT_DATE() using MEMBERS_ALL_CURRENT.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBER_LOYALTY_360_CURRENT AS
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
    COALESCE(l.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
    COALESCE(l.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
    COALESCE(l.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
    l.LAST_REDEMPTION_DATE,
    COALESCE(l.TOP_PARTNER, 'None') AS TOP_PARTNER,
    CURRENT_TIMESTAMP() AS LAST_SYNC_AT
FROM MEMBERS_ALL_CURRENT m
LEFT JOIN MEMBER_LOYALTY_360 l
    ON m.SOURCE_SYSTEM = l.SOURCE_SYSTEM AND m.MEMBER_ID = l.MEMBER_ID;


-- ==================== INLINED 03_TRANSFORMATIONS.SQL ====================

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
SET run_batch_id = (SELECT UUID_STRING());

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
        WHEN m.CANONICAL_CODE IS NULL THEN 'Unmapped country'
        WHEN LENGTH(s.COUNTRY) > 5 THEN 'Country exceeds 5 characters'
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
    EXTRACT_TS,
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
        COALESCE(
            EXTRACT_TS,
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}[_ -]?[0-9]{6}'), 'YYYYMMDD_HH24MISS'),
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}'), 'YYYYMMDD'),
            INGESTED_AT,
            CURRENT_TIMESTAMP()
        ) AS EXTRACT_TS,
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
    s.EXTRACT_TS,
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
                ORDER BY s.EXTRACT_TS DESC,
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
        EXTRACT_TS,
        LINE_NO,
        INGESTED_AT,
        STG_ROW_ID,
        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, MEMBER_ID 
            ORDER BY EXTRACT_TS DESC,
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
    EXTRACT_TS,
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
--   sql/snowflake/04_country_merges.generated.sql
--
-- In SnowSQL CLI, execute:
-- !source sql/snowflake/04_country_merges.generated.sql
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- STEP 3C: Audit Out-of-Order / Superseded Records into RAW_DLQ_QUARANTINE
-- Captures late-arriving older records that are blocked from overwriting newer core records
-- ----------------------------------------------------------------------------
MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        src.MEMBER_ID AS RECORD_ID,
        CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO), 256) AS RAW_RECORD_HASH,
        'Superseded by existing newer record in core table' AS ERROR_REASON,
        'DEDUPED_LATEST_MEMBERS' AS SOURCE_FEED,
        src.SOURCE_SYSTEM,
        src.SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM DEDUPED_LATEST_MEMBERS src
    JOIN MEMBERS_ALL m ON src.SOURCE_SYSTEM = m.SOURCE_SYSTEM AND src.MEMBER_ID = m.MEMBER_ID
    WHERE (
        m.EXTRACT_TS > src.EXTRACT_TS
        OR (
            m.EXTRACT_TS = src.EXTRACT_TS
            AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.value:txn_id::STRING ORDER BY raw.INGESTED_AT DESC) = 1
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



-- ==================== INLINED 04_COUNTRY_MERGES.SQL ====================

-- ============================================================================
-- Static Country MERGE & Migration DELETE Statements for Snowflake
-- Generated automatically by scripts/generate_country_merges.py
-- Single source of truth: CANONICAL_TABLE_MAP in src/country_router.py
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Target Country: AU -> TABLE_AU
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_AU target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'AU'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_AU if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_AU target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'AU'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: IND -> TABLE_INDIA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_INDIA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'IND'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_INDIA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_INDIA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'IND'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: USA -> TABLE_USA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_USA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'USA'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_USA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_USA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'USA'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);



-- ==================== INLINED 05_MART_AND_FINALIZE.SQL ====================

-- ============================================================================
-- SkyPoints Step 3: Analytical Mart Build & Finalize Landing Batch Tracking
-- NOTE: run_batch_id is a session variable initialized in transformations.sql.
-- All four scripts must execute in the same SnowSQL session in order.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 5: Loyalty 360 Analytical Mart (Sums Only COMPLETED Miles)
-- ----------------------------------------------------------------------------

-- Snapshot pre-update status (column renamed so it can never clash with the live table)
CREATE OR REPLACE TEMPORARY TABLE PRE_UPDATE_REDEMPTION_STATUS AS
SELECT TXN_ID, IS_AMBIGUOUS AS PREV_IS_AMBIGUOUS
FROM STG_REDEMPTIONS_FLATTENED;

-- Member IDs that appear in more than one source system
CREATE OR REPLACE TEMPORARY TABLE AMBIGUOUS_MEMBER_IDS AS
SELECT m.MEMBER_ID
FROM MEMBERS_ALL_CURRENT m
GROUP BY m.MEMBER_ID
HAVING COUNT(DISTINCT m.SOURCE_SYSTEM) > 1;

-- Flag redemptions (no correlated view subquery)
UPDATE STG_REDEMPTIONS_FLATTENED AS red
SET IS_AMBIGUOUS = IFF(
    red.MEMBER_ID IN (SELECT a.MEMBER_ID FROM AMBIGUOUS_MEMBER_IDS a),
    TRUE,
    FALSE
);

-- Members whose ambiguity flipped in this run
CREATE OR REPLACE TEMPORARY TABLE AMBIGUITY_CHANGED_MEMBERS AS
SELECT DISTINCT cur.MEMBER_ID
FROM STG_REDEMPTIONS_FLATTENED cur
JOIN PRE_UPDATE_REDEMPTION_STATUS prev ON cur.TXN_ID = prev.TXN_ID
WHERE cur.IS_AMBIGUOUS <> prev.PREV_IS_AMBIGUOUS;

MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        red.TXN_ID AS RECORD_ID,
        CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS), 256) AS RAW_RECORD_HASH,
        'Ambiguous member_id across source systems' AS ERROR_REASON,
        'STG_REDEMPTIONS_FLATTENED' AS SOURCE_FEED,
        'PARTNER_JSON' AS SOURCE_SYSTEM,
        'stg_redemptions' AS SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM STG_REDEMPTIONS_FLATTENED red
    WHERE red.IS_AMBIGUOUS = TRUE
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
AND target.RAW_RECORD_HASH = src.RAW_RECORD_HASH
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, RAW_RECORD_HASH, ERROR_REASON, SOURCE_FEED, SOURCE_SYSTEM, SOURCE_FILE_NAME, BATCH_ID, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.RAW_RECORD_HASH, src.ERROR_REASON, src.SOURCE_FEED, src.SOURCE_SYSTEM, src.SOURCE_FILE_NAME, src.BATCH_ID, CURRENT_TIMESTAMP());

-- Remove resolved redemptions from DLQ when ambiguity disappears
DELETE FROM RAW_DLQ_QUARANTINE
WHERE SOURCE_FEED = 'STG_REDEMPTIONS_FLATTENED'
  AND ERROR_REASON = 'Ambiguous member_id across source systems'
  AND RECORD_ID IN (
      SELECT red.TXN_ID FROM STG_REDEMPTIONS_FLATTENED red WHERE red.IS_AMBIGUOUS = FALSE
  );

-- 2. Build or Refresh MEMBER_LOYALTY_360 Mart (Idempotent MERGE for affected members)
-- Keyed on (SOURCE_SYSTEM, MEMBER_ID) from MEMBERS_ALL_CURRENT; touches only members affected by this batch,
-- including members whose redemptions were previously ambiguous and are now resolved.
-- Excludes IS_AMBIGUOUS = TRUE transactions from analytical calculations.
-- Breaks MODE(PARTNER) ties deterministically by highest count then alphabetical.
MERGE INTO MEMBER_LOYALTY_360 target
USING (
    WITH TouchedMembers AS (
        -- 1. Members touched in current batch
        SELECT SOURCE_SYSTEM, MEMBER_ID
        FROM DEDUPED_LATEST_MEMBERS
        UNION
        -- 2. Members with incoming redemptions in current batch
        SELECT m.SOURCE_SYSTEM, red.MEMBER_ID
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN MEMBERS_ALL_CURRENT m ON red.MEMBER_ID = m.MEMBER_ID
        WHERE red.BATCH_ID = $run_batch_id
        UNION
        -- 3. Members whose redemptions status flipped (became ambiguous or became resolved)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        JOIN AMBIGUITY_CHANGED_MEMBERS acm ON m.MEMBER_ID = acm.MEMBER_ID
        UNION
        -- 4. Members with redemptions whose member profile in MEMBERS_ALL spans multiple source systems (ambiguity detected)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        WHERE m.MEMBER_ID IN (
            SELECT MEMBER_ID FROM MEMBERS_ALL_CURRENT GROUP BY MEMBER_ID HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
        )
    ),
    PartnerCounts AS (
        SELECT
            red.MEMBER_ID,
            red.PARTNER,
            COUNT(*) AS CNT,
            ROW_NUMBER() OVER (PARTITION BY red.MEMBER_ID ORDER BY COUNT(*) DESC, red.PARTNER ASC) AS RN
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE AND red.PARTNER IS NOT NULL
        GROUP BY red.MEMBER_ID, red.PARTNER
    ),
    TopPartner AS (
        SELECT MEMBER_ID, PARTNER AS TOP_PARTNER
        FROM PartnerCounts
        WHERE RN = 1
    ),
    RedemptionSummary AS (
        SELECT
            red.MEMBER_ID,
            COUNT(DISTINCT red.TXN_ID) AS TOTAL_REDEMPTIONS,
            COALESCE(SUM(CASE WHEN red.STATUS = 'COMPLETED' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS TOTAL_MILES_REDEEMED,
            COALESCE(SUM(CASE WHEN red.STATUS = 'PENDING' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS PENDING_MILES_REDEEMED,
            MAX(red.TXN_DATE) AS LAST_REDEMPTION_DATE
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE
        GROUP BY red.MEMBER_ID
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
        COALESCE(rs.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
        COALESCE(rs.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
        COALESCE(rs.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
        rs.LAST_REDEMPTION_DATE,
        COALESCE(tp.TOP_PARTNER, 'None') AS TOP_PARTNER
    FROM MEMBERS_ALL_CURRENT m
    JOIN TouchedMembers tm ON m.SOURCE_SYSTEM = tm.SOURCE_SYSTEM AND m.MEMBER_ID = tm.MEMBER_ID
    LEFT JOIN RedemptionSummary rs ON m.MEMBER_ID = rs.MEMBER_ID
    LEFT JOIN TopPartner tp ON m.MEMBER_ID = tp.MEMBER_ID
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

-- Remove members from mart that no longer exist in unified current members view (e.g. cross-country moves or hard deletes)
DELETE FROM MEMBER_LOYALTY_360
WHERE (SOURCE_SYSTEM, MEMBER_ID) NOT IN (
    SELECT SOURCE_SYSTEM, MEMBER_ID FROM MEMBERS_ALL_CURRENT
);


-- ----------------------------------------------------------------------------
-- STEP 6: Mark Processed Records in Landing Tables for Batch Tracking
-- Stamps ONLY the records claimed and processed by this specific batch
-- ----------------------------------------------------------------------------
UPDATE RAW_MEMBER_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;

UPDATE RAW_REDEMPTIONS_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;


-- Capture snapshot of counts right after empty rerun to verify zero delta
CREATE OR REPLACE TEMPORARY TABLE POST_RERUN_SNAPSHOT AS
SELECT
    (SELECT COUNT(*) FROM STG_MEMBER_PROFILE) AS STG_MEMBER_COUNT,
    (SELECT COUNT(*) FROM RAW_DLQ_QUARANTINE) AS DLQ_COUNT,
    (SELECT COUNT(*) FROM STG_REDEMPTIONS_FLATTENED) AS STG_REDEMP_COUNT,
    (SELECT COUNT(*) FROM MEMBER_LOYALTY_360) AS MART_COUNT;

-- Step 5C: Simulate in-flight arrival landing after batch finalization (BATCH_ID IS NULL)
INSERT INTO RAW_MEMBER_LANDING (
    RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE, SOURCE_FILE_NAME
) VALUES
('D', 'FLAT_FILE', 'INFLIGHT_1', 'Inflight User', '20240101', '20240101', 'GLD', 'AgentN', 'CA', 'USA', '01011990', 'A', '90210', 'inflight_arrival.dat');

-- Run 4: Claims and processes in-flight record
-- ==================== INLINED 01_DDL.SQL ====================

-- ============================================================================
-- SkyPoints Airline Loyalty Program - Snowflake DDL Architecture
-- Scalable for billions of daily records across flat files and JSON feeds
-- ============================================================================

-- Database and Schema Setup
CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.CORE;
USE SCHEMA SKYPOINTS_DB.CORE;

-- ----------------------------------------------------------------------------
-- 1. RAW LANDING TABLE: Member Profile Flat File
-- Ingests raw pipe-delimited data as VARCHAR to guarantee zero load dropouts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_MEMBER_LANDING (
    RECORD_TYPE          VARCHAR(10),     -- 'H' for Header, 'D' for Detail
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_NAME          VARCHAR(255),
    MEMBER_ID            VARCHAR(50),
    ENROLLMENT_DATE      VARCHAR(50),
    LAST_FLIGHT_DATE     VARCHAR(50),
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    DOB                  VARCHAR(50),
    IS_ACTIVE            VARCHAR(10),
    POST_CODE            VARCHAR(50),
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dead Letter Queue (DLQ) table for quarantined malformed rows
CREATE TABLE IF NOT EXISTS RAW_DLQ_QUARANTINE (
    RECORD_ID            VARCHAR(50),
    RAW_RECORD           VARCHAR,
    RAW_RECORD_HASH      VARCHAR(64),
    ERROR_REASON         VARCHAR(500),
    SOURCE_FEED          VARCHAR(255),
    SOURCE_SYSTEM        VARCHAR(30),
    SOURCE_FILE_NAME     VARCHAR(255),
    BATCH_ID             VARCHAR(50),
    QUARANTINED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. STAGING TABLE: Member Profile with Derived Columns
-- Strongly-typed, normalized dates, plus Age & Stale_Member (> 90 days)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STG_MEMBER_PROFILE (
    STG_ROW_ID          VARCHAR          DEFAULT UUID_STRING(),
    BATCH_ID             VARCHAR(50),
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    POST_CODE            NUMBER(5, 0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1)       DEFAULT 'A',
    AGE                  INTEGER,         -- Derived: Exact calendar age
    STALE_MEMBER         VARCHAR(1),      -- Derived: 'Y' if days since Flight_Date > 90 else 'N'
    SOURCE_FILE_NAME     VARCHAR(255),
    EXTRACT_TS           TIMESTAMP_NTZ,
    LINE_NO              NUMBER,
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
    -- NOTE: Primary key constraints are removed from STG_MEMBER_PROFILE because staging
    -- legitimately holds multiple records per key during ingestion prior to deduplication.
    -- Snowflake does not enforce PRIMARY KEY constraints on country tables or marts.
)
CLUSTER BY (COUNTRY, ENROLLMENT_DATE);

-- ----------------------------------------------------------------------------
-- 3. COUNTRY-SPECIFIC TARGET TABLES
-- Dedicated physical country tables clustered on MEMBER_ID
-- NOTE: Primary key declarations are informational metadata; Snowflake does not
-- enforce primary key uniqueness. Pipeline deduplication and MERGE logic enforce uniqueness.
-- ----------------------------------------------------------------------------

-- Table: India
CREATE TABLE IF NOT EXISTS TABLE_INDIA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'IND',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: USA
CREATE TABLE IF NOT EXISTS TABLE_USA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'USA',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: Australia
CREATE TABLE IF NOT EXISTS TABLE_AU (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(50),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'AU',
    POST_CODE            NUMBER(5,0),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    EXTRACT_TS           TIMESTAMP_NTZ,
    SOURCE_FILE_NAME     VARCHAR(255),
    LINE_NO              NUMBER,
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- ----------------------------------------------------------------------------
-- 4. SEMI-STRUCTURED REDEMPTION TABLES & MART TABLES
-- ----------------------------------------------------------------------------

-- Landing table for raw JSON documents
CREATE TABLE IF NOT EXISTS RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
    BATCH_ID             VARCHAR(50),
    PROCESSED_AT         TIMESTAMP_NTZ,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Flattened relational redemptions table
CREATE TABLE IF NOT EXISTS STG_REDEMPTIONS_FLATTENED (
    TXN_ID               VARCHAR(50)      PRIMARY KEY,
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    FEED_DATE            DATE             NOT NULL,
    TXN_DATE             DATE             NOT NULL,
    PARTNER              VARCHAR(100)     NOT NULL,
    MILES_REDEEMED       INTEGER          NOT NULL,
    STATUS               VARCHAR(50)      NOT NULL,
    IS_AMBIGUOUS         BOOLEAN          DEFAULT FALSE,
    BATCH_ID             VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID, TXN_DATE);

-- Analytical mart table
CREATE TABLE IF NOT EXISTS MEMBER_LOYALTY_360 (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255),
    COUNTRY              VARCHAR(50),
    TIER_CODE            VARCHAR(50),
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
-- 5. SCHEMA EVOLUTION & MIGRATION STATEMENTS (FOR PRE-EXISTING SCHEMAS)
-- Safe, idempotent column additions executed BEFORE views are created/updated
-- so existing tables have all required columns before view compilation.
-- ----------------------------------------------------------------------------
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_MEMBER_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;
ALTER TABLE RAW_MEMBER_LANDING MODIFY COLUMN TIER_CODE VARCHAR(50);

ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE RAW_REDEMPTIONS_LANDING ADD COLUMN IF NOT EXISTS PROCESSED_AT TIMESTAMP_NTZ;

ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS RAW_RECORD_HASH VARCHAR(64);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_SYSTEM VARCHAR(30);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE RAW_DLQ_QUARANTINE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);
ALTER TABLE STG_MEMBER_PROFILE ADD COLUMN IF NOT EXISTS STG_ROW_ID VARCHAR;

ALTER TABLE STG_REDEMPTIONS_FLATTENED ADD COLUMN IF NOT EXISTS BATCH_ID VARCHAR(50);

ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_INDIA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_USA ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5,0);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_TS TIMESTAMP_NTZ;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS EXTRACT_DATE DATE;
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR(255);
ALTER TABLE TABLE_AU ADD COLUMN IF NOT EXISTS LINE_NO NUMBER;

-- ----------------------------------------------------------------------------
-- 6. REFERENCE TABLES: Country Table Mapping
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REF_COUNTRY_MAP (
    COUNTRY_CODE         VARCHAR(10)      PRIMARY KEY,
    CANONICAL_CODE       VARCHAR(10)      NOT NULL,
    TARGET_TABLE         VARCHAR(50)      NOT NULL
);

-- Seed Country Mappings (IND/INDIA -> TABLE_INDIA, USA -> TABLE_USA, AU/AUS -> TABLE_AU)
MERGE INTO REF_COUNTRY_MAP target
USING (
    SELECT 'IND' AS code, 'IND' AS canonical, 'TABLE_INDIA' AS tbl UNION ALL
    SELECT 'INDIA', 'IND', 'TABLE_INDIA' UNION ALL
    SELECT 'USA', 'USA', 'TABLE_USA' UNION ALL
    SELECT 'AU', 'AU', 'TABLE_AU' UNION ALL
    SELECT 'AUS', 'AU', 'TABLE_AU'
) src
ON target.COUNTRY_CODE = src.code
WHEN MATCHED THEN
    UPDATE SET target.CANONICAL_CODE = src.canonical, target.TARGET_TABLE = src.tbl
WHEN NOT MATCHED THEN
    INSERT (COUNTRY_CODE, CANONICAL_CODE, TARGET_TABLE) VALUES (src.code, src.canonical, src.tbl);

-- ----------------------------------------------------------------------------
-- 7. UNIFIED COUNTRY MEMBERS VIEW
-- Explicit column list projected across all country tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL AS
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_INDIA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_USA
UNION ALL
SELECT SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE, TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE, AGE, STALE_MEMBER, EXTRACT_TS, SOURCE_FILE_NAME, LINE_NO, LAST_UPDATED_AT FROM TABLE_AU;

-- ----------------------------------------------------------------------------
-- 8. DYNAMIC UNIFIED MEMBERS VIEW (Query-Time Recalculation)
-- Dynamically recomputes AGE and STALE_MEMBER at query time relative to CURRENT_DATE()
-- while preserving static stored attributes in the country tables.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBERS_ALL_CURRENT AS
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
    -- Dynamically recomputed exact calendar age at query time
    CASE
        WHEN DOB IS NULL THEN NULL
        ELSE DATEDIFF(year, DOB, CURRENT_DATE()) - 
             CASE 
                WHEN (MONTH(CURRENT_DATE()) < MONTH(DOB)) 
                  OR (MONTH(CURRENT_DATE()) = MONTH(DOB) 
                      AND DAY(CURRENT_DATE()) < DAY(DOB)) 
                THEN 1 ELSE 0 
             END
    END AS AGE,
    -- Dynamically recomputed stale member flag at query time (>90 days since flight)
    CASE 
        WHEN LAST_FLIGHT_DATE IS NULL THEN 'Y'
        WHEN DATEDIFF(day, LAST_FLIGHT_DATE, CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS STALE_MEMBER,
    EXTRACT_TS,
    SOURCE_FILE_NAME,
    LINE_NO,
    LAST_UPDATED_AT
FROM MEMBERS_ALL;

-- ----------------------------------------------------------------------------
-- 9. DYNAMIC ANALYTICAL MART: Member Loyalty 360 Current View
-- Reads stored redemption totals from MEMBER_LOYALTY_360 (the mart), which are
-- refreshed each run by the pipeline, and recomputes only Age and Stale_Member
-- dynamically relative to CURRENT_DATE() using MEMBERS_ALL_CURRENT.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MEMBER_LOYALTY_360_CURRENT AS
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
    COALESCE(l.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
    COALESCE(l.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
    COALESCE(l.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
    l.LAST_REDEMPTION_DATE,
    COALESCE(l.TOP_PARTNER, 'None') AS TOP_PARTNER,
    CURRENT_TIMESTAMP() AS LAST_SYNC_AT
FROM MEMBERS_ALL_CURRENT m
LEFT JOIN MEMBER_LOYALTY_360 l
    ON m.SOURCE_SYSTEM = l.SOURCE_SYSTEM AND m.MEMBER_ID = l.MEMBER_ID;


-- ==================== INLINED 03_TRANSFORMATIONS.SQL ====================

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
SET run_batch_id = (SELECT UUID_STRING());

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
        WHEN m.CANONICAL_CODE IS NULL THEN 'Unmapped country'
        WHEN LENGTH(s.COUNTRY) > 5 THEN 'Country exceeds 5 characters'
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
    EXTRACT_TS,
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
        COALESCE(
            EXTRACT_TS,
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}[_ -]?[0-9]{6}'), 'YYYYMMDD_HH24MISS'),
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(SOURCE_FILE_NAME, '[0-9]{8}'), 'YYYYMMDD'),
            INGESTED_AT,
            CURRENT_TIMESTAMP()
        ) AS EXTRACT_TS,
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
    s.EXTRACT_TS,
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
                ORDER BY s.EXTRACT_TS DESC,
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
        EXTRACT_TS,
        LINE_NO,
        INGESTED_AT,
        STG_ROW_ID,
        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, MEMBER_ID 
            ORDER BY EXTRACT_TS DESC,
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
    EXTRACT_TS,
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
--   sql/snowflake/04_country_merges.generated.sql
--
-- In SnowSQL CLI, execute:
-- !source sql/snowflake/04_country_merges.generated.sql
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- STEP 3C: Audit Out-of-Order / Superseded Records into RAW_DLQ_QUARANTINE
-- Captures late-arriving older records that are blocked from overwriting newer core records
-- ----------------------------------------------------------------------------
MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        src.MEMBER_ID AS RECORD_ID,
        CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', src.SOURCE_SYSTEM, src.MEMBER_ID, src.MEMBER_NAME, src.EXTRACT_TS, src.LAST_FLIGHT_DATE, src.LINE_NO), 256) AS RAW_RECORD_HASH,
        'Superseded by existing newer record in core table' AS ERROR_REASON,
        'DEDUPED_LATEST_MEMBERS' AS SOURCE_FEED,
        src.SOURCE_SYSTEM,
        src.SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM DEDUPED_LATEST_MEMBERS src
    JOIN MEMBERS_ALL m ON src.SOURCE_SYSTEM = m.SOURCE_SYSTEM AND src.MEMBER_ID = m.MEMBER_ID
    WHERE (
        m.EXTRACT_TS > src.EXTRACT_TS
        OR (
            m.EXTRACT_TS = src.EXTRACT_TS
            AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.value:txn_id::STRING ORDER BY raw.INGESTED_AT DESC) = 1
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



-- ==================== INLINED 04_COUNTRY_MERGES.SQL ====================

-- ============================================================================
-- Static Country MERGE & Migration DELETE Statements for Snowflake
-- Generated automatically by scripts/generate_country_merges.py
-- Single source of truth: CANONICAL_TABLE_MAP in src/country_router.py
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Target Country: AU -> TABLE_AU
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_AU target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'AU'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_AU if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_AU target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'AU'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: IND -> TABLE_INDIA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_INDIA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'IND'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_INDIA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_INDIA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'IND'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);

-- ----------------------------------------------------------------------------
-- Target Country: USA -> TABLE_USA
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_USA target
USING (
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
        EXTRACT_TS,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS src
    WHERE src.COUNTRY = 'USA'
      AND NOT EXISTS (
          SELECT 1 FROM MEMBERS_ALL m
          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM
            AND m.MEMBER_ID = src.MEMBER_ID
            AND (
                m.EXTRACT_TS > src.EXTRACT_TS
                OR (
                    m.EXTRACT_TS = src.EXTRACT_TS
                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)
                )
            )
      )
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED AND (
    src.EXTRACT_TS > target.EXTRACT_TS
    OR (
        src.EXTRACT_TS = target.EXTRACT_TS
        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
    )
    OR target.EXTRACT_TS IS NULL
) THEN
    UPDATE SET
        target.MEMBER_NAME = src.MEMBER_NAME,
        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,
        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        target.TIER_CODE = src.TIER_CODE,
        target.AGENT_NAME = src.AGENT_NAME,
        target.STATE = src.STATE,
        target.COUNTRY = src.COUNTRY,
        target.POST_CODE = src.POST_CODE,
        target.DOB = src.DOB,
        target.IS_ACTIVE = src.IS_ACTIVE,
        target.AGE = src.AGE,
        target.STALE_MEMBER = src.STALE_MEMBER,
        target.EXTRACT_TS = src.EXTRACT_TS,
        target.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME,
        target.LINE_NO = src.LINE_NO,
        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
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
        EXTRACT_TS,
        SOURCE_FILE_NAME,
        LINE_NO,
        LAST_UPDATED_AT
    ) VALUES (
        src.SOURCE_SYSTEM,
        src.MEMBER_ID,
        src.MEMBER_NAME,
        src.ENROLLMENT_DATE,
        src.LAST_FLIGHT_DATE,
        src.TIER_CODE,
        src.AGENT_NAME,
        src.STATE,
        src.COUNTRY,
        src.POST_CODE,
        src.DOB,
        src.IS_ACTIVE,
        src.AGE,
        src.STALE_MEMBER,
        src.EXTRACT_TS,
        src.SOURCE_FILE_NAME,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_USA if latest country changed and incoming is newer or equal on tie
DELETE FROM TABLE_USA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'USA'
      AND (
          d.EXTRACT_TS > target.EXTRACT_TS
          OR (
              d.EXTRACT_TS = target.EXTRACT_TS
              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) >= COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)
          )
          OR target.EXTRACT_TS IS NULL
      )
);



-- ==================== INLINED 05_MART_AND_FINALIZE.SQL ====================

-- ============================================================================
-- SkyPoints Step 3: Analytical Mart Build & Finalize Landing Batch Tracking
-- NOTE: run_batch_id is a session variable initialized in transformations.sql.
-- All four scripts must execute in the same SnowSQL session in order.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 5: Loyalty 360 Analytical Mart (Sums Only COMPLETED Miles)
-- ----------------------------------------------------------------------------

-- Snapshot pre-update status (column renamed so it can never clash with the live table)
CREATE OR REPLACE TEMPORARY TABLE PRE_UPDATE_REDEMPTION_STATUS AS
SELECT TXN_ID, IS_AMBIGUOUS AS PREV_IS_AMBIGUOUS
FROM STG_REDEMPTIONS_FLATTENED;

-- Member IDs that appear in more than one source system
CREATE OR REPLACE TEMPORARY TABLE AMBIGUOUS_MEMBER_IDS AS
SELECT m.MEMBER_ID
FROM MEMBERS_ALL_CURRENT m
GROUP BY m.MEMBER_ID
HAVING COUNT(DISTINCT m.SOURCE_SYSTEM) > 1;

-- Flag redemptions (no correlated view subquery)
UPDATE STG_REDEMPTIONS_FLATTENED AS red
SET IS_AMBIGUOUS = IFF(
    red.MEMBER_ID IN (SELECT a.MEMBER_ID FROM AMBIGUOUS_MEMBER_IDS a),
    TRUE,
    FALSE
);

-- Members whose ambiguity flipped in this run
CREATE OR REPLACE TEMPORARY TABLE AMBIGUITY_CHANGED_MEMBERS AS
SELECT DISTINCT cur.MEMBER_ID
FROM STG_REDEMPTIONS_FLATTENED cur
JOIN PRE_UPDATE_REDEMPTION_STATUS prev ON cur.TXN_ID = prev.TXN_ID
WHERE cur.IS_AMBIGUOUS <> prev.PREV_IS_AMBIGUOUS;

MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        red.TXN_ID AS RECORD_ID,
        CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', red.TXN_ID, red.MEMBER_ID, red.FEED_DATE, red.TXN_DATE, red.PARTNER, red.MILES_REDEEMED, red.STATUS), 256) AS RAW_RECORD_HASH,
        'Ambiguous member_id across source systems' AS ERROR_REASON,
        'STG_REDEMPTIONS_FLATTENED' AS SOURCE_FEED,
        'PARTNER_JSON' AS SOURCE_SYSTEM,
        'stg_redemptions' AS SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM STG_REDEMPTIONS_FLATTENED red
    WHERE red.IS_AMBIGUOUS = TRUE
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
AND target.RAW_RECORD_HASH = src.RAW_RECORD_HASH
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, RAW_RECORD_HASH, ERROR_REASON, SOURCE_FEED, SOURCE_SYSTEM, SOURCE_FILE_NAME, BATCH_ID, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.RAW_RECORD_HASH, src.ERROR_REASON, src.SOURCE_FEED, src.SOURCE_SYSTEM, src.SOURCE_FILE_NAME, src.BATCH_ID, CURRENT_TIMESTAMP());

-- Remove resolved redemptions from DLQ when ambiguity disappears
DELETE FROM RAW_DLQ_QUARANTINE
WHERE SOURCE_FEED = 'STG_REDEMPTIONS_FLATTENED'
  AND ERROR_REASON = 'Ambiguous member_id across source systems'
  AND RECORD_ID IN (
      SELECT red.TXN_ID FROM STG_REDEMPTIONS_FLATTENED red WHERE red.IS_AMBIGUOUS = FALSE
  );

-- 2. Build or Refresh MEMBER_LOYALTY_360 Mart (Idempotent MERGE for affected members)
-- Keyed on (SOURCE_SYSTEM, MEMBER_ID) from MEMBERS_ALL_CURRENT; touches only members affected by this batch,
-- including members whose redemptions were previously ambiguous and are now resolved.
-- Excludes IS_AMBIGUOUS = TRUE transactions from analytical calculations.
-- Breaks MODE(PARTNER) ties deterministically by highest count then alphabetical.
MERGE INTO MEMBER_LOYALTY_360 target
USING (
    WITH TouchedMembers AS (
        -- 1. Members touched in current batch
        SELECT SOURCE_SYSTEM, MEMBER_ID
        FROM DEDUPED_LATEST_MEMBERS
        UNION
        -- 2. Members with incoming redemptions in current batch
        SELECT m.SOURCE_SYSTEM, red.MEMBER_ID
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN MEMBERS_ALL_CURRENT m ON red.MEMBER_ID = m.MEMBER_ID
        WHERE red.BATCH_ID = $run_batch_id
        UNION
        -- 3. Members whose redemptions status flipped (became ambiguous or became resolved)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        JOIN AMBIGUITY_CHANGED_MEMBERS acm ON m.MEMBER_ID = acm.MEMBER_ID
        UNION
        -- 4. Members with redemptions whose member profile in MEMBERS_ALL spans multiple source systems (ambiguity detected)
        SELECT m.SOURCE_SYSTEM, m.MEMBER_ID
        FROM MEMBERS_ALL_CURRENT m
        WHERE m.MEMBER_ID IN (
            SELECT MEMBER_ID FROM MEMBERS_ALL_CURRENT GROUP BY MEMBER_ID HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
        )
    ),
    PartnerCounts AS (
        SELECT
            red.MEMBER_ID,
            red.PARTNER,
            COUNT(*) AS CNT,
            ROW_NUMBER() OVER (PARTITION BY red.MEMBER_ID ORDER BY COUNT(*) DESC, red.PARTNER ASC) AS RN
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE AND red.PARTNER IS NOT NULL
        GROUP BY red.MEMBER_ID, red.PARTNER
    ),
    TopPartner AS (
        SELECT MEMBER_ID, PARTNER AS TOP_PARTNER
        FROM PartnerCounts
        WHERE RN = 1
    ),
    RedemptionSummary AS (
        SELECT
            red.MEMBER_ID,
            COUNT(DISTINCT red.TXN_ID) AS TOTAL_REDEMPTIONS,
            COALESCE(SUM(CASE WHEN red.STATUS = 'COMPLETED' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS TOTAL_MILES_REDEEMED,
            COALESCE(SUM(CASE WHEN red.STATUS = 'PENDING' THEN red.MILES_REDEEMED ELSE 0 END), 0) AS PENDING_MILES_REDEEMED,
            MAX(red.TXN_DATE) AS LAST_REDEMPTION_DATE
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON red.MEMBER_ID = tm.MEMBER_ID
        WHERE red.IS_AMBIGUOUS = FALSE
        GROUP BY red.MEMBER_ID
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
        COALESCE(rs.TOTAL_REDEMPTIONS, 0) AS TOTAL_REDEMPTIONS,
        COALESCE(rs.TOTAL_MILES_REDEEMED, 0) AS TOTAL_MILES_REDEEMED,
        COALESCE(rs.PENDING_MILES_REDEEMED, 0) AS PENDING_MILES_REDEEMED,
        rs.LAST_REDEMPTION_DATE,
        COALESCE(tp.TOP_PARTNER, 'None') AS TOP_PARTNER
    FROM MEMBERS_ALL_CURRENT m
    JOIN TouchedMembers tm ON m.SOURCE_SYSTEM = tm.SOURCE_SYSTEM AND m.MEMBER_ID = tm.MEMBER_ID
    LEFT JOIN RedemptionSummary rs ON m.MEMBER_ID = rs.MEMBER_ID
    LEFT JOIN TopPartner tp ON m.MEMBER_ID = tp.MEMBER_ID
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

-- Remove members from mart that no longer exist in unified current members view (e.g. cross-country moves or hard deletes)
DELETE FROM MEMBER_LOYALTY_360
WHERE (SOURCE_SYSTEM, MEMBER_ID) NOT IN (
    SELECT SOURCE_SYSTEM, MEMBER_ID FROM MEMBERS_ALL_CURRENT
);


-- ----------------------------------------------------------------------------
-- STEP 6: Mark Processed Records in Landing Tables for Batch Tracking
-- Stamps ONLY the records claimed and processed by this specific batch
-- ----------------------------------------------------------------------------
UPDATE RAW_MEMBER_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;

UPDATE RAW_REDEMPTIONS_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE BATCH_ID = $run_batch_id AND PROCESSED_AT IS NULL;


-- Step 6: Single evaluation query returning scenario, expected, actual, PASS/FAIL
WITH AssertionResults AS (
    -- Scenario 1A: Member moving country deleted from old table (TABLE_USA)
    SELECT
        1 AS test_id,
        'Member moving country: deleted from old TABLE_USA' AS scenario,
        '0' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM TABLE_USA
    WHERE MEMBER_ID = 'MOVE_101'

    UNION ALL

    -- Scenario 1B: Newer extract date move: inserted into new TABLE_INDIA
    SELECT
        2 AS test_id,
        'Newer extract date move: inserted into new TABLE_INDIA' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM TABLE_INDIA
    WHERE MEMBER_ID = 'MOVE_101'

    UNION ALL

    -- Scenario 2: Same raw ID in two source systems both survive in their country tables
    SELECT
        3 AS test_id,
        'Composite key: Same ID in IND_SYS and USA_SYS both survive' AS scenario,
        '2' AS expected,
        TO_VARCHAR(
            (SELECT COUNT(*) FROM TABLE_INDIA WHERE SOURCE_SYSTEM = 'IND_SYS' AND MEMBER_ID = 'DUP_1') +
            (SELECT COUNT(*) FROM TABLE_USA WHERE SOURCE_SYSTEM = 'USA_SYS' AND MEMBER_ID = 'DUP_1')
        ) AS actual

    UNION ALL

    -- Scenario 3A: Rename collision older name quarantined in DLQ
    SELECT
        4 AS test_id,
        'Rename collision: older name quarantined in DLQ' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'REN_202'
      AND ERROR_REASON = 'Key collision: superseded by newer record'

    UNION ALL

    -- Scenario 3B: Rename collision newer name kept in target table
    SELECT
        5 AS test_id,
        'Rename collision: newer name kept in target table' AS scenario,
        'Alice Green' AS expected,
        MEMBER_NAME AS actual
    FROM TABLE_AU
    WHERE MEMBER_ID = 'REN_202'

    UNION ALL

    -- Scenario 4: Empty-string Tier Code treated as NULL and staged successfully
    SELECT
        6 AS test_id,
        'Empty-string tier code: treated as null, not quarantined' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM TABLE_AU
    WHERE MEMBER_ID = 'EMPTY_TIER_1'
      AND TIER_CODE IS NULL

    UNION ALL

    -- Scenario 5: Unmapped country quarantined at staging
    SELECT
        7 AS test_id,
        'Unmapped country: quarantined in DLQ at staging' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'BAD_CTRY_1'
      AND ERROR_REASON = 'Unmapped country'

    UNION ALL

    -- Scenario 6: Null country quarantined at staging
    SELECT
        8 AS test_id,
        'Null country: quarantined in DLQ at staging' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'NULL_CTRY_1'
      AND ERROR_REASON = 'Unmapped country: Country is required and cannot be empty'

    UNION ALL

    -- Scenario 7: Bad feed_date in redemption quarantined
    SELECT
        9 AS test_id,
        'Bad redemption feed_date: quarantined into DLQ' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE SOURCE_FEED = 'RAW_REDEMPTIONS_LANDING'
      AND ERROR_REASON = 'Invalid feed_date in redemption feed'

    UNION ALL

    -- Scenario 8A: Mart before rerun has expected total member rows
    SELECT
        10 AS test_id,
        'Mart before rerun: MEMBER_LOYALTY_360 total member rows' AS scenario,
        '10' AS expected,
        TO_VARCHAR(MART_COUNT) AS actual
    FROM PRE_RERUN_SNAPSHOT

    UNION ALL

    -- Scenario 8B: Mart before rerun reflects country merge (MOVE_101 updated to IND)
    SELECT
        11 AS test_id,
        'Mart before rerun: MOVE_101 country updated to IND' AS scenario,
        '1' AS expected,
        TO_VARCHAR(MOVE_101_COUNTRY_CORRECT) AS actual
    FROM PRE_RERUN_SNAPSHOT

    UNION ALL

    -- Scenario 8C: Mart before rerun: COMPLETED miles summed correctly and duplicate replay ignored
    SELECT
        12 AS test_id,
        'Mart before rerun: MOVE_101 COMPLETED miles summed correctly' AS scenario,
        '5000' AS expected,
        MOVE_101_COMPLETED_MILES AS actual
    FROM PRE_RERUN_SNAPSHOT

    UNION ALL

    -- Scenario 8D: Mart before rerun: PENDING miles tracked separately
    SELECT
        13 AS test_id,
        'Mart before rerun: MOVE_101 PENDING miles tracked separately' AS scenario,
        '0' AS expected,
        MOVE_101_PENDING_MILES AS actual
    FROM PRE_RERUN_SNAPSHOT

    UNION ALL

    -- Scenario 9: Ambiguous member_id redemption quarantined
    SELECT
        14 AS test_id,
        'Ambiguous member_id redemption: quarantined into DLQ' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE SOURCE_FEED = 'STG_REDEMPTIONS_FLATTENED'
      AND ERROR_REASON = 'Ambiguous member_id across source systems'

    UNION ALL

    -- Scenario 10A: Staging member row count unchanged after rerun (idempotent batch tracking)
    SELECT
        15 AS test_id,
        'Rerun delta: STG_MEMBER_PROFILE row count unchanged' AS scenario,
        '0' AS expected,
        TO_VARCHAR((SELECT STG_MEMBER_COUNT FROM POST_RERUN_SNAPSHOT) - (SELECT STG_MEMBER_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

    UNION ALL

    -- Scenario 10B: DLQ row count unchanged after rerun (idempotent DLQ merges)
    SELECT
        16 AS test_id,
        'Rerun delta: RAW_DLQ_QUARANTINE row count unchanged' AS scenario,
        '0' AS expected,
        TO_VARCHAR((SELECT DLQ_COUNT FROM POST_RERUN_SNAPSHOT) - (SELECT DLQ_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

    UNION ALL

    -- Scenario 10C: Staging redemptions count unchanged after rerun
    SELECT
        17 AS test_id,
        'Rerun delta: STG_REDEMPTIONS_FLATTENED row count unchanged' AS scenario,
        '0' AS expected,
        TO_VARCHAR((SELECT STG_REDEMP_COUNT FROM POST_RERUN_SNAPSHOT) - (SELECT STG_REDEMP_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

    UNION ALL

    -- Scenario 10D: Mart row count unchanged after rerun
    SELECT
        18 AS test_id,
        'Rerun delta: MEMBER_LOYALTY_360 row count unchanged' AS scenario,
        '0' AS expected,
        TO_VARCHAR((SELECT MART_COUNT FROM POST_RERUN_SNAPSHOT) - (SELECT MART_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

    UNION ALL

    -- Scenario 11A: Two distinct rows with missing member_id both preserved in DLQ
    SELECT
        19 AS test_id,
        'DLQ reject preservation: two rows with missing member_id both quarantined' AS scenario,
        '2' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE ERROR_REASON = 'Missing mandatory Member_Id'

    UNION ALL

    -- Scenario 11B: Same ID in two source systems both bad both preserved in DLQ
    SELECT
        20 AS test_id,
        'DLQ reject preservation: same ID in two source systems both bad both quarantined' AS scenario,
        '2' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'BAD_DUP_1'

    UNION ALL

    -- Scenario 11C: Same bad row in two batches yields 2 DLQ rows
    SELECT
        21 AS test_id,
        'DLQ reject preservation: same bad row in two batches yields 2 DLQ rows' AS scenario,
        '2' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'REPEAT_BAD_1'

    UNION ALL

    -- Scenario 12A: Rename across two batches: no DLQ row created
    SELECT
        22 AS test_id,
        'Rename across two batches: no DLQ row created' AS scenario,
        '0' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'BATCH_REN_1'

    UNION ALL

    -- Scenario 12B: Rename across two batches: newest name wins in target table
    SELECT
        23 AS test_id,
        'Rename across two batches: newest name wins in target table' AS scenario,
        'Updated Name' AS expected,
        MEMBER_NAME AS actual
    FROM TABLE_USA
    WHERE MEMBER_ID = 'BATCH_REN_1'

    UNION ALL

    -- Scenario 13: Ambiguity round trip: redemptions return to mart when ambiguity disappears
    SELECT
        24 AS test_id,
        'Ambiguity round trip: redemptions return to mart when ambiguity disappears' AS scenario,
        '3000' AS expected,
        TO_VARCHAR(COALESCE(TOTAL_MILES_REDEEMED, 0)) AS actual
    FROM MEMBER_LOYALTY_360
    WHERE MEMBER_ID = 'RESOLVE_101'

    UNION ALL

    -- Scenario 14: Invalid DOB quarantined into DLQ
    SELECT
        25 AS test_id,
        'Invalid DOB: quarantined into DLQ' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'BAD_DOB_1'
      AND ERROR_REASON = 'Invalid DOB: unparseable calendar date'

    UNION ALL

    -- Scenario 15: Non-numeric miles quarantined into DLQ without abort
    SELECT
        26 AS test_id,
        'Non-numeric miles: quarantined into DLQ without abort' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'TXN_NON_NUM_1'
      AND ERROR_REASON = 'Invalid miles_redeemed data type: non-numeric'

    UNION ALL

    -- Scenario 16: POST_CODE present in country target table
    SELECT
        27 AS test_id,
        'POST_CODE present in country target table' AS scenario,
        '40001' AS expected,
        TO_VARCHAR(COALESCE(MAX(POST_CODE), 0)) AS actual
    FROM TABLE_INDIA
    WHERE MEMBER_ID = 'MOVE_101'

    UNION ALL

    -- Scenario 17: Out-of-order older record audited in DLQ as superseded
    SELECT
        28 AS test_id,
        'Out-of-order older record audited in DLQ as superseded' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_DLQ_QUARANTINE
    WHERE RECORD_ID = 'MOVE_101'
      AND ERROR_REASON = 'Superseded by existing newer record in core table'

    UNION ALL

    -- Scenario 18: In-flight landing record claimed and processed on subsequent run
    SELECT
        29 AS test_id,
        'In-flight landing record processed successfully on subsequent run' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM TABLE_USA
    WHERE MEMBER_ID = 'INFLIGHT_1'

    UNION ALL

    -- Scenario 19: Interrupted batch recovery (unprocessed row with old batch ID is re-claimed and processed)
    SELECT
        30 AS test_id,
        'Interrupted batch: unprocessed landing record with old batch ID is re-claimed and processed' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM TABLE_USA
    WHERE MEMBER_ID = 'INTERRUPT_1'

    UNION ALL

    -- Scenario 20: Interrupted batch landing row stamped with PROCESSED_AT
    SELECT
        31 AS test_id,
        'Interrupted batch: landing row stamped with PROCESSED_AT' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM RAW_MEMBER_LANDING
    WHERE MEMBER_ID = 'INTERRUPT_1' AND PROCESSED_AT IS NOT NULL

    UNION ALL

    -- Scenario 21A: Same-day correction with equal line numbers deleted from old TABLE_USA
    SELECT
        32 AS test_id,
        'Same-day correction with equal line numbers: deleted from old TABLE_USA' AS scenario,
        '0' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM TABLE_USA
    WHERE MEMBER_ID = 'SAME_DAY_MOVE_1'

    UNION ALL

    -- Scenario 21B: Same-day correction with equal line numbers inserted into new TABLE_INDIA
    SELECT
        33 AS test_id,
        'Same-day correction with equal line numbers: inserted into new TABLE_INDIA' AS scenario,
        '1' AS expected,
        TO_VARCHAR(COUNT(*)) AS actual
    FROM TABLE_INDIA
    WHERE MEMBER_ID = 'SAME_DAY_MOVE_1'

    UNION ALL

    -- Scenario 22: Same-day correction in an earlier-sorting file name wins on later time; with identical timestamps later load wins
    SELECT
        34 AS test_id,
        'Same-day correction in earlier-sorting file name wins on time and later load wins on tie' AS scenario,
        'Tie Winner Later Load' AS expected,
        MEMBER_NAME AS actual
    FROM TABLE_USA
    WHERE MEMBER_ID = 'SAME_CTRY_TIE_1'
)
SELECT
    scenario,
    expected,
    actual,
    CASE WHEN expected = actual THEN 'PASS' ELSE 'FAIL' END AS status
FROM AssertionResults
ORDER BY test_id;
