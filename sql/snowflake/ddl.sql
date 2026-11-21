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

-- Dead Letter Queue (DLQ) table for quarantined malformed rows
CREATE TABLE IF NOT EXISTS RAW_DLQ_QUARANTINE (
    RECORD_ID            VARCHAR(50),
    RAW_RECORD           VARCHAR,
    ERROR_REASON         VARCHAR(500),
    SOURCE_FEED          VARCHAR(255),
    QUARANTINED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. STAGING TABLE: Member Profile with Derived Columns
-- Strongly-typed, normalized dates, plus Age & Stale_Member (> 90 days)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STG_MEMBER_PROFILE (
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
    AGE                  INTEGER,         -- Derived: Exact calendar age
    STALE_MEMBER         VARCHAR(1),      -- Derived: 'Y' if days since Flight_Date > 90 else 'N'
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_MEMBER PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID) RELY
)
CLUSTER BY (COUNTRY, ENROLLMENT_DATE);

-- ----------------------------------------------------------------------------
-- 3. COUNTRY-SPECIFIC TARGET TABLES
-- Dedicated physical country tables clustered on MEMBER_ID
-- ----------------------------------------------------------------------------

-- Table: India
CREATE TABLE IF NOT EXISTS TABLE_INDIA (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'IND',
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
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
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'USA',
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: Canada
CREATE TABLE IF NOT EXISTS TABLE_CAN (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'CAN',
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- Table: Philippines
CREATE TABLE IF NOT EXISTS TABLE_PHIL (
    SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE',
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'PHIL',
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
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
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50)      DEFAULT 'AU',
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1),
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)
)
CLUSTER BY (MEMBER_ID);

-- ----------------------------------------------------------------------------
-- 4. REFERENCE TABLES: Country Table Mapping
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REF_COUNTRY_MAP (
    COUNTRY_CODE         VARCHAR(10)      PRIMARY KEY,
    CANONICAL_CODE       VARCHAR(10)      NOT NULL,
    TARGET_TABLE         VARCHAR(50)      NOT NULL
);

-- Seed Country Mappings (IND/INDIA -> TABLE_INDIA, USA -> TABLE_USA, etc.)
MERGE INTO REF_COUNTRY_MAP target
USING (
    SELECT 'IND' AS code, 'IND' AS canonical, 'TABLE_INDIA' AS tbl UNION ALL
    SELECT 'INDIA', 'IND', 'TABLE_INDIA' UNION ALL
    SELECT 'USA', 'USA', 'TABLE_USA' UNION ALL
    SELECT 'CAN', 'CAN', 'TABLE_CAN' UNION ALL
    SELECT 'CANADA', 'CAN', 'TABLE_CAN' UNION ALL
    SELECT 'PHIL', 'PHIL', 'TABLE_PHIL' UNION ALL
    SELECT 'PH', 'PHIL', 'TABLE_PHIL' UNION ALL
    SELECT 'AU', 'AU', 'TABLE_AU' UNION ALL
    SELECT 'AUS', 'AU', 'TABLE_AU'
) src
ON target.COUNTRY_CODE = src.code
WHEN MATCHED THEN
    UPDATE SET target.CANONICAL_CODE = src.canonical, target.TARGET_TABLE = src.tbl
WHEN NOT MATCHED THEN
    INSERT (COUNTRY_CODE, CANONICAL_CODE, TARGET_TABLE) VALUES (src.code, src.canonical, src.tbl);

-- ----------------------------------------------------------------------------
-- 4B. UNIFIED COUNTRY MEMBERS VIEW
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 5. SEMI-STRUCTURED REDEMPTION TABLES
-- ----------------------------------------------------------------------------

-- Landing table for raw JSON documents
CREATE TABLE IF NOT EXISTS RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
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
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID, TXN_DATE);

-- ----------------------------------------------------------------------------
-- 5. ANALYTICAL MART: Member Loyalty 360 View
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MEMBER_LOYALTY_360 (
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
