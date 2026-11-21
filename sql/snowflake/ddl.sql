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
CREATE OR REPLACE TABLE RAW_MEMBER_LANDING (
    RECORD_TYPE          VARCHAR(10),     -- 'H' for Header, 'D' for Detail
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
    SOURCE_FILE_NAME     VARCHAR(255),
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. STAGING TABLE: Member Profile with Derived Columns
-- Strongly-typed, normalized dates, plus Age & Stale_Member (> 90 days)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE STG_MEMBER_PROFILE (
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    MEMBER_NAME          VARCHAR(255)     NOT NULL,
    ENROLLMENT_DATE      DATE             NOT NULL,
    LAST_FLIGHT_DATE     DATE,
    TIER_CODE            VARCHAR(10),
    AGENT_NAME           VARCHAR(255),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    DOB                  DATE,
    IS_ACTIVE            VARCHAR(1)       DEFAULT 'A',
    AGE                  INTEGER,         -- Derived: DATEDIFF(year, DOB, CURRENT_DATE)
    STALE_MEMBER         VARCHAR(1),      -- Derived: 'Y' if days since Flight_Date > 90 else 'N'
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_MEMBER PRIMARY KEY (MEMBER_ID) RELY
)
CLUSTER BY (COUNTRY, ENROLLMENT_DATE);

-- ----------------------------------------------------------------------------
-- 3. COUNTRY-SPECIFIC TARGET TABLES
-- Target tables storing deduplicated active profiles per country
-- ----------------------------------------------------------------------------

-- Table: India
CREATE OR REPLACE TABLE TABLE_INDIA (
    MEMBER_ID            VARCHAR(50)      PRIMARY KEY,
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
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID);

-- Table: USA
CREATE OR REPLACE TABLE TABLE_USA (
    MEMBER_ID            VARCHAR(50)      PRIMARY KEY,
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
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID);

-- Table: Canada
CREATE OR REPLACE TABLE TABLE_CAN (
    MEMBER_ID            VARCHAR(50)      PRIMARY KEY,
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
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID);

-- Table: Philippines
CREATE OR REPLACE TABLE TABLE_PHIL (
    MEMBER_ID            VARCHAR(50)      PRIMARY KEY,
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
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID);

-- Table: Australia
CREATE OR REPLACE TABLE TABLE_AU (
    MEMBER_ID            VARCHAR(50)      PRIMARY KEY,
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
    LAST_UPDATED_AT      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID);

-- ----------------------------------------------------------------------------
-- 4. SEMI-STRUCTURED REDEMPTION TABLES
-- ----------------------------------------------------------------------------

-- Landing table for raw JSON documents
CREATE OR REPLACE TABLE RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD         VARIANT,
    FEED_DATE            VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Flattened relational redemptions table
CREATE OR REPLACE TABLE STG_REDEMPTIONS_FLATTENED (
    TXN_ID               VARCHAR(50)      PRIMARY KEY,
    MEMBER_ID            VARCHAR(50)      NOT NULL,
    FEED_DATE            DATE             NOT NULL,
    TXN_DATE             DATE             NOT NULL,
    PARTNER              VARCHAR(100),
    MILES_REDEEMED       INTEGER,
    STATUS               VARCHAR(50),
    INGESTED_AT          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID, TXN_DATE);

-- ----------------------------------------------------------------------------
-- 5. ANALYTICAL MART: Member Loyalty 360 View
-- Combined member profile with aggregate partner redemption statistics
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE MEMBER_LOYALTY_360 (
    MEMBER_ID            VARCHAR(50)      PRIMARY KEY,
    MEMBER_NAME          VARCHAR(255),
    COUNTRY              VARCHAR(50),
    TIER_CODE            VARCHAR(10),
    ENROLLMENT_DATE      DATE,
    LAST_FLIGHT_DATE     DATE,
    AGE                  INTEGER,
    STALE_MEMBER         VARCHAR(1),
    TOTAL_REDEMPTIONS    INTEGER          DEFAULT 0,
    TOTAL_MILES_REDEEMED INTEGER          DEFAULT 0,
    LAST_REDEMPTION_DATE DATE,
    TOP_PARTNER          VARCHAR(100),
    LAST_SYNC_AT         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);
