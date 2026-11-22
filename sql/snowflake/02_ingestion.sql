-- ============================================================================
-- SkyPoints Snowflake Production Ingestion DDL & Pipeline Orchestration
-- NOTE: Not executed against a live account (documented DDL template for production deployment).
-- Provides:
-- 1. File Formats: Pipe-delimited CSV (with leading pipe) & JSON (with STRIP_OUTER_ARRAY)
-- 2. Internal Stage Definitions
-- 3. COPY INTO Statements with Metadata Mapping (METADATA$FILENAME & METADATA$FILE_ROW_NUMBER)
-- 4. Commented Snowpipe AUTO_INGEST continuous loading template
-- 5. Commented Snowflake STREAM + TASK Change Data Capture (CDC) scheduling template
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. FILE FORMAT DEFINITIONS
-- ----------------------------------------------------------------------------

-- Pipe-delimited file format for loyalty member feed.
-- Note: Files start each line with '|', so column $1 is empty; actual fields begin at $2.
CREATE OR REPLACE FILE FORMAT FF_PIPE_DELIMITED
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
    TRIM_SPACE = TRUE
    NULL_IF = ('', 'NULL', 'null', 'NONE');

-- JSON file format for semi-structured partner redemption feeds.
-- STRIP_OUTER_ARRAY handles both JSON arrays of records and individual JSON documents.
CREATE OR REPLACE FILE FORMAT FF_JSON
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = TRUE;


-- ----------------------------------------------------------------------------
-- 2. STAGE DEFINITIONS
-- Uses IF NOT EXISTS to prevent accidental dropping of stages and staged files
-- ----------------------------------------------------------------------------

CREATE STAGE IF NOT EXISTS MEMBER_FEED_STAGE
    COMMENT = 'Internal stage for incoming pipe-delimited loyalty member feeds';

CREATE STAGE IF NOT EXISTS REDEMPTIONS_FEED_STAGE
    COMMENT = 'Internal stage for incoming partner JSON redemption feeds';


-- ----------------------------------------------------------------------------
-- 3. COPY INTO STATEMENTS (Landing Ingestion)
-- Includes ON_ERROR = 'CONTINUE' for resilient, lossless ingestion of valid rows
-- ----------------------------------------------------------------------------

-- Copy pipe-delimited member records into RAW_MEMBER_LANDING.
-- Maps $2 as RECORD_TYPE, $3 as MEMBER_NAME, etc., capturing source filename and row number.
COPY INTO RAW_MEMBER_LANDING (
    RECORD_TYPE,
    SOURCE_SYSTEM,
    MEMBER_NAME,
    MEMBER_ID,
    ENROLLMENT_DATE,
    LAST_FLIGHT_DATE,
    TIER_CODE,
    AGENT_NAME,
    STATE,
    COUNTRY,
    DOB,
    IS_ACTIVE,
    SOURCE_FILE_NAME,
    LINE_NO,
    EXTRACT_TS
)
FROM (
    SELECT
        $2 AS RECORD_TYPE,
        'FLAT_FILE' AS SOURCE_SYSTEM,
        $3 AS MEMBER_NAME,
        $4 AS MEMBER_ID,
        $5 AS ENROLLMENT_DATE,
        $6 AS LAST_FLIGHT_DATE,
        $7 AS TIER_CODE,
        $8 AS AGENT_NAME,
        $9 AS STATE,
        $10 AS COUNTRY,
        $11 AS DOB,
        $12 AS IS_ACTIVE,
        METADATA$FILENAME AS SOURCE_FILE_NAME,
        METADATA$FILE_ROW_NUMBER AS LINE_NO,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(REGEXP_REPLACE(REGEXP_SUBSTR(METADATA$FILENAME, '[0-9]{8}[_ -]?[0-9]{6}'), '[^0-9]', ''), 'YYYYMMDDHH24MISS'),
            TRY_TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(METADATA$FILENAME, '[0-9]{8}'), 'YYYYMMDD'),
            CURRENT_TIMESTAMP()
        ) AS EXTRACT_TS
    FROM @MEMBER_FEED_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'FF_PIPE_DELIMITED')
ON_ERROR = 'CONTINUE';


-- Copy partner JSON documents into RAW_REDEMPTIONS_LANDING.
COPY INTO RAW_REDEMPTIONS_LANDING (
    FEED_PAYLOAD,
    FEED_DATE,
    INGESTED_AT
)
FROM (
    SELECT
        $1 AS FEED_PAYLOAD,
        COALESCE(
            $1:feed_date::STRING,
            REGEXP_SUBSTR(METADATA$FILENAME, '[0-9]{8}'),
            TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD')
        ) AS FEED_DATE,
        CURRENT_TIMESTAMP() AS INGESTED_AT
    FROM @REDEMPTIONS_FEED_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'FF_JSON')
ON_ERROR = 'CONTINUE';


-- ----------------------------------------------------------------------------
-- 4. SNOWPIPE AUTO-INGEST TEMPLATE (Continuous Serverless Ingestion)
-- ----------------------------------------------------------------------------
-- To configure continuous automated ingestion when files land in S3/GCS/Azure:
-- 1. Create an external stage pointing to the cloud storage bucket.
-- 2. Configure cloud notification events (e.g. AWS SQS/SNS) to trigger Snowpipe.
--
-- Example:
-- CREATE OR REPLACE PIPE PIPE_RAW_MEMBER_AUTOINGEST
--     AUTO_INGEST = TRUE
--     AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:123456789012:skypoints-member-file-arrival'
-- AS
-- COPY INTO RAW_MEMBER_LANDING (
--     RECORD_TYPE,
--     SOURCE_SYSTEM,
--     MEMBER_NAME,
--     MEMBER_ID,
--     ENROLLMENT_DATE,
--     LAST_FLIGHT_DATE,
--     TIER_CODE,
--     AGENT_NAME,
--     STATE,
--     COUNTRY,
--     DOB,
--     IS_ACTIVE,
--     SOURCE_FILE_NAME,
--     LINE_NO,
--     EXTRACT_DATE
-- )
-- FROM (
--     SELECT
--         $2,
--         'FLAT_FILE',
--         $3,
--         $4,
--         $5,
--         $6,
--         $7,
--         $8,
--         $9,
--         $10,
--         $11,
--         $12,
--         METADATA$FILENAME,
--         METADATA$FILE_ROW_NUMBER,
--         COALESCE(
--             TRY_TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '[0-9]{8}'), 'YYYYMMDD'),
--             CURRENT_DATE()
--         )
--     FROM @MEMBER_FEED_STAGE
-- )
-- FILE_FORMAT = (FORMAT_NAME = 'FF_PIPE_DELIMITED');


-- ----------------------------------------------------------------------------
-- 5. CHANGE DATA CAPTURE (CDC): STREAM + TASK SCHEDULING TEMPLATE
-- ----------------------------------------------------------------------------
-- Automated orchestration triggering the transformation workflow whenever new
-- records land in RAW_MEMBER_LANDING or RAW_REDEMPTIONS_LANDING.
--
-- Example:
-- CREATE OR REPLACE STREAM STREAM_RAW_MEMBER_LANDING
--     ON TABLE RAW_MEMBER_LANDING
--     APPEND_ONLY = TRUE;
--
-- CREATE OR REPLACE STREAM STREAM_RAW_REDEMPTIONS_LANDING
--     ON TABLE RAW_REDEMPTIONS_LANDING
--     APPEND_ONLY = TRUE;
--
-- Production Stored Procedure template invoked by TASK_ORCHESTRATE_SKYPOINTS:
-- Encapsulates the execution workflow defined in run_all.sql
-- CREATE OR REPLACE PROCEDURE EXECUTE_SKYPOINTS_PIPELINE()
--     RETURNS VARCHAR
--     LANGUAGE SQL
--     EXECUTE AS CALLER
-- AS
-- $$
-- BEGIN
--     -- Orchestrates the sequential pipeline stages:
--     -- 1. transformations.sql (Batch claiming, DLQ quarantine, Staging, Latest-wins dedup)
--     -- 2. generated_country_merges.sql (Country merges and migrations)
--     -- 3. 03_mart_and_finalize.sql (Loyalty 360 mart merge & batch stamping)
--     RETURN 'SkyPoints Pipeline executed successfully';
-- END;
-- $$;
--
-- CREATE OR REPLACE TASK TASK_ORCHESTRATE_SKYPOINTS
--     WAREHOUSE = 'COMPUTE_WH'
--     SCHEDULE = 'USING CRON 0 * * * * UTC'
--     WHEN SYSTEM$STREAM_HAS_DATA('STREAM_RAW_MEMBER_LANDING') 
--       OR SYSTEM$STREAM_HAS_DATA('STREAM_RAW_REDEMPTIONS_LANDING')
-- AS
--     CALL EXECUTE_SKYPOINTS_PIPELINE();
