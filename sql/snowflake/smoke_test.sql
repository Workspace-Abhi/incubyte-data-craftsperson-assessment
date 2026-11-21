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
CREATE OR REPLACE TRANSIENT SCHEMA SKYPOINTS_SMOKE_TEST;
USE SCHEMA SKYPOINTS_SMOKE_TEST;

-- Step 2: Execute production DDL
!source sql/snowflake/ddl.sql

-- Step 3: Seed initial historical state & load fixtures into landing
-- Pre-existing historical member in TABLE_USA who moves to India in today's batch
INSERT INTO TABLE_USA (
    SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, AGE, STALE_MEMBER
) VALUES (
    'FLAT_FILE', 'MOVE_101', 'David Miller', DATE '2020-01-01', DATE '2021-05-01',
    'GLD', 'AgentA', 'CA', 'USA', DATE '1985-05-03', 'A', 38, 'Y'
);

-- Member landing fixtures covering all test scenarios (loaded in a single batch)
INSERT INTO RAW_MEMBER_LANDING (
    RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE, SOURCE_FILE_NAME
) VALUES
-- 1. Member MOVE_101 moving country to IND with newer flight date
('D', 'FLAT_FILE', 'MOVE_101', 'David Miller', '20200101', '20230115', 'GLD', 'AgentA', 'MH', 'IND', '03051985', 'A', '40001', 'today_batch.dat'),

-- 2. Same raw ID 'DUP_1' across two distinct source systems
('D', 'IND_SYS', 'DUP_1', 'Vikas Patel', '20220101', '20230615', 'SLV', 'AgentB', 'DL', 'IND', '15081995', 'A', '11001', 'IND.csv'),
('D', 'USA_SYS', 'DUP_1', 'Sam Wilson', '20220101', '20230720', 'PLT', 'AgentC', 'NY', 'USA', '20101990', 'A', '10001', 'USA.csv'),

-- 3. Rename collision loaded in ONE insert statement (identical INGESTED_AT)
('D', 'FLAT_FILE', 'REN_202', 'Alice Brown', '20200101', '20210601', 'GLD', 'AgentD', 'VIC', 'AU', '12121992', 'A', '3000', 'older_event.dat'),
('D', 'FLAT_FILE', 'REN_202', 'Alice Green', '20200101', '20230815', 'GLD', 'AgentD', 'VIC', 'AU', '12121992', 'A', '3000', 'newer_event.dat'),

-- 4. Empty-string Tier Code (should be treated as NULL and valid, not rejected)
('D', 'FLAT_FILE', 'EMPTY_TIER_1', 'Carlos Ray', '20220101', '20230101', '   ', 'AgentE', 'NCR', 'PHIL', '01011990', 'A', '1200', 'ph_batch.dat'),

-- 5. Unmapped country 'GERMANY' (quarantined to DLQ)
('D', 'FLAT_FILE', 'BAD_CTRY_1', 'Hans Gruber', '20220101', '20230101', 'GLD', 'AgentF', 'BER', 'GERMANY', '01011990', 'A', '10115', 'eu_batch.dat'),

-- 6. Null country (quarantined to DLQ)
('D', 'FLAT_FILE', 'NULL_CTRY_1', 'Jane Doe', '20220101', '20230101', 'GLD', 'AgentG', 'CA', NULL, '01011990', 'A', '90210', 'us_batch.dat'),

-- 7. Two distinct rows with missing member_id in the same batch (both must be quarantined, not suppressed)
('D', 'FLAT_FILE', NULL, 'Missing ID Alpha', '20220101', '20230101', 'GLD', 'AgentH', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat'),
('D', 'FLAT_FILE', NULL, 'Missing ID Beta', '20220101', '20230101', 'GLD', 'AgentH', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat'),

-- 8. Same ID in two distinct source systems both bad (both must be quarantined)
('D', 'IND_SYS', 'BAD_DUP_1', 'Bad Ind User', '20220101', '20230101', 'GLD', 'AgentI', 'BER', 'GERMANY', '01011990', 'A', '11001', 'IND_bad.csv'),
('D', 'USA_SYS', 'BAD_DUP_1', 'Bad Usa User', '20220101', '20230101', 'GLD', 'AgentI', 'BER', 'GERMANY', '01011990', 'A', '10001', 'USA_bad.csv'),

-- 9. Bad row arriving in Batch 1 (will be repeated in Batch 2)
('D', 'FLAT_FILE', 'REPEAT_BAD_1', 'Repeat Bad Person', '20220101', '20230101', 'INVALID_TIER', 'AgentJ', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat'),

-- 10. Member BATCH_REN_1 original name in Batch 1 (will be renamed in Batch 2)
('D', 'FLAT_FILE', 'BATCH_REN_1', 'Original Name', '20200101', '20210101', 'GLD', 'AgentK', 'CA', 'USA', '01011990', 'A', '90210', 'batch_1.dat'),

-- 11. Member RESOLVE_101 ambiguous across two sources in Batch 1 (will be resolved in Batch 2)
('D', 'IND_SYS', 'RESOLVE_101', 'Rahul Ind', '20200101', '20210101', 'GLD', 'AgentL', 'MH', 'IND', '01011990', 'A', '40001', 'ind_1.dat'),
('D', 'USA_SYS', 'RESOLVE_101', 'Sam Usa', '20200101', '20210101', 'GLD', 'AgentL', 'CA', 'USA', '01011990', 'A', '90210', 'usa_1.dat');

-- Redemption fixtures
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
!source sql/snowflake/run_all.sql

-- Step 4B: Load Batch 2 arriving in a separate batch
-- Includes:
-- 1. Identical malformed record to test multi-batch reject preservation
-- 2. Rename across batches: BATCH_REN_1 renamed to 'Updated Name' with newer flight date
-- 3. Ambiguity resolution: remove spurious secondary source for RESOLVE_101 so ambiguity disappears
INSERT INTO RAW_MEMBER_LANDING (
    RECORD_TYPE, SOURCE_SYSTEM, MEMBER_ID, MEMBER_NAME, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, POST_CODE, SOURCE_FILE_NAME
) VALUES
('D', 'FLAT_FILE', 'REPEAT_BAD_1', 'Repeat Bad Person', '20220101', '20230101', 'INVALID_TIER', 'AgentJ', 'CA', 'USA', '01011990', 'A', '90210', 'batch_2.dat'),
('D', 'FLAT_FILE', 'BATCH_REN_1', 'Updated Name', '20200101', '20230601', 'GLD', 'AgentK', 'CA', 'USA', '01011990', 'A', '90210', 'batch_2.dat');

-- Resolve ambiguity for RESOLVE_101 before Batch 2 execution
DELETE FROM STG_MEMBER_PROFILE WHERE SOURCE_SYSTEM = 'USA_SYS' AND MEMBER_ID = 'RESOLVE_101';
DELETE FROM TABLE_USA WHERE SOURCE_SYSTEM = 'USA_SYS' AND MEMBER_ID = 'RESOLVE_101';

-- Step 4C: Run production pipeline for Batch 2 via run_all.sql (Run 2: Processes Batch 2)
!source sql/snowflake/run_all.sql

-- Step 5: Capture snapshot of mart and table counts BEFORE rerun into temporary table
CREATE OR REPLACE TEMPORARY TABLE PRE_RERUN_SNAPSHOT AS
SELECT
    (SELECT COUNT(*) FROM STG_MEMBER_PROFILE) AS STG_MEMBER_COUNT,
    (SELECT COUNT(*) FROM RAW_DLQ_QUARANTINE) AS DLQ_COUNT,
    (SELECT COUNT(*) FROM STG_REDEMPTIONS_FLATTENED) AS STG_REDEMP_COUNT,
    (SELECT COUNT(*) FROM MEMBER_LOYALTY_360) AS MART_COUNT,
    (SELECT COUNT(*) FROM MEMBER_LOYALTY_360 WHERE MEMBER_ID = 'MOVE_101' AND COUNTRY = 'IND') AS MOVE_101_COUNTRY_CORRECT,
    (SELECT TO_VARCHAR(COALESCE(TOTAL_MILES_REDEEMED, 0)) FROM MEMBER_LOYALTY_360 WHERE MEMBER_ID = 'MOVE_101') AS MOVE_101_COMPLETED_MILES,
    (SELECT TO_VARCHAR(COALESCE(PENDING_MILES_REDEEMED, 0)) FROM MEMBER_LOYALTY_360 WHERE MEMBER_ID = 'MOVE_101') AS MOVE_101_PENDING_MILES;

-- Step 5B: Execute Idempotent Rerun via run_all.sql (Run 3: Rerun adds no new rows)
-- All landing rows have been stamped with PROCESSED_AT, so a rerun processes 0 new rows
-- and does not duplicate staging, DLQ, or mart records.
!source sql/snowflake/run_all.sql

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

    -- Scenario 1B: Member moving country inserted into new table (TABLE_INDIA)
    SELECT
        2 AS test_id,
        'Member moving country: inserted into new TABLE_INDIA' AS scenario,
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
    FROM TABLE_PHIL
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
        '5' AS expected,
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
        TO_VARCHAR((SELECT COUNT(*) FROM STG_MEMBER_PROFILE) - (SELECT STG_MEMBER_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

    UNION ALL

    -- Scenario 10B: DLQ row count unchanged after rerun (idempotent DLQ merges)
    SELECT
        16 AS test_id,
        'Rerun delta: RAW_DLQ_QUARANTINE row count unchanged' AS scenario,
        '0' AS expected,
        TO_VARCHAR((SELECT COUNT(*) FROM RAW_DLQ_QUARANTINE) - (SELECT DLQ_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

    UNION ALL

    -- Scenario 10C: Staging redemptions count unchanged after rerun
    SELECT
        17 AS test_id,
        'Rerun delta: STG_REDEMPTIONS_FLATTENED row count unchanged' AS scenario,
        '0' AS expected,
        TO_VARCHAR((SELECT COUNT(*) FROM STG_REDEMPTIONS_FLATTENED) - (SELECT STG_REDEMP_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

    UNION ALL

    -- Scenario 10D: Mart row count unchanged after rerun
    SELECT
        18 AS test_id,
        'Rerun delta: MEMBER_LOYALTY_360 row count unchanged' AS scenario,
        '0' AS expected,
        TO_VARCHAR((SELECT COUNT(*) FROM MEMBER_LOYALTY_360) - (SELECT MART_COUNT FROM PRE_RERUN_SNAPSHOT)) AS actual

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
)
SELECT
    scenario,
    expected,
    actual,
    CASE WHEN expected = actual THEN 'PASS' ELSE 'FAIL' END AS status
FROM AssertionResults
ORDER BY test_id;
