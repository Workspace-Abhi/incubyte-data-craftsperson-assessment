-- ============================================================================
-- SkyPoints Step 3: Analytical Mart Build & Finalize Landing Batch Tracking
-- NOTE: run_batch_id is a session variable initialized in transformations.sql.
-- All four scripts must execute in the same SnowSQL session in order.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 5: Loyalty 360 Analytical Mart (Sums Only COMPLETED Miles)
-- ----------------------------------------------------------------------------

-- 1. Idempotently quarantine ambiguous redemptions whose MEMBER_ID spans multiple SOURCE_SYSTEMs
MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        r.TXN_ID AS RECORD_ID,
        CONCAT_WS('|', r.TXN_ID, r.MEMBER_ID, r.FEED_DATE, r.TXN_DATE, r.PARTNER, r.MILES_REDEEMED, r.STATUS) AS RAW_RECORD,
        'Ambiguous member_id across source systems' AS ERROR_REASON,
        'STG_REDEMPTIONS_FLATTENED' AS SOURCE_FEED
    FROM STG_REDEMPTIONS_FLATTENED r
    WHERE r.MEMBER_ID IN (
        SELECT MEMBER_ID
        FROM DEDUPED_LATEST_MEMBERS
        GROUP BY MEMBER_ID
        HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
    )
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, ERROR_REASON, SOURCE_FEED, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.ERROR_REASON, src.SOURCE_FEED, CURRENT_TIMESTAMP());

DELETE FROM STG_REDEMPTIONS_FLATTENED
WHERE MEMBER_ID IN (
    SELECT MEMBER_ID
    FROM DEDUPED_LATEST_MEMBERS
    GROUP BY MEMBER_ID
    HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
);

-- 2. Build or Refresh MEMBER_LOYALTY_360 Mart (Idempotent MERGE for affected members)
-- Keyed on (SOURCE_SYSTEM, MEMBER_ID) from MEMBERS_ALL; touches only members affected by this batch
MERGE INTO MEMBER_LOYALTY_360 target
USING (
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
        COALESCE(r.TOP_PARTNER, 'None') AS TOP_PARTNER
    FROM MEMBERS_ALL m
    LEFT JOIN RedemptionSummary r ON m.MEMBER_ID = r.MEMBER_ID
    WHERE EXISTS (
        SELECT 1 FROM DEDUPED_LATEST_MEMBERS d
        WHERE d.SOURCE_SYSTEM = m.SOURCE_SYSTEM AND d.MEMBER_ID = m.MEMBER_ID
    )
    OR EXISTS (
        SELECT 1 FROM STG_REDEMPTIONS_FLATTENED red
        WHERE red.MEMBER_ID = m.MEMBER_ID AND red.BATCH_ID = $run_batch_id
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
-- STEP 6: Mark Processed Records in Landing Tables for Batch Tracking
-- ----------------------------------------------------------------------------
UPDATE RAW_MEMBER_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP(), BATCH_ID = $run_batch_id
WHERE PROCESSED_AT IS NULL;

UPDATE RAW_REDEMPTIONS_LANDING
SET PROCESSED_AT = CURRENT_TIMESTAMP(), BATCH_ID = $run_batch_id
WHERE PROCESSED_AT IS NULL;
