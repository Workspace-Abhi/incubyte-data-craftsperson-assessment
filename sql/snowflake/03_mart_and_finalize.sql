-- ============================================================================
-- SkyPoints Step 3: Analytical Mart Build & Finalize Landing Batch Tracking
-- NOTE: run_batch_id is a session variable initialized in transformations.sql.
-- All four scripts must execute in the same SnowSQL session in order.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 5: Loyalty 360 Analytical Mart (Sums Only COMPLETED Miles)
-- ----------------------------------------------------------------------------

-- 1. Flag and idempotently quarantine ambiguous redemptions whose MEMBER_ID spans multiple SOURCE_SYSTEMs
-- History is preserved non-destructively by setting IS_AMBIGUOUS flag rather than deleting.
UPDATE STG_REDEMPTIONS_FLATTENED r
SET IS_AMBIGUOUS = CASE
    WHEN r.MEMBER_ID IN (
        SELECT MEMBER_ID
        FROM MEMBERS_ALL_CURRENT
        GROUP BY MEMBER_ID
        HAVING COUNT(DISTINCT SOURCE_SYSTEM) > 1
    ) THEN TRUE
    ELSE FALSE
END;

MERGE INTO RAW_DLQ_QUARANTINE target
USING (
    SELECT
        r.TXN_ID AS RECORD_ID,
        CONCAT_WS('|', r.TXN_ID, r.MEMBER_ID, r.FEED_DATE, r.TXN_DATE, r.PARTNER, r.MILES_REDEEMED, r.STATUS) AS RAW_RECORD,
        SHA2(CONCAT_WS('|', r.TXN_ID, r.MEMBER_ID, r.FEED_DATE, r.TXN_DATE, r.PARTNER, r.MILES_REDEEMED, r.STATUS), 256) AS RAW_RECORD_HASH,
        'Ambiguous member_id across source systems' AS ERROR_REASON,
        'STG_REDEMPTIONS_FLATTENED' AS SOURCE_FEED,
        'PARTNER_JSON' AS SOURCE_SYSTEM,
        'stg_redemptions' AS SOURCE_FILE_NAME,
        $run_batch_id AS BATCH_ID
    FROM STG_REDEMPTIONS_FLATTENED r
    WHERE r.IS_AMBIGUOUS = TRUE
) src
ON target.RECORD_ID = src.RECORD_ID
AND target.ERROR_REASON = src.ERROR_REASON
AND target.SOURCE_FEED = src.SOURCE_FEED
AND target.RAW_RECORD_HASH = src.RAW_RECORD_HASH
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, RAW_RECORD, RAW_RECORD_HASH, ERROR_REASON, SOURCE_FEED, SOURCE_SYSTEM, SOURCE_FILE_NAME, BATCH_ID, QUARANTINED_AT)
    VALUES (src.RECORD_ID, src.RAW_RECORD, src.RAW_RECORD_HASH, src.ERROR_REASON, src.SOURCE_FEED, src.SOURCE_SYSTEM, src.SOURCE_FILE_NAME, src.BATCH_ID, CURRENT_TIMESTAMP());

-- 2. Build or Refresh MEMBER_LOYALTY_360 Mart (Idempotent MERGE for affected members)
-- Keyed on (SOURCE_SYSTEM, MEMBER_ID) from MEMBERS_ALL_CURRENT; touches only members affected by this batch.
-- Excludes IS_AMBIGUOUS = TRUE transactions from analytical calculations.
-- Breaks MODE(PARTNER) ties deterministically by highest count then alphabetical.
MERGE INTO MEMBER_LOYALTY_360 target
USING (
    WITH TouchedMembers AS (
        SELECT SOURCE_SYSTEM, MEMBER_ID
        FROM DEDUPED_LATEST_MEMBERS
        UNION
        SELECT m.SOURCE_SYSTEM, red.MEMBER_ID
        FROM STG_REDEMPTIONS_FLATTENED red
        JOIN MEMBERS_ALL_CURRENT m ON red.MEMBER_ID = m.MEMBER_ID
        WHERE red.BATCH_ID = $run_batch_id
    ),
    PartnerCounts AS (
        SELECT
            r.MEMBER_ID,
            r.PARTNER,
            COUNT(*) AS CNT,
            ROW_NUMBER() OVER (PARTITION BY r.MEMBER_ID ORDER BY COUNT(*) DESC, r.PARTNER ASC) AS RN
        FROM STG_REDEMPTIONS_FLATTENED r
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON r.MEMBER_ID = tm.MEMBER_ID
        WHERE IS_AMBIGUOUS = FALSE AND r.PARTNER IS NOT NULL
        GROUP BY r.MEMBER_ID, r.PARTNER
    ),
    TopPartner AS (
        SELECT MEMBER_ID, PARTNER AS TOP_PARTNER
        FROM PartnerCounts
        WHERE RN = 1
    ),
    RedemptionSummary AS (
        SELECT
            r.MEMBER_ID,
            COUNT(DISTINCT r.TXN_ID) AS TOTAL_REDEMPTIONS,
            COALESCE(SUM(CASE WHEN r.STATUS = 'COMPLETED' THEN r.MILES_REDEEMED ELSE 0 END), 0) AS TOTAL_MILES_REDEEMED,
            COALESCE(SUM(CASE WHEN r.STATUS = 'PENDING' THEN r.MILES_REDEEMED ELSE 0 END), 0) AS PENDING_MILES_REDEEMED,
            MAX(r.TXN_DATE) AS LAST_REDEMPTION_DATE
        FROM STG_REDEMPTIONS_FLATTENED r
        JOIN (SELECT DISTINCT MEMBER_ID FROM TouchedMembers) tm ON r.MEMBER_ID = tm.MEMBER_ID
        WHERE IS_AMBIGUOUS = FALSE
        GROUP BY r.MEMBER_ID
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
        COALESCE(tp.TOP_PARTNER, 'None') AS TOP_PARTNER
    FROM MEMBERS_ALL_CURRENT m
    JOIN TouchedMembers tm ON m.SOURCE_SYSTEM = tm.SOURCE_SYSTEM AND m.MEMBER_ID = tm.MEMBER_ID
    LEFT JOIN RedemptionSummary r ON m.MEMBER_ID = r.MEMBER_ID
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
