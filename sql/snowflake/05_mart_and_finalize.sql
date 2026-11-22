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
