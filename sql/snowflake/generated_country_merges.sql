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
        EXTRACT_DATE,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS
    WHERE COUNTRY = 'AU'
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN
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
        target.EXTRACT_DATE = src.EXTRACT_DATE,
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
        EXTRACT_DATE,
        LINE_NO,
        LAST_UPDATED_AT
    )
    VALUES (
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
        src.EXTRACT_DATE,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_AU if latest country changed
DELETE FROM TABLE_AU target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'AU'
);

-- ----------------------------------------------------------------------------
-- Target Country: CAN -> TABLE_CAN
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_CAN target
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
        EXTRACT_DATE,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS
    WHERE COUNTRY = 'CAN'
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN
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
        target.EXTRACT_DATE = src.EXTRACT_DATE,
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
        EXTRACT_DATE,
        LINE_NO,
        LAST_UPDATED_AT
    )
    VALUES (
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
        src.EXTRACT_DATE,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_CAN if latest country changed
DELETE FROM TABLE_CAN target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'CAN'
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
        EXTRACT_DATE,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS
    WHERE COUNTRY = 'IND'
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN
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
        target.EXTRACT_DATE = src.EXTRACT_DATE,
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
        EXTRACT_DATE,
        LINE_NO,
        LAST_UPDATED_AT
    )
    VALUES (
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
        src.EXTRACT_DATE,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_INDIA if latest country changed
DELETE FROM TABLE_INDIA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'IND'
);

-- ----------------------------------------------------------------------------
-- Target Country: PHIL -> TABLE_PHIL
-- ----------------------------------------------------------------------------
MERGE INTO TABLE_PHIL target
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
        EXTRACT_DATE,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS
    WHERE COUNTRY = 'PHIL'
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN
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
        target.EXTRACT_DATE = src.EXTRACT_DATE,
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
        EXTRACT_DATE,
        LINE_NO,
        LAST_UPDATED_AT
    )
    VALUES (
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
        src.EXTRACT_DATE,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_PHIL if latest country changed
DELETE FROM TABLE_PHIL target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'PHIL'
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
        EXTRACT_DATE,
        LINE_NO
    FROM DEDUPED_LATEST_MEMBERS
    WHERE COUNTRY = 'USA'
) src
ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID
WHEN MATCHED THEN
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
        target.EXTRACT_DATE = src.EXTRACT_DATE,
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
        EXTRACT_DATE,
        LINE_NO,
        LAST_UPDATED_AT
    )
    VALUES (
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
        src.EXTRACT_DATE,
        src.LINE_NO,
        CURRENT_TIMESTAMP()
    );

-- Cross-country migration: delete member from TABLE_USA if latest country changed
DELETE FROM TABLE_USA target
WHERE EXISTS (
    SELECT 1
    FROM DEDUPED_LATEST_MEMBERS d
    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM
      AND d.MEMBER_ID = target.MEMBER_ID
      AND d.COUNTRY <> 'USA'
);

