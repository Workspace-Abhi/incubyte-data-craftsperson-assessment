"""Contract and text inspection tests validating Snowflake DDL, transformations, and smoke test scripts.

NOTE: These tests inspect file contents, structural syntax contracts, and SQL integrity
locally via pytest. Real behavioral execution and query evaluation is performed directly
against a live Snowflake warehouse using `sql/snowflake/smoke_test.sql`.
"""
from pathlib import Path


def test_ddl_and_transformations_scripts_exist():
    base_sql_dir = Path(__file__).resolve().parent.parent / "sql" / "snowflake"
    ddl_path = base_sql_dir / "ddl.sql"
    transformations_path = base_sql_dir / "transformations.sql"
    merges_path = base_sql_dir / "generated_country_merges.sql"
    mart_finalize_path = base_sql_dir / "03_mart_and_finalize.sql"
    run_all_path = base_sql_dir / "run_all.sql"
    smoke_test_path = base_sql_dir / "smoke_test.sql"

    assert ddl_path.exists(), "ddl.sql must exist"
    assert transformations_path.exists(), "transformations.sql must exist"
    assert merges_path.exists(), "generated_country_merges.sql must exist"
    assert mart_finalize_path.exists(), "03_mart_and_finalize.sql must exist"
    assert run_all_path.exists(), "run_all.sql must exist"
    assert smoke_test_path.exists(), "smoke_test.sql must exist"


def test_snowflake_smoke_test_coverage():
    smoke_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "smoke_test.sql"
    content = smoke_path.read_text(encoding="utf-8")

    # Schema isolation
    assert "SKYPOINTS_SMOKE_TEST" in content

    # Assert no copied transformation or DDL logic
    assert "INSERT INTO RAW_DLQ_QUARANTINE" not in content, "smoke_test.sql must not copy transformation DLQ logic"
    assert "MERGE INTO" not in content, "smoke_test.sql must not copy MERGE transformation logic"
    assert "CREATE OR REPLACE TEMPORARY TABLE DEDUPED" not in content, "smoke_test.sql must not copy dedupe logic"

    # Assert sourcing of run_all.sql once for run 1 and once for rerun
    assert "!source sql/snowflake/run_all.sql" in content
    assert content.count("!source sql/snowflake/run_all.sql") >= 2, "smoke_test.sql must test rerun idempotency by sourcing run_all.sql twice"

    # Coverage for test scenarios
    assert "Member moving country" in content
    assert "Composite key: Same ID in IND_SYS and USA_SYS both survive" in content
    assert "Rename collision: older name quarantined in DLQ" in content
    assert "Rename collision: newer name kept in target table" in content
    assert "Empty-string tier code: treated as null, not quarantined" in content
    assert "Unmapped country: quarantined in DLQ at staging" in content
    assert "Null country: quarantined in DLQ at staging" in content
    assert "Bad redemption feed_date: quarantined into DLQ" in content
    assert "Ambiguous member_id redemption: quarantined into DLQ" in content

    # Mart assertions before rerun and count snapshot
    assert "PRE_RERUN_SNAPSHOT" in content
    assert "Mart before rerun: MEMBER_LOYALTY_360 total member rows" in content
    assert "Mart before rerun: MOVE_101 country updated to IND" in content
    assert "Mart before rerun: MOVE_101 COMPLETED miles summed correctly" in content
    assert "Mart before rerun: MOVE_101 PENDING miles tracked separately" in content

    # Rerun delta equality checks
    assert "Rerun delta: STG_MEMBER_PROFILE row count unchanged" in content
    assert "Rerun delta: RAW_DLQ_QUARANTINE row count unchanged" in content
    assert "Rerun delta: STG_REDEMPTIONS_FLATTENED row count unchanged" in content
    assert "Rerun delta: MEMBER_LOYALTY_360 row count unchanged" in content

    # Final assertion status reporting
    assert "scenario," in content
    assert "expected," in content
    assert "actual," in content
    assert "CASE WHEN expected = actual THEN 'PASS' ELSE 'FAIL' END AS status" in content


def test_run_all_script_order():
    run_all_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "run_all.sql"
    content = run_all_path.read_text(encoding="utf-8")

    assert "run_batch_id is a session variable" in content

    pos_ddl = content.find("!source sql/snowflake/ddl.sql")
    pos_trans = content.find("!source sql/snowflake/transformations.sql")
    pos_merges = content.find("!source sql/snowflake/generated_country_merges.sql")
    pos_finalize = content.find("!source sql/snowflake/03_mart_and_finalize.sql")

    assert pos_ddl != -1, "run_all.sql must source ddl.sql"
    assert pos_trans != -1, "run_all.sql must source transformations.sql"
    assert pos_merges != -1, "run_all.sql must source generated_country_merges.sql"
    assert pos_finalize != -1, "run_all.sql must source 03_mart_and_finalize.sql"
    assert pos_ddl < pos_trans < pos_merges < pos_finalize, (
        "run_all.sql must execute in order: ddl -> transformations -> merges -> 03_mart_and_finalize"
    )


def test_snowflake_ddl_ref_country_map():
    ddl_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "ddl.sql"
    content = ddl_path.read_text(encoding="utf-8")
    assert "REF_COUNTRY_MAP" in content
    assert "'TABLE_INDIA'" in content
    assert "'TABLE_USA'" in content
    assert "'TABLE_CAN'" in content
    assert "'TABLE_PHIL'" in content
    assert "'TABLE_AU'" in content

    # Task 1: SOURCE_SYSTEM in DDL and composite primary keys
    assert "SOURCE_SYSTEM        VARCHAR(30)      NOT NULL DEFAULT 'FLAT_FILE'" in content
    assert "PRIMARY KEY (SOURCE_SYSTEM, MEMBER_ID)" in content

    # Task 2: CANONICAL_CODE in REF_COUNTRY_MAP
    assert "CANONICAL_CODE       VARCHAR(10)      NOT NULL" in content
    assert "'IND' AS canonical" in content
    assert "'AUS', 'AU', 'TABLE_AU'" in content

    # Batch tracking and row ID columns in DDL
    assert "BATCH_ID             VARCHAR(50)" in content
    assert "PROCESSED_AT         TIMESTAMP_NTZ" in content
    assert "STG_ROW_ID          VARCHAR          DEFAULT UUID_STRING()" in content

    # Task 4: MEMBERS_ALL unified country view
    assert "CREATE OR REPLACE VIEW MEMBERS_ALL AS" in content
    assert "SELECT * FROM TABLE_INDIA" in content
    assert "SELECT * FROM TABLE_USA" in content
    assert "SELECT * FROM TABLE_CAN" in content
    assert "SELECT * FROM TABLE_PHIL" in content
    assert "SELECT * FROM TABLE_AU" in content


def test_snowflake_transformations_and_merge():
    sql_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "transformations.sql"
    content = sql_path.read_text(encoding="utf-8")
    finalize_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "03_mart_and_finalize.sql"
    finalize_content = finalize_path.read_text(encoding="utf-8")

    # Static generation pointer and include instruction (replaces scripting block)
    assert "generated_country_merges.sql" in content
    assert "!source sql/snowflake/generated_country_merges.sql" in content

    # Batch tracking & landing filters
    assert "SET run_batch_id = UUID_STRING();" in content
    assert "WHERE PROCESSED_AT IS NULL" in content
    assert "UPDATE RAW_MEMBER_LANDING" in finalize_content
    assert "UPDATE RAW_REDEMPTIONS_LANDING" in finalize_content
    assert "SET PROCESSED_AT = CURRENT_TIMESTAMP(), BATCH_ID = $run_batch_id" in finalize_content

    # Task 1 & 2: SOURCE_SYSTEM composite dedup and member loyalty mart
    assert "PARTITION BY SOURCE_SYSTEM, MEMBER_ID" in content
    assert "Ambiguous member_id across source systems" in finalize_content
    assert "m.SOURCE_SYSTEM" in finalize_content

    # Unmapped country quarantine at staging
    assert "Unmapped country: Country is required and cannot be empty" in content
    assert "'Unmapped country'" in content
    assert "LEFT JOIN REF_COUNTRY_MAP" in content

    # Parity with Python validator & string sanitization
    assert "NULLIF(TRIM(" in content
    assert "'NULL', 'NONE'" in content
    assert "Invalid Post_Code: must be numeric" in content
    assert "Invalid domain value: Tier_Code not in (PLT, GLD, SLV)" in content
    assert "Invalid domain value: Is_Active must be in (A, I)" in content
    assert "Invalid Last_Flight_Date: unparseable calendar date" in content

    # Key collisions resolution before dedupe & deterministic tie-breaking
    assert "Key collision: superseded by newer record" in content
    assert "HAVING COUNT(CASE WHEN BATCH_ID = $run_batch_id THEN 1 END) > 0" in content
    assert "COUNT(DISTINCT LOWER(TRIM(MEMBER_NAME))) > 1" in content
    assert "ORDER BY COALESCE(s.LAST_FLIGHT_DATE, s.ENROLLMENT_DATE) DESC, s.INGESTED_AT DESC, s.STG_ROW_ID" in content
    assert "ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE) DESC, INGESTED_AT DESC, STG_ROW_ID" in content
    assert "DELETE FROM STG_MEMBER_PROFILE" not in content

    # Redemptions DLQ & Idempotent MERGE
    assert "STG_REDEMPTIONS_FLATTENED" in content
    assert "Invalid feed_date in redemption feed" in content
    assert "CURRENT_DATE()" not in content.split("STEP 4")[1]
    assert "Invalid miles_redeemed: value cannot be negative" in content
    assert "MERGE INTO STG_REDEMPTIONS_FLATTENED" in content
    assert "target.TXN_ID = src.TXN_ID" in content

    # Task 4: Idempotent MERGE into MEMBER_LOYALTY_360 from MEMBERS_ALL for affected members
    assert "MERGE INTO MEMBER_LOYALTY_360 target" in finalize_content
    assert "FROM MEMBERS_ALL m" in finalize_content
    assert "WHERE EXISTS" in finalize_content.split("STEP 5")[1]
    assert "red.BATCH_ID = $run_batch_id" in finalize_content
    assert "CREATE OR REPLACE TABLE MEMBER_LOYALTY_360" not in finalize_content


def test_generated_country_merges_matches_script_output():
    from scripts.generate_country_merges import generate_sql
    output_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "generated_country_merges.sql"
    assert output_path.exists(), "generated_country_merges.sql must exist"
    expected = generate_sql()
    actual = output_path.read_text(encoding="utf-8")
    assert actual.strip() == expected.strip(), "generated_country_merges.sql must match generate_country_merges.py output"

    # Static SQL assertions on generated country merges
    assert "MERGE INTO TABLE_INDIA target" in actual
    assert "MERGE INTO TABLE_USA target" in actual
    assert "MERGE INTO TABLE_CAN target" in actual
    assert "MERGE INTO TABLE_PHIL target" in actual
    assert "MERGE INTO TABLE_AU target" in actual
    assert "target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID" in actual
    assert "d.SOURCE_SYSTEM = target.SOURCE_SYSTEM" in actual
    assert "d.COUNTRY <> 'IND'" in actual
