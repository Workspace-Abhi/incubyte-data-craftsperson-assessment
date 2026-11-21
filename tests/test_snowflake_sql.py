"""Unit tests validating Snowflake DDL and Transformation SQL scripts."""
from pathlib import Path


def test_ddl_and_transformations_scripts_exist():
    base_sql_dir = Path(__file__).resolve().parent.parent / "sql" / "snowflake"
    ddl_path = base_sql_dir / "ddl.sql"
    transformations_path = base_sql_dir / "transformations.sql"
    assert ddl_path.exists(), "ddl.sql must exist"
    assert transformations_path.exists(), "transformations.sql must exist"


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

    # Static generation pointer and include instruction (replaces scripting block)
    assert "generated_country_merges.sql" in content
    assert "!include sql/snowflake/generated_country_merges.sql" in content

    # Task 1 & 2: SOURCE_SYSTEM composite dedup and member loyalty mart
    assert "PARTITION BY SOURCE_SYSTEM, MEMBER_ID" in content
    assert "Ambiguous member_id across source systems" in content
    assert "m.SOURCE_SYSTEM" in content

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

    # Key collisions resolution before dedupe
    assert "Key collision: superseded by newer record" in content
    assert "COUNT(DISTINCT LOWER(TRIM(MEMBER_NAME))) > 1" in content

    # Redemptions DLQ & Idempotent MERGE
    assert "STG_REDEMPTIONS_FLATTENED" in content
    assert "Invalid feed_date in redemption feed" in content
    assert "CURRENT_DATE()" not in content.split("STEP 4")[1].split("STEP 5")[0]
    assert "Invalid miles_redeemed: value cannot be negative" in content
    assert "MERGE INTO STG_REDEMPTIONS_FLATTENED" in content
    assert "target.TXN_ID = src.TXN_ID" in content

    # Task 4: Idempotent MERGE into MEMBER_LOYALTY_360 from MEMBERS_ALL for affected members
    assert "MERGE INTO MEMBER_LOYALTY_360 target" in content
    assert "FROM MEMBERS_ALL m" in content
    assert "WHERE EXISTS" in content.split("STEP 5")[1]
    assert "CREATE OR REPLACE TABLE MEMBER_LOYALTY_360" not in content


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
