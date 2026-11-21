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


def test_snowflake_transformations_scripting_and_merge():
    sql_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "transformations.sql"
    content = sql_path.read_text(encoding="utf-8")

    # Mapping-driven scripting block
    assert "EXECUTE IMMEDIATE" in content
    assert "IDENTIFIER(:1)" in content
    assert "CURSOR FOR SELECT DISTINCT TARGET_TABLE FROM REF_COUNTRY_MAP" in content

    # Task 1: SOURCE_SYSTEM composite dedup and merge/delete join keys
    assert "PARTITION BY SOURCE_SYSTEM, MEMBER_ID" in content
    assert "target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID" in content
    assert "d.SOURCE_SYSTEM = target.SOURCE_SYSTEM" in content
    assert "Ambiguous member_id across source systems" in content
    assert "m.SOURCE_SYSTEM" in content

    # Join-based anti-check DELETE
    assert "WHERE EXISTS" in content
    assert "m.TARGET_TABLE <> :1" in content

    # Unmapped country quarantine
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

    # Non-execution disclaimer
    assert "was not executed against a live Snowflake account" in content
