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


def test_snowflake_transformations_scripting_and_merge():
    sql_path = Path(__file__).resolve().parent.parent / "sql" / "snowflake" / "transformations.sql"
    content = sql_path.read_text(encoding="utf-8")

    # Mapping-driven scripting block
    assert "EXECUTE IMMEDIATE" in content
    assert "IDENTIFIER(:1)" in content
    assert "CURSOR FOR SELECT DISTINCT TARGET_TABLE FROM REF_COUNTRY_MAP" in content

    # Join-based anti-check DELETE
    assert "WHERE EXISTS" in content
    assert "m.TARGET_TABLE <> :1" in content

    # Unmapped country quarantine
    assert "Unmapped or missing country" in content
    assert "LEFT JOIN REF_COUNTRY_MAP" in content

    # Parity with Python validator
    assert "Invalid Post_Code: must be numeric" in content
    assert "Invalid domain value: Tier_Code not in (PLT, GLD, SLV)" in content
    assert "Invalid domain value: Is_Active must be in (A, I)" in content
    assert "Invalid Last_Flight_Date: unparseable calendar date" in content

    # Redemptions DLQ & Idempotent MERGE
    assert "STG_REDEMPTIONS_FLATTENED" in content
    assert "Invalid miles_redeemed: value cannot be negative" in content
    assert "MERGE INTO STG_REDEMPTIONS_FLATTENED" in content
    assert "target.TXN_ID = src.TXN_ID" in content

    # Non-execution disclaimer
    assert "was not executed against a live Snowflake account" in content
