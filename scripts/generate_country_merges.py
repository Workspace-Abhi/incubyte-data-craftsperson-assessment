"""Generates static Snowflake MERGE and DELETE statements per target country table.

Eliminates Snowflake Scripting blocks, dynamic IDENTIFIER(:1) execution, and cursor loops.
Single source of truth: CANONICAL_TABLE_MAP / CANONICAL_SNOWFLAKE_TABLE_MAP in src/country_router.py.
"""
import sys
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from src.country_router import CANONICAL_TABLE_MAP, CANONICAL_SNOWFLAKE_TABLE_MAP


def generate_sql() -> str:
    """Generate static Snowflake MERGE and DELETE statements for all canonical countries."""
    lines = [
        "-- ============================================================================",
        "-- Static Country MERGE & Migration DELETE Statements for Snowflake",
        "-- Generated automatically by scripts/generate_country_merges.py",
        "-- Single source of truth: CANONICAL_TABLE_MAP in src/country_router.py",
        "-- ============================================================================",
        "",
    ]

    for country_code in sorted(CANONICAL_TABLE_MAP.keys()):
        tbl = CANONICAL_SNOWFLAKE_TABLE_MAP.get(country_code, f"TABLE_{country_code}")
        lines.append("-- ----------------------------------------------------------------------------")
        lines.append(f"-- Target Country: {country_code} -> {tbl}")
        lines.append("-- ----------------------------------------------------------------------------")
        lines.append(f"MERGE INTO {tbl} target")
        lines.append("USING (")
        lines.append("    SELECT *")
        lines.append("    FROM DEDUPED_LATEST_MEMBERS")
        lines.append(f"    WHERE COUNTRY = '{country_code}'")
        lines.append(") src")
        lines.append("ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID")
        lines.append("WHEN MATCHED THEN")
        lines.append("    UPDATE SET")
        lines.append("        target.MEMBER_NAME = src.MEMBER_NAME,")
        lines.append("        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,")
        lines.append("        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,")
        lines.append("        target.TIER_CODE = src.TIER_CODE,")
        lines.append("        target.AGENT_NAME = src.AGENT_NAME,")
        lines.append("        target.STATE = src.STATE,")
        lines.append("        target.COUNTRY = src.COUNTRY,")
        lines.append("        target.DOB = src.DOB,")
        lines.append("        target.IS_ACTIVE = src.IS_ACTIVE,")
        lines.append("        target.AGE = src.AGE,")
        lines.append("        target.STALE_MEMBER = src.STALE_MEMBER,")
        lines.append("        target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()")
        lines.append("WHEN NOT MATCHED THEN")
        lines.append("    INSERT (")
        lines.append("        SOURCE_SYSTEM,")
        lines.append("        MEMBER_ID,")
        lines.append("        MEMBER_NAME,")
        lines.append("        ENROLLMENT_DATE,")
        lines.append("        LAST_FLIGHT_DATE,")
        lines.append("        TIER_CODE,")
        lines.append("        AGENT_NAME,")
        lines.append("        STATE,")
        lines.append("        COUNTRY,")
        lines.append("        DOB,")
        lines.append("        IS_ACTIVE,")
        lines.append("        AGE,")
        lines.append("        STALE_MEMBER,")
        lines.append("        LAST_UPDATED_AT")
        lines.append("    )")
        lines.append("    VALUES (")
        lines.append("        src.SOURCE_SYSTEM,")
        lines.append("        src.MEMBER_ID,")
        lines.append("        src.MEMBER_NAME,")
        lines.append("        src.ENROLLMENT_DATE,")
        lines.append("        src.LAST_FLIGHT_DATE,")
        lines.append("        src.TIER_CODE,")
        lines.append("        src.AGENT_NAME,")
        lines.append("        src.STATE,")
        lines.append("        src.COUNTRY,")
        lines.append("        src.DOB,")
        lines.append("        src.IS_ACTIVE,")
        lines.append("        src.AGE,")
        lines.append("        src.STALE_MEMBER,")
        lines.append("        CURRENT_TIMESTAMP()")
        lines.append("    );")
        lines.append("")
        lines.append(f"-- Cross-country migration: delete member from {tbl} if latest country changed")
        lines.append(f"DELETE FROM {tbl} target")
        lines.append("WHERE EXISTS (")
        lines.append("    SELECT 1")
        lines.append("    FROM DEDUPED_LATEST_MEMBERS d")
        lines.append("    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM")
        lines.append("      AND d.MEMBER_ID = target.MEMBER_ID")
        lines.append(f"      AND d.COUNTRY <> '{country_code}'")
        lines.append(");")
        lines.append("")

    return "\n".join(lines) + "\n"


def main():
    root_dir = Path(__file__).resolve().parent.parent
    output_file = root_dir / "sql" / "snowflake" / "generated_country_merges.sql"
    sql_content = generate_sql()
    output_file.write_text(sql_content, encoding="utf-8")
    print(f"Generated static country merges in: {output_file}")


if __name__ == "__main__":
    main()
