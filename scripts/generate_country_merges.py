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
        lines.append("    SELECT")
        lines.append("        SOURCE_SYSTEM,")
        lines.append("        MEMBER_ID,")
        lines.append("        MEMBER_NAME,")
        lines.append("        ENROLLMENT_DATE,")
        lines.append("        LAST_FLIGHT_DATE,")
        lines.append("        TIER_CODE,")
        lines.append("        AGENT_NAME,")
        lines.append("        STATE,")
        lines.append("        COUNTRY,")
        lines.append("        POST_CODE,")
        lines.append("        DOB,")
        lines.append("        IS_ACTIVE,")
        lines.append("        AGE,")
        lines.append("        STALE_MEMBER,")
        lines.append("        EXTRACT_DATE,")
        lines.append("        LINE_NO")
        lines.append("    FROM DEDUPED_LATEST_MEMBERS src")
        lines.append(f"    WHERE src.COUNTRY = '{country_code}'")
        lines.append("      AND NOT EXISTS (")
        lines.append("          SELECT 1 FROM MEMBERS_ALL m")
        lines.append("          WHERE m.SOURCE_SYSTEM = src.SOURCE_SYSTEM")
        lines.append("            AND m.MEMBER_ID = src.MEMBER_ID")
        lines.append("            AND (")
        lines.append("                m.EXTRACT_DATE > src.EXTRACT_DATE")
        lines.append("                OR (")
        lines.append("                    m.EXTRACT_DATE = src.EXTRACT_DATE")
        lines.append("                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) > COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)")
        lines.append("                )")
        lines.append("                OR (")
        lines.append("                    m.EXTRACT_DATE = src.EXTRACT_DATE")
        lines.append("                    AND COALESCE(m.LAST_FLIGHT_DATE, m.ENROLLMENT_DATE) = COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE)")
        lines.append("                    AND COALESCE(m.LINE_NO, 0) >= COALESCE(src.LINE_NO, 0)")
        lines.append(f"                    AND m.COUNTRY <> '{country_code}'")
        lines.append("                )")
        lines.append("            )")
        lines.append("      )")
        lines.append(") src")
        lines.append("ON target.SOURCE_SYSTEM = src.SOURCE_SYSTEM AND target.MEMBER_ID = src.MEMBER_ID")
        lines.append("WHEN MATCHED AND (")
        lines.append("    src.EXTRACT_DATE > target.EXTRACT_DATE")
        lines.append("    OR (")
        lines.append("        src.EXTRACT_DATE = target.EXTRACT_DATE")
        lines.append("        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) > COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)")
        lines.append("    )")
        lines.append("    OR (")
        lines.append("        src.EXTRACT_DATE = target.EXTRACT_DATE")
        lines.append("        AND COALESCE(src.LAST_FLIGHT_DATE, src.ENROLLMENT_DATE) = COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)")
        lines.append("        AND COALESCE(src.LINE_NO, 0) >= COALESCE(target.LINE_NO, 0)")
        lines.append("    )")
        lines.append("    OR target.EXTRACT_DATE IS NULL")
        lines.append(") THEN")
        lines.append("    UPDATE SET")
        lines.append("        target.MEMBER_NAME = src.MEMBER_NAME,")
        lines.append("        target.ENROLLMENT_DATE = src.ENROLLMENT_DATE,")
        lines.append("        target.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,")
        lines.append("        target.TIER_CODE = src.TIER_CODE,")
        lines.append("        target.AGENT_NAME = src.AGENT_NAME,")
        lines.append("        target.STATE = src.STATE,")
        lines.append("        target.COUNTRY = src.COUNTRY,")
        lines.append("        target.POST_CODE = src.POST_CODE,")
        lines.append("        target.DOB = src.DOB,")
        lines.append("        target.IS_ACTIVE = src.IS_ACTIVE,")
        lines.append("        target.AGE = src.AGE,")
        lines.append("        target.STALE_MEMBER = src.STALE_MEMBER,")
        lines.append("        target.EXTRACT_DATE = src.EXTRACT_DATE,")
        lines.append("        target.LINE_NO = src.LINE_NO,")
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
        lines.append("        POST_CODE,")
        lines.append("        DOB,")
        lines.append("        IS_ACTIVE,")
        lines.append("        AGE,")
        lines.append("        STALE_MEMBER,")
        lines.append("        EXTRACT_DATE,")
        lines.append("        LINE_NO,")
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
        lines.append("        src.POST_CODE,")
        lines.append("        src.DOB,")
        lines.append("        src.IS_ACTIVE,")
        lines.append("        src.AGE,")
        lines.append("        src.STALE_MEMBER,")
        lines.append("        src.EXTRACT_DATE,")
        lines.append("        src.LINE_NO,")
        lines.append("        CURRENT_TIMESTAMP()")
        lines.append("    );")
        lines.append("")
        lines.append(f"-- Cross-country migration: delete member from {tbl} if latest country changed and incoming is newer or equal on tie")
        lines.append(f"DELETE FROM {tbl} target")
        lines.append("WHERE EXISTS (")
        lines.append("    SELECT 1")
        lines.append("    FROM DEDUPED_LATEST_MEMBERS d")
        lines.append("    WHERE d.SOURCE_SYSTEM = target.SOURCE_SYSTEM")
        lines.append("      AND d.MEMBER_ID = target.MEMBER_ID")
        lines.append(f"      AND d.COUNTRY <> '{country_code}'")
        lines.append("      AND (")
        lines.append("          d.EXTRACT_DATE > target.EXTRACT_DATE")
        lines.append("          OR (")
        lines.append("              d.EXTRACT_DATE = target.EXTRACT_DATE")
        lines.append("              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) > COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)")
        lines.append("          )")
        lines.append("          OR (")
        lines.append("              d.EXTRACT_DATE = target.EXTRACT_DATE")
        lines.append("              AND COALESCE(d.LAST_FLIGHT_DATE, d.ENROLLMENT_DATE) = COALESCE(target.LAST_FLIGHT_DATE, target.ENROLLMENT_DATE)")
        lines.append("              AND COALESCE(d.LINE_NO, 0) >= COALESCE(target.LINE_NO, 0)")
        lines.append("          )")
        lines.append("          OR target.EXTRACT_DATE IS NULL")
        lines.append("      )")
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
