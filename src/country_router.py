"""Country router and 'latest record wins' deduplication engine."""
import re
from typing import Dict, List, Any, Optional
import duckdb


CANONICAL_COUNTRY_MAP = {
    "IND": "IND",
    "INDIA": "IND",
    "USA": "USA",
    "CAN": "CAN",
    "CANADA": "CAN",
    "PHIL": "PHIL",
    "PH": "PHIL",
    "AU": "AU",
    "AUS": "AU",
}

CANONICAL_TABLE_MAP = {
    "IND": "table_india",
    "USA": "table_usa",
    "CAN": "table_canada",
    "PHIL": "table_philippines",
    "AU": "table_australia",
}

CANONICAL_SNOWFLAKE_TABLE_MAP = {
    "IND": "TABLE_INDIA",
    "USA": "TABLE_USA",
    "CAN": "TABLE_CAN",
    "PHIL": "TABLE_PHIL",
    "AU": "TABLE_AU",
}


def normalize_country(country: Optional[str]) -> Optional[str]:
    """Map country aliases to canonical codes (IND, USA, CAN, PHIL, AU).

    Returns None if country is missing or not in the canonical map.
    """
    if not country:
        return None
    val = str(country).strip().upper()
    return CANONICAL_COUNTRY_MAP.get(val)


class CountryRouter:
    """Manages country-specific target tables with 'latest record wins' deduplication."""

    COUNTRY_TABLE_MAP = {
        "IND": "table_india",
        "INDIA": "table_india",
        "USA": "table_usa",
        "CAN": "table_canada",
        "CANADA": "table_canada",
        "PHIL": "table_philippines",
        "PH": "table_philippines",
        "AU": "table_australia",
        "AUS": "table_australia",
    }

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        self.conn = conn or duckdb.connect(":memory:")

    def get_table_name_for_country(self, country: Optional[str]) -> str:
        """Resolve clean standardized table name for a country code."""
        canonical = normalize_country(country)
        if canonical and canonical in CANONICAL_TABLE_MAP:
            return CANONICAL_TABLE_MAP[canonical]
        raise ValueError(f"Unmapped country: cannot route '{country}' to a target table")

    def ensure_target_table(self, table_name: str) -> None:
        """Ensure a country target table exists with strict identifier validation."""
        if not re.match(r"^table_[a-zA-Z0-9_]+$", table_name):
            raise ValueError(f"Invalid table identifier: '{table_name}'")

        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                source_system VARCHAR NOT NULL DEFAULT 'FLAT_FILE',
                member_id VARCHAR NOT NULL,
                member_name VARCHAR NOT NULL,
                enrollment_date DATE NOT NULL,
                last_flight_date DATE,
                tier_code VARCHAR,
                agent_name VARCHAR,
                state VARCHAR,
                country VARCHAR,
                post_code INTEGER,
                dob DATE,
                is_active VARCHAR DEFAULT 'A',
                age INTEGER,
                stale_member VARCHAR(1),
                extract_date DATE,
                line_no INTEGER,
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_system, member_id)
            );
        """)

    def execute_country_split(self) -> Dict[str, int]:
        """Apply 'latest record wins' rule on staging and route members to country target tables.

        Parameterized execution eliminates SQL injection risks.
        """
        # 1. Deduplicate staging using window function (latest event wins per source_system and member_id)
        self.conn.execute("""
            CREATE OR REPLACE TEMP TABLE tmp_latest_members AS
            WITH Ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY source_system, member_id
                        ORDER BY COALESCE(extract_date, DATE '1970-01-01') DESC,
                                 COALESCE(last_flight_date, enrollment_date) DESC,
                                 ingested_at DESC,
                                 COALESCE(line_no, 0) DESC
                    ) AS rn
                FROM stg_member_profile
            )
            SELECT * EXCLUDE (rn)
            FROM Ranked
            WHERE rn = 1;
        """)

        # 2. Get distinct countries present in the deduplicated batch
        countries_df = self.conn.execute("SELECT DISTINCT country FROM tmp_latest_members").fetchall()
        country_counts: Dict[str, int] = {}

        for (country_val,) in countries_df:
            target_table = self.get_table_name_for_country(country_val)
            self.ensure_target_table(target_table)

            if country_val is None:
                self.conn.execute(f"""
                    DELETE FROM {target_table}
                    WHERE (source_system, member_id) IN (
                        SELECT source_system, member_id FROM tmp_latest_members WHERE country IS NULL
                    );
                """)
                self.conn.execute(f"""
                    INSERT INTO {target_table} (
                        source_system, member_id, member_name, enrollment_date, last_flight_date,
                        tier_code, agent_name, state, country, post_code, dob,
                        is_active, age, stale_member, extract_date, line_no, last_updated_at
                    )
                    SELECT
                        source_system, member_id, member_name, enrollment_date, last_flight_date,
                        tier_code, agent_name, state, country, post_code, dob,
                        is_active, age, stale_member, extract_date, line_no, CURRENT_TIMESTAMP
                    FROM tmp_latest_members
                    WHERE country IS NULL;
                """)
            else:
                self.conn.execute(f"""
                    DELETE FROM {target_table}
                    WHERE (source_system, member_id) IN (
                        SELECT source_system, member_id FROM tmp_latest_members WHERE country = ?
                    );
                """, [country_val])
                self.conn.execute(f"""
                    INSERT INTO {target_table} (
                        source_system, member_id, member_name, enrollment_date, last_flight_date,
                        tier_code, agent_name, state, country, post_code, dob,
                        is_active, age, stale_member, extract_date, line_no, last_updated_at
                    )
                    SELECT
                        source_system, member_id, member_name, enrollment_date, last_flight_date,
                        tier_code, agent_name, state, country, post_code, dob,
                        is_active, age, stale_member, extract_date, line_no, CURRENT_TIMESTAMP
                    FROM tmp_latest_members
                    WHERE country = ?;
                """, [country_val])

            inserted_count = self.conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            country_counts[target_table] = inserted_count

        # 3. Clean up migration: remove members from other country tables if their latest country changed
        all_tables = [self.get_table_name_for_country(c[0]) for c in countries_df]
        for tbl in all_tables:
            self.conn.execute(f"""
                DELETE FROM {tbl}
                WHERE (source_system, member_id) IN (
                    SELECT source_system, member_id FROM tmp_latest_members
                    WHERE '{tbl}' <> CASE
                        WHEN UPPER(country) IN ('IND', 'INDIA') THEN 'table_india'
                        WHEN UPPER(country) IN ('USA') THEN 'table_usa'
                        WHEN UPPER(country) IN ('CAN', 'CANADA') THEN 'table_canada'
                        WHEN UPPER(country) IN ('PHIL', 'PH') THEN 'table_philippines'
                        WHEN UPPER(country) IN ('AU', 'AUS') THEN 'table_australia'
                        ELSE 'table_unknown'
                    END
                );
            """)

        return country_counts

    def get_country_records(self, table_name: str) -> List[Dict[str, Any]]:
        """Fetch all records from a given country table with native Python types."""
        self.ensure_target_table(table_name)
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
