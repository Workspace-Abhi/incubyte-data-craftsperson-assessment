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

    COUNTRY_TABLE_MAP = {k: CANONICAL_TABLE_MAP[v] for k, v in CANONICAL_COUNTRY_MAP.items()}

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
                source_file VARCHAR,
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
                extract_ts TIMESTAMP,
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
                        ORDER BY COALESCE(extract_ts, TIMESTAMP '1970-01-01 00:00:00') DESC,
                                 COALESCE(last_flight_date, enrollment_date) DESC,
                                 COALESCE(source_file, '') DESC,
                                 COALESCE(line_no, 0) DESC
                    ) AS rn
                FROM stg_member_profile
            )
            SELECT * EXCLUDE (rn)
            FROM Ranked
            WHERE rn = 1;
        """)

        # 2. Ensure all canonical country target tables exist
        for tbl in CANONICAL_TABLE_MAP.values():
            self.ensure_target_table(tbl)

        # 3. Incremental Country Merges: Delete existing row only if incoming batch record is newer
        countries_df = self.conn.execute("SELECT DISTINCT country FROM tmp_latest_members").fetchall()
        country_counts: Dict[str, int] = {}

        # View of all current records across country tables to prevent older late-arriving records from inserting
        union_sql = " UNION ALL ".join([f"SELECT source_system, member_id, country, extract_date, extract_ts, last_flight_date, enrollment_date, source_file, line_no FROM {t}" for t in CANONICAL_TABLE_MAP.values()])
        self.conn.execute(f"CREATE OR REPLACE TEMP VIEW tmp_all_country_members AS {union_sql};")

        for (country_val,) in countries_df:
            target_table = self.get_table_name_for_country(country_val)

            # Delete existing row in target_table ONLY if incoming record is newer or equal on tie (incoming later load wins)
            self.conn.execute(f"""
                DELETE FROM {target_table} target
                WHERE EXISTS (
                    SELECT 1 FROM tmp_latest_members src
                    WHERE src.source_system = target.source_system
                      AND src.member_id = target.member_id
                      AND src.country = ?
                      AND (
                          COALESCE(src.extract_ts, TIMESTAMP '1970-01-01 00:00:00') > COALESCE(target.extract_ts, TIMESTAMP '1970-01-01 00:00:00')
                          OR (
                              COALESCE(src.extract_ts, TIMESTAMP '1970-01-01 00:00:00') = COALESCE(target.extract_ts, TIMESTAMP '1970-01-01 00:00:00')
                              AND COALESCE(src.last_flight_date, src.enrollment_date) >= COALESCE(target.last_flight_date, target.enrollment_date)
                          )
                          OR target.extract_ts IS NULL
                      )
                );
            """, [country_val])

            # Insert incoming record if it does not already exist in target_table or any country table with a strictly newer record
            self.conn.execute(f"""
                INSERT INTO {target_table} (
                    source_system, source_file, member_id, member_name, enrollment_date, last_flight_date,
                    tier_code, agent_name, state, country, post_code, dob,
                    is_active, age, stale_member, extract_date, extract_ts, line_no, last_updated_at
                )
                SELECT
                    src.source_system, src.source_file, src.member_id, src.member_name, src.enrollment_date, src.last_flight_date,
                    src.tier_code, src.agent_name, src.state, src.country, src.post_code, src.dob,
                    src.is_active, src.age, src.stale_member, src.extract_date, src.extract_ts, src.line_no, CURRENT_TIMESTAMP
                FROM tmp_latest_members src
                WHERE src.country = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM {target_table} t
                      WHERE t.source_system = src.source_system
                        AND t.member_id = src.member_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM tmp_all_country_members existing
                      WHERE existing.source_system = src.source_system
                        AND existing.member_id = src.member_id
                        AND (
                            COALESCE(existing.extract_ts, TIMESTAMP '1970-01-01 00:00:00') > COALESCE(src.extract_ts, TIMESTAMP '1970-01-01 00:00:00')
                            OR (
                                COALESCE(existing.extract_ts, TIMESTAMP '1970-01-01 00:00:00') = COALESCE(src.extract_ts, TIMESTAMP '1970-01-01 00:00:00')
                                AND COALESCE(existing.last_flight_date, existing.enrollment_date) > COALESCE(src.last_flight_date, src.enrollment_date)
                            )
                        )
                  );
            """, [country_val])

            inserted_count = self.conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            country_counts[target_table] = inserted_count

        # 4. Clean up migration: remove members from other country tables if incoming record has a different country and is newer or equal on tie
        all_canonical_tables = list(set(CANONICAL_TABLE_MAP.values()))
        for tbl in all_canonical_tables:
            self.conn.execute(f"""
                DELETE FROM {tbl} target
                WHERE EXISTS (
                    SELECT 1 FROM tmp_latest_members d
                    WHERE d.source_system = target.source_system
                      AND d.member_id = target.member_id
                      AND '{tbl}' <> CASE
                          WHEN UPPER(d.country) IN ('IND', 'INDIA') THEN 'table_india'
                          WHEN UPPER(d.country) IN ('USA') THEN 'table_usa'
                          WHEN UPPER(d.country) IN ('CAN', 'CANADA') THEN 'table_canada'
                          WHEN UPPER(d.country) IN ('PHIL', 'PH') THEN 'table_philippines'
                          WHEN UPPER(d.country) IN ('AU', 'AUS') THEN 'table_australia'
                          ELSE NULL
                      END
                      AND (
                          COALESCE(d.extract_ts, TIMESTAMP '1970-01-01 00:00:00') > COALESCE(target.extract_ts, TIMESTAMP '1970-01-01 00:00:00')
                          OR (
                              COALESCE(d.extract_ts, TIMESTAMP '1970-01-01 00:00:00') = COALESCE(target.extract_ts, TIMESTAMP '1970-01-01 00:00:00')
                              AND COALESCE(d.last_flight_date, d.enrollment_date) >= COALESCE(target.last_flight_date, target.enrollment_date)
                          )
                          OR target.extract_ts IS NULL
                      )
                );
            """)

        return country_counts

    def get_country_records(self, table_name: str) -> List[Dict[str, Any]]:
        """Fetch all records from a specific country table."""
        self.ensure_target_table(table_name)
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
