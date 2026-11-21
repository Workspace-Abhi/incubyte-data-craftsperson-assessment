"""Staging engine and derived column computations (Age and Stale_Member)."""
from datetime import date
from typing import Optional, List, Dict, Any
import duckdb
from src.models import CleanMemberRecord


def compute_age(dob: Optional[date], reference_date: Optional[date] = None) -> Optional[int]:
    """Compute member age from date of birth against reference_date (defaults to today)."""
    if dob is None:
        return None
    ref = reference_date or date.today()
    if dob > ref:
        return None
    age = ref.year - dob.year - ((ref.month, ref.day) < (dob.month, dob.day))
    return age


def compute_stale_member(flight_date: Optional[date], reference_date: Optional[date] = None, threshold_days: int = 90) -> str:
    """Compute Stale_Member flag ('Y' or 'N').

    Flagged as 'Y' if days since Flight_Date > threshold_days (90) or if flight_date is None.
    """
    if flight_date is None:
        return "Y"
    ref = reference_date or date.today()
    days = (ref - flight_date).days
    return "Y" if days > threshold_days else "N"


class StagingEngine:
    """Manages DuckDB in-memory staging table with derived columns."""

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        self.conn = conn or duckdb.connect(":memory:")
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize staging schema in DuckDB."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_member_profile (
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
                line_no INTEGER,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def load_clean_records(self, records: List[CleanMemberRecord], reference_date: Optional[date] = None) -> int:
        """Enrich clean records with Age and Stale_Member and insert into staging."""
        if not records:
            return 0

        rows = []
        for r in records:
            age = compute_age(r.dob, reference_date=reference_date)
            stale = compute_stale_member(r.last_flight_date, reference_date=reference_date)
            rows.append((
                r.source_system or "FLAT_FILE",
                r.source_file,
                r.member_id,
                r.member_name,
                r.enrollment_date,
                r.last_flight_date,
                r.tier_code,
                r.agent_name,
                r.state,
                r.country,
                r.post_code,
                r.dob,
                r.is_active or "A",
                age,
                stale,
                r.extract_date,
                r.line_no,
            ))

        self.conn.executemany("""
            INSERT INTO stg_member_profile (
                source_system, source_file, member_id, member_name, enrollment_date, last_flight_date,
                tier_code, agent_name, state, country, post_code, dob,
                is_active, age, stale_member, extract_date, line_no
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, rows)

        return len(rows)

    def get_staged_records(self) -> List[Dict[str, Any]]:
        """Fetch all staged records as a list of dictionaries with native Python types."""
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM stg_member_profile")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
