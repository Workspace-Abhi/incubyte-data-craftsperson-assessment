"""Semi-structured JSON redemption feed parser and Loyalty 360 analytical join."""
from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Any, Optional, Union
import json
import duckdb
from src.validators import parse_and_validate_date, sanitize_string


@dataclass
class FlattenedRedemption:
    txn_id: str
    member_id: str
    feed_date: date
    txn_date: date
    partner: str
    miles_redeemed: int
    status: str


class RedemptionParser:
    """Parses semi-structured partner redemption feeds and loads into queryable tables."""

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        self.conn = conn or duckdb.connect(":memory:")
        self._init_schema()

    def _init_schema(self) -> None:
        """Create flattened redemption staging table and Loyalty 360 mart schema."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_redemptions_flattened (
                txn_id VARCHAR PRIMARY KEY,
                member_id VARCHAR NOT NULL,
                feed_date DATE NOT NULL,
                txn_date DATE NOT NULL,
                partner VARCHAR,
                miles_redeemed INTEGER,
                status VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def parse_payload(self, raw_input: Union[str, dict, list]) -> List[FlattenedRedemption]:
        """Parse raw JSON string, dictionary, or list into flattened redemption records."""
        if isinstance(raw_input, str):
            payload = json.loads(raw_input)
        else:
            payload = raw_input

        # Handle either a single feed dictionary or a list of feed dictionaries
        feed_items = payload if isinstance(payload, list) else [payload]
        flattened: List[FlattenedRedemption] = []

        for feed in feed_items:
            member_id = sanitize_string(feed.get("member_id"))
            feed_dt_str = sanitize_string(feed.get("feed_date"))
            feed_date, _ = parse_and_validate_date(feed_dt_str)
            if not feed_date:
                feed_date = date.today()

            redemptions = feed.get("redemptions", [])
            for txn in redemptions:
                txn_id = sanitize_string(txn.get("txn_id"))
                if not txn_id or not member_id:
                    continue  # Skip invalid transaction without key

                txn_dt_str = sanitize_string(txn.get("txn_date"))
                txn_date, _ = parse_and_validate_date(txn_dt_str)
                if not txn_date:
                    txn_date = feed_date

                partner = sanitize_string(txn.get("partner")) or "Unknown"
                miles = txn.get("miles_redeemed")
                try:
                    miles_int = int(miles) if miles is not None else 0
                except (ValueError, TypeError):
                    miles_int = 0

                status = sanitize_string(txn.get("status")) or "COMPLETED"

                flattened.append(
                    FlattenedRedemption(
                        txn_id=txn_id,
                        member_id=member_id,
                        feed_date=feed_date,
                        txn_date=txn_date,
                        partner=partner,
                        miles_redeemed=miles_int,
                        status=status,
                    )
                )

        return flattened

    def load_redemptions(self, redemptions: List[FlattenedRedemption]) -> int:
        """Insert or replace flattened redemptions into DuckDB staging."""
        if not redemptions:
            return 0

        rows = [
            (
                r.txn_id,
                r.member_id,
                r.feed_date,
                r.txn_date,
                r.partner,
                r.miles_redeemed,
                r.status,
            )
            for r in redemptions
        ]

        # Use INSERT OR REPLACE / ON CONFLICT to maintain idempotency
        self.conn.executemany("""
            INSERT OR REPLACE INTO stg_redemptions_flattened (
                txn_id, member_id, feed_date, txn_date, partner, miles_redeemed, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """, rows)

        return len(rows)

    def build_loyalty_360_mart(self, source_members_table: str = "tmp_latest_members") -> List[Dict[str, Any]]:
        """Join member profile data with flattened redemption aggregates to generate Member Loyalty 360 mart."""
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE member_loyalty_360 AS
            WITH RedemptionAggs AS (
                SELECT
                    member_id,
                    COUNT(DISTINCT txn_id) AS total_redemptions,
                    COALESCE(SUM(miles_redeemed), 0) AS total_miles_redeemed,
                    MAX(txn_date) AS last_redemption_date,
                    MODE(partner) AS top_partner
                FROM stg_redemptions_flattened
                GROUP BY member_id
            )
            SELECT
                m.member_id,
                m.member_name,
                m.country,
                m.tier_code,
                m.enrollment_date,
                m.last_flight_date,
                m.age,
                m.stale_member,
                COALESCE(r.total_redemptions, 0) AS total_redemptions,
                COALESCE(r.total_miles_redeemed, 0) AS total_miles_redeemed,
                r.last_redemption_date,
                COALESCE(r.top_partner, 'None') AS top_partner,
                CURRENT_TIMESTAMP AS last_sync_at
            FROM {source_members_table} m
            LEFT JOIN RedemptionAggs r ON m.member_id = r.member_id;
        """)

        cur = self.conn.cursor()
        cur.execute("SELECT * FROM member_loyalty_360")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
