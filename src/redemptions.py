"""Semi-structured JSON redemption feed parser and Loyalty 360 analytical join."""
from dataclasses import dataclass, field
from datetime import date
from typing import List, Dict, Any, Optional, Union, Tuple
import json
import duckdb
from src.validators import parse_and_validate_date, sanitize_string
from src.models import QuarantineRecord


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
    """Parses semi-structured partner redemption feeds with strict validation."""

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        self.conn = conn or duckdb.connect(":memory:")
        self.quarantined_redemptions: List[QuarantineRecord] = []
        self._init_schema()

    def _init_schema(self) -> None:
        """Create flattened redemption staging table and Loyalty 360 mart schema."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_redemptions_flattened (
                txn_id VARCHAR PRIMARY KEY,
                member_id VARCHAR NOT NULL,
                feed_date DATE NOT NULL,
                txn_date DATE NOT NULL,
                partner VARCHAR NOT NULL,
                miles_redeemed INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def parse_payload(self, raw_input: Union[str, dict, list]) -> Tuple[List[FlattenedRedemption], List[QuarantineRecord]]:
        """Parse raw JSON into validated flattened redemptions and quarantined DLQ items without data invention."""
        if isinstance(raw_input, str):
            payload = json.loads(raw_input)
        else:
            payload = raw_input

        feed_items = payload if isinstance(payload, list) else [payload]
        flattened: List[FlattenedRedemption] = []
        quarantine: List[QuarantineRecord] = []

        for feed in feed_items:
            member_id = sanitize_string(feed.get("member_id"))
            feed_dt_str = sanitize_string(feed.get("feed_date"))
            feed_date, err = parse_and_validate_date(feed_dt_str)
            if err or not feed_date:
                quarantine.append(
                    QuarantineRecord(
                        raw_data=feed,
                        error_reason=f"Invalid feed_date in redemption feed: {err or 'Missing feed_date'}",
                    )
                )
                continue

            if not member_id:
                quarantine.append(
                    QuarantineRecord(
                        raw_data=feed,
                        error_reason="Missing mandatory member_id in redemption feed",
                    )
                )
                continue

            redemptions = feed.get("redemptions")
            if not isinstance(redemptions, list):
                quarantine.append(
                    QuarantineRecord(
                        raw_data=feed,
                        error_reason="Invalid or missing 'redemptions' array",
                    )
                )
                continue

            for txn in redemptions:
                if not isinstance(txn, dict):
                    quarantine.append(
                        QuarantineRecord(
                            raw_data={"feed_member_id": member_id, "txn": str(txn)},
                            error_reason="Transaction item is not a valid JSON object",
                        )
                    )
                    continue

                # 1. Mandatory txn_id
                txn_id = sanitize_string(txn.get("txn_id"))
                if not txn_id:
                    quarantine.append(
                        QuarantineRecord(
                            raw_data=txn,
                            error_reason="Missing mandatory transaction txn_id",
                        )
                    )
                    continue

                # 2. Mandatory txn_date (do NOT invent/fallback to feed_date)
                txn_dt_str = sanitize_string(txn.get("txn_date"))
                if not txn_dt_str:
                    quarantine.append(
                        QuarantineRecord(
                            raw_data=txn,
                            error_reason="Missing mandatory transaction txn_date",
                        )
                    )
                    continue

                txn_date, err = parse_and_validate_date(txn_dt_str)
                if err or not txn_date:
                    quarantine.append(
                        QuarantineRecord(
                            raw_data=txn,
                            error_reason=f"Invalid txn_date: {err}",
                        )
                    )
                    continue

                # 3. Mandatory miles_redeemed (do NOT invent/default to 0)
                raw_miles = txn.get("miles_redeemed")
                if raw_miles is None:
                    quarantine.append(
                        QuarantineRecord(
                            raw_data=txn,
                            error_reason="Missing mandatory miles_redeemed",
                        )
                    )
                    continue
                try:
                    miles_int = int(raw_miles)
                    if miles_int < 0:
                        quarantine.append(
                            QuarantineRecord(
                                raw_data=txn,
                                error_reason="Invalid miles_redeemed: value cannot be negative",
                            )
                        )
                        continue
                except (ValueError, TypeError):
                    quarantine.append(
                        QuarantineRecord(
                            raw_data=txn,
                            error_reason=f"Invalid miles_redeemed data type: '{raw_miles}'",
                        )
                    )
                    continue

                # 4. Mandatory status & partner
                status = sanitize_string(txn.get("status"))
                if not status:
                    quarantine.append(
                        QuarantineRecord(
                            raw_data=txn,
                            error_reason="Missing mandatory transaction status",
                        )
                    )
                    continue
                status = status.upper()

                partner = sanitize_string(txn.get("partner"))
                if not partner:
                    quarantine.append(
                        QuarantineRecord(
                            raw_data=txn,
                            error_reason="Missing mandatory partner airline name",
                        )
                    )
                    continue

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

        self.quarantined_redemptions.extend(quarantine)
        return flattened, quarantine

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

        self.conn.executemany("""
            INSERT OR REPLACE INTO stg_redemptions_flattened (
                txn_id, member_id, feed_date, txn_date, partner, miles_redeemed, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """, rows)

        return len(rows)

    def build_loyalty_360_mart(self, source_members_table: str = "tmp_latest_members") -> List[Dict[str, Any]]:
        """Join member profile data with flattened redemptions.

        Sums only COMPLETED miles into total_miles_redeemed, tracking pending miles separately.
        Quarantines redemptions when member_id is ambiguous across multiple source systems.
        """
        # 1. Identify and quarantine redemptions with ambiguous member_id across multiple sources
        try:
            ambiguous_txns = self.conn.execute(f"""
                SELECT r.txn_id, r.member_id, r.feed_date, r.txn_date, r.partner, r.miles_redeemed, r.status
                FROM stg_redemptions_flattened r
                WHERE r.member_id IN (
                    SELECT member_id
                    FROM {source_members_table}
                    GROUP BY member_id
                    HAVING COUNT(DISTINCT source_system) > 1
                )
            """).fetchall()

            for row in ambiguous_txns:
                self.quarantined_redemptions.append(
                    QuarantineRecord(
                        raw_data={
                            "txn_id": row[0],
                            "member_id": row[1],
                            "feed_date": str(row[2]),
                            "txn_date": str(row[3]),
                            "partner": row[4],
                            "miles_redeemed": row[5],
                            "status": row[6],
                        },
                        error_reason="Ambiguous member_id across source systems",
                    )
                )

            if ambiguous_txns:
                self.conn.execute(f"""
                    DELETE FROM stg_redemptions_flattened
                    WHERE member_id IN (
                        SELECT member_id
                        FROM {source_members_table}
                        GROUP BY member_id
                        HAVING COUNT(DISTINCT source_system) > 1
                    )
                """)
        except duckdb.CatalogException:
            pass

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE member_loyalty_360 AS
            WITH RedemptionAggs AS (
                SELECT
                    member_id,
                    COUNT(DISTINCT txn_id) AS total_redemptions,
                    -- Only sum COMPLETED miles into redeemed balance
                    COALESCE(SUM(CASE WHEN UPPER(status) = 'COMPLETED' THEN miles_redeemed ELSE 0 END), 0) AS total_miles_redeemed,
                    COALESCE(SUM(CASE WHEN UPPER(status) = 'PENDING' THEN miles_redeemed ELSE 0 END), 0) AS pending_miles_redeemed,
                    MAX(txn_date) AS last_redemption_date,
                    MODE(partner) AS top_partner
                FROM stg_redemptions_flattened
                GROUP BY member_id
            )
            SELECT
                m.source_system,
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
                COALESCE(r.pending_miles_redeemed, 0) AS pending_miles_redeemed,
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
