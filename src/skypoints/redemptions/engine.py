"""Semi-structured JSON redemption feed parser and Loyalty 360 analytical join."""
from dataclasses import dataclass, field
from datetime import date
from typing import List, Dict, Any, Optional, Union, Tuple
import json
import duckdb
from skypoints.validation.validators import parse_and_validate_date, sanitize_string
from skypoints.models import QuarantineRecord


@dataclass
class FlattenedRedemption:
    txn_id: str
    member_id: str
    feed_date: date
    txn_date: date
    partner: str
    miles_redeemed: int
    status: str
    is_ambiguous: bool = False


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
                is_ambiguous BOOLEAN DEFAULT FALSE,
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
            if not isinstance(feed, dict):
                quarantine.append(
                    QuarantineRecord(
                        raw_data={"item": str(feed)},
                        error_reason="Feed item is not a valid JSON object",
                    )
                )
                continue

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
                r.is_ambiguous,
            )
            for r in redemptions
        ]

        self.conn.executemany("""
            INSERT OR REPLACE INTO stg_redemptions_flattened (
                txn_id, member_id, feed_date, txn_date, partner, miles_redeemed, status, is_ambiguous
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, rows)

        return len(rows)

    def quarantine_ambiguous_redemptions(self, source_members_table: str = "tmp_latest_members") -> List[QuarantineRecord]:
        """Identify, flag, and quarantine redemptions with ambiguous member_id across multiple sources."""
        quarantined: List[QuarantineRecord] = []
        try:
            # Update IS_AMBIGUOUS flag non-destructively: TRUE if spanning sources, FALSE otherwise
            self.conn.execute(f"""
                UPDATE stg_redemptions_flattened
                SET is_ambiguous = CASE
                    WHEN member_id IN (
                        SELECT member_id
                        FROM {source_members_table}
                        GROUP BY member_id
                        HAVING COUNT(DISTINCT source_system) > 1
                    ) THEN TRUE
                    ELSE FALSE
                END;
            """)

            ambiguous_txns = self.conn.execute("""
                SELECT r.txn_id, r.member_id, r.feed_date, r.txn_date, r.partner, r.miles_redeemed, r.status
                FROM stg_redemptions_flattened r
                WHERE r.is_ambiguous = TRUE
            """).fetchall()

            for row in ambiguous_txns:
                rec = QuarantineRecord(
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
                quarantined.append(rec)

            if ambiguous_txns:
                self.quarantined_redemptions.extend(quarantined)
        except duckdb.CatalogException:
            pass
        return quarantined

    def build_loyalty_360_mart(self, source_members_table: Optional[str] = None) -> List[Dict[str, Any]]:
        """Join member profile data with flattened redemptions without side effects.

        Sums only COMPLETED miles into total_miles_redeemed, tracking pending miles separately.
        Reads from country target tables (table_india, table_usa, table_canada, table_philippines, table_australia)
        by default, ensuring historical members not in the current batch remain present in the analytical mart.
        """
        # Ensure all 5 country target tables exist so UNION ALL is always safe
        country_tables = [
            "table_india",
            "table_usa",
            "table_canada",
            "table_philippines",
            "table_australia",
        ]
        for tbl in country_tables:
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
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
                    extract_ts TIMESTAMP,
                    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source_system, member_id)
                );
            """)

        self.conn.execute("""
            CREATE OR REPLACE TEMP VIEW members_all AS
            SELECT source_system, member_id, member_name, country, tier_code, enrollment_date, last_flight_date, age, stale_member FROM table_india
            UNION ALL
            SELECT source_system, member_id, member_name, country, tier_code, enrollment_date, last_flight_date, age, stale_member FROM table_usa
            UNION ALL
            SELECT source_system, member_id, member_name, country, tier_code, enrollment_date, last_flight_date, age, stale_member FROM table_canada
            UNION ALL
            SELECT source_system, member_id, member_name, country, tier_code, enrollment_date, last_flight_date, age, stale_member FROM table_philippines
            UNION ALL
            SELECT source_system, member_id, member_name, country, tier_code, enrollment_date, last_flight_date, age, stale_member FROM table_australia;
        """)

        members_source = source_members_table if source_members_table else "members_all"

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE member_loyalty_360 AS
            WITH PartnerCounts AS (
                SELECT
                    member_id,
                    partner,
                    COUNT(*) AS cnt,
                    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY COUNT(*) DESC, partner ASC) AS rn
                FROM stg_redemptions_flattened
                WHERE is_ambiguous = FALSE AND partner IS NOT NULL
                GROUP BY member_id, partner
            ),
            TopPartner AS (
                SELECT member_id, partner AS top_partner
                FROM PartnerCounts
                WHERE rn = 1
            ),
            RedemptionAggs AS (
                SELECT
                    r.member_id,
                    COUNT(DISTINCT r.txn_id) AS total_redemptions,
                    -- Only sum COMPLETED miles into redeemed balance
                    COALESCE(SUM(CASE WHEN UPPER(r.status) = 'COMPLETED' THEN r.miles_redeemed ELSE 0 END), 0) AS total_miles_redeemed,
                    COALESCE(SUM(CASE WHEN UPPER(r.status) = 'PENDING' THEN r.miles_redeemed ELSE 0 END), 0) AS pending_miles_redeemed,
                    MAX(r.txn_date) AS last_redemption_date
                FROM stg_redemptions_flattened r
                WHERE r.is_ambiguous = FALSE
                GROUP BY r.member_id
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
                COALESCE(tp.top_partner, 'None') AS top_partner,
                CURRENT_TIMESTAMP AS last_sync_at
            FROM {members_source} m
            LEFT JOIN RedemptionAggs r ON m.member_id = r.member_id
            LEFT JOIN TopPartner tp ON m.member_id = tp.member_id;
        """)

        cur = self.conn.cursor()
        cur.execute("SELECT * FROM member_loyalty_360")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
