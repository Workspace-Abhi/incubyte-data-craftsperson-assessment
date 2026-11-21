"""End-to-end SkyPoints Loyalty Data Pipeline Orchestrator."""
from dataclasses import dataclass, field
from datetime import date
from typing import List, Dict, Any, Optional
import duckdb
from src.flat_file_reader import read_pipe_delimited_file, read_csv_file
from src.validators import validate_member_batch
from src.staging import StagingEngine
from src.country_router import CountryRouter
from src.redemptions import RedemptionParser
from src.models import RawMemberRecord, QuarantineRecord


@dataclass
class PipelineMetrics:
    total_raw_records: int = 0
    valid_records_count: int = 0
    quarantined_records_count: int = 0
    key_collisions_count: int = 0
    warnings_count: int = 0
    country_table_counts: Dict[str, int] = field(default_factory=dict)
    total_redemptions_parsed: int = 0
    quarantined_redemptions_count: int = 0
    loyalty_360_members_count: int = 0


@dataclass
class PipelineResult:
    metrics: PipelineMetrics
    quarantined_records: List[QuarantineRecord]
    key_collisions: List[str]
    loyalty_360_data: List[Dict[str, Any]]
    country_data: Dict[str, List[Dict[str, Any]]]
    warnings: List[str] = field(default_factory=list)
    orphan_redemptions: List[Dict[str, Any]] = field(default_factory=list)


class SkyPointsPipeline:
    """Production pipeline managing validation, staging, routing, and analytical joins."""

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        self.conn = conn or duckdb.connect(":memory:")
        self.staging_engine = StagingEngine(self.conn)
        self.country_router = CountryRouter(self.conn)
        self.redemption_parser = RedemptionParser(self.conn)

    def run(
        self,
        flat_file_path: Optional[str] = None,
        json_file_path: Optional[str] = None,
        csv_file_mappings: Optional[Dict[str, str]] = None,
        csv_date_format_hints: Optional[Dict[str, str]] = None,
        reference_date: Optional[date] = None,
    ) -> PipelineResult:
        """Execute end-to-end pipeline.

        Args:
            flat_file_path: Path to pipe-delimited flat file.
            json_file_path: Path to partner redemption JSON feed.
            csv_file_mappings: Dict mapping CSV path to default country.
            reference_date: Reference date for Age and Stale_Member calculations.
            namespace_csv_ids: If True, prefixes CSV IDs with country (e.g. USA_1, IND_1)
                               to avoid cross-file ID collision data loss.
        """
        raw_records: List[RawMemberRecord] = []

        # 1. Ingest pipe-delimited flat file if provided
        if flat_file_path:
            raw_records.extend(read_pipe_delimited_file(flat_file_path))

        # 2. Ingest CSV files preserving raw IDs with explicit date format hints
        if csv_file_mappings:
            hints = csv_date_format_hints or {}
            from pathlib import Path
            for csv_path, default_country in csv_file_mappings.items():
                hint = hints.get(csv_path) or hints.get(Path(csv_path).name)
                raw_records.extend(
                    read_csv_file(
                        csv_path,
                        default_country=default_country,
                        date_format_hint=hint,
                    )
                )

        # 3. Validation & DLQ quarantine
        val_result = validate_member_batch(raw_records)

        # 4. Load staging table with Age and Stale_Member
        ref_dt = reference_date or date.today()
        self.staging_engine.load_clean_records(val_result.valid_records, reference_date=ref_dt)

        # 5. Split members per country with "latest record wins"
        country_counts = self.country_router.execute_country_split()

        # 6. Parse and load JSON redemption feed with strict validation (no invented data)
        redemptions_count = 0
        if json_file_path:
            with open(json_file_path, "r", encoding="utf-8") as jf:
                valid_redemptions, bad_redemptions = self.redemption_parser.parse_payload(jf.read())
                redemptions_count = self.redemption_parser.load_redemptions(valid_redemptions)

        # 7. Explicitly quarantine ambiguous redemptions across source systems
        self.redemption_parser.quarantine_ambiguous_redemptions("tmp_latest_members")

        # 8. Generate Loyalty 360 mart view (sums only COMPLETED miles) from union of country tables
        loyalty_360 = self.redemption_parser.build_loyalty_360_mart()

        # 9. Identify orphan redemptions whose member_id does not exist in member data
        orphan_rows = self.conn.execute("""
            SELECT r.txn_id, r.member_id, r.feed_date, r.txn_date, r.partner, r.miles_redeemed, r.status
            FROM stg_redemptions_flattened r
            WHERE r.member_id NOT IN (SELECT DISTINCT member_id FROM members_all)
        """).fetchall()

        orphan_redemptions = [
            {
                "txn_id": row[0],
                "member_id": row[1],
                "feed_date": str(row[2]),
                "txn_date": str(row[3]),
                "partner": row[4],
                "miles_redeemed": row[5],
                "status": row[6],
            }
            for row in orphan_rows
        ]

        # Collect country data
        country_data: Dict[str, List[Dict[str, Any]]] = {}
        for tbl in country_counts.keys():
            country_data[tbl] = self.country_router.get_country_records(tbl)

        all_quarantine = val_result.quarantine_records + self.redemption_parser.quarantined_redemptions

        metrics = PipelineMetrics(
            total_raw_records=len(raw_records),
            valid_records_count=len(val_result.valid_records),
            quarantined_records_count=len(all_quarantine),
            key_collisions_count=len(val_result.key_collisions),
            warnings_count=len(val_result.warnings),
            country_table_counts=country_counts,
            total_redemptions_parsed=redemptions_count,
            quarantined_redemptions_count=len(self.redemption_parser.quarantined_redemptions),
            loyalty_360_members_count=len(loyalty_360),
        )

        return PipelineResult(
            metrics=metrics,
            quarantined_records=all_quarantine,
            key_collisions=val_result.key_collisions,
            warnings=val_result.warnings,
            loyalty_360_data=loyalty_360,
            country_data=country_data,
            orphan_redemptions=orphan_redemptions,
        )
