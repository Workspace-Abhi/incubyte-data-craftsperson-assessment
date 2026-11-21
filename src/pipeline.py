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
    country_table_counts: Dict[str, int] = field(default_factory=dict)
    total_redemptions_parsed: int = 0
    loyalty_360_members_count: int = 0


@dataclass
class PipelineResult:
    metrics: PipelineMetrics
    quarantined_records: List[QuarantineRecord]
    loyalty_360_data: List[Dict[str, Any]]
    country_data: Dict[str, List[Dict[str, Any]]]


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
        reference_date: Optional[date] = None,
    ) -> PipelineResult:
        """Execute end-to-end pipeline."""
        raw_records: List[RawMemberRecord] = []

        # 1. Ingest pipe-delimited flat file if provided
        if flat_file_path:
            raw_records.extend(read_pipe_delimited_file(flat_file_path))

        # 2. Ingest CSV files if provided
        if csv_file_mappings:
            for csv_path, default_country in csv_file_mappings.items():
                raw_records.extend(read_csv_file(csv_path, default_country=default_country))

        # 3. Validation & DLQ quarantine
        val_result = validate_member_batch(raw_records)

        # 4. Load staging table with Age and Stale_Member
        ref_dt = reference_date or date.today()
        self.staging_engine.load_clean_records(val_result.valid_records, reference_date=ref_dt)

        # 5. Split members per country with "latest record wins"
        country_counts = self.country_router.execute_country_split()

        # 6. Parse and load JSON redemption feed
        redemptions_count = 0
        if json_file_path:
            with open(json_file_path, "r", encoding="utf-8") as jf:
                parsed_redemptions = self.redemption_parser.parse_payload(jf.read())
                redemptions_count = self.redemption_parser.load_redemptions(parsed_redemptions)

        # 7. Generate Loyalty 360 mart view
        loyalty_360 = self.redemption_parser.build_loyalty_360_mart("tmp_latest_members")

        # Collect country data
        country_data: Dict[str, List[Dict[str, Any]]] = {}
        for tbl in country_counts.keys():
            country_data[tbl] = self.country_router.get_country_records(tbl)

        metrics = PipelineMetrics(
            total_raw_records=len(raw_records),
            valid_records_count=len(val_result.valid_records),
            quarantined_records_count=len(val_result.quarantine_records),
            country_table_counts=country_counts,
            total_redemptions_parsed=redemptions_count,
            loyalty_360_members_count=len(loyalty_360),
        )

        return PipelineResult(
            metrics=metrics,
            quarantined_records=val_result.quarantine_records,
            loyalty_360_data=loyalty_360,
            country_data=country_data,
        )
