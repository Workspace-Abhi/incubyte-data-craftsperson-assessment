"""TDD tests for JSON redemption feed parser and Loyalty 360 join."""
import pytest
from datetime import date
import duckdb
from src.staging import StagingEngine
from src.redemptions import RedemptionParser, FlattenedRedemption
from src.models import CleanMemberRecord


SAMPLE_FEED = {
    "member_id": "223457",
    "feed_date": "20240115",
    "redemptions": [
        {
            "txn_id": "RX10091",
            "txn_date": "20240110",
            "partner": "AeroLink",
            "miles_redeemed": 12000,
            "status": "COMPLETED",
        },
        {
            "txn_id": "RX10092",
            "txn_date": "20240113",
            "partner": "SkyPoints",
            "miles_redeemed": 5000,
            "status": "PENDING",
        },
    ],
}


class TestRedemptionParser:
    def test_parse_sample_feed_json(self):
        parser = RedemptionParser()
        records = parser.parse_payload(SAMPLE_FEED)

        assert len(records) == 2

        rx1 = records[0]
        assert rx1.txn_id == "RX10091"
        assert rx1.member_id == "223457"
        assert rx1.feed_date == date(2024, 1, 15)
        assert rx1.txn_date == date(2024, 1, 10)
        assert rx1.partner == "AeroLink"
        assert rx1.miles_redeemed == 12000
        assert rx1.status == "COMPLETED"

        rx2 = records[1]
        assert rx2.txn_id == "RX10092"
        assert rx2.miles_redeemed == 5000
        assert rx2.status == "PENDING"

    def test_load_and_query_redemptions(self):
        conn = duckdb.connect(":memory:")
        parser = RedemptionParser(conn)
        records = parser.parse_payload(SAMPLE_FEED)
        count = parser.load_redemptions(records)

        assert count == 2

        df = conn.execute("SELECT * FROM stg_redemptions_flattened ORDER BY txn_id").fetchall()
        assert len(df) == 2
        assert df[0][0] == "RX10091"
        assert df[1][0] == "RX10092"

    def test_loyalty_360_join_combines_profile_and_redemptions(self):
        conn = duckdb.connect(":memory:")
        staging = StagingEngine(conn)
        parser = RedemptionParser(conn)

        # 1. Load member profile
        members = [
            CleanMemberRecord(
                member_id="223457",
                member_name="Elena",
                enrollment_date=date(2010, 10, 12),
                last_flight_date=date(2012, 10, 13),
                tier_code="GLD",
                country="USA",
                dob=date(1985, 5, 3),
            ),
            CleanMemberRecord(
                member_id="223458",
                member_name="Ravi",
                enrollment_date=date(2010, 10, 12),
                last_flight_date=date(2026, 11, 1),
                tier_code="SLV",
                country="IND",
                dob=date(1990, 1, 1),
            ),
        ]
        staging.load_clean_records(members)

        # Create temporary latest members table
        conn.execute("CREATE OR REPLACE TEMP TABLE tmp_latest_members AS SELECT * FROM stg_member_profile;")

        # 2. Load redemptions (only for Elena)
        redemption_records = parser.parse_payload(SAMPLE_FEED)
        parser.load_redemptions(redemption_records)

        # 3. Build Loyalty 360 mart
        mart = parser.build_loyalty_360_mart("tmp_latest_members")
        assert len(mart) == 2

        # Elena should have 2 redemptions and 17,000 total miles
        elena = next(r for r in mart if r["member_id"] == "223457")
        assert elena["member_name"] == "Elena"
        assert elena["total_redemptions"] == 2
        assert elena["total_miles_redeemed"] == 17000
        assert elena["last_redemption_date"] == date(2024, 1, 13)

        # Ravi should have 0 redemptions and 0 miles
        ravi = next(r for r in mart if r["member_id"] == "223458")
        assert ravi["member_name"] == "Ravi"
        assert ravi["total_redemptions"] == 0
        assert ravi["total_miles_redeemed"] == 0
        assert ravi["top_partner"] == "None"
