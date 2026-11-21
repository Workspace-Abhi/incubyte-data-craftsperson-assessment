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
        records, bad = parser.parse_payload(SAMPLE_FEED)

        assert len(bad) == 0
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

    def test_corrupt_transactions_quarantined_not_invented(self):
        parser = RedemptionParser()
        corrupt_feed = {
            "member_id": "999",
            "feed_date": "20240115",
            "redemptions": [
                {"txn_id": "", "txn_date": "20240110", "miles_redeemed": 100, "status": "COMPLETED"},  # missing id
                {"txn_id": "T2", "txn_date": "", "miles_redeemed": 100, "status": "COMPLETED"},       # missing date
                {"txn_id": "T3", "txn_date": "20240110", "miles_redeemed": -50, "status": "COMPLETED"}, # negative miles
            ],
        }
        valid, bad = parser.parse_payload(corrupt_feed)
        assert len(valid) == 0
        assert len(bad) == 3
        assert "Missing mandatory transaction txn_id" in bad[0].error_reason
        assert "Missing mandatory transaction txn_date" in bad[1].error_reason
        assert "cannot be negative" in bad[2].error_reason

    def test_load_and_query_redemptions(self):
        conn = duckdb.connect(":memory:")
        parser = RedemptionParser(conn)
        records, _ = parser.parse_payload(SAMPLE_FEED)
        count = parser.load_redemptions(records)

        assert count == 2

        df = conn.execute("SELECT * FROM stg_redemptions_flattened ORDER BY txn_id").fetchall()
        assert len(df) == 2
        assert df[0][0] == "RX10091"
        assert df[1][0] == "RX10092"

    def test_loyalty_360_join_sums_only_completed_miles(self):
        conn = duckdb.connect(":memory:")
        staging = StagingEngine(conn)
        parser = RedemptionParser(conn)

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
        conn.execute("CREATE OR REPLACE TEMP TABLE tmp_latest_members AS SELECT * FROM stg_member_profile;")

        redemption_records, _ = parser.parse_payload(SAMPLE_FEED)
        parser.load_redemptions(redemption_records)

        mart = parser.build_loyalty_360_mart("tmp_latest_members")
        assert len(mart) == 2

        # Elena: 1 COMPLETED (12,000) and 1 PENDING (5,000)
        elena = next(r for r in mart if r["member_id"] == "223457")
        assert elena["member_name"] == "Elena"
        assert elena["total_redemptions"] == 2
        assert elena["total_miles_redeemed"] == 12000  # ONLY COMPLETED miles!
        assert elena["pending_miles_redeemed"] == 5000  # PENDING miles tracked separately!

        # Ravi: 0 redemptions
        ravi = next(r for r in mart if r["member_id"] == "223458")
        assert ravi["total_redemptions"] == 0
        assert ravi["total_miles_redeemed"] == 0
        assert ravi["pending_miles_redeemed"] == 0

    def test_ambiguous_member_id_across_source_systems_quarantined(self):
        conn = duckdb.connect(":memory:")
        staging = StagingEngine(conn)
        parser = RedemptionParser(conn)

        # Member ID "1" exists in both IND source and USA source
        members = [
            CleanMemberRecord(
                member_id="1",
                member_name="Vikas",
                enrollment_date=date(2020, 1, 1),
                country="IND",
                source_system="IND",
            ),
            CleanMemberRecord(
                member_id="1",
                member_name="Sam",
                enrollment_date=date(2020, 1, 1),
                country="USA",
                source_system="USA",
            ),
        ]
        staging.load_clean_records(members)
        conn.execute("CREATE OR REPLACE TEMP TABLE tmp_latest_members AS SELECT * FROM stg_member_profile;")

        # Redemption feed for member_id "1"
        payload = {
            "feed_date": "20230501",
            "member_id": "1",
            "redemptions": [
                {
                    "txn_id": "TXN_AMBIG_1",
                    "txn_date": "20230415",
                    "partner": "SkyPoints",
                    "miles_redeemed": 3000,
                    "status": "COMPLETED",
                }
            ],
        }
        redemptions, _ = parser.parse_payload(payload)
        parser.load_redemptions(redemptions)

        # Quarantining ambiguous redemptions is now explicit before mart aggregation
        ambig = parser.quarantine_ambiguous_redemptions("tmp_latest_members")
        assert len(ambig) == 1
        assert ambig[0].raw_data["txn_id"] == "TXN_AMBIG_1"

        # Building the mart has no side effects and reflects the cleaned state
        mart = parser.build_loyalty_360_mart("tmp_latest_members")
        assert len(mart) == 2

        # Neither member should get the redemption
        for m in mart:
            assert m["total_redemptions"] == 0
            assert m["total_miles_redeemed"] == 0

        # Redemption is quarantined
        ambig_q = [q for q in parser.quarantined_redemptions if q.error_reason == "Ambiguous member_id across source systems"]
        assert len(ambig_q) == 1
        assert ambig_q[0].raw_data["txn_id"] == "TXN_AMBIG_1"

    def test_mart_builds_from_country_tables_preserving_historical_members(self):
        conn = duckdb.connect(":memory:")
        parser = RedemptionParser(conn)

        # Insert historical member directly into country target table (not in today's flat file batch)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS table_india (
                source_system VARCHAR NOT NULL,
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
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_system, member_id)
            );
            INSERT INTO table_india (
                source_system, member_id, member_name, enrollment_date, last_flight_date,
                tier_code, agent_name, state, country, dob, is_active, age, stale_member
            ) VALUES (
                'HIST_IND', 'HIST_001', 'Arjun', DATE '2021-01-01', DATE '2023-05-10',
                'GLD', 'Agent01', 'MH', 'IND', DATE '1992-04-12', 'A', 33, 'N'
            );
        """)

        # Process new redemption for this historical member
        payload = {
            "feed_date": "20240115",
            "member_id": "HIST_001",
            "redemptions": [
                {
                    "txn_id": "HIST_TXN_1",
                    "txn_date": "20240110",
                    "partner": "SkyPoints",
                    "miles_redeemed": 7500,
                    "status": "COMPLETED",
                }
            ],
        }
        redemptions, _ = parser.parse_payload(payload)
        parser.load_redemptions(redemptions)

        # Build mart without specifying source table (defaults to union of country tables)
        mart = parser.build_loyalty_360_mart()
        assert len(mart) == 1
        arjun = mart[0]
        assert arjun["member_id"] == "HIST_001"
        assert arjun["member_name"] == "Arjun"
        assert arjun["total_redemptions"] == 1
        assert arjun["total_miles_redeemed"] == 7500
        assert arjun["top_partner"] == "SkyPoints"

    def test_build_loyalty_360_mart_has_no_side_effects_on_repeated_runs(self):
        conn = duckdb.connect(":memory:")
        parser = RedemptionParser(conn)

        # Seed country table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS table_usa (
                source_system VARCHAR NOT NULL,
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
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_system, member_id)
            );
            INSERT INTO table_usa (
                source_system, member_id, member_name, enrollment_date, last_flight_date,
                tier_code, agent_name, state, country, dob, is_active, age, stale_member
            ) VALUES (
                'FLAT_FILE', 'USA_001', 'John', DATE '2022-01-01', DATE '2023-01-01',
                'PLT', 'AgentUS', 'NY', 'USA', DATE '1988-06-15', 'A', 36, 'N'
            );
        """)

        payload = {
            "feed_date": "20240115",
            "member_id": "USA_001",
            "redemptions": [
                {
                    "txn_id": "USA_TXN_1",
                    "txn_date": "20240112",
                    "partner": "Delta",
                    "miles_redeemed": 4000,
                    "status": "COMPLETED",
                }
            ],
        }
        redemptions, _ = parser.parse_payload(payload)
        parser.load_redemptions(redemptions)

        q_count_before = len(parser.quarantined_redemptions)

        # Run 1
        mart1 = parser.build_loyalty_360_mart()
        q_count_after1 = len(parser.quarantined_redemptions)

        # Run 2
        mart2 = parser.build_loyalty_360_mart()
        q_count_after2 = len(parser.quarantined_redemptions)

        assert q_count_before == q_count_after1 == q_count_after2 == 0
        assert len(mart1) == len(mart2) == 1
        assert mart1[0]["total_miles_redeemed"] == mart2[0]["total_miles_redeemed"] == 4000

    def test_non_dict_feed_item_quarantined_gracefully(self):
        parser = RedemptionParser()
        # Feed payload containing a non-dict item (e.g. integer or string) slipped in
        mixed_feed = [
            12345,
            {"member_id": "M1", "feed_date": "20240115", "redemptions": []},
            "unexpected_string",
        ]
        records, bad = parser.parse_payload(mixed_feed)
        assert len(records) == 0
        assert len(bad) == 2
        assert bad[0].error_reason == "Feed item is not a valid JSON object"
        assert bad[1].error_reason == "Feed item is not a valid JSON object"
