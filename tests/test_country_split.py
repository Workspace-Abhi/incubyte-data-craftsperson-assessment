"""TDD tests for country routing and 'latest record wins' migration resolution."""
import pytest
from datetime import date
from src.staging import StagingEngine
from src.country_router import CountryRouter
from src.models import CleanMemberRecord


class TestCountrySplitAndDeduplication:
    def test_country_routing_separates_countries(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        records = [
            CleanMemberRecord(member_id="101", member_name="Elena", enrollment_date=date(2010, 1, 1), country="USA"),
            CleanMemberRecord(member_id="102", member_name="Ravi", enrollment_date=date(2010, 1, 1), country="IND"),
            CleanMemberRecord(member_id="103", member_name="Mateo", enrollment_date=date(2010, 1, 1), country="PHIL"),
            CleanMemberRecord(member_id="104", member_name="Nora", enrollment_date=date(2010, 1, 1), country="CAN"),
            CleanMemberRecord(member_id="105", member_name="Jacob", enrollment_date=date(2010, 1, 1), country="AU"),
        ]
        engine.load_clean_records(records)
        counts = router.execute_country_split()

        assert counts["table_usa"] == 1
        assert counts["table_india"] == 1
        assert counts["table_philippines"] == 1
        assert counts["table_canada"] == 1
        assert counts["table_australia"] == 1

        usa_rows = router.get_country_records("table_usa")
        assert len(usa_rows) == 1
        assert usa_rows[0]["member_id"] == "101"

        ind_rows = router.get_country_records("table_india")
        assert len(ind_rows) == 1
        assert ind_rows[0]["member_id"] == "102"

    def test_latest_record_wins_when_member_moves_country(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Elena originally in India (flight in 2011)
        # Then moved to USA (flight in 2013)
        records = [
            CleanMemberRecord(
                member_id="M999",
                member_name="Elena",
                enrollment_date=date(2010, 1, 1),
                last_flight_date=date(2011, 5, 1),
                country="IND",
            ),
            CleanMemberRecord(
                member_id="M999",
                member_name="Elena",
                enrollment_date=date(2010, 1, 1),
                last_flight_date=date(2013, 8, 20),
                country="USA",
            ),
        ]
        engine.load_clean_records(records)
        router.execute_country_split()

        # Should be in USA table only (latest record wins)
        usa_records = router.get_country_records("table_usa")
        ind_records = router.get_country_records("table_india")

        assert len(usa_records) == 1
        assert usa_records[0]["member_id"] == "M999"
        assert usa_records[0]["country"] == "USA"
        assert len(ind_records) == 0

    def test_deduplicates_same_country_latest_wins(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Vikas with older flight date vs newer flight date
        records = [
            CleanMemberRecord(
                member_id="V01",
                member_name="Vikas",
                enrollment_date=date(2022, 1, 1),
                last_flight_date=date(2022, 3, 1),
                tier_code="SLV",
                country="IND",
            ),
            CleanMemberRecord(
                member_id="V01",
                member_name="Vikas",
                enrollment_date=date(2022, 1, 1),
                last_flight_date=date(2022, 6, 15),
                tier_code="GLD",  # upgraded tier in newer record
                country="IND",
            ),
        ]
        engine.load_clean_records(records)
        router.execute_country_split()

        ind_records = router.get_country_records("table_india")
        assert len(ind_records) == 1
        assert ind_records[0]["tier_code"] == "GLD"
        assert ind_records[0]["last_flight_date"] == date(2022, 6, 15)
