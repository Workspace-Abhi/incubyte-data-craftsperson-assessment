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

    def test_member_moving_between_two_countries_within_one_source(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Member M100 moves within FLAT_FILE source from IND to USA
        records = [
            CleanMemberRecord(
                member_id="M100",
                member_name="Aarav",
                enrollment_date=date(2021, 1, 1),
                last_flight_date=date(2021, 5, 10),
                country="IND",
                source_system="FLAT_FILE",
            ),
            CleanMemberRecord(
                member_id="M100",
                member_name="Aarav",
                enrollment_date=date(2021, 1, 1),
                last_flight_date=date(2023, 2, 20),
                country="USA",
                source_system="FLAT_FILE",
            ),
        ]
        engine.load_clean_records(records)
        router.execute_country_split()

        ind_rows = router.get_country_records("table_india")
        usa_rows = router.get_country_records("table_usa")

        assert len(ind_rows) == 0, "Member should be deleted from table_india after moving to USA"
        assert len(usa_rows) == 1
        assert usa_rows[0]["member_id"] == "M100"
        assert usa_rows[0]["country"] == "USA"

    def test_same_raw_id_in_different_sources_staying_as_two_members(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Same raw ID '1' exists in IND source and USA source
        records = [
            CleanMemberRecord(
                member_id="1",
                member_name="Ravi Kumar",
                enrollment_date=date(2020, 1, 1),
                last_flight_date=date(2022, 1, 10),
                country="IND",
                source_system="IND",
            ),
            CleanMemberRecord(
                member_id="1",
                member_name="John Smith",
                enrollment_date=date(2020, 5, 1),
                last_flight_date=date(2022, 4, 15),
                country="USA",
                source_system="USA",
            ),
        ]
        engine.load_clean_records(records)
        counts = router.execute_country_split()

        assert counts["table_india"] == 1
        assert counts["table_usa"] == 1

        ind_rows = router.get_country_records("table_india")
        usa_rows = router.get_country_records("table_usa")

        assert len(ind_rows) == 1
        assert ind_rows[0]["member_id"] == "1"
        assert ind_rows[0]["member_name"] == "Ravi Kumar"
        assert ind_rows[0]["source_system"] == "IND"

        assert len(usa_rows) == 1
        assert usa_rows[0]["member_id"] == "1"
        assert usa_rows[0]["member_name"] == "John Smith"
        assert usa_rows[0]["source_system"] == "USA"

    def test_unmapped_country_raises_error(self):
        router = CountryRouter()
        with pytest.raises(ValueError, match="Unmapped country"):
            router.get_table_name_for_country("GERMANY")

    def test_member_moves_country_with_lower_flight_date_but_newer_extract_date(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Older extract date (2023-01-01) with newer flight date (2023-05-01) in IND
        # Newer extract date (2023-06-01) with lower flight date (2021-01-01) in USA
        # Because extract date is prioritized over flight date, USA must win
        records = [
            CleanMemberRecord(
                member_id="M_RECENCY",
                member_name="Recency Test",
                enrollment_date=date(2020, 1, 1),
                last_flight_date=date(2023, 5, 1),
                country="IND",
                extract_date=date(2023, 1, 1),
            ),
            CleanMemberRecord(
                member_id="M_RECENCY",
                member_name="Recency Test",
                enrollment_date=date(2020, 1, 1),
                last_flight_date=date(2021, 1, 1),
                country="USA",
                extract_date=date(2023, 6, 1),
            ),
        ]
        engine.load_clean_records(records)
        router.execute_country_split()

        ind_rows = router.get_country_records("table_india")
        usa_rows = router.get_country_records("table_usa")

        assert len(ind_rows) == 0, "Older extract date in IND must be superseded and removed"
        assert len(usa_rows) == 1, "Newer extract date in USA must win"
        assert usa_rows[0]["member_id"] == "M_RECENCY"
        assert usa_rows[0]["country"] == "USA"

    def test_older_late_arriving_file_does_not_overwrite_newer_row(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Batch 1: Newer record arrives in USA
        batch1 = [
            CleanMemberRecord(
                member_id="M_LATE",
                member_name="Late Arrival Test",
                enrollment_date=date(2020, 1, 1),
                last_flight_date=date(2024, 2, 1),
                country="USA",
                tier_code="PLT",
                extract_date=date(2024, 2, 1),
            )
        ]
        engine.load_clean_records(batch1)
        router.execute_country_split()

        usa_rows = router.get_country_records("table_usa")
        assert len(usa_rows) == 1
        assert usa_rows[0]["tier_code"] == "PLT"
        assert usa_rows[0]["extract_date"] == date(2024, 2, 1)

        # Batch 2: Older late-arriving record for same member with tier_code="SLV" arrives
        batch2 = [
            CleanMemberRecord(
                member_id="M_LATE",
                member_name="Late Arrival Test",
                enrollment_date=date(2020, 1, 1),
                last_flight_date=date(2024, 1, 1),
                country="USA",
                tier_code="SLV",
                extract_date=date(2024, 1, 1),
            )
        ]
        engine.load_clean_records(batch2)
        router.execute_country_split()

        # The newer PLT record in table_usa must NOT be overwritten
        usa_rows_after = router.get_country_records("table_usa")
        assert len(usa_rows_after) == 1
        assert usa_rows_after[0]["tier_code"] == "PLT", "Older late-arriving record must not overwrite newer row"
        assert usa_rows_after[0]["extract_date"] == date(2024, 2, 1)

    def test_old_record_does_not_delete_newer_row_in_another_country(self):
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Batch 1: Member currently resides in USA (extract 2024-02-01)
        batch1 = [
            CleanMemberRecord(
                member_id="M_MIGRATE_PROTECT",
                member_name="Migration Test",
                enrollment_date=date(2020, 1, 1),
                last_flight_date=date(2024, 2, 1),
                country="USA",
                tier_code="GLD",
                extract_date=date(2024, 2, 1),
            )
        ]
        engine.load_clean_records(batch1)
        router.execute_country_split()

        assert len(router.get_country_records("table_usa")) == 1
        assert len(router.get_country_records("table_india")) == 0

        # Batch 2: Late-arriving historical record for India (extract 2024-01-01)
        batch2 = [
            CleanMemberRecord(
                member_id="M_MIGRATE_PROTECT",
                member_name="Migration Test",
                enrollment_date=date(2020, 1, 1),
                last_flight_date=date(2024, 1, 1),
                country="IND",
                tier_code="SLV",
                extract_date=date(2024, 1, 1),
            )
        ]
        engine.load_clean_records(batch2)
        router.execute_country_split()

        # Member must remain in table_usa and NOT be inserted in table_india or deleted from table_usa
        usa_rows = router.get_country_records("table_usa")
        ind_rows = router.get_country_records("table_india")
        assert len(usa_rows) == 1, "Newer record in table_usa must not be deleted by older cross-country record"
        assert len(ind_rows) == 0, "Older late-arriving record must not be inserted into table_india"
        assert usa_rows[0]["member_id"] == "M_MIGRATE_PROTECT"
        assert usa_rows[0]["extract_date"] == date(2024, 2, 1)

    def test_same_day_correction_cross_country_equal_line_numbers_moves_member(self):
        """Claude bug 2: A corrected same-day re-send moving a member from USA to IND

        with equal line numbers must move the member to IND and delete from USA, not leave them in neither table.
        """
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Batch 1: Member initially arrives in USA
        batch1 = [
            CleanMemberRecord(
                member_id="M_SAME_DAY",
                member_name="Same Day User",
                enrollment_date=date(2024, 1, 1),
                last_flight_date=date(2024, 1, 1),
                country="USA",
                source_file="file_a.dat",
                extract_date=date(2024, 1, 15),
                line_no=5,
            )
        ]
        engine.load_clean_records(batch1)
        router.execute_country_split()

        assert len(router.get_country_records("table_usa")) == 1
        assert len(router.get_country_records("table_india")) == 0

        # Batch 2: Same-day re-send correction moving member to IND with equal line_no but later file
        batch2 = [
            CleanMemberRecord(
                member_id="M_SAME_DAY",
                member_name="Same Day User",
                enrollment_date=date(2024, 1, 1),
                last_flight_date=date(2024, 1, 1),
                country="IND",
                source_file="file_b.dat",
                extract_date=date(2024, 1, 15),
                line_no=5,
            )
        ]
        engine.load_clean_records(batch2)
        router.execute_country_split()

        usa_rows = router.get_country_records("table_usa")
        ind_rows = router.get_country_records("table_india")

        assert len(usa_rows) == 0, "Member must be deleted from table_usa"
        assert len(ind_rows) == 1, "Member must be moved to table_india"
        assert ind_rows[0]["member_id"] == "M_SAME_DAY"
        assert ind_rows[0]["country"] == "IND"

    def test_same_country_tie_lower_line_no_does_not_throw_constraint_exception(self):
        """Claude bug 2: With a same-country tie and lower line number, DuckDB must not throw ConstraintException."""
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        # Batch 1: Member arrives with line_no = 10
        batch1 = [
            CleanMemberRecord(
                member_id="M_SAME_CTRY_TIE",
                member_name="Winner User",
                enrollment_date=date(2024, 1, 1),
                last_flight_date=date(2024, 1, 1),
                country="USA",
                source_file="file_1.dat",
                extract_date=date(2024, 1, 15),
                line_no=10,
            )
        ]
        engine.load_clean_records(batch1)
        router.execute_country_split()

        # Batch 2: Same-day record for same member with lower line_no = 5 arrives
        batch2 = [
            CleanMemberRecord(
                member_id="M_SAME_CTRY_TIE",
                member_name="Loser User",
                enrollment_date=date(2024, 1, 1),
                last_flight_date=date(2024, 1, 1),
                country="USA",
                source_file="file_1.dat",
                extract_date=date(2024, 1, 15),
                line_no=5,
            )
        ]
        engine.load_clean_records(batch2)
        # Must execute cleanly without ConstraintException
        router.execute_country_split()

        usa_rows = router.get_country_records("table_usa")
        assert len(usa_rows) == 1
        assert usa_rows[0]["member_name"] == "Winner User", "Existing higher line_no record must be retained"
        assert usa_rows[0]["line_no"] == 10

    def test_two_same_day_files_ranked_by_source_file_name_first(self):
        """Claude minor: Two same-day files in one batch are ranked by SOURCE_FILE_NAME before line_no."""
        engine = StagingEngine()
        router = CountryRouter(engine.conn)

        records = [
            CleanMemberRecord(
                member_id="M_FILE_TIE",
                member_name="File A User",
                enrollment_date=date(2024, 1, 1),
                country="USA",
                source_file="file_20240115_a.dat",
                extract_date=date(2024, 1, 15),
                line_no=99,
            ),
            CleanMemberRecord(
                member_id="M_FILE_TIE",
                member_name="File B User",
                enrollment_date=date(2024, 1, 1),
                country="USA",
                source_file="file_20240115_b.dat",
                extract_date=date(2024, 1, 15),
                line_no=1,
            ),
        ]
        engine.load_clean_records(records)
        router.execute_country_split()

        usa_rows = router.get_country_records("table_usa")
        assert len(usa_rows) == 1
        assert usa_rows[0]["member_name"] == "File B User", "File B must win over File A despite lower line_no"
