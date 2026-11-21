"""TDD tests for data validation, cleaning, domain rules, and DLQ routing."""
import pytest
from datetime import date
from src.validators import (
    parse_and_validate_date,
    sanitize_string,
    validate_member_record,
    validate_member_batch,
)
from src.models import RawMemberRecord, ValidationResult


class TestDateParsingAndValidation:
    def test_parse_valid_yyyymmdd(self):
        result, err = parse_and_validate_date("20101012")
        assert err is None
        assert result == date(2010, 10, 12)

    def test_parse_valid_iso_date(self):
        result, err = parse_and_validate_date("2022-01-15")
        assert err is None
        assert result == date(2022, 1, 15)

    def test_parse_iso_datetime_string_with_time(self):
        result, err = parse_and_validate_date("2022-05-11 00:00:00")
        assert err is None
        assert result == date(2022, 5, 11)

        result_t, err_t = parse_and_validate_date("2022-05-11T12:30:00")
        assert err_t is None
        assert result_t == date(2022, 5, 11)

    def test_parse_dob_ddmmyyyy_with_leading_zero(self):
        result, err = parse_and_validate_date("03051985")
        assert err is None
        assert result == date(1985, 5, 3)

    def test_parse_dob_with_dropped_leading_zero(self):
        result, err = parse_and_validate_date("3051985")
        assert err is None
        assert result == date(1985, 5, 3)

    def test_parse_usa_source_hint_dates(self):
        # 1052022 -> Jan 05, 2022
        d1, err1 = parse_and_validate_date("1052022", format_hint="USA")
        assert err1 is None
        assert d1 == date(2022, 1, 5)

        # 1152022 -> Jan 15, 2022
        d2, err2 = parse_and_validate_date("1152022", format_hint="USA")
        assert err2 is None
        assert d2 == date(2022, 1, 15)

        # 6152022 -> June 15, 2022
        d3, err3 = parse_and_validate_date("6152022", format_hint="USA")
        assert err3 is None
        assert d3 == date(2022, 6, 15)

        # 12282021 -> Dec 28, 2021
        d4, err4 = parse_and_validate_date("12282021", format_hint="USA")
        assert err4 is None
        assert d4 == date(2021, 12, 28)

    def test_parse_invalid_month_13_fails(self):
        result, err = parse_and_validate_date("2021-13-13")
        assert result is None
        assert "Invalid date format or out-of-range calendar values" in err

    def test_parse_null_string_returns_none_without_error_if_optional(self):
        result, err = parse_and_validate_date("NULL")
        assert result is None
        assert err is None

    def test_parse_empty_or_none_returns_none_without_error(self):
        result, err = parse_and_validate_date("")
        assert result is None
        assert err is None
        result, err = parse_and_validate_date(None)
        assert result is None
        assert err is None


class TestStringSanitization:
    def test_literal_null_string_becomes_none(self):
        assert sanitize_string("NULL") is None
        assert sanitize_string("null") is None
        assert sanitize_string("   ") is None

    def test_strips_whitespace(self):
        assert sanitize_string("  USA  ") == "USA"
        assert sanitize_string("Elena") == "Elena"


class TestMemberValidation:
    def test_valid_member_passes(self):
        raw = RawMemberRecord(
            member_name="Elena",
            member_id="223457",
            enrollment_date="20101012",
            last_flight_date="20121013",
            tier_code="GLD",
            agent_name="Sam",
            state="CA",
            country="USA",
            dob="03051985",
            is_active="A",
        )
        record, err = validate_member_record(raw)
        assert err is None
        assert record is not None
        assert record.member_name == "Elena"
        assert record.member_id == "223457"
        assert record.enrollment_date == date(2010, 10, 12)
        assert record.country == "USA"

    def test_missing_mandatory_name_fails(self):
        raw = RawMemberRecord(
            member_name="",
            member_id="223457",
            enrollment_date="20101012",
        )
        record, err = validate_member_record(raw)
        assert record is None
        assert "Missing mandatory field: Member_Name" in err

    def test_missing_mandatory_id_fails(self):
        raw = RawMemberRecord(
            member_name="Elena",
            member_id="",
            enrollment_date="20101012",
        )
        record, err = validate_member_record(raw)
        assert record is None
        assert "Missing mandatory field: Member_Id" in err

    def test_missing_mandatory_enrollment_date_fails(self):
        raw = RawMemberRecord(
            member_name="Elena",
            member_id="223457",
            enrollment_date=None,
        )
        record, err = validate_member_record(raw)
        assert record is None
        assert "Missing mandatory field: Enrollment_Date" in err

    def test_field_length_checks(self):
        # Member ID exceeds 18 chars
        raw = RawMemberRecord(
            member_name="Elena",
            member_id="12345678901234567890",  # 20 chars
            enrollment_date="20101012",
        )
        record, err = validate_member_record(raw)
        assert record is None
        assert "Member_Id exceeds 18 characters" in err

        # Country exceeds 5 chars
        raw2 = RawMemberRecord(
            member_name="Elena",
            member_id="223457",
            enrollment_date="20101012",
            country="UNITED_STATES",
        )
        record2, err2 = validate_member_record(raw2)
        assert record2 is None
        assert "Country exceeds 5 characters" in err2

    def test_tier_domain_check(self):
        # Invalid tier code within length limit
        raw = RawMemberRecord(
            member_name="Elena",
            member_id="223457",
            enrollment_date="20101012",
            tier_code="VIP",  # Not in PLT, GLD, SLV
            country="USA",
        )
        record, err = validate_member_record(raw)
        assert record is None
        assert "Invalid domain value: Tier_Code 'VIP'" in err

    def test_country_canonicalization_and_unmapped_quarantine(self):
        # AUS canonicalizes to AU
        rec1, err1 = validate_member_record(
            RawMemberRecord(member_name="A", member_id="1", enrollment_date="20200101", country="AUS")
        )
        assert err1 is None
        assert rec1.country == "AU"

        # INDIA canonicalizes to IND
        rec2, err2 = validate_member_record(
            RawMemberRecord(member_name="B", member_id="2", enrollment_date="20200101", country="INDIA")
        )
        assert err2 is None
        assert rec2.country == "IND"

        # PH is unmapped and gets quarantined
        rec3, err3 = validate_member_record(
            RawMemberRecord(member_name="C", member_id="3", enrollment_date="20200101", country="PH")
        )
        assert rec3 is None
        assert "Unmapped country" in err3

        # Unmapped country gets quarantined
        rec4, err4 = validate_member_record(
            RawMemberRecord(member_name="D", member_id="4", enrollment_date="20200101", country="GER")
        )
        assert rec4 is None
        assert "Unmapped country" in err4

        # Missing country gets quarantined
        rec5, err5 = validate_member_record(
            RawMemberRecord(member_name="E", member_id="5", enrollment_date="20200101", country=None)
        )
        assert rec5 is None
        assert "Unmapped country" in err5


class TestBatchValidationAndDLQ:
    def test_batch_splits_clean_and_dlq(self):
        records = [
            RawMemberRecord(
                member_name="Elena",
                member_id="223457",
                enrollment_date="20101012",
                country="USA",
                tier_code="GLD",
            ),
            RawMemberRecord(
                member_name="",
                member_id="223458",
                enrollment_date="20101012",
                country="IND",
            ),
            RawMemberRecord(
                member_name="Corrupt",
                member_id="223459",
                enrollment_date="2021-13-13",
                country="USA",
            ),
        ]
        result = validate_member_batch(records)
        assert len(result.valid_records) == 1
        assert len(result.quarantine_records) == 2
        assert result.valid_records[0].member_name == "Elena"

    def test_key_collision_detection_across_different_people(self):
        # ID 1 reused for Vikas and Sam
        records = [
            RawMemberRecord(member_name="Vikas", member_id="1", enrollment_date="20220101", country="IND"),
            RawMemberRecord(member_name="Sam", member_id="1", enrollment_date="20220101", country="USA"),
        ]
        result = validate_member_batch(records)
        assert len(result.valid_records) == 1
        assert len(result.quarantine_records) == 1
        assert len(result.key_collisions) == 1
        assert "Key collision detected: Member_Id '1' reused across multiple people" in result.key_collisions[0]

    def test_collision_rename_order_independent_favors_latest(self):
        # Alice gets married and renames from Alice Brown to Alice Green
        older = RawMemberRecord(
            member_name="Alice Brown",
            member_id="M500",
            enrollment_date="20200101",
            last_flight_date="20210601",
            country="USA",
        )
        newer = RawMemberRecord(
            member_name="Alice Green",
            member_id="M500",
            enrollment_date="20200101",
            last_flight_date="20230815",
            country="USA",
        )

        # 1. Forward order: older first, newer second
        res_fwd = validate_member_batch([older, newer])
        assert len(res_fwd.valid_records) == 1
        assert res_fwd.valid_records[0].member_name == "Alice Green"
        assert len(res_fwd.quarantine_records) == 1
        assert res_fwd.quarantine_records[0].error_reason == "Key collision: superseded by newer record"
        assert len(res_fwd.key_collisions) == 1

        # 2. Reverse order: newer first, older second -> same outcome!
        res_rev = validate_member_batch([newer, older])
        assert len(res_rev.valid_records) == 1
        assert res_rev.valid_records[0].member_name == "Alice Green"
        assert len(res_rev.quarantine_records) == 1
        assert res_rev.quarantine_records[0].error_reason == "Key collision: superseded by newer record"
        assert len(res_rev.key_collisions) == 1

    def test_identical_duplicates_not_counted_as_collision(self):
        rec1 = RawMemberRecord(member_name="Bob Smith", member_id="M600", enrollment_date="20200101", country="USA")
        rec2 = RawMemberRecord(member_name="Bob Smith", member_id="M600", enrollment_date="20200101", country="USA")

        res = validate_member_batch([rec1, rec2])
        assert len(res.key_collisions) == 0, "Identical duplicate records must not be counted as a collision"
        assert len(res.quarantine_records) == 0
        assert len(res.valid_records) == 2

    def test_sample_flat_file_triggers_both_data_quality_warnings(self):
        from pathlib import Path
        from src.flat_file_reader import read_pipe_delimited_file
        data_path = Path(__file__).resolve().parent.parent.parent / "data" / "samples" / "sample_member_profile.dat"
        raw_records = read_pipe_delimited_file(str(data_path))
        result = validate_member_batch(raw_records)

        # Both warnings should trigger on sample_member_profile.dat
        assert len(result.warnings) == 2
        id_warning = next(w for w in result.warnings if "Member_Id length differs" in w)
        assert "22345" in id_warning or "2256" in id_warning
        assert "batch mode length (6)" in id_warning

        dob_warning = next(w for w in result.warnings if "shares identical DOB" in w)
        assert "1985-05-03" in dob_warning

    def test_collision_deterministic_tie_break_on_identical_dates(self):
        # Two records with same date, same ID, different names and different line_no
        rec_a = RawMemberRecord(
            member_name="Alice A",
            member_id="TIE_1",
            enrollment_date="20200101",
            country="USA",
            line_no=1,
            source_file="file_a.dat",
        )
        rec_b = RawMemberRecord(
            member_name="Alice B",
            member_id="TIE_1",
            enrollment_date="20200101",
            country="USA",
            line_no=2,
            source_file="file_b.dat",
        )

        res1 = validate_member_batch([rec_a, rec_b])
        res2 = validate_member_batch([rec_b, rec_a])

        # Deterministic: line_no 2 > line_no 1, so Alice B wins regardless of input order
        assert res1.valid_records[0].member_name == "Alice B"
        assert res2.valid_records[0].member_name == "Alice B"

    def test_collision_intraday_timestamp_wins_over_alphabetical_file_order(self):
        from datetime import datetime
        # rec_morning has file 'zzz.dat' (sorts last) but earlier time (9am)
        rec_morning = RawMemberRecord(
            member_name="Morning Name",
            member_id="TIME_1",
            enrollment_date="20200101",
            country="USA",
            source_file="zzz_20240115_090000.dat",
            extract_ts=datetime(2024, 1, 15, 9, 0, 0),
            line_no=99,
        )
        # rec_afternoon has file 'aaa.dat' (sorts first) but later time (3pm)
        rec_afternoon = RawMemberRecord(
            member_name="Afternoon Name",
            member_id="TIME_1",
            enrollment_date="20200101",
            country="USA",
            source_file="aaa_20240115_150000.dat",
            extract_ts=datetime(2024, 1, 15, 15, 0, 0),
            line_no=1,
        )

        res = validate_member_batch([rec_morning, rec_afternoon])
        assert res.valid_records[0].member_name == "Afternoon Name"
        assert res.quarantine_records[0].error_reason == "Key collision: superseded by newer record"
