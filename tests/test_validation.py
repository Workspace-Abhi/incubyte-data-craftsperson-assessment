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

        # PH canonicalizes to PHIL
        rec3, err3 = validate_member_record(
            RawMemberRecord(member_name="C", member_id="3", enrollment_date="20200101", country="PH")
        )
        assert err3 is None
        assert rec3.country == "PHIL"

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
