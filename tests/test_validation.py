"""TDD tests for data validation, cleaning, and DLQ routing."""
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
        # 03051985 -> May 3, 1985 (or March 5, 1985 per standard DDMMYYYY format)
        result, err = parse_and_validate_date("03051985")
        assert err is None
        assert result == date(1985, 5, 3)

    def test_parse_dob_with_dropped_leading_zero(self):
        # 3051985 has 7 digits; after left-padding to 8 digits -> 03051985
        result, err = parse_and_validate_date("3051985")
        assert err is None
        assert result == date(1985, 5, 3)

    def test_parse_invalid_month_13_fails(self):
        # e.g., '2021-13-13' from real dirty data
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
        assert record.last_flight_date == date(2012, 10, 13)
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

    def test_invalid_enrollment_date_fails(self):
        raw = RawMemberRecord(
            member_name="Elena",
            member_id="223457",
            enrollment_date="2021-13-13",
        )
        record, err = validate_member_record(raw)
        assert record is None
        assert "Invalid Enrollment_Date" in err


class TestBatchValidationAndDLQ:
    def test_batch_splits_clean_and_dlq(self):
        records = [
            # Valid
            RawMemberRecord(
                member_name="Elena",
                member_id="223457",
                enrollment_date="20101012",
                country="USA",
            ),
            # Invalid (Missing Name)
            RawMemberRecord(
                member_name="",
                member_id="223458",
                enrollment_date="20101012",
                country="IND",
            ),
            # Invalid (Bad Date)
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
        assert "Missing mandatory field: Member_Name" in result.quarantine_records[0].error_reason
        assert "Invalid Enrollment_Date" in result.quarantine_records[1].error_reason
