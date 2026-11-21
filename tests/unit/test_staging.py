"""TDD tests for staging transformations (Age & Stale Member)."""
import pytest
from datetime import date
from src.models import CleanMemberRecord
from src.staging import compute_age, compute_stale_member, StagingEngine


class TestAgeComputation:
    def test_exact_age_birthday_passed(self):
        dob = date(1985, 5, 3)
        ref = date(2026, 11, 21)
        # Birthday passed in May 2026 -> 41 years
        assert compute_age(dob, ref) == 41

    def test_age_birthday_not_yet_occurred(self):
        dob = date(1985, 12, 25)
        ref = date(2026, 11, 21)
        # Birthday has not passed yet -> 40 years
        assert compute_age(dob, ref) == 40

    def test_null_dob_returns_none(self):
        assert compute_age(None) is None

    def test_future_dob_returns_none(self):
        dob = date(2030, 1, 1)
        ref = date(2026, 11, 21)
        assert compute_age(dob, ref) is None


class TestStaleMemberComputation:
    def test_flight_within_90_days_is_active(self):
        # 30 days ago
        flight_date = date(2026, 10, 22)
        ref = date(2026, 11, 21)
        assert compute_stale_member(flight_date, ref) == "N"

    def test_flight_exactly_90_days_is_not_stale(self):
        # 90 days ago
        flight_date = date(2026, 8, 23)
        ref = date(2026, 11, 21)
        assert compute_stale_member(flight_date, ref) == "N"

    def test_flight_beyond_90_days_is_stale(self):
        # 91 days ago
        flight_date = date(2026, 8, 22)
        ref = date(2026, 11, 21)
        assert compute_stale_member(flight_date, ref) == "Y"

    def test_null_flight_date_is_stale(self):
        assert compute_stale_member(None) == "Y"


class TestStagingEngine:
    def test_load_and_retrieve_staged_records(self):
        engine = StagingEngine()
        records = [
            CleanMemberRecord(
                member_name="Elena",
                member_id="223457",
                enrollment_date=date(2010, 10, 12),
                last_flight_date=date(2012, 10, 13),
                tier_code="GLD",
                agent_name="Sam",
                state="CA",
                country="USA",
                dob=date(1985, 5, 3),
                is_active="A",
            ),
            CleanMemberRecord(
                member_name="Ravi",
                member_id="223458",
                enrollment_date=date(2010, 10, 12),
                last_flight_date=date(2026, 11, 1),
                tier_code="SLV",
                agent_name="Sam",
                state="TN",
                country="IND",
                dob=date(1990, 1, 1),
                is_active="A",
            ),
        ]
        ref = date(2026, 11, 21)
        count = engine.load_clean_records(records, reference_date=ref)
        assert count == 2

        staged = engine.get_staged_records()
        assert len(staged) == 2

        # Verify Elena: flight in 2012 (> 90 days ago) -> Stale = 'Y'
        elena = next(r for r in staged if r["member_id"] == "223457")
        assert elena["age"] == 41
        assert elena["stale_member"] == "Y"

        # Verify Ravi: flight 20 days ago -> Stale = 'N'
        ravi = next(r for r in staged if r["member_id"] == "223458")
        assert ravi["age"] == 36
        assert ravi["stale_member"] == "N"
