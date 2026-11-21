"""Data models for raw, validated, and quarantined member records."""
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional, List, Dict, Any


@dataclass
class RawMemberRecord:
    member_name: Optional[str] = None
    member_id: Optional[str] = None
    enrollment_date: Optional[str] = None
    last_flight_date: Optional[str] = None
    tier_code: Optional[str] = None
    agent_name: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    post_code: Optional[str] = None
    dob: Optional[str] = None
    is_active: Optional[str] = None
    source_file: Optional[str] = None
    source_system: str = "FLAT_FILE"
    date_format_hint: Optional[str] = None
    extract_date: Optional[date] = None
    extract_ts: Optional[datetime] = None
    line_no: Optional[int] = None

    def __post_init__(self):
        if self.extract_ts is not None and self.extract_date is None:
            self.extract_date = self.extract_ts.date()
        elif self.extract_date is not None and self.extract_ts is None:
            self.extract_ts = datetime(self.extract_date.year, self.extract_date.month, self.extract_date.day)


@dataclass
class CleanMemberRecord:
    member_name: str
    member_id: str
    enrollment_date: date
    last_flight_date: Optional[date] = None
    tier_code: Optional[str] = None
    agent_name: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    post_code: Optional[int] = None
    dob: Optional[date] = None
    is_active: Optional[str] = "A"
    source_file: Optional[str] = None
    source_system: str = "FLAT_FILE"
    extract_date: Optional[date] = None
    extract_ts: Optional[datetime] = None
    line_no: Optional[int] = None

    def __post_init__(self):
        if self.extract_ts is not None and self.extract_date is None:
            self.extract_date = self.extract_ts.date()
        elif self.extract_date is not None and self.extract_ts is None:
            self.extract_ts = datetime(self.extract_date.year, self.extract_date.month, self.extract_date.day)


@dataclass
class QuarantineRecord:
    raw_data: Dict[str, Any]
    error_reason: str
    quarantined_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ValidationResult:
    valid_records: List[CleanMemberRecord] = field(default_factory=list)
    quarantine_records: List[QuarantineRecord] = field(default_factory=list)
    key_collisions: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
