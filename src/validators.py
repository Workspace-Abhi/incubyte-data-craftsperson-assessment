"""Data validation, parsing, and dead-letter queue (DLQ) routing logic."""
from datetime import date, datetime
from typing import Optional, Tuple, List, Dict, Any
from src.models import RawMemberRecord, CleanMemberRecord, QuarantineRecord, ValidationResult


def sanitize_string(val: Optional[str]) -> Optional[str]:
    """Sanitize input string: strip whitespace, handle 'NULL' literals and empty strings."""
    if val is None:
        return None
    val_str = str(val).strip()
    if val_str == "" or val_str.upper() == "NULL" or val_str.upper() == "NONE":
        return None
    return val_str


def parse_and_validate_date(val: Any) -> Tuple[Optional[date], Optional[str]]:
    """Parse date strings in multiple formats (YYYYMMDD, DDMMYYYY, YYYY-MM-DD, MM/DD/YYYY).

    Handles edge cases like dropped leading zeros (e.g. 3051985 -> 03051985) and literal 'NULL'.
    Returns (date_obj, None) on success, or (None, error_message) on failure.
    """
    if val is None:
        return None, None
    if isinstance(val, (datetime, date)):
        return (val.date() if isinstance(val, datetime) else val), None

    s = sanitize_string(val)
    if s is None:
        return None, None

    # Handle numeric-only strings
    if s.isdigit():
        # Handle dropped leading zero for 7-digit dates (e.g., 3051985 -> 03051985)
        if len(s) == 7:
            s = "0" + s

        if len(s) == 8:
            # Check if it starts with a realistic year (1900-2099) -> YYYYMMDD
            year_candidate = int(s[:4])
            if 1900 <= year_candidate <= 2099:
                try:
                    return datetime.strptime(s, "%Y%m%d").date(), None
                except ValueError:
                    pass

            # Otherwise, test DDMMYYYY (e.g., 03051985 -> May 3, 1985)
            try:
                return datetime.strptime(s, "%d%m%Y").date(), None
            except ValueError:
                pass

            # Fallback test MMDDYYYY
            try:
                return datetime.strptime(s, "%m%d%Y").date(), None
            except ValueError:
                pass

    # Common standard delimited date formats
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date(), None
        except ValueError:
            continue

    return None, f"Invalid date format or out-of-range calendar values: '{val}'"


def validate_member_record(raw: RawMemberRecord) -> Tuple[Optional[CleanMemberRecord], Optional[str]]:
    """Validate a single raw member record against business rules and data constraints."""
    # 1. Mandatory Field: Member Name
    clean_name = sanitize_string(raw.member_name)
    if not clean_name:
        return None, "Missing mandatory field: Member_Name"

    # 2. Mandatory Field: Member ID
    clean_id = sanitize_string(raw.member_id)
    if not clean_id:
        return None, "Missing mandatory field: Member_Id"

    # 3. Mandatory Field: Enrollment Date
    enroll_raw = sanitize_string(raw.enrollment_date)
    if not enroll_raw:
        return None, "Missing mandatory field: Enrollment_Date"

    enroll_date, err = parse_and_validate_date(enroll_raw)
    if err:
        return None, f"Invalid Enrollment_Date: {err}"
    if enroll_date is None:
        return None, "Missing mandatory field: Enrollment_Date"

    # 4. Optional Fields Validation
    flight_date, err = parse_and_validate_date(raw.last_flight_date)
    if err:
        return None, f"Invalid Last_Flight_Date: {err}"

    dob_date, err = parse_and_validate_date(raw.dob)
    if err:
        return None, f"Invalid Date_of_Birth: {err}"

    # Post code parsing
    post_code: Optional[int] = None
    clean_pc = sanitize_string(raw.post_code)
    if clean_pc:
        try:
            post_code = int(clean_pc)
        except ValueError:
            # Non-blocking or log warning; if strictly int keep as None
            post_code = None

    is_active = sanitize_string(raw.is_active)
    if is_active:
        is_active = is_active.upper()[:1]
    else:
        is_active = "A"

    return CleanMemberRecord(
        member_name=clean_name,
        member_id=clean_id,
        enrollment_date=enroll_date,
        last_flight_date=flight_date,
        tier_code=sanitize_string(raw.tier_code),
        agent_name=sanitize_string(raw.agent_name),
        state=sanitize_string(raw.state),
        country=sanitize_string(raw.country),
        post_code=post_code,
        dob=dob_date,
        is_active=is_active,
    ), None


def validate_member_batch(records: List[RawMemberRecord]) -> ValidationResult:
    """Validate a batch of raw records, separating clean records from quarantined DLQ items."""
    result = ValidationResult()

    for raw in records:
        clean, err = validate_member_record(raw)
        if err:
            result.quarantine_records.append(
                QuarantineRecord(
                    raw_data=raw.__dict__,
                    error_reason=err,
                )
            )
        else:
            result.valid_records.append(clean)

    return result
