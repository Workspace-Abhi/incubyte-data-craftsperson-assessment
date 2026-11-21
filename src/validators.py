"""Data validation, parsing, domain checks, and dead-letter queue (DLQ) routing logic."""
from datetime import date, datetime
from typing import Optional, Tuple, List, Dict, Any, Set
from src.models import RawMemberRecord, CleanMemberRecord, QuarantineRecord, ValidationResult
from src.country_router import normalize_country

# Valid business domain sets
VALID_TIER_CODES = {"PLT", "GLD", "SLV"}
VALID_ACTIVE_FLAGS = {"A", "I"}


def sanitize_string(val: Optional[str]) -> Optional[str]:
    """Sanitize input string: strip whitespace, handle 'NULL' literals and empty strings."""
    if val is None:
        return None
    val_str = str(val).strip()
    if val_str == "" or val_str.upper() == "NULL" or val_str.upper() == "NONE":
        return None
    return val_str


def parse_and_validate_date(val: Any, format_hint: Optional[str] = None) -> Tuple[Optional[date], Optional[str]]:
    """Parse date strings according to format hint or standard multi-format parsing.

    Handles edge cases like dropped leading zeros (e.g. 3051985 -> 03051985),
    USA single-digit month formats (e.g. 6152022 -> 2022-06-15, 1052022 -> 2022-01-05),
    and invalid calendar months like month 13 (2021-13-13).
    """
    if val is None:
        return None, None
    if isinstance(val, (datetime, date)):
        return (val.date() if isinstance(val, datetime) else val), None

    s = sanitize_string(val)
    if s is None:
        return None, None

    # 1. Source-specific hint parsing
    if format_hint and format_hint.upper() in ("USA", "MDDYYYY", "MMDDYYYY"):
        if s.isdigit():
            # 7-digit USA date: M (1 digit) + DD (2 digits) + YYYY (4 digits)
            # E.g. 6152022 -> June 15, 2022; 1052022 -> Jan 05, 2022; 1152022 -> Jan 15, 2022
            if len(s) == 7:
                m_str, d_str, y_str = s[0], s[1:3], s[3:]
                try:
                    return date(int(y_str), int(m_str), int(d_str)), None
                except ValueError as e:
                    return None, f"Invalid USA 7-digit date '{val}': {e}"
            # 8-digit USA date: MM (2 digits) + DD (2 digits) + YYYY (4 digits)
            elif len(s) == 8:
                m_str, d_str, y_str = s[:2], s[2:4], s[4:]
                try:
                    return date(int(y_str), int(m_str), int(d_str)), None
                except ValueError as e:
                    return None, f"Invalid USA 8-digit date '{val}': {e}"

    if format_hint and format_hint.upper() in ("YYYYMMDD", "FLAT_FILE"):
        if s.isdigit() and len(s) == 8:
            try:
                return datetime.strptime(s, "%Y%m%d").date(), None
            except ValueError as e:
                return None, f"Invalid YYYYMMDD date '{val}': {e}"

    # 2. Heuristic parsing for unhinted inputs
    if s.isdigit():
        # Handle 7-digit DOB dropped leading zero (e.g., 3051985 -> 03051985)
        if len(s) == 7:
            # If starts with reasonable month/day, pad with leading 0
            s_padded = "0" + s
            # Try DDMMYYYY
            try:
                return datetime.strptime(s_padded, "%d%m%Y").date(), None
            except ValueError:
                pass
            # Try USA MDDYYYY fallback
            try:
                m_str, d_str, y_str = s[0], s[1:3], s[3:]
                return date(int(y_str), int(m_str), int(d_str)), None
            except ValueError:
                pass

        if len(s) == 8:
            # Check if starts with a realistic year (1900-2099) -> YYYYMMDD
            year_candidate = int(s[:4])
            if 1900 <= year_candidate <= 2099:
                try:
                    return datetime.strptime(s, "%Y%m%d").date(), None
                except ValueError:
                    pass

            # Otherwise test DDMMYYYY (e.g., 03051985 -> May 3, 1985)
            try:
                return datetime.strptime(s, "%d%m%Y").date(), None
            except ValueError:
                pass

            # Test MMDDYYYY
            try:
                return datetime.strptime(s, "%m%d%Y").date(), None
            except ValueError:
                pass

    # Standard delimited date formats
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date(), None
        except ValueError:
            continue

    return None, f"Invalid date format or out-of-range calendar values: '{val}'"


def validate_member_record(raw: RawMemberRecord) -> Tuple[Optional[CleanMemberRecord], Optional[str]]:
    """Validate a single raw member record against schema constraints, field lengths, and domain values."""
    # 1. Mandatory Field: Member Name (Max length 255)
    clean_name = sanitize_string(raw.member_name)
    if not clean_name:
        return None, "Missing mandatory field: Member_Name"
    if len(clean_name) > 255:
        return None, f"Field length exceeded: Member_Name exceeds 255 characters (length={len(clean_name)})"

    # 2. Mandatory Field: Member ID (Max length 18 per spec)
    clean_id = sanitize_string(raw.member_id)
    if not clean_id:
        return None, "Missing mandatory field: Member_Id"
    if len(clean_id) > 18:
        return None, f"Field length exceeded: Member_Id exceeds 18 characters (length={len(clean_id)})"

    # 3. Mandatory Field: Enrollment Date
    enroll_raw = sanitize_string(raw.enrollment_date)
    if not enroll_raw:
        return None, "Missing mandatory field: Enrollment_Date"

    enroll_date, err = parse_and_validate_date(enroll_raw, format_hint=raw.date_format_hint)
    if err:
        return None, f"Invalid Enrollment_Date: {err}"
    if enroll_date is None:
        return None, "Missing mandatory field: Enrollment_Date"

    # 4. Optional Fields Validation
    flight_date, err = parse_and_validate_date(raw.last_flight_date, format_hint=raw.date_format_hint)
    if err:
        return None, f"Invalid Last_Flight_Date: {err}"

    dob_date, err = parse_and_validate_date(raw.dob, format_hint=None)
    if err:
        return None, f"Invalid Date_of_Birth: {err}"

    # 5. Schema Length & Domain Validations
    clean_state = sanitize_string(raw.state)
    if clean_state and len(clean_state) > 5:
        return None, f"Field length exceeded: State exceeds 5 characters ('{clean_state}')"

    clean_country = sanitize_string(raw.country)
    if not clean_country:
        return None, "Unmapped country: Country is required and cannot be empty"
    clean_country = clean_country.upper()
    if len(clean_country) > 5:
        return None, f"Field length exceeded: Country exceeds 5 characters ('{clean_country}')"

    canonical_country = normalize_country(clean_country)
    if not canonical_country:
        return None, f"Unmapped country: '{clean_country}' is not recognized as a mapped country"
    clean_country = canonical_country

    clean_tier = sanitize_string(raw.tier_code)
    if clean_tier:
        clean_tier = clean_tier.upper()
        if len(clean_tier) > 5:
            return None, f"Field length exceeded: Tier_Code exceeds 5 characters ('{clean_tier}')"
        if clean_tier not in VALID_TIER_CODES:
            return None, f"Invalid domain value: Tier_Code '{clean_tier}' is not in {VALID_TIER_CODES}"

    clean_agent = sanitize_string(raw.agent_name)
    if clean_agent and len(clean_agent) > 255:
        return None, f"Field length exceeded: Agent_Name exceeds 255 characters"

    # Post code parsing (Max length 5 per spec)
    post_code: Optional[int] = None
    clean_pc = sanitize_string(raw.post_code)
    if clean_pc:
        if len(clean_pc) > 5:
            return None, f"Field length exceeded: Post_Code exceeds 5 digits ('{clean_pc}')"
        try:
            post_code = int(clean_pc)
        except ValueError:
            return None, f"Invalid data type: Post_Code must be numeric ('{clean_pc}')"

    is_active = sanitize_string(raw.is_active)
    if is_active:
        is_active = is_active.upper()[:1]
        if is_active not in VALID_ACTIVE_FLAGS:
            return None, f"Invalid domain value: Is_Active must be in {VALID_ACTIVE_FLAGS} ('{is_active}')"
    else:
        is_active = "A"

    return CleanMemberRecord(
        member_name=clean_name,
        member_id=clean_id,
        enrollment_date=enroll_date,
        last_flight_date=flight_date,
        tier_code=clean_tier,
        agent_name=clean_agent,
        state=clean_state,
        country=clean_country,
        post_code=post_code,
        dob=dob_date,
        is_active=is_active,
        source_file=raw.source_file,
        source_system=raw.source_system,
    ), None


def validate_member_batch(records: List[RawMemberRecord]) -> ValidationResult:
    """Validate a batch of raw records, detecting key collisions and routing errors to DLQ.

    Order-independent: Groups by (source_system, member_id). If names differ, keeps
    the record with latest COALESCE(last_flight_date, enrollment_date), tie-breaking
    by source_file name and row order (index). Quarantines older conflicting records
    with reason 'Key collision: superseded by newer record'.

    Also computes non-blocking batch data quality warnings (member_id length anomalies
    and DOB concentration > 50%).
    """
    from collections import Counter
    result = ValidationResult()

    # 1. First pass: individual validation and grouping valid records
    groups: Dict[Tuple[str, str], List[Tuple[CleanMemberRecord, RawMemberRecord, int]]] = {}

    for idx, raw in enumerate(records):
        clean, err = validate_member_record(raw)
        if err:
            result.quarantine_records.append(
                QuarantineRecord(
                    raw_data=raw.__dict__,
                    error_reason=err,
                )
            )
        else:
            key = (clean.source_system, clean.member_id)
            if key not in groups:
                groups[key] = []
            groups[key].append((clean, raw, idx))

    # 2. Second pass: evaluate collision and renames per (source_system, member_id)
    for (src_sys, mid), items in groups.items():
        distinct_names = {c.member_name.strip().lower() for c, _, _ in items}

        if len(distinct_names) <= 1:
            # Same name throughout: identical duplicates or multiple events for same person
            for clean, _, _ in items:
                result.valid_records.append(clean)
        else:
            # Names differ: sort deterministically by COALESCE(last_flight_date, enrollment_date) DESC,
            # source_file name DESC, and row index DESC
            items_sorted = sorted(
                items,
                key=lambda triple: (
                    triple[0].last_flight_date or triple[0].enrollment_date,
                    triple[0].source_file or "",
                    triple[2],
                ),
                reverse=True,
            )
            winner_clean, _, _ = items_sorted[0]
            winner_name_norm = winner_clean.member_name.strip().lower()

            collision_msg = (
                f"Key collision detected: Member_Id '{mid}' reused across multiple people "
                f"in source '{src_sys}'. Kept latest '{winner_clean.member_name}'"
            )
            result.key_collisions.append(collision_msg)

            # Keep the latest record
            result.valid_records.append(winner_clean)

            # Quarantine older conflicting records
            for clean, raw, _ in items_sorted[1:]:
                if clean.member_name.strip().lower() != winner_name_norm:
                    result.quarantine_records.append(
                        QuarantineRecord(
                            raw_data=raw.__dict__,
                            error_reason="Key collision: superseded by newer record",
                        )
                    )
                else:
                    # Same name duplicate of the winning record
                    result.valid_records.append(clean)

    # 3. Non-blocking data quality warnings across valid records in this batch
    if result.valid_records:
        # Check A: Member_Id length differs from batch mode length
        id_lengths = [len(c.member_id) for c in result.valid_records if c.member_id]
        if id_lengths:
            len_counts = Counter(id_lengths)
            mode_len, _ = len_counts.most_common(1)[0]
            anomalous_ids = [c.member_id for c in result.valid_records if c.member_id and len(c.member_id) != mode_len]
            if anomalous_ids:
                result.warnings.append(
                    f"Data quality warning: Member_Id length differs from batch mode length ({mode_len}). "
                    f"Anomalous count: {len(anomalous_ids)}, IDs: {sorted(set(anomalous_ids))}"
                )

        # Check B: More than 50% of the batch shares one identical DOB
        valid_dobs = [c.dob for c in result.valid_records if c.dob]
        if valid_dobs and len(result.valid_records) > 0:
            dob_counts = Counter(valid_dobs)
            mode_dob, dob_count = dob_counts.most_common(1)[0]
            if dob_count / len(result.valid_records) > 0.5:
                result.warnings.append(
                    f"Data quality warning: More than 50% of batch ({dob_count}/{len(result.valid_records)}) "
                    f"shares identical DOB: {mode_dob}"
                )

    return result
