"""Flat file reader supporting pipe-delimited loyalty feeds."""
from typing import List, Optional, Union
import re
from datetime import date, datetime
from pathlib import Path

try:
    from skypoints.models import RawMemberRecord
except ImportError:
    from src.skypoints.models import RawMemberRecord

FilePath = Union[str, Path]


def extract_ts_from_file_path(path: Path) -> datetime:
    """Extract extract_ts from filename: parses YYYYMMDD and optional HHMMSS / HHMMSSTT, defaulting to 00:00:00, fallback to load time."""
    name = path.name
    match = re.search(r'(\d{8})(?:[_\s-]?(\d{6}))?', name)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        try:
            d = datetime.strptime(date_str, "%Y%m%d")
            if time_str:
                return d.replace(
                    hour=int(time_str[:2]),
                    minute=int(time_str[2:4]),
                    second=int(time_str[4:6]),
                )
            return d
        except ValueError:
            pass
    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return datetime.now()


def extract_date_from_file_path(path: Path) -> date:
    """Extract extract_date from filename, falling back to mtime date."""
    return extract_ts_from_file_path(path).date()


def read_pipe_delimited_file(file_path: FilePath) -> List[RawMemberRecord]:
    """Read pipe-delimited airline loyalty feed with Header (|H|) and Detail (|D|) records.

    Format: |D|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active
    """
    records: List[RawMemberRecord] = []
    path = Path(file_path)
    if not path.exists():
        return records

    file_extract_ts = extract_ts_from_file_path(path)

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            clean_line = line.strip()
            if not clean_line:
                continue

            parts = [p.strip() for p in clean_line.split("|")]
            if parts and parts[0] == "":
                parts = parts[1:]
            if parts and parts[-1] == "":
                parts = parts[:-1]

            if not parts:
                continue

            rec_type = parts[0].upper()
            if rec_type != "D":
                continue

            member_name = parts[1] if len(parts) > 1 else None
            member_id = parts[2] if len(parts) > 2 else None
            enrollment_date = parts[3] if len(parts) > 3 else None
            last_flight_date = parts[4] if len(parts) > 4 else None
            tier_code = parts[5] if len(parts) > 5 else None
            agent_name = parts[6] if len(parts) > 6 else None
            state = parts[7] if len(parts) > 7 else None
            country = parts[8] if len(parts) > 8 else None
            dob = parts[9] if len(parts) > 9 else None
            is_active = parts[10] if len(parts) > 10 else None

            records.append(
                RawMemberRecord(
                    member_name=member_name,
                    member_id=member_id,
                    enrollment_date=enrollment_date,
                    last_flight_date=last_flight_date,
                    tier_code=tier_code,
                    agent_name=agent_name,
                    state=state,
                    country=country,
                    dob=dob,
                    is_active=is_active,
                    source_file=path.name,
                    source_system="FLAT_FILE",
                    date_format_hint="YYYYMMDD",
                    extract_ts=file_extract_ts,
                    line_no=line_no,
                )
            )

    return records
