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


def extract_date_from_file_path(path: Path) -> date:
    """Extract extract_date from the first YYYYMMDD in filename, falling back to mtime."""
    match = re.search(r'(\d{8})', path.name)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d").date()
        except ValueError:
            pass
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).date()
    except Exception:
        return date.today()


def read_pipe_delimited_file(file_path: FilePath) -> List[RawMemberRecord]:
    """Read pipe-delimited airline loyalty feed with Header (|H|) and Detail (|D|) records.

    Format: |D|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active
    """
    records: List[RawMemberRecord] = []
    path = Path(file_path)
    if not path.exists():
        return records

    file_extract_date = extract_date_from_file_path(path)

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

            record_type = parts[0].upper()
            if record_type != "D":
                continue

            name = parts[1] if len(parts) > 1 else None
            member_id = parts[2] if len(parts) > 2 else None
            enroll_dt = parts[3] if len(parts) > 3 else None
            flight_dt = parts[4] if len(parts) > 4 else None
            tier = parts[5] if len(parts) > 5 else None
            agent = parts[6] if len(parts) > 6 else None
            state = parts[7] if len(parts) > 7 else None
            country = parts[8] if len(parts) > 8 else None
            dob = parts[9] if len(parts) > 9 else None
            is_active = parts[10] if len(parts) > 10 else "A"

            records.append(
                RawMemberRecord(
                    member_name=name,
                    member_id=member_id,
                    enrollment_date=enroll_dt,
                    last_flight_date=flight_dt,
                    tier_code=tier,
                    agent_name=agent,
                    state=state,
                    country=country,
                    dob=dob,
                    is_active=is_active,
                    source_file=path.name,
                    source_system="FLAT_FILE",
                    date_format_hint="FLAT_FILE",
                    extract_date=file_extract_date,
                    line_no=line_no,
                )
            )

    return records
