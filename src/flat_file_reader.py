"""Flat file and dataset reader supporting pipe-delimited feeds and legacy CSVs."""
from typing import List, Optional, Union
import csv
from pathlib import Path
from src.models import RawMemberRecord

FilePath = Union[str, Path]


def read_pipe_delimited_file(file_path: FilePath) -> List[RawMemberRecord]:
    """Read pipe-delimited airline loyalty feed with Header (|H|) and Detail (|D|) records.

    Format: |D|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active
    """
    records: List[RawMemberRecord] = []
    path = Path(file_path)
    if not path.exists():
        return records

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
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

            # Position 1: Member Name
            # Position 2: Member ID
            # Position 3: Enrollment Date (YYYYMMDD)
            # Position 4: Last Flight Date (YYYYMMDD)
            # Position 5: Tier Code
            # Position 6: Agent Name
            # Position 7: State
            # Position 8: Country
            # Position 9: DOB
            # Position 10: Is_Active / Flag
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
                )
            )

    return records


def read_csv_file(
    file_path: FilePath,
    default_country: Optional[str] = None,
    date_format_hint: Optional[str] = None,
) -> List[RawMemberRecord]:
    """Read CSV files (e.g. IND.csv, USA.csv, AUS.csv) with flexible column mapping."""
    records: List[RawMemberRecord] = []
    path = Path(file_path)
    if not path.exists():
        return records

    # Auto-detect USA format hint if not provided and filename contains USA
    hint = date_format_hint
    if hint is None and "USA" in path.name.upper():
        hint = "USA"

    src_system = path.stem.upper() if path.stem else "CSV"

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            norm_row = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items() if k}

            raw_id = norm_row.get("id") or norm_row.get("unique_id") or norm_row.get("member_id")
            member_name = norm_row.get("name") or norm_row.get("member_name")
            enroll_dt = norm_row.get("enrollmentdate") or norm_row.get("enrollment_date") or norm_row.get("date_of_enrollment")
            flight_dt = norm_row.get("flightdate") or norm_row.get("flight_date") or norm_row.get("date_of_flight") or norm_row.get("last_flight_date")
            dob = norm_row.get("dob") or norm_row.get("date_of_birth")
            tier = norm_row.get("tiercode") or norm_row.get("tier_type") or norm_row.get("tier_code")
            country = norm_row.get("country") or default_country

            records.append(
                RawMemberRecord(
                    member_name=member_name,
                    member_id=raw_id,
                    enrollment_date=enroll_dt,
                    last_flight_date=flight_dt,
                    tier_code=tier,
                    country=country,
                    dob=dob,
                    source_file=path.name,
                    source_system=src_system,
                    date_format_hint=hint,
                )
            )

    return records
