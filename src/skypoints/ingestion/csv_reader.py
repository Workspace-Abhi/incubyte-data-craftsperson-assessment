"""CSV reader supporting legacy country CSV exports."""
from typing import List, Optional, Union
import csv
from pathlib import Path

try:
    from skypoints.models import RawMemberRecord
    from skypoints.ingestion.flat_file import extract_ts_from_file_path
except ImportError:
    from src.skypoints.models import RawMemberRecord
    from src.skypoints.ingestion.flat_file import extract_ts_from_file_path

FilePath = Union[str, Path]


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

    hint = date_format_hint
    src_system = path.stem.upper() if path.stem else "CSV"
    file_extract_ts = extract_ts_from_file_path(path)

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line_no, row in enumerate(reader, start=2):
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
                    extract_ts=file_extract_ts,
                    line_no=line_no,
                )
            )

    return records
