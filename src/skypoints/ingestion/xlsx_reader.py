"""Excel reader supporting original company XLSX files (IND.xlsx, USA.xlsx, AUS.xlsx)."""
from typing import List, Optional, Union
from pathlib import Path
import openpyxl

try:
    from skypoints.models import RawMemberRecord
    from skypoints.ingestion.flat_file import extract_ts_from_file_path
except ImportError:
    from src.skypoints.models import RawMemberRecord
    from src.skypoints.ingestion.flat_file import extract_ts_from_file_path

FilePath = Union[str, Path]


def read_xlsx_file(
    file_path: FilePath,
    default_country: Optional[str] = None,
    date_format_hint: Optional[str] = None,
) -> List[RawMemberRecord]:
    """Read XLSX file and extract raw member records with flexible column normalization."""
    records: List[RawMemberRecord] = []
    path = Path(file_path)
    if not path.exists():
        return records

    hint = date_format_hint
    src_system = path.stem.upper() if path.stem else "XLSX"
    file_extract_ts = extract_ts_from_file_path(path)

    wb = openpyxl.load_workbook(path, data_only=True)
    sheet = wb.active

    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        return records

    # Normalize headers
    headers = [str(cell).strip().lower().replace(" ", "_") if cell is not None else "" for cell in rows[0]]

    for line_no, row_values in enumerate(rows[1:], start=2):
        if not any(v is not None and str(v).strip() != "" for v in row_values):
            continue

        norm_row = {}
        for h, v in zip(headers, row_values):
            if h:
                norm_row[h] = str(v).strip() if v is not None else None

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
