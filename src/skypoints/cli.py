"""SkyPoints Loyalty Pipeline CLI & Live Demonstration Runner.

Usage:
    python -m src.main
"""
import sys
from pathlib import Path
from datetime import date
from skypoints.pipeline import SkyPointsPipeline


def print_banner():
    print("=" * 80)
    print("  SKYPOINTS GLOBAL AIRLINE LOYALTY ETL PIPELINE")
    print("  Incubyte Data Craftsperson Assessment Demonstration")
    print("=" * 80)


def print_section(title: str):
    print(f"\n--- {title} " + "-" * (75 - len(title)))


def format_table(rows: list, headers: list, max_rows: int = 5):
    if not rows:
        print("  (No rows found)")
        return
    col_widths = {h: len(h) for h in headers}
    for row in rows[:max_rows]:
        for h in headers:
            val_str = str(row.get(h, ""))
            col_widths[h] = max(col_widths[h], len(val_str))

    header_line = " | ".join(h.ljust(col_widths[h]) for h in headers)
    sep_line = "-+-".join("-" * col_widths[h] for h in headers)
    print(f"  {header_line}")
    print(f"  {sep_line}")
    for row in rows[:max_rows]:
        line = " | ".join(str(row.get(h, "")).ljust(col_widths[h]) for h in headers)
        print(f"  {line}")
    if len(rows) > max_rows:
        print(f"  ... and {len(rows) - max_rows} more rows.")


def main():
    print_banner()

    root_dir = Path(__file__).resolve().parent.parent.parent
    data_dir = root_dir / "data"
    raw_dir = data_dir / "raw"
    samples_dir = data_dir / "samples"
    derived_dir = data_dir / "derived"

    json_file = samples_dir / "sample_redemptions.json"
    if not json_file.exists():
        json_file = data_dir / "sample_redemptions.json"

    # Ingest only company raw files: IND.csv, USA.csv, and AUS.xlsx
    csv_mappings = {
        str(raw_dir / "IND.csv"): "IND",
        str(raw_dir / "USA.csv"): "USA",
    }
    xlsx_mappings = {
        str(raw_dir / "AUS.xlsx"): "AUS",
    }
    csv_date_hints = {
        str(raw_dir / "USA.csv"): "USA",
    }

    print(f"Data Directory: {data_dir}")
    print("Ingesting Company Raw Feeds: IND.csv, USA.csv, AUS.xlsx")
    if json_file.exists():
        print(f"Ingesting Partner JSON: {json_file.name}")

    pipeline = SkyPointsPipeline()
    # Fixed reference date matching assessment data timeframe
    ref_date = date(2024, 1, 15)
    result = pipeline.run(
        flat_file_path=None,
        json_file_path=str(json_file) if json_file.exists() else None,
        csv_file_mappings=csv_mappings,
        xlsx_file_mappings=xlsx_mappings,
        csv_date_format_hints=csv_date_hints,
        reference_date=ref_date,
    )

    print_section("1. INGESTION & DATA QUALITY VALIDATION METRICS")
    m = result.metrics
    print(f"  Total Raw Records Ingested : {m.total_raw_records}")
    print(f"  Valid Staged Records       : {m.valid_records_count}")
    print(f"  Quarantined (DLQ) Records  : {m.quarantined_records_count}")
    print(f"  Key Collisions Detected    : {m.key_collisions_count}")
    print(f"  Data Quality Warnings      : {m.warnings_count}")
    print(f"  Orphan Redemptions Detected: {len(result.orphan_redemptions)}")

    if result.warnings:
        print("\n  [Data Quality Warnings]")
        for w in result.warnings:
            print(f"    ! {w}")

    if result.quarantined_records:
        print("\n  [Dead Letter Queue Samples (Quarantined)]")
        for q in result.quarantined_records[:3]:
            print(f"    - Error: {q.error_reason}")
            print(f"      Raw Data: {q.raw_data}")

    print_section("2. COUNTRY-SPECIFIC TARGET TABLES (LATEST RECORD WINS)")
    for tbl, count in m.country_table_counts.items():
        print(f"  * {tbl.ljust(22)}: {count} member(s)")

    print_section("3. SAMPLE: TABLE_INDIA PREVIEW")
    format_table(
        result.country_data.get("table_india", []),
        ["member_id", "member_name", "country", "tier_code", "age", "stale_member", "last_flight_date"],
    )

    print_section("4. SAMPLE: TABLE_USA PREVIEW")
    format_table(
        result.country_data.get("table_usa", []),
        ["member_id", "member_name", "country", "tier_code", "age", "stale_member", "last_flight_date"],
    )

    print_section("5. MEMBER LOYALTY 360 MART (COMPLETED VS PENDING MILES)")
    format_table(
        result.loyalty_360_data,
        ["member_id", "member_name", "country", "tier_code", "age", "stale_member", "total_redemptions", "total_miles_redeemed", "pending_miles_redeemed", "top_partner"],
    )

    print("\n" + "=" * 80)
    print("  PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 80)


if __name__ == "__main__":
    main()
