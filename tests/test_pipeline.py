"""End-to-end integration tests for SkyPoints pipeline execution."""
import pytest
from pathlib import Path
from datetime import date
from src.pipeline import SkyPointsPipeline


def test_full_pipeline_with_sample_files():
    data_dir = Path(__file__).parent.parent / "data"
    flat_file = str(data_dir / "sample_member_profile.dat")
    json_file = str(data_dir / "sample_redemptions.json")
    csv_mappings = {
        str(data_dir / "IND.csv"): "IND",
        str(data_dir / "USA.csv"): "USA",
    }

    pipeline = SkyPointsPipeline()
    ref_date = date(2024, 1, 15)
    result = pipeline.run(
        flat_file_path=flat_file,
        json_file_path=json_file,
        csv_file_mappings=csv_mappings,
        reference_date=ref_date,
    )

    metrics = result.metrics
    assert metrics.total_raw_records > 0
    assert metrics.valid_records_count > 0
    assert metrics.total_redemptions_parsed == 2

    # Check country split
    assert "table_india" in metrics.country_table_counts
    assert "table_usa" in metrics.country_table_counts
    assert metrics.country_table_counts["table_india"] >= 1
    assert metrics.country_table_counts["table_usa"] >= 1

    # Check Loyalty 360 mart
    assert len(result.loyalty_360_data) > 0
    elena = next(r for r in result.loyalty_360_data if r["member_id"] == "223457")
    assert elena["member_name"] == "Elena"
    assert elena["total_redemptions"] == 2
    assert elena["total_miles_redeemed"] == 17000
    assert elena["top_partner"] in ("AeroLink", "SkyPoints")
