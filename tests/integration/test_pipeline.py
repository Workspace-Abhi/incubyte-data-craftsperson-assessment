"""End-to-end integration tests for SkyPoints pipeline execution."""
import pytest
from pathlib import Path
from datetime import date
from src.pipeline import SkyPointsPipeline


def test_full_pipeline_with_sample_files():
    root_dir = Path(__file__).resolve().parent.parent.parent
    data_dir = root_dir / "data"
    flat_file = str(data_dir / "samples" / "sample_member_profile.dat")
    json_file = str(data_dir / "samples" / "sample_redemptions.json")
    csv_mappings = {
        str(data_dir / "derived" / "IND.csv"): "IND",
        str(data_dir / "derived" / "USA.csv"): "USA",
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

    # Check Loyalty 360 mart: COMPLETED miles vs PENDING miles
    assert len(result.loyalty_360_data) > 0
    elena = next(r for r in result.loyalty_360_data if r["member_id"] == "223457")
    assert elena["member_name"] == "Elena"
    assert elena["total_redemptions"] == 2
    assert elena["total_miles_redeemed"] == 12000  # Only COMPLETED miles!
    assert elena["pending_miles_redeemed"] == 5000  # PENDING miles tracked separately!
    assert elena["top_partner"] in ("AeroLink", "SkyPoints")
    assert isinstance(result.orphan_redemptions, list)


def test_pipeline_orphan_redemptions_detection(tmp_path):
    import json
    pipeline = SkyPointsPipeline()

    # Create dummy redemption JSON with an orphan member_id
    orphan_payload = {
        "feed_date": "20240115",
        "member_id": "ORPHAN_MEMBER_999",
        "redemptions": [
            {
                "txn_id": "ORPHAN_TXN_01",
                "txn_date": "20240110",
                "partner": "SkyPoints",
                "miles_redeemed": 500,
                "status": "COMPLETED",
            }
        ],
    }
    json_file = tmp_path / "orphan_feed.json"
    json_file.write_text(json.dumps(orphan_payload), encoding="utf-8")

    result = pipeline.run(json_file_path=str(json_file))
    assert len(result.orphan_redemptions) == 1
    assert result.orphan_redemptions[0]["member_id"] == "ORPHAN_MEMBER_999"
    assert result.orphan_redemptions[0]["txn_id"] == "ORPHAN_TXN_01"


def test_pipeline_with_raw_company_files():
    root_dir = Path(__file__).resolve().parent.parent.parent
    data_dir = root_dir / "data"
    raw_dir = data_dir / "raw"
    samples_dir = data_dir / "samples"

    flat_file = str(samples_dir / "sample_member_profile.dat")
    json_file = str(samples_dir / "sample_redemptions.json")
    xlsx_mappings = {
        str(raw_dir / "AUS.xlsx"): "AUS",
    }
    csv_mappings = {
        str(raw_dir / "IND.csv"): "IND",
        str(raw_dir / "USA.csv"): "USA",
    }
    date_hints = {
        str(raw_dir / "USA.csv"): "USA",
    }

    pipeline = SkyPointsPipeline()
    ref_date = date(2024, 1, 15)
    result = pipeline.run(
        flat_file_path=flat_file,
        json_file_path=json_file,
        xlsx_file_mappings=xlsx_mappings,
        csv_file_mappings=csv_mappings,
        csv_date_format_hints=date_hints,
        reference_date=ref_date,
    )

    metrics = result.metrics
    assert metrics.total_raw_records > 0
    assert metrics.valid_records_count > 0
    assert "table_india" in metrics.country_table_counts
    assert "table_usa" in metrics.country_table_counts
    assert "table_australia" in metrics.country_table_counts
