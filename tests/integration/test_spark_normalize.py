from pathlib import Path
import shutil

import pytest

pytest.importorskip("pyspark")

from tfl_cycle_analytics.config import Settings
from tfl_cycle_analytics.processing import normalize_all_raw_journeys


def test_normalize_all_raw_journeys_writes_partitioned_parquet(tmp_path):
    raw_dir = tmp_path / "data" / "raw" / "journeys"
    raw_dir.mkdir(parents=True)
    shutil.copy(Path("tests/fixtures/raw/2024-01-sample.csv"), raw_dir / "JourneyDataExtract_2024-01.csv")

    settings = Settings(
        deploy_target="local",
        lake_root=tmp_path / "data",
        duckdb_path=tmp_path / "warehouse" / "test.duckdb",
        project_root=tmp_path,
        gcp_project_id="",
        gcs_bucket="",
        bq_dataset="tfl_cycle_analytics",
        bq_location="asia-southeast2",
    )
    settings.raw_journeys_dir.mkdir(parents=True, exist_ok=True)
    settings.silver_journeys_dir.mkdir(parents=True, exist_ok=True)
    settings.temp_dir.mkdir(parents=True, exist_ok=True)

    result = normalize_all_raw_journeys(settings)
    parquet_files = list(settings.silver_journeys_dir.rglob("*.parquet"))

    assert result["raw_file_count"] == 1
    assert parquet_files
