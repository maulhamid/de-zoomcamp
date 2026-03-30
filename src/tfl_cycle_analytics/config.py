from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
import os


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_date(key: str, fallback: str) -> date:
    return date.fromisoformat(os.getenv(key, fallback))


@dataclass(slots=True)
class Settings:
    deploy_target: str = field(default_factory=lambda: os.getenv("DEPLOY_TARGET", "local"))
    project_root: Path = field(default_factory=_root_dir)
    lake_root: Path = field(default_factory=lambda: Path(os.getenv("LAKE_ROOT", str(_root_dir() / "data"))))
    duckdb_path: Path = field(
        default_factory=lambda: Path(os.getenv("DUCKDB_PATH", str(_root_dir() / "warehouse" / "tfl_cycle_analytics.duckdb")))
    )
    gcp_project_id: str = field(default_factory=lambda: os.getenv("GCP_PROJECT_ID", ""))
    gcs_bucket: str = field(default_factory=lambda: os.getenv("GCS_BUCKET", ""))
    bq_dataset: str = field(default_factory=lambda: os.getenv("BQ_DATASET", "tfl_cycle_analytics"))
    bq_location: str = field(default_factory=lambda: os.getenv("BQ_LOCATION", "asia-southeast2"))
    backfill_start: date = field(default_factory=lambda: _env_date("BACKFILL_START", "2024-01-01"))
    backfill_end: date = field(default_factory=lambda: _env_date("BACKFILL_END", "2026-02-28"))
    normalization_engine: str = field(
        default_factory=lambda: os.getenv(
            "NORMALIZATION_ENGINE",
            "pandas" if os.getenv("DEPLOY_TARGET", "local") == "local" else "spark",
        )
    )
    dbt_schema: str = field(default_factory=lambda: os.getenv("DBT_SCHEMA", "analytics"))
    dbt_raw_schema: str = field(default_factory=lambda: os.getenv("DBT_RAW_SCHEMA", "raw"))
    google_application_credentials: str = field(default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))

    def __post_init__(self) -> None:
        if not self.project_root.is_absolute():
            self.project_root = self.project_root.resolve()
        if not self.lake_root.is_absolute():
            self.lake_root = (self.project_root / self.lake_root).resolve()
        if not self.duckdb_path.is_absolute():
            self.duckdb_path = (self.project_root / self.duckdb_path).resolve()

    @property
    def raw_journeys_dir(self) -> Path:
        return self.lake_root / "raw" / "journeys"

    @property
    def raw_stations_dir(self) -> Path:
        return self.lake_root / "raw" / "stations"

    @property
    def silver_journeys_dir(self) -> Path:
        return self.lake_root / "silver" / "journeys"

    @property
    def gold_dir(self) -> Path:
        return self.lake_root / "gold"

    @property
    def manifests_dir(self) -> Path:
        return self.project_root / ".tmp" / "manifests"

    @property
    def temp_dir(self) -> Path:
        return self.project_root / ".tmp"

    @property
    def dbt_project_dir(self) -> Path:
        return self.project_root / "dbt" / "tfl_cycle_analytics"

    @property
    def airflow_dags_dir(self) -> Path:
        return self.project_root / "orchestration" / "dags"

    @property
    def station_api_url(self) -> str:
        return "https://api.tfl.gov.uk/BikePoint"

    @property
    def journey_base_url(self) -> str:
        return "https://cycling.data.tfl.gov.uk/"

    @classmethod
    def from_env(cls) -> "Settings":
        settings = cls()
        settings.project_root.mkdir(parents=True, exist_ok=True)
        settings.raw_journeys_dir.mkdir(parents=True, exist_ok=True)
        settings.raw_stations_dir.mkdir(parents=True, exist_ok=True)
        settings.silver_journeys_dir.mkdir(parents=True, exist_ok=True)
        settings.gold_dir.mkdir(parents=True, exist_ok=True)
        settings.manifests_dir.mkdir(parents=True, exist_ok=True)
        settings.temp_dir.mkdir(parents=True, exist_ok=True)
        settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        return settings
