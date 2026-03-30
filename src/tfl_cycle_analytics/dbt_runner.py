from __future__ import annotations

import os

from tfl_cycle_analytics.config import Settings
from tfl_cycle_analytics.utils import run_subprocess


def dbt_environment(settings: Settings) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "DBT_TARGET": "local" if settings.deploy_target == "local" else "gcp",
            "DBT_PROFILES_DIR": str(settings.dbt_project_dir),
            "DUCKDB_PATH": str(settings.duckdb_path),
            "DBT_SCHEMA": settings.dbt_schema,
            "DBT_RAW_SCHEMA": settings.dbt_raw_schema if settings.deploy_target == "local" else settings.bq_dataset,
            "GCP_PROJECT_ID": settings.gcp_project_id,
            "BQ_DATASET": settings.bq_dataset,
            "BQ_LOCATION": settings.bq_location,
            "GOOGLE_APPLICATION_CREDENTIALS": settings.google_application_credentials,
        }
    )
    return env


def run_dbt(settings: Settings, args: list[str]) -> None:
    run_subprocess(["dbt", *args], cwd=settings.dbt_project_dir, extra_env=dbt_environment(settings))


def run_staging_models(settings: Settings) -> None:
    run_dbt(settings, ["run", "--select", "stg_journeys", "stg_stations"])


def run_mart_models(settings: Settings) -> None:
    run_dbt(
        settings,
        [
            "run",
            "--select",
            "dim_station",
            "fct_trips",
            "mart_daily_ridership",
            "mart_station_popularity",
            "mart_duration_distribution",
        ],
    )


def run_data_quality_checks(settings: Settings) -> None:
    run_dbt(
        settings,
        [
            "test",
            "--select",
            "source:raw.*",
            "stg_journeys",
            "stg_stations",
            "dim_station",
            "fct_trips",
            "mart_daily_ridership",
            "mart_station_popularity",
            "mart_duration_distribution",
        ],
    )
