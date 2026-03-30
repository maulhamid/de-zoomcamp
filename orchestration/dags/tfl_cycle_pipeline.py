from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.edgemodifier import Label
import pendulum

from tfl_cycle_analytics.config import Settings
from tfl_cycle_analytics.dashboard_export import export_dashboard_assets
from tfl_cycle_analytics.dbt_runner import run_data_quality_checks, run_mart_models, run_staging_models
from tfl_cycle_analytics.processing import normalize_all_raw_journeys
from tfl_cycle_analytics.sources import (
    discover_selected_journey_files,
    download_selected_journey_files,
    download_station_snapshot,
)
from tfl_cycle_analytics.warehouse import load_duckdb_raw_tables


LOCAL_TIMEZONE = pendulum.timezone("Asia/Jakarta")
DEFAULT_ARGS = {
    "owner": "codex",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _settings() -> Settings:
    return Settings.from_env()


@dag(
    dag_id="tfl_cycle_backfill",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TIMEZONE),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"start_date": "2024-01-01", "end_date": "2026-02-28"},
    tags=["tfl", "batch", "backfill"],
)
def tfl_cycle_backfill():
    @task(task_id="discover_source_files")
    def discover_sources() -> list[dict[str, str]]:
        context = get_current_context()
        params = context["params"]
        settings = _settings()
        return discover_selected_journey_files(
            settings,
            start_date=pendulum.parse(params["start_date"]).date(),
            end_date=pendulum.parse(params["end_date"]).date(),
        )

    @task(task_id="download_raw_journeys")
    def download_raw_journeys(selected_files: list[dict[str, str]]) -> dict[str, str | int]:
        context = get_current_context()
        params = context["params"]
        settings = _settings()
        return download_selected_journey_files(
            settings,
            selected_files,
            start_date=pendulum.parse(params["start_date"]).date(),
            end_date=pendulum.parse(params["end_date"]).date(),
        )

    @task(task_id="download_station_snapshot")
    def ingest_station_snapshot():
        return download_station_snapshot(_settings())

    @task(task_id="normalize_to_parquet")
    def normalize(download_manifest: dict[str, str | int | list[str]]):
        return normalize_all_raw_journeys(_settings(), requested_files=download_manifest.get("files"))

    @task(task_id="load_duckdb_raw")
    def load_warehouse():
        return load_duckdb_raw_tables(_settings())

    @task(task_id="run_dbt_staging")
    def dbt_staging():
        run_staging_models(_settings())

    @task(task_id="run_dbt_marts")
    def dbt_marts():
        run_mart_models(_settings())

    @task(task_id="export_dashboard_dataset")
    def export_dashboard():
        return export_dashboard_assets(_settings())

    @task(task_id="data_quality_checks")
    def quality_checks():
        run_data_quality_checks(_settings())

    discovered = discover_sources()
    journeys = download_raw_journeys(discovered)
    stations = ingest_station_snapshot()
    normalized = normalize(journeys)
    loaded = load_warehouse()
    staging = dbt_staging()
    marts = dbt_marts()
    dashboard = export_dashboard()
    checks = quality_checks()

    [journeys, stations] >> Label("raw lake ready") >> normalized
    normalized >> loaded >> staging >> marts >> dashboard >> checks


@dag(
    dag_id="tfl_cycle_incremental",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TIMEZONE),
    schedule="0 9 * * 6",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tfl", "batch", "incremental"],
)
def tfl_cycle_incremental():
    @task(task_id="discover_source_files")
    def discover_sources() -> list[dict[str, str]]:
        settings = _settings()
        return discover_selected_journey_files(settings, start_date=settings.backfill_start, end_date=settings.backfill_end)

    @task(task_id="download_raw_journeys")
    def download_raw_journeys(selected_files: list[dict[str, str]]) -> dict[str, str | int]:
        settings = _settings()
        return download_selected_journey_files(
            settings,
            selected_files,
            start_date=settings.backfill_start,
            end_date=settings.backfill_end,
        )

    @task(task_id="download_station_snapshot")
    def ingest_station_snapshot():
        return download_station_snapshot(_settings())

    @task(task_id="normalize_to_parquet")
    def normalize(download_manifest: dict[str, str | int | list[str]]):
        return normalize_all_raw_journeys(_settings(), requested_files=download_manifest.get("files"))

    @task(task_id="load_duckdb_raw")
    def load_warehouse():
        return load_duckdb_raw_tables(_settings())

    @task(task_id="run_dbt_staging")
    def dbt_staging():
        run_staging_models(_settings())

    @task(task_id="run_dbt_marts")
    def dbt_marts():
        run_mart_models(_settings())

    @task(task_id="export_dashboard_dataset")
    def export_dashboard():
        return export_dashboard_assets(_settings())

    @task(task_id="data_quality_checks")
    def quality_checks():
        run_data_quality_checks(_settings())

    discovered = discover_sources()
    journeys = download_raw_journeys(discovered)
    stations = ingest_station_snapshot()
    normalized = normalize(journeys)
    loaded = load_warehouse()
    staging = dbt_staging()
    marts = dbt_marts()
    dashboard = export_dashboard()
    checks = quality_checks()

    [journeys, stations] >> normalized >> loaded >> staging >> marts >> dashboard >> checks


tfl_cycle_backfill()
tfl_cycle_incremental()
