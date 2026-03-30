from __future__ import annotations

import argparse
from datetime import date
import json

from tfl_cycle_analytics.config import Settings
from tfl_cycle_analytics.dashboard_export import export_dashboard_assets
from tfl_cycle_analytics.dbt_runner import run_data_quality_checks, run_mart_models, run_staging_models
from tfl_cycle_analytics.processing import normalize_all_raw_journeys
from tfl_cycle_analytics.sources import download_journey_files, download_station_snapshot
from tfl_cycle_analytics.warehouse import load_duckdb_raw_tables


def _iso_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TfL cycle analytics pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download_journeys = subparsers.add_parser("download-journeys", help="Download monthly TfL journey files")
    download_journeys.add_argument("--start-date", type=_iso_date)
    download_journeys.add_argument("--end-date", type=_iso_date)

    subparsers.add_parser("download-stations", help="Download the latest BikePoint station snapshot")
    subparsers.add_parser("normalize-silver", help="Normalize raw journey files into partitioned parquet")
    subparsers.add_parser("load-warehouse", help="Load silver parquet and stations into DuckDB")
    subparsers.add_parser("run-dbt-staging", help="Build dbt staging models")
    subparsers.add_parser("run-dbt-marts", help="Build dbt marts")
    subparsers.add_parser("test-dbt", help="Run dbt data quality checks")
    subparsers.add_parser("export-dashboard", help="Export dashboard parquet assets")

    sync_incremental = subparsers.add_parser("sync-incremental", help="Run the full local pipeline")
    sync_incremental.add_argument("--start-date", type=_iso_date)
    sync_incremental.add_argument("--end-date", type=_iso_date)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.command == "download-journeys":
        result = download_journey_files(settings, args.start_date or settings.backfill_start, args.end_date or settings.backfill_end)
    elif args.command == "download-stations":
        result = download_station_snapshot(settings)
    elif args.command == "normalize-silver":
        result = normalize_all_raw_journeys(settings)
    elif args.command == "load-warehouse":
        result = load_duckdb_raw_tables(settings)
    elif args.command == "run-dbt-staging":
        run_staging_models(settings)
        result = {"status": "staging models completed"}
    elif args.command == "run-dbt-marts":
        run_mart_models(settings)
        result = {"status": "mart models completed"}
    elif args.command == "test-dbt":
        run_data_quality_checks(settings)
        result = {"status": "dbt tests completed"}
    elif args.command == "export-dashboard":
        result = export_dashboard_assets(settings)
    elif args.command == "sync-incremental":
        start_date = args.start_date or settings.backfill_start
        end_date = args.end_date or settings.backfill_end
        result = {
            "journeys": download_journey_files(settings, start_date, end_date),
            "stations": download_station_snapshot(settings),
            "silver": normalize_all_raw_journeys(settings),
        }
        result["warehouse"] = load_duckdb_raw_tables(settings)
        run_staging_models(settings)
        run_mart_models(settings)
        run_data_quality_checks(settings)
        result["dashboard"] = export_dashboard_assets(settings)
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

