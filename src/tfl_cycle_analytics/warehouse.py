from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from tfl_cycle_analytics.config import Settings
from tfl_cycle_analytics.utils import read_json


def latest_station_snapshot(settings: Settings) -> Path:
    snapshots = sorted(settings.raw_stations_dir.glob("bikepoint_snapshot_*.json"))
    if not snapshots:
        raise RuntimeError("No station snapshot found. Run station ingestion first.")
    return snapshots[-1]


def flatten_station_snapshot(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for item in payload:
        properties = {prop.get("key"): prop.get("value") for prop in item.get("additionalProperties", [])}
        terminal_name = properties.get("TerminalName")
        station_id = pd.to_numeric(terminal_name, errors="coerce")
        if pd.isna(station_id) and item.get("id"):
            station_id = pd.to_numeric(str(item.get("id", "")).replace("BikePoints_", ""), errors="coerce")

        flattened.append(
            {
                "station_id": station_id,
                "station_key": item.get("id"),
                "station_name": item.get("commonName"),
                "place_type": item.get("placeType"),
                "lat": item.get("lat"),
                "lon": item.get("lon"),
                "terminal_name": terminal_name,
                "nb_bikes": pd.to_numeric(properties.get("NbBikes"), errors="coerce"),
                "nb_empty_docks": pd.to_numeric(properties.get("NbEmptyDocks"), errors="coerce"),
                "nb_docks": pd.to_numeric(properties.get("NbDocks"), errors="coerce"),
                "install_date": properties.get("InstallDate"),
                "locked": properties.get("Locked"),
                "snapshot_ts": properties.get("Modified"),
            }
        )
    return flattened


def load_duckdb_raw_tables(settings: Settings) -> dict[str, Any]:
    parquet_files = list(settings.silver_journeys_dir.rglob("*.parquet"))
    if not parquet_files:
        raise RuntimeError("No silver parquet files found. Run Spark normalization first.")

    station_rows = flatten_station_snapshot(read_json(latest_station_snapshot(settings)))
    if not station_rows:
        raise RuntimeError("Station snapshot is empty.")

    connection = duckdb.connect(str(settings.duckdb_path))
    try:
        connection.execute("create schema if not exists raw")
        connection.execute("create schema if not exists analytics")
        connection.execute(
            "create or replace table raw.journeys as select * from read_parquet(?)",
            [str(settings.silver_journeys_dir / "*" / "*" / "*.parquet")],
        )
        station_frame = pd.DataFrame(station_rows)
        connection.register("station_frame", station_frame)
        connection.execute("create or replace table raw.stations_latest as select * from station_frame")
        connection.unregister("station_frame")
        journey_count = connection.execute("select count(*) from raw.journeys").fetchone()[0]
        station_count = connection.execute("select count(*) from raw.stations_latest").fetchone()[0]
    finally:
        connection.close()

    return {
        "duckdb_path": str(settings.duckdb_path),
        "journey_count": journey_count,
        "station_count": station_count,
    }
