from pathlib import Path

import duckdb
import pandas as pd

from dashboard.streamlit_app import build_daily_ridership, build_station_popularity, filter_fact_trips, load_fact_trips


def test_dashboard_helpers_load_and_aggregate(tmp_path):
    db_path = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("create schema analytics")
    connection.register(
        "fact_frame",
        pd.DataFrame(
            [
                {
                    "started_date": "2024-01-01",
                    "started_at": "2024-01-01 08:00:00",
                    "hour_of_day": 8,
                    "duration_seconds": 600,
                    "duration_band": "0-15 min",
                    "is_weekend": False,
                    "start_station_name_canonical": "Brunswick Square",
                    "start_station_name": "Brunswick Square",
                },
                {
                    "started_date": "2024-01-01",
                    "started_at": "2024-01-01 09:00:00",
                    "hour_of_day": 9,
                    "duration_seconds": 1600,
                    "duration_band": "15-30 min",
                    "is_weekend": False,
                    "start_station_name_canonical": "Embankment (Savoy)",
                    "start_station_name": "Embankment (Savoy)",
                },
            ]
        ),
    )
    connection.execute("create table analytics.fct_trips as select * from fact_frame")
    connection.close()

    frame = load_fact_trips(Path(db_path))
    filtered = filter_fact_trips(frame, pd.Timestamp("2024-01-01").date(), pd.Timestamp("2024-01-01").date(), "All trips", ["0-15 min", "15-30 min"])
    daily = build_daily_ridership(filtered)
    stations = build_station_popularity(filtered)

    assert len(frame) == 2
    assert int(daily["trip_count"].sum()) == 2
    assert stations.iloc[0]["trip_count"] == 1
