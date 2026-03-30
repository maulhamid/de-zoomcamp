from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from tfl_cycle_analytics.config import Settings


def resolve_table_schema(connection: duckdb.DuckDBPyConnection, preferred_schema: str, table: str) -> str:
    result = connection.execute(
        """
        select table_schema
        from information_schema.tables
        where table_name = ?
        order by case when table_schema = ? then 0 else 1 end, table_schema
        limit 1
        """,
        [table, preferred_schema],
    ).fetchone()
    if result is None:
        raise RuntimeError(f"Dashboard loader could not find table '{table}' in DuckDB.")
    return result[0]


def load_fact_trips(duckdb_path: Path, schema: str = "analytics") -> pd.DataFrame:
    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        resolved_schema = resolve_table_schema(connection, schema, "fct_trips")
        return connection.execute(
            f"""
            select
                started_date,
                started_at,
                hour_of_day,
                duration_seconds,
                duration_band,
                is_weekend,
                coalesce(start_station_name_canonical, start_station_name) as start_station_name
            from {resolved_schema}.fct_trips
            """
        ).df()
    finally:
        connection.close()


def filter_fact_trips(
    frame: pd.DataFrame,
    start_date: date,
    end_date: date,
    weekend_mode: str,
    duration_bands: list[str],
) -> pd.DataFrame:
    filtered = frame.copy()
    filtered["started_date"] = pd.to_datetime(filtered["started_date"]).dt.date
    filtered = filtered[(filtered["started_date"] >= start_date) & (filtered["started_date"] <= end_date)]
    if weekend_mode == "Weekday only":
        filtered = filtered[filtered["is_weekend"] == False]  # noqa: E712
    elif weekend_mode == "Weekend only":
        filtered = filtered[filtered["is_weekend"] == True]  # noqa: E712
    if duration_bands:
        filtered = filtered[filtered["duration_band"].isin(duration_bands)]
    return filtered


def build_daily_ridership(filtered: pd.DataFrame) -> pd.DataFrame:
    return (
        filtered.groupby("started_date", as_index=False)
        .size()
        .rename(columns={"size": "trip_count"})
        .sort_values("started_date")
    )


def build_station_popularity(filtered: pd.DataFrame) -> pd.DataFrame:
    ranked = filtered.copy()
    ranked["start_station_name"] = ranked["start_station_name"].astype("string").str.strip()
    ranked = ranked[ranked["start_station_name"].notna() & (ranked["start_station_name"] != "")]
    return (
        ranked.groupby("start_station_name", as_index=False)
        .size()
        .rename(columns={"size": "trip_count"})
        .sort_values(["trip_count", "start_station_name"], ascending=[False, True])
        .head(10)
    )


def render_dashboard() -> None:
    settings = Settings.from_env()
    st.set_page_config(page_title="TfL Cycle Analytics", page_icon="🚲", layout="wide")
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(213, 246, 255, 0.7), transparent 28%),
                linear-gradient(135deg, #f8fafc 0%, #eef6ff 48%, #fdf6ec 100%);
            color: #0f172a;
        }
        .kpi-card {
            padding: 1rem 1.2rem;
            border-radius: 16px;
            background: rgba(255, 255, 255, 0.78);
            border: 1px solid rgba(15, 23, 42, 0.08);
            box-shadow: 0 12px 40px rgba(15, 23, 42, 0.06);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("TfL Cycle Demand Analytics")
    st.caption("Batch analytics on Santander Cycle Hire journeys using Airflow, Spark, DuckDB, and dbt.")

    if not settings.duckdb_path.exists():
        st.warning("The DuckDB warehouse does not exist yet. Run the Airflow DAG or the CLI pipeline first.")
        st.stop()

    fact_trips = load_fact_trips(settings.duckdb_path, settings.dbt_schema)
    if fact_trips.empty:
        st.warning("No trips are available in the warehouse yet.")
        st.stop()

    min_date = pd.to_datetime(fact_trips["started_date"]).dt.date.min()
    max_date = pd.to_datetime(fact_trips["started_date"]).dt.date.max()

    with st.sidebar:
        st.header("Filters")
        selected_range = st.date_input("Trip date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        if isinstance(selected_range, tuple):
            start_date, end_date = selected_range
        else:
            start_date, end_date = min_date, max_date
        weekend_mode = st.radio("Ride type", ["All trips", "Weekday only", "Weekend only"], index=0)
        available_bands = sorted(fact_trips["duration_band"].dropna().unique().tolist())
        duration_bands = st.multiselect("Duration band", options=available_bands, default=available_bands)

    filtered = filter_fact_trips(fact_trips, start_date, end_date, weekend_mode, duration_bands)
    if filtered.empty:
        st.warning("No trips match the selected filters.")
        st.stop()

    total_trips = int(len(filtered))
    avg_duration_min = round(filtered["duration_seconds"].fillna(0).mean() / 60, 1)
    busiest_hour = int(filtered.groupby("hour_of_day").size().sort_values(ascending=False).index[0])

    kpi_cols = st.columns(3)
    kpi_values = [
        ("Trips in scope", f"{total_trips:,}"),
        ("Average duration", f"{avg_duration_min} min"),
        ("Busiest hour", f"{busiest_hour:02d}:00"),
    ]
    for column, (title, value) in zip(kpi_cols, kpi_values):
        with column:
            st.markdown(f"<div class='kpi-card'><small>{title}</small><h2>{value}</h2></div>", unsafe_allow_html=True)

    daily_ridership = build_daily_ridership(filtered)
    station_popularity = build_station_popularity(filtered)

    chart_cols = st.columns([1.35, 1])
    with chart_cols[0]:
        st.subheader("Daily trip volume")
        st.vega_lite_chart(
            daily_ridership,
            {
                "mark": {"type": "line", "point": True, "color": "#0f766e", "strokeWidth": 3},
                "encoding": {
                    "x": {"field": "started_date", "type": "temporal", "title": "Date"},
                    "y": {"field": "trip_count", "type": "quantitative", "title": "Trips"},
                    "tooltip": [
                        {"field": "started_date", "type": "temporal", "title": "Date"},
                        {"field": "trip_count", "type": "quantitative", "title": "Trips"},
                    ],
                },
                "height": 360,
            },
            use_container_width=True,
        )

    with chart_cols[1]:
        st.subheader("Top departure stations")
        st.vega_lite_chart(
            station_popularity,
            {
                "mark": {"type": "bar", "cornerRadiusEnd": 6, "color": "#f97316"},
                "encoding": {
                    "y": {
                        "field": "start_station_name",
                        "type": "nominal",
                        "sort": "-x",
                        "title": "Station",
                    },
                    "x": {"field": "trip_count", "type": "quantitative", "title": "Trips"},
                    "tooltip": [
                        {"field": "start_station_name", "type": "nominal", "title": "Station"},
                        {"field": "trip_count", "type": "quantitative", "title": "Trips"},
                    ],
                },
                "height": 360,
            },
            use_container_width=True,
        )

    st.subheader("Duration mix")
    duration_mix = (
        filtered.groupby("duration_band", as_index=False)
        .size()
        .rename(columns={"size": "trip_count"})
        .sort_values("trip_count", ascending=False)
    )
    st.vega_lite_chart(
        duration_mix,
        {
            "mark": {"type": "arc", "innerRadius": 60},
            "encoding": {
                "theta": {"field": "trip_count", "type": "quantitative"},
                "color": {
                    "field": "duration_band",
                    "type": "nominal",
                    "scale": {
                        "range": ["#0f766e", "#14b8a6", "#f59e0b", "#f97316", "#cbd5e1"],
                    },
                },
                "tooltip": [
                    {"field": "duration_band", "type": "nominal", "title": "Duration band"},
                    {"field": "trip_count", "type": "quantitative", "title": "Trips"},
                ],
            },
            "height": 320,
        },
        use_container_width=True,
    )


if __name__ == "__main__":
    render_dashboard()
