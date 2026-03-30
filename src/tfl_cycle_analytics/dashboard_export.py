from __future__ import annotations

import duckdb

from tfl_cycle_analytics.config import Settings


DASHBOARD_TABLES = [
    "mart_daily_ridership",
    "mart_station_popularity",
    "mart_duration_distribution",
    "fct_trips",
    "dim_station",
]


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
        raise RuntimeError(f"Dashboard export could not find table '{table}' in DuckDB.")
    return result[0]


def export_dashboard_assets(settings: Settings) -> dict[str, list[str]]:
    if settings.deploy_target != "local":
        return {"exported_files": []}

    exported: list[str] = []
    connection = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        settings.gold_dir.mkdir(parents=True, exist_ok=True)
        for table in DASHBOARD_TABLES:
            output = settings.gold_dir / f"{table}.parquet"
            schema_name = resolve_table_schema(connection, settings.dbt_schema, table)
            connection.execute(
                f"COPY (SELECT * FROM {schema_name}.{table}) TO '{output.as_posix()}' (FORMAT PARQUET)"
            )
            exported.append(str(output))
    finally:
        connection.close()
    return {"exported_files": exported}
