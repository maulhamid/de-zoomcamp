from __future__ import annotations

from functools import reduce
import logging
import os
from pathlib import Path
import shutil
import zipfile

import pandas as pd

from tfl_cycle_analytics.config import Settings
from tfl_cycle_analytics.utils import canonicalize_column_name, ensure_directory, sha1_hexdigest


EXPECTED_COLUMN_ALIASES = {
    "rental_id": {"rentalid"},
    "duration": {"duration", "durationseconds"},
    "bike_id": {"bikeid"},
    "ended_at": {"enddate", "endtime"},
    "end_station_id": {"endstationid", "endstationnumber"},
    "end_station_name": {"endstationname"},
    "started_at": {"startdate", "starttime"},
    "start_station_id": {"startstationid", "startstationnumber"},
    "start_station_name": {"startstationname"},
}
TIMESTAMP_FORMATS = [
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
]
LOGGER = logging.getLogger(__name__)


def match_canonical_columns(columns: list[str]) -> dict[str, str]:
    matched: dict[str, str] = {}
    normalized_lookup = {canonicalize_column_name(column): column for column in columns}
    for target_name, aliases in EXPECTED_COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in normalized_lookup:
                matched[target_name] = normalized_lookup[alias]
                break
    return matched


def local_raw_journey_files(settings: Settings) -> list[Path]:
    files = [path for path in settings.raw_journeys_dir.glob("*") if path.is_file() and path.suffix.lower() in {".csv", ".zip", ".gz"}]
    return sorted(files)


def expand_raw_inputs(raw_file: Path, extraction_root: Path) -> list[Path]:
    if raw_file.suffix.lower() != ".zip":
        return [raw_file]

    extract_dir = ensure_directory(extraction_root / raw_file.stem)
    csv_outputs = list(extract_dir.glob("*.csv"))
    if csv_outputs:
        return sorted(csv_outputs)

    with zipfile.ZipFile(raw_file) as archive:
        archive.extractall(extract_dir)
    return sorted(path for path in extract_dir.rglob("*.csv"))


def _build_spark_session():
    _ensure_java_runtime()

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("tfl-cycle-demand-analytics")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )


def _ensure_java_runtime() -> None:
    java_home = os.environ.get("JAVA_HOME")
    java_bin = Path(java_home) / "bin" / "java" if java_home else None
    if java_bin and java_bin.exists():
        return

    resolved_java = shutil.which("java")
    if resolved_java is None:
        raise RuntimeError("Java runtime is not installed or not available on PATH.")

    resolved_home = Path(resolved_java).resolve().parent.parent
    os.environ["JAVA_HOME"] = str(resolved_home)
    os.environ["PATH"] = f"{resolved_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"


def _standardize_dataframe(dataframe):
    from pyspark.sql import functions as F

    matched_columns = match_canonical_columns(dataframe.columns)
    selections = []
    for canonical_name in EXPECTED_COLUMN_ALIASES:
        if canonical_name in matched_columns:
            selections.append(F.col(matched_columns[canonical_name]).alias(canonical_name))
        else:
            selections.append(F.lit(None).alias(canonical_name))

    base = dataframe.select(*selections)
    timestamp_formats = [
        "dd/MM/yyyy HH:mm",
        "dd/MM/yyyy HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm",
        "MM/dd/yyyy HH:mm",
        "M/d/yyyy H:mm",
    ]

    def parse_timestamp(column_name: str):
        return F.coalesce(*[F.to_timestamp(F.col(column_name), fmt) for fmt in timestamp_formats])

    started_at = parse_timestamp("started_at")
    ended_at = parse_timestamp("ended_at")
    duration_from_source = F.regexp_extract(F.col("duration").cast("string"), r"(\d+)", 1).cast("int")
    computed_duration = (F.unix_timestamp(ended_at) - F.unix_timestamp(started_at)).cast("int")
    duration_seconds = F.coalesce(duration_from_source, computed_duration)

    normalized = (
        base.withColumn("started_at", started_at)
        .withColumn("ended_at", ended_at)
        .withColumn("start_station_id", F.regexp_extract(F.col("start_station_id").cast("string"), r"(\d+)", 1).cast("int"))
        .withColumn("end_station_id", F.regexp_extract(F.col("end_station_id").cast("string"), r"(\d+)", 1).cast("int"))
        .withColumn("duration_seconds", duration_seconds)
        .withColumn(
            "trip_id",
            F.coalesce(
                F.col("rental_id").cast("string"),
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("bike_id").cast("string"), F.lit("")),
                        F.coalesce(F.date_format(started_at, "yyyy-MM-dd HH:mm:ss"), F.lit("")),
                        F.coalesce(F.date_format(ended_at, "yyyy-MM-dd HH:mm:ss"), F.lit("")),
                        F.coalesce(F.col("start_station_id").cast("string"), F.lit("")),
                        F.coalesce(F.col("end_station_id").cast("string"), F.lit("")),
                    ),
                    256,
                ),
            ),
        )
        .filter(F.col("started_at").isNotNull() & F.col("ended_at").isNotNull())
        .withColumn("started_date", F.to_date("started_at"))
        .withColumn("ended_date", F.to_date("ended_at"))
        .withColumn("year", F.year("started_at"))
        .withColumn("month", F.month("started_at"))
        .withColumn("hour_of_day", F.hour("started_at"))
        .withColumn("day_of_week", F.date_format("started_at", "E"))
        .withColumn("is_weekend", F.dayofweek("started_at").isin([1, 7]))
        .withColumn(
            "duration_band",
            F.when(F.col("duration_seconds").isNull(), F.lit("unknown"))
            .when(F.col("duration_seconds") < 15 * 60, F.lit("0-15 min"))
            .when(F.col("duration_seconds") < 30 * 60, F.lit("15-30 min"))
            .when(F.col("duration_seconds") < 60 * 60, F.lit("30-60 min"))
            .otherwise(F.lit("60+ min")),
        )
        .dropDuplicates(["trip_id"])
    )
    return normalized


def _parse_timestamp_series(series: pd.Series) -> pd.Series:
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    text_series = series.astype("string")
    for timestamp_format in TIMESTAMP_FORMATS:
        parsed = parsed.fillna(pd.to_datetime(text_series, format=timestamp_format, errors="coerce"))
    return parsed.fillna(pd.to_datetime(text_series, errors="coerce", dayfirst=True))


def _extract_numeric_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.extract(r"(\d+)")[0]


def _standardize_pandas_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    matched_columns = match_canonical_columns(dataframe.columns.tolist())
    normalized: dict[str, pd.Series] = {}
    for canonical_name in EXPECTED_COLUMN_ALIASES:
        if canonical_name in matched_columns:
            normalized[canonical_name] = dataframe[matched_columns[canonical_name]]
        else:
            normalized[canonical_name] = pd.Series([pd.NA] * len(dataframe), index=dataframe.index, dtype="object")

    base = pd.DataFrame(normalized)
    base["started_at"] = _parse_timestamp_series(base["started_at"])
    base["ended_at"] = _parse_timestamp_series(base["ended_at"])
    base["start_station_id"] = pd.to_numeric(_extract_numeric_series(base["start_station_id"]), errors="coerce").astype("Int64")
    base["end_station_id"] = pd.to_numeric(_extract_numeric_series(base["end_station_id"]), errors="coerce").astype("Int64")

    duration_from_source = pd.to_numeric(_extract_numeric_series(base["duration"]), errors="coerce")
    computed_duration = (base["ended_at"] - base["started_at"]).dt.total_seconds()
    base["duration_seconds"] = duration_from_source.fillna(computed_duration).round().astype("Int64")

    valid_rows = base["started_at"].notna() & base["ended_at"].notna()
    base = base.loc[valid_rows].copy()

    rental_id_as_string = base["rental_id"].astype("string")
    base["trip_id"] = rental_id_as_string.where(rental_id_as_string.notna())
    missing_trip_id = base["trip_id"].isna()
    if missing_trip_id.any():
        base.loc[missing_trip_id, "trip_id"] = base.loc[missing_trip_id].apply(
            lambda row: sha1_hexdigest(
                [
                    row.get("bike_id"),
                    row.get("started_at"),
                    row.get("ended_at"),
                    row.get("start_station_id"),
                    row.get("end_station_id"),
                ]
            ),
            axis=1,
        )

    base["started_date"] = base["started_at"].dt.date
    base["ended_date"] = base["ended_at"].dt.date
    base["year"] = base["started_at"].dt.year.astype("Int64")
    base["month"] = base["started_at"].dt.month.astype("Int64")
    base["hour_of_day"] = base["started_at"].dt.hour.astype("Int64")
    base["day_of_week"] = base["started_at"].dt.strftime("%a")
    base["is_weekend"] = base["started_at"].dt.dayofweek >= 5
    base["duration_band"] = (
        base["duration_seconds"]
        .map(
            lambda value: "unknown"
            if pd.isna(value)
            else "0-15 min"
            if value < 15 * 60
            else "15-30 min"
            if value < 30 * 60
            else "30-60 min"
            if value < 60 * 60
            else "60+ min"
        )
        .astype("string")
    )

    normalized_columns = [
        "trip_id",
        "rental_id",
        "bike_id",
        "started_at",
        "ended_at",
        "started_date",
        "ended_date",
        "start_station_id",
        "start_station_name",
        "end_station_id",
        "end_station_name",
        "duration_seconds",
        "duration_band",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "year",
        "month",
    ]
    return base[normalized_columns].drop_duplicates(subset=["trip_id"]).reset_index(drop=True)


def _write_partitioned_chunk(dataframe: pd.DataFrame, destination_root: Path, part_counter: int) -> int:
    if dataframe.empty:
        return part_counter

    for (year, month), partition in dataframe.groupby(["year", "month"], dropna=True):
        partition_dir = destination_root / f"year={int(year):04d}" / f"month={int(month):02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / f"part-{part_counter:05d}.parquet"
        partition.drop(columns=["year", "month"]).to_parquet(output_path, index=False)
        part_counter += 1
    return part_counter


def selected_raw_journey_files(settings: Settings, requested_files: list[str] | None = None) -> list[Path]:
    if requested_files:
        raw_files = [Path(path) for path in requested_files]
    else:
        raw_files = local_raw_journey_files(settings)

    project_root = settings.project_root.resolve()
    resolved_files: list[Path] = []
    for raw_file in raw_files:
        candidate = raw_file if raw_file.is_absolute() else (project_root / raw_file)
        if candidate.exists():
            resolved_files.append(candidate)
    return sorted(set(path.resolve() for path in resolved_files))


def _normalize_with_spark(raw_files: list[Path], settings: Settings) -> dict[str, str | int]:
    spark = _build_spark_session()
    extraction_root = ensure_directory(settings.temp_dir / "extracted")
    frames = []
    expanded_inputs: list[str] = []
    try:
        for raw_file in raw_files:
            for input_path in expand_raw_inputs(raw_file, extraction_root):
                expanded_inputs.append(str(input_path))
                frame = spark.read.option("header", True).option("escape", '"').csv(str(input_path))
                if frame.columns:
                    frames.append(_standardize_dataframe(frame))

        if not frames:
            raise RuntimeError("Journey files were discovered, but no readable CSV payloads were found.")

        combined = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)
        shutil.rmtree(settings.silver_journeys_dir, ignore_errors=True)
        settings.silver_journeys_dir.mkdir(parents=True, exist_ok=True)
        (
            combined.repartition(8, "year", "month")
            .write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(str(settings.silver_journeys_dir))
        )
    finally:
        try:
            spark.stop()
        except Exception:
            pass

    return {
        "raw_file_count": len(raw_files),
        "expanded_input_count": len(expanded_inputs),
        "silver_root": str(settings.silver_journeys_dir),
        "engine": "spark",
    }


def _normalize_with_pandas(raw_files: list[Path], settings: Settings) -> dict[str, str | int]:
    extraction_root = ensure_directory(settings.temp_dir / "extracted")
    expanded_inputs: list[str] = []
    seen_trip_ids: set[str] = set()
    written_rows = 0
    part_counter = 0

    shutil.rmtree(settings.silver_journeys_dir, ignore_errors=True)
    settings.silver_journeys_dir.mkdir(parents=True, exist_ok=True)

    for raw_file in raw_files:
        for input_path in expand_raw_inputs(raw_file, extraction_root):
            expanded_inputs.append(str(input_path))
            for chunk in pd.read_csv(input_path, chunksize=100_000):
                if chunk.empty:
                    continue
                normalized_chunk = _standardize_pandas_frame(chunk)
                if normalized_chunk.empty:
                    continue

                normalized_chunk = normalized_chunk.loc[~normalized_chunk["trip_id"].isin(seen_trip_ids)].copy()
                if normalized_chunk.empty:
                    continue

                seen_trip_ids.update(normalized_chunk["trip_id"].astype(str).tolist())
                written_rows += len(normalized_chunk)
                part_counter = _write_partitioned_chunk(normalized_chunk, settings.silver_journeys_dir, part_counter)

    if written_rows == 0:
        raise RuntimeError("Journey files were discovered, but no readable CSV payloads were found.")

    return {
        "raw_file_count": len(raw_files),
        "expanded_input_count": len(expanded_inputs),
        "written_row_count": written_rows,
        "silver_root": str(settings.silver_journeys_dir),
        "engine": "pandas",
    }


def normalize_all_raw_journeys(settings: Settings, requested_files: list[str] | None = None) -> dict[str, str | int]:
    raw_files = selected_raw_journey_files(settings, requested_files)
    if not raw_files:
        raise RuntimeError("No raw journey files were found. Run ingestion first.")

    if settings.normalization_engine == "pandas":
        return _normalize_with_pandas(raw_files, settings)

    if settings.normalization_engine != "spark":
        raise RuntimeError(f"Unsupported normalization engine: {settings.normalization_engine}")

    try:
        return _normalize_with_spark(raw_files, settings)
    except Exception:
        if settings.deploy_target != "local":
            raise
        LOGGER.exception("Spark normalization failed in local mode; falling back to pandas.")
        return _normalize_with_pandas(raw_files, settings)
