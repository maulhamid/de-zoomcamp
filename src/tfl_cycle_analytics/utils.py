from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any, Iterable, Mapping, Sequence


YEAR_MONTH_PATTERNS = (
    re.compile(r"(?P<year>20\d{2})[-_ ](?P<month>0[1-9]|1[0-2])"),
    re.compile(r"(?P<month>0[1-9]|1[0-2])[-_ ](?P<year>20\d{2})"),
    re.compile(r"(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])"),
)
MONTH_NAME_PATTERN = re.compile(
    r"(?P<month_name>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[-_ ]?(?P<year>20\d{2})",
    re.IGNORECASE,
)
MONTH_LOOKUP = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def canonicalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def parse_year_month_token(value: str) -> str | None:
    for pattern in YEAR_MONTH_PATTERNS:
        match = pattern.search(value)
        if not match:
            continue
        year = match.group("year")
        month = match.group("month")
        return f"{year}-{month}"
    month_name_match = MONTH_NAME_PATTERN.search(value)
    if month_name_match:
        year = month_name_match.group("year")
        month = MONTH_LOOKUP[month_name_match.group("month_name").lower()[:3]]
        return f"{year}-{month}"
    return None


def month_tokens_between(start_date: date, end_date: date) -> set[str]:
    current = date(start_date.year, start_date.month, 1)
    limit = date(end_date.year, end_date.month, 1)
    values: set[str] = set()
    while current <= limit:
        values.add(current.strftime("%Y-%m"))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return values


def sha1_hexdigest(parts: Iterable[Any]) -> str:
    payload = "||".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def compute_duration_seconds(
    started_at: datetime | None,
    ended_at: datetime | None,
    fallback_duration: int | None = None,
) -> int | None:
    if fallback_duration is not None and fallback_duration >= 0:
        return int(fallback_duration)
    if started_at is None or ended_at is None:
        return None
    return max(int((ended_at - started_at).total_seconds()), 0)


def duration_band(duration_seconds: int | None) -> str:
    if duration_seconds is None:
        return "unknown"
    if duration_seconds < 15 * 60:
        return "0-15 min"
    if duration_seconds < 30 * 60:
        return "15-30 min"
    if duration_seconds < 60 * 60:
        return "30-60 min"
    return "60+ min"


def deduplicate_trip_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduplicated: list[dict[str, Any]] = []
    for record in records:
        trip_key = record.get("trip_id") or sha1_hexdigest(
            [
                record.get("rental_id"),
                record.get("bike_id"),
                record.get("started_at"),
                record.get("ended_at"),
                record.get("start_station_id"),
                record.get("end_station_id"),
            ]
        )
        if trip_key in seen:
            continue
        seen.add(trip_key)
        deduplicated.append(dict(record))
    return deduplicated


def run_subprocess(command: list[str], cwd: Path, extra_env: Mapping[str, str] | None = None) -> None:
    env = dict(**extra_env) if extra_env else None
    completed = subprocess.run(command, cwd=cwd, env=env, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command {' '.join(command)} failed with code {completed.returncode}\n"
            f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )
