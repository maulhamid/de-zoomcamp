from datetime import date, datetime

from tfl_cycle_analytics.utils import (
    canonicalize_column_name,
    compute_duration_seconds,
    deduplicate_trip_records,
    duration_band,
    month_tokens_between,
    parse_year_month_token,
)


def test_parse_year_month_token_handles_multiple_patterns():
    assert parse_year_month_token("JourneyDataExtract_2024-01.csv") == "2024-01"
    assert parse_year_month_token("JourneyDataExtractJan2024.csv") == "2024-01"
    assert parse_year_month_token("202402_cyclehire.zip") == "2024-02"


def test_month_tokens_between_generates_closed_range():
    values = month_tokens_between(date(2024, 1, 1), date(2024, 3, 31))
    assert values == {"2024-01", "2024-02", "2024-03"}


def test_canonicalize_column_name_removes_spacing_and_punctuation():
    assert canonicalize_column_name("StartStation Id") == "startstationid"


def test_compute_duration_seconds_prefers_fallback_and_never_returns_negative():
    started_at = datetime(2024, 1, 1, 8, 0, 0)
    ended_at = datetime(2024, 1, 1, 8, 10, 0)
    assert compute_duration_seconds(started_at, ended_at, fallback_duration=600) == 600
    assert compute_duration_seconds(ended_at, started_at, fallback_duration=None) == 0


def test_duration_band_matches_expected_buckets():
    assert duration_band(200) == "0-15 min"
    assert duration_band(1200) == "15-30 min"
    assert duration_band(2500) == "30-60 min"
    assert duration_band(4000) == "60+ min"
    assert duration_band(None) == "unknown"


def test_deduplicate_trip_records_keeps_first_record():
    deduplicated = deduplicate_trip_records(
        [
            {"trip_id": "a", "duration_seconds": 100},
            {"trip_id": "a", "duration_seconds": 200},
            {"trip_id": "b", "duration_seconds": 300},
        ]
    )
    assert deduplicated == [
        {"trip_id": "a", "duration_seconds": 100},
        {"trip_id": "b", "duration_seconds": 300},
    ]
