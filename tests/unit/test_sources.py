from datetime import date
from pathlib import Path

from tfl_cycle_analytics.sources import parse_directory_listing, select_source_files


def test_parse_directory_listing_extracts_urls_and_filenames():
    fixture = Path("tests/fixtures/raw/source_index.html").read_text(encoding="utf-8")
    records = parse_directory_listing(fixture, "https://cycling.data.tfl.gov.uk/")
    assert any(record["filename"] == "JourneyDataExtractJan2024.zip" for record in records)


def test_select_source_files_filters_by_backfill_window():
    fixture = Path("tests/fixtures/raw/source_index.html").read_text(encoding="utf-8")
    records = parse_directory_listing(fixture, "https://cycling.data.tfl.gov.uk/")
    selected = select_source_files(records, date(2024, 2, 1), date(2024, 3, 31))
    assert [item["filename"] for item in selected] == [
        "JourneyDataExtractFeb2024.zip",
        "JourneyDataExtractMar2024.zip",
    ]
