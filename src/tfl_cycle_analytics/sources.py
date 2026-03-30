from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse
import re
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from tfl_cycle_analytics.config import Settings
from tfl_cycle_analytics.utils import month_tokens_between, parse_year_month_token, write_json


SUPPORTED_EXTENSIONS = (".csv", ".zip", ".gz")
JOURNEY_HINT_PATTERN = re.compile(r"(journeydataextract|cyclehire|santander)", re.IGNORECASE)
S3_LIST_HOST = "https://s3-eu-west-1.amazonaws.com"


def is_journey_filename(filename: str) -> bool:
    return JOURNEY_HINT_PATTERN.search(filename) is not None and filename.lower().endswith(SUPPORTED_EXTENSIONS)


def parse_directory_listing(html: str, base_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        absolute_url = urljoin(base_url, href)
        filename = Path(urlparse(absolute_url).path).name
        if not filename:
            continue
        records.append(
            {
                "filename": filename,
                "url": absolute_url,
                "year_month": parse_year_month_token(filename),
            }
        )
    return records


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(requests.RequestException),
)
def discover_tfl_journey_files(base_url: str) -> list[dict[str, Any]]:
    bucket_name = urlparse(base_url).netloc
    continuation_token: str | None = None
    discovered: list[dict[str, Any]] = []

    while True:
        params: dict[str, str] = {"list-type": "2"}
        if continuation_token:
            params["continuation-token"] = continuation_token

        response = requests.get(f"{S3_LIST_HOST}/{bucket_name}/", params=params, timeout=60)
        response.raise_for_status()
        root = ET.fromstring(response.text)

        contents_elements = [element for element in root.iter() if element.tag.endswith("Contents")]
        for contents in contents_elements:
            key = next((child.text for child in contents if child.tag.endswith("Key")), None)
            if not key:
                continue
            filename = Path(key).name
            if not is_journey_filename(filename):
                continue
            encoded_key = "/".join(quote(part) for part in key.split("/"))
            discovered.append(
                {
                    "key": key,
                    "filename": filename,
                    "url": urljoin(base_url, encoded_key),
                    "year_month": parse_year_month_token(key),
                }
            )

        is_truncated = next((child.text for child in root if child.tag.endswith("IsTruncated")), "false")
        if is_truncated != "true":
            break
        continuation_token = next((child.text for child in root if child.tag.endswith("NextContinuationToken")), None)

    return sorted(discovered, key=lambda item: (item.get("year_month") or "9999-99", item["filename"]))


def select_source_files(files: list[dict[str, Any]], start_date: date, end_date: date) -> list[dict[str, Any]]:
    valid_months = month_tokens_between(start_date, end_date)
    selected = [item for item in files if item.get("year_month") in valid_months]
    return sorted(selected, key=lambda item: (item["year_month"], item["filename"]))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(requests.RequestException),
)
def _download(url: str) -> requests.Response:
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    return response


def download_source_file(source: dict[str, Any], destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return destination
    response = _download(source["url"])
    with destination.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 512):
            if chunk:
                handle.write(chunk)
    return destination


def discover_selected_journey_files(settings: Settings, start_date: date, end_date: date) -> list[dict[str, Any]]:
    discovered = discover_tfl_journey_files(settings.journey_base_url)
    selected = select_source_files(discovered, start_date, end_date)
    if not selected:
        raise RuntimeError("No journey files matched the requested date window.")
    return selected


def download_selected_journey_files(settings: Settings, selected: list[dict[str, Any]], start_date: date, end_date: date) -> dict[str, Any]:
    downloaded_files: list[str] = []
    for source in selected:
        output_path = settings.raw_journeys_dir / source["filename"]
        download_source_file(source, output_path)
        downloaded_files.append(str(output_path))

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "file_count": len(downloaded_files),
        "files": downloaded_files,
    }
    manifest_path = settings.manifests_dir / f"journeys_{start_date.isoformat()}_{end_date.isoformat()}.json"
    write_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def download_journey_files(settings: Settings, start_date: date, end_date: date) -> dict[str, Any]:
    selected = discover_selected_journey_files(settings, start_date, end_date)
    return download_selected_journey_files(settings, selected, start_date, end_date)


def download_station_snapshot(settings: Settings) -> dict[str, Any]:
    response = requests.get(settings.station_api_url, timeout=60)
    response.raise_for_status()
    payload = response.json()
    generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = settings.raw_stations_dir / f"bikepoint_snapshot_{generated_at}.json"
    write_json(output_path, payload)
    return {"station_snapshot_path": str(output_path), "record_count": len(payload)}
