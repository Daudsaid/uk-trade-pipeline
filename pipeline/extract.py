"""
extract.py — Download monthly OTS trade data from the uktradeinfo REST API
and save each month as a CSV to data/raw/ots_YYYYMM.csv.

The OTS endpoint paginates at 40,000 rows and uses OData @odata.nextLink for
continuation. We fetch JSON to follow pagination, then request CSV per page
and concatenate into a single file per month.
"""
import io
import logging
from pathlib import Path
from typing import Optional

import requests

from pipeline.config import API_BASE_URL, DOWNLOAD_DIR

logger = logging.getLogger(__name__)

OTS_ENDPOINT = f"{API_BASE_URL}/OTS"
PAGE_SIZE = 40_000


def _month_id(year: int, month: int) -> int:
    """Return the MonthId integer used by the API (e.g. 202401)."""
    return year * 100 + month


def download_period(
    year: int,
    month: int,
    dest_dir: Path = DOWNLOAD_DIR,
    timeout: int = 120,
) -> Optional[Path]:
    """
    Download all OTS rows for the given year/month from the uktradeinfo API.

    Handles pagination via @odata.nextLink and writes a single CSV to
    dest_dir/ots_YYYYMM.csv. Returns the Path on success, None on failure.
    """
    month_id = _month_id(year, month)
    filename = f"ots_{month_id}.csv"
    dest_path = dest_dir / filename

    if dest_path.exists():
        logger.info("Already downloaded: %s", dest_path)
        return dest_path

    logger.info("Fetching OTS data for %04d-%02d (MonthId=%d)", year, month, month_id)

    # First pass: JSON to discover total rows and collect page URLs via nextLink.
    # We then re-request each page as CSV and concatenate.
    session = requests.Session()
    dest_dir.mkdir(parents=True, exist_ok=True)

    header_written = False
    page_num = 0
    next_url: Optional[str] = (
        f"{OTS_ENDPOINT}?$filter=MonthId eq {month_id}&$top={PAGE_SIZE}"
    )

    try:
        with open(dest_path, "w", encoding="utf-8", newline="") as out_fh:
            while next_url:
                page_num += 1
                logger.info("  Page %d: %s", page_num, next_url)

                # Fetch JSON to get nextLink and the row count for this page.
                json_resp = session.get(
                    next_url,
                    headers={"Accept": "application/json"},
                    timeout=timeout,
                )
                json_resp.raise_for_status()
                payload = json_resp.json()

                rows = payload.get("value", [])
                next_url = payload.get("@odata.nextLink")

                if not rows:
                    logger.info("  Page %d returned 0 rows — stopping.", page_num)
                    break

                # Build the CSV URL for this page by appending $format=csv.
                # Strip the nextLink's domain prefix so we use the same base,
                # then just add $format=csv to the current page's query.
                csv_url = _add_csv_format(json_resp.url)
                csv_resp = session.get(
                    csv_url,
                    headers={"Accept": "text/csv"},
                    timeout=timeout,
                )
                csv_resp.raise_for_status()

                # Parse the CSV text; skip the header on pages after the first.
                csv_text = csv_resp.content.decode("utf-8-sig")
                reader = io.StringIO(csv_text)
                lines = reader.readlines()

                if not lines:
                    break

                if header_written:
                    # Skip the header row (index 0) on subsequent pages.
                    lines = lines[1:]

                out_fh.writelines(lines)
                header_written = True
                logger.info(
                    "  Page %d: wrote %d data rows (total so far from %d pages)",
                    page_num,
                    len(lines),
                    page_num,
                )

    except requests.RequestException as exc:
        logger.error("Request failed for MonthId=%d: %s", month_id, exc)
        if dest_path.exists():
            dest_path.unlink()
        return None
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error for MonthId=%d: %s", month_id, exc)
        if dest_path.exists():
            dest_path.unlink()
        return None

    size_kb = dest_path.stat().st_size / 1024
    logger.info("Saved %s (%.1f KB, %d pages)", dest_path, size_kb, page_num)
    return dest_path


def _add_csv_format(url: str) -> str:
    """Append $format=csv to a URL that may already have query parameters."""
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}$format=csv"
