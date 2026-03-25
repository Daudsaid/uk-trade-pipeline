"""
lookup.py — Fetch reference/lookup data from the uktradeinfo API and upsert
into PostgreSQL dimension tables.

Endpoints:
  /Country   → dim_countries(country_id, country_name, country_code)
  /Port      → dim_ports(port_id, port_name)
  /Commodity → dim_commodities(commodity_id, commodity_code, commodity_desc)

Pagination follows OData @odata.nextLink convention.
"""
import logging
from typing import Any

import psycopg2
import psycopg2.extras
import requests

from pipeline.config import API_BASE_URL, DB_CONFIG

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL — create dimension tables if they don't exist
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS dim_countries (
    country_id   INTEGER PRIMARY KEY,
    country_name VARCHAR(255),
    country_code VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_ports (
    port_id   INTEGER PRIMARY KEY,
    port_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_commodities (
    commodity_id   INTEGER PRIMARY KEY,
    commodity_code VARCHAR(20),
    commodity_desc TEXT
);
"""

# ---------------------------------------------------------------------------
# Per-endpoint config: (url, row_mapper, table, insert_sql)
# ---------------------------------------------------------------------------

_ENDPOINTS: list[dict[str, Any]] = [
    {
        "url": f"{API_BASE_URL}/Country",
        "table": "dim_countries",
        "mapper": lambda r: (
            r.get("CountryId"),
            r.get("CountryName"),
            r.get("CountryCode"),
        ),
        "insert": """
            INSERT INTO dim_countries (country_id, country_name, country_code)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """,
    },
    {
        "url": f"{API_BASE_URL}/Port",
        "table": "dim_ports",
        "mapper": lambda r: (
            r.get("PortId"),
            r.get("PortName"),
        ),
        "insert": """
            INSERT INTO dim_ports (port_id, port_name)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """,
    },
    {
        "url": f"{API_BASE_URL}/Commodity",
        "table": "dim_commodities",
        "mapper": lambda r: (
            r.get("CommodityId"),
            r.get("CommodityCode"),
            r.get("CommodityDescription"),
        ),
        "insert": """
            INSERT INTO dim_commodities (commodity_id, commodity_code, commodity_desc)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """,
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_all(url: str, timeout: int = 60) -> list[dict]:
    """GET *url* and follow @odata.nextLink pagination, returning all records."""
    session = requests.Session()
    records: list[dict] = []
    next_url: str | None = url
    page = 0

    while next_url:
        page += 1
        logger.info("  GET %s (page %d)", next_url, page)
        resp = session.get(next_url, headers={"Accept": "application/json"}, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        records.extend(payload.get("value", []))
        next_url = payload.get("@odata.nextLink")

    logger.info("  Fetched %d records in %d page(s)", len(records), page)
    return records


def _ensure_tables(cur: psycopg2.extensions.cursor) -> None:
    cur.execute(_DDL)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_lookups() -> dict[str, int]:
    """
    Fetch all lookup endpoints and upsert into the corresponding dim tables.

    Returns
    -------
    dict[str, int]
        Mapping of table name → number of rows inserted.
    """
    counts: dict[str, int] = {}

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                _ensure_tables(cur)

        for ep in _ENDPOINTS:
            table = ep["table"]
            logger.info("Loading %s from %s", table, ep["url"])

            try:
                records = _fetch_all(ep["url"])
            except requests.RequestException as exc:
                logger.error("Failed to fetch %s: %s", ep["url"], exc)
                counts[table] = 0
                continue

            rows = [ep["mapper"](r) for r in records]
            # Drop rows where the primary key is None
            rows = [r for r in rows if r[0] is not None]

            if not rows:
                logger.warning("No rows to insert for %s", table)
                counts[table] = 0
                continue

            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, ep["insert"], rows)
                    counts[table] = cur.rowcount

            logger.info("Inserted %d rows into %s", counts[table], table)

    except Exception:
        logger.exception("load_lookups failed.")
        raise
    finally:
        conn.close()

    return counts
