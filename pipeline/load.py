"""
load.py — Bulk-insert a cleaned DataFrame into the PostgreSQL trade_flows table.
"""
import logging

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from pipeline.config import DB_CONFIG

logger = logging.getLogger(__name__)

INSERT_SQL = """
INSERT INTO trade_flows (
    period, flow_type, commodity_id, country_id, port_id,
    value_gbp, net_mass_kg, loaded_at
)
VALUES %s
ON CONFLICT DO NOTHING
"""

# Column order must match INSERT_SQL above
_COLUMNS = [
    "period", "flow_type", "commodity_id", "country_id", "port_id",
    "value_gbp", "net_mass_kg", "loaded_at",
]


def _to_tuples(df: pd.DataFrame) -> list[tuple]:
    """
    Convert DataFrame rows to tuples ready for psycopg2.

    - Replaces all pandas/numpy NaN and NaT with None.
    - period is passed as-is (datetime.date or 'YYYY-MM-DD' string);
      the SQL casts it to DATE via the ::date annotation in the template.
    """
    subset = df[_COLUMNS].copy()

    # Replace every flavour of NA that pandas/numpy can produce with None
    subset = subset.where(subset.notna(), other=None)

    # numpy int/float types are not JSON-serialisable and can confuse psycopg2
    # in edge cases — convert to plain Python scalars via object dtype route
    return [
        tuple(None if (v is not None and isinstance(v, float) and np.isnan(v)) else v
              for v in row)
        for row in subset.itertuples(index=False, name=None)
    ]


def load(df: pd.DataFrame) -> int:
    """
    Bulk-insert *df* into trade_flows, skipping duplicate rows.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned DataFrame produced by pipeline.transform.transform().

    Returns
    -------
    int
        Number of rows actually inserted (duplicates excluded).
    """
    if df.empty:
        logger.warning("Empty DataFrame — nothing to load.")
        return 0

    rows = _to_tuples(df)
    attempted = len(rows)
    logger.info("Attempting to insert %d rows into trade_flows.", attempted)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:  # transaction: commits on success, rolls back on exception
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    INSERT_SQL,
                    rows,
                    template=(
                        "(%s::date, %s, %s, %s, %s, %s, %s, %s)"
                    ),
                )
                inserted = cur.rowcount
    except Exception:
        logger.exception("Load failed — transaction rolled back.")
        raise
    finally:
        conn.close()

    logger.info(
        "Inserted %d / %d rows (%d duplicates skipped).",
        inserted,
        attempted,
        attempted - inserted,
    )
    return inserted
