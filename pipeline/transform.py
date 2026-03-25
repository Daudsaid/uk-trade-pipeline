"""
transform.py — Clean and normalise raw HMRC OTS CSV files.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

COLUMN_MAP = {
    "MonthId": "period",
    "FlowTypeId": "flow_type",
    "CommodityId": "commodity_id",
    "CountryId": "country_id",
    "PortId": "port_id",
    "Value": "value_gbp",
    "NetMass": "net_mass_kg",
}

FLOW_TYPE_MAP = {
    1: "import",
    2: "export",
    3: "eu_import",
    4: "eu_export",
}


def transform(filepath: Path) -> pd.DataFrame:
    """
    Load, clean, and normalise a raw HMRC OTS CSV file.

    Parameters
    ----------
    filepath : Path
        Path to a raw ots_YYYYMM.csv file.

    Returns
    -------
    pd.DataFrame with canonical columns and cleaned data.
    """
    df = pd.read_csv(filepath, encoding="latin-1", low_memory=False)
    rows_raw = len(df)
    logger.info("Loaded %d rows from %s", rows_raw, filepath)

    # Rename to snake_case schema columns (keep unrecognised columns in place)
    df.rename(columns=COLUMN_MAP, inplace=True)

    # Convert period: 202401 (int) → 2024-01-01 (date string)
    df["period"] = pd.to_datetime(
        df["period"].astype(str), format="%Y%m", errors="coerce"
    ).dt.date

    # Map flow_type int → string label
    df["flow_type"] = df["flow_type"].map(FLOW_TYPE_MAP)

    # Cast value and mass to float, coercing bad values to NaN
    df["value_gbp"] = pd.to_numeric(df["value_gbp"], errors="coerce").astype("float64")
    df["net_mass_kg"] = pd.to_numeric(df["net_mass_kg"], errors="coerce")

    # Drop rows where BOTH value_gbp and net_mass_kg are NaN
    df = df[~(df["value_gbp"].isna() & df["net_mass_kg"].isna())]

    # Drop aggregate/suppressed rows (negative commodity_id)
    df = df[df["commodity_id"] >= 0]

    # Add audit timestamp
    df["loaded_at"] = datetime.now(timezone.utc)

    # Select and order final columns, dropping all others
    final_cols = ["period", "flow_type", "commodity_id", "country_id", "port_id",
                  "value_gbp", "net_mass_kg", "loaded_at"]
    df = df[final_cols]

    rows_clean = len(df)
    logger.info(
        "Cleaning complete: %d → %d rows (%d dropped) from %s",
        rows_raw,
        rows_clean,
        rows_raw - rows_clean,
        filepath,
    )

    return df.reset_index(drop=True)
