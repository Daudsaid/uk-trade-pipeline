"""
trade_pipeline_dag.py — Monthly Airflow DAG for the UK Trade Flow ETL pipeline.

Schedule: 06:00 UTC on the 2nd of each month, processing the previous month's
data (HMRC publishes OTS data with ~1 month lag).

Task chain:
    download_data → validate_file → transform_data → load_data → log_summary
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _prev_month(logical_date: datetime) -> tuple[int, int]:
    """Return (year, month) for the calendar month before *logical_date*."""
    first_of_current = logical_date.replace(day=1)
    last_of_prev = first_of_current - timedelta(days=1)
    return last_of_prev.year, last_of_prev.month


# ── Task callables ─────────────────────────────────────────────────────────────

def task_download_data(**context) -> str:
    """
    Download the OTS CSV for the previous calendar month.

    Derives year/month from logical_date to account for the ~1 month HMRC lag,
    then calls pipeline.extract.download_period(). Pushes the local CSV path
    to XCom under key 'csv_path'.
    """
    from pipeline.extract import download_period

    logical_date: datetime = context["logical_date"]
    year, month = _prev_month(logical_date)

    log.info("Downloading OTS data for %d-%02d", year, month)
    filepath = download_period(year, month)

    if filepath is None:
        raise AirflowException(
            f"download_period returned None for {year}-{month:02d} — "
            "check API connectivity and logs."
        )

    str_path = str(filepath)
    context["ti"].xcom_push(key="csv_path", value=str_path)
    log.info("Download complete: %s", str_path)
    return str_path


def task_validate_file(**context) -> None:
    """
    Validate that the downloaded CSV exists and contains more than one line
    (i.e. at least a header row plus one data row).

    Raises AirflowException on any validation failure.
    """
    csv_path: str = context["ti"].xcom_pull(key="csv_path", task_ids="download_data")

    if not csv_path:
        raise AirflowException("XCom key 'csv_path' is empty — download task may have failed.")

    p = Path(csv_path)

    if not p.exists():
        raise AirflowException(f"CSV file does not exist: {csv_path}")

    with p.open("r", encoding="utf-8", errors="replace") as fh:
        line_count = sum(1 for _ in fh)

    if line_count <= 1:
        raise AirflowException(
            f"CSV file has only {line_count} line(s) — expected header + data rows: {csv_path}"
        )

    log.info("Validation passed: %s (%d lines)", csv_path, line_count)


def task_transform_data(**context) -> str:
    """
    Read the raw CSV, apply pipeline.transform.transform(), and persist the
    cleaned DataFrame as a Parquet file at data/processed/ots_YYYYMM.parquet.

    Pushes the Parquet path to XCom under key 'parquet_path'.
    """
    from pipeline.transform import transform

    csv_path: str = context["ti"].xcom_pull(key="csv_path", task_ids="download_data")
    filepath = Path(csv_path)

    df = transform(filepath)

    if df.empty:
        raise AirflowException(
            f"Transform produced an empty DataFrame from {csv_path} — check raw file."
        )

    # Derive period tag from the filename (e.g. ots_202401.csv → 202401)
    period_tag = filepath.stem.replace("ots_", "")  # '202401'

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = PROCESSED_DIR / f"ots_{period_tag}.parquet"
    df.to_parquet(parquet_path, index=False)

    str_parquet = str(parquet_path)
    context["ti"].xcom_push(key="parquet_path", value=str_parquet)
    log.info("Transform complete — %d rows saved to %s", len(df), str_parquet)
    return str_parquet


def task_load_data(**context) -> int:
    """
    Read the cleaned Parquet file from XCom and bulk-insert it into PostgreSQL
    via pipeline.load.load(). Pushes the inserted row count to XCom under
    key 'rows_inserted'.
    """
    import pandas as pd
    from pipeline.load import load

    parquet_path: str = context["ti"].xcom_pull(key="parquet_path", task_ids="transform_data")
    df = pd.read_parquet(parquet_path)

    rows_inserted = load(df)
    context["ti"].xcom_push(key="rows_inserted", value=rows_inserted)
    log.info("Load complete — %d rows inserted.", rows_inserted)
    return rows_inserted


def task_log_summary(**context) -> None:
    """
    Emit a structured summary log covering period, rows inserted, raw file size,
    and wall-clock completion time.
    """
    logical_date: datetime = context["logical_date"]
    year, month = _prev_month(logical_date)

    rows_inserted: int = context["ti"].xcom_pull(key="rows_inserted", task_ids="load_data") or 0
    csv_path: str = context["ti"].xcom_pull(key="csv_path", task_ids="download_data") or ""
    parquet_path: str = context["ti"].xcom_pull(key="parquet_path", task_ids="transform_data") or ""

    csv_size_kb = Path(csv_path).stat().st_size / 1024 if csv_path and Path(csv_path).exists() else 0
    parquet_size_kb = (
        Path(parquet_path).stat().st_size / 1024
        if parquet_path and Path(parquet_path).exists()
        else 0
    )

    completion_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info(
        "Pipeline summary | period=%d-%02d | rows_inserted=%d | "
        "csv_size=%.1f KB | parquet_size=%.1f KB | completed_at=%s",
        year,
        month,
        rows_inserted,
        csv_size_kb,
        parquet_size_kb,
        completion_time,
    )


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="uk_trade_pipeline",
    description="Monthly ETL pipeline for UK HMRC OTS trade flow data",
    default_args=DEFAULT_ARGS,
    schedule="0 6 2 * *",       # 06:00 UTC on the 2nd of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trade", "hmrc", "etl"],
) as dag:

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=task_download_data,
    )

    validate_file = PythonOperator(
        task_id="validate_file",
        python_callable=task_validate_file,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform_data,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=task_load_data,
    )

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_summary,
    )

    download_data >> validate_file >> transform_data >> load_data >> log_summary
