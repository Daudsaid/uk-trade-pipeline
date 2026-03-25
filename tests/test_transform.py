"""Unit tests for pipeline/transform.py"""
import io
import pytest
import pandas as pd
from pathlib import Path

from pipeline.transform import (
    _normalise_columns,
    _cast_types,
    transform_file,
    transform_files,
)


def _write_csv(tmp_path: Path, content: str, name: str = "test.csv") -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


SAMPLE_CSV = """\
PERIOD,COMMODITY CODE,COMMODITY DESCRIPTION,FLOW TYPE,COUNTRY CODE,COUNTRY NAME,VALUE (£000),NET MASS (KG)
202401,01011010,Live horses,EU_IMPORTS,DE,Germany,1500,32000
202401,01011010,Live horses,EU_IMPORTS,FR,France,800,17000
202401,,Bad row,EU_IMPORTS,FR,France,100,500
"""


class TestNormaliseColumns:
    def test_strips_and_uppercases(self):
        df = pd.DataFrame(columns=["  period ", "commodity code"])
        df = _normalise_columns(df)
        assert "period" in df.columns
        assert "commodity_code" in df.columns

    def test_unknown_columns_preserved(self):
        df = pd.DataFrame(columns=["PERIOD", "SOME_EXTRA_COL"])
        df = _normalise_columns(df)
        assert "SOME_EXTRA_COL" in df.columns


class TestCastTypes:
    def test_value_gbp_numeric(self):
        df = pd.DataFrame({"value_gbp": ["1000", "bad", "2500.5"]})
        df = _cast_types(df)
        assert pd.api.types.is_float_dtype(df["value_gbp"])
        assert pd.isna(df.loc[1, "value_gbp"])

    def test_net_mass_numeric(self):
        df = pd.DataFrame({"net_mass_kg": ["500", None, "750"]})
        df = _cast_types(df)
        assert pd.api.types.is_float_dtype(df["net_mass_kg"])

    def test_period_converted_to_date_string(self):
        df = pd.DataFrame({"period": ["202401", "202312"]})
        df = _cast_types(df)
        assert df.loc[0, "period"] == "2024-01-01"
        assert df.loc[1, "period"] == "2023-12-01"

    def test_country_code_uppercased(self):
        df = pd.DataFrame({"country_code": ["de", "fr"]})
        df = _cast_types(df)
        assert list(df["country_code"]) == ["DE", "FR"]


class TestTransformFile:
    def test_returns_dataframe(self, tmp_path):
        p = _write_csv(tmp_path, SAMPLE_CSV)
        df = transform_file(p)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # bad row (no commodity_code) dropped

    def test_canonical_columns_present(self, tmp_path):
        p = _write_csv(tmp_path, SAMPLE_CSV)
        df = transform_file(p)
        for col in ("period", "commodity_code", "flow_type", "country_code", "value_gbp"):
            assert col in df.columns

    def test_flow_type_override(self, tmp_path):
        csv = "PERIOD,COMMODITY CODE,COUNTRY CODE,VALUE (£000)\n202401,0101,DE,1000\n"
        p = _write_csv(tmp_path, csv)
        df = transform_file(p, flow_type_override="NON_EU_IMPORTS")
        assert df.loc[0, "flow_type"] == "NON_EU_IMPORTS"

    def test_value_gbp_is_numeric(self, tmp_path):
        p = _write_csv(tmp_path, SAMPLE_CSV)
        df = transform_file(p)
        assert pd.api.types.is_float_dtype(df["value_gbp"])


class TestTransformFiles:
    def test_empty_list_returns_empty_df(self):
        df = transform_files([])
        assert df.empty

    def test_concatenates_multiple_files(self, tmp_path):
        p1 = _write_csv(tmp_path, SAMPLE_CSV, "a.csv")
        p2 = _write_csv(tmp_path, SAMPLE_CSV, "b.csv")
        df = transform_files([p1, p2])
        assert len(df) == 4  # 2 valid rows × 2 files

    def test_missing_file_skipped(self, tmp_path):
        p1 = _write_csv(tmp_path, SAMPLE_CSV, "real.csv")
        missing = tmp_path / "ghost.csv"
        df = transform_files([p1, missing])
        assert len(df) == 2
