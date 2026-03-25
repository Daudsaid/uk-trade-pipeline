"""Unit tests for pipeline/load.py"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call

from pipeline.load import _df_to_tuples, load_dataframe


SAMPLE_DF = pd.DataFrame(
    {
        "period": ["2024-01-01", "2024-01-01"],
        "commodity_code": ["01011010", "01011020"],
        "commodity_desc": ["Live horses", "Live donkeys"],
        "flow_type": ["EU_IMPORTS", "EU_IMPORTS"],
        "country_code": ["DE", "FR"],
        "country_name": ["Germany", "France"],
        "value_gbp": [1500.0, 800.0],
        "net_mass_kg": [32000.0, 17000.0],
    }
)


class TestDfToTuples:
    def test_returns_correct_length(self):
        rows = _df_to_tuples(SAMPLE_DF.copy())
        assert len(rows) == 2

    def test_tuple_order(self):
        rows = _df_to_tuples(SAMPLE_DF.copy())
        period, commodity_code, commodity_desc, flow_type, country_code, country_name, value_gbp, net_mass_kg = rows[0]
        assert period == "2024-01-01"
        assert commodity_code == "01011010"
        assert flow_type == "EU_IMPORTS"
        assert country_code == "DE"
        assert value_gbp == 1500.0

    def test_missing_optional_columns_become_none(self):
        df = SAMPLE_DF[["period", "commodity_code", "flow_type", "country_code", "value_gbp"]].copy()
        rows = _df_to_tuples(df)
        # commodity_desc (index 2) should be None
        assert rows[0][2] is None


class TestLoadDataframe:
    def test_empty_df_returns_zero(self):
        result = load_dataframe(pd.DataFrame())
        assert result == 0

    def test_calls_execute_values(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("pipeline.load.get_connection", return_value=mock_conn), \
             patch("pipeline.load.psycopg2.extras.execute_values") as mock_ev:
            result = load_dataframe(SAMPLE_DF.copy(), batch_size=10)

        mock_ev.assert_called_once()
        assert result == 2

    def test_batching(self):
        df = pd.concat([SAMPLE_DF] * 5, ignore_index=True)  # 10 rows

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("pipeline.load.get_connection", return_value=mock_conn), \
             patch("pipeline.load.psycopg2.extras.execute_values") as mock_ev:
            result = load_dataframe(df, batch_size=3)

        # 10 rows / batch_size 3 → 4 batches
        assert mock_ev.call_count == 4
        assert result == 10
