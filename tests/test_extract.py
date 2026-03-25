"""Unit tests for pipeline/extract.py"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipeline.extract import _build_filename, download_csv, download_period
from pipeline.config import FLOW_TYPES


class TestBuildFilename:
    def test_known_flow_key(self):
        name = _build_filename("EU_IMPORTS", 2024, 1)
        assert name == "smke19202401.csv"

    def test_month_zero_padded(self):
        name = _build_filename("EU_EXPORTS", 2023, 3)
        assert "03" in name

    def test_all_flow_keys(self):
        for key in FLOW_TYPES:
            name = _build_filename(key, 2024, 12)
            assert name.endswith(".csv")
            assert "2024" in name
            assert "12" in name


class TestDownloadCsv:
    def test_unknown_flow_key_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown flow_key"):
            download_csv("INVALID_KEY", 2024, 1, dest_dir=tmp_path)

    def test_returns_existing_file_without_request(self, tmp_path):
        filename = _build_filename("EU_IMPORTS", 2024, 1)
        existing = tmp_path / filename
        existing.write_text("id,value\n1,100")

        with patch("pipeline.extract.requests.get") as mock_get:
            result = download_csv("EU_IMPORTS", 2024, 1, dest_dir=tmp_path)
            mock_get.assert_not_called()

        assert result == existing

    def test_successful_download(self, tmp_path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.iter_content.return_value = [b"col1,col2\nval1,val2"]

        with patch("pipeline.extract.requests.get", return_value=mock_response):
            result = download_csv("EU_IMPORTS", 2024, 1, dest_dir=tmp_path)

        assert result is not None
        assert result.exists()
        assert result.read_bytes() == b"col1,col2\nval1,val2"

    def test_http_error_returns_none(self, tmp_path):
        import requests as req

        with patch("pipeline.extract.requests.get") as mock_get:
            mock_get.side_effect = req.RequestException("Connection error")
            result = download_csv("EU_IMPORTS", 2024, 1, dest_dir=tmp_path)

        assert result is None


class TestDownloadPeriod:
    def test_returns_list_of_paths(self, tmp_path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.iter_content.return_value = [b"a,b\n1,2"]

        with patch("pipeline.extract.requests.get", return_value=mock_response):
            paths = download_period(2024, 1, dest_dir=tmp_path)

        assert len(paths) == len(FLOW_TYPES)
        for p in paths:
            assert isinstance(p, Path)
            assert p.exists()
