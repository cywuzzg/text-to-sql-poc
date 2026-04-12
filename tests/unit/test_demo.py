"""Unit tests for scripts/demo.py — argument parsing and pipeline selection."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Make scripts/ importable
SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


def _import_demo():
    """Import demo fresh each test (avoids module caching issues)."""
    import importlib
    import demo
    importlib.reload(demo)
    return demo


class TestDemoArgParsing:
    def test_no_args_selects_local_mode(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["demo.py"])
        demo = _import_demo()
        parser = demo._build_parser()
        args = parser.parse_args([])
        assert not args.minio
        assert not args.duckdb

    def test_minio_flag_sets_minio(self, monkeypatch):
        demo = _import_demo()
        parser = demo._build_parser()
        args = parser.parse_args(["--minio"])
        assert args.minio is True
        assert args.duckdb is False

    def test_duckdb_flag_sets_duckdb(self, monkeypatch):
        demo = _import_demo()
        parser = demo._build_parser()
        args = parser.parse_args(["--duckdb"])
        assert args.duckdb is True
        assert args.minio is False


class TestDemoDuckDBMode:
    def test_duckdb_mode_builds_duckdb_file_pipeline(self, tmp_path, monkeypatch):
        """When --duckdb is passed and the .duckdb file exists, build_duckdb_file_pipeline is called."""
        db_file = tmp_path / "ecommerce.duckdb"
        db_file.touch()  # create empty file so existence check passes

        monkeypatch.setattr(sys, "argv", ["demo.py", "--duckdb"])

        mock_pipeline = MagicMock()
        demo = _import_demo()
        with patch("text_to_sql.config.DUCKDB_PATH", str(db_file)):
            with patch.object(demo, "build_duckdb_file_pipeline", return_value=mock_pipeline) as mock_build:
                with patch("builtins.input", side_effect=KeyboardInterrupt):
                    demo.main()
                mock_build.assert_called_once()

    def test_duckdb_mode_exits_if_file_missing(self, tmp_path, monkeypatch):
        """When --duckdb is passed but the .duckdb file does not exist, exit(1)."""
        missing_db = tmp_path / "no_such.duckdb"
        monkeypatch.setattr(sys, "argv", ["demo.py", "--duckdb"])

        demo = _import_demo()
        with patch("text_to_sql.config.DUCKDB_PATH", str(missing_db)):
            with pytest.raises(SystemExit) as exc_info:
                demo.main()
        assert exc_info.value.code == 1

    def test_duckdb_mode_prints_mode_info(self, tmp_path, monkeypatch, capsys):
        """--duckdb mode prints a message showing the .duckdb file path."""
        db_file = tmp_path / "ecommerce.duckdb"
        db_file.touch()

        monkeypatch.setattr(sys, "argv", ["demo.py", "--duckdb"])

        mock_pipeline = MagicMock()
        demo = _import_demo()
        with patch("text_to_sql.config.DUCKDB_PATH", str(db_file)):
            with patch.object(demo, "build_duckdb_file_pipeline", return_value=mock_pipeline):
                with patch("builtins.input", side_effect=KeyboardInterrupt):
                    demo.main()

        captured = capsys.readouterr()
        assert "DuckDB" in captured.out
        assert str(db_file) in captured.out


class TestDemoLocalMode:
    def test_local_mode_exits_if_data_dir_missing(self, tmp_path, monkeypatch):
        """Default local mode exits when data_dir doesn't exist."""
        missing_dir = tmp_path / "no_such_dir"
        monkeypatch.setattr(sys, "argv", ["demo.py", "--data-dir", str(missing_dir)])

        demo = _import_demo()
        with pytest.raises(SystemExit) as exc_info:
            demo.main()
        assert exc_info.value.code == 1

    def test_local_mode_builds_local_pipeline(self, tmp_path, monkeypatch):
        """Default local mode calls build_local_pipeline when data_dir exists."""
        monkeypatch.setattr(sys, "argv", ["demo.py", "--data-dir", str(tmp_path)])

        mock_pipeline = MagicMock()
        demo = _import_demo()
        with patch.object(demo, "build_local_pipeline", return_value=mock_pipeline):
            with patch("builtins.input", side_effect=KeyboardInterrupt):
                demo.main()
