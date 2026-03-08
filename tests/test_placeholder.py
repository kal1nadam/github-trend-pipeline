"""Tests for pipeline.transform — SQL file discovery and ordering."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from pipeline.transform import iter_sql_files


class TestIterSqlFiles:
    def test_returns_a_list(self):
        files = iter_sql_files()
        assert isinstance(files, list)

    def test_all_returned_files_exist(self):
        for f in iter_sql_files():
            assert f.exists(), f"File not found: {f}"

    def test_all_returned_files_are_sql(self):
        for f in iter_sql_files():
            assert f.suffix == ".sql"

    def test_staging_before_models(self):
        files = iter_sql_files()
        paths = [str(f) for f in files]
        staging = [p for p in paths if "10_staging" in p]
        models = [p for p in paths if "20_models" in p]
        marts = [p for p in paths if "30_marts" in p]
        assert staging, "Expected staging SQL files"
        assert models, "Expected model SQL files"
        assert marts, "Expected mart SQL files"
        # Indices: all staging before all models, all models before all marts
        assert max(paths.index(p) for p in staging) < min(paths.index(p) for p in models)
        assert max(paths.index(p) for p in models) < min(paths.index(p) for p in marts)

    def test_files_within_directory_are_sorted(self):
        files = iter_sql_files()
        for dir_name in ["10_staging", "20_models", "30_marts"]:
            dir_files = [f for f in files if dir_name in str(f)]
            assert dir_files == sorted(dir_files), f"Files in {dir_name} are not sorted"

    def test_missing_directory_is_skipped(self, tmp_path, monkeypatch):
        """iter_sql_files should not raise if a directory doesn't exist."""
        monkeypatch.setattr("pipeline.transform.SQL_ROOT", tmp_path)
        result = iter_sql_files()
        assert result == []

    def test_contains_stg_github_events(self):
        files = iter_sql_files()
        names = [f.name for f in files]
        assert "stg_github_events.sql" in names

    def test_contains_trending_repos_daily(self):
        files = iter_sql_files()
        names = [f.name for f in files]
        assert "00_trending_repos_daily.sql" in names

    def test_does_not_include_setup_sql(self):
        files = iter_sql_files()
        for f in files:
            assert "00_setup" not in str(f), f"Setup SQL should not be included: {f}"


class TestTransformMain:
    def test_dry_run_does_not_call_bq(self, monkeypatch):
        """With --dry-run, no BQ queries should be executed."""
        monkeypatch.setattr("sys.argv", ["transform", "--dry-run"])
        executed = []
        with patch("pipeline.transform.BQQueryRunner") as MockRunner:
            mock_instance = MagicMock()
            mock_instance.run.side_effect = lambda *a, **kw: executed.append(a)
            MockRunner.return_value = mock_instance
            with patch("google.cloud.bigquery.Client"):
                from pipeline.transform import main

                main()
        assert executed == [], "BQ runner should not be called in dry-run mode"
