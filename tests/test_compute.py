"""Tests for pipeline.compute — SQL templates and orchestration logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipeline.compute import ALERTS_INSERT_SQL, LANG_ALERTS_INSERT_SQL, SUMMARY_INSERT_SQL
from pipeline.config import Settings


def _make_settings(**overrides) -> Settings:
    base = dict(
        gcp_project_id="test-project",
        bq_location="US",
        raw_dataset="raw_github",
        stg_dataset="stg_github",
        mart_dataset="mart_github",
        source_events_project="githubarchive",
        source_events_dataset="day",
        source_repos_table="bigquery-public-data.github_repos.sample_repos",
        lookback_days=14,
        min_events_threshold=50,
        alert_z_threshold_low=3.0,
        alert_growth_threshold_low=3.0,
        max_repo_alerts=50,
        max_language_alerts=20,
    )
    base.update(overrides)
    return Settings(**base)


@pytest.fixture()
def runner():
    with patch("google.cloud.bigquery.Client"):
        from pipeline.bq import BQQueryRunner

        return BQQueryRunner(_make_settings())


class TestAlertsInsertSql:
    def test_contains_mart_dataset_placeholder(self):
        assert "${MART_DATASET}" in ALERTS_INSERT_SQL

    def test_contains_date_placeholder(self):
        assert "${DATE}" in ALERTS_INSERT_SQL

    def test_contains_alert_z_threshold_placeholder(self):
        assert "${ALERT_Z_THRESHOLD_LOW}" in ALERTS_INSERT_SQL

    def test_contains_alert_growth_threshold_placeholder(self):
        assert "${ALERT_GROWTH_THRESHOLD_LOW}" in ALERTS_INSERT_SQL

    def test_contains_max_repo_alerts_placeholder(self):
        assert "${MAX_REPO_ALERTS}" in ALERTS_INSERT_SQL

    def test_renders_without_remaining_placeholders(self, runner):
        rendered = runner.render_sql(ALERTS_INSERT_SQL, extra={"DATE": "2025-01-01"})
        assert "${" not in rendered

    def test_renders_correct_mart_dataset(self, runner):
        rendered = runner.render_sql(ALERTS_INSERT_SQL, extra={"DATE": "2025-01-01"})
        assert "mart_github" in rendered

    def test_renders_correct_date(self, runner):
        rendered = runner.render_sql(ALERTS_INSERT_SQL, extra={"DATE": "2025-01-01"})
        assert "2025-01-01" in rendered

    def test_inserts_into_alerts_daily(self):
        assert "alerts_daily" in ALERTS_INSERT_SQL

    def test_severity_levels_present(self):
        assert "'high'" in ALERTS_INSERT_SQL
        assert "'medium'" in ALERTS_INSERT_SQL
        assert "'low'" in ALERTS_INSERT_SQL


class TestLangAlertsInsertSql:
    def test_contains_mart_dataset_placeholder(self):
        assert "${MART_DATASET}" in LANG_ALERTS_INSERT_SQL

    def test_contains_date_placeholder(self):
        assert "${DATE}" in LANG_ALERTS_INSERT_SQL

    def test_contains_max_language_alerts_placeholder(self):
        assert "${MAX_LANGUAGE_ALERTS}" in LANG_ALERTS_INSERT_SQL

    def test_renders_without_remaining_placeholders(self, runner):
        rendered = runner.render_sql(LANG_ALERTS_INSERT_SQL, extra={"DATE": "2025-01-01"})
        assert "${" not in rendered

    def test_inserts_into_alerts_daily(self):
        assert "alerts_daily" in LANG_ALERTS_INSERT_SQL

    def test_sets_alert_type_language(self):
        assert "'language'" in LANG_ALERTS_INSERT_SQL

    def test_severity_levels_present(self):
        assert "'high'" in LANG_ALERTS_INSERT_SQL
        assert "'medium'" in LANG_ALERTS_INSERT_SQL


class TestSummaryInsertSql:
    def test_contains_mart_dataset_placeholder(self):
        assert "${MART_DATASET}" in SUMMARY_INSERT_SQL

    def test_contains_date_placeholder(self):
        assert "${DATE}" in SUMMARY_INSERT_SQL

    def test_renders_without_remaining_placeholders(self, runner):
        rendered = runner.render_sql(SUMMARY_INSERT_SQL, extra={"DATE": "2025-01-01"})
        assert "${" not in rendered

    def test_inserts_into_daily_summary(self):
        assert "daily_summary" in SUMMARY_INSERT_SQL

    def test_selects_top_repos_and_languages(self):
        assert "top_repos" in SUMMARY_INSERT_SQL
        assert "top_languages" in SUMMARY_INSERT_SQL


class TestComputeMain:
    def _make_mock_result(self):
        result = MagicMock()
        result.job_id = "test-job-id"
        result.bytes_processed = 0
        result.bytes_billed = 0
        return result

    def test_main_calls_bq_four_times(self, monkeypatch):
        """main() should run: cleanup + repo alerts + lang alerts + summary."""
        monkeypatch.setattr("sys.argv", ["compute", "--date", "2025-01-01"])
        with patch("google.cloud.bigquery.Client"):
            with patch("pipeline.compute.BQQueryRunner") as MockRunner:
                mock_instance = MagicMock()
                mock_instance.run.return_value = self._make_mock_result()
                MockRunner.return_value = mock_instance
                from pipeline.compute import main

                main()
        assert mock_instance.run.call_count == 4

    def test_main_passes_date_to_alerts(self, monkeypatch):
        """The date passed via CLI should appear in rendered SQL calls."""
        monkeypatch.setattr("sys.argv", ["compute", "--date", "2025-06-15"])
        with patch("google.cloud.bigquery.Client"):
            with patch("pipeline.compute.BQQueryRunner") as MockRunner:
                mock_instance = MagicMock()
                mock_instance.run.return_value = self._make_mock_result()
                # Capture the extra dicts passed to run()
                captured_extras = []

                def fake_run(sql, extra=None, job_config=None):
                    if extra:
                        captured_extras.append(extra)
                    return self._make_mock_result()

                mock_instance.run.side_effect = fake_run
                MockRunner.return_value = mock_instance
                from pipeline.compute import main

                main()
        # The date should appear in at least some of the extra dicts
        dates = [e.get("DATE") for e in captured_extras if e]
        assert any(d == "2025-06-15" for d in dates)

    def test_main_requires_date_argument(self, monkeypatch):
        """main() should exit with an error if --date is not provided."""
        monkeypatch.setattr("sys.argv", ["compute"])
        with pytest.raises(SystemExit):
            from pipeline.compute import main

            main()
