"""Tests for pipeline.bq — SQL rendering logic (no BigQuery connection required)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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
    """BQQueryRunner with a mocked BigQuery client."""
    with patch("google.cloud.bigquery.Client"):
        from pipeline.bq import BQQueryRunner

        return BQQueryRunner(_make_settings())


class TestRenderSql:
    def test_replaces_raw_dataset(self, runner):
        rendered = runner.render_sql("FROM `${RAW_DATASET}.events`")
        assert "raw_github.events" in rendered
        assert "${RAW_DATASET}" not in rendered

    def test_replaces_stg_dataset(self, runner):
        rendered = runner.render_sql("FROM `${STG_DATASET}.stg_github_events`")
        assert "stg_github.stg_github_events" in rendered
        assert "${STG_DATASET}" not in rendered

    def test_replaces_mart_dataset(self, runner):
        rendered = runner.render_sql("FROM `${MART_DATASET}.alerts_daily`")
        assert "mart_github.alerts_daily" in rendered
        assert "${MART_DATASET}" not in rendered

    def test_replaces_gcp_project_id(self, runner):
        rendered = runner.render_sql("PROJECT = '${GCP_PROJECT_ID}'")
        assert "test-project" in rendered
        assert "${GCP_PROJECT_ID}" not in rendered

    def test_replaces_lookback_days(self, runner):
        rendered = runner.render_sql("INTERVAL ${LOOKBACK_DAYS} DAY")
        assert "14" in rendered
        assert "${LOOKBACK_DAYS}" not in rendered

    def test_replaces_min_events_threshold(self, runner):
        rendered = runner.render_sql("events >= ${MIN_EVENTS_THRESHOLD}")
        assert "50" in rendered
        assert "${MIN_EVENTS_THRESHOLD}" not in rendered

    def test_replaces_alert_thresholds(self, runner):
        sql = "z >= ${ALERT_Z_THRESHOLD_LOW} OR ratio >= ${ALERT_GROWTH_THRESHOLD_LOW}"
        rendered = runner.render_sql(sql)
        assert "3.0" in rendered
        assert "${ALERT_Z_THRESHOLD_LOW}" not in rendered
        assert "${ALERT_GROWTH_THRESHOLD_LOW}" not in rendered

    def test_replaces_max_alerts(self, runner):
        sql = "LIMIT ${MAX_REPO_ALERTS} OR ${MAX_LANGUAGE_ALERTS}"
        rendered = runner.render_sql(sql)
        assert "50" in rendered
        assert "20" in rendered

    def test_extra_mapping_overrides(self, runner):
        rendered = runner.render_sql(
            "DATE = '${DATE}'",
            extra={"DATE": "2025-01-01"},
        )
        assert "2025-01-01" in rendered
        assert "${DATE}" not in rendered

    def test_extra_mapping_can_add_new_keys(self, runner):
        rendered = runner.render_sql(
            "TABLE = '${SOURCE_TABLE}'",
            extra={"SOURCE_TABLE": "20250101"},
        )
        assert "20250101" in rendered

    def test_no_placeholders_unchanged(self, runner):
        sql = "SELECT 1 AS one"
        assert runner.render_sql(sql) == sql

    def test_multiple_replacements_in_one_query(self, runner):
        sql = (
            "SELECT * FROM `${RAW_DATASET}.events` "
            "JOIN `${MART_DATASET}.alerts_daily` "
            "WHERE days = ${LOOKBACK_DAYS}"
        )
        rendered = runner.render_sql(sql)
        assert "raw_github" in rendered
        assert "mart_github" in rendered
        assert "14" in rendered
        assert "${" not in rendered

    def test_custom_dataset_values(self):
        with patch("google.cloud.bigquery.Client"):
            from pipeline.bq import BQQueryRunner

            runner = BQQueryRunner(_make_settings(raw_dataset="custom_raw", mart_dataset="custom_mart"))
        rendered = runner.render_sql("${RAW_DATASET} and ${MART_DATASET}")
        assert "custom_raw" in rendered
        assert "custom_mart" in rendered
