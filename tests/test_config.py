"""Tests for pipeline.config — Settings loading and helpers."""

from __future__ import annotations

import pytest

from pipeline.config import Settings, _opt, _req


class TestOpt:
    def test_returns_default_when_var_unset(self):
        assert _opt("_NONEXISTENT_VAR_XYZ_", "fallback") == "fallback"

    def test_returns_env_value_when_set(self, monkeypatch):
        monkeypatch.setenv("_TEST_OPT_VAR_", "hello")
        assert _opt("_TEST_OPT_VAR_", "fallback") == "hello"

    def test_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("_TEST_OPT_VAR_", "  padded  ")
        assert _opt("_TEST_OPT_VAR_", "fallback") == "padded"

    def test_empty_string_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("_TEST_OPT_VAR_", "")
        assert _opt("_TEST_OPT_VAR_", "default") == "default"

    def test_whitespace_only_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("_TEST_OPT_VAR_", "   ")
        assert _opt("_TEST_OPT_VAR_", "default") == "default"


class TestReq:
    def test_raises_when_var_not_set(self):
        with pytest.raises(ValueError, match="_MISSING_VAR_XYZ_"):
            _req("_MISSING_VAR_XYZ_")

    def test_returns_value_when_set(self, monkeypatch):
        monkeypatch.setenv("_TEST_REQ_VAR_", "required_value")
        assert _req("_TEST_REQ_VAR_") == "required_value"

    def test_raises_on_empty_string(self, monkeypatch):
        monkeypatch.setenv("_TEST_REQ_VAR_", "")
        with pytest.raises(ValueError):
            _req("_TEST_REQ_VAR_")

    def test_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("_TEST_REQ_VAR_", "  value  ")
        assert _req("_TEST_REQ_VAR_") == "value"


class TestSettingsLoad:
    def test_loads_with_defaults(self, monkeypatch):
        # GCP_PROJECT_ID is set by conftest autouse fixture
        s = Settings.load()
        assert s.gcp_project_id == "test-project"
        assert s.bq_location == "US"
        assert s.raw_dataset == "raw_github"
        assert s.stg_dataset == "stg_github"
        assert s.mart_dataset == "mart_github"
        assert s.source_events_project == "githubarchive"
        assert s.source_events_dataset == "day"
        assert s.lookback_days == 14
        assert s.min_events_threshold == 50
        assert s.alert_z_threshold_low == 3.0
        assert s.alert_growth_threshold_low == 3.0
        assert s.max_repo_alerts == 50
        assert s.max_language_alerts == 20

    def test_custom_values_override_defaults(self, monkeypatch):
        monkeypatch.setenv("BQ_LOCATION", "EU")
        monkeypatch.setenv("LOOKBACK_DAYS", "7")
        monkeypatch.setenv("MIN_EVENTS_THRESHOLD", "100")
        monkeypatch.setenv("MAX_REPO_ALERTS", "25")
        s = Settings.load()
        assert s.bq_location == "EU"
        assert s.lookback_days == 7
        assert s.min_events_threshold == 100
        assert s.max_repo_alerts == 25

    def test_raises_without_gcp_project_id(self, monkeypatch):
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        with pytest.raises(ValueError, match="GCP_PROJECT_ID"):
            Settings.load()

    def test_settings_is_frozen(self, monkeypatch):
        s = Settings.load()
        with pytest.raises((AttributeError, TypeError)):
            s.gcp_project_id = "other"  # type: ignore[misc]

    def test_dataset_names_are_configurable(self, monkeypatch):
        monkeypatch.setenv("RAW_DATASET", "my_raw")
        monkeypatch.setenv("STG_DATASET", "my_stg")
        monkeypatch.setenv("MART_DATASET", "my_mart")
        s = Settings.load()
        assert s.raw_dataset == "my_raw"
        assert s.stg_dataset == "my_stg"
        assert s.mart_dataset == "my_mart"
