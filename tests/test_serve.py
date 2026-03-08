"""Tests for pipeline.serve — FastAPI endpoints (BigQuery is mocked)."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

# Set env before any module-level code in serve.py runs
os.environ.setdefault("GCP_PROJECT_ID", "test-project")


@pytest.fixture(scope="module")
def client():
    """FastAPI TestClient with BigQuery mocked at import time."""
    with patch("google.cloud.bigquery.Client"):
        from fastapi.testclient import TestClient

        from pipeline.serve import app

        return TestClient(app)


class TestHealthEndpoint:
    def test_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    def test_returns_project_and_mart_dataset(self, client):
        resp = client.get("/health")
        data = resp.json()
        assert "project" in data
        assert "mart_dataset" in data


class TestTrendingReposEndpoint:
    def test_requires_date_param(self, client):
        resp = client.get("/trending/repos")
        assert resp.status_code == 422  # FastAPI validation error

    def test_valid_request_calls_bq(self, client):
        with patch("pipeline.serve._run_query", return_value=[]) as mock_q:
            resp = client.get("/trending/repos?date=2025-01-01")
            assert resp.status_code == 200
            assert resp.json() == []
            mock_q.assert_called_once()

    def test_language_filter_forwarded(self, client):
        with patch("pipeline.serve._run_query", return_value=[]) as mock_q:
            resp = client.get("/trending/repos?date=2025-01-01&language=Python")
            assert resp.status_code == 200
            # Verify call was made (language param is embedded in query params)
            mock_q.assert_called_once()

    def test_limit_out_of_range_rejected(self, client):
        resp = client.get("/trending/repos?date=2025-01-01&limit=0")
        assert resp.status_code == 422

    def test_returns_list(self, client):
        fake_row = {
            "event_date": "2025-01-01",
            "repo_name": "torvalds/linux",
            "primary_language": "C",
            "license": "GPL-2.0",
            "events_today": 500,
            "actors_today": 100,
            "stars_today": 50,
            "growth_events_ratio": None,
            "z_events": None,
            "trend_score": 8.5,
        }
        with patch("pipeline.serve._run_query", return_value=[fake_row]):
            resp = client.get("/trending/repos?date=2025-01-01")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 1
            assert data[0]["repo_name"] == "torvalds/linux"


class TestTrendingLanguagesEndpoint:
    def test_requires_date_param(self, client):
        resp = client.get("/trending/languages")
        assert resp.status_code == 422

    def test_valid_request(self, client):
        with patch("pipeline.serve._run_query", return_value=[]):
            resp = client.get("/trending/languages?date=2025-01-01")
            assert resp.status_code == 200
            assert resp.json() == []


class TestAlertsEndpoint:
    def test_requires_date_param(self, client):
        resp = client.get("/alerts")
        assert resp.status_code == 422

    def test_valid_request(self, client):
        with patch("pipeline.serve._run_query", return_value=[]):
            resp = client.get("/alerts?date=2025-01-01")
            assert resp.status_code == 200

    def test_severity_filter_accepted(self, client):
        with patch("pipeline.serve._run_query", return_value=[]):
            resp = client.get("/alerts?date=2025-01-01&severity=high")
            assert resp.status_code == 200

    def test_alert_type_filter_accepted(self, client):
        with patch("pipeline.serve._run_query", return_value=[]):
            resp = client.get("/alerts?date=2025-01-01&alert_type=repo")
            assert resp.status_code == 200


class TestSummaryEndpoint:
    def test_requires_date_param(self, client):
        resp = client.get("/summary")
        assert resp.status_code == 422

    def test_returns_empty_summary_when_no_data(self, client):
        with patch("pipeline.serve._run_query", return_value=[]):
            resp = client.get("/summary?date=2025-01-01")
            assert resp.status_code == 200
            data = resp.json()
            assert data["event_date"] == "2025-01-01"
            assert "No summary available" in data["summary_text"]
            assert data["top_repos"] == []

    def test_returns_summary_row(self, client):
        fake_row = {
            "event_date": "2025-01-01",
            "summary_text": "Daily GitHub trend summary for 2025-01-01: ...",
            "top_repos": ["torvalds/linux"],
            "top_languages": ["C"],
            "created_at": "2025-01-01T00:00:00",
        }
        with patch("pipeline.serve._run_query", return_value=[fake_row]):
            resp = client.get("/summary?date=2025-01-01")
            assert resp.status_code == 200
            data = resp.json()
            assert data["top_repos"] == ["torvalds/linux"]
