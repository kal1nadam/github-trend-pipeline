"""Tests for pipeline.api_models — Pydantic response models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeline.api_models import AlertItem, DailySummary, TrendingLanguage, TrendingRepo


class TestTrendingRepo:
    def test_valid_minimal(self):
        repo = TrendingRepo(
            event_date="2025-01-01",
            repo_name="torvalds/linux",
            primary_language="C",
            license="GPL-2.0",
            events_today=500,
            actors_today=100,
            stars_today=50,
            trend_score=8.5,
        )
        assert repo.repo_name == "torvalds/linux"
        assert repo.growth_events_ratio is None
        assert repo.z_events is None

    def test_valid_with_optional_fields(self):
        repo = TrendingRepo(
            event_date="2025-01-01",
            repo_name="foo/bar",
            primary_language="Python",
            license="MIT",
            events_today=100,
            actors_today=20,
            stars_today=10,
            growth_events_ratio=4.5,
            z_events=7.2,
            trend_score=6.1,
        )
        assert repo.growth_events_ratio == 4.5
        assert repo.z_events == 7.2

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            TrendingRepo(  # type: ignore[call-arg]
                event_date="2025-01-01",
                repo_name="foo/bar",
                # missing primary_language, license, events_today, etc.
                trend_score=1.0,
            )

    def test_trend_score_zero(self):
        repo = TrendingRepo(
            event_date="2025-01-01",
            repo_name="a/b",
            primary_language="Go",
            license="Apache-2.0",
            events_today=50,
            actors_today=10,
            stars_today=0,
            trend_score=0.0,
        )
        assert repo.trend_score == 0.0


class TestTrendingLanguage:
    def test_valid(self):
        lang = TrendingLanguage(
            event_date="2025-01-01",
            primary_language="Python",
            trending_repos_count=10,
            events_today_total=5000,
            actors_today_total=1000,
            stars_today_total=200,
            avg_trend_score=4.5,
            total_trend_score=45.0,
            top_repos=[{"repo_name": "foo/bar", "trend_score": 9.0}],
        )
        assert lang.primary_language == "Python"
        assert lang.trending_repos_count == 10

    def test_top_repos_empty_list(self):
        lang = TrendingLanguage(
            event_date="2025-01-01",
            primary_language="Rust",
            trending_repos_count=0,
            events_today_total=0,
            actors_today_total=0,
            stars_today_total=0,
            avg_trend_score=0.0,
            total_trend_score=0.0,
            top_repos=[],
        )
        assert lang.top_repos == []


class TestAlertItem:
    def test_valid_minimal(self):
        alert = AlertItem(
            event_date="2025-01-01",
            alert_type="repo",
            entity="foo/bar",
            severity="high",
            created_at="2025-01-01T00:00:00Z",
        )
        assert alert.severity == "high"
        assert alert.trend_score is None
        assert alert.primary_language is None

    def test_language_alert(self):
        alert = AlertItem(
            event_date="2025-01-01",
            alert_type="language",
            entity="Python",
            severity="medium",
            trend_score=5.2,
            events_today=9000,
            actors_today=1800,
            stars_today=400,
            primary_language="Python",
            created_at="2025-01-01T12:00:00Z",
        )
        assert alert.alert_type == "language"
        assert alert.trend_score == 5.2

    def test_all_optional_fields_none(self):
        alert = AlertItem(
            event_date="2025-01-01",
            alert_type="repo",
            entity="org/repo",
            severity="low",
            created_at="",
        )
        assert alert.z_events is None
        assert alert.growth_events_ratio is None

    def test_missing_required_raises(self):
        with pytest.raises(ValidationError):
            AlertItem(  # type: ignore[call-arg]
                event_date="2025-01-01",
                # missing alert_type, entity, severity, created_at
            )


class TestDailySummary:
    def test_valid(self):
        summary = DailySummary(
            event_date="2025-01-01",
            summary_text="Daily GitHub trend summary for 2025-01-01: ...",
            top_repos=["torvalds/linux", "python/cpython"],
            top_languages=["Python", "Go", "Rust"],
            created_at="2025-01-01T00:00:00Z",
        )
        assert len(summary.top_repos) == 2
        assert "Python" in summary.top_languages

    def test_empty_lists(self):
        summary = DailySummary(
            event_date="2025-01-01",
            summary_text="No data.",
            top_repos=[],
            top_languages=[],
            created_at="",
        )
        assert summary.top_repos == []
        assert summary.top_languages == []

    def test_missing_required_raises(self):
        with pytest.raises(ValidationError):
            DailySummary(  # type: ignore[call-arg]
                event_date="2025-01-01",
                # missing summary_text, top_repos, top_languages, created_at
            )
