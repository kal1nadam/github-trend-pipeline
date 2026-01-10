from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel


class TrendingRepo(BaseModel):
    event_date: str
    repo_name: str
    primary_language: str
    license: str
    events_today: int
    actors_today: int
    stars_today: int
    growth_events_ratio: Optional[float] = None
    z_events: Optional[float] = None
    trend_score: float

class TrendingLanguage(BaseModel):
    event_date: str
    primary_language: str
    trending_repos_count: int
    events_today_total: int
    actors_today_total: int
    stars_today_total: int
    avg_trend_score: float
    total_trend_score: float
    top_repos: Any

class AlertItem(BaseModel):
    event_date: str
    alert_type: str
    entity: str
    severity: str
    trend_score: Optional[float] = None
    z_events: Optional[float] = None
    growth_events_ratio: Optional[float] = None
    events_today: Optional[int] = None
    actors_today: Optional[int] = None
    stars_today: Optional[int] = None
    primary_language: Optional[str] = None
    created_at: str

class DailySummary(BaseModel):
    event_date: str
    summary_text: str
    top_repos: list[str]
    top_languages: list[str]
    created_at: str