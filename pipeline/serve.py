from __future__ import annotations

import os
from typing import List, Optional

from fastapi import FastAPI, Query
from google.cloud import bigquery

from pipeline.config import Settings
from pipeline.api_models import TrendingRepo, TrendingLanguage, AlertItem, DailySummary

app = FastAPI(
    title="GitHub Trend Pipeline API",
    description="API for querying GitHub trending repositories and alerts.",
    version="0.1.0",
)

settings = Settings.load()
bq_client = bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)

MART_DATASET = settings.mart_dataset

def _run_query(sql: str, params: list[bigquery.ScalarQueryParameter]) -> list[dict]:
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = bq_client.query(sql, job_config=job_config).result()
    return [dict(row) for row in rows]

@app.get("/health")
def health():
    return {"status": "ok", "project": settings.gcp_project_id, "mart_dataset": MART_DATASET}

@app.get("/trending/repos", response_model=List[TrendingRepo])
def trending_repos(
    date: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=200),
    language: Optional[str] = Query(None, description="Filter by primary_language")
):
    sql = f"""
    SELECT
        CAST(event_date AS STRING) AS event_date,
        repo_name,
        primary_language,
        license,
        events_today,
        actors_today,
        stars_today,
        growth_events_ratio,
        z_events,
        trend_score
    FROM `{MART_DATASET}.trending_repos_enriched`
    WHERE event_date = DATE(@date)
        AND (@language IS NULL OR primary_language = @language)
    ORDER BY trend_score DESC
    LIMIT @limit;
    """
    params = [
        bigquery.ScalarQueryParameter("date", "STRING", date),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("language", "STRING", language),
    ]
    return _run_query(sql, params)

@app.get("/trending/languages", response_model=List[TrendingLanguage])
def trending_languages(
    date: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=200)
):
    sql = f"""
    SELECT
        CAST(event_date AS STRING) AS event_date,
        primary_language,
        trending_repos_count,
        events_today_total,
        actors_today_total,
        stars_today_total,
        avg_trend_score,
        total_trend_score,
        top_repos
    FROM `{MART_DATASET}.trending_languages_daily`
    WHERE event_date = DATE(@date)
    ORDER BY total_trend_score DESC
    LIMIT @limit;
    """
    params = [
        bigquery.ScalarQueryParameter("date", "STRING", date),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]
    return _run_query(sql, params)

@app.get("/alerts", response_model=List[AlertItem])
def alerts(
    date: str = Query(..., description="YYYY-MM-DD"),
    alert_type: Optional[str] = Query(None, description="repo|language"),
    severity: Optional[str] = Query(None, description="low|medium|high"),
    limit: int = Query(100, ge=1, le=500)
):
    sql = f"""
    SELECT
        CAST(event_date AS STRING) AS event_date,
        alert_type,
        entity,
        severity,
        trend_score,
        z_events,
        growth_events_ratio,
        events_today,
        actors_today,
        stars_today,
        primary_language,
        CAST(created_at AS STRING) AS created_at
    FROM `{MART_DATASET}.alerts_daily`
    WHERE event_date = DATE(@date)
        AND (@alert_type IS NULL OR alert_type = @alert_type)
        AND (@severity IS NULL OR severity = @severity)
    ORDER BY 
        CASE severity
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 2
            WHEN 'low' THEN 1
            ELSE 0
        END DESC,
        COALESCE(trend_score, 0) DESC
    LIMIT @limit;
    """
    params = [
        bigquery.ScalarQueryParameter("date", "STRING", date),
        bigquery.ScalarQueryParameter("alert_type", "STRING", alert_type),
        bigquery.ScalarQueryParameter("severity", "STRING", severity),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]
    return _run_query(sql, params)

@app.get("/summary", response_model=Optional[DailySummary])
def summary(date: str = Query(..., description="YYYY-MM-DD")):
    sql = f"""
    SELECT
        CAST(event_date AS STRING) AS event_date,
        summary_text,
        top_repos,
        top_languages,
        CAST(created_at AS STRING) AS created_at
    FROM `{MART_DATASET}.daily_summary`
    WHERE event_date = DATE(@date)
    LIMIT 1;
    """
    params = [
        bigquery.ScalarQueryParameter("date", "STRING", date),
    ]
    rows = _run_query(sql, params)
    if not rows:
        return {
            "event_date": date,
            "summary_text": "No summary available for this date.",
            "top_repos": [],
            "top_languages": [],
            "created_at": "",
        }
    return rows[0]