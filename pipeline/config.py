from __future__ import annotations

import os
from dataclasses import dataclass

def _req(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise ValueError(f"Environment variable '{name}' is required but not set.")
    return val

def _opt(name: str, default: str) -> str:
    val = os.getenv(name, "").strip()
    return val if val else default

@dataclass(frozen=True)
class Settings:
    gcp_project_id: str
    bq_location: str

    raw_dataset: str
    stg_dataset: str
    mart_dataset: str

    source_events_project: str
    source_events_dataset: str
    source_repos_table: str

    lookback_days: int
    min_events_threshold: int

    @staticmethod
    def load() -> Settings:
        return Settings(
            gcp_project_id=_req("GCP_PROJECT_ID"),
            bq_location=_opt("BQ_LOCATION", "US"),

            raw_dataset=_opt("RAW_DATASET", "raw_github"),
            stg_dataset=_opt("STG_DATASET", "stg_github"),
            mart_dataset=_opt("MART_DATASET", "mart_github"),

            source_events_project=_opt("SOURCE_EVENTS_PROJECT", "githubarchive"),
            source_events_dataset=_opt("SOURCE_EVENTS_DATASET", "day"),
            source_repos_table=_opt("SOURCE_REPOS_TABLE", "bigquery-public-data.github_repos.sample_repos"),

            lookback_days=int(_opt("LOOKBACK_DAYS", "14")),
            min_events_threshold=int(_opt("MIN_EVENTS_THRESHOLD", "50")),
        )