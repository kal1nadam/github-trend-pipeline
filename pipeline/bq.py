from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from google.cloud import bigquery

from pipeline.config import Settings

@dataclass
class QueryResult:
    job_id: str
    bytes_processed: int
    bytes_billed: int


class BQQueryRunner:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)

    def render_sql(self, sql: str, extra: dict[str, str] | None = None) -> str:
        """
        Very small templating to keep SQL files portable.
        Use placeholders like ${RAW_DATASET}, ${STG_DATASET}, ${MART_DATASET}.
        """
        mapping = {
            "RAW_DATASET": self.settings.raw_dataset,
            "STG_DATASET": self.settings.stg_dataset,
            "MART_DATASET": self.settings.mart_dataset,
            "GCP_PROJECT_ID": self.settings.gcp_project_id,
            "LOOKBACK_DAYS": str(self.settings.lookback_days),
            "MIN_EVENTS_THRESHOLD": str(self.settings.min_events_threshold),

            "ALERT_Z_THRESHOLD_LOW": str(self.settings.alert_z_threshold_low),
            "ALERT_GROWTH_THRESHOLD_LOW": str(self.settings.alert_growth_threshold_low),
            "MAX_REPO_ALERTS": str(self.settings.max_repo_alerts),
            "MAX_LANGUAGE_ALERTS": str(self.settings.max_language_alerts),
            }
        if extra:
            mapping.update(extra)
        
        rendered = sql
        for key, val in mapping.items():
            rendered = rendered.replace(f"${{{key}}}", val)
        return rendered
    
    def run(self, sql: str, extra: dict[str, str] | None = None, job_config: Optional[bigquery.QueryJobConfig] = None) -> QueryResult:
        rendered = self.render_sql(sql, extra=extra)
        # print(f"Running SQL:\n{rendered}\n")
        # return
        job = self.client.query(rendered, job_config=job_config)
        job.result() # Wait for completion
        stats = job._properties.get("statistics", {}).get("query", {})
        billed = int(stats.get("totalBytesBilled", 0))
        processed = int(stats.get("totalBytesProcessed", 0))
        return QueryResult(
            job_id=job.job_id,
            bytes_processed=processed,
            bytes_billed=billed,
        )