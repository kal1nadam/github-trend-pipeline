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

    def render_sql(self, sql: str) -> str:
        """
        Very small templating to keep SQL files portable.
        Use placeholders like ${RAW_DATASET}, ${STG_DATASET}, ${MART_DATASET}.
        """
        return (
            sql.replace("${RAW_DATASET}", self.settings.raw_dataset)
            .replace("${STG_DATASET}", self.settings.stg_dataset)
            .replace("${MART_DATASET}", self.settings.mart_dataset)
            .replace("${GCP_PROJECT_ID}", self.settings.gcp_project_id)
        )
    
    def run(self, sql: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> QueryResult:
        rendered = self.render_sql(sql)
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