from __future__ import annotations

import argparse
from datetime import date

from google.cloud import bigquery

from pipeline.config import Settings
from pipeline.bq import BQQueryRunner

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

# Query hard coded as it is part of the application logic
EXTRACT_SQL = """
-- Extract a single day from GH Archive into the raw table.
DECLARE target_date DATE DEFAULT DATE("${DATE}");

INSERT INTO `${RAW_DATASET}.events` (event_date, created_at, type, repo_name, actor_login, payload)
SELECT
  target_date AS event_date,
  created_at,
  type,
  repo.name AS repo_name,
  actor.login AS actor_login,
  TO_JSON(payload) AS payload
FROM `${SOURCE_PROJECT}.${SOURCE_DATASET}.${SOURCE_TABLE}`
WHERE DATE(created_at) = target_date;
"""

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    args = parser.parse_args()

    settings = Settings.load()
    bq_runner = BQQueryRunner(settings)

    d = date.fromisoformat(args.date)
    # Target github archive table
    src_table = yyyymmdd(d)

    sql = (EXTRACT_SQL
           .replace("${DATE}", args.date)
           .replace("${SOURCE_PROJECT}", settings.source_events_project)
           .replace("${SOURCE_DATASET}", settings.source_events_dataset)
           .replace("${SOURCE_TABLE}", src_table))
    
    # User job config with labels - helps with cost tracking
    job_config = bigquery.QueryJobConfig(labels={"project": "github-trend-pipeline", "step": "extract"})

    print(f"Extracting {args.date} from `githubarchive.day.{src_table}` into `{settings.raw_dataset}.events` ...")
    res = bq_runner.run(sql, job_config=job_config)
    print(f"Done. job_id={res.job_id} processed={res.bytes_processed} billed={res.bytes_billed}")

if __name__ == "__main__":
    main()