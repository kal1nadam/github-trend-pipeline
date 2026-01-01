from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.config import Settings
from pipeline.bq import BQQueryRunner

SQL_ROOT = Path(__file__).resolve().parents[1] / "sql"
ORDERED_DIRS = ["00_setup", "10_staging", "20_models", "30_marts"]

def iter_sql_files() -> list[Path]:
    files: list[Path] = []
    for dir_name in ORDERED_DIRS:
        folder = SQL_ROOT / dir_name
        if not folder.exists():
            continue
        files.extend(sorted(folder.rglob("*.sql")))
    return files

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, SQL files will be printed but not executed.",
    )
    args = parser.parse_args()

    settings = Settings.load()
    bq_runner = BQQueryRunner(settings)

    sql_files = iter_sql_files()
    if not sql_files:
        raise SystemExit("No SQL files found to execute.")
    
    print("SQL execution order:")
    for sql_file in sql_files:
        print(f" - {sql_file.relative_to(SQL_ROOT)}")

    if args.dry_run:
        print("\nDry run mode - SQL files will not be executed.")
        return
    
    for sql_file in sql_files:
        sql = sql_file.read_text(encoding="utf-8")
        if not sql.strip():
            print(f"Skipping empty SQL file: {sql_file.relative_to(SQL_ROOT)}")
            continue
        
        print(f"\nRunning: {sql_file.relative_to(SQL_ROOT)}")
        res = bq_runner.run(sql)
        print(f"\nCompleted: {sql_file.relative_to(SQL_ROOT)}")



if __name__ == "__main__":
    main()