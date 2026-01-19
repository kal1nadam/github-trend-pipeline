

import argparse
from pathlib import Path

from pipeline.config import Settings
from pipeline.bq import BQQueryRunner


SQL_ROOT = Path(__file__).resolve().parents[1] / "sql"
SETUP_DIR = SQL_ROOT / "00_setup"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="If set, setup SQL files will be printed but not executed.")
    args = parser.parse_args()

    settings = Settings.load()
    bq = BQQueryRunner(settings)

    files = sorted(SETUP_DIR.rglob("*.sql"))
    if not files:
        raise SystemExit(f"No setup SQL files found under: {SETUP_DIR}")
    
    print("Setup SQL files to be executed:")
    for file in files:
        print(f" - {file.relative_to(SQL_ROOT)}")

    if args.dry_run:
        return
    
    for file in files:
        sql = file.read_text(encoding="utf-8").strip()
        if not sql:
            continue
        print(f"\nRunning: {file.relative_to(SQL_ROOT)}")
        res = bq.run(sql)
        print(f"\nCompleted: {file.relative_to(SQL_ROOT)}")


if __name__ == "__main__":
    main()