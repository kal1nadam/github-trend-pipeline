from __future__ import annotations

from datetime import datetime, timedelta, timezone
import subprocess
import sys

def yesterday_utc_iso() -> str:
    d = datetime.now(timezone.utc).date() - timedelta(days=1)
    return d.isoformat()

def main() -> None:
    date_str = yesterday_utc_iso()
    print(f"Running daily pipeline for date: {date_str}")

    cmds = [
        [sys.executable, "-m", "pipeline.extract", "--date", date_str],
        [sys.executable, "-m", "pipeline.transform"],
        [sys.executable, "-m", "pipeline.compute", "--date", date_str],
    ]

    for cmd in cmds:
        print(f"\nExecuting: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    print("\nDaily pipeline run completed.")

if __name__ == "__main__":
    main()