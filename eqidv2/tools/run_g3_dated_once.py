"""Wait for a completed daily G source, then run one research-only G-3 replay."""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research.g3_dated_replay import run


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--deadline-hour", type=int, default=20)
    args = parser.parse_args()
    status_path = args.output_dir.parent / "g3_dated_run_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = datetime.combine(args.date, datetime.min.time(), ZoneInfo("Asia/Kolkata"))
    deadline = deadline.replace(hour=args.deadline_hour)
    while datetime.now(ZoneInfo("Asia/Kolkata")) < deadline:
        try:
            result = run(args.date, args.output_dir)
        except (FileNotFoundError, RuntimeError) as exc:
            # The G scheduler and post-close data refresh can still be running.
            # Record the current state, then retry without altering either job.
            status_path.write_text(json.dumps({"state": "WAITING_FOR_G_SOURCE", "date": str(args.date),
                                               "last_error": str(exc), "checked_at": datetime.now().isoformat()},
                                              indent=2), encoding="utf-8")
            time.sleep(60)
            continue
        except Exception as exc:
            status_path.write_text(json.dumps({"state": "FAILED", "date": str(args.date),
                                               "error_type": type(exc).__name__, "error": str(exc),
                                               "checked_at": datetime.now().isoformat()}, indent=2), encoding="utf-8")
            raise
        status_path.write_text(json.dumps({"state": "SUCCESS", "date": str(args.date),
                                           "summary": str(args.output_dir / "summary.json"),
                                           "completed_at": datetime.now().isoformat()}, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2, default=str))
        return 0
    status_path.write_text(json.dumps({"state": "FAILED_DEADLINE", "date": str(args.date),
                                       "deadline": deadline.isoformat()}, indent=2), encoding="utf-8")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
