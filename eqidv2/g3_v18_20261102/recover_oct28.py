"""Recover the missing completed Oct28 source into this new research package only."""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fno_v13_replay_source as source
import fno_v13_v10_g_3_daily_replay as replay


def main() -> None:
    output = Path(__file__).resolve().parent / "sources/2026-10-28"
    if output.exists():
        raise FileExistsError("Recovery refuses to overwrite an existing source run")
    result = replay.replay_day(date(2026, 10, 28), output,
                               roots=source.DataRoots(), snapshot_root=output / "input_snapshots")
    compact = dict(session_date=result["session_date"], state=result["state"],
                   complete=result["complete"], execution_authority=False,
                   blockers=result["coverage"]["blocking_problems"],
                   excluded_stocks=result["coverage"].get("excluded_stocks", []),
                   opening_exclusions=len(result["coverage"].get("opening_0920_exclusions", [])),
                   universe_count=result["coverage"].get("universe_count"),
                   included_stocks=result["coverage"].get("included_stocks"),
                   metrics=result["metrics"],
                   note="Offline reconstruction from existing local raw archives; all new artifacts are isolated; no publisher called.")
    (output / "recovery_status.json").write_text(json.dumps(compact, indent=2), encoding="utf-8")
    print(json.dumps(compact, indent=2), flush=True)
    if not result["complete"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
