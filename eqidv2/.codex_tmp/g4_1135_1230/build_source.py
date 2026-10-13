"""Temporary, guarded source build for the G4 11:35--12:30 research pass."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from research.g4_late_slot_source import load_late_source

CLOCKS = ("11:35", "11:40", "11:45", "11:50", "11:55", "12:00", "12:05", "12:10", "12:15", "12:20", "12:25", "12:30")
CACHE = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\source_cache\1135_1230_v4")


def main():
    candidates, paths, days, evidence, snapshots = load_late_source(
        CACHE, clocks=CLOCKS, sid_base=6_000_000, verify_snapshots=True)
    counts = candidates.groupby(["hhmm_int", "side"]).size().rename("candidates").reset_index()
    destination = Path(__file__).resolve().parent
    candidates.to_parquet(destination / "candidates.parquet", index=False)
    counts.to_csv(destination / "source_counts.csv", index=False)
    summary = {
        "cache": str(CACHE), "clocks": CLOCKS, "sid_base": 6_000_000,
        "candidate_count": len(candidates), "path_count": len(paths),
        "session_count": len(days), "start_day": str(min(days)), "end_day": str(max(days)),
        "days": [str(day) for day in days],
        "candidate_days": sorted(str(day) for day in candidates.day.unique()),
        "counts_by_clock_and_side": counts.to_dict("records"),
        "snapshot_count": len(snapshots), "evidence": evidence,
    }
    (destination / "source_summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in summary.items() if key not in ("evidence", "candidate_days", "days")}, indent=2), flush=True)


if __name__ == "__main__":
    main()
