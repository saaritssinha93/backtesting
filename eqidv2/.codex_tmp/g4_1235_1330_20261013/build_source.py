"""Immutable offline source build for G4 12:35--13:30 research."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from research.g4_late_slot_source import load_late_source, sha256, _write_paths

CLOCKS = ("12:35", "12:40", "12:45", "12:50", "12:55", "13:00", "13:05", "13:10", "13:15", "13:20", "13:25", "13:30")
CACHE = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\source_cache\1235_1330_v6")


def main():
    candidates, paths, days, evidence, snapshots = load_late_source(
        CACHE, clocks=CLOCKS, sid_base=7_000_000, verify_snapshots=True)
    counts = candidates.groupby(["hhmm_int", "side"]).size().rename("candidates").reset_index()
    destination = Path(__file__).resolve().parent
    candidates.to_parquet(destination / "candidates.parquet", index=False)
    _write_paths(destination / "paths.npz", paths)
    counts.to_csv(destination / "source_counts.csv", index=False)
    summary = {
        "scope": "BACKTEST_ONLY", "execution_authority": False,
        "cache": str(CACHE), "clocks": CLOCKS, "sid_base": 7_000_000,
        "candidate_count": len(candidates), "path_count": len(paths),
        "session_count": len(days), "start_day": str(min(days)), "end_day": str(max(days)),
        "days": [str(day) for day in days],
        "candidate_days": sorted(str(day) for day in candidates.day.unique()),
        "counts_by_clock_and_side": counts.to_dict("records"),
        "snapshot_count": len(snapshots), "evidence": evidence,
        "source_envelope": {"price_change_abs_pct_min": 0.10, "oi_change_pct_min": 0.05,
                            "oi_change_pct_max": 1.0, "five_min_volume_min": 0.8,
                            "confirmation": "EXACT_NEXT_MINUTE_DIRECTION_AND_SIGNAL_CLOSE",
                            "ema": "DIRECTIONAL_FIVE_MINUTE_EMA9_20_50_STACK"},
        "output_sha256": {name: sha256(destination / name) for name in
                          ("candidates.parquet", "paths.npz", "source_counts.csv")},
        "builder_sha256": sha256(Path(__file__)),
        "loader_sha256": sha256(ROOT / "research/g4_late_slot_source.py"),
        "replay_source_sha256": sha256(ROOT / "fno_v13_replay_source.py"),
    }
    (destination / "source_summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in summary.items() if key not in ("evidence", "candidate_days", "days")}, indent=2), flush=True)


if __name__ == "__main__":
    main()
