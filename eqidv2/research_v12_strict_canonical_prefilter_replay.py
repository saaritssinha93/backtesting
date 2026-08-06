"""Strict research wrapper for the canonical hourly K300 prefilter replay.

It delegates ranking/output logic to ``research_prefilter_multi_day_replay``
while enforcing two properties that its recall-oriented defaults do not:

* current bars flagged gap/partial by the canonical builder are removed;
* maximum eligible staleness is zero seconds.

No production module or data source is mutated.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from experimental_prefilter.config import PrefilterConfig as BasePrefilterConfig
import research_prefilter_multi_day_replay as replay


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


_load_range_once = replay.load_range_once


def _strict_load_range_once(*args, **kwargs):
    bars, stats = _load_range_once(*args, **kwargs)
    gap = pd.to_numeric(bars.get("gap_filled", 0), errors="coerce").fillna(1.0)
    before = len(bars)
    bars = bars.loc[gap.lt(0.5)].copy()
    stats = dict(stats)
    stats["strict_rows_before_gap_filter"] = int(before)
    stats["strict_gap_or_partial_rows_removed"] = int(before - len(bars))
    stats["rows"] = int(len(bars))
    stats["strict_current_bar_max_staleness_seconds"] = 0.0
    return bars, stats


def _strict_config() -> BasePrefilterConfig:
    return BasePrefilterConfig(max_staleness_seconds=0.0)


def _validate_output(output_dir: Path, start_date: str, end_date: str, budget: int) -> dict:
    paths = sorted(output_dir.glob(f"hourly_candidates_*_k{budget}.csv"))
    if len(paths) != 1:
        raise RuntimeError(f"expected one combined candidate file, found {len(paths)}")
    path = paths[0]
    frame = pd.read_csv(path)
    frame["slot"] = pd.to_datetime(frame["slot_ist"], utc=True).dt.tz_convert("Asia/Kolkata")
    frame["bar"] = pd.to_datetime(frame["date"], utc=True).dt.tz_convert("Asia/Kolkata")
    stale = pd.to_numeric(frame["staleness_seconds"], errors="coerce")
    if not frame["slot"].eq(frame["bar"]).all() or not stale.eq(0.0).all():
        raise RuntimeError("strict canonical output contains stale/non-slot selections")
    groups = frame.groupby("slot", sort=True)
    counts = groups.size()
    unique_tickers = groups["ticker"].nunique()
    rank_sets_valid = all(
        set(pd.to_numeric(group["selection_rank"], errors="raise").astype(int))
        == set(range(1, budget + 1))
        for _, group in groups
    )
    if not counts.eq(budget).all() or not unique_tickers.eq(budget).all() or not rank_sets_valid:
        raise RuntimeError("strict canonical K-budget/rank/uniqueness validation failed")
    dates = sorted(frame["slot"].dt.strftime("%Y-%m-%d").unique())
    expected_slots = len(dates) * 7
    if len(counts) != expected_slots:
        raise RuntimeError(f"slot schedule incomplete: {len(counts)} != {expected_slots}")
    contract = {
        "research_only": True,
        "start_date": start_date,
        "end_date": end_date,
        "sessions": len(dates),
        "session_dates": dates,
        "slots": len(counts),
        "budget": budget,
        "rows": len(frame),
        "all_selected_bars_exact_slot": True,
        "all_selected_staleness_seconds_zero": True,
        "all_slots_exact_budget_unique_tickers_and_ranks": True,
        "combined_candidates": str(path.resolve()),
        "combined_candidates_sha256": _sha256(path),
    }
    (output_dir / "strict_contract.json").write_text(
        json.dumps(contract, indent=2), encoding="utf-8"
    )
    return contract


def main() -> int:
    parser = replay.build_parser()
    args = parser.parse_args()
    replay.PrefilterConfig = _strict_config
    replay.load_range_once = _strict_load_range_once
    result = replay.run(args)
    contract = _validate_output(
        Path(args.output_dir), args.start_date, args.end_date, int(args.budget)
    )
    print(json.dumps({"strict_contract": contract}, indent=2), flush=True)
    return int(result)


if __name__ == "__main__":
    raise SystemExit(main())
