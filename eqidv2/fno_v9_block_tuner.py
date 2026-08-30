"""Tune V9's untuned slot blocks, one search per block per side.

V9 ships 56 legs on 28 slots that have never been optimised; they all run one
hand-picked loose baseline.  This driver searches thresholds and stop/target
brackets for those slots, treating each *block* as one search rather than each
slot, and reusing the V5/V6 optimizer's protocol wholesale:

  * fitting happens on TRAIN only, ranked by robust PF (PF excluding the single
    best day), so a config that collapses without its luckiest session never
    reaches the shortlist;
  * hard structural guards are applied before ranking (min trades, min day-win
    rate, cap on best-day share of net, min days traded);
  * the TEST window is scored exactly once, after selection is final;
  * a day-block permutation test estimates how often a PF this good arises by
    chance given how many configurations were tried.

Why blocks and not slots
------------------------
Per-slot tuning would run 28 searches over 20-33 fills each -- below the
min-trades guard and a multiple-testing disaster.  Pooling a block gives one
search over 278 (morning) or 466 (afternoon) fills, which clears the guard and
cuts the search surface from 28 to 2 per side.

Transferability constraints
---------------------------
Two axes of the parent optimizer are deliberately pinned so a winner can be
written straight back into a V9 ``SetupSpec``:

  * ``regime`` is pinned to "off".  SetupSpec has no breadth-gate field, so a
    regime-conditional winner could not be expressed in V9 without a schema
    change -- and pinning it also shrinks the search fivefold.
  * brackets are restricted to plain (stop, target) pairs with breakeven and
    trail disabled, because SetupSpec carries only ``stop_pct``/``target_pct``.

The parent optimizer also has no picker/max_entries axis: it takes every
qualifying signal in the window, whereas a V9 leg takes the top ``max_entries``
per day per slot.  A tuned block config therefore describes a *threshold and
bracket*, not a selection rule.  Wiring one into V9 and re-running V9 is the
faithful confirmation, and this module prints that as the required next step
rather than claiming the tuned numbers transfer directly.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_backtest_provenance as provenance
import fno_oi_ema_confirm_optimize as opt
import fno_oi_ema_confirm_sweep as sw
import fno_oi_ema_confirm_v9_extended_slots as v9


TUNER_VERSION = "FNO_V9_BLOCK_TUNER_20260820"
RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v9_block_tuning"
REPORT_PATH = RESULT_DIR / "v9_block_tuning_report.md"

# The two untuned blocks, as inclusive hhmm bounds on the signal slot.
BLOCKS: dict[str, tuple[int, int]] = {
    "MORNING_EXTENDED": (950, 1030),
    "AFTERNOON": (1230, 1400),
}

# Plain (stop, target, breakeven, trail) pairs only.  Breakeven and trail are
# disabled throughout because a V9 SetupSpec cannot express them.
TRANSFERABLE_BRACKETS: dict[str, Any] = {
    "bracket": [
        (0.30, 1.00, 0.0, 0.0),
        (0.40, 1.50, 0.0, 0.0),
        (0.50, 1.50, 0.0, 0.0),
        (0.50, 2.00, 0.0, 0.0),
        (0.60, 2.00, 0.0, 0.0),
        (0.75, 2.00, 0.0, 0.0),
        (0.75, 2.50, 0.0, 0.0),
        (1.00, 1.00, 0.0, 0.0),
        (1.00, 2.50, 0.0, 0.0),
        (1.00, 3.00, 0.0, 0.0),
        (0.50, 2.50, 0.0, 0.0),
    ]
}


def block_grid(window: tuple[int, int]) -> dict[str, list[Any]]:
    """The parent grid with the window pinned and the regime axis disabled."""

    grid = {key: list(values) for key, values in opt.DEFAULT_GRID.items()}
    grid["window"] = [window]
    grid["regime"] = ["off"]
    return grid


def _combo_count(grid: dict[str, list[Any]], brackets: dict[str, Any]) -> int:
    total = 1
    for values in grid.values():
        total *= max(1, len(values))
    return int(total * len(brackets["bracket"]))


def _as_setup_line(block: str, side: str, row: pd.Series) -> str:
    """Render a winner as the V9 baseline dict it would become."""

    return (
        f'    "{side}": {{"picker": "<choose>", "max_entries": <choose>, '
        f'"price_change_pct": {float(row["price_change_pct"]):.2f}, '
        f'"oi_change_pct": {float(row["oi_change_pct"]):.2f}, '
        f'"volume_ratio": {float(row["volume_ratio"]):.1f}, '
        f'"body_ratio": {float(row["body_ratio"]):.2f}, '
        f'"max_wick_ratio": {float(row["max_wick_ratio"]):.2f}, '
        f'"stop_pct": {float(row["stop_pct"]):.2f}, '
        f'"target_pct": {float(row["target_pct"]):.2f}}},  # {block}'
    )


def render_report(results: dict[str, dict[str, Any]], meta: dict[str, Any]) -> str:
    lines = [
        "# V9 Block Tuning",
        "",
        f"- Tuner: `{TUNER_VERSION}`",
        f"- Parent optimizer: `{Path(opt.__file__).name}`",
        f"- Train: {meta['train_from']} to {meta['train_to']} ({meta['n_train']} days)",
        f"- Test: {meta['test_from']} to {meta['test_to']} ({meta['n_test']} days)",
        f"- Cost: {meta['cost_bps']:g} bps",
        f"- Guards: {json.dumps(meta['guards'], sort_keys=True)}",
        f"- Candidates evaluated: {meta['evaluated']:,}",
        "",
        "## Protocol",
        "",
        "Ranked on TRAIN by robust PF (PF excluding the single best day). "
        "Structural guards applied before ranking. TEST scored exactly once "
        "after selection was final. `regime` pinned off and breakeven/trail "
        "disabled so every winner is expressible as a V9 SetupSpec.",
        "",
    ]
    for block, (lo, hi) in BLOCKS.items():
        lines.extend([f"## {block}  (slots {lo}-{hi})", ""])
        for side in ("LONG", "SHORT"):
            entry = results.get(block, {}).get(side, {})
            df = entry.get("shortlist")
            if df is None or df.empty:
                lines.extend(
                    [
                        f"### {side}",
                        "",
                        "**No configuration survived the structural guards on "
                        "TRAIN.** Nothing to score on TEST. This is a clean "
                        "rejection, not a failure to search.",
                        "",
                    ]
                )
                continue
            best = df.iloc[0]
            perm = entry.get("permutation") or {}
            lines.extend(
                [
                    f"### {side}",
                    "",
                    f"- Survivors on TRAIN: {len(df)}",
                    f"- Best TRAIN: PF {float(best['pf']):.3f}, robust PF "
                    f"{float(best['robust_pf']):.3f}, {int(best['trades'])} trades, "
                    f"win {float(best['win_rate']):.1%}, day-win "
                    f"{float(best['day_win_rate']):.1%}, top-day share "
                    f"{float(best['top_day_share']):.1%}, net "
                    f"{float(best['net_sum']):+.2f} pp",
                    f"- Same config on TEST: PF "
                    f"{float(best.get('test_pf', float('nan'))):.3f}, "
                    f"{int(best.get('test_trades', 0))} trades, win "
                    f"{float(best.get('test_win_rate', float('nan'))):.1%}, net "
                    f"{float(best.get('test_net_sum', float('nan'))):+.2f} pp",
                ]
            )
            if perm:
                lines.append(
                    f"- Permutation test on TEST: PF "
                    f"{perm['observed_pf']:.3f} on {perm['n']} trades, "
                    f"**p = {perm['p_value']:.3f}**"
                )
            lines.extend(
                [
                    "",
                    "Winning thresholds (picker and cap still to be chosen):",
                    "",
                    "```python",
                    _as_setup_line(block, side, best),
                    "```",
                    "",
                ]
            )
    lines.extend(
        [
            "## Required next step",
            "",
            "These numbers come from the parent optimizer, which takes every "
            "qualifying signal in the window. A V9 leg instead takes the top "
            "`max_entries` per day per slot, so the tuned PF does **not** "
            "transfer directly. Write a winner into V9's block baseline, "
            "re-run V9, and read the V9 numbers as the real result.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--split-day", default="2026-07-17")
    p.add_argument("--through-day", default="")
    p.add_argument("--cost-bps", type=float, default=15.0)
    p.add_argument("--top-n", type=int, default=25)
    p.add_argument("--min-trades", type=int, default=40)
    p.add_argument("--min-day-win", type=float, default=0.45)
    p.add_argument("--max-top-day-share", type=float, default=0.25)
    p.add_argument("--min-days-traded", type=int, default=20)
    p.add_argument("--permutations", type=int, default=500)
    p.add_argument("--square-off", default="1530")
    p.add_argument("--max-forward-bars", type=int, default=400)
    p.add_argument("--rebuild-cache", action="store_true")
    p.add_argument(
        "--blocks",
        default="all",
        choices=("all", *BLOCKS),
        help="restrict the tuning run to one block",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    signals, paths = opt.load_signals(
        args.square_off,
        args.max_forward_bars,
        args.rebuild_cache,
        universe_path=v9.BACKTEST_UNIVERSE_PATH,
        universe_date=v9.BACKTEST_UNIVERSE_DATE,
        require_persisted_mapping=True,
        require_complete_sources=True,
        expected_universe_hashes=v9.BACKTEST_UNIVERSE_HASHES,
    )
    ctx = sw.build_market_context()
    signals = signals.merge(
        ctx[["day", "hhmm_int", "breadth", "nifty_ret_day"]],
        on=["day", "hhmm_int"],
        how="left",
    )
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    if args.through_day:
        through = pd.Timestamp(args.through_day).date()
        signals = signals.loc[signals["day"].le(through)].copy()

    split = pd.Timestamp(args.split_day).date()
    days = sorted(set(signals["day"]))
    train_days = {d for d in days if d < split}
    test_days = {d for d in days if d >= split}
    print(
        f"[SPLIT] train {len(train_days)} days | test {len(test_days)} days",
        flush=True,
    )
    guards = opt.Guards(
        min_trades=args.min_trades,
        min_day_win=args.min_day_win,
        max_top_day_share=args.max_top_day_share,
        min_days_traded=args.min_days_traded,
    )

    selected_blocks = (
        dict(BLOCKS) if args.blocks == "all" else {args.blocks: BLOCKS[args.blocks]}
    )
    results: dict[str, dict[str, Any]] = {}
    evaluated_total = 0
    for block, window in selected_blocks.items():
        grid = block_grid(window)
        print(
            f"[{block}] window {window} | "
            f"{_combo_count(grid, TRANSFERABLE_BRACKETS):,} candidates per side",
            flush=True,
        )
        results[block] = {}
        for side in ("LONG", "SHORT"):
            shortlist, evaluated = opt.optimise(
                signals,
                paths,
                side=side,
                train_days=train_days,
                test_days=test_days,
                grid=grid,
                brackets=TRANSFERABLE_BRACKETS,
                guards=guards,
                cost_bps=args.cost_bps,
                top_n=args.top_n,
            )
            evaluated_total += evaluated
            perm: dict[str, Any] = {}
            if not shortlist.empty:
                common.atomic_write_csv(
                    shortlist, RESULT_DIR / f"shortlist_{block}_{side}.csv"
                )
                perm = (
                    opt.permutation_test(
                        signals,
                        paths,
                        shortlist.iloc[0],
                        side=side,
                        days=test_days,
                        cost_bps=args.cost_bps,
                        n_iter=args.permutations,
                    )
                    or {}
                )
            results[block][side] = {"shortlist": shortlist, "permutation": perm}
            survivors = 0 if shortlist.empty else len(shortlist)
            print(f"[{block}][{side}] survivors={survivors}", flush=True)

    meta = {
        "train_from": min(train_days) if train_days else None,
        "train_to": max(train_days) if train_days else None,
        "test_from": min(test_days) if test_days else None,
        "test_to": max(test_days) if test_days else None,
        "n_train": len(train_days),
        "n_test": len(test_days),
        "evaluated": evaluated_total,
        "cost_bps": args.cost_bps,
        "guards": asdict(guards),
    }
    common.atomic_write_text(REPORT_PATH, render_report(results, meta))
    print(
        f"[DONE] {time.monotonic() - started:.0f}s | evaluated "
        f"{evaluated_total:,} | {REPORT_PATH}",
        flush=True,
    )
    print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
