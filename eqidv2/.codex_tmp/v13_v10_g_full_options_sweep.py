from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import fno_v13_v10_g_options_backtest as bt


SOURCE = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g\options_3lots_5min_20260914")
OUTPUT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g\sl_target_sweep_full_20260915")


def finite_pf(value: float | None) -> float:
    if value is None:
        return 0.0
    if math.isinf(value):
        return 999.0
    return float(value)


def main() -> None:
    inputs = SOURCE / "frozen_input"
    spec = json.loads((SOURCE / "research_spec.json").read_text(encoding="utf-8"))
    mapped = pd.read_parquet(inputs / "mapped.parquet")
    paths = {p.stem: pd.read_parquet(p) for p in (inputs / "paths").glob("*.parquet")}
    setup_ids = mapped.setup_id.unique()
    cutoff = spec["train_cutoff"]
    rows: list[dict] = []
    pairs = [(s / 100, t / 100) for s in range(2, 51) for t in range(1, 101)]
    for index, pair in enumerate(pairs, 1):
        policy = {"default": pair, **{setup: pair for setup in setup_ids}}
        frame, _, _, _ = bt.replay(
            mapped,
            paths,
            policy,
            capital=spec["capital"],
            slippage_bps=spec["slippage_bps"],
            participation=spec["previous_bar_participation"],
        )
        periods = {
            "full": frame,
            "train": frame.loc[frame.day.le(cutoff)],
            "later": frame.loc[frame.day.gt(cutoff)],
            "dated": frame.loc[frame.mapping_status.eq("MAPPED_CAUSAL")],
        }
        row = {"stop_pct": pair[0] * 100, "target_pct": pair[1] * 100}
        for label, sub in periods.items():
            m = bt.metrics(sub)
            row.update(
                {
                    f"{label}_net_pnl": m["net_pnl"],
                    f"{label}_win_rate_pct": m["win_rate_pct"],
                    f"{label}_profit_factor": finite_pf(m["profit_factor"]),
                    f"{label}_drawdown": m["daily_realized_drawdown"],
                    f"{label}_wins": m["wins"],
                    f"{label}_losses": m["losses"],
                    f"{label}_closed": m["closed"],
                }
            )
        rows.append(row)
        if index % 250 == 0 or index == len(pairs):
            print(f"[SWEEP] {index}/{len(pairs)}", flush=True)

    result = pd.DataFrame(rows)
    indexed = result.set_index(["stop_pct", "target_pct"])
    neighbor_means = []
    neighbor_mins = []
    neighbor_std = []
    for row in result.itertuples(index=False):
        neighbors = indexed.loc[
            (slice(max(2, row.stop_pct - 2), min(50, row.stop_pct + 2)),
             slice(max(1, row.target_pct - 2), min(100, row.target_pct + 2))),
            "full_net_pnl",
        ]
        neighbor_means.append(float(neighbors.mean()))
        neighbor_mins.append(float(neighbors.min()))
        neighbor_std.append(float(neighbors.std(ddof=0)))
    result["neighbor_5x5_mean_net"] = neighbor_means
    result["neighbor_5x5_min_net"] = neighbor_mins
    result["neighbor_5x5_std_net"] = neighbor_std

    # A transparent rank blend for discovering balanced candidates. Raw metrics
    # remain in the CSV and the final recommendation is assessed separately.
    result["rank_net"] = result.full_net_pnl.rank(pct=True)
    result["rank_win"] = result.full_win_rate_pct.rank(pct=True)
    result["rank_pf"] = result.full_profit_factor.clip(upper=20).rank(pct=True)
    result["rank_drawdown"] = (-result.full_drawdown).rank(pct=True)
    result["rank_later"] = result.later_net_pnl.rank(pct=True)
    result["rank_robust"] = result.neighbor_5x5_mean_net.rank(pct=True)
    result["balanced_score"] = (
        .30 * result.rank_net
        + .18 * result.rank_win
        + .18 * result.rank_pf
        + .10 * result.rank_drawdown
        + .14 * result.rank_later
        + .10 * result.rank_robust
    )

    OUTPUT.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT / "full_sl_target_sweep_1pct_grid.csv", index=False)
    leaders = pd.concat(
        [
            result.nlargest(25, "full_net_pnl").assign(leader="MAX_NET"),
            result.nlargest(25, "full_win_rate_pct").assign(leader="MAX_WIN_RATE"),
            result.nlargest(25, "full_profit_factor").assign(leader="MAX_PF"),
            result.nlargest(25, "balanced_score").assign(leader="BALANCED"),
            result.nlargest(25, "neighbor_5x5_mean_net").assign(leader="ROBUST_NET"),
        ],
        ignore_index=True,
    ).drop_duplicates(["stop_pct", "target_pct", "leader"])
    leaders.to_csv(OUTPUT / "leaderboards.csv", index=False)
    print(result.nlargest(15, "balanced_score")[
        ["stop_pct", "target_pct", "full_net_pnl", "full_win_rate_pct", "full_profit_factor",
         "full_drawdown", "later_net_pnl", "neighbor_5x5_mean_net", "balanced_score"]
    ].to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
