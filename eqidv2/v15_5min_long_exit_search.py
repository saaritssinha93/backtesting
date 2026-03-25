from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from v15_5min_sltarget_search import (
    OUT_DIR as BASE_OUT_DIR,
    _build_trade_paths,
    _calc_metrics,
    _latest_v15_csv,
    _load_1m_cache,
    _run_exact_pair,
)


ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "outputs_v15_5min_long_exit_search"

# Current tuned short leg kept fixed while searching long exits.
SHORT_STOP_PCT = 0.0066
SHORT_TARGET_PCT = 0.01075


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Focused v15_5min long-exit search")
    p.add_argument("--validate-top", type=int, default=5, help="Exact-validate top N candidates")
    return p.parse_args()


def _simulate_long_dynamic(
    path: dict,
    stop_pct: float,
    target_pct: float,
    be_trigger_pct: float,
    trail_pct: float,
    *,
    be_pad_pct: float = 0.0001,
    enable_breakeven: bool = True,
    enable_trailing_stop: bool = True,
) -> dict:
    entry = float(path["entry_price"])
    hi = path["high"]
    lo = path["low"]
    close = path["close"]
    times = path["times"]

    sl_curr = entry * (1.0 - stop_pct)
    tgt = entry * (1.0 + target_pct)
    be_armed = False
    be_level = entry * (1.0 + be_pad_pct)
    best_price = entry

    exit_idx = len(close) - 1
    exit_price = float(close[-1])
    outcome = "EOD"

    for idx in range(len(close)):
        bar_hi = float(hi[idx])
        bar_lo = float(lo[idx])

        if np.isfinite(bar_hi) and bar_hi > best_price:
            best_price = bar_hi

        if (
            enable_breakeven
            and not be_armed
            and np.isfinite(bar_hi)
            and bar_hi >= entry * (1.0 + be_trigger_pct)
        ):
            be_armed = True
            sl_curr = max(sl_curr, be_level)

        if be_armed and enable_trailing_stop:
            trail_sl = best_price * (1.0 - trail_pct)
            sl_curr = max(sl_curr, trail_sl)

        hit_sl = np.isfinite(bar_lo) and bar_lo <= sl_curr
        hit_tgt = np.isfinite(bar_hi) and bar_hi >= tgt

        if hit_sl and hit_tgt:
            exit_idx = idx
            exit_price = float(sl_curr)
            outcome = "BE" if be_armed else "SL"
            break
        if hit_sl:
            exit_idx = idx
            exit_price = float(sl_curr)
            outcome = "BE" if be_armed else "SL"
            break
        if hit_tgt:
            exit_idx = idx
            exit_price = float(tgt)
            outcome = "TARGET"
            break

    raw_price_pct = (exit_price - entry) / entry * 100.0 if entry else 0.0
    net_price_pct = raw_price_pct - float(path["cost_pct"])
    leverage = float(path["leverage"])
    pnl_pct = net_price_pct * leverage
    pnl_pct_gross = raw_price_pct * leverage
    pnl_rs = float(path["position_size_rs"]) * pnl_pct / 100.0

    return {
        "ticker": path["ticker"],
        "trade_date": path["trade_date"],
        "entry_time_ist": path["entry_time_ist"],
        "side": "LONG",
        "exit_time_ist": pd.Timestamp(times[exit_idx]),
        "exit_price": exit_price,
        "outcome": outcome,
        "pnl_pct": pnl_pct,
        "pnl_pct_gross": pnl_pct_gross,
        "pnl_rs": pnl_rs,
        "position_size_rs": float(path["position_size_rs"]),
        "leverage": leverage,
        "quality_score": float(path["quality_score"]),
    }


def _score(df: pd.DataFrame, specs: List[tuple[str, bool, float]]) -> pd.DataFrame:
    out = df.copy()
    total = np.zeros(len(out), dtype=float)
    for col, invert, weight in specs:
        s = out[col].astype(float)
        lo, hi = float(s.min()), float(s.max())
        if hi == lo:
            norm = np.full(len(out), 100.0, dtype=float)
        else:
            if invert:
                norm = (hi - s.to_numpy()) / (hi - lo) * 100.0
            else:
                norm = (s.to_numpy() - lo) / (hi - lo) * 100.0
        out[f"norm_{col}"] = np.round(norm, 2)
        total += norm * weight
    out["score"] = np.round(total, 2)
    return out.sort_values(["score", "sum_pnl_pct", "profit_factor", "day_win_pct"], ascending=[False, False, False, False])


def main() -> None:
    args = _parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (BASE_OUT_DIR / "tmp").mkdir(parents=True, exist_ok=True)

    csv_path = _latest_v15_csv()
    df = pd.read_csv(csv_path)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["entry_time_ist"] = pd.to_datetime(df["entry_time_ist"])

    short_df = df[df["side"].astype(str).str.upper().eq("SHORT")].copy()
    long_df = df[df["side"].astype(str).str.upper().eq("LONG")].copy()

    cache = _load_1m_cache(long_df["ticker"].astype(str).unique())
    long_paths = _build_trade_paths(long_df, cache)

    stop_values = [0.0026, 0.0028, 0.0030, 0.0031, 0.0032, 0.0036, 0.0040]
    target_values = [0.0105, 0.01075, 0.0110, 0.01125, 0.0115]
    be_values = [0.0040, 0.0045, 0.0050, 0.0055, 0.0060, 0.0065]
    trail_values = [0.0022, 0.0025, 0.0028, 0.0031, 0.0034]

    rows = []
    exact_short_metrics = _calc_metrics(short_df)
    for stop_pct in stop_values:
        for target_pct in target_values:
            for be_trigger_pct in be_values:
                for trail_pct in trail_values:
                    sim_long = pd.DataFrame(
                        _simulate_long_dynamic(
                            path,
                            stop_pct=stop_pct,
                            target_pct=target_pct,
                            be_trigger_pct=be_trigger_pct,
                            trail_pct=trail_pct,
                        )
                        for path in long_paths
                    )
                    long_metrics = _calc_metrics(sim_long)
                    combined_metrics = _calc_metrics(pd.concat([short_df, sim_long], ignore_index=True))
                    row = {
                        "long_stop_pct": round(stop_pct, 6),
                        "long_target_pct": round(target_pct, 6),
                        "long_be_trigger_pct": round(be_trigger_pct, 6),
                        "long_trail_pct": round(trail_pct, 6),
                        "long_rr": round(target_pct / stop_pct, 3),
                    }
                    row.update({f"long_{k}": v for k, v in long_metrics.items()})
                    row.update({f"combined_{k}": v for k, v in combined_metrics.items()})
                    rows.append(row)

    approx = pd.DataFrame(rows).drop_duplicates(
        subset=["long_stop_pct", "long_target_pct", "long_be_trigger_pct", "long_trail_pct"]
    )
    long_scored = _score(
        approx,
        [
            ("long_sum_pnl_pct", False, 0.25),
            ("long_profit_factor", False, 0.30),
            ("long_day_win_pct", False, 0.20),
            ("long_trade_win_pct", False, 0.10),
            ("long_max_drawdown_pct", True, 0.15),
        ],
    )
    combined_scored = _score(
        approx,
        [
            ("combined_sum_pnl_pct", False, 0.30),
            ("combined_profit_factor", False, 0.25),
            ("combined_day_win_pct", False, 0.20),
            ("combined_trade_win_pct", False, 0.10),
            ("combined_max_drawdown_pct", True, 0.10),
            ("combined_trades_per_day", False, 0.05),
        ],
    )

    long_scored.to_csv(OUT_DIR / "long_exit_candidates_longscore.csv", index=False)
    combined_scored.to_csv(OUT_DIR / "long_exit_candidates_combinedscore.csv", index=False)

    seeds = pd.concat(
        [
            combined_scored.head(max(args.validate_top, 5)),
            combined_scored.nlargest(3, "combined_sum_pnl_pct"),
            combined_scored.nlargest(3, "combined_profit_factor"),
            combined_scored.nlargest(3, "combined_day_win_pct"),
            long_scored.head(3),
        ],
        ignore_index=True,
    ).drop_duplicates(
        subset=["long_stop_pct", "long_target_pct", "long_be_trigger_pct", "long_trail_pct"]
    )

    exact_results = []
    for idx, row in enumerate(seeds.head(args.validate_top).itertuples(index=False), 1):
        label = (
            f"long_rank{idx}_lSL{int(round(row.long_stop_pct * 10000)):04d}"
            f"_lTGT{int(round(row.long_target_pct * 10000)):04d}"
            f"_lBE{int(round(row.long_be_trigger_pct * 10000)):04d}"
            f"_lTR{int(round(row.long_trail_pct * 10000)):04d}"
        )
        exact_results.append(
            _run_exact_pair(
                SHORT_STOP_PCT,
                SHORT_TARGET_PCT,
                float(row.long_stop_pct),
                float(row.long_target_pct),
                label,
                long_be_trigger_pct=float(row.long_be_trigger_pct),
                long_trail_pct=float(row.long_trail_pct),
            )
        )

    summary = {
        "source_v15_5min_csv": str(csv_path),
        "current_short_fixed": {
            "short_stop_pct": SHORT_STOP_PCT,
            "short_target_pct": SHORT_TARGET_PCT,
            "short_metrics": exact_short_metrics,
        },
        "baseline_long": _calc_metrics(long_df),
        "top_long_approx": long_scored.head(15).to_dict(orient="records"),
        "top_combined_approx": combined_scored.head(15).to_dict(orient="records"),
        "exact_validation": exact_results,
    }
    with (OUT_DIR / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
