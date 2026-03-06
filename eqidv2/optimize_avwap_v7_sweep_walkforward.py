# -*- coding: utf-8 -*-
"""
Walk-forward side-weighted optimizer for AVWAP V7 sweep.

What it does:
1) Runs multiple parameter scenarios over full data (SHORT + LONG).
2) Scores each scenario with a side-weighted objective.
3) Performs walk-forward model selection:
   - pick best scenario on train window
   - evaluate selected scenario on next test window
4) Saves leaderboard + fold-by-fold picks.

This is an optimizer utility; it does not alter live strategy files.

Examples:
    python optimize_avwap_v7_sweep_walkforward.py
    python optimize_avwap_v7_sweep_walkforward.py --max-workers 4 --day-loss-guard -3.0
    python optimize_avwap_v7_sweep_walkforward.py --dry-run
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from avwap_combined_runner_v7_sweep import (
    _add_notional_pnl,
    _apply_day_loss_guard,
    _run_side_parallel,
)
from avwap_v11_refactored.avwap_common_v7_sweep import (
    StrategyConfig,
    build_market_regime_map,
    compute_backtest_metrics,
    default_long_config,
    default_short_config,
)


@dataclass
class ScenarioSpec:
    name: str
    short_overrides: Dict[str, Any]
    long_overrides: Dict[str, Any]


def _default_scenarios() -> List[ScenarioSpec]:
    """
    Curated scenario set (kept intentionally compact for practical runtime).
    """
    return [
        ScenarioSpec("baseline", {}, {}),
        ScenarioSpec(
            "tight_stops_guard_m3",
            {"stop_pct": 0.0068, "target_pct": 0.0090},
            {"stop_pct": 0.0058, "target_pct": 0.0110},
        ),
        ScenarioSpec(
            "freq_long_1",
            {},
            {
                "enable_setup_a_close_continuation_break": True,
                "lag_bars_long_a_close_continuation_break": 1,
            },
        ),
        ScenarioSpec(
            "freq_long_2",
            {},
            {
                "enable_setup_a_close_continuation_break": True,
                "lag_bars_long_a_close_continuation_break": 1,
                "enable_setup_b_huge_c1_close_reclaim_break": True,
                "lag_bars_long_b_huge_c1_close_reclaim_break": 2,
            },
        ),
        ScenarioSpec(
            "freq_balanced_1",
            {"target_pct": 0.0088, "stop_pct": 0.0070},
            {
                "target_pct": 0.0115,
                "stop_pct": 0.0058,
                "enable_setup_a_close_continuation_break": True,
            },
        ),
        ScenarioSpec(
            "freq_balanced_2",
            {"target_pct": 0.0092, "stop_pct": 0.0068},
            {
                "target_pct": 0.0120,
                "stop_pct": 0.0058,
                "enable_setup_a_close_continuation_break": True,
                "enable_setup_b_huge_c1_close_reclaim_break": True,
            },
        ),
        ScenarioSpec(
            "quality_low_dd_1",
            {"target_pct": 0.0090, "stop_pct": 0.0065, "trail_pct": 0.0028},
            {"target_pct": 0.0110, "stop_pct": 0.0055, "trail_pct": 0.0028},
        ),
        ScenarioSpec(
            "quality_low_dd_2",
            {"target_pct": 0.0090, "stop_pct": 0.0063, "be_trigger_pct": 0.0045},
            {
                "target_pct": 0.0110,
                "stop_pct": 0.0053,
                "be_trigger_pct": 0.0055,
                "enable_setup_a_close_continuation_break": True,
            },
        ),
        ScenarioSpec(
            "long_bias_1",
            {"target_pct": 0.0090, "stop_pct": 0.0068},
            {
                "target_pct": 0.0122,
                "stop_pct": 0.0056,
                "enable_setup_a_close_continuation_break": True,
                "enable_setup_b_huge_c1_close_reclaim_break": True,
            },
        ),
        ScenarioSpec(
            "long_bias_2",
            {"target_pct": 0.0089, "stop_pct": 0.0068},
            {
                "target_pct": 0.0120,
                "stop_pct": 0.0054,
                "enable_setup_a_close_continuation_break": True,
                "lag_bars_long_a_close_continuation_break": 0,
                "enable_setup_b_huge_c1_close_reclaim_break": True,
                "lag_bars_long_b_huge_c1_close_reclaim_break": 1,
            },
        ),
        ScenarioSpec(
            "short_bias_1",
            {"target_pct": 0.0095, "stop_pct": 0.0068, "be_trigger_pct": 0.0055},
            {"target_pct": 0.0110, "stop_pct": 0.0059},
        ),
    ]


def _apply_overrides(cfg: StrategyConfig, overrides: Dict[str, Any]) -> StrategyConfig:
    out = cfg
    for k, v in (overrides or {}).items():
        if not hasattr(out, k):
            raise ValueError(f"Unknown StrategyConfig override: {k}")
        setattr(out, k, v)
    return out


def _score_trades(
    df: pd.DataFrame,
    short_weight: float,
    long_weight: float,
    target_min_trades_per_day: float,
    target_max_trades_per_day: float,
    dd_weight: float = 0.12,
    pf_weight: float = 1.25,
    pnl_weight: float = 0.06,
    daily_win_weight: float = 1.00,
) -> Dict[str, float]:
    if df.empty:
        return {
            "score": -1e9,
            "trades_per_day": 0.0,
            "short_trades_per_day": 0.0,
            "long_trades_per_day": 0.0,
            "sum_pnl_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "daily_win_pct": 0.0,
            "short_sum_pnl_pct": 0.0,
            "long_sum_pnl_pct": 0.0,
            "notional_pnl_rs": 0.0,
        }

    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    d = d.dropna(subset=["trade_date"]).copy()
    if d.empty:
        return {
            "score": -1e9,
            "trades_per_day": 0.0,
            "short_trades_per_day": 0.0,
            "long_trades_per_day": 0.0,
            "sum_pnl_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "daily_win_pct": 0.0,
            "short_sum_pnl_pct": 0.0,
            "long_sum_pnl_pct": 0.0,
            "notional_pnl_rs": 0.0,
        }

    mt = compute_backtest_metrics(d)
    short_df = d[d["side"].astype(str).str.upper().eq("SHORT")].copy()
    long_df = d[d["side"].astype(str).str.upper().eq("LONG")].copy()
    short_m = compute_backtest_metrics(short_df) if not short_df.empty else None
    long_m = compute_backtest_metrics(long_df) if not long_df.empty else None

    unique_days = max(1, int(mt.unique_days))
    trades_per_day = float(mt.total_trades) / float(unique_days)
    short_tpd = float(len(short_df)) / float(unique_days)
    long_tpd = float(len(long_df)) / float(unique_days)

    daily = d.groupby("trade_date", as_index=False)["pnl_pct"].sum()
    daily_win_pct = float((daily["pnl_pct"] > 0).mean() * 100.0) if not daily.empty else 0.0

    short_sum = float(short_m.sum_pnl_pct) if short_m else 0.0
    long_sum = float(long_m.sum_pnl_pct) if long_m else 0.0
    pf = float(mt.profit_factor if np.isfinite(mt.profit_factor) else 5.0)
    pf = float(np.clip(pf, 0.0, 5.0))
    dd = float(max(0.0, mt.max_drawdown_pct))
    sum_pnl = float(mt.sum_pnl_pct)

    penalty = 0.0
    if trades_per_day < target_min_trades_per_day:
        penalty += float(target_min_trades_per_day - trades_per_day) ** 2
    elif trades_per_day > target_max_trades_per_day:
        penalty += float(trades_per_day - target_max_trades_per_day) ** 2

    side_weighted_pnl = (short_weight * short_sum) + (long_weight * long_sum)
    score = (
        (pnl_weight * sum_pnl)
        + (pf_weight * pf)
        + (daily_win_weight * (daily_win_pct / 100.0))
        + (0.08 * side_weighted_pnl)
        - (dd_weight * dd)
        - (0.50 * penalty)
    )

    return {
        "score": float(score),
        "trades_per_day": trades_per_day,
        "short_trades_per_day": short_tpd,
        "long_trades_per_day": long_tpd,
        "sum_pnl_pct": sum_pnl,
        "profit_factor": float(mt.profit_factor),
        "max_drawdown_pct": dd,
        "daily_win_pct": daily_win_pct,
        "short_sum_pnl_pct": short_sum,
        "long_sum_pnl_pct": long_sum,
        "notional_pnl_rs": float(pd.to_numeric(d.get("pnl_rs", 0.0), errors="coerce").fillna(0.0).sum()),
    }


def _scenario_trade_df(
    sc: ScenarioSpec,
    max_workers: int,
    enable_day_loss_guard: bool,
    day_loss_guard_pct: float,
    enable_regime_filter: bool,
    regime_allow_neutral: bool,
) -> pd.DataFrame:
    short_cfg = default_short_config()
    long_cfg = default_long_config()
    short_cfg = _apply_overrides(short_cfg, sc.short_overrides)
    long_cfg = _apply_overrides(long_cfg, sc.long_overrides)

    if enable_regime_filter:
        regime_map, regime_src = build_market_regime_map(short_cfg)
        if regime_map:
            short_cfg.market_regime_map = regime_map
            long_cfg.market_regime_map = regime_map
            short_cfg.enable_market_regime_filter = True
            long_cfg.enable_market_regime_filter = True
            short_cfg.market_regime_allow_neutral = bool(regime_allow_neutral)
            long_cfg.market_regime_allow_neutral = bool(regime_allow_neutral)
            print(f"[{sc.name}] regime enabled from {regime_src} | bars={len(regime_map)}")
        else:
            print(f"[{sc.name}] regime requested but index parquet missing; running without regime filter.")

    short_df = _run_side_parallel("SHORT", short_cfg, max_workers=max_workers)
    long_df = _run_side_parallel("LONG", long_cfg, max_workers=max_workers)
    combined = pd.concat([short_df, long_df], ignore_index=True) if (not short_df.empty or not long_df.empty) else pd.DataFrame()
    if combined.empty:
        return combined

    if enable_day_loss_guard:
        combined, guard_stats = _apply_day_loss_guard(combined, float(day_loss_guard_pct))
        print(
            f"[{sc.name}] day-loss-guard threshold={day_loss_guard_pct:.2f}% | "
            f"blocked_trades={guard_stats.get('blocked_trades', 0)} "
            f"blocked_days={guard_stats.get('blocked_days', 0)}"
        )
    combined = _add_notional_pnl(combined)
    combined["trade_date"] = pd.to_datetime(combined.get("trade_date"), errors="coerce").dt.date
    combined = combined.dropna(subset=["trade_date"]).copy()
    return combined


def _slice_by_days(df: pd.DataFrame, days_set: set[date]) -> pd.DataFrame:
    if df.empty or not days_set:
        return pd.DataFrame(columns=df.columns if not df.empty else None)
    return df[df["trade_date"].isin(days_set)].copy()


def _iter_walkforward_splits(days: List[date], train_days: int, test_days: int) -> Iterable[Tuple[int, set[date], set[date]]]:
    fold = 0
    i = 0
    n = len(days)
    while i + train_days + test_days <= n:
        fold += 1
        tr = set(days[i : i + train_days])
        te = set(days[i + train_days : i + train_days + test_days])
        yield fold, tr, te
        i += test_days


def main() -> int:
    ap = argparse.ArgumentParser(description="Walk-forward side-weighted optimizer for AVWAP V7 sweep.")
    ap.add_argument("--out-dir", type=str, default="outputs", help="Output directory for CSV/JSON artifacts.")
    ap.add_argument("--max-workers", type=int, default=1, help="Workers per side scan. Use 1 for stable local runs.")
    ap.add_argument("--train-days", type=int, default=45, help="Walk-forward train window size.")
    ap.add_argument("--test-days", type=int, default=10, help="Walk-forward test window size.")
    ap.add_argument("--short-weight", type=float, default=1.0, help="Objective weight for SHORT side.")
    ap.add_argument("--long-weight", type=float, default=1.0, help="Objective weight for LONG side.")
    ap.add_argument("--target-min-trades-per-day", type=float, default=5.0, help="Lower target for trades/day penalty.")
    ap.add_argument("--target-max-trades-per-day", type=float, default=15.0, help="Upper target for trades/day penalty.")
    ap.add_argument("--enable-day-loss-guard", action="store_true", help="Apply day-loss guard after merge.")
    ap.add_argument("--day-loss-guard", type=float, default=-3.0, help="Day-loss threshold pct if enabled.")
    ap.add_argument("--enable-regime-filter", action="store_true", help="Enable regime map if index parquet exists.")
    ap.add_argument("--regime-allow-neutral", action="store_true", help="Allow neutral bars in regime gate.")
    ap.add_argument("--scenarios-json", type=str, default="", help="Optional custom scenarios JSON.")
    ap.add_argument("--dry-run", action="store_true", help="Only print resolved scenarios and exit.")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.scenarios_json:
        path = Path(args.scenarios_json).expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        scenarios = [
            ScenarioSpec(
                name=str(x["name"]),
                short_overrides=dict(x.get("short_overrides", {})),
                long_overrides=dict(x.get("long_overrides", {})),
            )
            for x in payload
        ]
    else:
        scenarios = _default_scenarios()

    if args.dry_run:
        print(f"Scenarios loaded: {len(scenarios)}")
        for s in scenarios:
            print(f"- {s.name}")
        return 0

    print(f"[INFO] Running {len(scenarios)} scenarios | max_workers={args.max_workers}")

    scenario_trades: Dict[str, pd.DataFrame] = {}
    leaderboard_rows: List[Dict[str, Any]] = []

    for idx, sc in enumerate(scenarios, start=1):
        print(f"\n=== Scenario {idx}/{len(scenarios)}: {sc.name} ===")
        df = _scenario_trade_df(
            sc=sc,
            max_workers=int(args.max_workers),
            enable_day_loss_guard=bool(args.enable_day_loss_guard),
            day_loss_guard_pct=float(args.day_loss_guard),
            enable_regime_filter=bool(args.enable_regime_filter),
            regime_allow_neutral=bool(args.regime_allow_neutral),
        )
        scenario_trades[sc.name] = df
        scored = _score_trades(
            df=df,
            short_weight=float(args.short_weight),
            long_weight=float(args.long_weight),
            target_min_trades_per_day=float(args.target_min_trades_per_day),
            target_max_trades_per_day=float(args.target_max_trades_per_day),
        )
        leaderboard_rows.append({"scenario": sc.name, **scored})
        print(
            f"[{sc.name}] trades={len(df)} tpd={scored['trades_per_day']:.2f} "
            f"pf={scored['profit_factor']:.3f} dd={scored['max_drawdown_pct']:.2f} "
            f"sum_pnl={scored['sum_pnl_pct']:.2f} score={scored['score']:.4f}"
        )

    leaderboard = pd.DataFrame(leaderboard_rows).sort_values("score", ascending=False).reset_index(drop=True)
    leaderboard_csv = out_dir / "v7_sweep_walkforward_leaderboard.csv"
    leaderboard.to_csv(leaderboard_csv, index=False)
    print(f"\n[OK] Leaderboard saved: {leaderboard_csv}")

    # Walk-forward selection
    all_days = sorted({d for df in scenario_trades.values() for d in df.get("trade_date", pd.Series(dtype="object")).dropna().tolist()})
    if not all_days:
        print("[WARN] No trades produced by any scenario.")
        return 0

    folds: List[Dict[str, Any]] = []
    wf_selected_test_parts: List[pd.DataFrame] = []
    for fold, train_days_set, test_days_set in _iter_walkforward_splits(
        all_days, int(args.train_days), int(args.test_days)
    ):
        train_rows: List[Dict[str, Any]] = []
        for sc in scenarios:
            sdf = scenario_trades.get(sc.name, pd.DataFrame())
            train_df = _slice_by_days(sdf, train_days_set)
            score = _score_trades(
                df=train_df,
                short_weight=float(args.short_weight),
                long_weight=float(args.long_weight),
                target_min_trades_per_day=float(args.target_min_trades_per_day),
                target_max_trades_per_day=float(args.target_max_trades_per_day),
            )
            train_rows.append({"scenario": sc.name, **score})

        train_rank = pd.DataFrame(train_rows).sort_values("score", ascending=False).reset_index(drop=True)
        best_scenario = str(train_rank.iloc[0]["scenario"])

        best_test_df = _slice_by_days(scenario_trades[best_scenario], test_days_set)
        best_test_score = _score_trades(
            df=best_test_df,
            short_weight=float(args.short_weight),
            long_weight=float(args.long_weight),
            target_min_trades_per_day=float(args.target_min_trades_per_day),
            target_max_trades_per_day=float(args.target_max_trades_per_day),
        )
        wf_selected_test_parts.append(best_test_df)

        folds.append(
            {
                "fold": fold,
                "train_start": str(min(train_days_set)),
                "train_end": str(max(train_days_set)),
                "test_start": str(min(test_days_set)),
                "test_end": str(max(test_days_set)),
                "picked_scenario": best_scenario,
                "test_score": best_test_score["score"],
                "test_sum_pnl_pct": best_test_score["sum_pnl_pct"],
                "test_pf": best_test_score["profit_factor"],
                "test_dd_pct": best_test_score["max_drawdown_pct"],
                "test_trades_per_day": best_test_score["trades_per_day"],
                "test_daily_win_pct": best_test_score["daily_win_pct"],
                "test_notional_pnl_rs": best_test_score["notional_pnl_rs"],
            }
        )

    folds_df = pd.DataFrame(folds)
    folds_csv = out_dir / "v7_sweep_walkforward_folds.csv"
    folds_df.to_csv(folds_csv, index=False)
    print(f"[OK] Walk-forward folds saved: {folds_csv}")

    wf_test_all = pd.concat(wf_selected_test_parts, ignore_index=True) if wf_selected_test_parts else pd.DataFrame()
    wf_summary = _score_trades(
        df=wf_test_all,
        short_weight=float(args.short_weight),
        long_weight=float(args.long_weight),
        target_min_trades_per_day=float(args.target_min_trades_per_day),
        target_max_trades_per_day=float(args.target_max_trades_per_day),
    )
    summary = {
        "scenarios": len(scenarios),
        "folds": int(len(folds_df)),
        "max_workers": int(args.max_workers),
        "train_days": int(args.train_days),
        "test_days": int(args.test_days),
        "short_weight": float(args.short_weight),
        "long_weight": float(args.long_weight),
        "target_min_trades_per_day": float(args.target_min_trades_per_day),
        "target_max_trades_per_day": float(args.target_max_trades_per_day),
        "enable_day_loss_guard": bool(args.enable_day_loss_guard),
        "day_loss_guard_pct": float(args.day_loss_guard),
        "enable_regime_filter": bool(args.enable_regime_filter),
        "wf_test_summary": wf_summary,
    }
    summary_path = out_dir / "v7_sweep_walkforward_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[OK] Summary saved: {summary_path}")

    print("\nTop scenarios (full-period objective):")
    print(leaderboard.head(10).to_string(index=False))
    print("\nWalk-forward selected-test summary:")
    print(json.dumps(wf_summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
