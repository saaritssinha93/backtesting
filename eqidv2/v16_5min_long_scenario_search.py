from __future__ import annotations

import argparse
import json
import math
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v16_5min as base
import v16_5min_sltarget_search as slsearch


ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "outputs_v16_5min_long_scenario_search"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run exact v16 5min long-only entry/exit scenario search"
    )
    p.add_argument(
        "--scenario-set",
        choices=["quick", "full"],
        default="full",
        help="Scenario breadth to evaluate",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=max(1, int(os.getenv("EQIDV16_5MIN_MAX_WORKERS", "4"))),
        help="Workers passed into the v16 scanner",
    )
    p.add_argument(
        "--top-exit-exact",
        type=int,
        default=2,
        help="Exact-validate top N exit candidates per entry scenario",
    )
    p.add_argument(
        "--keep-top",
        type=int,
        default=12,
        help="Rows to retain in final ranking outputs",
    )
    p.add_argument(
        "--scenario-names",
        default="",
        help="Comma-separated list of scenario names to run (empty = run all)",
    )
    p.add_argument(
        "--merge-existing",
        action="store_true",
        default=True,
        help="Merge new results into existing exact_summary.csv / approx_summary.csv",
    )
    return p.parse_args()


def _profit_factor(pnl: pd.Series) -> float:
    pos = float(pnl[pnl > 0].sum())
    neg = float(-pnl[pnl < 0].sum())
    if neg <= 0:
        return math.inf if pos > 0 else 0.0
    return pos / neg


def _max_drawdown(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = pnl.cumsum()
    dd = equity.cummax() - equity
    return float(dd.max())


def _safe_round(value: float, digits: int = 4) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return 999.0
    return round(float(value), digits)


def _trade_dates_from_data(cfg: base.StrategyConfig) -> List[pd.Timestamp]:
    mode_map, _, _, _ = base._build_nifty_intraday_context(cfg)
    days = sorted(
        {
            pd.Timestamp(key[:10]).normalize()
            for key in mode_map.keys()
            if isinstance(key, str) and len(key) >= 10
        }
    )
    return days


def _calc_metrics(df: pd.DataFrame, all_trade_days: Iterable[pd.Timestamp]) -> Dict[str, float]:
    all_days = sorted({pd.Timestamp(d).normalize() for d in all_trade_days})
    market_days = len(all_days)
    if df.empty:
        return {
            "trades": 0,
            "trade_days": 0,
            "market_days": market_days,
            "trades_per_trade_day": 0.0,
            "trades_per_market_day": 0.0,
            "sum_pnl_pct": 0.0,
            "avg_pnl_pct": 0.0,
            "trade_win_pct": 0.0,
            "day_win_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "sum_pnl_rs": 0.0,
            "target_rate_pct": 0.0,
            "sl_rate_pct": 0.0,
            "eod_rate_pct": 0.0,
        }
    work = df.sort_values(["entry_time_ist", "ticker"], kind="mergesort").copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce").dt.normalize()
    pnl = pd.to_numeric(work["pnl_pct"], errors="coerce").fillna(0.0)
    day_pnl = work.groupby("trade_date", sort=True)["pnl_pct"].sum()
    outcome = work["outcome"].astype(str).str.upper()
    trade_days = int(work["trade_date"].nunique())
    return {
        "trades": int(len(work)),
        "trade_days": trade_days,
        "market_days": market_days,
        "trades_per_trade_day": _safe_round(len(work) / max(trade_days, 1), 2),
        "trades_per_market_day": _safe_round(len(work) / max(market_days, 1), 2),
        "sum_pnl_pct": _safe_round(pnl.sum(), 4),
        "avg_pnl_pct": _safe_round(pnl.mean(), 4),
        "trade_win_pct": _safe_round((pnl > 0).mean() * 100.0, 2),
        "day_win_pct": _safe_round((day_pnl > 0).mean() * 100.0, 2),
        "profit_factor": _safe_round(_profit_factor(pnl), 3),
        "max_drawdown_pct": _safe_round(_max_drawdown(pnl), 4),
        "sum_pnl_rs": _safe_round(
            pd.to_numeric(work.get("pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum(),
            2,
        ),
        "target_rate_pct": _safe_round((outcome == "TARGET").mean() * 100.0, 2),
        "sl_rate_pct": _safe_round((outcome == "SL").mean() * 100.0, 2),
        "eod_rate_pct": _safe_round((outcome == "EOD").mean() * 100.0, 2),
    }


def _scenario_rows(which: str) -> List[dict]:
    baseline = {
        "name": "baseline",
        "entry": {},
        "filters": {},
        "notes": "Current live/backtest parity profile",
    }
    full = [
        baseline,
        {
            "name": "looser_rs_adx",
            "entry": {"nifty_both_rs_long": 0.50, "adx_min": 20.0},
            "filters": {},
            "notes": "Open participation a bit through BOTH-mode RS and ADX",
        },
        {
            "name": "looser_rsi_volume",
            "entry": {"rsi_min_long": 48.0, "volume_min_ratio": 0.70},
            "filters": {},
            "notes": "Broader continuation participation",
        },
        {
            "name": "looser_qs_and_rs",
            "entry": {
                "quality_score_min": 3.5,
                "nifty_both_rs_long": 0.50,
                "volume_min_ratio": 0.75,
            },
            "filters": {},
            "notes": "Relax pre-entry quality gate with moderate RS loosen",
        },
        {
            "name": "looser_or_deadzone",
            "entry": {"nifty_both_rs_long": 0.50, "adx_min": 20.0},
            "filters": {
                "long_avwap_dead_lo": 1.15,
                "long_avwap_dead_hi": 1.35,
            },
            "notes": "Keep OR gate, narrow only the AVWAP dead band",
        },
        {
            "name": "or_gate_off",
            "entry": {"nifty_both_rs_long": 0.50, "adx_min": 20.0},
            "filters": {"or_gate_enabled": False},
            "notes": "Test if OR gate is over-suppressing longs",
        },
        {
            "name": "tight_quality",
            "entry": {"adx_min": 24.0, "rsi_min_long": 52.0, "quality_score_min": 4.5},
            "filters": {},
            "notes": "Higher quality / lower participation control run",
        },
        {
            "name": "max_participation_safe",
            "entry": {
                "nifty_both_rs_long": 0.35,
                "adx_min": 20.0,
                "rsi_min_long": 48.0,
                "volume_min_ratio": 0.70,
                "quality_score_min": 3.5,
            },
            "filters": {
                "long_qs_abs_max": 10.5,
                "long_avwap_dist_max": 3.2,
            },
            "notes": "Most permissive scenario that still keeps exhaustion caps",
        },
        {
            "name": "balanced_plus",
            "entry": {
                "nifty_both_rs_long": 0.60,
                "adx_min": 21.0,
                "rsi_min_long": 49.0,
                "volume_min_ratio": 0.75,
            },
            "filters": {
                "long_avwap_dead_lo": 1.10,
                "long_avwap_dead_hi": 1.40,
            },
            "notes": "Middle-ground participation boost without removing major guards",
        },
        # --- NEW TARGETED SCENARIOS (Run-12 search) ---
        {
            "name": "or_gate_off_strict_rs",
            "entry": {},
            "filters": {"or_gate_enabled": False},
            "notes": "OR gate OFF only; RS/ADX unchanged at baseline — isolates gate contribution",
        },
        {
            "name": "no_avwap_dead",
            "entry": {},
            "filters": {"long_avwap_dead_lo": 99.0, "long_avwap_dead_hi": 100.0},
            "notes": "Remove AVWAP dead zone entirely; baseline RS/OR gate kept — recovers 39 blocked trades",
        },
        {
            "name": "or_gate_off_no_avwap_dead",
            "entry": {},
            "filters": {
                "or_gate_enabled": False,
                "long_avwap_dead_lo": 99.0,
                "long_avwap_dead_hi": 100.0,
            },
            "notes": "OR gate OFF + AVWAP dead zone removed; strict RS stays — maximum baseline-quality participation",
        },
        {
            "name": "looser_rsi_or_off",
            "entry": {"rsi_min_long": 48.0, "volume_min_ratio": 0.70},
            "filters": {"or_gate_enabled": False},
            "notes": "looser_rsi_volume profile + OR gate OFF — more trades, checks if quality holds",
        },
    ]
    if which == "quick":
        keep = {"baseline", "looser_rs_adx", "looser_qs_and_rs", "balanced_plus", "or_gate_off"}
        return [row for row in full if row["name"] in keep]
    return full


def _exit_grid_for_scenario(name: str) -> Tuple[List[float], List[float]]:
    if name in {"tight_quality"}:
        stop_vals = [0.0055, 0.0060, 0.0065]
        tgt_vals = [0.00825, 0.0090, 0.0100]
    elif name in {"or_gate_off", "max_participation_safe",
                  "or_gate_off_strict_rs", "or_gate_off_no_avwap_dead", "looser_rsi_or_off"}:
        stop_vals = [0.0060, 0.0065, 0.0070]
        tgt_vals = [0.0075, 0.00825, 0.0090]
    elif name in {"no_avwap_dead"}:
        # Narrower SL range since quality profile is near-baseline
        stop_vals = [0.0055, 0.0060, 0.0065]
        tgt_vals = [0.0075, 0.00825, 0.0090]
    else:
        stop_vals = [0.0055, 0.0060, 0.0065]
        tgt_vals = [0.0075, 0.00825, 0.0090]
    return stop_vals, tgt_vals


@contextmanager
def _temporary_globals(overrides: Dict[str, object]) -> Iterator[None]:
    original: Dict[str, object] = {}
    try:
        for key, value in overrides.items():
            original[key] = getattr(base, key)
            setattr(base, key, value)
        yield
    finally:
        for key, value in original.items():
            setattr(base, key, value)


def _configure_long_cfg(out_dir: Path, entry_overrides: Dict[str, float]) -> base.StrategyConfig:
    short_cfg = base.default_short_config(reports_dir=out_dir)
    long_cfg = base.default_long_config_v9(reports_dir=out_dir)
    dir_15m = base._resolve_15m_dir()
    short_cfg.dir_15m = str(dir_15m)
    long_cfg.dir_15m = str(dir_15m)
    short_cfg.end_15m = "_stocks_indicators_5min.parquet"
    long_cfg.end_15m = "_stocks_indicators_5min.parquet"
    short_cfg.market_regime_tickers = tuple(base.NIFTY_CONTEXT_TICKERS)
    long_cfg.market_regime_tickers = tuple(base.NIFTY_CONTEXT_TICKERS)
    _, long_cfg = base.apply_live_parity_profile(short_cfg, long_cfg)

    if base.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        long_cfg.min_bars_left_after_entry = 0
    if base.FORCE_LIVE_PARITY_DISABLE_TOPN:
        long_cfg.enable_topn_per_day = False
        long_cfg.topn_per_day = 0
    if base.FINAL_SIGNAL_WINDOW_OVERRIDE:
        long_cfg.use_time_windows = bool(base.FINAL_LONG_USE_TIME_WINDOWS)
        long_cfg.signal_windows = list(base.FINAL_LONG_SIGNAL_WINDOWS)
    if base.TEST_TARGET_OVERRIDE:
        long_cfg.target_pct = float(base.TEST_LONG_TARGET_PCT)

    for key, value in entry_overrides.items():
        if key == "nifty_both_rs_long":
            continue
        setattr(long_cfg, key, value)

    vix_map = base._load_india_vix(ROOT)
    long_cfg.vix_scale_enabled = base.VIX_SCALE_ENABLED
    if base.VIX_SCALE_ENABLED and vix_map:
        long_cfg.vix_daily = vix_map
        long_cfg.vix_baseline = base.VIX_BASELINE
        long_cfg.vix_scale_min = base.VIX_SCALE_MIN
        long_cfg.vix_scale_max = base.VIX_SCALE_MAX
        long_cfg.vix_scale_target = base.VIX_SCALE_TARGET
        long_cfg.vix_scale_sl = base.VIX_SCALE_SL

    regime_map, regime_source = base.build_market_regime_map(long_cfg)
    if regime_map:
        long_cfg.market_regime_map = regime_map
        long_cfg.enable_market_regime_filter = True
    else:
        regime_source = ""
        long_cfg.enable_market_regime_filter = False
    setattr(long_cfg, "_regime_source", regime_source)
    return long_cfg


def _apply_post_scan_exact(long_df: pd.DataFrame, cfg: base.StrategyConfig) -> pd.DataFrame:
    work = long_df.copy()
    if base.NIFTY_CONTEXT_ENABLED:
        mode_map, nifty_ret_map, _, _ = base._build_nifty_intraday_context(cfg)
        if mode_map:
            _, work = base._apply_nifty_intraday_context(
                pd.DataFrame(),
                work,
                cfg,
                mode_map,
                nifty_ret_map,
            )
    if base.V16_OR_GATE_ENABLED:
        _, work = base._enrich_with_or_levels(
            pd.DataFrame(),
            work,
            dir_15m=cfg.dir_15m,
            parquet_suffix=cfg.end_15m,
        )
    _, work = base._apply_v16_post_scan_filters(pd.DataFrame(), work)
    return work.reset_index(drop=True)


def _exact_reprice_long(
    entry_df: pd.DataFrame,
    cfg: base.StrategyConfig,
    stop_pct: float,
    target_pct: float,
) -> pd.DataFrame:
    if entry_df.empty:
        return entry_df.copy()
    work = entry_df.copy()
    work["side"] = "LONG"
    work["stop_price"] = pd.to_numeric(work["entry_price"], errors="coerce") * (1.0 - float(stop_pct))
    work["target_price"] = pd.to_numeric(work["entry_price"], errors="coerce") * (1.0 + float(target_pct))
    work["sl_price"] = work["stop_price"]
    dir_1m = base._resolve_5min_dir()
    suffix = ".parquet"
    if dir_1m.is_dir():
        sample_files = list(dir_1m.glob("*"))[:5]
        for sf in sample_files:
            if sf.suffix:
                suffix = sf.suffix
                break
    resolved = base._resolve_exits_5min(
        work,
        dir_1m,
        suffix,
        cfg.parquet_engine,
        eod_exit_time=base.V15_EOD_EXIT_TIME,
    )
    resolved = base._add_notional_pnl(base._sort_trades_for_output(resolved))
    return resolved.reset_index(drop=True)


def _rank_exact_candidates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    target_mid = 4.0
    trades_per_market_day = out["trades_per_market_day"].astype(float)
    distance = (trades_per_market_day - target_mid).abs()
    in_band = trades_per_market_day.between(3.0, 5.0, inclusive="both")
    if distance.max() == distance.min():
        freq_score = np.full(len(out), 100.0)
    else:
        freq_score = (distance.max() - distance.to_numpy()) / (distance.max() - distance.min()) * 100.0
    freq_score = np.where(in_band.to_numpy(), np.minimum(100.0, freq_score + 20.0), freq_score)
    out["freq_score"] = np.round(freq_score, 2)

    specs = [
        ("profit_factor", False, 0.28),
        ("sum_pnl_pct", False, 0.25),
        ("trade_win_pct", False, 0.14),
        ("day_win_pct", False, 0.10),
        ("max_drawdown_pct", True, 0.13),
        ("freq_score", False, 0.10),
    ]
    score = np.zeros(len(out), dtype=float)
    for col, invert, weight in specs:
        vals = out[col].astype(float)
        lo, hi = float(vals.min()), float(vals.max())
        if hi == lo:
            norm = np.full(len(out), 100.0, dtype=float)
        else:
            if invert:
                norm = (hi - vals.to_numpy()) / (hi - lo) * 100.0
            else:
                norm = (vals.to_numpy() - lo) / (hi - lo) * 100.0
        out[f"norm_{col}"] = np.round(norm, 2)
        score += norm * weight
    out["score"] = np.round(score, 2)
    return out.sort_values(
        ["score", "profit_factor", "sum_pnl_pct", "trade_win_pct"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def _print_progress(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> None:
    args = _parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    base.MAX_WORKERS = max(1, int(args.max_workers))
    base.ENABLE_LEGACY_CHARTS = False
    base.ENABLE_ENHANCED_CHARTS = False

    scenarios = _scenario_rows(args.scenario_set)
    if args.scenario_names:
        allowed = {n.strip() for n in args.scenario_names.split(",") if n.strip()}
        scenarios = [s for s in scenarios if s["name"] in allowed]
        _print_progress(f"Scenario filter active: running only {sorted(allowed)}")

    summary_rows: List[dict] = []
    exact_rows: List[dict] = []
    exact_artifacts: List[dict] = []

    baseline_cfg = _configure_long_cfg(OUT_DIR / "_baseline_probe", {})
    all_trade_days = _trade_dates_from_data(baseline_cfg)
    _print_progress(f"Market days detected for backtest window: {len(all_trade_days)}")

    for idx, scenario in enumerate(scenarios, start=1):
        name = scenario["name"]
        run_dir = OUT_DIR / name
        run_dir.mkdir(parents=True, exist_ok=True)
        entry_overrides = dict(scenario.get("entry", {}))
        filter_overrides = dict(scenario.get("filters", {}))

        temp_globals = {}
        if "nifty_both_rs_long" in entry_overrides:
            temp_globals["NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT"] = float(entry_overrides["nifty_both_rs_long"])
            temp_globals["NIFTY_RS_BOTH_MODE_THRESHOLD_PCT"] = float(entry_overrides["nifty_both_rs_long"])
        filter_map = {
            "or_gate_enabled": "V16_OR_GATE_ENABLED",
            "long_qs_abs_max": "V16_LONG_QS_ABS_MAX",
            "long_avwap_dist_max": "V16_LONG_AVWAP_DIST_ATR_MAX",
            "long_avwap_dead_lo": "V16_LONG_AVWAP_DIST_DEAD_LO",
            "long_avwap_dead_hi": "V16_LONG_AVWAP_DIST_DEAD_HI",
        }
        for key, attr in filter_map.items():
            if key in filter_overrides:
                temp_globals[attr] = filter_overrides[key]

        _print_progress(f"Scenario {idx}/{len(scenarios)}: {name} | scan starting")
        started = time.perf_counter()
        with _temporary_globals(temp_globals):
            cfg = _configure_long_cfg(run_dir, entry_overrides)
            scanned = base._run_side_parallel("LONG", cfg, base.MAX_WORKERS)
            filtered = _apply_post_scan_exact(scanned, cfg)

            scenario_meta = {
                "scenario": name,
                "notes": scenario.get("notes", ""),
                "entry_overrides": json.dumps(entry_overrides, sort_keys=True),
                "filter_overrides": json.dumps(filter_overrides, sort_keys=True),
                "scan_trades": int(len(scanned)),
                "post_filter_trades": int(len(filtered)),
                "regime_source": getattr(cfg, "_regime_source", ""),
            }

            if filtered.empty:
                summary_rows.append(
                    {
                        **scenario_meta,
                        "stop_pct": 0.0,
                        "target_pct": 0.0,
                        "rr": 0.0,
                        **_calc_metrics(filtered, all_trade_days),
                        "scan_seconds": round(time.perf_counter() - started, 1),
                    }
                )
                _print_progress(f"Scenario {name}: no trades after filters")
                continue

            filtered_csv = run_dir / f"{name}_post_filter_entries.csv"
            filtered.to_csv(filtered_csv, index=False)

            tickers = filtered["ticker"].astype(str).unique()
            cache_1m = slsearch._load_1m_cache(tickers)
            cache_5m = slsearch._load_5m_cache(tickers)
            paths = slsearch._build_trade_paths(filtered, cache_1m, cache_5m)
            stop_vals, tgt_vals = _exit_grid_for_scenario(name)
            approx_grid, _ = slsearch._run_side_grid(paths, stop_vals, tgt_vals)
            approx_grid.insert(0, "scenario", name)
            approx_grid.insert(1, "notes", scenario.get("notes", ""))
            approx_grid["entry_overrides"] = json.dumps(entry_overrides, sort_keys=True)
            approx_grid["filter_overrides"] = json.dumps(filter_overrides, sort_keys=True)
            approx_grid["scan_trades"] = int(len(scanned))
            approx_grid["post_filter_trades"] = int(len(filtered))
            approx_grid.to_csv(run_dir / f"{name}_approx_exit_grid.csv", index=False)

            best_approx = approx_grid.head(1).copy()
            if best_approx.empty:
                continue
            best_approx_row = best_approx.iloc[0].to_dict()
            best_approx_row.update(scenario_meta)
            best_approx_row["scan_seconds"] = round(time.perf_counter() - started, 1)
            summary_rows.append(best_approx_row)

            exact_candidates = approx_grid.head(max(1, int(args.top_exit_exact))).copy()
            for cand in exact_candidates.itertuples(index=False):
                exact_started = time.perf_counter()
                exact_df = _exact_reprice_long(
                    filtered,
                    cfg,
                    stop_pct=float(cand.stop_pct),
                    target_pct=float(cand.target_pct),
                )
                exact_metrics = _calc_metrics(exact_df, all_trade_days)
                exact_row = {
                    **scenario_meta,
                    "scenario": name,
                    "notes": scenario.get("notes", ""),
                    "stop_pct": round(float(cand.stop_pct), 6),
                    "target_pct": round(float(cand.target_pct), 6),
                    "rr": round(float(cand.target_pct) / float(cand.stop_pct), 3),
                    "scan_seconds": round(time.perf_counter() - started, 1),
                    "exact_exit_seconds": round(time.perf_counter() - exact_started, 1),
                    **exact_metrics,
                }
                exact_rows.append(exact_row)

                exact_csv = run_dir / f"{name}_exact_sl_{cand.stop_pct:.4f}_tgt_{cand.target_pct:.4f}.csv"
                exact_df.to_csv(exact_csv, index=False)
                exact_artifacts.append(
                    {
                        "scenario": name,
                        "csv": str(exact_csv),
                        "metrics": exact_row,
                    }
                )
                _print_progress(
                    f"Scenario {name}: exact SL={cand.stop_pct:.4f} TGT={cand.target_pct:.4f} "
                    f"-> PF {exact_metrics['profit_factor']:.3f}, "
                    f"PnL {exact_metrics['sum_pnl_pct']:.2f}%, "
                    f"TPD(market) {exact_metrics['trades_per_market_day']:.2f}"
                )

        _print_progress(
            f"Scenario {name}: completed in {time.perf_counter() - started:.1f}s"
        )

    approx_summary = pd.DataFrame(summary_rows)
    exact_summary = pd.DataFrame(exact_rows)

    approx_path = OUT_DIR / "approx_summary.csv"
    exact_path = OUT_DIR / "exact_summary.csv"
    artifacts_path = OUT_DIR / "exact_artifacts.json"

    # Merge with existing CSVs so partial runs don't lose prior results
    if args.merge_existing and args.scenario_names:
        if approx_path.exists() and not approx_summary.empty:
            existing_approx = pd.read_csv(approx_path)
            run_names = approx_summary["scenario"].unique().tolist()
            existing_approx = existing_approx[~existing_approx["scenario"].isin(run_names)]
            approx_summary = pd.concat([existing_approx, approx_summary], ignore_index=True)
        if exact_path.exists() and not exact_summary.empty:
            existing_exact = pd.read_csv(exact_path)
            run_names = exact_summary["scenario"].unique().tolist()
            existing_exact = existing_exact[~existing_exact["scenario"].isin(run_names)]
            exact_summary = pd.concat([existing_exact, exact_summary], ignore_index=True)

    if not approx_summary.empty:
        approx_summary = approx_summary.sort_values(
            ["profit_factor", "sum_pnl_pct", "trade_win_pct"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
    if not exact_summary.empty:
        exact_summary = _rank_exact_candidates(exact_summary)

    if not approx_summary.empty:
        approx_summary.head(args.keep_top).to_csv(approx_path, index=False)
    if not exact_summary.empty:
        exact_summary.head(args.keep_top).to_csv(exact_path, index=False)
    artifacts_path.write_text(json.dumps(exact_artifacts, indent=2), encoding="utf-8")

    best_exact = exact_summary.head(1).to_dict(orient="records") if not exact_summary.empty else []
    best_approx = approx_summary.head(1).to_dict(orient="records") if not approx_summary.empty else []
    summary_json = {
        "generated_at": pd.Timestamp.now(tz=base.IST).isoformat(),
        "scenario_set": args.scenario_set,
        "max_workers": base.MAX_WORKERS,
        "market_days": len(all_trade_days),
        "best_exact": best_exact,
        "best_approx": best_approx,
        "approx_summary_csv": str(approx_path),
        "exact_summary_csv": str(exact_path),
        "exact_artifacts_json": str(artifacts_path),
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(summary_json, indent=2), encoding="utf-8")

    _print_progress(f"Approx summary: {approx_path}")
    _print_progress(f"Exact summary: {exact_path}")
    if best_exact:
        row = best_exact[0]
        _print_progress(
            "Best exact candidate: "
            f"{row['scenario']} | SL={row['stop_pct']:.4f} TGT={row['target_pct']:.4f} | "
            f"PF={row['profit_factor']:.3f} | PnL={row['sum_pnl_pct']:.2f}% | "
            f"Win={row['trade_win_pct']:.2f}% | "
            f"TPD(market)={row['trades_per_market_day']:.2f} | "
            f"TPD(trade)={row['trades_per_trade_day']:.2f} | "
            f"MaxDD={row['max_drawdown_pct']:.2f}%"
        )


if __name__ == "__main__":
    main()
