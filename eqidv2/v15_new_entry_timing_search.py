from __future__ import annotations

import argparse
import json
import math
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import time as dtime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

import avwap_combined_runner_v15 as runner


ANALYSIS_START = "2026-02-23"
ANALYSIS_END = "2026-03-30"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs_v15_new_entry_timing_search"


@dataclass(frozen=True)
class Scenario:
    name: str
    notes: str
    short_lag_a_mod: Optional[int] = None
    short_entry_cutoff: Optional[dtime] = None
    short_signal_avwap_dist_atr_max: Optional[float] = None
    short_slip_cap: Optional[float] = None
    long_lag_a_mod: Optional[int] = None


SCENARIOS: List[Scenario] = [
    Scenario(
        name="baseline_current",
        notes="Current v15_new live-parity profile.",
    ),
    Scenario(
        name="short_a_mod_lag0",
        notes="Earlier short A_MOD entry on the signal bar instead of waiting one extra 15m bar.",
        short_lag_a_mod=0,
    ),
    Scenario(
        name="short_cutoff_1230",
        notes="Keep current lag, but stop accepting short entries after 12:30.",
        short_entry_cutoff=dtime(12, 30),
    ),
    Scenario(
        name="short_cutoff_1230_avwap16",
        notes="Keep current lag, stop after 12:30, and reject more overstretched short entries.",
        short_entry_cutoff=dtime(12, 30),
        short_signal_avwap_dist_atr_max=1.60,
    ),
    Scenario(
        name="short_cutoff_1200",
        notes="Keep current lag, but stop accepting short entries after 12:00.",
        short_entry_cutoff=dtime(12, 0),
    ),
    Scenario(
        name="short_lag0_cutoff_1230",
        notes="Earlier short A_MOD entry plus earlier short cutoff.",
        short_lag_a_mod=0,
        short_entry_cutoff=dtime(12, 30),
    ),
    Scenario(
        name="short_lag0_cutoff_1230_avwap16",
        notes="Earlier short A_MOD entry, earlier cutoff, and tighter short extension filter.",
        short_lag_a_mod=0,
        short_entry_cutoff=dtime(12, 30),
        short_signal_avwap_dist_atr_max=1.60,
    ),
    Scenario(
        name="short_lag0_cutoff_1200_avwap16",
        notes="Very selective short profile for late-stage fade protection.",
        short_lag_a_mod=0,
        short_entry_cutoff=dtime(12, 0),
        short_signal_avwap_dist_atr_max=1.60,
    ),
]


def _to_date_series(df: pd.DataFrame) -> pd.Series:
    if df.empty or "trade_date" not in df.columns:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(df["trade_date"], errors="coerce")


def _filter_window(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    dates = _to_date_series(df)
    mask = dates.between(pd.Timestamp(start), pd.Timestamp(end), inclusive="both")
    return df.loc[mask].copy()


def _profit_factor_from_pnl(values: Iterable[float]) -> float:
    gains = 0.0
    losses = 0.0
    for val in values:
        x = float(val or 0.0)
        if x > 0:
            gains += x
        elif x < 0:
            losses += abs(x)
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _setup_summary(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    work = df.copy()
    work["side"] = work["side"].astype(str).str.upper()
    work["setup"] = work["setup"].astype(str)
    work["outcome"] = work["outcome"].astype(str).str.upper()
    rows: List[Dict[str, Any]] = []
    for (side, setup), grp in work.groupby(["side", "setup"], dropna=False):
        pnl = pd.to_numeric(grp.get("pnl_rs"), errors="coerce").fillna(0.0)
        wins = grp["outcome"].isin({"TARGET", "EOD"}).mean() * 100.0 if len(grp) else 0.0
        rows.append(
            {
                "side": str(side),
                "setup": str(setup),
                "trades": int(len(grp)),
                "win_rate_pct": round(float(wins), 2),
                "profit_factor": round(float(_profit_factor_from_pnl(pnl.tolist())), 4)
                if math.isfinite(_profit_factor_from_pnl(pnl.tolist()))
                else "inf",
                "pnl_rs": round(float(pnl.sum()), 2),
                "avg_quality_score": round(
                    float(pd.to_numeric(grp.get("quality_score"), errors="coerce").dropna().mean())
                    if "quality_score" in grp.columns and not pd.to_numeric(grp.get("quality_score"), errors="coerce").dropna().empty
                    else 0.0,
                    4,
                ),
            }
        )
    rows.sort(key=lambda item: (item["side"], -item["pnl_rs"], item["setup"]))
    return rows


def _entry_time_summary(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or "entry_time_ist" not in df.columns:
        return {}
    times = pd.to_datetime(df["entry_time_ist"], errors="coerce")
    hours = times.dt.tz_localize(None) if hasattr(times.dt, "tz_localize") else times
    noon = times.dt.hour.ge(12).sum()
    after_1230 = ((times.dt.hour > 12) | ((times.dt.hour == 12) & (times.dt.minute >= 30))).sum()
    after_1300 = ((times.dt.hour > 13) | ((times.dt.hour == 13) & (times.dt.minute >= 0))).sum()
    return {
        "after_1200_trades": int(noon),
        "after_1230_trades": int(after_1230),
        "after_1300_trades": int(after_1300),
    }


def _metrics_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "trades": 0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "pnl_rs": 0.0,
            "max_drawdown_pct": 0.0,
        }
    metrics = runner.compute_backtest_metrics(df)
    return {
        "trades": int(getattr(metrics, "total_trades", 0)),
        "win_rate_pct": round(float(getattr(metrics, "hit_rate_pct", 0.0)), 2),
        "profit_factor": round(float(getattr(metrics, "profit_factor", 0.0)), 4)
        if math.isfinite(float(getattr(metrics, "profit_factor", 0.0)))
        else "inf",
        "pnl_rs": round(float(df.get("pnl_rs", pd.Series(dtype=float)).sum()), 2),
        "max_drawdown_pct": round(float(getattr(metrics, "max_drawdown_pct", 0.0)), 2),
    }


@contextmanager
def _scenario_globals(scenario: Scenario):
    tracked = {
        "SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW": runner.SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW,
        "V15_SHORT_ENTRY_CUTOFF": runner.V15_SHORT_ENTRY_CUTOFF,
        "V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX": runner.V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX,
        "LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH": runner.LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH,
    }
    try:
        if scenario.short_lag_a_mod is not None:
            runner.SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW = int(scenario.short_lag_a_mod)
        if scenario.short_entry_cutoff is not None:
            runner.V15_SHORT_ENTRY_CUTOFF = scenario.short_entry_cutoff
        if scenario.short_signal_avwap_dist_atr_max is not None:
            runner.V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = float(
                scenario.short_signal_avwap_dist_atr_max
            )
        if scenario.long_lag_a_mod is not None:
            runner.LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH = int(scenario.long_lag_a_mod)
        yield
    finally:
        for key, value in tracked.items():
            setattr(runner, key, value)


def _build_cfgs(reports_dir: Path):
    dir_15m = runner._resolve_15m_dir()
    short_cfg = runner.default_short_config(reports_dir=reports_dir)
    long_cfg = runner.default_long_config_v9(reports_dir=reports_dir)
    short_cfg.dir_15m = str(dir_15m)
    long_cfg.dir_15m = str(dir_15m)
    short_cfg.market_regime_tickers = tuple(runner.NIFTY_CONTEXT_TICKERS)
    long_cfg.market_regime_tickers = tuple(runner.NIFTY_CONTEXT_TICKERS)
    short_cfg, long_cfg = runner.apply_live_parity_profile(short_cfg, long_cfg)

    regime_map, _ = runner.build_market_regime_map(short_cfg)
    if regime_map:
        short_cfg.market_regime_map = regime_map
        long_cfg.market_regime_map = regime_map
        short_cfg.enable_market_regime_filter = True
        long_cfg.enable_market_regime_filter = True
    else:
        short_cfg.enable_market_regime_filter = False
        long_cfg.enable_market_regime_filter = False
    return short_cfg, long_cfg


def _run_scenario(scenario: Scenario) -> Dict[str, Any]:
    scenario_dir = OUTPUT_DIR / scenario.name
    scenario_dir.mkdir(parents=True, exist_ok=True)

    with _scenario_globals(scenario):
        short_cfg, long_cfg = _build_cfgs(scenario_dir)
        short_df, long_df = runner._run_both_parallel(short_cfg, long_cfg, runner.MAX_WORKERS)

        if runner.NIFTY_CONTEXT_ENABLED:
            mode_map, nifty_ret_map, _, _ = runner._build_nifty_intraday_context(short_cfg)
            if mode_map:
                short_df, long_df = runner._apply_nifty_intraday_context(
                    short_df,
                    long_df,
                    short_cfg,
                    mode_map,
                    nifty_ret_map,
                )

        short_df, long_df = runner._replace_current_day_with_live_parity(short_df, long_df)

        dir_1m = runner._resolve_5min_dir()
        suffix_1m = ".parquet"
        if dir_1m.is_dir():
            sample_files = list(dir_1m.glob("*"))[:5]
            for sample in sample_files:
                if sample.suffix:
                    suffix_1m = sample.suffix
                    break

        if not short_df.empty:
            short_df = runner._resolve_exits_5min(
                short_df,
                dir_1m,
                suffix_1m,
                short_cfg.parquet_engine,
                eod_exit_time=runner.V15_EOD_EXIT_TIME,
            )
            short_df = runner._add_notional_pnl(short_df)
            short_df = runner._sort_trades_for_output(short_df)
        if not long_df.empty:
            long_df = runner._resolve_exits_5min(
                long_df,
                dir_1m,
                suffix_1m,
                long_cfg.parquet_engine,
                eod_exit_time=runner.V15_EOD_EXIT_TIME,
            )
            long_df = runner._add_notional_pnl(long_df)
            long_df = runner._sort_trades_for_output(long_df)

        combined = pd.concat([short_df, long_df], ignore_index=True) if (not short_df.empty or not long_df.empty) else pd.DataFrame()
        if not combined.empty:
            combined = runner._add_notional_pnl(combined)
            combined = runner._sort_trades_for_output(combined)

        short_recent = _filter_window(short_df, ANALYSIS_START, ANALYSIS_END)
        long_recent = _filter_window(long_df, ANALYSIS_START, ANALYSIS_END)
        combined_recent = _filter_window(combined, ANALYSIS_START, ANALYSIS_END)

    result = {
        "scenario": scenario.name,
        "notes": scenario.notes,
        "overrides": {
            "short_lag_a_mod": scenario.short_lag_a_mod,
            "short_entry_cutoff": scenario.short_entry_cutoff.strftime("%H:%M")
            if scenario.short_entry_cutoff is not None
            else None,
            "short_signal_avwap_dist_atr_max": scenario.short_signal_avwap_dist_atr_max,
            "short_slip_cap": scenario.short_slip_cap,
            "long_lag_a_mod": scenario.long_lag_a_mod,
        },
        "combined": _metrics_snapshot(combined_recent),
        "short": _metrics_snapshot(short_recent),
        "long": _metrics_snapshot(long_recent),
        "combined_entry_timing": _entry_time_summary(combined_recent),
        "short_entry_timing": _entry_time_summary(short_recent),
        "setup_summary": _setup_summary(combined_recent),
    }

    if not combined_recent.empty:
        combined_recent.to_csv(scenario_dir / "combined_recent.csv", index=False)
    if not short_recent.empty:
        short_recent.to_csv(scenario_dir / "short_recent.csv", index=False)
    if not long_recent.empty:
        long_recent.to_csv(scenario_dir / "long_recent.csv", index=False)
    (scenario_dir / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def _select_scenarios(selected_names: Optional[List[str]]) -> List[Scenario]:
    if not selected_names:
        return list(SCENARIOS)
    lookup = {scenario.name: scenario for scenario in SCENARIOS}
    selected: List[Scenario] = []
    for name in selected_names:
        key = str(name).strip()
        if key not in lookup:
            raise ValueError(f"Unknown scenario: {key}")
        selected.append(lookup[key])
    return selected


def main() -> None:
    parser = argparse.ArgumentParser(description="Run targeted v15_new entry-timing replay scenarios.")
    parser.add_argument(
        "--scenario",
        action="append",
        dest="scenarios",
        help="Scenario name to run. Repeat for multiple scenarios. Default: run all.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Override runner.MAX_WORKERS for this process.",
    )
    args = parser.parse_args()

    selected_scenarios = _select_scenarios(args.scenarios)
    if args.max_workers is not None and int(args.max_workers) > 0:
        runner.MAX_WORKERS = int(args.max_workers)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, Any]] = []
    for scenario in selected_scenarios:
        print(f"[RUN] {scenario.name}")
        results.append(_run_scenario(scenario))

    summary_rows: List[Dict[str, Any]] = []
    for item in results:
        summary_rows.append(
            {
                "scenario": item["scenario"],
                "notes": item["notes"],
                "combined_pnl_rs": item["combined"]["pnl_rs"],
                "combined_pf": item["combined"]["profit_factor"],
                "combined_trades": item["combined"]["trades"],
                "combined_win_rate_pct": item["combined"]["win_rate_pct"],
                "combined_max_drawdown_pct": item["combined"]["max_drawdown_pct"],
                "short_pnl_rs": item["short"]["pnl_rs"],
                "short_pf": item["short"]["profit_factor"],
                "short_trades": item["short"]["trades"],
                "long_pnl_rs": item["long"]["pnl_rs"],
                "long_pf": item["long"]["profit_factor"],
                "long_trades": item["long"]["trades"],
                "after_1230_trades": item["combined_entry_timing"].get("after_1230_trades", 0),
                "after_1300_trades": item["combined_entry_timing"].get("after_1300_trades", 0),
            }
        )

    summary_df = pd.DataFrame(summary_rows).sort_values(
        by=["combined_pnl_rs", "combined_pf", "combined_trades"],
        ascending=[False, False, True],
    )
    summary_csv = OUTPUT_DIR / "scenario_summary.csv"
    summary_json = OUTPUT_DIR / "scenario_summary.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_json.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print("\n=== Scenario Summary ===")
    if summary_df.empty:
        print("No results.")
    else:
        print(summary_df.to_string(index=False))
    print(f"\n[FILE SAVED] {summary_csv}")
    print(f"[FILE SAVED] {summary_json}")


if __name__ == "__main__":
    main()
