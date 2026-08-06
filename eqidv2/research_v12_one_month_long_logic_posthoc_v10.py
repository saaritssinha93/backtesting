"""Post-hoc robustness refinement of the one-month V9 LONG rule search.

This module runs only after V9's locked test has been opened.  It therefore
uses the full 2026-07-06 through 2026-08-04 month and must never be described
as validation or out-of-sample evidence.  Its purpose is narrower: determine
whether the promising pullback-bounce pattern occupies a stable local
threshold region while preserving at least three exact V12 trades/session.

The hourly prefilter, 1% stop, 2% target, completed-5m signal, exact next-1m
entry, exact 1m exit, statutory costs, risk sizing, one ticker/day, and daily
cap remain unchanged.  Production is never touched.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, fields, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

import research_v12_one_month_long_logic_optimizer_v9 as v9


PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
POSTHOC_IN_SAMPLE = True
SEED_CONFIG_ID = "R008169_PULLBACK_BOUNCE"
SEARCH_SEED = 20260806
DEFAULT_LOCAL_TRIALS = 50_000
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_one_month_long_logic_posthoc_v10_20260706_20260804"
)


def _choice(rng: np.random.Generator, values: Sequence[Any]) -> Any:
    value = values[int(rng.integers(0, len(values)))]
    return value.item() if isinstance(value, np.generic) else value


def _normalise_config_row(row: pd.Series) -> v9.RuleConfig:
    values: dict[str, Any] = {}
    for field in fields(v9.RuleConfig):
        value = row[field.name]
        if pd.isna(value) and field.name not in {"config_id", "family"}:
            value = None
        if field.name.startswith("require_"):
            value = bool(value)
        elif field.name in {"rank_min", "rank_max", "signal_minute_min", "signal_minute_max"}:
            value = int(value)
        values[field.name] = value
    return v9.RuleConfig(**values)


def load_seed() -> v9.RuleConfig:
    ledger = pd.read_parquet(v9.OUTPUT_DIR / "complete_trial_ledger.parquet")
    rows = ledger.loc[ledger["config_id"].astype(str).eq(SEED_CONFIG_ID)]
    if len(rows) != 1:
        raise RuntimeError(f"seed config lookup returned {len(rows)} rows")
    return _normalise_config_row(rows.iloc[0])


def local_configurations(seed: v9.RuleConfig, total: int) -> list[v9.RuleConfig]:
    if total < 100:
        raise ValueError("at least 100 local trials are required")
    rng = np.random.default_rng(SEARCH_SEED)
    choices = {
        "rank": ((200, 280), (200, 300), (210, 300), (220, 300)),
        "start": (585, 600, 615),
        "end": (735, 750, 765, 780),
        "atr": (0.35, 0.40, 0.45, 0.50),
        "session": (0.75, 1.0, 1.25),
        "vwap": (0.80, 0.90, 1.00, 1.10, 1.20, 1.30),
        "close": (0.55, 0.60, 0.65, 0.70, 0.75),
        "range": (None, 0.15, 0.20, 0.25, 0.35),
        "ret_min": (0.05, 0.10, 0.11, 0.12, 0.15, 0.20),
        "ret_max": (0.30, 0.35, 0.40, 0.50),
        "ret30": (None, 0.0, 0.20),
        "ema20": (0.0, 0.25, 0.50, 0.75, 1.0),
        "margin": (None, 0.0, 0.025, 0.05, 0.075),
        "previous_ret": (-0.20, -0.15, -0.10, -0.05, 0.0),
    }
    configs: list[v9.RuleConfig] = []
    seen: set[str] = set()
    ordinal = 1
    # Always include the seed and the two simple, pre-observed perturbations.
    anchors = (
        seed,
        replace(seed, vwap_dist_atr_min=1.10),
        replace(seed, ret_5m_min=0.11),
        replace(seed, vwap_dist_atr_min=1.10, ret_5m_min=0.11),
    )
    for anchor in anchors:
        config = replace(anchor, config_id=f"L{ordinal:06d}_PULLBACK_BOUNCE")
        key = v9.config_hash(replace(config, config_id="CANONICAL"))
        seen.add(key)
        configs.append(config)
        ordinal += 1
    while len(configs) < total:
        rank_min, rank_max = _choice(rng, choices["rank"])
        start = int(_choice(rng, choices["start"]))
        end = int(_choice(rng, [value for value in choices["end"] if value >= start + 90]))
        config = replace(
            seed,
            config_id=f"L{ordinal:06d}_PULLBACK_BOUNCE",
            rank_min=int(rank_min),
            rank_max=int(rank_max),
            signal_minute_min=start,
            signal_minute_max=end,
            atr_pct_min=float(_choice(rng, choices["atr"])),
            session_return_min=float(_choice(rng, choices["session"])),
            vwap_dist_atr_min=float(_choice(rng, choices["vwap"])),
            close_position_min=float(_choice(rng, choices["close"])),
            range_pct_min=_choice(rng, choices["range"]),
            ret_5m_min=float(_choice(rng, choices["ret_min"])),
            ret_5m_max=float(_choice(rng, choices["ret_max"])),
            ret_30m_min=_choice(rng, choices["ret30"]),
            ema20_dist_atr_min=float(_choice(rng, choices["ema20"])),
            score_margin_min=_choice(rng, choices["margin"]),
            previous_ret_5m_max=float(_choice(rng, choices["previous_ret"])),
            require_contiguous_previous=True,
            require_bullish_reversal=False,
            require_vwap_reclaim=False,
        )
        key = v9.config_hash(replace(config, config_id="CANONICAL"))
        if key not in seen:
            seen.add(key)
            configs.append(config)
        ordinal += 1
    return configs


def _weekly_metrics(arrays: v9.SearchArrays, selected: np.ndarray) -> list[dict[str, Any]]:
    bounds = ((0, 5), (5, 10), (10, 15), (15, 20), (20, 22))
    return [v9.performance_from_indices(arrays, selected, range(start, end)) for start, end in bounds]


def _remove_best_day_metrics(arrays: v9.SearchArrays, selected: np.ndarray) -> dict[str, Any]:
    daily = np.zeros(len(arrays.sessions), dtype=float)
    for index in selected:
        daily[arrays.day_code[index]] += arrays.pnl[index]
    best_day = int(np.argmax(daily))
    positions = [position for position in range(len(arrays.sessions)) if position != best_day]
    return {
        "removed_day": arrays.sessions[best_day],
        **v9.performance_from_indices(arrays, selected, positions),
    }


def _robust_score(
    full: Mapping[str, Any],
    cost15: Mapping[str, Any],
    weeks: Sequence[Mapping[str, Any]],
    remove_best: Mapping[str, Any],
) -> float:
    weekly_logs = np.log([
        max(float(item["shrunk_profit_factor"]), 1e-9) for item in weeks
    ])
    frequency = min(float(full["trades_per_session"]), 6.0)
    return float(
        math.log(max(float(full["shrunk_profit_factor"]), 1e-9))
        + 0.35 * math.log(max(float(cost15["shrunk_profit_factor"]), 1e-9))
        + 0.25 * float(np.median(weekly_logs))
        - 0.20 * float(np.std(weekly_logs))
        + 0.15 * math.log(max(frequency, 0.25) / 3.0)
        + 0.10 * math.log(max(float(remove_best["shrunk_profit_factor"]), 1e-9))
    )


def run_local_search(
    exact: pd.DataFrame,
    sessions: Sequence[str],
    seed: v9.RuleConfig,
    total: int,
) -> tuple[pd.DataFrame, v9.RuleConfig, np.ndarray]:
    arrays = v9.SearchArrays(exact, sessions)
    configs = local_configurations(seed, total)
    rows: list[dict[str, Any]] = []
    candidates: list[tuple[float, v9.RuleConfig, np.ndarray]] = []
    seen_entries: set[str] = set()
    for count, config in enumerate(configs, 1):
        selected = arrays.selected_indices(config)
        signature = v9._signature(selected)
        full = v9.performance_from_indices(arrays, selected, range(22))
        cost15 = v9.performance_from_indices(arrays, selected, range(22), cost_multiplier=1.5)
        weeks = _weekly_metrics(arrays, selected)
        remove_best = _remove_best_day_metrics(arrays, selected)
        positive_weeks = int(sum(item["net_pnl_rs"] > 0 for item in weeks))
        score = _robust_score(full, cost15, weeks, remove_best)
        gate = (
            full["trades"] >= 66
            and full["trades_per_session"] >= 3.0
            and full["active_days"] >= 20
            and full["profit_factor"] >= 1.25
            and full["net_pnl_rs"] > 0
            and positive_weeks >= 4
            and cost15["profit_factor"] >= 1.15
            and remove_best["net_pnl_rs"] > 0
            and remove_best["profit_factor"] >= 1.05
        )
        record = {
            **v9.json_safe(asdict(config)),
            **{f"full_{key}": value for key, value in full.items()},
            **{f"cost1p5_{key}": value for key, value in cost15.items()},
            "positive_weeks": positive_weeks,
            "worst_week_net_pnl_rs": min(item["net_pnl_rs"] for item in weeks),
            "median_week_net_pnl_rs": float(np.median([item["net_pnl_rs"] for item in weeks])),
            "remove_best_day": remove_best["removed_day"],
            "remove_best_day_net_pnl_rs": remove_best["net_pnl_rs"],
            "remove_best_day_profit_factor": remove_best["profit_factor"],
            "entry_signature": signature,
            "robust_score": score,
            "robustness_gate": bool(gate),
        }
        rows.append(record)
        if gate and signature not in seen_entries:
            seen_entries.add(signature)
            candidates.append((score, config, selected))
        if count % 5_000 == 0 or count == len(configs):
            print(f"[posthoc-local] {count:,}/{len(configs):,} gated={len(candidates):,}", flush=True)
    if not candidates:
        raise RuntimeError("no local post-hoc configuration passed the robustness gate")
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, champion, selected = candidates[0]
    return pd.DataFrame(rows), champion, selected


def day_block_bootstrap(
    trades: pd.DataFrame,
    sessions: Sequence[str],
    *,
    samples: int = 20_000,
    searched_configurations: int = 150_000,
) -> dict[str, Any]:
    day_pnls = {
        day: pd.to_numeric(
            trades.loc[trades["trade_date"].eq(day), "net_pnl_rs"], errors="coerce"
        ).dropna().to_numpy(dtype=float)
        for day in sessions
    }
    rng = np.random.default_rng(20260807)
    net_values = np.empty(samples, dtype=float)
    pf_values = np.empty(samples, dtype=float)
    days = np.asarray(list(sessions), dtype=object)
    for index in range(samples):
        sampled = rng.choice(days, size=len(days), replace=True)
        pnl = np.concatenate([day_pnls[str(day)] for day in sampled if len(day_pnls[str(day)])])
        gains = float(pnl[pnl > 0].sum()) if len(pnl) else 0.0
        losses = float(-pnl[pnl < 0].sum()) if len(pnl) else 0.0
        net_values[index] = float(pnl.sum()) if len(pnl) else 0.0
        pf_values[index] = gains / losses if losses else np.nan
    finite_pf = pf_values[np.isfinite(pf_values)]
    return {
        "samples": samples,
        "probability_net_positive": float(np.mean(net_values > 0)),
        "net_pnl_95pct_ci_rs": [float(value) for value in np.quantile(net_values, [0.025, 0.975])],
        "profit_factor_95pct_ci": [float(value) for value in np.quantile(finite_pf, [0.025, 0.975])],
        "warning": (
            "Post-selection bootstrap; does not correct for "
            f"{searched_configurations:,} searched configurations."
        ),
    }


def write_posthoc_config(path: Path, config: v9.RuleConfig) -> None:
    content = f'''"""Post-hoc in-sample V12 LONG research candidate.

This configuration used the full {v9.START_DATE} through {v9.END_DATE} month
for local refinement.  It has no untouched holdout and is not approved for
production.
"""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
POSTHOC_IN_SAMPLE = True
REQUIRES_FRESH_HOLDOUT = True

SETUP_NAME = {v9.SETUP!r}
CONFIG_ID = {config.config_id!r}
CONFIG_SHA256 = {v9.config_hash(config)!r}
FAMILY = {config.family!r}

PREFILTER_JOB_CHANGED = False
PREFILTER_PRIMARY_SIDE = "LONG"
PREFILTER_RANK_MIN = {config.rank_min}
PREFILTER_RANK_MAX = {config.rank_max}

SIGNAL_TIMEFRAME = "5min_completed_bar"
ENTRY_TIMEFRAME = "exact_next_available_1min"
EXIT_TIMEFRAME = "exact_1min_with_conservative_5min_gap_fallback"
STOP_LOSS_PCT = {v9.STOP_LOSS_PCT!r}
TARGET_PCT = {v9.TARGET_PCT!r}
ONE_TICKER_PER_DAY = True
DAILY_CAP = {v9.DAILY_CAP}
STATUTORY_COSTS = True
V12_RISK_SIZING = True
PAPER_ENTRY_SLIPPAGE_BPS = {float(v9.v12.V7_PAPER_SLIPPAGE_PCT) * 10_000.0!r}
RISK_EQUITY_RS = {float(v9.v12.RISK_EQUITY_RS)!r}
RISK_PCT_PER_TRADE = {float(v9.v12.RISK_PCT_PER_TRADE)!r}
RISK_MIN_NOTIONAL_RS = {float(v9.v12.RISK_MIN_NOTIONAL_RS)!r}
RISK_MAX_NOTIONAL_RS = {float(v9.v12.RISK_MAX_NOTIONAL_RS)!r}
INTRADAY_LEVERAGE = {float(v9.v12.V7_INTRADAY_LEVERAGE)!r}

ENTRY_SELECTION = "first chronological passing signal per ticker/day"
ENTRY_TIE_BREAK = ("signal_time_ist", "selection_rank", "ticker")
STOP_TARGET_SAME_BAR_POLICY = "STOP_FIRST"
ONE_MINUTE_GAP_POLICY = "CONSERVATIVE_5MIN_FALLBACK"
MISSING_FEATURE_POLICY = "FAIL_CLOSED"
PREFILTER_MEMBERSHIP_POLICY = "LONG at signal hour; same hourly list valid within that hour"

RULE = {v9.json_safe(asdict(config))!r}
'''
    path.write_text(content, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=DEFAULT_LOCAL_TRIALS)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args(argv)
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    splits = v9.session_calendar()
    exact_path = v9.OUTPUT_DIR / "exact_candidate_universe.parquet"
    raw_path = v9.OUTPUT_DIR / "entry_engine_raw.parquet"
    cache_manifest_path = v9.OUTPUT_DIR / "exact_cache_manifest.json"
    exact = pd.read_parquet(exact_path)
    raw = pd.read_parquet(raw_path)
    v9.validate_exact_cache_manifest(cache_manifest_path, exact, raw)
    seed = load_seed()
    ledger, champion, selected = run_local_search(
        exact, splits["all"], seed, int(args.trials)
    )
    trades = v9.selected_trade_frame(exact, selected)
    daily = v9.daily_results(trades, splits["all"])
    full = v9.detailed_performance(trades, splits["all"])
    periods = {
        "development_slice": v9.detailed_performance(trades, splits["development"]),
        "validation_slice": v9.detailed_performance(trades, splits["validation"]),
        "formerly_locked_test_slice": v9.detailed_performance(trades, splits["test"]),
        "full_month_in_sample": full,
    }
    cost_stress = [
        v9.detailed_performance(trades, splits["all"], cost_multiplier=value)
        for value in (1.0, 1.25, 1.50, 2.0)
    ]
    arrays = v9.SearchArrays(exact, splits["all"])
    perturbations = []
    for label, candidate in v9.perturbation_configs(champion):
        candidate_trades = v9.selected_trade_frame(exact, arrays.selected_indices(candidate))
        perturbations.append({"stress": label, **v9.detailed_performance(candidate_trades, splits["all"])})
    total_configuration_trials = 100_000 + int(args.trials)
    bootstrap = day_block_bootstrap(
        trades,
        splits["all"],
        searched_configurations=total_configuration_trials,
    )
    summary = {
        "research_only": True,
        "production_approved": False,
        "posthoc_in_sample": True,
        "requires_fresh_holdout": True,
        "verdict": "POSTHOC_CANDIDATE_NOT_VALIDATED",
        "search": {
            "global_trials_preceding_this_run": 100_000,
            "local_trials": int(args.trials),
            "total_configuration_trials": total_configuration_trials,
            "seed_config_id": SEED_CONFIG_ID,
            "test_reused_for_refinement": True,
        },
        "champion": {**v9.json_safe(asdict(champion)), "config_sha256": v9.config_hash(champion)},
        "period_results": periods,
        "cost_stress": cost_stress,
        "bootstrap": bootstrap,
        "perturbation_count": len(perturbations),
        "perturbations_pf_ge_1p2": int(sum(row["profit_factor"] >= 1.2 for row in perturbations)),
        "execution_contract": {
            "hourly_prefilter_changed": False,
            "prefilter_side": "LONG",
            "signal": "completed_5min",
            "entry": "exact_next_available_1min",
            "exit": "exact_1min_with_conservative_5min_gap_fallback",
            "statutory_costs": True,
            "stop_loss_pct": v9.STOP_LOSS_PCT,
            "target_pct": v9.TARGET_PCT,
            "daily_cap": v9.DAILY_CAP,
            "one_ticker_per_day": True,
            "complete_1m_grid_rows_in_universe": int((~exact["path_fallback_applied"]).sum()),
            "five_minute_fallback_rows_in_universe": int(exact["path_fallback_applied"].sum()),
            "selected_five_minute_fallback_rows": int(trades["path_fallback_applied"].sum()),
            "selected_source_max_window_incomplete_rows": int(
                trades["max_window_complete"].eq(False).sum()
            ),
            "max_drawdown_basis": full["max_drawdown_basis"],
        },
    }

    ledger.to_parquet(out / "posthoc_local_trial_ledger.parquet", index=False)
    ledger.sort_values("robust_score", ascending=False).head(5000).to_csv(
        out / "top_5000_posthoc_local_trials.csv", index=False
    )
    trades.to_csv(out / "posthoc_champion_trades.csv", index=False)
    daily.to_csv(out / "posthoc_champion_daily_results.csv", index=False)
    pd.DataFrame(cost_stress).to_csv(out / "posthoc_cost_stress.csv", index=False)
    pd.DataFrame(perturbations).to_csv(out / "posthoc_logic_perturbations.csv", index=False)
    write_posthoc_config(out / "posthoc_one_month_long_setup_conf.py", champion)
    (out / "posthoc_summary.json").write_text(
        json.dumps(v9.json_safe(summary), indent=2), encoding="utf-8"
    )
    report = f"""# Post-hoc one-month LONG setup refinement

This is **in-sample research, not validation**.  The full month was reused
after V9's locked winner failed.

- Trials: {summary['search']['total_configuration_trials']:,} total ({args.trials:,} local).
- Trades: {full['trades']} ({full['trades_per_session']:.2f}/session; median {full['median_trades_per_session']:.1f}).
- Active days: {full['active_days']}/22.
- Net P&L: Rs {full['net_pnl_rs']:,.2f}.
- PF: {full['profit_factor']:.3f}.
- Win rate: {full['win_rate_pct']:.1f}%.
- Max drawdown: Rs {full['max_drawdown_rs']:,.2f}.

`PRODUCTION_APPROVED=False`.  A fresh future holdout is mandatory.
"""
    (out / "POSTHOC_RESEARCH_REPORT.md").write_text(report, encoding="utf-8")
    artifacts = [
        "posthoc_local_trial_ledger.parquet", "top_5000_posthoc_local_trials.csv",
        "posthoc_champion_trades.csv", "posthoc_champion_daily_results.csv",
        "posthoc_cost_stress.csv", "posthoc_logic_perturbations.csv",
        "posthoc_one_month_long_setup_conf.py", "posthoc_summary.json",
        "POSTHOC_RESEARCH_REPORT.md",
    ]
    manifest = {
        "artifacts": {
            name: {"sha256": v9.sha256(out / name), "bytes": (out / name).stat().st_size}
            for name in artifacts
        },
        "sources": {
            str(Path(__file__).resolve()): v9.sha256(Path(__file__).resolve()),
            str(Path(v9.__file__).resolve()): v9.sha256(Path(v9.__file__).resolve()),
            str((v9.OUTPUT_DIR / "exact_candidate_universe.parquet").resolve()): v9.sha256(
                v9.OUTPUT_DIR / "exact_candidate_universe.parquet"
            ),
            str(cache_manifest_path.resolve()): v9.sha256(cache_manifest_path),
        },
    }
    (out / "integrity_manifest.json").write_text(
        json.dumps(v9.json_safe(manifest), indent=2), encoding="utf-8"
    )
    print(json.dumps(v9.json_safe(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
