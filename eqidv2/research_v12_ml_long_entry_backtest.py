"""Research-only V12 replay of the frozen prefilter long-entry rule.

The signal was learned in a separate six-month research package.  This module
does not alter V12 production configuration.  It replays the signal through
the compatible V12 global execution/economic layers and writes auditable
CSV/JSON/Markdown artifacts.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import avwap_5min_ID_v12_backtesting as v12
import research_v12_prefilter_train_test_optimizer as optimizer


START_DATE = "2026-06-04"
END_DATE = "2026-08-03"
TRAIN_END = "2026-07-06"
VALIDATION_START = "2026-07-07"
SETUP = "ML_LONG_VOL_EXPANSION_VWAP_V1"
PRODUCTION_APPROVED = False

SOURCE_CACHE = Path(
    r"C:\TradingData\eqidv2_experiments\prefilter_long_5m_gt5pct_20260205_20260804"
    r"\causal_entry_opportunities_v2.parquet"
)
CALENDAR_CSV = Path(
    r"C:\TradingData\eqidv2_experiments\v12_prefilter_2mo_20260604_20260803_k300"
    r"\combined\calendar_daily.csv"
)
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_ml_long_entry_backtest_20260604_20260803"
)

PRIMARY_SL_PCT = 0.90
PRIMARY_TARGET_PCT = 1.50
SL_GRID = (0.50, 0.70, 0.90, 1.10, 1.30, 1.50)
TARGET_GRID = (0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50, 3.00)
DAILY_CAP = 15


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _profit_factor(values: pd.Series) -> float:
    pnl = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(pnl.loc[pnl > 0].sum())
    losses = float(-pnl.loc[pnl < 0].sum())
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return gains / losses


def load_sessions() -> list[str]:
    calendar = pd.read_csv(CALENDAR_CSV)
    sessions = calendar["date"].astype(str)
    return sessions.loc[sessions.between(START_DATE, END_DATE)].tolist()


def load_signal_candidates() -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    columns = [
        "ticker", "membership_slot_ist", "membership_hour", "selection_rank",
        "selection_bucket", "primary_side", "selection_reason", "overall_score",
        "long_score", "activity_score", "staleness_seconds", "signal_time_ist",
        "trade_date", "signal_open", "signal_high", "signal_low", "signal_close",
        "signal_volume", "signal_minute", "RSI", "ADX", "CCI", "MFI",
        "atr_pct", "vwap_dist_atr", "ema20_dist_atr", "ema50_dist_atr",
        "ema200_dist_atr", "macd_hist_atr", "bb_width_atr", "ret_5m_pct",
        "ret_15m_pct", "ret_30m_pct", "ret_60m_pct",
        "session_return_so_far_pct", "gap_pct", "range_pct", "body_pct",
        "upper_wick_pct", "lower_wick_pct", "close_position_in_bar",
        "volume_ratio20", "traded_value_rs", "entry_execution_time_ist",
        "entry_price", "hit_5pct", "max_forward_return_pct", "eod_return_pct",
        "max_window_complete", "pre_entry_data_invalid",
    ]
    source = pd.read_parquet(SOURCE_CACHE, columns=columns)
    funnel: list[dict[str, Any]] = []

    def retain(label: str, mask: pd.Series) -> None:
        nonlocal source
        before = len(source)
        source = source.loc[mask.fillna(False)].copy()
        funnel.append({"stage": label, "before": before, "after": len(source), "removed": before - len(source)})

    retain("two_month_session_window", source["trade_date"].astype(str).between(START_DATE, END_DATE))
    # Do not gate on max_window_complete: it is a future/outcome-availability
    # field and is not knowable at entry time.  V12's own 1-minute entry and
    # exit coverage checks determine whether a trade is executable/resolvable.
    retain("causal_pre_entry_data_valid", source["pre_entry_data_invalid"].eq(False))
    retain("prefilter_primary_side_long", source["primary_side"].astype(str).str.upper().eq("LONG"))
    ranks = pd.to_numeric(source["selection_rank"], errors="coerce")
    retain("hourly_prefilter_rank_200_300_inclusive", ranks.between(200, 300, inclusive="both"))
    minutes = pd.to_numeric(source["signal_minute"], errors="coerce")
    retain("v12_scan_not_before_09_30", minutes >= 570)
    minutes = pd.to_numeric(source["signal_minute"], errors="coerce")
    retain("frozen_signal_not_after_14_15", minutes <= 855)
    retain("atr_pct_gte_1_05", pd.to_numeric(source["atr_pct"], errors="coerce") >= 1.05)
    retain("range_pct_gte_1_25", pd.to_numeric(source["range_pct"], errors="coerce") >= 1.25)
    retain("vwap_dist_atr_gte_0_05", pd.to_numeric(source["vwap_dist_atr"], errors="coerce") >= 0.05)

    source["setup"] = SETUP
    source["side"] = "LONG"
    source["bar_time_ist"] = source["signal_time_ist"]
    source["decision_ready_at_ist"] = source["signal_time_ist"]
    source["decision_ready_source"] = "completed_5min_signal_bar"
    source["quality_score"] = 301.0 - pd.to_numeric(source["selection_rank"], errors="coerce")
    source["score"] = source["quality_score"]
    source["research_source_entry_time_ist"] = source["entry_execution_time_ist"]
    source["research_source_entry_price"] = source["entry_price"]
    source = source.sort_values(
        ["trade_date", "signal_time_ist", "selection_rank", "ticker"],
        kind="mergesort",
    ).reset_index(drop=True)
    source["_optimizer_row_id"] = np.arange(len(source), dtype=int)
    return source, funnel


def apply_daily_cap(selected: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if selected.empty:
        return selected.copy(), selected.copy()
    work = selected.copy()
    work["_entry_ts"] = pd.to_datetime(work["v7_signal_entry_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    work["trade_date"] = work["_entry_ts"].dt.strftime("%Y-%m-%d")
    work = work.sort_values(
        ["trade_date", "_entry_ts", "selection_rank", "ticker"],
        kind="mergesort",
    )
    work["daily_sequence"] = work.groupby("trade_date", sort=False).cumcount() + 1
    accepted = work.loc[work["daily_sequence"] <= DAILY_CAP].copy()
    rejected = work.loc[work["daily_sequence"] > DAILY_CAP].copy()
    if not rejected.empty:
        rejected["reject_reason"] = f"portfolio_daily_cap_{DAILY_CAP}"
    return (
        accepted.drop(columns=["_entry_ts"], errors="ignore").reset_index(drop=True),
        rejected.drop(columns=["_entry_ts"], errors="ignore").reset_index(drop=True),
    )


def metrics(trades: pd.DataFrame, sessions: Iterable[str]) -> dict[str, Any]:
    sessions = list(sessions)
    day = pd.DataFrame({"trade_date": sessions})
    if trades.empty:
        grouped = pd.DataFrame(columns=["trade_date", "trades", "gross_pnl_rs", "cost_rs", "net_pnl_rs"])
    else:
        grouped = trades.groupby("trade_date", as_index=False).agg(
            trades=("ticker", "size"), gross_pnl_rs=("gross_pnl_rs", "sum"),
            cost_rs=("cost_rs", "sum"), net_pnl_rs=("net_pnl_rs", "sum"),
        )
    day = day.merge(grouped, on="trade_date", how="left").fillna(0)
    day["trades"] = day["trades"].astype(int)
    day["cum_pnl_rs"] = day["net_pnl_rs"].cumsum()
    day["drawdown_rs"] = day["cum_pnl_rs"] - day["cum_pnl_rs"].cummax().clip(lower=0)
    pnl = pd.to_numeric(trades.get("net_pnl_rs", pd.Series(dtype=float)), errors="coerce")
    return {
        "trades": int(len(trades)), "sessions": len(sessions),
        "trades_per_session": float(len(trades) / len(sessions)) if sessions else 0.0,
        "median_trades_per_session": float(day["trades"].median()) if len(day) else 0.0,
        "maximum_trades_in_session": int(day["trades"].max()) if len(day) else 0,
        "zero_trade_sessions": int(day["trades"].eq(0).sum()),
        "positive_sessions": int(day["net_pnl_rs"].gt(0).sum()),
        "gross_pnl_rs": float(trades.get("gross_pnl_rs", pd.Series(dtype=float)).sum()),
        "cost_rs": float(trades.get("cost_rs", pd.Series(dtype=float)).sum()),
        "net_pnl_rs": float(pnl.sum()), "profit_factor": _profit_factor(pnl),
        "win_rate_pct": float(pnl.gt(0).mean() * 100) if len(pnl) else 0.0,
        "mean_net_pnl_per_trade_rs": float(pnl.mean()) if len(pnl) else 0.0,
        "median_net_pnl_per_trade_rs": float(pnl.median()) if len(pnl) else 0.0,
        "max_drawdown_rs": float(day["drawdown_rs"].min()) if len(day) else 0.0,
    }


def daily_summary(trades: pd.DataFrame, sessions: list[str]) -> pd.DataFrame:
    day = pd.DataFrame({"trade_date": sessions})
    grouped = trades.groupby("trade_date", as_index=False).agg(
        trades=("ticker", "size"), gross_pnl_rs=("gross_pnl_rs", "sum"),
        cost_rs=("cost_rs", "sum"), net_pnl_rs=("net_pnl_rs", "sum"),
    ) if not trades.empty else pd.DataFrame(columns=["trade_date", "trades", "gross_pnl_rs", "cost_rs", "net_pnl_rs"])
    out = day.merge(grouped, on="trade_date", how="left").fillna(0)
    out["trades"] = out["trades"].astype(int)
    out["cum_pnl_rs"] = out["net_pnl_rs"].cumsum()
    out["drawdown_rs"] = out["cum_pnl_rs"] - out["cum_pnl_rs"].cummax().clip(lower=0)
    return out


def grid_summary(outcomes: pd.DataFrame, sessions: list[str]) -> pd.DataFrame:
    rows = []
    for (sl_pct, tgt_pct), group in outcomes.groupby(["sl_pct", "tgt_pct"], sort=True):
        record = metrics(group, sessions)
        record.update({"sl_pct": float(sl_pct), "tgt_pct": float(tgt_pct)})
        rows.append(record)
    return pd.DataFrame(rows).sort_values(
        ["net_pnl_rs", "profit_factor"], ascending=[False, False], kind="mergesort"
    ).reset_index(drop=True)


def hourly_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["signal_hour", "trades", "gross_pnl_rs", "cost_rs", "net_pnl_rs", "profit_factor", "win_rate_pct"])
    work = trades.copy()
    signal_ts = pd.to_datetime(work["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    work["signal_hour"] = signal_ts.dt.strftime("%H:00")
    rows = []
    for hour, group in work.groupby("signal_hour", sort=True):
        rows.append({
            "signal_hour": hour, "trades": int(len(group)),
            "gross_pnl_rs": float(group["gross_pnl_rs"].sum()),
            "cost_rs": float(group["cost_rs"].sum()),
            "net_pnl_rs": float(group["net_pnl_rs"].sum()),
            "profit_factor": _profit_factor(group["net_pnl_rs"]),
            "win_rate_pct": float(group["net_pnl_rs"].gt(0).mean() * 100),
        })
    return pd.DataFrame(rows)


def write_report(summary: dict[str, Any], grid: pd.DataFrame, output: Path) -> None:
    p = summary["primary_exit_results"]
    best = grid.iloc[0].to_dict() if not grid.empty else {}
    verdict = "profitable" if p["net_pnl_rs"] > 0 and (p["profit_factor"] or 0) >= 1 else "not profitable"
    text = f"""# V12 replay — frozen ML long entry

## Conclusion

The predeclared **0.90% stop / 1.50% target** V12 replay was **{verdict}** over {p['sessions']} sessions: {p['trades']} trades, net P&L **Rs {p['net_pnl_rs']:,.2f}**, profit factor **{p['profit_factor'] or 0:.3f}**, win rate **{p['win_rate_pct']:.2f}%**, and max drawdown **Rs {p['max_drawdown_rs']:,.2f}**.

This is an in-sample historical replay of a rule learned from a six-month window that includes these dates. It is not a fresh holdout and cannot justify production promotion.

## Compatible V12 pipeline applied

- causal hourly LONG prefilter membership, ranks 200–300 inclusive;
- completed 5-minute rule: ATR% >= 1.05, bar range% >= 1.25, VWAP distance/ATR >= 0.05;
- V12 scan window 09:30 through the frozen rule's 14:15 cutoff;
- V12 next-available 1-minute-open entry (signal + 1 minute, maximum one-minute delay);
- V12 one-entry-per-ticker/day selection, then a chronological 15-trade daily cap;
- V12 risk sizing (Rs 200,000 equity, 0.25% risk budget), V7 5 bps adverse entry fill;
- exact 1-minute stop/target/EOD resolution and statutory intraday-equity costs.

Legacy setup-name-specific masks, pre-momentum gates, and special entry policies were not applied: they belong to other named chart patterns and are not valid filters for this new signal. There is no new post-entry ML filter.

## Frequency and split results

- Overall: {p['trades_per_session']:.2f} trades/session; median {p['median_trades_per_session']:.1f}; maximum {p['maximum_trades_in_session']}; {p['zero_trade_sessions']} zero-trade sessions.
- TRAIN-era slice (Jun 4–Jul 6): net Rs {summary['split_results']['train_era']['net_pnl_rs']:,.2f}, PF {summary['split_results']['train_era']['profit_factor'] or 0:.3f}.
- Validation-era slice (Jul 7–Aug 3): net Rs {summary['split_results']['validation_era']['net_pnl_rs']:,.2f}, PF {summary['split_results']['validation_era']['profit_factor'] or 0:.3f}.

## Exit sensitivity (exploratory only)

The full predeclared 48-combination grid is in `exit_grid_summary.csv`. The best historical combination by net P&L was SL {best.get('sl_pct', 0):.2f}% / target {best.get('tgt_pct', 0):.2f}%: net Rs {best.get('net_pnl_rs', 0):,.2f}, PF {(best.get('profit_factor') or 0):.3f}. This is post-hoc sensitivity, not a selected or approved exit.

## Safety status

`PRODUCTION_APPROVED = False`. No live configuration, service, or process was changed or restarted.
"""
    output.write_text(text, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sessions = load_sessions()
    if len(sessions) != 41 or sessions[0] != START_DATE or sessions[-1] != END_DATE:
        raise RuntimeError(f"unexpected session calendar: {len(sessions)} {sessions[:1]} {sessions[-1:]}")

    candidates, funnel = load_signal_candidates()
    loader = optimizer.install_windowed_1m_loader(v12, start_date=START_DATE, end_date=END_DATE)
    prewarm = optimizer.prewarm_windowed_1m_loader(loader, candidates["ticker"], workers=8)
    optimizer.install_day_1m_adapter(v12, loader)
    v12._V11_EXACT_LIVE_PARITY = False
    v12._V11_COST_MODEL = "statutory"
    v12._V11_SLIPPAGE_BPS = 0.0

    old_exit = v12.v6.SETUP_EXIT_RULES.get(SETUP)
    v12.v6.SETUP_EXIT_RULES[SETUP] = (PRIMARY_SL_PCT, PRIMARY_TARGET_PCT)
    try:
        raw, entry_rejects = v12._v7_entry_engine_raw_rows(candidates)
        selected_pre_cap = v12._select_v7_entry_engine_signals(raw)
    finally:
        if old_exit is None:
            v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v12.v6.SETUP_EXIT_RULES[SETUP] = old_exit

    selected, cap_rejects = apply_daily_cap(selected_pre_cap)
    funnel.extend([
        {"stage": "v12_executable_1min_entry", "before": len(candidates), "after": len(raw), "removed": len(candidates) - len(raw)},
        {"stage": "v12_one_ticker_per_day", "before": len(raw), "after": len(selected_pre_cap), "removed": len(raw) - len(selected_pre_cap)},
        {"stage": f"portfolio_daily_cap_{DAILY_CAP}", "before": len(selected_pre_cap), "after": len(selected), "removed": len(selected_pre_cap) - len(selected)},
    ])
    if selected.empty:
        raise RuntimeError("no selected V12 entries")
    selected["_optimizer_row_id"] = pd.to_numeric(selected["_optimizer_row_id"], errors="raise").astype(int)

    exits = {SETUP: [(sl, target) for sl in SL_GRID for target in TARGET_GRID]}
    outcomes = optimizer.resolve_exit_grid(selected, exits, v12, progress_label="ml-long-v12")
    expected = len(selected) * len(SL_GRID) * len(TARGET_GRID)
    if len(outcomes) != expected:
        raise RuntimeError(f"exit coverage failure: {len(outcomes)}/{expected}")
    primary = outcomes.loc[
        outcomes["sl_pct"].eq(PRIMARY_SL_PCT) & outcomes["tgt_pct"].eq(PRIMARY_TARGET_PCT)
    ].copy()
    selected_context = selected.drop(columns=["trade_date"], errors="ignore")
    primary = primary.merge(
        selected_context,
        on=["_optimizer_row_id", "ticker", "side", "setup", "signal_time_ist"],
        how="left", validate="one_to_one", suffixes=("", "_signal"),
    )
    primary = primary.sort_values(["trade_date", "entry_time_ist", "ticker"], kind="mergesort").reset_index(drop=True)

    train_sessions = [d for d in sessions if d <= TRAIN_END]
    validation_sessions = [d for d in sessions if d >= VALIDATION_START]
    primary_metrics = metrics(primary, sessions)
    split = {
        "train_era": metrics(primary.loc[primary["trade_date"].le(TRAIN_END)], train_sessions),
        "validation_era": metrics(primary.loc[primary["trade_date"].ge(VALIDATION_START)], validation_sessions),
    }
    grid = grid_summary(outcomes, sessions)
    daily = daily_summary(primary, sessions)
    monthly_rows = []
    for month, days in daily.groupby(daily["trade_date"].str[:7], sort=True):
        month_trades = primary.loc[primary["trade_date"].str.startswith(month)]
        record = metrics(month_trades, days["trade_date"].tolist())
        record["month"] = month
        monthly_rows.append(record)
    monthly = pd.DataFrame(monthly_rows)
    hourly = hourly_summary(primary)

    timing = selected[[
        "_optimizer_row_id", "ticker", "signal_time_ist", "v7_signal_entry_time_ist",
        "v7_signal_entry_price", "research_source_entry_time_ist", "research_source_entry_price",
    ]].copy()
    timing["v12_minus_signal_minutes"] = (
        pd.to_datetime(timing["v7_signal_entry_time_ist"], utc=True)
        - pd.to_datetime(timing["signal_time_ist"], utc=True)
    ).dt.total_seconds() / 60
    timing["v12_minus_research_entry_minutes"] = (
        pd.to_datetime(timing["v7_signal_entry_time_ist"], utc=True)
        - pd.to_datetime(timing["research_source_entry_time_ist"], utc=True)
    ).dt.total_seconds() / 60

    contract = {
        "production_approved": PRODUCTION_APPROVED,
        "research_only": True, "setup": SETUP,
        "date_window": {"start": START_DATE, "end": END_DATE, "sessions": len(sessions)},
        "prefilter": {"primary_side": "LONG", "rank_min_inclusive": 200, "rank_max_inclusive": 300, "membership": "causal hourly snapshot +5 through +60 minutes"},
        "entry_rule": {"timeframe": "completed 5-minute", "atr_pct_gte": 1.05, "range_pct_gte": 1.25, "vwap_dist_atr_gte": 0.05, "signal_minute_min": 570, "signal_minute_max": 855},
        "v12_execution": {"entry": "next available 1-minute open after signal", "maximum_delay_minutes": 1, "one_entry_per_ticker_per_day": True, "daily_cap": DAILY_CAP, "risk_equity_rs": v12.RISK_EQUITY_RS, "risk_pct_per_trade": v12.RISK_PCT_PER_TRADE, "entry_slippage_pct": v12.V7_PAPER_SLIPPAGE_PCT, "cost_model": "NSE statutory intraday equity"},
        "primary_placeholder_exit": {"sl_pct": PRIMARY_SL_PCT, "target_pct": PRIMARY_TARGET_PCT},
        "exit_sensitivity": {"sl_pct": SL_GRID, "target_pct": TARGET_GRID, "status": "exploratory_not_promoted"},
        "incompatible_filters_not_applied": ["legacy named-setup masks", "legacy named-setup pre-entry momentum gates", "legacy named-setup special entry policies"],
        "honesty": "Signal thresholds were learned using a six-month sample containing this two-month window; results are not fresh holdout evidence. Future outcome fields (including max_window_complete and hit_5pct) are not entry gates.",
    }
    summary = {
        "contract": contract, "candidate_funnel": funnel,
        "prewarm_1m": prewarm, "primary_exit_results": primary_metrics,
        "split_results": split,
        "entry_engine_reject_reasons": entry_rejects.get("reject_reason", pd.Series(dtype=str)).value_counts().to_dict(),
        "daily_cap_rejects": int(len(cap_rejects)),
        "exit_grid_best_by_historical_net": grid.iloc[0].to_dict(),
    }

    pd.DataFrame(funnel).to_csv(OUTPUT_DIR / "candidate_funnel.csv", index=False)
    if entry_rejects.empty:
        entry_rejects = pd.DataFrame(columns=["ticker", "side", "setup", "signal_time_ist", "candidate_id", "reject_reason"])
    entry_rejects.to_csv(OUTPUT_DIR / "entry_engine_rejects.csv", index=False)
    cap_rejects.to_csv(OUTPUT_DIR / "daily_cap_rejects.csv", index=False)
    selected.to_csv(OUTPUT_DIR / "v12_selected_entries.csv", index=False)
    outcomes.to_csv(OUTPUT_DIR / "exit_grid_outcomes.csv", index=False)
    grid.to_csv(OUTPUT_DIR / "exit_grid_summary.csv", index=False)
    primary.to_csv(OUTPUT_DIR / "primary_trades.csv", index=False)
    daily.to_csv(OUTPUT_DIR / "daily_summary.csv", index=False)
    monthly.to_csv(OUTPUT_DIR / "monthly_summary.csv", index=False)
    hourly.to_csv(OUTPUT_DIR / "hourly_summary.csv", index=False)
    timing.to_csv(OUTPUT_DIR / "entry_timing_audit.csv", index=False)
    (OUTPUT_DIR / "filter_contract.json").write_text(json.dumps(_json_value(contract), indent=2), encoding="utf-8")
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(_json_value(summary), indent=2), encoding="utf-8")
    write_report(summary, grid, OUTPUT_DIR / "RESEARCH_REPORT.md")

    artifacts = []
    for path in sorted(OUTPUT_DIR.iterdir()):
        if path.is_file() and path.name != "integrity_manifest.json":
            artifacts.append({"file": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    manifest = {"production_approved": False, "artifact_count": len(artifacts), "artifacts": artifacts}
    (OUTPUT_DIR / "integrity_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(_json_value(summary), indent=2))


if __name__ == "__main__":
    main()
