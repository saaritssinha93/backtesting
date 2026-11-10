"""Exploratory portfolio replay of causal 1% momentum alerts beside G3 V19.

This is a NEW signal family, not a change to the frozen G3 setup contract.
Alert generation never uses final-day mover labels. The fixed 1.5% target is
an explicit research assumption; stop, trigger expiry, costs, and portfolio
constraints use the inherited G3 simulator.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from fno_v13_v10_g_3_daily_replay import simulate
from g3_v18_20261102 import data
from g3_v18_20261102.run import metrics
from g3_v19_uncapped_20261103 import selection as v19


PACKAGE = Path(__file__).resolve().parent
INPUT = PACKAGE / "results_full_20261104"
OUTPUT = PACKAGE / "portfolio_trial_20261104"
TARGET_PCT = 1.5
STOP_PCT = 1.25
VERSION = "G3-V22-20261104-MOMENTUM-ALERT-PORTFOLIO-TRIAL"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def _orders(alerts: pd.DataFrame, baseline: pd.DataFrame, day: str, day_index: int) -> pd.DataFrame:
    existing = set(zip(baseline.tradingsymbol, baseline.side))
    daily = alerts.loc[alerts.day.eq(day)].copy()
    daily = daily.loc[[(symbol, side) not in existing
                       for symbol, side in zip(daily.tradingsymbol, daily.side)]].copy()
    daily = daily.sort_values(["confirmation_ts", "tradingsymbol", "side"]).reset_index(drop=True)
    if daily.empty:
        return pd.DataFrame()
    daily["day"] = date.fromisoformat(day)
    daily["sid"] = day_index * 1_000_000 + 500_000 + np.arange(len(daily), dtype=np.int64)
    daily["signal_id"] = [f"{VERSION}|{day}|{symbol}|{side}|{stamp}"
                          for symbol, side, stamp in zip(
                              daily.tradingsymbol, daily.side, daily.confirmation_ts)]
    daily["signal_ts"] = pd.to_datetime(daily.first_cross_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    daily["confirmation_ts"] = pd.to_datetime(daily.confirmation_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    daily["hhmm"] = daily.signal_ts.dt.strftime("%H%M")
    daily["hhmm_int"] = daily.hhmm.astype(int)
    daily["setup_id"] = [f"MOMENTUM_1PCT_{side}" for side in daily.side]
    daily["picker"] = "max_move"
    daily["price_change_pct"] = (daily.confirmation_close / daily.first_open - 1) * 100
    daily["traded_value"] = 0.0
    daily["trigger"] = np.where(daily.side.eq("LONG"),
                                daily.confirmation_high, daily.confirmation_low)
    daily["native_stop_pct"] = STOP_PCT
    daily["native_target_pct"] = TARGET_PCT
    daily["stop_pct"] = STOP_PCT
    daily["target_pct"] = TARGET_PCT
    daily["order_origin"] = "NEW_1PCT_MOMENTUM_ALERT"
    daily["research_variant"] = VERSION
    daily["execution_authority"] = False
    return daily


def run() -> None:
    if OUTPUT.exists():
        raise FileExistsError(f"Output already exists: {OUTPUT}")
    summary = json.loads((INPUT / "summary.json").read_text(encoding="utf-8"))
    alerts = pd.read_csv(INPUT / "alerts_within_five_minutes.csv", low_memory=False)
    if len(alerts) != summary["scenarios"]["within_five_minutes"]["universe_alerts"]:
        raise ValueError("Alert count differs from sealed scanner summary")
    days = data.available_days("2026-11-04")
    if len(days) != summary["sessions"] or days[-1] != summary["latest_sealed_day"]:
        raise ValueError("Sealed day cohort differs from alert scan")
    top = pd.read_csv(INPUT / "top2_cases_within_five_minutes.csv", low_memory=False)
    books, sources = [], []
    for day_index, day in enumerate(days):
        raw, minutes, proof = data.load_day(day)
        raw = raw.reset_index(drop=True)
        raw["sid"] = day_index * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        baseline, _, _ = v19.select(raw)
        baseline = baseline.copy()
        baseline["order_origin"] = "G3_V19_BASELINE"
        momentum = _orders(alerts, baseline, day, day_index)
        orders = pd.concat([baseline, momentum], ignore_index=True, sort=False)
        if orders.sid.duplicated().any() or orders.signal_id.duplicated().any():
            raise ValueError("Duplicate portfolio trial identity")
        paths = data.execution_paths(orders, minutes)
        book, _ = simulate(orders, paths, [date.fromisoformat(day)])
        book["research_arm"] = VERSION
        book["execution_authority"] = False
        books.append(book)
        sources.append(dict(day=day, feature_stocks=proof["feature_stocks"],
                            minute_stocks=proof["minute_stocks"],
                            loader_source=proof["loader_source"],
                            run_manifest=proof.get("run_manifest"),
                            run_manifest_sha256=proof.get("run_manifest_sha256")))
        print(f"{day} [{day_index+1}/{len(days)}] baseline={len(baseline)} "
              f"new_alert_orders={len(momentum)} executed={int(book.portfolio_executed.sum())}", flush=True)
    ledger = pd.concat(books, ignore_index=True, sort=False)
    measure = metrics(ledger, days)
    prior = pd.read_csv(PACKAGE.parent / "g3_v21_selective_confirmation_20261104/results_full_20261104/overall_comparison.csv")
    base = prior.loc[prior.period.eq("full_64") & prior.arm.eq("v19_uncapped")].iloc[0]
    keys_selected = set(zip(ledger.day.astype(str), ledger.side, ledger.tradingsymbol))
    executed = ledger.loc[ledger.portfolio_executed.astype(str).str.lower().eq("true")].copy()
    keys_executed = set(zip(executed.day.astype(str), executed.side, executed.tradingsymbol))
    top_keys = list(zip(top.day, top.side, top.tradingsymbol))
    top["trial_selected"] = [key in keys_selected for key in top_keys]
    top["trial_executed"] = [key in keys_executed for key in top_keys]
    # Evaluate entry chronology after orders and fills are fixed.
    extreme = pd.to_datetime(top.extreme_time, utc=True).dt.tz_convert("Asia/Kolkata")
    top["extreme_time_ist"] = extreme
    executed["day"] = executed.day.astype(str)
    first_entries = executed.groupby(["day", "side", "tradingsymbol"], sort=False).entry_ts.min()
    top["first_entry_ts"] = [first_entries.get(key, pd.NaT) for key in top_keys]
    first = pd.to_datetime(top.first_entry_ts, errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    top["trial_entry_before_extreme"] = first.notna() & first.lt(extreme)
    # A trade entered after the 3% crossing is reported separately.
    three = pd.to_datetime(top.first_3pct_time, utc=True).dt.tz_convert("Asia/Kolkata")
    top["trial_entry_before_3pct"] = first.notna() & first.lt(three)
    new = ledger.loc[ledger.order_origin.eq("NEW_1PCT_MOMENTUM_ALERT")]
    result = dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        days=days, alert_source=str(INPUT / "alerts_within_five_minutes.csv"),
        alert_source_sha256=_sha(INPUT / "alerts_within_five_minutes.csv"),
        baseline_reference=str(PACKAGE.parent / "g3_v21_selective_confirmation_20261104/results_full_20261104/overall_comparison.csv"),
        target_pct=TARGET_PCT, stop_pct=STOP_PCT,
        unchanged_simulator="G3 fixed capital, staged stop, 10-minute trigger expiry, 5 bps costs",
        baseline_metrics=dict(selected_orders=int(base.selected_orders), trades=int(base.trades),
                              win_rate_pct=float(base.win_rate_pct), net_pnl_rs=float(base.net_pnl_rs),
                              daily_close_drawdown_rs=float(base.daily_close_drawdown_rs)),
        trial_metrics=measure,
        new_alert_orders=len(new), new_alert_executed=int(new.portfolio_executed.sum()),
        top2_cases=len(top), top2_selected=int(top.trial_selected.sum()),
        top2_executed=int(top.trial_executed.sum()),
        top2_entry_before_extreme=int(top.trial_entry_before_extreme.sum()),
        top2_entry_before_3pct=int(top.trial_entry_before_3pct.sum()),
        limitations=["1.5% target is a new research assumption, not a frozen G3 setup target",
                     "Alert condition selected after inspecting the 255 historical mover cases",
                     "Earlier G3 slot allowlist was tuned using history through October 27",
                     "No live execution authority"])
    OUTPUT.mkdir(parents=True)
    ledger.to_csv(OUTPUT / "portfolio_trades.csv", index=False)
    top.to_csv(OUTPUT / "top2_portfolio_comparison.csv", index=False)
    pd.DataFrame([dict(day=day, **metrics(ledger, [day])) for day in days]).to_csv(
        OUTPUT / "daily_results.csv", index=False)
    _dump(OUTPUT / "summary.json", result)
    _dump(OUTPUT / "source_provenance.json", dict(sessions=sources,
        scanner_summary_sha256=_sha(INPUT / "summary.json"),
        runner_source=str(Path(__file__)), runner_sha256=_sha(Path(__file__))))
    _dump(OUTPUT / "manifest.json", {p.name:_sha(p) for p in OUTPUT.iterdir() if p.is_file()})
    print(json.dumps({k:v for k,v in result.items() if k not in ("days", "limitations")}, indent=2), flush=True)


if __name__ == "__main__":
    run()
