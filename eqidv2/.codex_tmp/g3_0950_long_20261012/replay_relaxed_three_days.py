"""Isolated, outcome-blind rule scenarios for 09:50 LONG on Oct 8/9/12."""
from __future__ import annotations

import hashlib
import json
import math
import sys
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_daily_replay as replay
import fno_v13_v10_g_2_extend_results as ext

OUT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_3_0950_long_20261012")
G = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g\runs")
G3 = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\runs")
LIVE = Path(r"C:\TradingData\eqidv2\fno_oi\v13_v10_g_3_live")
RUNS = {
    date(2026, 10, 8): (G / "2026-10-08/20261008T164156506767",
                        G3 / "2026-10-08/20261009T1302_dated_replay"),
    date(2026, 10, 9): (G / "2026-10-09/20261009T162013087486",
                        G3 / "2026-10-09/20261009_postclose_frozen_g3"),
}
DAYS = [*RUNS, date(2026, 10, 12)]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def candidates() -> tuple[pd.DataFrame, dict]:
    rows, sources = [], {}
    for day, (run, g3_run) in RUNS.items():
        summary = json.loads((g3_run / "summary.json").read_text())
        if sha(run / "feature_ledger.csv") != summary["provenance"]["feature_ledger_sha256"]:
            raise RuntimeError(f"Feature ledger hash mismatch: {day}")
        snapshot_manifest = Path(summary["provenance"]["snapshot_manifest"])
        if sha(snapshot_manifest) != summary["provenance"]["snapshot_manifest_sha256"]:
            raise RuntimeError(f"Snapshot manifest hash mismatch: {day}")
        frame = pd.read_csv(run / "feature_ledger.csv", float_precision="round_trip")
        frame = frame.loc[frame.signal_end.eq("09:50") & frame.base_long_pass.astype(str).str.lower().eq("true")]
        sources[str(day)] = dict(feature_ledger=str(run / "feature_ledger.csv"),
                                 feature_ledger_sha256=sha(run / "feature_ledger.csv"),
                                 snapshot_manifest=str(snapshot_manifest),
                                 snapshot_manifest_sha256=sha(snapshot_manifest))
        for r in frame.to_dict("records"):
            rows.append((day, r))
    day = date(2026, 10, 12)
    scanner_path = LIVE / "scanner_5m" / str(day) / "slot_0950.json"
    confirmation_path = LIVE / "confirmation_1m" / str(day) / "slot_0951.json"
    scanner, confirmation = (json.loads(p.read_text()) for p in (scanner_path, confirmation_path))
    if scanner["strategy_fingerprint"] != confirmation["strategy_fingerprint"]:
        raise RuntimeError("Saved live packets disagree")
    wanted = {r["tradingsymbol"] for r in scanner["candidates"] if r["side"] == "LONG"}
    found = {r["tradingsymbol"] for r in confirmation["feature_evaluations"]}
    if wanted != found or len(wanted) != 5:
        raise RuntimeError("Incomplete 09:50 live pool")
    rows.extend((day, r) for r in confirmation["feature_evaluations"])
    sources[str(day)] = dict(scanner=str(scanner_path), scanner_sha256=sha(scanner_path),
                             confirmation=str(confirmation_path), confirmation_sha256=sha(confirmation_path),
                             sail_minutes=str(OUT / "sail_oct12_minutes.parquet"),
                             sail_minutes_sha256=sha(OUT / "sail_oct12_minutes.parquet"))
    normalized = []
    for sid, (day, row) in enumerate(rows, 8_000_000):
        row = row.copy()
        row.update(sid=sid, day=day, side="LONG", signal_timestamp=row["signal_ts"],
                   confirmation_timestamp=row["confirmation_ts"],
                   v9_1m_feature_ts=row["confirmation_ts"],
                   v9_5m_feature_ts=row["signal_ts"],
                   v9_feature_available_ts=row["confirmation_ts"],
                   v9_exact_confirmation_present=True,
                   confirm_open=row["confirmation_open"], confirm_high=row["confirmation_high"],
                   confirm_low=row["confirmation_low"], confirm_close=row["confirmation_close"],
                   trigger=row["confirmation_high"])
        if contract.base_signal_side(row, "09:50", session_date=day) != "LONG":
            raise RuntimeError((day, row["tradingsymbol"], "source base gate disagreement"))
        normalized.append(row)
    return pd.DataFrame(normalized), sources


ORIGINAL_VALID = contract._confirmation_valid


def green_only(row: dict, side: str) -> bool:
    if ORIGINAL_VALID(row, side):
        return True
    if side != "LONG" or any(contract._true(row.get(flag, False)) for flag in
                             ("gap_filled", "opening_snapshot", "provisional_stale", "confirmation_source_flagged")):
        return False
    o, h, lo, c = (contract._number(row.get(k)) for k in
                    ("confirm_open", "confirm_high", "confirm_low", "confirm_close"))
    return all(math.isfinite(x) and x > 0 for x in (o, h, lo, c)) and h > lo and h >= c > o >= lo


def select(frame: pd.DataFrame, *, scenario: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    donor = contract._ACTIVE_BY_ID["0956_LONG"]
    if scenario == "one_minute_only":
        setup = replace(donor, signal_end="09:50", confirmation_end="09:51", body_ratio=.24)
        minute_volume = .70
        contract._confirmation_valid = green_only
        direction = "09:51 close > 09:51 open; may remain below 09:50 signal close"
    elif scenario == "retain_displacement":
        setup = replace(donor, signal_end="09:50", confirmation_end="09:51",
                        price_change_pct=.10, volume_ratio=.80, body_ratio=.30)
        minute_volume = .40
        contract._confirmation_valid = ORIGINAL_VALID
        direction = "09:51 close > 09:51 open and > 09:50 signal close"
    else:
        raise ValueError(scenario)
    contract._CORE_BY_ID[setup.setup_id] = setup
    contract._ACTIVE_BY_ID[setup.setup_id] = setup
    contract.CORE_SETUPS = (*contract.CORE_SETUPS, setup)
    contract.ACTIVE_SETUPS = (*contract.ACTIVE_SETUPS, setup)
    contract.MIN_CONFIRMATION_VOLUME_BY_SIDE["LONG"] = minute_volume
    audit = contract.selection_audit(frame)
    selected = audit.loc[audit.v9_selected.eq(True)].copy()
    if len(selected) != 3 or set(pd.to_datetime(selected.day).dt.date) != set(DAYS):
        raise RuntimeError(f"Expected one selected order per day, got {selected[['day','tradingsymbol']].to_dict('records')}")
    rule = dict(scenario=scenario, direction=direction, setup=asdict(setup),
                confirmation_volume_minimum=minute_volume,
                changes_vs_0955_clone=dict(body_ratio=f"0.40 -> {setup.body_ratio:.2f}",
                    confirmation_volume=f"1.10 -> {minute_volume:.2f}",
                    direction="close > signal close -> green only" if scenario == "one_minute_only" else "unchanged",
                    five_minute_price=f"0.20 -> {setup.price_change_pct:.2f}",
                    five_minute_volume=f"1.00 -> {setup.volume_ratio:.2f}"))
    return audit, selected, rule


def paths_for(selected: pd.DataFrame, sources: dict) -> dict:
    paths = {}
    for day in DAYS:
        order = selected.loc[pd.to_datetime(selected.day).dt.date.eq(day)]
        if len(order) != 1:
            raise RuntimeError(f"Expected one order: {day}")
        if day in RUNS:
            snapshot = Path(sources[str(day)]["snapshot_manifest"]).parent
            paths.update(ext._selected_paths(order, day, snapshot))
        else:
            if order.iloc[0].tradingsymbol != "SAIL":
                raise RuntimeError("Today's selected symbol needs its own minute source")
            minute = pd.read_parquet(OUT / "sail_oct12_minutes.parquet")
            path = minute.loc[minute.ts.gt(pd.Timestamp(order.iloc[0].confirmation_ts)) &
                              minute.ts.le(pd.Timestamp("2026-10-12 15:15", tz=contract.IST))].copy()
            expected = pd.date_range("2026-10-12 09:52", "2026-10-12 15:15", freq="min", tz=contract.IST)
            if len(path) != len(expected) or not path.ts.reset_index(drop=True).eq(pd.Series(expected)).all():
                raise RuntimeError("Incomplete SAIL execution path")
            paths[int(order.iloc[0].sid)] = dict(timestamp_ns=path.ts.astype("int64").to_numpy(),
                **{name:path[name].to_numpy(float) for name in ("open", "high", "low", "close")})
    return paths


def main() -> None:
    frame, sources = candidates()
    results = []
    for scenario in ("one_minute_only", "retain_displacement"):
        audit, selected, rule = select(frame, scenario=scenario)
        paths = paths_for(selected, sources)
        ledger, metric = replay.simulate(selected, paths, DAYS)
        trades = []
        for row in ledger.to_dict("records"):
            trades.append(dict(date=str(row["day"]), symbol=row["tradingsymbol"],
                               selected=True, filled=bool(row["filled"]),
                               executed=bool(row["portfolio_executed"]),
                               trigger=float(row["trigger"]),
                               entry_time=str(row["entry_ts"]) if pd.notna(row["entry_ts"]) else None,
                               entry_price=float(row["entry_price"]) if pd.notna(row["entry_price"]) else None,
                               exit_time=str(row["exit_ts"]) if pd.notna(row["exit_ts"]) else None,
                               exit_price=float(row["exit_price"]) if pd.notna(row["exit_price"]) else None,
                               exit_reason=row["exit_reason"],
                               net_pnl_rs=float(row["portfolio_net_profit_rupees"])))
        results.append(dict(rule=rule, audit_count=len(audit), selected_count=len(selected),
                            executed_count=sum(t["executed"] for t in trades),
                            modeled_net_pnl_rs=sum(t["net_pnl_rs"] for t in trades),
                            trades=trades, simulator_metrics=metric))
        print(scenario, json.dumps(trades, indent=2), flush=True)
    result = dict(scope="09:50 LONG on October 8, 9 and 12, 2026",
                  outcome_status="COUNTERFACTUAL_HINDSIGHT_REUSED_DATES_NO_LIVE_ORDER",
                  target_pct=.90, initial_stop_pct=1.25,
                  tightened_stop_pct=1., tighten_after_minutes=120,
                  entry_expiry_minutes=10, capital_per_entry_rs=100000,
                  leverage=5, round_trip_cost_bps=5,
                  sources=sources, scenarios=results)
    target = OUT / "relaxed_0950_three_day_replay.json"
    target.write_text(json.dumps(result, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    print(target)


if __name__ == "__main__":
    main()
