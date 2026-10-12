"""Research-only stricter 09:50 LONG, with fixed 1% stop and 1% target."""
from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_2_backtest as g2
from research.g3_freeze import DEFAULT_FROZEN
import run_comparison as previous
import replay_relaxed_three_days as recent_replay

OUT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_3_0950_long_20261012")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


VARIANTS = {
    "clone_1pct": dict(price_change_pct=.20, oi_change_pct=.10, volume_ratio=1.,
                        body_ratio=.40, max_wick_ratio=.60, minute_volume=1.10),
    "moderately_strict_1pct": dict(price_change_pct=.30, oi_change_pct=.15, volume_ratio=1.30,
                        body_ratio=.50, max_wick_ratio=.50, minute_volume=1.30),
    "strict_1pct": dict(price_change_pct=.40, oi_change_pct=.20, volume_ratio=1.50,
                        body_ratio=.60, max_wick_ratio=.40, minute_volume=1.50),
}


def configure(name: str) -> contract.SetupSpec:
    donor = contract._ACTIVE_BY_ID["0956_LONG"]
    settings = VARIANTS[name]
    setup = replace(donor, signal_end="09:50", confirmation_end="09:51",
                    price_change_pct=settings["price_change_pct"],
                    oi_change_pct=settings["oi_change_pct"],
                    volume_ratio=settings["volume_ratio"],
                    body_ratio=settings["body_ratio"],
                    max_wick_ratio=settings["max_wick_ratio"],
                    stop_pct=1., target_pct=1.)
    contract.CORE_SETUPS = tuple(s for s in contract.CORE_SETUPS if s.setup_id != "0951_LONG") + (setup,)
    contract.ACTIVE_SETUPS = tuple(s for s in contract.ACTIVE_SETUPS if s.setup_id != "0951_LONG") + (setup,)
    contract._CORE_BY_ID[setup.setup_id] = setup
    contract._ACTIVE_BY_ID[setup.setup_id] = setup
    contract.MIN_CONFIRMATION_VOLUME_BY_SIDE["LONG"] = settings["minute_volume"]
    return setup


def simulate(orders: pd.DataFrame, paths: dict) -> pd.DataFrame:
    if orders.empty:
        return orders.copy()
    orders = orders.copy()
    orders["native_stop_pct"] = 1.
    orders["native_target_pct"] = 1.
    orders["initial_stop_pct"] = 1.
    orders["tightened_stop_pct"] = 1.
    g2.g.v9.validate_paths(orders, paths)
    trades = g2.g.v9.v5.simulate_native(orders, paths, cost_bps=5., max_entry_delay_minutes=10)
    trades = g2.g.v9.v5.apply_fixed_capital_model(trades, 100_000., 5.)
    base = g2.g.v9.V9Config(portfolio_capital_rupees=1_000_000.,
        capital_per_entry_rupees=100_000., leverage_factor=5., max_positions=None, cost_bps=5.)
    return g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())[0]


def result(name: str, setup: contract.SetupSpec, ledger: pd.DataFrame, days: list[date]) -> dict:
    selected = ledger.loc[ledger.portfolio_executed.eq(True)] if len(ledger) else ledger
    pnl = selected.portfolio_net_profit_rupees.astype(float) if len(selected) else pd.Series(dtype=float)
    training = set(days[:30])
    late = set(days[30:])
    byday = pd.to_datetime(selected.day).dt.date if len(selected) else pd.Series(dtype=object)
    buckets = {}
    for label, group in (("first_30", training), ("later_16", late)):
        sub = selected.loc[byday.isin(group)] if len(selected) else selected
        net = sub.portfolio_net_profit_rupees.astype(float) if len(sub) else pd.Series(dtype=float)
        buckets[label] = dict(sessions=len(group), fills=len(sub), wins=int(net.gt(0).sum()),
                              losses=int(net.lt(0).sum()), net_pnl_rs=float(net.sum()),
                              win_rate_pct=100*float(net.gt(0).mean()) if len(net) else None)
    rows = []
    for trade in ledger.to_dict("records"):
        rows.append(dict(date=str(trade["day"]), symbol=trade["tradingsymbol"],
                         filled=bool(trade["filled"]), executed=bool(trade["portfolio_executed"]),
                         exit_reason=trade.get("exit_reason"),
                         entry_time=str(trade["entry_ts"]) if pd.notna(trade["entry_ts"]) else None,
                         exit_time=str(trade["exit_ts"]) if pd.notna(trade["exit_ts"]) else None,
                         entry_price=float(trade["entry_price"]) if pd.notna(trade["entry_price"]) else None,
                         exit_price=float(trade["exit_price"]) if pd.notna(trade["exit_price"]) else None,
                         net_pnl_rs=float(trade["portfolio_net_profit_rupees"])))
    return dict(name=name, setup=asdict(setup), minute_volume_min=VARIANTS[name]["minute_volume"],
                selected_orders=len(ledger), fills=len(selected), wins=int(pnl.gt(0).sum()),
                losses=int(pnl.lt(0).sum()), win_rate_pct=100*float(pnl.gt(0).mean()) if len(pnl) else None,
                net_pnl_rs=float(pnl.sum()), split=buckets, trades=rows)


def main() -> None:
    signals, bundle, provenance = previous.load_signals()
    days = [date.fromisoformat(x) for x in provenance["days"]]
    frozen = pd.read_csv(DEFAULT_FROZEN / "trades.csv")
    baseline = contract.select_orders(signals)
    if set(baseline.sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Accepted 46-session G3 baseline parity failed")
    for item in provenance["extensions"]:
        path = Path(item["snapshot"]) / "snapshot_manifest.json"
        if sha(path) != provenance["verified_snapshot_manifest_hashes"][str(path)]:
            raise RuntimeError(f"Extension source drift: {path}")
    results = []
    for name in VARIANTS:
        setup = configure(name)
        audit = contract.selection_audit(signals)
        orders = audit.loc[audit.setup_id.eq("0951_LONG") & audit.v9_selected.eq(True)].copy()
        paths = previous.paths_for(orders, bundle, provenance)
        ledger = simulate(orders, paths)
        item = result(name, setup, ledger, days)
        results.append(item)
        print(name, {key:item[key] for key in ("selected_orders", "fills", "wins", "losses", "win_rate_pct", "net_pnl_rs", "split")}, flush=True)
    configure("moderately_strict_1pct")
    recent, recent_sources = recent_replay.candidates()
    recent_audit = contract.selection_audit(recent)
    recent_orders = recent_audit.loc[recent_audit.setup_id.eq("0951_LONG") & recent_audit.v9_selected.eq(True)]
    recent_counts = {str(day): int(pd.to_datetime(recent_orders.day).dt.date.eq(day).sum())
                     for day in recent_replay.DAYS}
    if any(recent_counts.values()):
        raise RuntimeError(f"Unexpected recent qualifying orders: {recent_counts}")
    data = dict(study="Research-only 09:50 LONG stricter setup with fixed 1% stop and 1% target",
                sessions=len(days), first_date=str(days[0]), last_date=str(days[-1]),
                frozen_baseline_order_parity=True,
                stop_target_applies_to_new_0950_long_only=True,
                round_trip_cost_bps=5., capital_per_entry_rs=100000., leverage=5.,
                assessment="Exploratory reused history; a threshold fitted on this data is not independent validation",
                moderately_strict_recent_selected_orders=recent_counts,
                recent_sources=recent_sources,
                activation_decision=dict(required_win_rate_pct=70.,
                    exploratory_observed_win_rate_pct=results[1]["win_rate_pct"],
                    exploratory_fills=results[1]["fills"],
                    later_16_validation_fills=results[1]["split"]["later_16"]["fills"],
                    recent_validation_fills=sum(recent_counts.values()),
                    status="NOT_ACTIVATED_INSUFFICIENT_INDEPENDENT_EVIDENCE"),
                variants=results)
    target = OUT / "strict_0950_long_1pct_study.json"
    target.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(target)


if __name__ == "__main__":
    main()
