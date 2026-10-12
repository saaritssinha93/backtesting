"""Research-only replay of the stricter 09:50 LONG with staged stop and 1% target."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

import pandas as pd

import strict_1pct_study as prior


OUT = prior.OUT / "moderately_strict_0950_long_staged_125_to100_target100.json"
PRIOR = prior.OUT / "strict_0950_long_1pct_study.json"


def main() -> None:
    signals, bundle, provenance = prior.previous.load_signals()
    days = [date.fromisoformat(value) for value in provenance["days"]]
    frozen = pd.read_csv(prior.DEFAULT_FROZEN / "trades.csv")
    baseline = prior.contract.select_orders(signals)
    if set(baseline.sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Frozen G3 baseline order parity failed")
    for item in provenance["extensions"]:
        manifest = Path(item["snapshot"]) / "snapshot_manifest.json"
        if prior.sha(manifest) != provenance["verified_snapshot_manifest_hashes"][str(manifest)]:
            raise RuntimeError(f"Source snapshot drift: {manifest}")

    setup = prior.configure("moderately_strict_1pct")
    audit = prior.contract.selection_audit(signals)
    orders = audit.loc[audit.setup_id.eq("0951_LONG") & audit.v9_selected.eq(True)].copy()
    paths = prior.previous.paths_for(orders, bundle, provenance)
    orders["native_stop_pct"] = prior.g2.INITIAL_STOP_PCT
    orders["native_target_pct"] = 1.0
    prior.g2.g.v9.validate_paths(orders, paths)
    trades = prior.g2.simulate_staged(orders, paths, cost_bps=5., max_entry_delay_minutes=10)
    trades = prior.g2.g.v9.v5.apply_fixed_capital_model(trades, 100_000., 5.)
    base = prior.g2.g.v9.V9Config(portfolio_capital_rupees=1_000_000.,
        capital_per_entry_rupees=100_000., leverage_factor=5., max_positions=None, cost_bps=5.)
    ledger = prior.g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())[0]
    current = prior.result("moderately_strict_1pct", setup, ledger, days)
    previous = json.loads(PRIOR.read_text(encoding="utf-8"))
    fixed = next(value for value in previous["variants"] if value["name"] == "moderately_strict_1pct")
    fixed_by_key = {(row["date"], row["symbol"]): row for row in fixed["trades"]}
    changed_trades = []
    for row in current["trades"]:
        old = fixed_by_key.pop((row["date"], row["symbol"]))
        differences = {key: {"fixed_1pct": old[key], "staged_stop": row[key]}
                       for key in ("filled", "executed", "exit_reason", "exit_time", "exit_price", "net_pnl_rs")
                       if old[key] != row[key]}
        if differences:
            changed_trades.append({"date": row["date"], "symbol": row["symbol"], "changes": differences})
    if fixed_by_key:
        raise RuntimeError(f"Missing previous selections: {list(fixed_by_key)}")
    if current["selected_orders"] != fixed["selected_orders"]:
        raise RuntimeError("Selection count changed")
    result = {
        "study": "Stricter 09:50 LONG; only exit changed from fixed 1% stop to 1.25% initial stop tightened to 1% after 120 minutes; 1% target retained",
        "sessions": len(days), "period_start": str(days[0]), "period_end": str(days[-1]),
        "frozen_baseline_order_parity": True,
        "source_study": str(PRIOR),
        "selection": {**asdict(setup), "confirmation_minute_volume_ratio": prior.VARIANTS["moderately_strict_1pct"]["minute_volume"]},
        "exit": {"initial_stop_pct": prior.g2.INITIAL_STOP_PCT,
                 "tightened_stop_pct": prior.g2.TIGHTENED_STOP_PCT,
                 "tighten_after_minutes": prior.g2.TIGHTEN_AFTER_MINUTES,
                 "target_pct": 1.0, "round_trip_cost_bps": 5.0},
        "fixed_1pct_reference": {key: fixed[key] for key in
            ("selected_orders", "fills", "wins", "losses", "win_rate_pct", "net_pnl_rs", "split")},
        "staged_result": {key: current[key] for key in
            ("selected_orders", "fills", "wins", "losses", "win_rate_pct", "net_pnl_rs", "split", "trades")},
        "pnl_change_from_fixed_1pct_rs": current["net_pnl_rs"] - fixed["net_pnl_rs"],
        "changed_trades": changed_trades,
        "recent_2026_10_08_09_12_selected_orders": previous["moderately_strict_recent_selected_orders"],
        "assessment": "Exploratory in-sample threshold; no independent qualifying fills in the later 16 sessions or three recent sessions",
        "activation_decision": {
            "required_win_rate_pct": 70.0,
            "observed_win_rate_pct": current["win_rate_pct"],
            "observed_fills": current["fills"],
            "status": "NOT_ACTIVATED_INSUFFICIENT_INDEPENDENT_EVIDENCE"},
        "production_modified": False,
    }
    OUT.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({key: result[key] for key in
        ("fixed_1pct_reference", "staged_result", "pnl_change_from_fixed_1pct_rs", "changed_trades", "activation_decision")}
        | {"staged_result": {key: value for key, value in result["staged_result"].items() if key != "trades"}},
        indent=2), flush=True)
    print(OUT, flush=True)


if __name__ == "__main__":
    main()
