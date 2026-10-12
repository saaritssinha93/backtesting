"""Full G3 portfolio replay with exploratory 09:50 LONG grid candidate."""
from __future__ import annotations

import json
from datetime import date

import pandas as pd

import stepwise_confirmation_study as stepwise


study = stepwise.study
OUT = study.OUT / "candidate_035body_055wick_060volume_full_g3_replay.json"


def metrics(ledger):
    fills = ledger.loc[ledger.portfolio_executed.eq(True)]
    net = fills.portfolio_net_profit_rupees.astype(float)
    return dict(selections=len(ledger), fills=len(fills), wins=int(net.gt(0).sum()),
        losses=int(net.lt(0).sum()), win_rate_pct=100*float(net.gt(0).mean()) if len(net) else None,
        net_pnl_rs=float(net.sum()))


def main():
    signals, bundle, provenance = study.previous.load_signals()
    days = [date.fromisoformat(value) for value in provenance["days"]]
    frozen = pd.read_csv(study.DEFAULT_FROZEN / "trades.csv")
    original_orders = study.contract.select_orders(signals)
    if set(original_orders.sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Original G3 selected orders differ from frozen reference")
    setup = stepwise.set_rule(.35, .55, .60, False)
    pool = signals.loc[signals.signal_end.eq("09:50") & signals.side.eq("LONG")].copy()
    audit = study.contract.selection_audit(pool)
    added = audit.loc[audit.setup_id.eq("0951_LONG") & audit.v9_selected.eq(True)].copy()
    if len(added) != 8 or original_orders.sid.isin(added.sid).any():
        raise RuntimeError("Unexpected 09:50 candidate selection")
    combined_orders = pd.concat((original_orders, added), ignore_index=True)
    paths = study.previous.paths_for(combined_orders, bundle, provenance)
    baseline, _ = study.previous.replay.simulate(original_orders, paths, days)
    study.previous.compare_to_frozen(baseline, frozen)
    experiment, _ = study.previous.replay.simulate(combined_orders, paths, days)
    previous = experiment.loc[experiment.sid.isin(original_orders.sid)].copy()
    study.previous.compare_to_frozen(previous, frozen)
    new = experiment.loc[experiment.sid.isin(added.sid)].copy()
    baseline_metric = metrics(baseline)
    experiment_metric = metrics(experiment)
    new_metric = metrics(new)
    if abs(experiment_metric["net_pnl_rs"] - baseline_metric["net_pnl_rs"] - new_metric["net_pnl_rs"]) > 1e-6:
        raise RuntimeError("Full-portfolio P&L did not decompose into baseline plus new slot")
    report = dict(study="Full portfolio replay for exploratory 09:50 LONG candidate",
        sessions=46, period_start=str(days[0]), period_end=str(days[-1]),
        setup=dict(signal_end="09:50", confirmation_end="09:51", side="LONG",
            price_change_pct=setup.price_change_pct, oi_change_pct=setup.oi_change_pct,
            five_minute_volume_ratio_min=setup.volume_ratio,
            confirmation_body_ratio_min=setup.body_ratio,
            confirmation_wick_ratio_max=setup.max_wick_ratio,
            confirmation_volume_ratio_min=.60, picker=setup.picker, max_entries=1),
        exit=dict(initial_stop_pct=1.25, tightened_stop_pct=1.0,
                  tighten_after_minutes=120, target_pct=1.0),
        frozen_baseline_trade_and_pnl_parity=True,
        original_trades_unchanged_in_experiment=True,
        baseline=baseline_metric, with_candidate=experiment_metric,
        added_0950_long=new_metric,
        new_trades=[dict(date=str(row["day"]),symbol=row["tradingsymbol"],
            filled=bool(row["filled"]),executed=bool(row["portfolio_executed"]),
            exit_reason=row["exit_reason"],net_pnl_rs=float(row["portfolio_net_profit_rupees"]))
            for row in new.to_dict("records")],
        recent_2026_10_08_09_12_selections=0,
        assessment="One result selected from 90 exploratory combinations; 7 fills, including only one in the later 16 sessions, do not establish 70% predictive win rate",
        production_modified=False)
    OUT.write_text(json.dumps(report,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print(json.dumps({key:report[key] for key in ("baseline","with_candidate","added_0950_long",
        "original_trades_unchanged_in_experiment")},indent=2),flush=True)
    print(OUT,flush=True)


if __name__ == "__main__":
    main()
