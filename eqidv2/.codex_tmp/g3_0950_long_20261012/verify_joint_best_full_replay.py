"""Contract/portfolio parity for train-selected and best apparent joint configurations."""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd

import strict_1pct_study as study


ROOT = study.OUT / "joint_0950_5m_1m"
SEARCH = ROOT / "joint_search.json"
OUT = ROOT / "verified_full_replays.json"


def metric(frame):
    executed = frame.loc[frame.portfolio_executed.eq(True)]
    pnl = executed.portfolio_net_profit_rupees.astype(float)
    return dict(selected=len(frame),fills=len(executed),wins=int(pnl.gt(0).sum()),
        losses=int(pnl.lt(0).sum()),win_rate_pct=100*float(pnl.gt(0).mean()) if len(pnl) else None,
        net_pnl_rs=float(pnl.sum()))


def configure(config):
    donor = study.contract._ACTIVE_BY_ID["0956_LONG"]
    setup = replace(donor,signal_end="09:50",confirmation_end="09:51",
        price_change_pct=config["five_minute_price_min"],
        oi_change_pct=config["five_minute_oi_min"],
        volume_ratio=config["five_minute_volume_min"],
        body_ratio=config["one_minute_body_min"],
        max_wick_ratio=config["one_minute_wick_max"],
        stop_pct=1.25,target_pct=1.)
    key = setup.setup_id
    study.contract.CORE_SETUPS = tuple(s for s in study.contract.CORE_SETUPS if s.setup_id != key)+(setup,)
    study.contract.ACTIVE_SETUPS = tuple(s for s in study.contract.ACTIVE_SETUPS if s.setup_id != key)+(setup,)
    study.contract._CORE_BY_ID[key] = setup
    study.contract._ACTIVE_BY_ID[key] = setup
    study.contract.MIN_CONFIRMATION_VOLUME_BY_SIDE["LONG"] = config["one_minute_volume_min"]
    return setup


def main():
    search = json.loads(SEARCH.read_text())
    signals,bundle,provenance = study.previous.load_signals()
    days = [date.fromisoformat(value) for value in provenance["days"]]
    frozen = pd.read_csv(study.DEFAULT_FROZEN / "trades.csv")
    original_orders = study.contract.select_orders(signals)
    if set(original_orders.sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Original frozen G3 selection parity failed")
    historical = signals.loc[signals.signal_end.eq("09:50") & signals.side.eq("LONG")]
    recent,_ = study.recent_replay.candidates()
    results = {}
    for label,key in (("train_selected","stage2_one_minute_train_selected"),
                      ("best_apparent_train_and_all49","grid_best_train_and_all49_apparent")):
        case = search[key]
        setup = configure(case["config"])
        audit = study.contract.selection_audit(historical)
        added = audit.loc[audit.setup_id.eq(setup.setup_id) & audit.v9_selected.eq(True)].copy()
        recent_audit = study.contract.selection_audit(recent)
        later = recent_audit.loc[recent_audit.setup_id.eq(setup.setup_id) & recent_audit.v9_selected.eq(True)]
        found = {(str(row["day"]),row["tradingsymbol"]) for row in
                 [*added.to_dict("records"),*later.to_dict("records")]}
        expected = {(row["date"],row["symbol"]) for row in case["selected"]}
        if found != expected:
            raise RuntimeError(f"Contract selection mismatch {label}: {found ^ expected}")
        if len(later):
            raise RuntimeError(f"Recent paths need replay for {label}")
        combined_orders = pd.concat((original_orders,added),ignore_index=True)
        paths = study.previous.paths_for(combined_orders,bundle,provenance)
        baseline,_ = study.previous.replay.simulate(original_orders,paths,days)
        study.previous.compare_to_frozen(baseline,frozen)
        combined,_ = study.previous.replay.simulate(combined_orders,paths,days)
        old = combined.loc[combined.sid.isin(original_orders.sid)]
        study.previous.compare_to_frozen(old,frozen)
        new = combined.loc[combined.sid.isin(added.sid)]
        original_metric,combined_metric,new_metric = metric(baseline),metric(combined),metric(new)
        if (new_metric["selected"] != case["all49"]["selected"] or
            new_metric["fills"] != case["all49"]["fills"] or
            new_metric["wins"] != case["all49"]["wins"] or
            abs(new_metric["net_pnl_rs"] - case["all49"]["net_pnl_rs"]) > 1e-6):
            raise RuntimeError(f"Full portfolio differs from precomputed candidate outcomes: {label}")
        if abs(combined_metric["net_pnl_rs"]-original_metric["net_pnl_rs"]-new_metric["net_pnl_rs"])>1e-6:
            raise RuntimeError(f"Portfolio P&L decomposition failed: {label}")
        results[label]=dict(config=case["config"],new_slot=new_metric,baseline=original_metric,
            with_slot=combined_metric,contract_selection_parity=True,
            original_trade_and_pnl_frozen_parity=True,
            new_trades=[dict(date=str(row["day"]),symbol=row["tradingsymbol"],
                filled=bool(row["filled"]),executed=bool(row["portfolio_executed"]),
                exit_reason=row["exit_reason"],net_pnl_rs=float(row["portfolio_net_profit_rupees"]))
                for row in new.to_dict("records")])
        print(label,new_metric,"portfolio",combined_metric,flush=True)
    output=dict(study="Full G3 contract and portfolio checks for 09:50 joint-search candidates",
        sessions=46,source_search=str(SEARCH),results=results,
        conclusion="Neither candidate has independent evidence that future win rate is at least 70%; production unchanged",
        production_modified=False)
    OUT.write_text(json.dumps(output,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print(OUT,flush=True)


if __name__ == "__main__":
    main()
