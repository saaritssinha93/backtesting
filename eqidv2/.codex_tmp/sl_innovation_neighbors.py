"""Postselection stability audit of ONE frozen progress-stop candidate.

This grid is diagnostic only. It must not choose a replacement configuration.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

import sl_innovation_common as common
import sl_innovation_progress as progress


FROZEN_NAME="TRAIL_A1.25_KEEP0.25_STAGED"
FROZEN_ARM=1.25
FROZEN_RETAIN=.25
ARMS=(1.,1.10,1.15,1.20,1.25,1.30,1.35,1.40,1.50)
RETAINS=(.20,.25,.30)
KEYS=["day","sid","setup_id","tradingsymbol","side"]


def trade_pnl(item):
    return pd.DataFrame(item["trades_detail"]).set_index(KEYS).portfolio_net_profit_rupees.sort_index()


def gate(row,base):
    tests={
        "win_rate_not_lower":row["wins"]>=base["wins"],
        "positive_days_not_lower":row["positive_sessions"]>=base["positive_sessions"],
        "win_or_positive_day_improvement":row["wins"]>base["wins"] or row["positive_sessions"]>base["positive_sessions"],
        "initial_hard_stop_not_larger":row["rule"]["hard_stop"]<=base["rule"]["hard_stop"],
        "net_profit_improvement":row["net_profit_rupees"]>base["net_profit_rupees"]+1e-8,
        "average_loss_not_larger":row["average_loss_magnitude_rupees"]<=base["average_loss_magnitude_rupees"]+1e-8,
        "minute_close_drawdown_not_larger":row["minute_close_drawdown_rupees"]<=base["minute_close_drawdown_rupees"]+1e-8,
    }
    return {"passes":all(tests.values()),"conditions":tests}


def compact(row):
    return {"name":row["rule"]["name"],"arm_pct":row["rule"]["arm_pct"],"retain_fraction":row["rule"]["retain_fraction"],
            **{key:row[key] for key in ("wins","win_rate_pct","net_profit_rupees","positive_sessions", "average_loss_magnitude_rupees","worst_loss_rupees","minute_close_drawdown_rupees","daily_close_drawdown_rupees","profit_factor")},
            "passes_original_balanced_gate":row["original_balanced_gate"]["passes"]}


def main():
    original=json.loads((common.OUT/"progress_analysis.json").read_text(encoding="utf-8"))
    originally_selected=next(r for r in original["results"] if r["rule"]["name"]==FROZEN_NAME)
    ctx=common.context()
    controls=common.controls(ctx)
    staged=next(r for r in controls if r["rule"]["name"]=="TIGHTEN_1.25_TO_1.00_AFTER_120M")
    rows=[]
    for arm in ARMS:
        for retain in RETAINS:
            rule=dict(name=f"TRAIL_A{arm:.2f}_KEEP{retain:.2f}_STAGED",family="PROGRESS",mode="FRACTION",
                      hard_stop=1.25,tighten_minutes=120,arm_pct=arm,retain_fraction=retain)
            row=common.evaluate(ctx,rule,progress.exit_progress)
            assert row["trades"]==85 and row["portfolio_rejected_trades"]==0
            row["original_balanced_gate"]=gate(row,staged)
            rows.append(row)
            print(f"{rule['name']} wins={row['wins']} net={row['net_profit_rupees']:.2f} average_loss={row['average_loss_magnitude_rupees']:.2f} MTMDD={row['minute_close_drawdown_rupees']:.2f} gate={row['original_balanced_gate']['passes']}",flush=True)
    frozen=next(r for r in rows if r["rule"]["name"]==FROZEN_NAME)
    for field in ("wins","net_profit_rupees","average_loss_magnitude_rupees","minute_close_drawdown_rupees","daily_close_drawdown_rupees"):
        assert abs(frozen[field]-originally_selected[field])<1e-6,(field,frozen[field],originally_selected[field])
    frozen_pnl=trade_pnl(frozen)
    staged_pnl=trade_pnl(staged)
    for row in rows:
        observed=trade_pnl(row).reindex(frozen_pnl.index)
        assert not observed.isna().any()
        delta=observed-frozen_pnl
        row["relative_to_frozen_candidate"]={
            "net_delta_rupees":float(delta.sum()),"improved_trades":int(delta.gt(1e-7).sum()),
            "worsened_trades":int(delta.lt(-1e-7).sum()),
            "all_trade_pnl_identical":bool(np.allclose(observed,frozen_pnl,atol=1e-7,rtol=0)),
        }
        delta_staged=observed-staged_pnl.reindex(observed.index)
        row["relative_to_original_staged"]={"net_delta_rupees":float(delta_staged.sum()),
            "improved_trades":int(delta_staged.gt(1e-7).sum()),"worsened_trades":int(delta_staged.lt(-1e-7).sum())}
    fixed_retain=[r for r in rows if r["rule"]["retain_fraction"]==FROZEN_RETAIN]
    same_outcomes=[r["rule"]["arm_pct"] for r in fixed_retain if r["relative_to_frozen_candidate"]["all_trade_pnl_identical"]]
    local=[r for r in rows if abs(r["rule"]["arm_pct"]-FROZEN_ARM)<=.10000001]
    summary={
        "fixed_selected_candidate":compact(frozen),
        "replacement_selection_permitted":False,
        "same_trade_pnl_arms_at_frozen_retain":same_outcomes,
        "full_27_grid_balanced_gate_pass_count":sum(r["original_balanced_gate"]["passes"] for r in rows),
        "local_15_rules_arm_1_15_to_1_35_balanced_gate_pass_count":sum(r["original_balanced_gate"]["passes"] for r in local),
        "local_minimum_net_rupees":min(r["net_profit_rupees"] for r in local),
        "local_maximum_net_rupees":max(r["net_profit_rupees"] for r in local),
        "local_minimum_wins":min(r["wins"] for r in local),
        "local_maximum_wins":max(r["wins"] for r in local),
        "lowest_net_observed_neighbor_not_selected":compact(min(rows,key=lambda r:r["net_profit_rupees"])),
        "highest_net_observed_neighbor_not_selected":compact(max(rows,key=lambda r:r["net_profit_rupees"])),
        "frozen_retain_arm_diagnostics":[compact(r) for r in fixed_retain],
        "fixed_arm_retention_diagnostics":[compact(r) for r in rows if r["rule"]["arm_pct"]==FROZEN_ARM],
    }
    checks={"original_control_parity":4,"selected_candidate_original_result_parity":True,
            "progress_synthetic_checks":progress.synthetic_checks(),"all_rules_85_trades_zero_portfolio_rejections":True}
    notes=[
        "This is a postselection neighborhood stability audit. The candidate arm1.25%/retain25% was frozen BEFORE this grid. No replacement is selected from these additional results.",
        "27 rules: arm1.00/1.10/1.15/1.20/1.25/1.30/1.35/1.40/1.50%; retain20/25/30% of greatest completed full post-entry 5min close profit. Original hard1.25% tightens to1% after120min for every rule.",
        "All43sessions and85trades are reused history. Stability is not out-of-sample validation. Targets,entry,cost5bps,position sizing,portfolio allocation unchanged.",
        "Controls verified against original outputs. Profit floor is gross before costs and becomes active following open; stop gaps and stop-first ties preserved.",
        "Volatility engine terminal-bar audit: pre-existing skip at exit_volatility terminal-bar check prevents newly calculated final-close stop being reported as active. No volatility fix or rerun required.",
    ]
    output=dict(status="POSTSELECTION_DIAGNOSTIC_NOT_NEW_OPTIMIZATION",window=["2026-07-29","2026-09-30"],sessions=43,
                october_1="EXCLUDED_INCOMPLETE",checks=checks,notes=notes,controls=controls,summary=summary,results=rows)
    path=common.OUT/"neighbor_analysis.json"
    path.write_text(json.dumps(common.timing.clean(output),indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print(json.dumps(summary,indent=2),flush=True)
    print(str(path),flush=True)


if __name__=="__main__":
    main()
