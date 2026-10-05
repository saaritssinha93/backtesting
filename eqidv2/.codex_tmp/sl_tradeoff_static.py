"""Fixed-stop tradeoff diagnostics on sealed, previously seen V13-G history.

Research only. No entry, live configuration, or original result is changed.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd

import v13_g2_sl_sweep as sweep


ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs/v13_sl_tradeoff_20261004/static_analysis.json"
EPS = 1e-8


def stats(ledger, days):
    dates = pd.to_datetime(ledger.day).dt.strftime("%Y-%m-%d")
    selected = ledger.loc[dates.isin([str(d) for d in days])].copy()
    ex = selected.loc[selected.portfolio_executed.eq(True)].copy()
    p = ex.portfolio_net_profit_rupees.to_numpy(float)
    losing = np.sort(p[p < -EPS])
    winning = p[p > EPS]
    daily = ex.assign(day=pd.to_datetime(ex.day).dt.strftime("%Y-%m-%d")).groupby("day").agg(
        net=("portfolio_net_profit_rupees", "sum"),
        trades=("portfolio_executed", "sum"),
        wins=("portfolio_net_profit_rupees", lambda x: int(x.gt(EPS).sum())),
    ).reindex([str(d) for d in days], fill_value=0)
    curve = np.r_[0.0, daily.net.cumsum().to_numpy(float)]
    active = daily.trades.gt(0)
    durations = pd.to_numeric(ex.holding_minutes, errors="coerce")
    return {
        "selected_orders": len(selected), "trades": len(ex),
        "wins": len(winning), "losses": len(losing),
        "win_rate_pct": 100 * len(winning) / len(ex) if len(ex) else 0,
        "net_rupees": float(p.sum()),
        "profit_factor": float(winning.sum() / -losing.sum()) if len(losing) else None,
        "average_loss_rupees": float(losing.mean()) if len(losing) else None,
        "worst_loss_rupees": float(losing.min()) if len(losing) else None,
        "worst10pct_all_trades_mean_rupees": float(np.sort(p)[:max(1, math.ceil(.1*len(p)))].mean()) if len(p) else None,
        "worst10pct_losing_trades_mean_rupees": float(losing[:max(1, math.ceil(.1*len(losing)))].mean()) if len(losing) else None,
        "daily_close_drawdown_rupees": float(np.max(np.maximum.accumulate(curve)-curve)),
        "positive_days": int(daily.net.gt(EPS).sum()),
        "negative_days": int(daily.net.lt(-EPS).sum()),
        "active_days": int(active.sum()),
        "positive_active_day_pct": float(100*daily.loc[active].net.gt(EPS).mean()) if active.any() else None,
        "stop_exits": int(ex.exit_reason.eq("STOP").sum()),
        "target_exits": int(ex.exit_reason.eq("TARGET").sum()),
        "time_exits": int(ex.exit_reason.eq("TIME_EXIT_1515").sum()),
        "mean_holding_minutes": float(durations.mean()) if len(ex) else None,
        "median_holding_minutes": float(durations.median()) if len(ex) else None,
        "cost_rupees": float(ex.portfolio_cost_rupees.sum()),
    }, daily


def summarize(ledger, days, summary):
    full, daily = stats(ledger, days)
    full.update({k: summary[k] for k in (
        "peak_concurrent_positions", "peak_reserved_capital_rupees", "peak_open_initial_risk_rupees", "portfolio_rejected_trades"
    )})
    subsets = {
        "JUL_AUG_REUSED": [d for d in days if str(d)<"2026-09-01"],
        "SEPTEMBER_REUSED": [d for d in days if str(d)>="2026-09-01"],
        "LATE_SEPTEMBER_REUSED": [d for d in days if str(d)>="2026-09-15"],
    }
    return {
        "full": full,
        "by_side": {str(side): stats(ledger.loc[ledger.side.eq(side)], days)[0] for side in sorted(ledger.side.unique())},
        "by_month": {m: stats(ledger, [d for d in days if str(d).startswith(m)])[0] for m in sorted({str(d)[:7] for d in days})},
        "chronological_diagnostics": {label: stats(ledger, ds)[0] for label,ds in subsets.items()},
        "daily": daily.reset_index(names="day").to_dict(orient="records"),
    }


def ci(values):
    return {"low_95": float(np.quantile(values,.025)), "median": float(np.median(values)), "high_95": float(np.quantile(values,.975)), "fraction_above_zero": float(np.mean(values>EPS))}


def bootstrap(candidate, baseline, indices):
    cn = candidate.net.to_numpy(float)[indices].sum(axis=1)
    bn = baseline.net.to_numpy(float)[indices].sum(axis=1)
    cw = candidate.wins.to_numpy(float)[indices].sum(axis=1)
    bw = baseline.wins.to_numpy(float)[indices].sum(axis=1)
    ct = candidate.trades.to_numpy(float)[indices].sum(axis=1)
    bt = baseline.trades.to_numpy(float)[indices].sum(axis=1)
    return {"net_delta_rupees": ci(cn-bn), "win_rate_delta_percentage_points": ci(100*cw/ct-100*bw/bt)}


def pareto(rows):
    # More wins, higher net, lower absolute mean loss, and lower daily DD.
    vectors = np.array([[r["full"]["win_rate_pct"], r["full"]["net_rupees"], r["full"]["average_loss_rupees"], -r["full"]["daily_close_drawdown_rupees"]] for r in rows])
    return [r["stop_pct"] for i,r in enumerate(rows) if not any(np.all(v>=vectors[i]-EPS) and np.any(v>vectors[i]+EPS) for j,v in enumerate(vectors) if j!=i)]


def cost_stress():
    """Focused total transaction-cost sensitivity; unchanged entry/exit fills."""
    published, segments, days = sweep.prepared_segments()
    base = published["base"]
    records = []
    for stop in (.75,1.,1.15,1.2,1.23,1.25,2.75):
        raw = pd.concat([sweep.ext._simulate(orders,paths,base,stop_pct=stop)[0] for _,orders,paths in segments],ignore_index=True,sort=False)
        baseline,_ = sweep.g2.g.v9.v6.apply_portfolio_constraints(raw,base.portfolio_config())
        original_wins = baseline.portfolio_net_profit_rupees.gt(EPS)
        for bps in (5.,10.,15.):
            repriced = raw.copy()
            repriced["cost_pct"] = bps/100
            repriced["net_return_pct"] = pd.to_numeric(repriced.gross_return_pct)-bps/100
            repriced = sweep.g2.g.v9.v5.apply_fixed_capital_model(repriced,base.capital_per_entry_rupees,base.leverage_factor)
            ledger,summary = sweep.g2.g.v9.v6.apply_portfolio_constraints(repriced,base.portfolio_config())
            assert ledger.portfolio_executed.equals(baseline.portfolio_executed)
            assert ledger.portfolio_executed.sum()==85
            stress_stats,_ = stats(ledger,days)
            delta = stress_stats["net_rupees"]-baseline.portfolio_net_profit_rupees.sum()
            expected_delta = -85*(bps-base.cost_bps)/10000*base.capital_per_entry_rupees*base.leverage_factor
            assert abs(delta-expected_delta)<1e-6
            flips = original_wins & ledger.portfolio_net_profit_rupees.lt(-EPS)
            flip_rows = ledger.loc[flips,["day","tradingsymbol","side","setup_id","gross_return_pct","portfolio_net_profit_rupees"]].copy()
            flip_rows["day"] = pd.to_datetime(flip_rows.day).dt.strftime("%Y-%m-%d")
            record = {
                "stop_pct":stop,"total_cost_bps":bps,"additional_cost_bps_vs_baseline":bps-base.cost_bps,
                "full":stress_stats,"net_delta_vs_same_stop_at_5bps":float(delta),
                "winning_trades_flipped_to_loss":int(flips.sum()),
                "flipped_trades":flip_rows.to_dict(orient="records"),
            }
            records.append(record)
            print(f"SL {stop:.2f} costs {bps:.0f}bps: {stress_stats['wins']}W/{stress_stats['losses']}L net {stress_stats['net_rupees']:.2f}, PF {stress_stats['profit_factor']:.3f}, flips {int(flips.sum())}",flush=True)
    out = OUT.with_name("cost_stress_analysis.json")
    out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps({
        "window":[str(days[0]),str(days[-1])],"sessions":len(days),
        "assumptions":"Total round-trip costs5,10,15bps of fixed500000-rupee exposure. Extra bps can proxy moderate fees/slippage, but do not model altered fills, gaps or liquidity. Original gap-through engine and stop/target paths retained. All85 fills retained under portfolio rechecks.",
        "evidence":"REUSED_HISTORY_COST_SENSITIVITY_NOT_OUT_OF_SAMPLE",
        "records":records,
    },indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print(str(out))


def main():
    published, segments, days = sweep.prepared_segments()
    base = published["base"]
    stops = [round(x/100,2) for x in range(60,151)] + [1.75,2.0,2.75]
    records, daily_by_stop = [], {}
    for s in stops:
        raw = pd.concat([sweep.ext._simulate(orders, paths, base, stop_pct=s)[0] for _,orders,paths in segments], ignore_index=True, sort=False)
        ledger, summary = sweep.g2.g.v9.v6.apply_portfolio_constraints(raw, base.portfolio_config())
        record = {"stop_pct": s, **summarize(ledger, days, summary)}
        daily_by_stop[s] = stats(ledger,days)[1]
        # Equal all-in nominal stop risk of Rs5,250 includes the 5bps round-trip cost.
        factor = (1.0 + base.cost_bps / 100) / (s + base.cost_bps / 100)
        sized = sweep.g2.g.v9.v5.apply_fixed_capital_model(raw, base.capital_per_entry_rupees*factor, base.leverage_factor)
        rl, rs = sweep.g2.g.v9.v6.apply_portfolio_constraints(sized, base.portfolio_config())
        record["equal_nominal_stop_risk"] = {
            "size_factor": factor,
            "capital_per_trade_rupees": base.capital_per_entry_rupees*factor,
            "exposure_per_trade_rupees": base.capital_per_entry_rupees*base.leverage_factor*factor,
            "nominal_stop_loss_including_cost_rupees": 5250.,
            "note": "Gap-through fills can still exceed nominal risk; no extra slippage modeled.",
            **summarize(rl, days, rs),
        }
        record["equal_gross_stop_risk_scaling"] = {
            "size_factor": 1/s,
            "net_rupees_if_same_executed_trades": record["full"]["net_rupees"]/s,
            "nominal_stop_including_cost_rupees": 5000+250/s,
        }
        records.append(record)
        if round(s*100)%10==0 or s in (1.25,1.75,2.0,2.75):
            print(f"stop={s:.2f}, wins={record['full']['wins']}, net={record['full']['net_rupees']:.2f}, loss={record['full']['average_loss_rupees']:.2f}", flush=True)

    bystop = {r["stop_pct"]:r for r in records}
    assert bystop[1.0]["full"]["trades"] == 85
    assert bystop[1.0]["full"]["wins"] == 57
    assert abs(bystop[1.0]["full"]["net_rupees"]-220659.01)<.01
    assert bystop[1.25]["full"]["wins"] == 59
    assert abs(bystop[1.25]["full"]["net_rupees"]-223643.92)<.01

    # Paired cluster bootstrap: preserve daily trade co-movement in both variants.
    rng = np.random.default_rng(20261004)
    day_indices = rng.integers(0,len(days),size=(10000,len(days)))
    # Consecutive 5-observed-session blocks, circular to avoid unequal edge weights.
    starts = rng.integers(0,len(days),size=(10000,math.ceil(len(days)/5)))
    block_indices = ((starts[:,:,None]+np.arange(5))%len(days)).reshape(10000,-1)[:,:len(days)]
    for record in records:
        s = record["stop_pct"]
        record["bootstrap_vs_1pct"] = {
            "paired_day": bootstrap(daily_by_stop[s],daily_by_stop[1.0],day_indices),
            "paired_five_session_block": bootstrap(daily_by_stop[s],daily_by_stop[1.0],block_indices),
        }
        neighbors = [r for r in records if abs(r["stop_pct"]-s)<=.0500001]
        record["neighborhood_0_05_percentage_points"] = {
            "stop_values": [r["stop_pct"] for r in neighbors],
            "net_min": min(r["full"]["net_rupees"] for r in neighbors),
            "net_median": float(np.median([r["full"]["net_rupees"] for r in neighbors])),
            "net_max": max(r["full"]["net_rupees"] for r in neighbors),
            "win_rate_min": min(r["full"]["win_rate_pct"] for r in neighbors),
            "win_rate_max": max(r["full"]["win_rate_pct"] for r in neighbors),
        }

    payload = {
        "window": [str(days[0]),str(days[-1])], "sessions":len(days), "iterations":len(stops),
        "evidence": "REUSED_HISTORY_DIAGNOSTICS_NOT_UNTOUCHED_OUT_OF_SAMPLE",
        "october_1": "EXCLUDED_INCOMPLETE_FINAL_SCAN_SLOT",
        "assumptions": {"baseline_capital_per_trade":base.capital_per_entry_rupees,"leverage":base.leverage_factor,"cost_bps":base.cost_bps,"portfolio_capital":base.portfolio_capital_rupees,"stop_pct_units":"price percentage points, 1.0 means 1%"},
        "metrics_note": "Daily DD is close-to-close, not intraday mark-to-market. Lower-tail means use worst ceil(10% of sample). Both unconditional trade-tail and loss-conditional tail are reported. Split selection and bootstrap are descriptive because entries and exits were selected on reused data; intervals do not correct grid search or strategy selection bias. 5-session circular blocks are weekly-sized, not calendar-week resampling.",
        "pareto_fixed_exposure": pareto(records),
        "pareto_equal_nominal_stop_risk": pareto([{"stop_pct":r["stop_pct"],"full":r["equal_nominal_stop_risk"]["full"]} for r in records]),
        "records":records,
    }
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(payload,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print(json.dumps({"output":str(OUT),"pareto_fixed_exposure":payload["pareto_fixed_exposure"],"iterations":len(stops)},indent=2))


if __name__ == "__main__":
    if "--cost-stress-only" in sys.argv:
        cost_stress()
    else:
        main()
