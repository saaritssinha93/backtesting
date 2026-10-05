"""Causal, research-only completed-candle chandelier exit sensitivity."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import sl_tradeoff_timing as timing
import sl_innovation_common as common


OUT = Path(__file__).resolve().parent.parent / "outputs/v13_sl_innovation_20261004/volatility_analysis.json"
MINUTE = timing.MINUTE


def exit_volatility(path, entry_index, entry, is_long, target_pct, hard_stop,
                    *, wait_minutes=15, activation_profit=.5, atr_bars=14,
                    atr_multiple=4., **ignored):
    """A ratchet decided after a full candle becomes active at the next open.

    The entry candle is excluded from both the high-water anchor and ATR because
    its pre-fill extremes are unavailable to the position. All TR samples use a
    completed post-entry candle and its already-known previous close. No partial
    ATR window is allowed. Activation uses completed CLOSE profit, not high/low.
    Once activated, protection stays active even if the close falls below its gate.
    """
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    stop = entry * (1 - sign * hard_stop / 100)
    current_distance = hard_stop
    entry_label = int(path["timestamp_ns"][entry_index])
    anchor = None
    completed_tr = []
    active = False
    for j in range(entry_index, len(path["close"])):
        op, hi, lo, close = (float(path[key][j]) for key in ("open", "high", "low", "close"))
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            event = "OPEN" if j > entry_index and stop_open else "INTRABAR"
            fill = op if event == "OPEN" else stop
            reason = "TIGHTENED_STOP" if current_distance < hard_stop-1e-10 else "STOP"
            return j, fill, reason, current_distance, event
        if target_hit:
            return j, target, "TARGET", current_distance, "INTRABAR"
        # Terminal-bar decisions cannot create a fictitious next-open execution.
        if j == entry_index or j == len(path["close"])-1:
            continue
        prior_close = float(path["close"][j-1])
        true_range = max(hi-lo, abs(hi-prior_close), abs(lo-prior_close))
        completed_tr.append(true_range)
        anchor = hi if anchor is None and is_long else lo if anchor is None else max(anchor,hi) if is_long else min(anchor,lo)
        close_elapsed = (int(path["timestamp_ns"][j])-entry_label)/MINUTE
        close_profit = sign*(close/entry-1)*100
        if close_elapsed >= wait_minutes and close_profit >= activation_profit and len(completed_tr) >= atr_bars:
            active = True
        if active:
            atr = float(np.mean(completed_tr[-atr_bars:]))
            proposed_stop = anchor-sign*atr_multiple*atr
            stop = max(stop,proposed_stop) if is_long else min(stop,proposed_stop)
            current_distance = -sign*(stop/entry-1)*100
    return len(path["close"])-1, float(path["close"][-1]), "TIME_EXIT_1515", current_distance, "CLOSE"


def path_from_rows(rows):
    return {"timestamp_ns": np.arange(1,len(rows)+1,dtype=np.int64)*MINUTE,
            **{key:np.array([r[i] for r in rows],dtype=float) for i,key in enumerate(("open","high","low","close"))}}


def mirrored(path):
    return {"timestamp_ns":path["timestamp_ns"].copy(),"open":200-path["open"],
            "high":200-path["low"],"low":200-path["high"],"close":200-path["close"]}


def synthetic_checks():
    checks=[]
    # Same-candle low does not retroactively hit a freshly calculated trail.
    p=path_from_rows([(100,100.1,99.8,100),(100,101,99.4,100.8),(100.4,100.5,100.2,100.3)])
    kw=dict(wait_minutes=1,activation_profit=.5,atr_bars=1,atr_multiple=.2)
    for side,data,expected in ((True,p,100.4),(False,mirrored(p),99.6)):
        out=exit_volatility(data,0,100,side,3,1.25,**kw)
        assert out[0]==2 and abs(out[1]-expected)<1e-8 and out[2]=="TIGHTENED_STOP" and out[4]=="OPEN",out
        checks.append(f"{'long' if side else 'short'}_completed_candle_then_following_open_gap")
    # The entry candle's favorable excursion must never seed the chandelier.
    p=path_from_rows([(100,102,99.5,100),(100,100.8,99.9,100.6),(100.6,100.7,100.4,100.6)])
    for side,data in ((True,p),(False,mirrored(p))):
        out=exit_volatility(data,0,100,side,3,1.25,wait_minutes=1,activation_profit=.5,atr_bars=1,atr_multiple=.5)
        assert out[2]=="TIME_EXIT_1515",out
        checks.append(f"{'long' if side else 'short'}_entry_bar_extremes_excluded")
    # ATR expansion cannot loosen already established protection.
    p=path_from_rows([(100,100.1,99.8,100),(100.7,100.8,100.7,100.8),(100.8,100.85,100.75,100.8),(100.8,101.2,100.6,100.9),(100.9,101.,100.5,100.7)])
    for side,data,expected in ((True,p,100.55),(False,mirrored(p),99.45)):
        out=exit_volatility(data,0,100,side,3,1.25,wait_minutes=2,activation_profit=.5,atr_bars=1,atr_multiple=3.)
        assert out[0]==4 and abs(out[1]-expected)<1e-8 and out[2]=="TIGHTENED_STOP",out
        checks.append(f"{'long' if side else 'short'}_ratchet_never_loosens")
    # Enough elapsed time alone cannot bypass the full ATR warm-up window.
    p=path_from_rows([(100,100.1,99.8,100),(100,101,99.4,100.8),(100.4,100.5,100.2,100.3)])
    out=exit_volatility(p,0,100,True,3,1.25,wait_minutes=1,activation_profit=.5,atr_bars=14,atr_multiple=.2)
    assert out[2]=="TIME_EXIT_1515",out
    checks.append("full_post_entry_atr_window_required")
    # Original hard stop is immediate, and same-bar stop/target ties are stop-first.
    p=path_from_rows([(100,104,98,101)])
    assert exit_volatility(p,0,100,True,3,1.25)[:3]==(0,98.75,"STOP")
    assert exit_volatility(mirrored(p),0,100,False,3,1.25)[:3]==(0,101.25,"STOP")
    checks.append("hard_stop_immediate_and_stop_first_ties_both_sides")
    # A hard-stop gap must fill at the worse opening price.
    p=path_from_rows([(100,100.1,99.8,100),(98,98.5,97.5,98.2)])
    assert exit_volatility(p,0,100,True,3,1.25)[:3]==(1,98.0,"STOP")
    assert exit_volatility(mirrored(p),0,100,False,3,1.25)[:3]==(1,102.0,"STOP")
    checks.append("hard_stop_gap_worse_open_both_sides")
    # Changing candles after an already-triggered exit cannot change that exit.
    p=path_from_rows([(100,100.1,99.8,100),(100,101,99.4,100.8),(100.4,100.5,100.2,100.3),(100,110,90,100)])
    a=exit_volatility(p,0,100,True,3,1.25,**kw)
    for key in ("open","high","low","close"):
        p[key][-1]=500
    assert exit_volatility(p,0,100,True,3,1.25,**kw)==a
    checks.append("future_candles_cannot_change_earlier_exit")
    return checks


def rules():
    return [dict(name=f"ATR_CHANDELIER_WAIT{wait}_PROGRESS{progress:.2f}_ATR{bars}_MULT{multiple:g}",
                 family="VOLATILITY_CHANDELIER",hard_stop=1.25,wait_minutes=wait,
                 activation_profit=progress,atr_bars=bars,atr_multiple=multiple)
            for wait in (15,30) for progress in (.5,.75) for bars in (14,28) for multiple in (3.,4.,5.)]


def simulate(segments,base,rule):
    previous=timing.exit_path
    def adapted(path,entry_index,entry,is_long,target_pct,hard_stop,**ignored):
        return exit_volatility(path,entry_index,entry,is_long,target_pct,hard_stop,
                               **{k:rule[k] for k in ("wait_minutes","activation_profit","atr_bars","atr_multiple")})
    timing.exit_path=adapted
    try:
        return timing.simulate(segments,base,rule)
    finally:
        timing.exit_path=previous


def evaluate_exit(path,entry_index,entry,is_long,target_pct,rule):
    return exit_volatility(path,entry_index,entry,is_long,target_pct,rule["hard_stop"],
                           **{key:rule[key] for key in ("wait_minutes","activation_profit","atr_bars","atr_multiple")})


def main():
    checks=synthetic_checks()
    ctx=common.context()
    controls=common.controls(ctx)
    results=[]
    keys=["day","sid","setup_id","tradingsymbol","side"]
    for rule in rules():
        row=common.evaluate(ctx,rule,evaluate_exit)
        assert row["trades"]==85 and row["portfolio_rejected_trades"]==0
        row["comparisons"]={}
        trades=pd.DataFrame(row["trades_detail"]).set_index(keys)
        for control in controls:
            baseline=pd.DataFrame(control["trades_detail"]).set_index(keys)
            current=trades.portfolio_net_profit_rupees.reindex(baseline.index)
            assert not current.isna().any()
            original=baseline.portfolio_net_profit_rupees
            delta=current-original
            row["comparisons"][control["rule"]["name"]]={
                "net_delta_rupees":float(delta.sum()),
                "rescued_winners":int(((original<0)&(current>0)).sum()),
                "lost_winners":int(((original>0)&(current<0)).sum()),
                "improved_trades":int(delta.gt(1e-7).sum()),
                "worsened_trades":int(delta.lt(-1e-7).sum()),
            }
        results.append(row)
        print(f"{rule['name']} wins={row['wins']} net={row['net_profit_rupees']:.2f} PF={row['profit_factor']:.3f} meanloss={row['average_loss_magnitude_rupees']:.2f} MTMDD={row['minute_close_drawdown_rupees']:.2f}",flush=True)
    dimensions=("wait_minutes","activation_profit","atr_bars","atr_multiple")
    for row in results:
        neighbors=[]
        for other in results:
            differences=[key for key in dimensions if row["rule"][key]!=other["rule"][key]]
            if len(differences)!=1:
                continue
            key=differences[0]
            if key=="atr_multiple" and abs(row["rule"][key]-other["rule"][key])>1.:
                continue
            neighbors.append({"name":other["rule"]["name"],"changed_dimension":key,
                              "wins":other["wins"],"net_profit_rupees":other["net_profit_rupees"],
                              "average_loss_magnitude_rupees":other["average_loss_magnitude_rupees"],
                              "minute_close_drawdown_rupees":other["minute_close_drawdown_rupees"]})
        row["immediate_neighbors"]=neighbors
        row["neighborhood_summary"]={
            "minimum_wins":min([row["wins"]]+[n["wins"] for n in neighbors]),
            "maximum_wins":max([row["wins"]]+[n["wins"] for n in neighbors]),
            "minimum_net_rupees":min([row["net_profit_rupees"]]+[n["net_profit_rupees"] for n in neighbors]),
            "maximum_net_rupees":max([row["net_profit_rupees"]]+[n["net_profit_rupees"] for n in neighbors]),
        }
    output=common.save(OUT.name,results,controls,checks,[
        "24 rules: hard1.25%, activation after15/30 completed post-entry minutes AND completed-close profit>=0.50/0.75%; rolling14/28-bar 1min ATR times3/4/5.",
        "ATR and chandelier favorable anchor exclude the entry candle. Each TR observation uses a full completed post-entry candle and its already-known previous close; full ATR warmup required.",
        "New stops activate at following open, remain active after activation, and ratchet only toward reduced risk. Entry-bar pre-fill extremes cannot activate protection.",
        "Existing original targets unchanged; hard stop immediately active; stop-first same-bar ties; stop gaps fill at worse open.",
        "Cost5bps baseline;10/15bps fixed-exposure stress provided. Dynamic fills recorded at actual next-open time; mark-to-market uses candle closes.",
        "All43 sessions and85 trades were previously seen. Splits and neighbors are diagnostics, not untouched validation. No live strategy settings changed.",
    ])
    print(str(output))


if __name__=="__main__":
    main()
