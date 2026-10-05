"""Research-only causal stop/timer tests. Never changes live or frozen configs."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import v13_g2_sl_sweep as source

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs/v13_sl_tradeoff_20261004"
MINUTE = 60_000_000_000


def exit_path(path, entry_index, entry, is_long, target_pct, hard_stop,
              tighten_minutes=None, tighten_stop=1.0, timer_minutes=None,
              timer_adverse=None):
    """Minute closes signal time exits for the following open; hard stop always active.

    Stored times are candle END labels. The entry bar end is used as a conservative
    entry-time proxy. A scheduled stop tightening activates only at a bar OPEN at
    least tighten_minutes later. Static ties retain the native stop-first rule.
    """
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    entry_label = int(path["timestamp_ns"][entry_index])
    pending = False
    for j in range(entry_index, len(path["close"])):
        end_ns = int(path["timestamp_ns"][j])
        open_elapsed = (end_ns - MINUTE - entry_label) / MINUTE
        current_stop = hard_stop
        if tighten_minutes is not None and open_elapsed >= tighten_minutes:
            current_stop = min(hard_stop, tighten_stop)
        stop = entry * (1 - sign * current_stop / 100)
        op, hi, lo = (float(path[key][j]) for key in ("open", "high", "low"))
        stop_open = op <= stop if is_long else op >= stop
        target_open = op >= target if is_long else op <= target
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if pending:
            if stop_open:
                return j, op, "STOP_GAP_BEFORE_TIMER", current_stop, "OPEN"
            if target_open:
                return j, target, "TARGET", current_stop, "OPEN"
            return j, op, "ADVERSE_TIMER_NEXT_OPEN", current_stop, "OPEN"
        if stop_hit:
            fill = op if j > entry_index and stop_open else stop
            reason = "TIGHTENED_STOP" if current_stop < hard_stop else "STOP"
            return j, fill, reason, current_stop, "OPEN" if j > entry_index and stop_open else "INTRABAR"
        if target_hit:
            return j, target, "TARGET", current_stop, "INTRABAR"
        close_elapsed = (end_ns - entry_label) / MINUTE
        close_return = sign * (float(path["close"][j]) / entry - 1) * 100
        if (timer_minutes is not None and close_elapsed >= timer_minutes
                and close_return <= -timer_adverse and j < len(path["close"]) - 1):
            pending = True
    return len(path["close"]) - 1, float(path["close"][-1]), "TIME_EXIT_1515", current_stop, "CLOSE"


def validate_synthetic():
    def path(ohlc):
        return {"timestamp_ns": np.arange(1, len(ohlc) + 1, dtype=np.int64) * MINUTE,
                **{k: np.array([r[i] for r in ohlc]) for i, k in enumerate(("open", "high", "low", "close"))}}
    # A timer based on the completed second candle cannot fill at that candle's close.
    p = path([(100,100.2,99.8,100), (100,100.1,99.3,99.4), (99.2,100.5,99.1,100.2)])
    assert exit_path(p,0,100,True,2,1.25,timer_minutes=1,timer_adverse=.5)[:3] == (2,99.2,"ADVERSE_TIMER_NEXT_OPEN")
    # Pending timer cannot hide a gap through the hard stop.
    p["open"][2] = 98
    assert exit_path(p,0,100,True,2,1.25,timer_minutes=1,timer_adverse=.5)[:3] == (2,98.0,"STOP_GAP_BEFORE_TIMER")
    # A scheduled tighter stop cannot use the same candle's later time to act at its earlier open.
    p = path([(100,100.1,99.8,100),(100,100.1,98.9,99),(98.8,99.3,98.7,99)])
    assert exit_path(p,0,100,True,2,1.25,tighten_minutes=1)[:3] == (2,98.8,"TIGHTENED_STOP")
    # Short direction and conservative same-bar stop/target tie.
    p = path([(100,103,97,100)])
    assert exit_path(p,0,100,False,2,1.25)[:3] == (0,101.25,"STOP")
    # No next bar is fabricated when the loss signal occurs at the terminal close.
    p = path([(100,100.1,99.8,100),(100,100.1,99.3,99.4)])
    assert exit_path(p,0,100,True,2,1.25,timer_minutes=1,timer_adverse=.5)[:3] == (1,99.4,"TIME_EXIT_1515")
    # A short timer also fills at next open, even when price rebounds favorably.
    p = path([(100,100.2,99.8,100),(100,100.7,99.9,100.6),(100.2,100.4,99.8,100)])
    assert exit_path(p,0,100,False,2,1.25,timer_minutes=1,timer_adverse=.5)[:3] == (2,100.2,"ADVERSE_TIMER_NEXT_OPEN")
    return 6


def simulate(segments, base, rule):
    frames = []
    for segment_name, orders, paths in segments:
        work = orders.copy()
        work["native_stop_pct"] = rule["hard_stop"]
        native = source.g2.g.v9.v5.simulate_native(work, paths, cost_bps=base.cost_bps, max_entry_delay_minutes=10)
        frame = native.copy()
        for ix, row in native.iterrows():
            if not row.filled:
                continue
            p = paths[int(row.sid)]
            j, price, reason, final_stop, event = exit_path(
                p, int(row.entry_path_index), float(row.entry_price), row.side == "LONG",
                float(row.native_target_pct), rule["hard_stop"],
                tighten_minutes=rule.get("tighten_minutes"),
                timer_minutes=rule.get("timer_minutes"), timer_adverse=rule.get("timer_adverse"),
            )
            gross = (1 if row.side == "LONG" else -1) * (price / float(row.entry_price) - 1) * 100
            if rule["family"] == "STATIC":
                assert j == int(row.exit_path_index) and abs(price-float(row.exit_price)) < 1e-8
                assert reason == row.exit_reason
            excursion_end = max(int(row.entry_path_index), j-1) if event == "OPEN" else j
            mfe, mae = source.g2.g.v9.v5._excursions(p, float(row.entry_price), int(row.entry_path_index), excursion_end, row.side == "LONG")
            # Include actual fill, but exclude later high/low of an open-exit candle.
            mfe,mae=max(mfe,gross),min(mae,gross)
            bar_end = pd.Timestamp(int(p["timestamp_ns"][j]), tz="UTC").tz_convert("Asia/Kolkata")
            execution_ts = bar_end-pd.Timedelta(minutes=1) if event == "OPEN" else bar_end
            # Preserve published end-label convention for static baselines; explicitly
            # timestamp custom next-open fills when applying causal capital constraints.
            exit_ts=bar_end if rule["family"]=="STATIC" else execution_ts
            hit_stop=reason in ("STOP","TIGHTENED_STOP","STOP_GAP_BEFORE_TIMER")
            sign=1 if row.side=="LONG" else -1
            stop_level=float(row.entry_price)*(1-sign*final_stop/100)
            target_level=float(row.entry_price)*(1+sign*float(row.native_target_pct)/100)
            target_in_bar=p["high"][j]>=target_level if sign==1 else p["low"][j]<=target_level
            gap=hit_stop and event=="OPEN" and abs(price-stop_level)>1e-8
            changes = dict(exit_path_index=j, exit_price=price, exit_ts=exit_ts,
                           exit_bar_end_ts=str(bar_end), exit_execution_ts=str(execution_ts), exit_event=event,
                           exit_reason=reason, gross_return_pct=gross,
                           net_return_pct=gross-base.cost_bps/100, mfe_pct=mfe, mae_pct=mae,
                           holding_minutes=(exit_ts-pd.Timestamp(row.entry_ts)).total_seconds()/60,
                           initial_stop_pct=rule["hard_stop"], active_stop_pct_at_exit=final_stop,
                           stop_hit=hit_stop,target_hit=reason=="TARGET",first_target_hit=reason=="TARGET",
                           runner_target_hit=reason=="TARGET",same_bar_ambiguous=hit_stop and event=="INTRABAR" and target_in_bar,
                           exit_gap_through=gap,exit_gap_bps=abs(price/stop_level-1)*10000 if gap else 0.0)
            for key,value in changes.items():
                frame.loc[ix,key] = value
        frame["segment"] = segment_name
        frame = source.g2.g.v9.v5.apply_fixed_capital_model(frame, base.capital_per_entry_rupees, base.leverage_factor)
        frames.append(frame)
    all_trades = pd.concat(frames, ignore_index=True, sort=False)
    return source.g2.g.v9.v6.apply_portfolio_constraints(all_trades, base.portfolio_config())


def mark_to_market(ledger, segments, days):
    """Close-marked equity only; not tick-level or intra-minute maximum drawdown."""
    lookup = {name: paths for name, _, paths in segments}
    equity = [0.0]
    carry = 0.0
    for day in map(str, days):
        start = pd.Timestamp(f"{day} 09:16", tz="Asia/Kolkata")
        grid = pd.date_range(start, pd.Timestamp(f"{day} 15:15", tz="Asia/Kolkata"), freq="min").asi8
        pnl = np.zeros(len(grid))
        selected = ledger[ledger.day.astype(str).eq(day) & ledger.portfolio_executed.eq(True)]
        for r in selected.itertuples():
            p = lookup[r.segment][int(r.sid)]
            a,b = int(r.entry_path_index),int(r.exit_path_index)
            size,cost = float(r.exposure_per_entry_rupees), float(r.portfolio_cost_rupees)
            path_ts = p["timestamp_ns"]
            grid_indices = np.searchsorted(grid,path_ts[a:b])
            sign = 1 if r.side=="LONG" else -1
            pnl[grid_indices] += sign * (p["close"][a:b]/float(r.entry_price)-1)*size-cost
            pnl[grid>=path_ts[b]] += float(r.portfolio_net_profit_rupees)
        assert abs(pnl[-1]-float(selected.portfolio_net_profit_rupees.sum())) < 1e-6
        equity.extend((carry+pnl).tolist())
        carry += float(pnl[-1])
    curve = np.array(equity)
    return float(np.max(np.maximum.accumulate(curve)-curve))


def clean(v):
    if isinstance(v, dict): return {k:clean(x) for k,x in v.items()}
    if isinstance(v, list): return [clean(x) for x in v]
    if isinstance(v, np.generic): return clean(v.item())
    if isinstance(v, float) and not np.isfinite(v): return None
    return v


def main():
    test_count=validate_synthetic()
    published, segments, days = source.prepared_segments()
    base=published["base"]
    rules=[dict(name=f"STATIC_{s:.2f}", family="STATIC",hard_stop=s) for s in (1.0,1.25,2.75)]
    rules += [dict(name=f"TIGHTEN_{s:.2f}_TO_1.00_AFTER_{m}M",family="TIGHTEN",hard_stop=s,tighten_minutes=m)
              for s in (1.20,1.25,1.30) for m in (60,90,105,115,120,125,135,150,180)]
    rules += [dict(name=f"TIGHTEN_1.25_TO_1.00_AFTER_{m}M",family="TIGHTEN",hard_stop=1.25,tighten_minutes=m) for m in (15,30)]
    rules += [dict(name=f"HARD_{s:.2f}_AGE_{m}M_CLOSE_BELOW_{a:.2f}",family="ADVERSE_TIMER",hard_stop=s,timer_minutes=m,timer_adverse=a)
              for s in (1.0,1.25) for m in (30,60,90,120) for a in (.5,.75)]
    rows=[]; store={}
    for rule in rules:
        ledger, summary = simulate(segments,base,rule)
        ex=ledger[ledger.portfolio_executed.eq(True)].copy()
        pnl=ex.portfolio_net_profit_rupees.to_numpy(float)
        losers=pnl[pnl<0]
        metric=source.metrics(ledger,days,rule["hard_stop"])
        metric.update(rule=rule,worst_loss_rupees=float(-pnl.min()),
                      avg_loss_magnitude_rupees=float(-losers.mean()),
                      minute_close_drawdown_rupees=mark_to_market(ledger,segments,days),
                      peak_open_initial_risk_rupees=summary["peak_open_initial_risk_rupees"],
                      custom_exit_count=int(ex.exit_reason.isin(["TIGHTENED_STOP","ADVERSE_TIMER_NEXT_OPEN","STOP_GAP_BEFORE_TIMER"]).sum()))
        active_days=int(ex.day.astype(str).nunique())
        metric["active_days"]=active_days
        metric["positive_session_pct_active"]=100*metric["positive_sessions"]/active_days
        metric["equal_5250_nominal_stop_risk_net_rupees"]=float(pnl.sum())*1.05/(rule["hard_stop"]+.05)
        # Diagnostic chronological slices reuse the already-seen history.
        metric["slices"]={}
        for name, mask in [("jul_aug",ex.day.astype(str).lt("2026-09-01")),("september",ex.day.astype(str).ge("2026-09-01"))]:
            p=ex.loc[mask,"portfolio_net_profit_rupees"]
            metric["slices"][name]=dict(trades=len(p),wins=int(p.gt(0).sum()),net=float(p.sum()))
        daily=ex.groupby(ex.day.astype(str)).portfolio_net_profit_rupees.sum().reindex(list(map(str,days)),fill_value=0)
        metric["daily_net"]=daily.to_dict()
        rows.append(metric)
        store[rule["name"]]=ex
        print(rule["name"],metric["wins"],round(metric["net_profit_rupees"],2),round(metric["minute_close_drawdown_rupees"],2),flush=True)
    baseline=store["STATIC_1.00"]
    keys=["segment","day","sid","setup_id","tradingsymbol","side"]
    baseline_pnl=baseline.set_index(keys).portfolio_net_profit_rupees
    for item in rows:
        ex=store[item["rule"]["name"]]
        other=ex.set_index(keys).portfolio_net_profit_rupees.reindex(baseline_pnl.index)
        assert not other.isna().any()
        delta=other-baseline_pnl
        item["changed_vs_1pct"]=dict(improved=int(delta.gt(1e-7).sum()),worsened=int(delta.lt(-1e-7).sum()),
            rescued_winners=int(((baseline_pnl<0)&(other>0)).sum()),lost_winners=int(((baseline_pnl>0)&(other<0)).sum()))
        if item["rule"]["name"]=="TIGHTEN_1.25_TO_1.00_AFTER_120M":
            compare=ex.merge(baseline[keys+["portfolio_net_profit_rupees","exit_reason","exit_ts"]],on=keys,suffixes=("","_1pct"))
            compare["delta_rupees"]=compare.portfolio_net_profit_rupees-compare.portfolio_net_profit_rupees_1pct
            fields=keys+["entry_ts","exit_ts","exit_execution_ts","exit_reason","portfolio_net_profit_rupees", "exit_reason_1pct","exit_ts_1pct","portfolio_net_profit_rupees_1pct","delta_rupees"]
            item["changed_trades"]=compare.loc[compare.delta_rupees.abs().gt(1e-7),fields].astype({"day":str,"entry_ts":str,"exit_ts":str,"exit_ts_1pct":str}).to_dict("records")
    rng=np.random.default_rng(20261005)
    samples=rng.integers(0,len(days),size=(30000,len(days)))
    ref=np.array(list(rows[0]["daily_net"].values()))
    for item in rows:
        diff=np.array(list(item["daily_net"].values()))-ref
        sims=diff[samples].sum(axis=1)
        item["paired_day_bootstrap_net_delta_95pct"]=np.quantile(sims,[.025,.975]).tolist()
    output=dict(status="REUSED_HISTORY_DIAGNOSTIC_NO_UNTOUCHED_HOLDOUT",window=[str(days[0]),str(days[-1])],
        rule_count=len(rules),synthetic_checks_passed=test_count,
        modeling_notes=["Minute end labels; entry end used as conservative time origin.",
          "Timer uses completed closes and following opens. Gap-through hard stops take precedence.",
          "Scheduled tightening at bar open only after specified elapsed time.",
          "Static exit_ts keeps source candle-end label; dynamic open exits carry actual next-open timestamp. MTM snapshots use end of execution candle.",
          "MAE/MFE include entire exit minute and may exceed actual pre-exit excursions.",
          "Close-marked drawdown includes unrealized P&L and full modeled costs at entry; excludes intra-minute excursions.",
          "Gross exposure Rs500000/trade; cost5bps; all targets and original order selections retained."],results=rows)
    OUT.mkdir(parents=True,exist_ok=True)
    (OUT/"timing_analysis.json").write_text(json.dumps(clean(output),indent=2,allow_nan=False)+"\n",encoding="utf-8")


if __name__=="__main__": main()
