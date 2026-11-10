"""Run predeclared selectors using verified full pools and native execution."""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from datetime import date
import numpy as np
import pandas as pd
from g3_v18_20261102.run import sha, dump, guard_writes, metrics
from g3_v18_20261102.data import execution_paths
from fno_v13_v10_g_3_daily_replay import simulate
from research.g3_0920_backtest import allocate
from . import selection, features

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent


def choose(frame, variant):
    if variant in selection.VARIANTS:
        return selection.select(frame, variant)
    if variant in ('or_market','or_market_loose','or_market_strict','or_group_score'):
        path = 'or_market' if variant == 'or_group_score' else variant
        mask = frame.gate_oi_lower | features.market_path(frame,path)
        return selection.select(frame,variant,removed_gates=('oi_lower',),extra_mask=mask,
            ranking_score='group_score' if variant=='or_group_score' else None)
    if variant == 'group_score':
        return selection.select(frame,variant,ranking_score='group_score')
    if variant == 'tod_rvol':
        return selection.select(frame,variant,removed_gates=('vol5',),extra_mask=frame.lab_tod_rvol.ge(1)&frame.lab_tod_count.ge(5))
    if variant.startswith('extension'):
        cap = float(variant[-1])
        mask = frame.v9_1m_atr14.gt(0)&np.isfinite(frame.lab_extension_atr)&frame.lab_extension_atr.le(cap)
        return selection.select(frame,variant,extra_mask=mask)
    if variant == 'efficiency30':
        return selection.select(frame,variant,extra_mask=frame.v18_stock_ready.eq(True)&frame.v18_efficiency_6.ge(.3))
    if variant == 'opening_breakout':
        mask = np.where(frame.side.eq('LONG'),frame.close>frame.lab_or_high,frame.close<frame.lab_or_low)
        mask &= frame.v18_stock_ready.eq(True)
        return selection.select(frame,variant,extra_mask=mask)
    raise ValueError(variant)


def relabel(frame, variant):
    out = frame.copy()
    out['day'] = out.day.astype(str).str[:10]
    out['variant'] = variant
    out['strategy_version'] = selection.VERSION+':'+variant
    out['source_control_version'] = selection.control.STRATEGY_VERSION
    out['strategy_fingerprint'] = 'research:'+variant
    out['execution_authority'] = False
    out['signal_id'] = ['lab:'+variant+':'+str(sid) for sid in out.sid]
    return out


def allocated(master, orders, variant, cost=5):
    frame = master.loc[master.sid.isin(orders.sid)].copy().reset_index(drop=True)
    # Selection metadata must describe this arm, not the union simulation.
    flags = orders.set_index('sid')
    for key in ('v9_selected','v9_filter_pass','v9_rank_in_setup_day','v10_g_f_core','v9_decision','lab_ranking_score'):
        if key in orders:
            frame[key] = frame.sid.map(flags[key])
    frame['cost_pct'] = cost/100.
    frame['net_return_pct'] = frame.gross_return_pct - cost/100.
    frame = relabel(frame,variant)
    return relabel(allocate(frame),variant)


def mtm_metrics(book, minutes):
    """Minute-close marked equity; actual stop fills included at their bar end.

    Round-trip cost charged at entry conservatively. Intrabar extremes and
    executable spread are unobserved, so this is not tick-level drawdown.
    """
    times = pd.Index(sorted(minutes.ts.unique()))
    curve = np.zeros(len(times),dtype=float)
    exposure = np.zeros(len(times),dtype=float)
    done = book.loc[book.portfolio_executed.eq(True)]
    by_symbol = {sym:p.set_index('ts').close for sym,p in minutes.groupby('tradingsymbol')}
    for row in done.itertuples():
        entry, exit = pd.Timestamp(row.entry_ts), pd.Timestamp(row.exit_ts)
        before = times < entry
        active = (times >= entry)&(times < exit)
        after = times >= exit
        # Exposure from model amount rather than an assumed round share count.
        amount = float(row.portfolio_trade_exposure_rupees)
        prices = by_symbol[row.tradingsymbol].reindex(times).to_numpy(float)
        if not np.isfinite(prices[active]).all():
            raise ValueError('Missing required minute-close MTM')
        sign = 1 if row.side=='LONG' else -1
        curve[active] += sign*(prices[active]/row.entry_price-1)*amount-float(row.portfolio_cost_rupees)
        curve[after] += float(row.portfolio_net_profit_rupees)
        exposure[active] += amount
    full = np.r_[0,curve]
    return dict(minute_close_drawdown_rs=float((np.maximum.accumulate(full)-full).max()),
        max_simultaneous_exposure_rs=float(exposure.max()) if len(exposure) else 0.,
        mtm_closing_pnl_rs=float(curve[-1]) if len(curve) else 0.)


def control_parity(raw, orders, ledger):
    expected = selection.native_control(raw)
    key = ['setup_id','tradingsymbol']
    if set(map(tuple,orders[key].to_numpy())) != set(map(tuple,expected[key].to_numpy())):
        raise ValueError('Custom control selection does not match native control')
    day = str(raw.day.iloc[0])[:10]
    path = ROOT/'g3_v18_20261102/results_20261102_complete/control_trades.csv'
    old = pd.read_csv(path,low_memory=False)
    old = old.loc[old.day.astype(str).str[:10].eq(day)]
    if day == '2026-11-02':
        p = Path('C:/TradingData/eqidv2/g3_g7_v4_through_20261102/g3_daily_20261102/portfolio_trades.csv')
        old = pd.read_csv(p,low_memory=False)
    if len(old):
        joined = ledger.merge(old,on=key,suffixes=('_new','_saved'),validate='one_to_one')
        if len(joined)!=len(old) or len(joined)!=len(ledger):
            raise ValueError('Control order identity differs from saved ledger')
        for col in ['entry_price','exit_price','portfolio_net_profit_rupees']:
            a,b = [pd.to_numeric(joined[col+s],errors='coerce') for s in ('_new','_saved')]
            if not np.allclose(a,b,atol=1e-5,rtol=0,equal_nan=True):
                raise ValueError(f'Saved control mismatch {day}: {col}')
        for col in ['filled','portfolio_executed','exit_reason']:
            a,b = [joined[col+s].astype(str).str.lower() for s in ('_new','_saved')]
            if not a.equals(b):
                raise ValueError(f'Saved control mismatch {day}: {col}')
    return dict(day=day,native_selected=len(expected),saved_orders=len(old),passed=True)


def run(inputs:Path,output:Path,days=None):
    inputs,output=inputs.resolve(),output.resolve()
    if not output.is_relative_to(PACKAGE) or output.exists():
        raise ValueError('New lab child output required')
    manifest=json.loads((inputs/'manifest.json').read_text())
    protocol=json.loads((inputs/'protocol.json').read_text())
    if sha(inputs/'protocol.json') != manifest['protocol_sha256']:
        raise ValueError('Protocol changed')
    calendar=[d for d in manifest['days'] if not days or d in days]
    code_files=[p for p in PACKAGE.glob('*.py')]
    config=dict(protocol=protocol,input_manifest_sha256=sha(inputs/'manifest.json'),
        days=calendar,code_hashes={p.name:sha(p) for p in code_files},
        control_payload=selection.control.strategy_payload(),status='FROZEN_BEFORE_OUTCOMES')
    guard_writes(PACKAGE)
    output.mkdir(parents=True)
    dump(output/'config.json',config)
    totals,model_pools,books,parities,failcounts=[],[],[],[],[]
    for n,day in enumerate(calendar):
        source=inputs/day
        rec=next(s for s in manifest['sessions'] if s['day']==day)
        if sha(source/'raw.parquet')!=rec['raw_sha256'] or sha(source/'minutes.parquet')!=rec['minutes_sha256']:
            raise ValueError('Frozen input changed')
        raw=pd.read_parquet(source/'raw.parquet')
        minutes=pd.read_parquet(source/'minutes.parquet')
        prepared=features.grouped_scores(selection.prepare(raw))
        folder=output/day;folder.mkdir()
        prepared.to_parquet(folder/'gates.parquet',index=False)
        allslot_prepared=prepared
        needed={s.signal_end for s in selection.donor.ACTIVE_SETUPS}
        observed=set(raw.signal_ts.dt.strftime('%H:%M'))
        if needed-observed:
            supplemental=PACKAGE/'expanded_slots'/f'{day}.parquet'
            if not supplemental.is_file():
                raise ValueError(f'All51 slot ablation requires missing-clock reconstruction: {day}')
            extra=pd.read_parquet(supplemental)
            if not set(extra.tradingsymbol).issubset(set(raw.tradingsymbol)):
                raise ValueError('Slot expansion changed the frozen stock cohort')
            extra['signal_ts']=pd.to_datetime(extra.signal_ts,utc=True).dt.tz_convert('Asia/Kolkata')
            if not set(extra.signal_ts.dt.strftime('%H:%M')).issubset(needed-observed):
                raise ValueError('Supplement overlaps existing clocks')
            extra['sid']=manifest['days'].index(day)*1_000_000+900_000+np.arange(len(extra))
            expanded=pd.concat([raw,extra],ignore_index=True,sort=False)
            allslot_prepared=features.grouped_scores(selection.prepare(expanded))
            allslot_prepared.to_parquet(folder/'all51_gates.parquet',index=False)
            dump(folder/'slot_expansion.json',dict(path=str(supplemental),sha256=sha(supplemental),
                missing_clocks=sorted(needed-observed),extra_rows=len(extra),cohort_unchanged=True))
        chosen,decisions={},[]
        model_pool=None
        for variant in protocol['rules']:
            orders,audit=choose(allslot_prepared if variant=='all51_slots' else prepared,variant)
            chosen[variant]=orders
            audit['variant']=variant
            cols=['sid','day','setup_id','tradingsymbol','variant','v9_selected','v9_filter_pass','v9_rank_in_setup_day','first_failed_gate','lab_core_pass']
            decisions.append(audit[cols])
            for gate,count in audit.loc[~audit.v9_filter_pass].first_failed_gate.value_counts().items():
                failcounts.append(dict(day=day,variant=variant,first_failed_gate=gate,candidates=int(count)))
            if variant=='or_market':
                model_pool=audit.loc[audit.v9_filter_pass].copy()
        union=pd.concat([*chosen.values(),model_pool],ignore_index=True).drop_duplicates('sid').reset_index(drop=True)
        union['signal_id']=['union:'+str(sid) for sid in union.sid]
        paths=execution_paths(union,minutes)
        master,_=simulate(union,paths,[date.fromisoformat(day)])
        master=relabel(master,'independent_candidate_outcomes')
        master.to_parquet(folder/'candidate_outcomes.parquet',index=False)
        pd.concat(decisions,ignore_index=True).to_parquet(folder/'selection_decisions.parquet',index=False)
        out_fields=['sid','filled','entry_ts','exit_ts','entry_price','exit_price','gross_return_pct','net_return_pct','exit_reason']
        model_pool=model_pool.drop(columns=[c for c in out_fields if c in model_pool and c!='sid'])
        model_pool=model_pool.merge(master[out_fields],on='sid',validate='one_to_one')
        model_pools.append(model_pool)
        daily_books=[]
        for variant,orders in chosen.items():
            for cost in protocol['costs_bps']:
                book=allocated(master,orders,variant,cost)
                metric=metrics(book,[day])
                done=book.loc[book.portfolio_executed.eq(True)]
                metric.update(day=day,variant=variant,cost_bps=cost,
                    losing_days=int(metric['net_pnl_rs']<0),
                    turnover_entry_rs=float(done.portfolio_trade_exposure_rupees.sum()),
                    turnover_roundtrip_rs=float((done.portfolio_trade_exposure_rupees*(1+done.exit_price/done.entry_price)).sum()),
                    capital_rejections=int((book.filled.eq(True)&~book.portfolio_executed.eq(True)).sum()))
                if cost==5:
                    metric.update(mtm_metrics(book,minutes))
                    if not np.isclose(metric['mtm_closing_pnl_rs'],metric['net_pnl_rs'],atol=1e-5):
                        raise ValueError('MTM closing balance mismatch')
                    daily_books.append(book)
                    if variant=='control':
                        parities.append(control_parity(raw,orders,book))
                totals.append(metric)
        # A predeclared fill stress actually changes trigger/fill eligibility.
        for variant in ('control','or_market','group_score','or_group_score'):
            stressed=chosen[variant].copy()
            sign=np.where(stressed.side.eq('LONG'),1.,-1.)
            stressed['trigger']=stressed.trigger*(1+sign*.0001)
            stress,_=simulate(stressed,execution_paths(stressed,minutes),[date.fromisoformat(day)])
            stress=relabel(stress,variant+'__trigger1bp')
            daily_books.append(stress)
            metric=metrics(stress,[day]);metric.update(day=day,variant=variant+'__trigger1bp',cost_bps=5)
            totals.append(metric)
        daybook=pd.concat(daily_books,ignore_index=True)
        daybook.to_parquet(folder/'trades.parquet',index=False)
        books.append(daybook)
        pd.DataFrame([r for r in totals if r['day']==day]).to_csv(folder/'daily.csv',index=False)
        print(f'{day} [{n+1}/{len(calendar)}] full={len(raw)} matched={len(prepared)} simulated={len(master)} modelpool={len(model_pool)} parity=PASS',flush=True)
    pd.DataFrame(totals).to_csv(output/'daily_results.csv',index=False)
    pd.DataFrame(failcounts).to_csv(output/'gate_failures.csv',index=False)
    pd.concat(books,ignore_index=True).to_parquet(output/'trades.parquet',index=False)
    pd.concat(model_pools,ignore_index=True).to_parquet(output/'model_pool.parquet',index=False)
    dump(output/'control_parity.json',parities)
    dump(output/'rules_complete.json',dict(days=calendar,variants=protocol['rules'],native_parity_days=len(parities),execution_authority=False))
    return output

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--inputs',type=Path,default=PACKAGE/'inputs')
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--days',nargs='*')
    p.add_argument('--start');p.add_argument('--end')
    a=p.parse_args()
    days=a.days
    if a.start or a.end:
        planned=json.loads((a.inputs/'manifest.json').read_text())['days']
        days=[d for d in planned if (not a.start or d>=a.start) and (not a.end or d<=a.end)]
    run(a.inputs,a.output,days)
