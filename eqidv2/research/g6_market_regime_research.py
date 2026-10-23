"""Market diagnostics and causal entry-veto experiments on a sealed G6 run.

No production configuration, historical baseline, or latest pointer is changed.
Run: python -m research.g6_market_regime_research --asof-day 2026-10-22 --asof-time 12:00
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g6_direction_refinements as allocation
from research import g6_g4_replica_backtest as g6
from research.g4_extended_target_sweep import read_frozen_csv

ROOT = Path(__file__).resolve().parents[1]
DATA = Path('C:/TradingData/eqidv2/fno_oi')
IST = 'Asia/Kolkata'
VERSION = 'G6-MARKET-REGIME-RESEARCH-V1'
RULES = {
    'baseline': 'Approved G6 Nifty futures: reject only when slot and session both oppose',
    'recent_and_vwap': 'Also reject: 15-minute Nifty move opposes by >0.10% and price is adverse to futures VWAP',
    'breadth_opposition': 'Also reject: >60% of covered F&O futures oppose from open and Nifty 15-minute move opposes',
    'breadth_reversal': 'Also reject: breadth moves >=10 percentage points against side in 15 minutes and Nifty 15-minute move opposes',
    'session_and_recent': 'Also reject: Nifty session direction opposes and 15-minute move opposes by >0.05%',
    'chop': 'Also reject: 30-minute trend efficiency <0.25 (six completed returns required)',
    'chop_high_vol': 'Also reject: efficiency <0.25 and same-clock realized volatility >1.25x prior-20-session median',
    'high_vol': 'Also reject: same-clock realized volatility >1.50x prior-20-session median',
    'vwap_crosses': 'Also reject: at least three futures session-VWAP sign crossings during last 30 minutes',
    'market_extension': 'Also reject: Nifty extends in trade direction >2 times mean true range of last 14 five-minute bars from VWAP',
    'weak_participation': 'Also reject: volume <0.70x prior-20-session same-clock median and efficiency <0.35',
    'cross_index_opposition': 'Also reject: >=3 of Bank, Financial, Midcap and Next50 futures oppose in both session and 15-minute returns',
    'combined': 'Also reject: breadth_reversal OR chop_high_vol',
}
DESCRIPTORS = ['gap_pct', 'session_return_pct', 'return15_pct', 'return30_pct',
    'efficiency30', 'rv_pct', 'range_pct', 'vwap_distance_pct', 'vwap_crosses30',
    'breadth_open_pct', 'breadth_prevclose_pct', 'breadth_change15_pp',
    'dispersion_pct', 'volume', 'oi_change_pct', 'rv_relative20', 'volume_relative20']


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False), encoding='utf-8')


def timestamps(values):
    return pd.to_datetime(values, utc=True).dt.tz_convert(IST).dt.as_unit('ns')


def index_features(frame, cutoff, previous_close=np.nan):
    """Compute using exactly completed bars, never the rest of the day's data."""
    stamp = pd.Timestamp(cutoff)
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=20)
    f = frame.loc[frame.ts.between(start, stamp)].sort_values('ts')
    expected = pd.date_range(start, stamp, freq='5min')
    if len(f) != len(expected) or not np.array_equal(f.ts.astype('int64'), expected.asi8):
        return {'index_complete': False}
    v = f[['open', 'high', 'low', 'close', 'volume', 'oi']].to_numpy(float)
    if not len(v) or not np.isfinite(v[:, :5]).all() or (v[:, :4] <= 0).any() or (v[:, 4] < 0).any():
        return {'index_complete': False}
    o, h, l, c, vol, oi = v.T
    if np.any(h < np.maximum(o, c)) or np.any(l > np.minimum(o, c)) or np.any(h < l):
        raise ValueError('Invalid index OHLC ordering')
    prev = np.r_[o[0], c[:-1]]
    changes = c - prev
    returns = np.log(c / prev)
    typ = (h + l + c) / 3
    cv = vol.cumsum()
    vw = np.divide((typ * vol).cumsum(), cv, out=np.full(len(cv), np.nan), where=cv > 0)
    tr = np.maximum(h - l, np.maximum(abs(h - prev), abs(l - prev)))
    eff = abs(c[-1] - c[-7]) / np.abs(np.diff(c[-7:])).sum() if len(c) >= 7 and np.abs(np.diff(c[-7:])).sum() > 0 else np.nan
    signs = np.sign(c[-7:] - vw[-7:])
    signs = signs[np.isfinite(signs) & (signs != 0)]
    flips = int(np.sum(signs[1:] != signs[:-1])) if len(c) >= 7 else np.nan
    atr = float(tr[-14:].mean()) if len(c) >= 14 else np.nan
    return dict(index_complete=True, index_feature_ts=str(stamp), bars=len(c),
        open=float(o[0]), close=float(c[-1]), high=float(h.max()), low=float(l.min()),
        gap_pct=float((o[0] / previous_close - 1) * 100) if np.isfinite(previous_close) else np.nan,
        session_return_pct=float((c[-1] / o[0] - 1) * 100),
        return15_pct=float((c[-1] / c[-4] - 1) * 100) if len(c) >= 4 else np.nan,
        return30_pct=float((c[-1] / c[-7] - 1) * 100) if len(c) >= 7 else np.nan,
        efficiency30=float(eff), rv_pct=float(np.sqrt(np.square(returns).sum()) * 100),
        range_pct=float((h.max() - l.min()) / o[0] * 100),
        vwap_distance_pct=float((c[-1] / vw[-1] - 1) * 100),
        vwap_crosses30=flips, extension_atr=float((c[-1] - vw[-1]) / atr) if atr > 0 else np.nan,
        volume=float(vol.sum()), oi_change_pct=float((oi[-1] / oi[0] - 1) * 100) if oi[0] > 0 else np.nan)


def breadth_features(raw, cutoff):
    """Archived dated universe; require >=90% of snapshot and 150 names on time."""
    f = raw.loc[~raw.is_index_future.eq(True)].copy()
    universe = f.underlying.nunique()
    f = f.loc[f.quality_state.eq('VALID') & timestamps(f.fetch_timestamp).le(cutoff)]
    f = f.drop_duplicates('underlying', keep=False)
    valid = np.isfinite(f[['price_change_pct_from_open', 'price_change_pct_day']].to_numpy(float)).all(axis=1)
    f = f.loc[valid]
    n = len(f)
    ready = n >= 150 and n >= .9 * universe
    result = dict(breadth_complete=ready, breadth_n=n, breadth_universe=universe,
                  breadth_open_pct=np.nan, breadth_prevclose_pct=np.nan, dispersion_pct=np.nan)
    if ready:
        result.update(breadth_open_pct=float(f.price_change_pct_from_open.gt(0).mean() * 100),
            breadth_prevclose_pct=float(f.price_change_pct_day.gt(0).mean() * 100),
            dispersion_pct=float(f.price_change_pct_from_open.std(ddof=0)),
            median_stock_return_pct=float(f.price_change_pct_from_open.median()))
    return result


def add_history(market):
    """Same-clock reference distributions strictly exclude the current day."""
    m = market.sort_values(['clock', 'day']).copy()
    for column in ('rv_pct', 'volume', 'range_pct', 'efficiency30', 'breadth_open_pct', 'dispersion_pct'):
        for n, minimum in ((20, 10), (60, 20)):
            group = m.groupby('clock')[column]
            med = group.transform(lambda s: s.shift().rolling(n, min_periods=minimum).median())
            count = group.transform(lambda s: s.shift().rolling(n, min_periods=1).count())
            m[f'{column}_median{n}'] = med
            m[f'{column}_prior_n{n}'] = count
            ranks = []
            for _, part in m.groupby('clock', sort=False):
                a = part[column].to_numpy(float)
                out = []
                for i, value in enumerate(a):
                    previous = a[max(0, i - n):i]
                    previous = previous[np.isfinite(previous)]
                    out.append(float((previous <= value).mean() * 100) if len(previous) >= minimum and np.isfinite(value) else np.nan)
                ranks.append(pd.Series(out, index=part.index))
            m[f'{column}_percentile{n}'] = pd.concat(ranks)
    m['rv_relative20'] = m.rv_pct / m.rv_pct_median20
    m['volume_relative20'] = m.volume / m.volume_median20
    return m.sort_values(['day', 'clock']).reset_index(drop=True)


def decisions(f):
    sign = f.side.map({'LONG': 1, 'SHORT': -1})
    r = sign * f.return15_pct
    breadth = sign * (f.breadth_open_pct - 50)
    momentum = sign * f.breadth_change15_pp
    chop = f.efficiency30.lt(.25)
    high = f.rv_relative20.gt(1.25)
    broadrev = momentum.le(-10) & r.lt(0)
    counts = sum(((sign * f[p + '_session_return_pct']).lt(0) &
                  (sign * f[p + '_return15_pct']).lt(0)).astype(int)
                 for p in ('BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50'))
    complete = f[[p + '_return15_pct' for p in ('BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50')]].notna().all(axis=1)
    v = pd.DataFrame(dict(baseline=False,
        recent_and_vwap=r.lt(-.10) & (sign * f.vwap_distance_pct).lt(0),
        breadth_opposition=breadth.lt(-10) & r.lt(0), breadth_reversal=broadrev,
        session_and_recent=(sign * f.session_return_pct).lt(0) & r.lt(-.05),
        chop=chop, chop_high_vol=chop & high, high_vol=f.rv_relative20.gt(1.5),
        vwap_crosses=f.vwap_crosses30.ge(3), market_extension=(sign * f.extension_atr).gt(2),
        weak_participation=f.volume_relative20.lt(.7) & f.efficiency30.lt(.35),
        cross_index_opposition=complete & (counts >= 3), combined=broadrev | (chop & high)), index=f.index)
    assert list(v) == list(RULES)
    return v.fillna(False)


def stats(book, days):
    s = allocation.bounded.stats(book, days)
    b = book.loc[book.day.isin(days) & book.portfolio_executed.eq(True)]
    p = b.portfolio_net_profit_rupees
    daily = b.groupby('day').portfolio_net_profit_rupees.sum().reindex(days, fill_value=0)
    s.update(stop_rate_pct=100*s['stop_exits']/len(b) if len(b) else 0,
        positive_trade_pnl_rs=float(p[p > 0].sum()), worst_day_rs=float(daily.min()),
        portfolio_return_pct=s['net_pnl_rs']/1000000*100,
        expectancy_rs=float(p.mean()) if len(p) else 0,
        profit_factor=float(p[p > 0].sum() / -p[p < 0].sum()) if (p < 0).any() else np.nan)
    return s


def compare(books, days, period):
    ref = books['baseline']
    original = ref.loc[ref.day.isin(days) & ref.portfolio_executed.eq(True)].set_index('sid')
    control = stats(ref, days)
    records = []
    for policy, book in books.items():
        s = stats(book, days)
        executed = set(book.loc[book.day.isin(days) & book.portfolio_executed.eq(True), 'sid'])
        removed = original.loc[~original.index.isin(executed)]
        pnl = removed.portfolio_net_profit_rupees
        added = book.loc[book.day.isin(days) & book.portfolio_executed.eq(True) & ~book.sid.isin(original.index)]
        records.append(dict(policy=policy, period=period, **s,
            delta_net_rs=s['net_pnl_rs']-control['net_pnl_rs'],
            delta_losing_days=s['losing_days']-control['losing_days'],
            delta_stops=s['stop_exits']-control['stop_exits'],
            losing_trades_avoided=int((pnl < 0).sum()), winning_trades_excluded=int((pnl > 0).sum()),
            loss_value_avoided_rs=float(-pnl[pnl < 0].sum()), winner_value_excluded_rs=float(pnl[pnl > 0].sum()),
            winner_value_excluded_pct=float(pnl[pnl > 0].sum()/control['positive_trade_pnl_rs']*100) if control['positive_trade_pnl_rs'] else 0,
            newly_allocated_trades=len(added), newly_allocated_pnl_rs=float(added.portfolio_net_profit_rupees.sum())))
    return pd.DataFrame(records)


def build_market(source, days, today, through, output, provenance):
    rawdir = DATA / 'raw_contracts_5m'
    raw = {}
    def load(path):
        path = Path(path)
        key = str(path.resolve())
        if key not in raw:
            before = sha(path)
            rank = 'rankings' in path.parts
            columns = ['timestamp','underlying','is_index_future','quality_state','fetch_timestamp',
                       'price_change_pct_from_open','price_change_pct_day'] if rank else None
            f = pd.read_parquet(path, columns=columns)
            if sha(path) != before:
                raise ValueError('Input changed during read: ' + key)
            f['ts'] = timestamps(f.timestamp)
            f['day'] = f.ts.dt.strftime('%Y-%m-%d')
            provenance[key] = before
            if rank:
                return f
            raw[key] = f
        return raw[key]
    records = []
    for day in [*days, today]:
        sealed = source/'nifty_inputs'/f'{day}.parquet'
        if sealed.exists():
            frame = load(sealed)
            symbol = str(frame.tradingsymbol.iloc[0])
        else:
            contracts = []
            for path in rawdir.glob('NIFTY26*FUT_5minute.parquet'):
                tmp = load(path)
                part = tmp.loc[tmp.day.eq(day)]
                if len(part) and pd.Timestamp(part.expiry.iloc[0]).strftime('%Y-%m-%d') >= day:
                    contracts.append((pd.Timestamp(part.expiry.iloc[0]), path, part))
            if not contracts:
                raise ValueError('No Nifty contract for ' + day)
            _, path, frame = min(contracts, key=lambda x: x[0])
            symbol = str(frame.tradingsymbol.iloc[0])
        frame = frame.loc[frame.day.eq(day)].copy()
        month = symbol[len('NIFTY'):-len('FUT')]
        history = load(rawdir/(symbol+'_5minute.parquet'))
        previous = history.loc[history.day.lt(day) & history.ts.dt.strftime('%H:%M').eq('15:30')].sort_values('ts')
        pc = float(previous.close.iloc[-1]) if len(previous) and (pd.Timestamp(day)-pd.Timestamp(previous.day.iloc[-1])).days <= 7 else np.nan
        other = {}
        for name in ('BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50'):
            path = rawdir/(name+month+'FUT_5minute.parquet')
            other[name] = load(path) if path.exists() else None
        end = through if day == today else '13:30'
        for stamp in pd.date_range(day+' 09:20', day+' '+end, freq='5min', tz=IST):
            clock = stamp.strftime('%H:%M')
            result = dict(day=day, clock=clock, nifty_contract=symbol,
                          **index_features(frame, stamp, pc))
            path = DATA/'rankings'/day/('fno_oi_rankings_'+clock.replace(':','')+'.parquet')
            if path.exists():
                ranks = load(path)
                # Each G6 slot confirms one minute after the five-minute bar.
                result.update(breadth_features(ranks, stamp+pd.Timedelta(minutes=1)))
            else:
                result.update(breadth_complete=False, breadth_n=0, breadth_universe=0,
                              breadth_open_pct=np.nan, breadth_prevclose_pct=np.nan, dispersion_pct=np.nan)
            for name, f in other.items():
                values = index_features(f, stamp) if f is not None else {}
                result.update({name+'_'+k: values.get(k, np.nan) for k in ('session_return_pct','return15_pct')})
            records.append(result)
        print('Market features:', day, flush=True)
    market = pd.DataFrame(records)
    lookup = market.set_index(['day','clock']).breadth_open_pct
    def lag(row):
        stamp = pd.Timestamp(row.day+' '+row.clock)-pd.Timedelta(minutes=15)
        return lookup.get((row.day, stamp.strftime('%H:%M')), np.nan)
    market['breadth_change15_pp'] = market.breadth_open_pct-market.apply(lag,axis=1)
    # Today's market raw inputs are mutable: preserve the exact feature input slices.
    frozen = output/'market_inputs'
    frozen.mkdir()
    for i, (key, f) in enumerate(raw.items()):
        if '/nifty_inputs/' in key.replace('\\','/'):
            continue
        if 'rankings' in key:
            continue  # Per-file hashes and dated archives identify these immutable snapshots.
        columns = [c for c in f if c != 'day']
        f.loc[f.day.le(today) & (~f.day.eq(today) | f.ts.le(pd.Timestamp(today+' '+through, tz=IST))), columns].to_parquet(frozen/f'{i:03d}_{Path(key).name}', index=False)
    return add_history(market)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--asof-day', default='2026-10-22')
    p.add_argument('--asof-time', default='12:00')
    p.add_argument('--output', type=Path)
    p.add_argument('--market-cache', type=Path)
    args = p.parse_args()
    pointer = json.loads((g6.G6_ROOT/'latest/bounded_exit.json').read_text())
    source = g6.G6_ROOT/pointer['run_dir']
    if sha(source/'manifest.json') != pointer['manifest_sha256']:
        raise ValueError('Baseline pointer hash mismatch')
    allocation.verify_package(source)
    summary = json.loads((source/'summary.json').read_text())
    days = summary['days']
    if args.asof_day <= days[-1]:
        raise ValueError('As-of session must follow closed historical baseline')
    output = args.output or ROOT/'outputs'/('g6_market_regime_'+datetime.now().strftime('%Y%m%dT%H%M%S'))
    output = output.resolve()
    if output.exists():
        raise ValueError('Fresh output directory required')
    output.mkdir(parents=True)
    dump(output/'rules.json', dict(version=VERSION, rules=RULES, scope='BACKTEST_ONLY', execution_authority=False,
        thresholds_frozen_before_outcome_analysis=True, missing_features='Keep approved G6 decision; never interpret missing as zero',
        training_sessions=35, selection='Training net >= baseline, losing days and drawdown <= baseline, <=10% baseline positive trade PnL excluded; maximize avoided negative PnL, tie by net PnL. Otherwise baseline.'))
    provenance = {}
    if args.market_cache:
        cache = args.market_cache.resolve()
        proof = json.loads((cache/'market_cache_manifest.json').read_text())
        if proof['source_manifest_sha256'] != sha(source/'manifest.json') or proof['asof_day'] != args.asof_day or proof['asof_time'] != args.asof_time:
            raise ValueError('Wrong market feature cache')
        for name,digest in proof['files'].items():
            if sha(cache/name) != digest:
                raise ValueError('Market feature cache changed: '+name)
        market = pd.read_csv(cache/'market_slots.csv')
        provenance = json.loads((cache/'source_provenance.json').read_text())
        shutil.copytree(cache/'market_inputs',output/'market_inputs')
    else:
        market = build_market(source, days, args.asof_day, args.asof_time, output, provenance)
    market.to_csv(output/'market_slots.csv', index=False)
    dump(output/'source_provenance.json',provenance)
    cache_files=[output/'market_slots.csv',output/'source_provenance.json',*list((output/'market_inputs').glob('*'))]
    dump(output/'market_cache_manifest.json',dict(source_manifest_sha256=sha(source/'manifest.json'),asof_day=args.asof_day,asof_time=args.asof_time,
        files={str(x.relative_to(output)):sha(x) for x in cache_files}))
    original = read_frozen_csv(source/'g6_unfiltered/portfolio_trades.csv')
    saved = read_frozen_csv(source/'g6_bounded_exit/portfolio_trades.csv')
    audit = pd.read_csv(source/'nifty_direction_audit.csv')
    keys = original[['sid','day','setup_id','tradingsymbol','side','confirmation_ts']].copy()
    keys['clock'] = timestamps(keys.confirmation_ts).dt.floor('5min').dt.strftime('%H:%M')
    features = keys.merge(market, on=['day','clock'], how='left', validate='many_to_one').set_index('sid').loc[original.sid].reset_index()
    assert features.index.equals(original.index) and features.sid.equals(original.sid)
    complete_index = features.index_complete.fillna(False)
    if not (pd.to_datetime(features.loc[complete_index,'index_feature_ts'], utc=True) <= pd.to_datetime(features.loc[complete_index,'confirmation_ts'], utc=True)).all():
        raise ValueError('Future feature used')
    if not (timestamps(features.confirmation_ts).dt.minute % 5 == 1).all():
        raise ValueError('Breadth availability cutoff assumes G6 confirmation one minute after bar')
    baseline_allowed = original.sid.map(audit.set_index('sid').both_counter_allowed).astype(bool)
    signs = features.side.map({'LONG':1,'SHORT':-1})
    canonical = (signs*features.session_return_pct).ge(0) | (signs*original.sid.map(audit.set_index('sid').slot_candle)).ge(0)
    assert np.array_equal(canonical[complete_index], baseline_allowed[complete_index])
    vetoes = decisions(features)
    vetoes.loc[~complete_index,:] = False
    config = json.loads((source/'config.json').read_text())
    books = {}
    for policy in RULES:
        veto = (~baseline_allowed) | vetoes[policy]
        books[policy] = allocation.allocate(original, veto, config, policy)
        assert not (books[policy].portfolio_executed.eq(True) & veto).any()
    for field in ('portfolio_executed','portfolio_net_profit_rupees'):
        pd.testing.assert_series_equal(books['baseline'][field], saved[field], check_dtype=False, check_names=False, atol=1e-7, rtol=0)
    periods = {'all55':days, 'training35':days[:35], 'validation20':days[35:], 'october':[d for d in days if d.startswith('2026-10')], 'last_closed_day':days[-1:]}
    comparison = pd.concat([compare(books,d,period) for period,d in periods.items()],ignore_index=True)
    training = comparison.loc[comparison.period.eq('training35')].copy()
    baseline = training.loc[training.policy.eq('baseline')].iloc[0]
    eligible = training.loc[training.policy.ne('baseline') & training.delta_net_rs.ge(-1e-7)
        & training.losing_days.le(baseline.losing_days) & training.daily_close_drawdown_rs.le(baseline.daily_close_drawdown_rs+1e-7)
        & training.winner_value_excluded_pct.le(10)]
    selected = eligible.sort_values(['loss_value_avoided_rs','net_pnl_rs'],ascending=False).policy.iloc[0] if len(eligible) else 'baseline'
    comparison.to_csv(output/'policy_comparison.csv',index=False)
    daily=[]
    for policy, book in books.items():
        for day in days:
            daily.append(dict(policy=policy,day=day,**stats(book,[day])))
        folder=output/'policies'/policy
        folder.mkdir(parents=True)
        columns=['sid','day','setup_id','tradingsymbol','side','confirmation_ts','filled','entry_ts','exit_ts','exit_reason','gross_return_pct','portfolio_executed','portfolio_net_profit_rupees','research_veto']
        book[columns].to_csv(folder/'trades.csv',index=False)
    daily=pd.DataFrame(daily)
    daily['delta_net_rs']=daily.net_pnl_rs-daily.day.map(daily.loc[daily.policy.eq('baseline')].set_index('day').net_pnl_rs)
    daily.to_csv(output/'daily_comparison.csv',index=False)
    group=[]
    for policy,book in books.items():
        for (setup,side),part in book.groupby(['setup_id','side']):
            group.append(dict(policy=policy,setup_id=setup,side=side,**stats(part,days)))
    pd.DataFrame(group).to_csv(output/'slot_side_comparison.csv',index=False)
    trade = features.merge(saved[['sid','portfolio_executed','portfolio_net_profit_rupees','exit_reason']],on='sid',validate='one_to_one')
    for policy in RULES:
        trade['veto_'+policy]=vetoes[policy].to_numpy()
    trade.to_csv(output/'trade_features.csv',index=False)
    # Descriptive bins are fixed, never estimated from future trade outcomes.
    bins={'efficiency30':[-np.inf,.25,.5,np.inf], 'rv_relative20':[-np.inf,.75,1.25,1.5,np.inf],
          'breadth_open_pct':[-np.inf,40,60,np.inf], 'breadth_change15_pp':[-np.inf,-10,10,np.inf],
          'volume_relative20':[-np.inf,.7,1.3,np.inf]}
    associations=[]
    for field,edges in bins.items():
        b=trade.loc[trade.portfolio_executed.eq(True)].copy()
        b['bin']=pd.cut(b[field],edges).astype(str)
        for (bucket,side),part in b.groupby(['bin','side']):
            pnl=part.portfolio_net_profit_rupees
            associations.append(dict(parameter=field,bin=bucket,side=side,trades=len(part),
                stops=int(part.exit_reason.isin(['STOP','TIGHTENED_STOP']).sum()),
                stop_rate_pct=100*part.exit_reason.isin(['STOP','TIGHTENED_STOP']).mean(),net_pnl_rs=float(pnl.sum()),expectancy_rs=float(pnl.mean())))
    pd.DataFrame(associations).to_csv(output/'parameter_outcomes.csv',index=False)
    # The current morning is a diagnostic only; no incomplete-day PnL enters selection.
    latest=market.loc[market.day.isin([days[-1],args.asof_day]) & market.clock.le(args.asof_time)]
    latest.to_csv(output/'latest_market_comparison.csv',index=False)
    costs=[]
    for cost in (5,10,15):
        candidates={name:allocation.allocate(original,(~baseline_allowed)|vetoes[name],config,name,cost) for name in dict.fromkeys(['baseline',selected])}
        costs.append(compare(candidates,days[35:],'validation20').assign(cost_bps=cost))
    pd.concat(costs).to_csv(output/'cost_sensitivity.csv',index=False)
    availability=[]
    for field in DESCRIPTORS:
        availability.append(dict(parameter=field,available_orders=int(features[field].notna().sum()),total_orders=len(features),coverage_pct=float(features[field].notna().mean()*100)))
    for field in ('historical_intraday_VIX','historical_option_skew','cash_sector_membership','historical_orderbook_spread_depth','macro_event_calendar','cross_stock_correlation'):
        availability.append(dict(parameter=field,available_orders=0,total_orders=len(features),coverage_pct=0))
    coverage=pd.DataFrame(availability)
    coverage.to_csv(output/'coverage.csv',index=False)
    checks={
        'baseline_allocation_and_pnl_exact':True,
        'unique_order_ids':bool(not features.sid.duplicated().any()),
        'index_features_at_or_before_confirmation':True,
        'chronological_reference_excludes_current_day':True,
        'daily_totals_reconcile':bool(all(np.isclose(daily.loc[daily.policy.eq(name),'net_pnl_rs'].sum(),stats(book,days)['net_pnl_rs']) for name,book in books.items())),
        'pnl_delta_reconciles_saved_losses_minus_lost_winners_plus_new_allocation':bool(np.allclose(comparison.delta_net_rs,comparison.loss_value_avoided_rs-comparison.winner_value_excluded_rs+comparison.newly_allocated_pnl_rs,atol=1e-7)),
        'current_morning_excluded_from_outcomes':args.asof_day not in days,
        'production_pointer_unchanged':pointer==json.loads((g6.G6_ROOT/'latest/bounded_exit.json').read_text()),
    }
    if not all(checks.values()):
        raise ValueError(checks)
    result=dict(version=VERSION,source_run=str(source),source_manifest_sha256=sha(source/'manifest.json'),
        first_day=days[0],last_closed_day=days[-1],sessions=len(days),orders=len(original),
        baseline_trades=int(saved.portfolio_executed.sum()),complete_index_orders=int(complete_index.sum()),training_days=days[:35],validation_days=days[35:],
        selected_using_training_only=selected,asof_day=args.asof_day,asof_time=args.asof_time,
        scope='BACKTEST_ONLY',execution_authority=False,checks=checks,
        capital_rs=config['portfolio_capital_rs'],capital_per_trade_rs=config['capital_per_entry_rs'],leverage=config['leverage'],cost_bps=5,
        limitations=[
            'Chronological validation is a retrospective check; this historical period was already used for earlier strategy research and is not an untouched prospective holdout.',
            'Historical index bars are causal by bar end, but historical fetch/arrival latency cannot be established for every repaired index bar.',
            'A missing 09:30 Nifty bar on 18 September invalidates seven order feature windows; all new vetoes preserve the approved baseline for those orders.',
            'Breadth uses dated archived F&O futures, not cash-stock breadth; require 150 names and 90% timely snapshot coverage. Missing features preserve the approved baseline decision.',
            '60-session reference uses at most available prior sessions, with actual n reported; there are fewer than 60 prior sessions in this source.',
            'No historical VIX/options skew/sector membership/orderbook/event calendar/cross-stock correlation was fabricated. Their absence is reported.',
            'Daily drawdown is based on closed-day PnL, not intraday mark-to-market. Existing fill paths and costs are inherited; gates precede portfolio reallocation.',
            'Candidate thresholds are research assumptions. No policy is deployed or advertised as preventing all losses.',
        ])
    dump(output/'summary.json',result)
    dump(output/'source_provenance.json',provenance)
    dump(output/'checks.json',checks)
    report(output,result,comparison,daily,latest,coverage)
    # Typed intermediary for the result workbook; no formulas are pre-rendered strings.
    display=['policy','period','trades','stop_exits','stop_rate_pct','losing_days','net_pnl_rs','delta_net_rs','loss_value_avoided_rs','winner_value_excluded_rs','winner_value_excluded_pct','daily_close_drawdown_rs']
    noon=latest.loc[latest.clock.eq(args.asof_time),['day','clock',*DESCRIPTORS]]
    workbook_data=dict(summary=result,policies=comparison[display].replace({np.nan:None}).to_dict('records'),
        daily=daily[['policy','day','trades','stop_exits','losing_days','net_pnl_rs','delta_net_rs']].to_dict('records'),
        market=noon.replace({np.nan:None}).to_dict('records'),
        coverage=coverage.to_dict('records'),rules=[dict(policy=k,rule=v) for k,v in RULES.items()],
        trades=trade[['day','setup_id','tradingsymbol','side','portfolio_executed','exit_reason','portfolio_net_profit_rupees',*DESCRIPTORS]].replace({np.nan:None}).to_dict('records'))
    dump(output/'workbook_data.json',workbook_data)
    dump(output/'manifest.json',dict(files={str(x.relative_to(output)):sha(x) for x in output.rglob('*') if x.is_file()},code_sha256=sha(__file__)))
    print('OUTPUT',output,flush=True)
    print('TRAINING_SELECTED',selected,flush=True)
    print(comparison.loc[comparison.period.isin(['all55','validation20']),['policy','period','trades','stop_exits','losing_days','net_pnl_rs','delta_net_rs','winner_value_excluded_pct']].to_string(index=False),flush=True)


def report(output,result,comparison,daily,latest,coverage):
    def table(frame):
        return '<div class="table">'+frame.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}',na_rep='Unavailable')+'</div>'
    selected=result['selected_using_training_only']
    cols=['policy','trades','stop_exits','stop_rate_pct','losing_days','net_pnl_rs','delta_net_rs','loss_value_avoided_rs','winner_value_excluded_rs','winner_value_excluded_pct','daily_close_drawdown_rs']
    body=['<!doctype html><html><head><meta charset="utf-8"><title>G6 market conditions</title><style>body{font:15px/1.6 Arial;color:#182c40;background:#f7f9fb;max-width:1500px;margin:auto;padding:28px}h1,h2{color:#18324d}.table{overflow:auto;margin:20px 0}table{border-collapse:collapse;background:white}td,th{padding:9px;border-bottom:1px solid #dbe2e8;text-align:right;white-space:nowrap}th{background:#203e59;color:white}td:first-child{text-align:left}a{color:#185c97}.note{background:#fff3d6;padding:14px}</style></head><body><h1>G6 market conditions and filter results</h1>',
        f'<p>{result["first_day"]} through {result["last_closed_day"]}: {result["sessions"]} completed sessions, {result["baseline_trades"]} baseline trades. Current-morning diagnostics: {result["asof_day"]} through {result["asof_time"]} IST.</p>',
        f'<p class="note">Training-only selection: <b>{html.escape(selected)}</b>. First 35 sessions choose a rule; the final 20 sessions evaluate that choice. Results are retrospective research; production settings remain as previously selected.</p>',
        '<h2>Chronological validation: final 20 sessions</h2>',table(comparison.loc[comparison.period.eq('validation20'),cols]),
        '<h2>All 55 completed sessions</h2>',table(comparison.loc[comparison.period.eq('all55'),cols]),
        '<h2>Current morning versus previous session, same clock</h2>',table(latest.loc[latest.clock.isin(['09:30','10:10','10:40','11:10',result['asof_time']]),['day','clock',*DESCRIPTORS]]),
        '<h2>Daywise baseline and training-selected rule</h2>',table(daily.loc[daily.policy.isin(['baseline',selected]),['day','policy','trades','stop_exits','net_pnl_rs','delta_net_rs']]),
        '<h2>Rules fixed before examining outcomes</h2>',table(pd.DataFrame([{'policy':k,'rule':v} for k,v in RULES.items()])),
        '<h2>Feature coverage</h2>',table(coverage),
        '<h2>Interpretation and limitations</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in result['limitations'])+'</ul>',
        '<p>Capital: Rs 1,000,000; capital per entry Rs 100,000; exposure 5x; baseline round-trip cost 5 bps. Stop exits include initial and tightened stops. Winners/losses excluded are measured against baseline executed trades; newly allocated trades are reconciled separately.</p>',
        '<p>Sources and hashes: <a href="source_provenance.json">market inputs</a>; <a href="summary.json">sealed G6 source and methodology</a>; <a href="checks.json">verification</a>.</p>',
        '<p>'+ ' | '.join(f'<a href="{name}">{name}</a>' for name in ['policy_comparison.csv','daily_comparison.csv','slot_side_comparison.csv','trade_features.csv','market_slots.csv','parameter_outcomes.csv','cost_sensitivity.csv'])+'</p></body></html>']
    (output/'report.html').write_text(''.join(body),encoding='utf-8')


if __name__ == '__main__':
    main()
