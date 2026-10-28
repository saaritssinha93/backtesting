"""Retrospective G7 V2 entry-stretch audit. Research only; no trading writes.

Two fixed limits (3 and 4 ATR) are borrowed from existing G4 research and
applied to the known trigger, not the subsequent realized fill. No optimization.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research.g7_entry_quality_20261026 import verify_members, same_book, choose
from research.g8_exit_engine import allocate_variant, metrics, _flags
from research.g8_vix_selection_research import sha, dump
from research.g4_vwap_rsi_features import _check_flags

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_8/entry_quality_research/2026-10-26/20261026T152938356201_fixed_rules')
DAILY = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/daily/runs/2026-10-26/20261026T194254854688_5b5a77f8_g7_nifty_futures')
DAY = '2026-10-26'
IST = 'Asia/Kolkata'
OHLC = ['open', 'high', 'low', 'close']


def causal_features(raw, row):
    """Only candles ending at/before confirmation contribute to entry features."""
    stamp = pd.Timestamp(row['confirmation_ts'])
    stamp = stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)
    if row['side'] not in ('LONG', 'SHORT'):
        raise ValueError('Invalid side')
    sign = 1 if row['side'] == 'LONG' else -1
    prefix = raw.loc[raw.ts.le(stamp)].sort_values('ts').copy()
    if len(prefix) < 20 or prefix.ts.duplicated().any() or prefix.iloc[-1].ts != stamp:
        raise ValueError('Missing or duplicate confirmation/warmup')
    values = prefix[OHLC].apply(pd.to_numeric, errors='coerce')
    if not np.isfinite(values).all().all() or values.le(0).any().any():
        raise ValueError('Invalid OHLC prefix')
    if (values.high < values[['open', 'low', 'close']].max(axis=1)).any() or (values.low > values[['open', 'high', 'close']].min(axis=1)).any():
        raise ValueError('Inconsistent OHLC')
    if not np.allclose(values.iloc[-1], [row['confirmation_'+c] for c in OHLC], atol=.001, rtol=0):
        raise ValueError('Archived confirmation differs from price source')
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
    session = prefix.loc[prefix.ts.ge(start)]
    expected = pd.date_range(start, stamp, freq='min')
    if not pd.DatetimeIndex(session.ts).equals(expected):
        raise ValueError('Incomplete session through confirmation')
    _check_flags(prefix.tail(20))
    _check_flags(session)
    for window in (prefix.tail(20), session):
        if 'is_synthetic' in window and _flags(window.is_synthetic.where(window.is_synthetic.notna(), False), 'is_synthetic').any():
            raise ValueError('Synthetic pre-entry candles')
    prev = values.close.shift()
    tr = pd.concat([values.high-values.low, (values.high-prev).abs(), (values.low-prev).abs()], axis=1).max(axis=1)
    atr = float(tr.tail(14).mean())
    ema = float(values.close.ewm(span=20, adjust=False).mean().iloc[-1])
    trigger = float(row['trigger'])
    if not np.isfinite([trigger, atr, ema]).all() or min(trigger, atr, ema) <= 0:
        raise ValueError('Invalid feature or trigger')
    vol = pd.to_numeric(session.volume, errors='coerce')
    if not np.isfinite(vol).all() or vol.lt(0).any() or vol.sum() <= 0:
        raise ValueError('Invalid session volume')
    vwap = float((((session.high+session.low+session.close)/3)*vol).sum()/vol.sum())
    lo, hi = float(session.low.min()), float(session.high.max())
    local = session.tail(20)
    edge = hi if sign == 1 else lo
    local_edge = float(local.high.max() if sign == 1 else local.low.min())
    return dict(sid=int(row['sid']), feature_at=stamp.isoformat(), ema20=ema, atr14=atr,
        trigger_extension_atr=sign*(trigger-ema)/atr,
        close_extension_atr=sign*(float(row['confirmation_close'])-ema)/atr,
        session_vwap=vwap, trigger_vwap_distance_pct=sign*(trigger/vwap-1)*100,
        session_low_so_far=lo, session_high_so_far=hi,
        trigger_distance_from_session_edge_atr=sign*(trigger-edge)/atr,
        trigger_distance_from_local20_edge_atr=sign*(trigger-local_edge)/atr,
        session_move_to_confirmation_pct=(float(row['confirmation_close'])/float(session.open.iloc[0])-1)*100,
        atr_current_session_bars=min(14, len(session)), ema_includes_previous_sessions=True)


def stretch_decisions(features):
    base = _flags(features.allowed_g7_v2, 'allowed_g7_v2')
    extension = pd.to_numeric(features.trigger_extension_atr, errors='coerce')
    if not np.isfinite(extension).all():
        raise ValueError('Missing extension cannot masquerade as a rejection')
    available = pd.to_datetime(features.feature_at, utc=True)
    confirmation = pd.to_datetime(features.confirmation_ts, utc=True)
    if available.isna().any() or confirmation.isna().any() or (available > confirmation).any():
        raise ValueError('Feature is from the future')
    return dict(g7_v2=base, trigger_max3atr=base & extension.le(3), trigger_max4atr=base & extension.le(4))


def path_diagnostics(raw, row):
    """Post-entry observations for explanations only; never used in decisions."""
    entry, end = pd.Timestamp(row['entry_ts']), pd.Timestamp(row['exit_ts'])
    sign, price = (1 if row['side'] == 'LONG' else -1), float(row['entry_price'])
    # Exclude entry and exit candles: intrabar event ordering is unknown.
    active = raw.loc[raw.ts.gt(entry) & raw.ts.lt(end)].copy()
    returns = sign*(active.close/price-1)*100
    out = dict(holding_minutes=(end-entry).total_seconds()/60)
    out['best_completed_close_pct_before_exit'] = float(returns.max()) if len(returns) else np.nan
    out['best_completed_close_time'] = active.loc[returns.idxmax(), 'ts'] if len(returns) else None
    for minute in (1, 5, 15):
        found = active.loc[active.ts.eq(entry+pd.Timedelta(minutes=minute))]
        out[f'completed_close_return_after_{minute}m_pct'] = float(sign*(found.iloc[0].close/price-1)*100) if len(found) else np.nan
    return out


def run():
    proof = verify_members(SOURCE)
    daily_proof = verify_members(DAILY, 'bf0c8f2fd778a90d81fe4bbe86616ad11136c43a7f616b8fab8c3578b78da0e9')
    config = json.loads((DAILY/'config.json').read_text())
    prior = json.loads((SOURCE/'summary.json').read_text())
    periods = {('yesterday' if k == 'today' else k): v for k,v in prior['periods'].items()}
    protected = {str(p): sha(p) for p in [ROOT/'configs/strategies/active_profile.json'] if p.exists()}
    features = pd.read_csv(SOURCE/'entry_features_and_decisions.csv').set_index('sid', drop=False)
    native = pd.read_csv(SOURCE/'no_index_filter/portfolio_trades.csv')
    baseline = pd.read_csv(SOURCE/'g7_v2/portfolio_trades.csv')
    if len(native) != 596 or set(native.sid) != set(features.sid):
        raise ValueError('Wrong candidate inventory')
    if native.sid.duplicated().any() or baseline.sid.duplicated().any() or set(baseline.sid) != set(native.sid):
        raise ValueError('Duplicate or mismatched candidates')
    baseline = baseline.set_index('sid').loc[native.sid].reset_index()
    rows = native.set_index('sid')
    out = DAILY.parents[3]/'entry_extremes_research'/DAY/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'_fixed_limits')
    out.mkdir(parents=True, exist_ok=False)
    dump(out/'protocol.json', dict(scope='BACKTEST_ONLY', execution_authority=False,
        candidates=['trigger_max3atr', 'trigger_max4atr'], limits_atr=[3,4],
        decision='Original G7 Nifty gate AND directional (known trigger - EMA20)/ATR14 <= limit',
        provenance='3/4 ATR limits already exist in G4 entry-veto research; using trigger here instead of close.',
        calculation='EMA20 spans sessions; ATR14 simple average true range includes overnight gap. No day-end extrema.',
        independent_holdout=False, yesterday_excluded_from_selection=True,
        selection='Same development/validation criteria as prior fixed-rule study; later eight sessions assessed separately.'))
    calculated, diagnostics, chart_sources = [], [], {}
    sources = features.groupby(['stock_source_path','stock_source_sha256'], sort=False)
    for number, ((path, digest), group) in enumerate(sources, 1):
        path = Path(path)
        if sha(path) != digest:
            raise ValueError('Stock source changed: '+str(path))
        columns = ['date', *OHLC, 'volume', 'gap_filled','opening_snapshot','provisional_stale','is_synthetic']
        schema = pq.read_schema(path).names
        raw = pd.read_parquet(path, columns=[c for c in columns if c in schema])
        raw['ts'] = pd.to_datetime(raw.date, utc=True).dt.tz_convert(IST)
        for sid in group.sid:
            row = rows.loc[sid].to_dict(); row['sid'] = sid
            calculated.append(causal_features(raw, row))
            b = baseline.loc[baseline.sid.eq(sid)].iloc[0]
            if str(row['day']) == DAY and _flags(pd.Series([b.portfolio_executed]), 'executed').iloc[0]:
                diagnostics.append(dict(sid=sid, **path_diagnostics(raw, row)))
                chart_sources[sid] = raw.loc[raw.ts.ge(pd.Timestamp(row['confirmation_ts'])-pd.Timedelta(minutes=20)) & raw.ts.le(pd.Timestamp(row['exit_ts']))].copy()
        if sha(path) != digest:
            raise ValueError('Source changed while reading')
        if number % 30 == 0:
            print('Verified price files', number, '/', len(sources), flush=True)
    f = features.join(pd.DataFrame(calculated).set_index('sid'), validate='one_to_one')
    masks = stretch_decisions(f)
    comparisons, daily_results, books = [], [], {}
    for policy, mask in masks.items():
        book = allocate_variant(native, mask, config, policy)
        book['strategy'] = 'G7_V2_ENTRY_EXTREMES_RESEARCH'
        books[policy] = book
        if policy == 'g7_v2':
            same_book(book, baseline)
        executed = _flags(book.portfolio_executed, 'portfolio_executed')
        changed = executed.ne(_flags(baseline.portfolio_executed, 'baseline'))
        for period, days in periods.items():
            stats = metrics(book, days)
            select = baseline.day.isin(days)
            comparisons.append(dict(policy=policy, period=period, **stats,
                net_change_rs=stats['net_pnl_rs']-metrics(baseline, days)['net_pnl_rs'],
                changed_executions=int((changed & select).sum()),
                changed_days=int(baseline.loc[changed & select, 'day'].nunique())))
        for day in periods['all']:
            daily_results.append(dict(policy=policy, day=day, **metrics(book,[day])))
        folder = out/policy; folder.mkdir()
        book.to_csv(folder/'portfolio_trades.csv', index=False)
        same_book(pd.read_csv(folder/'portfolio_trades.csv'), book)
        f['allowed_'+policy] = mask
    comparison = pd.DataFrame(comparisons)
    selected, selection = choose(comparison, ['trigger_max3atr','trigger_max4atr'])
    audit_cols = ['sid','tradingsymbol','side','confirmation_ts','trigger','entry_ts','entry_price','exit_ts','exit_reason','portfolio_executed','portfolio_net_profit_rupees','mfe_pct','mae_pct']
    yesterday = baseline.loc[baseline.day.eq(DAY), audit_cols].set_index('sid')
    new_cols = list(pd.DataFrame(calculated).columns.drop('sid'))
    yesterday = yesterday.join(f[new_cols]).join(pd.DataFrame(diagnostics).set_index('sid'))
    for policy, book in books.items():
        indexed = book.set_index('sid')
        yesterday[policy+'_executed'] = indexed.portfolio_executed
        yesterday[policy+'_net_rs'] = indexed.portfolio_net_profit_rupees
    f.to_csv(out/'entry_features_and_decisions.csv', index=False)
    yesterday.to_csv(out/'yesterday_entry_audit.csv')
    comparison.to_csv(out/'comparison.csv', index=False)
    pd.DataFrame(daily_results).to_csv(out/'daywise_comparison.csv', index=False)
    pd.DataFrame(selection).to_csv(out/'selection_audit.csv', index=False)
    plot_entries(yesterday, chart_sources, out)
    summary = dict(scope='BACKTEST_ONLY', execution_authority=False, promotion=False,
        baseline_reproduced=True, selected_policy=selected, independent_holdout=False,
        candidates=596, sessions=len(periods['all']), periods=periods,
        source_packages=[proof,daily_proof], protected_files=protected,
        limitations=prior['limitations']+['Full-prefix EMA/ATR recomputed from sealed prices; ATR includes previous-session bars at the open.',
            'Trigger stretch is known at confirmation; realized gap-through entry may have different stretch.',
            'Post-entry diagnostics exclude entry/exit candles to avoid ambiguous intrabar ordering; not selection features.'])
    if any(sha(Path(p)) != h for p,h in protected.items()):
        raise ValueError('Active configuration changed')
    dump(out/'summary.json', summary)
    html = "<!doctype html><meta charset='utf-8'><title>G7 V2: 26 October entry audit</title><style>body{font:15px system-ui;margin:32px}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:7px}img{max-width:100%}</style>"
    html += '<h1>G7 V2 entry extremes — 26 October 2026</h1><p>Retrospective research only; no live or paper changes. Full archived baseline reproduced exactly. Two fixed 3/4 ATR limits applied to the known trigger using completed candles only. EMA spans sessions; ATR14 includes overnight gaps.</p>'
    html += '<p>Eight trades, four wins and four losses. BANDHANBNK is stretched; CAMS and KFINTECH fail quickly with lower stretch. BANDHANBNK and GODREJPROP first have favourable movement and subsequently reverse. See completed-close returns below; future observations never enter filter decisions.</p>'
    html += '<p>BANDHANBNK was shorted at the known session low after a 2.70% opening-to-confirmation fall. KFINTECH was bought at the known session high after a 3.29% rise. CAMS was only 0.15 ATR above the known session low. GODREJPROP was 4.53 ATR below the known session high, so its loss cannot be explained by buying that high. These are price-context observations, not proof that a reversal was predictable.</p>'
    cols = ['tradingsymbol','side','entry_ts','portfolio_net_profit_rupees','trigger_extension_atr','trigger_distance_from_session_edge_atr','best_completed_close_pct_before_exit','completed_close_return_after_1m_pct','exit_reason']
    html += yesterday.loc[_flags(yesterday.portfolio_executed,'executed'),cols].to_html(index=False,float_format=lambda x:f'{x:,.3f}')
    html += '<h2>Price paths</h2><p>Completed minute closes relative to entry. Shading shows post-entry observations, used for diagnosis only. Entry/exit candles have ambiguous intrabar ordering.</p><img src="yesterday_price_paths.png">'
    html += f'<p>Development/validation screening selected: {selected or "NONE"}. These are previously inspected historical data, not independent validation. Three missing sessions (24/25 August and 1 October) are excluded. INR 10 lakh portfolio, INR 1 lakh margin with 5x exposure per entry, 5bps model costs.</p>'
    for period in periods:
        html += '<h2>'+period+'</h2>'+comparison.loc[comparison.period.eq(period),['policy','trades','wins','losses','net_pnl_rs','net_change_rs','daily_close_drawdown_rs']].to_html(index=False,float_format=lambda x:f'{x:,.2f}')
    html += pd.DataFrame(selection).to_html(index=False)
    html += '<h2>What this supports</h2><p>Neither blanket stretch limit qualifies: both remove many historical winners and worsen the validation-period daily drawdown. Retain the existing G7 V2 configuration. A pullback/retest acceptance rule for entries at the known session edge and a separate profit-giveback exit rule are hypotheses for future testing, not validated fixes. A generic one-bar hold was already tested in the preceding fixed-rule study and did not qualify. Do not tune these rules against these eight known outcomes.</p>'
    html += '<p><a href="yesterday_entry_audit.csv">All 14 opportunities and entry audit</a> | <a href="daywise_comparison.csv">Daywise comparison</a> | <a href="summary.json">Method and limitations</a></p>'
    (out/'report.html').write_text(html,encoding='utf-8')
    code = [Path(__file__), ROOT/'research/g8_exit_engine.py',ROOT/'research/g7_entry_quality_20261026.py',ROOT/'research/g4_vwap_rsi_features.py']
    dump(out/'manifest.json',dict(files={p.relative_to(out).as_posix():sha(p) for p in out.rglob('*') if p.is_file()},code={str(p.relative_to(ROOT)):sha(p) for p in code}))
    verify_members(out)
    print(comparison[['policy','period','trades','wins','losses','net_pnl_rs','net_change_rs','daily_close_drawdown_rs']].to_string(index=False),flush=True)
    print('OUTPUT',out,'SELECTED',selected,flush=True)
    return out


def plot_entries(audit, prices, out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(4,2,figsize=(14,14),constrained_layout=True)
    for ax,(sid,row) in zip(axes.flat,audit.loc[_flags(audit.portfolio_executed,'executed')].sort_values('entry_ts').iterrows()):
        raw = prices[sid]
        entry = pd.Timestamp(row.entry_ts)
        x = (raw.ts-entry).dt.total_seconds()/60
        sign = 1 if row.side == 'LONG' else -1
        y = sign*(raw.close/row.entry_price-1)*100
        ax.plot(x,y,color='#2457a7',lw=1.4)
        ax.axhline(0,color='#666',lw=.7); ax.axvline(0,color='#222',ls='--',lw=.9)
        ax.axvspan(0,max(x),color='#e8eef8',alpha=.5)
        ax.set_title(f'{row.tradingsymbol} {row.side} | {entry:%H:%M} | Rs {row.portfolio_net_profit_rupees:,.0f}\nTrigger stretch {row.trigger_extension_atr:.2f} ATR; {row.exit_reason}',fontsize=10)
        ax.set_xlabel('Minutes from modeled entry'); ax.set_ylabel('Directional close return (%)')
        ax.grid(alpha=.18)
    fig.suptitle('G7 V2 — 26 October 2026 | All eight executed trades',fontsize=16)
    fig.savefig(out/'yesterday_price_paths.png',dpi=140)
    plt.close(fig)


if __name__ == '__main__':
    run()
