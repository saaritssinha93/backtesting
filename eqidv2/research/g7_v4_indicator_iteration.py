"""Bounded indicator-entry research on the USER-SELECTED protected G7 V4.

Two predefined stages; development chooses once before later periods are
evaluated. All historical evidence is retrospective. No default activation.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research import g7_v4_selected_backtest as selected
from research import g7_v4_extremes as features_v4
from research import g7_v4_profit_protection as changes
from research.g4_vwap_rsi_features import wilder_rsi
from research.g7_entry_quality_20261026 import same_book, verify_members
from research.g8_exit_engine import _flags, allocate_variant, metrics
from research.g8_vix_selection_research import dump, sha
import fno_v13_corrected_v5_backtest as native_engine

base = selected.history
ROOT = selected.ROOT
OUT_ROOT = base.OUT_ROOT / 'protected_indicator_research'
VERSION = 'V13-V10-G-7-V4-PROTECTED-ENTRY-INDICATORS-RESEARCH'
ROUND1 = {
    'vwap_rsi': 'Reject VWAP directional z>2.5 AND RSI14 weakening over3 completed minutes.',
    'historical_rsi': 'Reject prior60-session same-clock historical95 extreme AND weakening RSI14.',
    'rolling_divergence': 'Reject VWAP/historical extreme AND new10-bar price edge with RSI at least3 points weaker than its prior10-bar extreme.',
    'chop_rejection': 'Reject VWAP/historical extreme AND CHOP14>=61.8 AND confirmation rejection.',
    'low_efficiency': 'Reject VWAP/historical extreme AND10-change efficiency ratio<0.30 AND weakening RSI14.',
    'exhaustion_volume': 'Reject VWAP/historical extreme AND weakening RSI14 AND last5-bar volume below prior5 AND directional wick>=25%.',
}
ROUND2 = {
    'vwap_divergence': 'Narrow divergence veto: VWAP z>2.5 AND rolling price/RSI divergence.',
    'consensus2': 'Reject VWAP/historical extreme AND at least2 of RSI weakness/divergence/choppy inefficiency/volume rejection.',
    'early_hist_late_vwap': 'Before30 session bars reject historical95 AND rejection; afterwards reject VWAP z>2.5 AND RSI weakness.',
    'risk_confirm_next': 'Only VWAP+RSI risk orders wait for first completed minute to hold trigger and confirmation edge; arm next minute inside original10-minute expiry.',
}
POLICIES = {**ROUND1, **ROUND2}
ACCEPTANCE = dict(
    reference='protected_g7_v4', net_profit='>=baseline', losing_amount='<=95% baseline',
    loss_count='<baseline', win_rate='>=baseline', winners='>=95% baseline absolute wins',
    trade_retention='>=80% baseline', daily_close_drawdown='<=baseline',
    changed_trades_min=5, changed_days_min=3,
    ranking='highest development net profit, then smallest losing amount, then absolute winners, then name',
    selection='First35 development sessions only; single lock; no later replacement.',
    later_checks='Same gates on14-session validation and8-session assessment; cost15/30bps later blocks net>=baseline.',
)


def indicator_row(raw, row):
    """Current-session prefix only; no pivots requiring future confirmation."""
    stamp = features_v4.timestamp(row['confirmation_ts'])
    window = features_v4.checked_session(raw, stamp)
    for name in ('open', 'high', 'low', 'close'):
        if not np.isclose(window[name].iloc[-1], row['confirmation_' + name], atol=.001, rtol=0):
            raise ValueError('Confirmation OHLC drift')
    sign = 1 if row['side'] == 'LONG' else -1 if row['side'] == 'SHORT' else None
    if sign is None:
        raise ValueError('Unknown side')
    result = dict(sid=int(row['sid']), indicator_available_at=stamp.isoformat(),
        indicator_ready=len(window) >= 30, efficiency10=np.nan, chop14=np.nan,
        volume_ratio5=np.nan, price_rsi_divergence=False, directional_close_position=np.nan)
    if len(window) < 30:
        return result
    close = window.close.to_numpy(float)
    high, low = window.high.to_numpy(float), window.low.to_numpy(float)
    volume = window.volume.to_numpy(float)
    travel = np.abs(np.diff(close[-11:])).sum()
    er = abs(close[-1] - close[-11]) / travel if travel > 0 else 0.
    previous = close[-15:-1]
    tr = np.maximum.reduce([high[-14:] - low[-14:], abs(high[-14:] - previous), abs(low[-14:] - previous)])
    span = high[-14:].max() - low[-14:].min()
    chop = 100 * np.log10(tr.sum() / span) / np.log10(14) if span > 0 and tr.sum() > 0 else np.nan
    rsi = wilder_rsi(close)
    price = high if sign == 1 else low
    prior_edge = price[-11:-1].max() if sign == 1 else price[-11:-1].min()
    prior_rsi = rsi[-11:-1].max() if sign == 1 else rsi[-11:-1].min()
    divergence = sign * (price[-1] - prior_edge) > 0 and sign * (rsi[-1] - prior_rsi) <= -3.
    candle_range = high[-1] - low[-1]
    position = ((close[-1] - low[-1]) if sign == 1 else (high[-1] - close[-1])) / candle_range if candle_range > 0 else .5
    prior_volume = volume[-10:-5].sum()
    result.update(efficiency10=float(er), chop14=float(chop),
        volume_ratio5=float(volume[-5:].sum() / prior_volume) if prior_volume > 0 else np.nan,
        price_rsi_divergence=bool(divergence), directional_close_position=float(position))
    return result


def build_features(original, native):
    indexed = native.set_index('sid', drop=False)
    records = []
    groups = original.groupby(['stock_source_path', 'stock_source_sha256'], sort=False)
    for number, ((filename, digest), group) in enumerate(groups, 1):
        path = Path(filename)
        if sha(path) != digest:
            raise ValueError('Stock source changed')
        schema = pq.read_schema(path).names
        columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'gap_filled', 'opening_snapshot', 'provisional_stale', 'is_synthetic']
        raw = pd.read_parquet(path, columns=[c for c in columns if c in schema])
        raw['ts'] = pd.to_datetime(raw.date, utc=True).dt.tz_convert(features_v4.IST)
        # Filter before per-order feature computation to keep historical files bounded.
        wanted = set(group.day)
        raw = raw.loc[raw.ts.dt.strftime('%Y-%m-%d').isin(wanted)]
        for sid in group.sid:
            records.append(indicator_row(raw, indexed.loc[sid]))
        if sha(path) != digest:
            raise ValueError('Stock source changed while reading')
        if number % 30 == 0:
            print('INDICATOR PREFIXES', number, '/', len(groups), flush=True)
    extra = pd.DataFrame(records).set_index('sid')
    return original.join(extra, validate='one_to_one')


def decisions(f):
    """Only advertised pre-entry features; never read outcome columns."""
    old = features_v4.flags(f)
    available = pd.to_datetime(f.indicator_available_at, utc=True)
    confirmation = pd.to_datetime(f.confirmation_ts, utc=True)
    if available.isna().any() or (available > confirmation).any():
        raise ValueError('Noncausal new indicator feature')
    ready = _flags(f.indicator_ready, 'indicator_ready')
    for name in ('efficiency10', 'directional_close_position'):
        if not np.isfinite(f.loc[ready, name]).all():
            raise ValueError('Missing ready indicator: ' + name)
    if not f.loc[ready, 'efficiency10'].between(0, 1 + 1e-10).all():
        raise ValueError('Invalid efficiency ratio')
    weak = _flags(f.rsi_ready, 'rsi_ready') & f.signed_rsi_delta3.lt(0)
    divergence = ready & _flags(f.price_rsi_divergence, 'price_rsi_divergence')
    choppy = ready & f.chop14.ge(61.8)
    inefficient = ready & f.efficiency10.lt(.30)
    volume = ready & f.volume_ratio5.lt(1.) & f.directional_wick_fraction.ge(.25)
    extreme = old['extreme']
    consensus = weak.astype(int) + divergence.astype(int) + (choppy & inefficient).astype(int) + volume.astype(int)
    result = dict(
        vwap_rsi=old['vwap'] & weak,
        historical_rsi=old['historical'] & weak,
        rolling_divergence=extreme & divergence,
        chop_rejection=extreme & choppy & _flags(f.rejection, 'rejection'),
        low_efficiency=extreme & inefficient & weak,
        exhaustion_volume=extreme & weak & volume,
        vwap_divergence=old['vwap'] & divergence,
        consensus2=extreme & consensus.ge(2),
        early_hist_late_vwap=(f.session_bars.lt(30) & old['historical'] & _flags(f.rejection, 'rejection')) | (f.session_bars.ge(30) & old['vwap'] & weak),
        risk_confirm_next=old['vwap'] & weak)
    return result


def confirm_next(native, paths, risk):
    """A first-bar observation cannot create an entry inside that same bar."""
    if not risk.index.is_unique or set(risk.index) != set(native.sid):
        raise ValueError('Risk decision inventory differs')
    records = []
    for source in native.to_dict('records'):
        row = source.copy()
        row['indicator_entry_status'] = 'ORIGINAL'
        if risk.loc[row['sid']]:
            p = paths[int(row['sid'])]
            stamp = features_v4.timestamp(row['confirmation_ts'])
            expected = pd.date_range(stamp + pd.Timedelta(minutes=1), periods=10, freq='min').tz_convert('UTC').asi8
            if not np.array_equal(p['timestamp_ns'][:10], expected):
                raise ValueError('Missing original10-minute entry window')
            a = np.column_stack([p[k][:10] for k in ('open', 'high', 'low', 'close')]).astype(float)
            if not np.isfinite(a).all() or (a <= 0).any() or (a[:, 1] < a.max(axis=1)).any() or (a[:, 2] > a.min(axis=1)).any():
                raise ValueError('Invalid original entry-window OHLC')
            sign = 1 if row['side'] == 'LONG' else -1
            edge = row['confirmation_low'] if sign == 1 else row['confirmation_high']
            holds = sign * (p['close'][0] - row['trigger']) >= 0 and sign * (p['close'][0] - edge) >= 0
            found = native_engine._entry(SimpleNamespace(**row), p, delay_bars=1,
                trigger_buffer_pct=0., worse_fill_bps=0., max_entry_delay_minutes=9) if holds else None
            row.update(filled=False, g8_baseline_filled=False, entry_ts=pd.NaT, entry_price=np.nan,
                entry_path_index=np.nan, exit_ts=pd.NaT, exit_price=np.nan, exit_path_index=np.nan,
                gross_return_pct=np.nan, net_return_pct=np.nan, exit_reason='UNFILLED',
                indicator_entry_status='FIRST_MINUTE_FAILED' if not holds else 'ARMED_EXPIRED')
            if found:
                i, price, trigger, gap, overshoot = found
                row.update(filled=True, g8_baseline_filled=True, entry_path_index=i, entry_price=price,
                    trigger_used=trigger, entry_gap_through=gap, entry_overshoot_bps=overshoot,
                    entry_ts=pd.Timestamp(int(p['timestamp_ns'][i]), tz='UTC').tz_convert(features_v4.IST),
                    indicator_entry_status='DELAYED_FILLED')
        records.append(row)
    return pd.DataFrame(records)


def make_book(native, paths, f, settings, base_config, name):
    masks = decisions(f)
    allowed = _flags(f.allowed_g7_v2, 'nifty')
    if name == 'g7_v4':
        source, permitted = native, allowed
    elif name == 'risk_confirm_next':
        source, permitted = confirm_next(native, paths, masks[name]), allowed
    else:
        source, permitted = native, allowed & ~masks[name]
    raw = selected.replay(source, paths, base_config, settings)
    cfg = dict(base_config, max_positions_per_symbol=1, variant_id=VERSION,
               protection=settings['exit_policy'])
    book = allocate_variant(raw, permitted, cfg, name, cost_bps=base_config['model_cost_bps_round_trip'])
    book['strategy'] = 'V13-V10-G-7'
    book['strategy_version'] = settings['variant_id'] if name == 'g7_v4' else VERSION
    book['indicator_policy'] = name
    return book


def compare(book, reference, days):
    r = changes.comparison(book, reference, days)
    r['win_count_change'] = r['wins'] - metrics(reference, days)['wins']
    r['loss_count_reduction'] = metrics(reference, days)['losses'] - r['losses']
    return r


def choose(table, names, period='development'):
    t = table.loc[table.period.eq(period)].set_index('policy')
    b = t.loc['g7_v4']
    eligible, audit = [], []
    for name in names:
        r = t.loc[name]
        failures = []
        if r.net_pnl_rs < b.net_pnl_rs - 1e-6: failures.append('net profit reduced')
        if r.negative_trade_sum_abs_rs > .95 * b.negative_trade_sum_abs_rs + 1e-6: failures.append('losing amount reduction below5%')
        if r.losses >= b.losses: failures.append('loss count not reduced')
        if r.win_rate_pct < b.win_rate_pct - 1e-8: failures.append('win rate reduced')
        if r.wins < .95 * b.wins: failures.append('winner retention below95%')
        if r.trades < .8 * b.trades: failures.append('trade retention below80%')
        if r.daily_close_drawdown_rs > b.daily_close_drawdown_rs + 1e-6: failures.append('larger daily-close drawdown')
        if r.changed_executions < 5 or r.changed_days < 3: failures.append('insufficient changed trades/days')
        audit.append(dict(policy=name, period=period, eligible=not failures, failure_reasons='; '.join(failures)))
        if not failures: eligible.append((r.net_pnl_rs, -r.negative_trade_sum_abs_rs, r.wins, name))
    return (max(eligible)[3] if eligible else None), audit


def reversal_audit(native, paths, baseline, f):
    """Post-entry outcomes are separate diagnostics, never decision features.

    A 'fast adverse move' requires an observable first5-minute signed open or
    completed close <=-0.3% before baseline exit. Entry/exit bar ranges excluded.
    It is a reproducible proxy; minute OHLC cannot establish intrabar order.
    """
    rows = []
    for row in baseline.loc[_flags(baseline.portfolio_executed, 'executed')].itertuples():
        p = paths[int(row.sid)]
        end = min(int(row.entry_path_index) + 6, int(row.exit_path_index))
        start = int(row.entry_path_index) + 1
        closes = p['close'][start:end]
        sign = 1 if row.side == 'LONG' else -1
        returns = sign * (closes / row.entry_price - 1) * 100
        # Stop/target in an early exit bar precludes observing that bar close;
        # the executed fill itself remains known and is safe as a diagnostic.
        early_exit = row.holding_minutes <= 5
        worst = min(float(returns.min()) if len(returns) else 0., row.gross_return_pct if early_exit else 0.)
        rows.append(dict(sid=row.sid, day=row.day, tradingsymbol=row.tradingsymbol, side=row.side,
            net_pnl_rs=row.portfolio_net_profit_rupees, exit_reason=row.exit_reason,
            first5_observed_closes=len(returns), first5_worst_observed_pct=worst,
            fast_adverse=worst <= -.3, net_loser=row.portfolio_net_profit_rupees < 0))
    return pd.DataFrame(rows).set_index('sid').join(f[list(POLICIES)].add_prefix('flag_'))


def source_state():
    paths = [selected.CONFIG, selected.OUT_ROOT / 'latest.json', base.OUT_ROOT / 'latest.json']
    return {**base.protected_hashes(), **{str(p): sha(p) for p in paths}}


def report(out, summary, table, daily, audit, diagnostics):
    show = lambda f: '<div class="scroll">' + f.to_html(index=False, float_format=lambda x: f'{x:,.2f}') + '</div>'
    html = '''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Protected G7 V4 indicator research</title><style>body{font:16px/1.5 system-ui;margin:30px;color:#20334a;max-width:1600px}.result{padding:18px;background:#eef5f8;border-left:5px solid #3f6e94}.scroll{overflow:auto}table{border-collapse:collapse;font-size:13px}td,th{border:1px solid #ccd5df;padding:8px;text-align:right}a{color:#145f99}</style></head><body>'''
    chosen = summary['selected_candidate']
    html += '<h1>G7 V4: indicators to reduce extreme entries and reversals</h1><p class="result">Baseline: selected +0.1% gross protection after completed-close +0.6%, with one active position per symbol. Development selection: <b>' + (chosen or 'NONE') + '</b>. Validation pass: ' + str(summary['validation_qualified']) + '; assessment pass: ' + str(summary['assessment_qualified']) + '; higher-cost pass: ' + str(summary['cost_qualified']) + '. No automatic default change.</p>'
    html += '<p>Ten predefined candidates in two stages. The first stage tests indicator families separately. The second tests narrower confirmation, consensus, early-session treatment and a one-minute entry observation. Both stages were declared before computing candidate outcomes;35 development sessions select one rule once. Later results cannot select a replacement.</p>'
    html += '<h2>Fixed iterations</h2>'
    for number, policies in ((1, ROUND1), (2, ROUND2)):
        html += '<h3>Stage ' + str(number) + '</h3><ul>' + ''.join('<li><b>' + k + '</b>: ' + v + '</li>' for k, v in policies.items()) + '</ul>'
    html += '<h2>Indicator definitions and timing</h2><p>All observations stop at original confirmation. VWAP uses session HLC3 weighted by incremental volume; z-score uses its weighted population deviation. RSI14 uses Wilder smoothing, reset by session, with3-minute weakening. Historical extremes use the pinned prior60 same-stock/same-clock references, excluding the current day. Efficiency10=absolute10-close net move/sum of absolute10 close changes;0.30 is fixed. CHOP14=100 log10(sum14 true ranges/(highest14 high-lowest14 low))/log10(14);61.8 is fixed. Rolling divergence compares a new10-bar price edge with the prior10 RSI extreme,3-point minimum; it uses no future-confirmed pivot. Volume contraction compares last5 minutes with the preceding5. New indicators need30 valid session bars; warmup preserves entries. Undefined CHOP/volume features are explicitly unavailable and cannot create a veto.</p>'
    html += '<p>The delayed candidate applies only to VWAP+RSI risk orders. It observes the first post-confirmation completed close, requires it to hold the original trigger and adverse confirmation edge, then arms at the next minute open. The original10-minute entry expiry remains; gaps reprice actual entries, targets/stops and +0.6/+0.1 protection. Every selected opportunity, including previously unfilled/capital-blocked orders, is replayed before allocation.</p>'
    cols = ['policy', 'trades', 'wins', 'losses', 'win_rate_pct', 'net_pnl_rs', 'net_change_rs', 'negative_trade_sum_abs_rs', 'loss_amount_reduction_rs', 'daily_close_drawdown_rs']
    for period in ('all', 'october', 'development', 'validation', 'assessment', 'known_oct26', 'final_oct27'):
        html += '<h2>' + period + '</h2>' + show(table.loc[table.period.eq(period), cols])
    html += '<h2>Development selection</h2><p>Require net profit at least baseline, at least5% less losing-trade amount, fewer losses, no lower win rate or larger daily-close drawdown,95% winner retention,80% trade retention and5 changed trades across3 days. More absolute wins are reported separately; fewer traded winners do not count as an increase in wins.</p>' + show(audit)
    html += '<h2>Development-only reversal diagnosis</h2><p>Fast adverse proxy: observable completed post-entry close or executed exit at -0.3% within5 minutes; entry/exit candle ranges cannot establish reversal ordering. This diagnosis does not choose thresholds or labels for entry logic.</p>' + show(diagnostics)
    delta = table.loc[table.period.eq('all') & table.policy.ne('g7_v4'), ['policy', 'avoided_baseline_losers', 'missed_baseline_winners', 'loser_to_winner', 'winner_harmed', 'new_allocations']]
    html += '<h2>Losers avoided and winners sacrificed</h2>' + show(delta)
    html += '<h2>October daily results: every iteration</h2>' + show(daily.loc[daily.index.str.startswith('2026-10')].reset_index())
    html += '<h2>What the evidence means</h2><p>59 available sessions from29 July to27 October2026.35 development,14 validation,8 assessment, then26/27 October diagnostics. All history was inspected in earlier research, so later partitions are not fresh out-of-sample evidence. The ten candidates are disclosed, and no significance or future-profit claim is made. Existing V4 stays selected regardless of retrospective selection; any proposed default change is a separate decision.</p><p>The inherited universe uses after-close stock-day eligibility, and setup targets were previously inspected/tuned. These limitations prevent treating this exercise as a validated live return estimate. Missing source sessions24/25 August and1 October are excluded. Fixed Rs500,000 theoretical exposure, Rs1lakh margin, Rs10lakh portfolio;5bps base cost and15/30bps stresses. Drawdown uses daily-close realized equity.</p>'
    html += '<h2>Files</h2><ul>' + ''.join(f'<li><a href="{n}">{n}</a></li>' for n in ('comparison.csv', 'october_daywise_all_candidates.csv', 'october_daywise_selected_comparison.csv', 'all_period_totals.csv', 'monthly_totals.csv', 'development_selection.csv', 'trade_changes.csv', 'cost_sensitivity.csv', 'entry_features_and_decisions.csv', 'development_reversal_diagnostics.csv', 'protocol.json', 'selection_lock.json', 'summary.json')) + '</ul></body></html>'
    (out / 'report.html').write_text(html, encoding='utf-8')


def run():
    protected = source_state()
    settings = base.read(selected.CONFIG)
    source_pointer = base.read(selected.OUT_ROOT / 'latest.json')
    source_output = Path(source_pointer['output'])
    verify_members(source_output, source_pointer['manifest_sha256'])
    selected.verify(source_output)
    out = OUT_ROOT / 'runs' / base.FINAL_DAY / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f') + '_two_stages_fixed')
    out.mkdir(parents=True, exist_ok=False)
    protocol = dict(version=VERSION, scope='BACKTEST_ONLY', execution_authority=False,
        baseline_output=str(source_output), baseline_manifest_sha256=sha(source_output / 'manifest.json'),
        baseline_config_sha256=sha(selected.CONFIG), round1=ROUND1, round2=ROUND2, candidate_count=10,
        selection=ACCEPTANCE, protection=settings['exit_policy'], independent_holdout=False,
        no_future_pivots=True, missing_warmup='preserve original entry; report coverage',
        stop_rule='No additional candidates/thresholds/combinations after seeing outcomes; no later-period replacement.')
    dump(out / 'protocol.json', protocol)
    digest = sha(out / 'protocol.json')
    print('LOCKED PROTOCOL', out / 'protocol.json', flush=True)
    cache = Path(settings['default_cache'])
    native, _, original, paths, provenance = selected.verify_sources(settings, cache)
    f = build_features(original, native)
    flags = decisions(f)
    for name, mask in flags.items(): f[name] = mask
    f.to_csv(out / 'entry_features_and_decisions.csv', index=False)
    cfg = provenance['base_config']
    periods = dict(provenance['periods'], october=[d for d in provenance['periods']['all'] if d.startswith('2026-10')],
                   later_blocks=provenance['periods']['validation'] + provenance['periods']['assessment'])
    books, training = {}, []
    for name in ['g7_v4', *ROUND1, *ROUND2]:
        book = make_book(native, paths, f, settings, cfg, name)
        book['indicator_protocol_sha256'] = digest
        books[name] = book
        if name == 'g7_v4': same_book(book, pd.read_csv(source_output / 'portfolio_trades.csv'))
        stats = compare(book, books['g7_v4'], periods['development'])
        training.append(dict(policy=name, period='development', **stats))
        print('STAGE', 0 if name == 'g7_v4' else 1 if name in ROUND1 else 2, name,
              'DEVELOPMENT net', round(stats['net_pnl_rs'], 2), 'losses', stats['losses'], 'loss amount', round(stats['negative_trade_sum_abs_rs'], 2), flush=True)
    chosen, audit = choose(pd.DataFrame(training), list(POLICIES))
    dump(out / 'selection_lock.json', dict(selected_candidate=chosen, uses='DEVELOPMENT_ONLY',
        locked_at_utc=datetime.now(timezone.utc).isoformat(), protocol_sha256=digest, later_replacement_forbidden=True))
    table = pd.DataFrame([dict(policy=name, period=p, **compare(book, books['g7_v4'], days))
        for name, book in books.items() for p, days in periods.items()])
    valid = choose(table, [chosen], 'validation')[0] if chosen else None
    assessed = choose(table, [chosen], 'assessment')[0] if chosen else None
    descriptive = [r for p in periods for r in choose(table, list(POLICIES), p)[1]]
    daily, costs, trade_deltas, monthly = [], [], [], []
    allowed = _flags(f.allowed_g7_v2, 'nifty')
    for name, book in books.items():
        folder = out / name
        folder.mkdir()
        book.to_csv(folder / 'portfolio_trades.csv', index=False)
        for day in periods['all']: daily.append(dict(policy=name, day=day, **metrics(book, [day])))
        trade_deltas.append(changes.differences(book, books['g7_v4']).assign(policy=name))
        months = sorted({d[:7] for d in periods['all']})
        for month in months:
            monthly.append(dict(policy=name, month=month, **compare(book, books['g7_v4'], [d for d in periods['all'] if d.startswith(month)])))
        for cost in (5, 15, 30):
            mask = _flags(book.set_index('sid').g8_direction_allowed, 'allowed')
            stress = allocate_variant(book, mask, dict(cfg, max_positions_per_symbol=1), name, cost_bps=cost)
            for p, days in periods.items(): costs.append(dict(policy=name, period=p, cost_bps=cost, **metrics(stress, days)))
    daily, costs = pd.DataFrame(daily), pd.DataFrame(costs)
    wide = daily.pivot(index='day', columns='policy', values='net_pnl_rs').reindex(columns=books)
    cost_ok = False
    if chosen:
        ct = costs.loc[costs.period.eq('later_blocks')].set_index(['policy', 'cost_bps'])
        cost_ok = all(ct.loc[(chosen, c)].net_pnl_rs >= ct.loc[('g7_v4', c)].net_pnl_rs - 1e-6 for c in (15, 30))
    diagnosis = reversal_audit(native, paths, books['g7_v4'], f)
    development = diagnosis.loc[diagnosis.day.isin(periods['development'])]
    diagnostics = []
    for name in POLICIES:
        for state in (False, True):
            group = development.loc[development['flag_' + name].eq(state)]
            diagnostics.append(dict(indicator=name, flagged=state, trades=len(group), losses=int(group.net_loser.sum()),
                fast_adverse=int(group.fast_adverse.sum()), net_pnl_rs=float(group.net_pnl_rs.sum()),
                loss_rate_pct=100 * group.net_loser.mean() if len(group) else np.nan,
                fast_adverse_pct=100 * group.fast_adverse.mean() if len(group) else np.nan))
    diagnostics = pd.DataFrame(diagnostics)
    diagnosis.to_csv(out / 'reversal_audit.csv')
    diagnostics.to_csv(out / 'development_reversal_diagnostics.csv', index=False)
    table.to_csv(out / 'comparison.csv', index=False)
    table.loc[table.period.eq('all')].to_csv(out / 'all_period_totals.csv', index=False)
    pd.DataFrame(monthly).to_csv(out / 'monthly_totals.csv', index=False)
    daily.to_csv(out / 'daywise_details.csv', index=False)
    wide.to_csv(out / 'daywise_all_candidates.csv')
    wide.loc[periods['october']].to_csv(out / 'october_daywise_all_candidates.csv')
    final_policy = chosen or 'g7_v4'
    compact = wide.loc[periods['october'], ['g7_v4']].rename(columns={'g7_v4': 'baseline_g7_v4_net_rs'})
    compact['selected_policy'] = final_policy
    compact['selected_net_rs'] = wide.loc[periods['october'], final_policy]
    compact['net_change_rs'] = compact.selected_net_rs - compact.baseline_g7_v4_net_rs
    compact.to_csv(out / 'october_daywise_selected_comparison.csv')
    pd.DataFrame(audit).to_csv(out / 'development_selection.csv', index=False)
    pd.DataFrame(descriptive).to_csv(out / 'period_qualification.csv', index=False)
    pd.concat(trade_deltas, ignore_index=True).to_csv(out / 'trade_changes.csv', index=False)
    costs.to_csv(out / 'cost_sensitivity.csv', index=False)
    summary = dict(version=VERSION, cache=str(cache), cache_manifest_sha256=sha(cache / 'manifest.json'),
        baseline_output=str(source_output), baseline_manifest_sha256=sha(source_output / 'manifest.json'),
        protocol_sha256=digest, selected_config=settings, periods=periods, candidate_count=10,
        selected_candidate=chosen, validation_qualified=chosen is not None and valid == chosen,
        assessment_qualified=chosen is not None and assessed == chosen, cost_qualified=bool(cost_ok),
        retrospective_qualified=bool(chosen and valid == chosen and assessed == chosen and cost_ok),
        default_changed=False, execution_authority=False, independent_holdout=False,
        indicator_ready=int(_flags(f.indicator_ready, 'ready').sum()), protected_files_unchanged=protected)
    dump(out / 'summary.json', summary)
    report(out, summary, table, wide, pd.DataFrame(audit), diagnostics)
    if source_state() != protected or sha(out / 'protocol.json') != digest:
        raise ValueError('Fixed protocol or selected default changed')
    code = [Path(__file__), Path(selected.__file__), selected.CONFIG, Path(features_v4.__file__), Path(changes.__file__), Path(base.__file__),
        ROOT / 'research/g3_sl_target_simulator.py', ROOT / 'research/g8_exit_engine.py',
        ROOT / 'research/g4_vwap_rsi_features.py', ROOT / 'fno_v13_corrected_v5_backtest.py',
        ROOT / 'tests/test_g7_v4_indicator_iteration.py']
    base.seal(out, code={str(p.relative_to(ROOT)): sha(p) for p in code})
    verify(out)
    dump(OUT_ROOT / 'latest.json', dict(output=str(out), manifest_sha256=sha(out / 'manifest.json'), execution_authority=False))
    print(table.loc[table.period.isin(['all', 'october']), ['policy', 'period', 'trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs']].to_string(index=False), flush=True)
    print('SELECTED', chosen, 'VALIDATION', summary['validation_qualified'], 'ASSESSMENT', summary['assessment_qualified'], 'OUTPUT', out, flush=True)
    return out


def verify(out):
    out = Path(out)
    verify_members(out)
    summary = base.read(out / 'summary.json')
    for name, digest in base.read(out / 'manifest.json')['code'].items():
        if sha(ROOT / name) != digest: raise ValueError('Code changed: ' + name)
    if sha(out / 'protocol.json') != summary['protocol_sha256']: raise ValueError('Protocol changed')
    verify_members(summary['baseline_output'], summary['baseline_manifest_sha256'])
    cache = Path(summary['cache'])
    verify_members(cache, summary['cache_manifest_sha256'])
    native, _, _, paths, provenance = base.load_cache(cache)
    f = pd.read_csv(out / 'entry_features_and_decisions.csv').set_index('sid', drop=False)
    fresh_flags = decisions(f)
    for name, flags in fresh_flags.items():
        if not flags.equals(_flags(f[name], name)): raise ValueError('Saved entry decision changed')
    books = {}
    for name in ['g7_v4', *POLICIES]:
        actual = make_book(native, paths, f, summary['selected_config'], provenance['base_config'], name)
        saved = pd.read_csv(out / name / 'portfolio_trades.csv')
        same_book(actual, saved)
        books[name] = actual
    same_book(books['g7_v4'], pd.read_csv(Path(summary['baseline_output']) / 'portfolio_trades.csv'))
    table = pd.read_csv(out / 'comparison.csv')
    for row in table.itertuples():
        stats = compare(books[row.policy], books['g7_v4'], summary['periods'][row.period])
        for key in ('trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs', 'changed_executions', 'changed_days', 'missed_baseline_winners', 'avoided_baseline_losers'):
            if not np.isclose(stats[key], getattr(row, key), rtol=0, atol=1e-6): raise ValueError('Accounting differs: ' + row.policy + '/' + key)
    chosen, _ = choose(table, list(POLICIES))
    if chosen != summary['selected_candidate'] or chosen != base.read(out / 'selection_lock.json')['selected_candidate']:
        raise ValueError('Development selection changed')
    costs = pd.read_csv(out / 'cost_sensitivity.csv')
    cfg = provenance['base_config']
    exposure = cfg['capital_per_entry_rs'] * cfg['leverage']
    np.testing.assert_allclose(costs.cost_rs, costs.trades * exposure * costs.cost_bps / 10000, rtol=0, atol=1e-6)
    np.testing.assert_allclose(costs.net_pnl_rs, costs.gross_pnl_rs - costs.cost_rs, rtol=0, atol=1e-6)
    indexed_costs = costs.set_index(['policy', 'cost_bps', 'period'])
    for name, book in books.items():
        mask = _flags(book.set_index('sid').g8_direction_allowed, 'allowed')
        for cost in (5, 15, 30):
            stress = allocate_variant(book, mask, dict(cfg, max_positions_per_symbol=1), name, cost_bps=cost)
            for p, days in summary['periods'].items():
                stats = metrics(stress, days)
                for key in ('trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs'):
                    if not np.isclose(stats[key], indexed_costs.loc[(name, cost, p), key], rtol=0, atol=1e-6):
                        raise ValueError('Reallocated cost metrics differ: ' + name + '/' + key)
    valid = choose(table, [chosen], 'validation')[0] if chosen else None
    assessed = choose(table, [chosen], 'assessment')[0] if chosen else None
    cost_ok = False
    if chosen:
        ct = costs.loc[costs.period.eq('later_blocks')].set_index(['policy', 'cost_bps'])
        cost_ok = all(ct.loc[(chosen, c)].net_pnl_rs >= ct.loc[('g7_v4', c)].net_pnl_rs - 1e-6 for c in (15, 30))
    expected = dict(validation_qualified=chosen is not None and valid == chosen,
        assessment_qualified=chosen is not None and assessed == chosen, cost_qualified=bool(cost_ok),
        retrospective_qualified=bool(chosen and valid == chosen and assessed == chosen and cost_ok))
    for key, value in expected.items():
        if summary[key] != value: raise ValueError('Qualification differs: ' + key)
    wide = pd.read_csv(out / 'daywise_all_candidates.csv').set_index('day')
    for name, book in books.items():
        for day in summary['periods']['all']:
            if not np.isclose(wide.loc[day, name], metrics(book, [day])['net_pnl_rs'], rtol=0, atol=1e-6): raise ValueError('Daily accounting differs')
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path)
    args = parser.parse_args()
    print('VERIFIED', verify(args.verify)) if args.verify else run()
