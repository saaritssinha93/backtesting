"""Reconcile frozen G4 entry-guard experiments and produce a daywise plan.

Run: python -m research.g4_entry_guard_daywise_report
This reads historical ledgers only and never changes strategy configuration.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_bounded_exit_backtest as bounded
from research import g4_g3_mirror_backtest as base


ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/research')
SLOTS = ROOT / '20261016T132548_entry_veto_slot_diagnostics'
GLOBAL = ROOT / '20261016T131914_entry_veto_v1'
FOUR = 'four_observed_slot_rules'
STOPS = ['STOP', 'TIGHTENED_STOP']
NAMES = {
    'current_g4': 'Current G4',
    '0930_short_stretch3': '09:30 SHORT: 3 ATR',
    '1140_short_stretch3': '11:40 SHORT: 3 ATR',
    '1230_long_stretch3': '12:30 LONG: 3 ATR',
    '1320_short_pending': '13:20 SHORT: pending invalidation',
    FOUR: 'Four selective rules',
    'stretch_3atr': 'All setups: 3 ATR',
    'stretch_4atr': 'All setups: 4 ATR',
    'pending_invalidation': 'All setups: pending invalidation',
    'stretch_3atr_and_pending': 'All setups: 3 ATR + pending',
    'stretch_4atr_and_pending': 'All setups: 4 ATR + pending',
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(folder):
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    for name, expected in manifest['files'].items():
        if digest(folder / name) != expected:
            raise ValueError(f'Changed source artifact: {folder / name}')
    return manifest


def executed(frame):
    return frame.loc[frame.portfolio_executed.eq(True)].set_index('sid', drop=False)


def day_label(value):
    return 'Profit' if value > 1e-8 else 'Loss' if value < -1e-8 else 'Flat'


def delta_rows(before, after):
    b, a = executed(before), executed(after)
    rows = []
    for sid in sorted(set(b.index) | set(a.index)):
        old, new = b.loc[sid] if sid in b.index else None, a.loc[sid] if sid in a.index else None
        row = old if old is not None else new
        bp = float(old.portfolio_net_profit_rupees) if old is not None else 0.
        ap = float(new.portfolio_net_profit_rupees) if new is not None else 0.
        if old is None or new is None or not np.isclose(bp, ap, atol=1e-7, rtol=0):
            rows.append(dict(sid=int(sid), day=row.day, setup_id=row.setup_id,
                symbol=row.tradingsymbol, side=row.side,
                change='ADDED' if old is None else 'REMOVED' if new is None else 'RESIZED',
                exit_reason=row.exit_reason, before_net_rs=bp, after_net_rs=ap,
                net_change_rs=ap-bp))
    return pd.DataFrame(rows)


def compact_day(before, after, day):
    b, a = bounded.stats(before, [day]), bounded.stats(after, [day])
    return dict(day=day, before_net_rs=b['net_pnl_rs'], after_net_rs=a['net_pnl_rs'],
        net_change_rs=a['net_pnl_rs']-b['net_pnl_rs'],
        before_trades=b['trades'], after_trades=a['trades'],
        before_wins=b['wins'], after_wins=a['wins'],
        before_losses=b['losses'], after_losses=a['losses'],
        before_stops=b['stop_exits'], after_stops=a['stop_exits'],
        before_day=day_label(b['net_pnl_rs']), after_day=day_label(a['net_pnl_rs']))


def table(frame, labels=None):
    work = frame.copy()
    if 'scenario' in work:
        work['scenario'] = work.scenario.map(NAMES)
    if labels:
        work = work.rename(columns=labels)
    return '<div class="scroll">' + work.to_html(index=False, border=0,
        na_rep='—', float_format=lambda value: f'{value:,.2f}') + '</div>'


def run():
    verify(SLOTS)
    verify(GLOBAL)
    slotmeta = json.loads((SLOTS/'summary.json').read_text(encoding='utf-8'))
    globalmeta = json.loads((GLOBAL/'summary.json').read_text(encoding='utf-8'))
    baseline = Path(globalmeta['source_run'])
    if digest(GLOBAL/'manifest.json') != slotmeta['source_guard_manifest_sha256']:
        raise ValueError('Guard source identity changed')
    if digest(baseline/'manifest.json') != globalmeta['source_manifest_sha256']:
        raise ValueError('Baseline source identity changed')
    verify(baseline)
    days = base.read_frozen_csv(baseline/'g4_bounded_exit/daily_results.csv').day.tolist()
    books = {name: base.read_frozen_csv(SLOTS/(name+'_portfolio.csv'))
             for name in list(NAMES)[:6]}
    for name in list(NAMES)[6:]:
        books[name] = base.read_frozen_csv(GLOBAL/name/'portfolio_trades.csv')
    for book in books.values():
        assert book.sid.is_unique
    before, after = books['current_g4'], books[FOUR]
    changes = delta_rows(before, after)
    daily = pd.DataFrame([compact_day(before, after, day) for day in days])
    audit = base.read_frozen_csv(GLOBAL/'guard_audit.csv').set_index('sid')
    changes = changes.merge(audit[['entry_veto_extension_atr', 'pending_cancel_ts',
                                  'pending_cancel_close']], on='sid', validate='one_to_one')
    changes = changes.sort_values(['day', 'setup_id', 'symbol']).reset_index(drop=True)
    full = pd.DataFrame([dict(scenario=name, **bounded.stats(book, days))
                         for name, book in books.items()])
    full['net_change_rs'] = full.net_pnl_rs-full.iloc[0].net_pnl_rs
    reported = base.read_frozen_csv(SLOTS/'comparison.csv')
    for record in reported.loc[reported.period.eq('all')].to_dict('records'):
        actual = full.loc[full.scenario.eq(record['scenario'])].iloc[0]
        for key in ('trades','wins','losses','stop_exits','profitable_days','losing_days',
                    'net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs'):
            assert np.isclose(actual[key], record[key], rtol=0, atol=1e-6), (key, actual[key], record[key])
    assert np.isclose(daily.net_change_rs.sum(), changes.net_change_rs.sum(), rtol=0, atol=1e-6)
    assert np.isclose(daily.after_net_rs.sum(), full.loc[full.scenario.eq(FOUR)].iloc[0].net_pnl_rs, rtol=0, atol=1e-6)
    losses = daily.loc[daily.before_day.eq('Loss') | daily.after_day.eq('Loss')]
    removed, added = changes.loc[changes.change.eq('REMOVED')], changes.loc[changes.change.eq('ADDED')]
    effects = {key: value for key, value in dict(
        days_improved=int(daily.net_change_rs.gt(1e-8).sum()),
        days_worsened=int(daily.net_change_rs.lt(-1e-8).sum()),
        days_unchanged=int(daily.net_change_rs.abs().le(1e-8).sum()),
        loss_to_profit=daily.loc[daily.before_day.eq('Loss') & daily.after_day.eq('Profit'), 'day'].tolist(),
        profit_to_loss=daily.loc[daily.before_day.eq('Profit') & daily.after_day.eq('Loss'), 'day'].tolist(),
        removed_trades=len(removed), added_trades=len(added),
        removed_winners=int(removed.before_net_rs.gt(0).sum()),
        removed_losers=int(removed.before_net_rs.lt(0).sum()),
        removed_stops=int(removed.exit_reason.isin(STOPS).sum()),
        added_stops=int(added.exit_reason.isin(STOPS).sum()),
        removed_exit_counts=removed.exit_reason.value_counts().to_dict(),
        added_exit_counts=added.exit_reason.value_counts().to_dict(),
        removed_winner_pnl_rs=float(removed.loc[removed.before_net_rs.gt(0), 'before_net_rs'].sum()),
        removed_loss_abs_rs=float(-removed.loc[removed.before_net_rs.lt(0), 'before_net_rs'].sum()),
        added_pnl_rs=float(added.after_net_rs.sum()),
        improvements_rs=float(daily.loc[daily.net_change_rs.gt(0), 'net_change_rs'].sum()),
        deteriorations_rs=float(daily.loc[daily.net_change_rs.lt(0), 'net_change_rs'].sum()),
    ).items()}
    keydays = pd.DataFrame([dict(scenario=name, **compact_day(before, book, day))
                           for day in ['2026-10-14','2026-10-15'] for name,book in books.items()])
    details = []
    for day in ['2026-10-14','2026-10-15']:
        existing = before.loc[before.day.eq(day)]
        surviving = after.set_index('sid')
        for row in existing.to_dict('records'):
            sid = row['sid']
            new = surviving.loc[sid] if sid in surviving.index else None
            feature = audit.loc[sid]
            details.append(dict(day=day, sid=sid, setup_id=row['setup_id'],
                symbol=row['tradingsymbol'], side=row['side'],
                confirmation_time=row['confirmation_ts'], entry_time=row['entry_ts'], exit_time=row['exit_ts'],
                exit_reason=row['exit_reason'], before_executed=row['portfolio_executed'],
                after_executed=False if new is None else bool(new.portfolio_executed),
                before_net_rs=row['portfolio_net_profit_rupees'],
                after_net_rs=0. if new is None else new.portfolio_net_profit_rupees,
                rule_action='VETO' if new is None else 'UNCHANGED',
                extension_atr=feature.entry_veto_extension_atr,
                pending_cancel_time=feature.pending_cancel_ts,
                pending_cancel_close=feature.pending_cancel_close))
    details = pd.DataFrame(details).sort_values(['day','confirmation_time','side']).reset_index(drop=True)
    output = ROOT/datetime.now().strftime('%Y%m%dT%H%M%S_entry_guard_daywise_plan')
    output.mkdir()
    for name, frame in [('daily_comparison',daily),('trade_changes',changes),
                        ('full_comparison',full),('oct14_oct15_comparison',keydays),
                        ('oct14_oct15_orders',details),('losing_day_transitions',losses)]:
        frame.to_csv(output/(name+'.csv'), index=False)
    summary = dict(first_date=days[0], last_date=days[-1], sessions=len(days),
        scope='BACKTEST_ONLY_PLAN', default_configuration_changed=False,
        comparison='Current bounded-exit G4 versus four post-hoc selective entry rules',
        sources={str(path):digest(path/'manifest.json') for path in (SLOTS, GLOBAL, baseline)},
        effects=effects, notes=[
            'October 15 motivated the rules and is not a holdout.',
            'These results end October 15, not October 16.',
            'No stock substitution after a rejected entry. Portfolio allocation is recalculated.',
            'Stops avoided here means entries prevented that would have stopped; no open-position exit rules change.',
            'One-minute historical bars do not measure exchange cancellation latency or fill/cancel races.',
            'Fixed 5bps roundtrip costs, Rs500000 exposure per full trade, Rs1000000 portfolio capital.'])
    (output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
    build_html(output, full, daily, changes, losses, keydays, details, effects, reported)
    proof = dict(code_sha256={str(Path(__file__).resolve()):digest(Path(__file__))},
                 files={p.name:digest(p) for p in output.iterdir() if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf-8')
    verify(output)
    print('OUTPUT', output)
    print('EFFECTS', json.dumps(effects,indent=2))
    print('CHANGED DAYS\n',daily.loc[daily.net_change_rs.abs().gt(1e-8)].to_string(index=False))
    print('LOSING DAYS\n',losses.to_string(index=False))
    print('KEY DAYS\n', keydays[['scenario','day','before_net_rs','after_net_rs','net_change_rs','after_trades','after_stops']].to_string(index=False))
    print('ORDERS\n',details[['day','setup_id','symbol','exit_reason','before_net_rs','after_net_rs','rule_action','extension_atr','pending_cancel_time']].to_string(index=False))
    print('TRADE CHANGES\n',changes.to_string(index=False))


def build_html(output, full, daily, changes, losses, keydays, details, effects, reported):
    labels = dict(scenario='Experiment', day='Date', before_net_rs='Current net Rs',
        after_net_rs='Selective net Rs', net_change_rs='Change Rs',
        before_trades='Current trades', after_trades='Selective trades',
        before_stops='Current stops', after_stops='Selective stops',
        before_wins='Current wins', after_wins='Selective wins',
        before_losses='Current losses', after_losses='Selective losses',
        before_day='Current day', after_day='Selective day', net_pnl_rs='Net Rs',
        stop_exits='Stop exits', losing_days='Losing days', profitable_days='Profitable days',
        win_rate_pct='Trade win %', negative_trade_sum_abs_rs='Losing-trade total Rs',
        daily_close_drawdown_rs='Daily-close drawdown Rs')
    rules = pd.DataFrame([
        ['09:30','09:31','SHORT','Reject if (EMA20 − close) / ATR14 > 3'],
        ['11:40','11:41','SHORT','Reject if (EMA20 − close) / ATR14 > 3'],
        ['12:30','12:31','LONG','Reject if (close − EMA20) / ATR14 > 3'],
        ['13:20','13:21','SHORT','Cancel unfilled entry after a completed 1m close above confirmation high'],
    ],columns=['5m setup','1m confirmation','Side','Additional research rule'])
    show = ['scenario','trades','wins','losses','win_rate_pct','stop_exits','profitable_days',
            'losing_days','net_pnl_rs','net_change_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']
    parts = ['''<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 entry guards: daywise evidence and plan</title>
<style>:root{color-scheme:dark}body{background:#101825;color:#edf3fb;font:15px/1.6 system-ui,sans-serif;max-width:1500px;margin:auto;padding:28px}h1,h2,h3{color:#fff}h2{margin-top:36px}a{color:#94d5ff}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:9px 11px;border-bottom:1px solid #415269;text-align:right;white-space:nowrap}th{background:#25374d;color:#fff;position:sticky;top:0}td:first-child,th:first-child{text-align:left}.scroll{overflow:auto;max-height:720px;border:1px solid #415269;border-radius:6px}.note{background:#223044;padding:16px 20px;border-left:4px solid #eabd73}.links{display:flex;gap:18px;flex-wrap:wrap}li{margin:7px 0}code{color:#c1e4ff}@media print{body{background:white;color:#111}h1,h2,h3{color:#111}.scroll{max-height:none;overflow:visible}td,th{white-space:normal;font-size:9px}th{position:static}.note{background:#eee}}</style></head><body>
<h1>G4 entry guards: daywise evidence and plan</h1>
<p>29 July–15 October 2026, 52 sessions. Baseline: G4 with targets bounded to 0.60–2.00%, initial SL 1.20%, tightening to 1.00% after 120 minutes.</p>
<p class="note"><strong>The selective rules turn 15 October profitable and reduce stop exits, but lower total historical profit.</strong> The four slots were chosen after inspecting 15 October losses. This is a diagnostic comparison, not independent proof of improvement. No active strategy or live configuration is changed by this report.</p>
<p class="links"><a href="daily_comparison.csv">All 52 days CSV</a><a href="trade_changes.csv">Every changed execution CSV</a><a href="oct14_oct15_orders.csv">14–15 October orders CSV</a><a href="full_comparison.csv">All experiments CSV</a></p>
<h2>The four selective changes being evaluated</h2>''', table(rules),
        '''<p>The extension test uses the completed one-minute confirmation close, EMA20 and ATR14 (simple mean of true range). It measures directional distance from the EMA, not distance from a known future peak. Exactly 3 ATR passes. The pending check watches only the existing 10-minute entry window. A trigger fill within a minute takes priority over cancellation inferred from that minute’s close. A wick alone does not cancel the order.</p>
<p>These checks run after existing ranking. A veto skips the selected stock with no replacement. The existing five-minute selection, one-minute filters, target, stop, entry size and 15:15 exit remain the basis of this experiment. Portfolio allocations are recalculated after vetoes.</p>
<h2>Full-period comparison</h2>''', table(full[show],labels),
        '<h2>What is actually avoided with the four selective rules</h2><ul>',
        f'<li>{effects["removed_trades"]} baseline executions removed: {effects["removed_losers"]} losers and {effects["removed_winners"]} winners. {effects["added_trades"]} other execution becomes possible after capital is freed.</li>',
        f'<li>{effects["removed_stops"]} stop exits removed and {effects["added_stops"]} new stop exits added. Exit types removed: {html.escape(str(effects["removed_exit_counts"]))}.</li>',
        f'<li>Losses avoided: Rs {effects["removed_loss_abs_rs"]:,.2f}. Profits sacrificed: Rs {effects["removed_winner_pnl_rs"]:,.2f}. Added execution P&amp;L: Rs {effects["added_pnl_rs"]:,.2f}.</li>',
        f'<li>{effects["days_improved"]} days improve, {effects["days_worsened"]} worsen, and {effects["days_unchanged"]} have unchanged P&amp;L. Losing to profitable: {html.escape(str(effects["loss_to_profit"]))}. Profitable to losing: {html.escape(str(effects["profit_to_loss"]))}.</li></ul>',
        '<p>“Avoided stop exits” means preventing entries that would later stop out. It does not mean changing the stop or rescuing a position after entry.</p>',
        '<h2>All losing days and their transitions</h2>', table(losses,labels),
        '<h2>Each rule separately: earlier history versus 15 October</h2>',
        table(reported[['scenario','period','trades','stop_exits','losing_days','net_pnl_rs','net_change_rs','removed_winners','removed_losers']],labels),
        '<p>Individual effects need not add exactly because allocation can change. The 13:20 pending rule avoids just one previously executed trade, KEI on 15 October. It has no earlier executed-trade evidence of improvement.</p>',
        '<h2>14 and 15 October: every alternative</h2>',
        table(keydays[['day','scenario','before_net_rs','after_net_rs','net_change_rs','after_trades','after_losses','after_stops']],
              dict(labels,after_net_rs='Experiment net Rs',after_trades='Experiment trades',
                   after_losses='Experiment losses',after_stops='Experiment stops')),
        '<h2>14 and 15 October: order-by-order selective rule result</h2>',
        table(details[['day','setup_id','symbol','side','exit_reason','before_net_rs','after_net_rs','rule_action','extension_atr','pending_cancel_time']],labels),
        '<p>Setup IDs use the one-minute confirmation clock: for example 0931_SHORT is the 09:30 five-minute setup. An UNFILLED order contributes zero P&amp;L and is not counted as a saved losing trade. A pending flag shown for an unrelated setup is diagnostic only; the selective pending rule applies solely to 1321_SHORT.</p>',
        '<h2>Daywise results: all 52 sessions</h2>', table(daily,labels),
        '<h2>Every changed execution</h2>',table(changes,labels),
        '''<h2>Proposed implementation and validation plan</h2>
<ol>
<li><strong>Keep the current G4 baseline as the comparison.</strong> Do not enable a blanket 3 ATR filter. Its lost winners and lost P&amp;L outweigh the stop-count improvement in this history.</li>
<li><strong>Expose optional per-setup settings.</strong> Proposed fields: guard mode OFF / ADVISORY / BLOCK, maximum directional extension, pending invalidation enabled, and pending-entry expiry. The four rows above define the tested candidate. Keep the 13:20 pending rule separately selectable.</li>
<li><strong>Log decisions from completed candles.</strong> Record symbol, setup, side, confirmation timestamp, EMA, ATR, extension, opposite candle boundary, cancellation timestamp, broker order status and rule reason. Missing or stale candles must be reported rather than assigned an invented indicator value.</li>
<li><strong>Run fresh paper observation first.</strong> Freeze thresholds for a proposed minimum of 20 new completed trading sessions, extending the observation if too few flagged entries occur. This is an operational observation window, not statistical proof. Track avoided loss, rejected winners, net P&amp;L after realistic costs, stop frequency per executed trade, profitable-day rate and drawdown. Compare the pending rule separately from the three stretch rules. Do not retune against each new losing day.</li>
<li><strong>Make cancellation compatible with real execution.</strong> Before any live blocking, integrate broker order updates, partial fills, cancellation confirmation and fill/cancel races. Cancel only the unfilled remainder. Preserve stop protection on any filled quantity. A dashboard refreshing every 15 seconds must not be treated as a 15-second candle or as guaranteed immediate cancellation. Kite documents pending-order cancellation and asynchronous order updates: <a href="https://kite.trade/docs/connect/v3/orders/#cancelling-orders">orders</a> and <a href="https://kite.trade/docs/connect/v3/postbacks/">postbacks</a>.</li>
<li><strong>Decide using a declared objective.</strong> For profit improvement, require positive net P&amp;L change on new data after costs and slippage, with no deterioration in losing-day count or daily drawdown. If accepting lower profit for lower drawdown, agree that trade-off explicitly before evaluating it. Retain the original baseline if the new evidence does not support a change.</li>
</ol>
<h2>Code and configuration work needed before promotion</h2>
<table><thead><tr><th>Area</th><th>Current state</th><th>Proposed work</th></tr></thead><tbody>
<tr><td>Candidate configuration</td><td>Global experiments are in v13_v10_g_4_entry_veto_research_v1.json; selective rules are declared in the research script</td><td>Create a separately versioned candidate with the four explicit setup IDs and independent guard switches</td></tr>
<tr><td>Shared rule calculation</td><td>research/g4_entry_veto_rules.py implements causal stretch and pending checks</td><td>Reuse the same formulas and timestamp conventions for historical and paper evaluation</td></tr>
<tr><td>Normal G4 backtest runner</td><td>fno_v13_v10_g_4_backtest.py currently defaults to the bounded-exit profile</td><td>Add an explicit optional candidate profile; verify no candidate effect when disabled</td></tr>
<tr><td>Dashboard report</td><td>Research results have a separate HTML report</td><td>Show the exact profile, date coverage, guard reasons, rejected stocks and baseline comparison</td></tr>
<tr><td>Paper and live entry handling</td><td>These experiments do not implement broker cancellation</td><td>Build and test order-state handling separately before enabling any live cancellation</td></tr>
<tr><td>Validation</td><td>Frozen backtest accounting reconciles</td><td>Check enabled/disabled parity, long/short symmetry, boundary equality, same-minute fills, partial fills, stale data and fill/cancel races</td></tr>
</tbody></table>
<h2>Evidence limits and accounting</h2>
<p>These are G4 backtest comparisons, not a new G3 comparison or observed live results. The data ends 15 October. October 15 motivated the rule selection and is not a holdout; earlier dates were also used in strategy development. Simulated exposure is Rs500,000 per full trade, with Rs100,000 capital per position and Rs1,000,000 total portfolio capital. Costs remain 5bps roundtrip (Rs250 per full trade). These costs are modeled, not an itemized Zerodha contract-note reconciliation. One-minute OHLC cannot reproduce spread, queue position or cancellation latency. Stop-first handling of ambiguous stop/target bars and the baseline fill rules are preserved.</p>
<p>Frozen input manifests were verified. Trade-level changes reconcile to daywise and full-period P&amp;L. Source manifest hashes and output hashes are saved beside this report. Research calculations do not change live orders, strategy defaults or dashboard pointers.</p>
</body></html>''']
    (output/'report.html').write_text(''.join(parts),encoding='utf-8')


if __name__ == '__main__':
    run()
