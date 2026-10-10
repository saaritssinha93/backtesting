"""Assemble isolated G2 research evidence into a linked HTML report.

Never edits inputs, strategies, trading status, or baseline outputs.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def js(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def frame(path):
    if not Path(path).exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def choose(root, *names):
    for name in names:
        if (root / name).is_file():
            return root / name
    return root / names[0]


def subset(df, cols):
    return df[[c for c in cols if c in df]].copy()


def val(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 'Unavailable'
    if isinstance(v, float):
        return f'{v:,.4f}'
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def display_timing(df):
    out = df.copy()
    for kind in ['peak','trough']:
        values = []
        for _, row in out.iterrows():
            if pd.notna(row.get(kind+'_time')):
                values.append(pd.Timestamp(row[kind+'_time']).strftime('%H:%M')+' bar end')
            elif pd.notna(row.get(kind+'_time_interval_start')) and pd.notna(row.get(kind+'_time_interval_end')):
                a = pd.Timestamp(row[kind+'_time_interval_start']).strftime('%H:%M')
                b = pd.Timestamp(row[kind+'_time_interval_end']).strftime('%H:%M')
                values.append(a+'–'+b+' inferred auction window; exact time unknown')
            else:
                values.append('Unknown in stored intraday data')
        out[kind+'_when_IST'] = values
    return out


STYLE = '''body{font:15px/1.55 Arial,sans-serif;margin:0;color:#172735;background:#f5f7f9}
main{max-width:1450px;margin:auto;padding:30px}h1{font-size:29px;line-height:1.2}h2{font-size:22px;margin-top:34px}
h3{font-size:17px}a{color:#075991}p{max-width:1120px}.note{background:#fff3da;border-left:4px solid #b57915;padding:14px 18px}
.box{background:white;border:1px solid #dbe2e7;border-radius:7px;padding:14px 20px;margin:16px 0}
.scroll{overflow:auto;max-height:660px;background:white;border:1px solid #dbe2e7;margin:14px 0}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:9px 11px;border-bottom:1px solid #e2e7eb;text-align:left;vertical-align:top}
th{position:sticky;top:0;background:#203e55;color:white;z-index:1;white-space:normal}td{max-width:460px;overflow-wrap:anywhere}
tr:nth-child(even){background:#f6f8fa}summary{cursor:pointer;font-weight:bold;padding:10px}details{margin:12px 0}
nav{display:flex;flex-wrap:wrap;gap:8px 18px;margin:20px 0}.muted{color:#556572}code{font-size:12px}
input{padding:9px;border:1px solid #8e9ba5;border-radius:4px;min-width:240px}.stocklinks{display:flex;flex-wrap:wrap;gap:7px 15px}
@media print{main{padding:0}.scroll{max-height:none;overflow:visible}nav,input{display:none}th{position:static}}
'''


def start(title):
    return ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width,initial-scale=1">',
            '<title>' + html.escape(title) + '</title><style>' + STYLE + '</style></head><body><main>',
            '<h1>' + html.escape(title) + '</h1>']


def para(parts, text, cls=''):
    parts.append(f'<p class="{cls}">{html.escape(text)}</p>')


def table(parts, title, df, anchor=None):
    if title:
        parts.append(f'<h2 id="{html.escape(anchor or title.lower().replace(" ", "-"))}">{html.escape(title)}</h2>')
    if df.empty:
        para(parts, 'No rows in this evidence table. See the surrounding status and source files; an empty table alone does not establish a successful evaluation.')
    else:
        parts.append('<div class="scroll">' + df.to_html(index=False, escape=True, na_rep='Unavailable',
            float_format=lambda n: f'{n:,.4f}', border=0) + '</div>')


def link(parts, root, path, label=None):
    p = Path(path)
    if p.exists():
        href = p.relative_to(root).as_posix()
        parts.append(f'<p><a href="{html.escape(href)}">{html.escape(label or href)}</a></p>')


def build(root):
    root = Path(root).resolve()
    mdir, sdir, hdir = root/'movement', root/'selection', root/'history'
    movement = js(mdir/'summary.json')
    audit = js(choose(sdir, 'summary.json', 'audit_summary.json'))
    provenance = js(sdir/'provenance.json')
    history = frame(choose(hdir, 'comparison.csv', 'historical_comparison.csv'))
    daywise = frame(choose(hdir, 'daywise.csv', 'historical_daywise.csv'))
    hsummary = js(hdir/'summary.json') if (hdir/'summary.json').exists() else {}
    moves = frame(mdir/'universe_movements.csv')
    slots = frame(sdir/'all_stock_slot_audit.csv')
    gates = frame(sdir/'all_indicator_checks.csv')
    cf = frame(sdir/'top20_slot_counterfactuals.csv')
    earliest = frame(choose(sdir, 'top20_earliest_counterfactual_entries.csv', 'earliest_counterfactual_entries.csv'))
    single = frame(choose(sdir, 'single_blocker_candidates.csv', 'single_blocker_misses.csv'))
    up, down = frame(mdir/'gainers_ge_2pct.csv'), frame(mdir/'decliners_le_minus2pct.csv')
    assert len(moves) == movement['dated_stock_universe']
    assert moves.symbol.is_unique and len(slots) == len(moves) * audit['setups']
    assert len(gates) == audit['indicator_checks']
    assert set(up.symbol) == set(moves.loc[moves.peak_pct.ge(2), 'symbol'])
    assert set(down.symbol) == set(moves.loc[moves.trough_pct.le(-2), 'symbol'])
    if cf.empty or earliest.empty or history.empty:
        raise RuntimeError('Refusing final report until counterfactual and historical result files exist')
    protected = js(root/'protected_baselines_before.json')
    after = [dict(path=x['Path'], before=x['Hash'].lower(), after=sha(x['Path'])) for x in protected['files']]
    assert all(x['before'] == x['after'] for x in after), 'Protected baseline changed'
    groups = movement['groups']
    qualifying = set(up.symbol) | set(down.symbol)
    qslots = slots.loc[slots.symbol.isin(qualifying)].copy()
    qslots.to_csv(root/'qualifying_stock_slot_audit.csv', index=False)
    capture = []
    for name, members, side in [('Reached +2%', up, 'LONG'), ('Reached -2%', down, 'SHORT')]:
        ss = slots.loc[slots.symbol.isin(set(members.symbol)) & slots.side.eq(side)]
        selected = set(ss.loc[ss.selected.eq(True), 'symbol'])
        capture.append(dict(group=name, stocks=len(members), selected_any_time=len(selected),
            not_selected=len(members)-len(selected), pre_extreme_entries=0 if not selected else None,
            interpretation='Frozen G2 reconstruction, not live promoted-G execution'))
    pd.DataFrame(capture).to_csv(root/'qualifying_selection_summary.csv', index=False)
    day = movement['session_date']
    parts = start(f'Frozen V13-V10-G-2 universe audit — {day} IST')
    para(parts, 'A retrospective research report. Frozen G and standard G-2 remain unchanged. Prices and signals use the dated stock universe; all counterfactual outcomes are modeled, not broker fills.')
    para(parts, 'Bottom line: 213 reconciled stocks; 105 reached +2% and 15 reached -2%; frozen G-2 selected no orders and made no trades. There were real filter/confirmation blocks, not evidence of a universal missing-data failure. Relaxing gates can admit the eventual movers, but most tested day-specific policies lost money. The strongest modest historical research candidate is LONG-only confirmation volume 1.10, not a broad 1.00 relaxation.', 'box')
    parts.append('<nav><a href="#coverage">Coverage</a><a href="#movement">Moves</a><a href="#top-long">Top LONG</a><a href="#top-short">Top SHORT</a><a href="#selection">Selection</a><a href="#counterfactuals">Counterfactuals</a><a href="#history">History</a><a href="#stocks">Every stock</a><a href="#files">Files</a></nav>')
    para(parts, 'Important correction to the earlier conversation: these NSE FnO-underlying cash stocks have continuous trading through 15:15, followed by the 15:15–15:35 closing auction. Do not label absent 15:16–15:30 regular cash candles as missing continuous-session bars. Padded five-minute bars after 15:15 are not valid auction execution evidence.', 'note')
    parts.append('<p><a href="https://www.nseindia.com/static/products-services/closing-auction-session">Official NSE closing-auction session rules</a> · <a href="https://www.nseindia.com/all-reports">NSE official daily reports</a></p>')
    table(parts, 'Universe reconciliation and data coverage', pd.DataFrame([
        {'Measure':'Dated snapshot rows','Value':movement['dated_universe_total_rows']},
        {'Measure':'Index rows excluded','Value':movement['index_rows_excluded']},
        {'Measure':'Unique stock universe','Value':len(moves)},
        {'Measure':'Official daily-price usable stocks','Value':movement['usable_official_daily_universe']},
        {'Measure':'Expected continuous 1m timestamps','Value':movement['coverage']['frozen_1m']['total_expected_bars']},
        {'Measure':'Missing continuous 1m timestamps','Value':movement['coverage']['frozen_1m']['missing_timestamp_count']},
        {'Measure':'Positive-volume valid 1m bars used for extrema','Value':movement['coverage']['frozen_1m']['total_valid_bars']},
        {'Measure':'Zero-volume 1m bars excluded from extrema','Value':movement['coverage']['frozen_1m']['zero_volume_count']},
        {'Measure':'Valid continuous 5m bars','Value':movement['coverage']['current_5m']['total_valid_bars']},
        {'Measure':'Official highs without matched intraday time','Value':movement['official_high_not_matched_count']},
        {'Measure':'Official lows without matched intraday time','Value':movement['official_low_not_matched_count']},
    ]), 'coverage')
    para(parts, movement['session_finalization'])
    para(parts, f"Official Oct8 closes and Oct9 prior-close fields agree for all {len(moves)} stocks; all {movement['cas_final_close_matched_count']} auction final prices agree with official Oct9 closes. The cached previous-close comparison flags {movement['cached_previous_close_mismatch_count']} differences under the documented float32 tolerance, so movement reporting uses official prices. This does not change the frozen strategy's native feature definitions.")
    para(parts, 'The auction archive explains 11 unmatched highs and one unmatched low. BHARTIARTL high (official 1820.0 versus stored 1819.7) and GODREJCP low (858.1 versus 858.6) remain unreconciled in timestamp. BAJAJHLDNG is the additional official-only +2% stock: +2.0755% at auction versus a +1.5189% continuous-session peak. GMRAIRPORT also reaches its official high at auction; no exact execution timestamp is available.')
    para(parts, 'Zero-volume minutes are distinguished from absent timestamps and corrupt OHLCV. They are excluded from movement extrema, but the frozen simulator retains its established handling. We do not silently change execution logic. Official daily high/low may include auction or other session prints missing from the intraday store; no precise timestamp is invented for those extremes.')
    para(parts, 'The source feature ledger belongs to a completed promoted-G replay but is used only as a sealed feature source. Frozen G-2 selection is independently rebuilt from the original configuration and verified against the native selector. The promoted 09:25 branch is not included.')
    para(parts, 'Strategy five-minute features are generated from sealed one-minute equity inputs; the separately stored current/live five-minute files are checked for coverage, not substituted into the frozen replay. The native price-change gate compares a five-minute close with the prior five-minute close, whereas this movement report compares prices with the official previous-session close. Those are intentionally different references. Exact signal/prior-signal futures OI pairs and OI-change arithmetic were independently reconciled for all 1,917 feature rows.')
    table(parts, 'Exact strategy and replay inputs', pd.DataFrame([
        {'item': key, 'value': provenance[key]} for key in
        ['baseline_policy','feature_source_run','source_bundle','g_config','snapshot',
         'sealed_bundle_verified','snapshot_verified','native_independent_empty_and_nonempty_selection_parity']]))
    rows = []
    labels = [('gainers_ge_2pct','Full-session peak ≥ +2%','peak_move_pct'),
              ('decliners_le_minus2pct','Full-session trough ≤ −2%','trough_move_pct'),
              ('smaller_intraday_loss_gt_minus2_lt0','Trough strictly between −2% and 0%','trough_move_pct'),
              ('smaller_closing_loss_gt_minus2_lt0','Close strictly between −2% and 0%','closing_return_pct')]
    for key, label, metric in labels:
        g = groups[key]; s = g[metric]
        rows.append(dict(group=label,count=g['count'],percent_of_usable=g['pct_of_usable'],
            mean_move_pct=s['mean'],median_move_pct=s['median'],minimum_pct=s['minimum'],maximum_pct=s['maximum']))
    table(parts, 'Universe movement summary', pd.DataFrame(rows), 'movement')
    para(parts, f"Equal-weight official close-to-close return: {movement['equal_weight_official_close_to_close_return_pct']:.4f}%. Stocks appearing in both ±2% lists: {groups['both_peak_ge2_trough_le_minus2']['count']}. These groups overlap; they are not a partition of the universe.")
    table(parts, 'All-universe move statistics', pd.DataFrame([{'metric':k,**movement[k]} for k in
        ['universe_peak_move_pct','universe_trough_move_pct','universe_closing_return_pct']]))
    para(parts, movement['sum_caution'], 'note')
    para(parts, f"Continuous-trading observations prove {groups['observed_cts_gainers_ge_2pct']['count']} stocks reached +2% and {groups['observed_cts_decliners_le_minus2pct']['count']} reached −2%. Full-session official lists contain {len(up)} and {len(down)} respectively. The official-only difference is retained, not silently assigned an intraday entry time.")
    moves, up, down = display_timing(moves), display_timing(up), display_timing(down)
    move_cols = ['symbol','official_previous_close','official_high','official_low','official_close','peak_pct','trough_pct',
        'close_return_pct','peak_when_IST','peak_time_precision','trough_when_IST','trough_time_precision','frozen_1m_valid_bars','current_5m_valid_bars']
    topcols = ['symbol','official_previous_close','peak_pct','trough_pct','close_return_pct','peak_when_IST','trough_when_IST']
    table(parts, 'Top 10 LONG opportunities by full-session peak', subset(display_timing(frame(mdir/'top10_long.csv')),topcols), 'top-long')
    table(parts, 'Top 10 SHORT opportunities by full-session trough', subset(display_timing(frame(mdir/'top10_short.csv')),topcols), 'top-short')
    para(parts, movement['timing_caution'])
    table(parts, 'Every stock reaching +2% or more', subset(up,move_cols))
    table(parts, 'Every stock reaching −2% or less', subset(down,move_cols))
    table(parts, 'Frozen G-2 selection of qualifying movers', pd.DataFrame(capture), 'selection')
    para(parts, f"Audited {audit['feature_rows']:,} feature rows, {audit['stock_setup_cases']:,} stock/setup combinations and {audit['indicator_checks']:,} indicator checks. Frozen G-2 selected {audit['selected_orders']} orders. No downstream broker submission, fill, expiry or capital rejection is invented for a stock that never passed selection.")
    table(parts, 'Selection funnel by setup', frame(sdir/'stage_counts_by_setup.csv'))
    para(parts, 'Ten stock/setup cases pass the five-minute stage, but none passes all subsequent confirmation/setup checks. Price/OI pairs are usable at every audited stock/setup. FORCEMOT and PETRONET at the 11:20 signal have present but zero-range 11:21 confirmation candles; their body/wick ratios are undefined. These are not missing stock histories. Their genuine price/OI/EMA/volume and zero-range confirmation failures remain explicit, and the undefined ratios are distinguished from unavailable source data.')
    para(parts, 'Gate rows are reconstructed from sealed finalized features. A failed diagnostic check is not automatically a recorded live rejection. One-minute checks after a failed five-minute stage are labelled NOT_REACHED_DIAGNOSTIC_ONLY. Missing live scanner evidence is recorded separately; it does not erase the historical reconstruction.')
    para(parts, 'The recorded promoted-G live scanner artifact inventory contains only the 11:20 SUCCESS slot for this date. The morning live scanner outcomes are not evidenced by that inventory and are not relabelled as historical filter failures. The frozen G-2 replay is a separate retrospective result, not proof that these inputs were available to the live process at decision time.')
    link(parts,root,sdir/'live_artifact_inventory.csv','Recorded live scanner, selection and order artifact inventory')
    table(parts, 'Single-blocker misses', subset(single,['symbol','setup_id','side','signal_time','gate','observed','operator','required','margin','unit','decision_time','pipeline_evaluation']))
    para(parts, 'Margins use the units in each row: percentage points, ratio, fraction, rupees or seconds. Smaller absolute margins are comparable within the same gate and unit only. A small margin does not establish that relaxing it improves profitability.')
    link(parts,root,sdir/'narrowest_misses_by_gate.csv','Download all narrowest misses, grouped by gate')
    table(parts, 'Earliest modeled counterfactual entries for both top-10 lists', subset(earliest,[
        'symbol','side','signal_time','confirmation_time','entry_ts','entry_price','changed_independent_conditions',
        'original_failed_checks','effective_rank','effective_quota','exit_ts','exit_reason',
        'portfolio_net_profit_rupees','entry_before_measured_extreme','policy_id']), 'counterfactuals')
    para(parts, 'Thirteen of the top 20 stocks obtain at least one modeled fill within the tested families. No feasible entry was established for INOXWIND, PERSISTENT, TATAELXSI, FORTIS, ITC, ATHERENERG or TVSMOTOR without changing additional structural rules. All their setup-specific blockers are retained; this does not prove no other strategy could trade them.')
    para(parts, 'Minimal change means the boundary changes to failed conditions within the declared tested family, followed by a universe-wide reranking and, where recorded, the smallest tested slot quota needed. It is not a proof of a globally optimal or globally minimal strategy. Exact thresholds chosen after seeing this day are hindsight fitted even when their runtime inputs are causal.', 'note')
    para(parts, 'All candidate policies are applied to every available stock in their setup. No ticker exception or future ±2% label is used as a signal input. Original core-priority reservation, trigger semantics, 10-minute expiry, staged stop, targets, 5-bps round-trip costs, sizing and capital checks are retained. Native sizing uses Rs100,000 capital per trade at 5x exposure, a Rs1,000,000 portfolio capital cap, and fractional quantities. There is no configured symbol/day quota or concurrent-symbol cap; neither is invented as a rejection. Missing data and wrong confirmation direction are not fabricated away. A same-minute entry and extreme have unresolved ordering.')
    link(parts,root,sdir/'top20_slot_counterfactuals.csv','All top-20 stock/setup counterfactuals, including failed and unfilled cases')
    link(parts,root,sdir/'counterfactual_policies.json','Exact universe-wide diagnostic policies')
    table(parts, 'Counterfactual whole-universe session outcomes', frame(choose(sdir,'counterfactual_session_policy_comparison.csv','counterfactual_policy_session_metrics.csv')))
    para(parts, 'Twenty independent, overlapping session policies produced six positive outcomes, thirteen negative outcomes and one unfilled/zero outcome, ranging from -Rs19,500 to +Rs9,750. They must not be summed. SAIL at 10:00 needs only a volume relaxation, but fills after its 09:40 peak and loses Rs5,250. COLPAL enters before its peak and still loses Rs5,250: a large move from the previous close is not the same as a profitable trade from the entry price.')
    table(parts, 'Available-history comparison against frozen G-2', subset(history,[
        'variant','trades','net_pnl','net_delta_vs_baseline','win_rate_pct','profit_factor',
        'closed_trade_drawdown','day_close_drawdown','added_winners','added_losers','removed_trades']), 'history')
    para(parts, 'Scope: 48 source-eligible sessions from 29 July through 9 October 2026, not every possible session or the full 213 stocks on every historical day. Earlier uncaptured contracts, missing dates and declared reduced-universe exclusions are listed. The original sealed source uses contract-month membership snapshots, not independently reconstructed daily point-in-time universes. Exact archive parity was verified over 43 sessions through September 30: 94 selected orders, 85 fills and Rs229,917.039554 net P&L.')
    para(parts, 'These are historical research comparisons, not untouched out-of-sample validation. The original strategy was fitted on reused history. An increase in historical net P&L does not establish future profitability. Drawdowns below are realized closed-trade or day-close measures, not intraday marked-to-market risk.', 'note')
    if hsummary:
        parts.append('<details><summary>Historical scope, exclusions and validation status</summary>')
        parts.append('<pre>'+html.escape(json.dumps(hsummary,indent=2))+'</pre></details>')
    table(parts, 'Historical day-by-day results and differences', subset(daywise,[
        'date','variant','trades','selected_orders','net_pnl','net_delta_vs_baseline','wins','losses']))
    link(parts,root,choose(hdir,'historical_session_coverage.csv','extension_coverage.csv'),'Historical session coverage and exclusions')
    link(parts,root,hdir/'unrepresented_dates.csv','Dates without usable replay evidence')
    link(parts,root,choose(hdir,'added_removed_trades.csv','historical_added_removed.csv'),'Added and removed historical trades, including losers')
    para(parts, 'Exact top-20 hindsight-fitted combinations are not validated by testing a different, simpler volume rule. Any exact policy without its own full-history replay remains UNVALIDATED. Strict-candidate files alone cannot validate relaxing upstream EMA, direction or base gates. Some earlier raw history exists, but September 24 full raw features and newly admitted execution paths are not fully verified; historical full-universe +/-2% missed-opportunity capture was not computed. These gaps prevent claiming complete validation of the exact policies.')
    parts.append('<h2>Prioritized changes and research decisions</h2><ol>')
    for text in [
        'P0 — Make coverage calendars CAS-aware and separate actual traded bars, zero-volume records, auction observations and synthetic padding. Preserve the frozen backtest while improving reporting.',
        'P0 — Use official prior-session closes for movement/opportunity reporting and reconcile cached reference values. Changing the strategy’s own feature definition would be a new experiment, not a baseline repair.',
        'P1 — Publish the complete selection funnel and gate evidence when there are zero trades. Keep live data-readiness failure separate from historical filter rejection.',
        'P1 — First shadow-test LONG-only confirmation volume 1.20 to 1.10: +Rs9,100 across 48 eligible sessions, three additional trades (two wins, one loss), essentially unchanged profit factor and unchanged realized drawdown. The sample of added trades is far too small for promotion.',
        'P1 — Keep BOTH-side 1.10 as a secondary experiment: +Rs11,712.77 but eight added trades include four losers, one baseline loser is displaced, and profit factor falls from 2.7355 to 2.6204. Higher net profit alone is not decisive.',
        'P1 — Do not promote BOTH-side 1.00 on this evidence: it captures ICICIGI on Oct9 (09:55 signal, 09:57 modeled fill at Rs1,679.50, target at 13:56, +Rs4,250) but loses Rs11,167.99 relative to G-2 across history, adds eleven losers, and displaces two baseline winners.',
        'P2 — Separately shadow-test SHORT 09:35 setup OI 0.50% to 0.10%: +Rs3,667 and lower realized closed-trade drawdown (Rs22,130.90 to Rs17,000), but three added losers and a lower profit factor. This is a small-sample tradeoff, not an approved change.',
        'P2 — Retain exact top-20 counterfactuals as unvalidated diagnostics until all affected raw historical candidates and minute paths can be replayed. Then use a prespecified untouched forward sample before considering promotion.',
        'P2 — Capture official auction results and timestamps separately so full-session extreme timing and before-extreme entry claims can be verified.'
    ]:
        parts.append('<li>'+html.escape(text)+'</li>')
    parts.append('</ol>')
    parts.append('<h2 id="stocks">Stock-by-stock evidence for all dated symbols</h2><p>Open a symbol for every setup, every gate, provenance and applicable entry counterfactuals.</p><input id="stock-search" placeholder="Filter symbols" aria-label="Filter symbols"><div class="stocklinks">')
    stocksdir=root/'stocks'; stocksdir.mkdir(exist_ok=True)
    gate_cols=['setup_id','side','stage','gate','observed','operator','required','margin','unit','threshold_result','pipeline_evaluation','decision_time','evidence_csv_row']
    slot_cols=['setup_id','side','signal_time','confirmation_time','data_usable','five_minute_pass','confirmation_reached','confirmation_pass_diagnostic','selection_status','failures','ranking_guard','slot_capacity_guard','capital_guard','entry_trigger','entry_expiry','live_promoted_scanner_slot_state','evidence_csv_row']
    for symbol in sorted(moves.symbol):
        parts.append(f'<a data-symbol="{html.escape(symbol)}" href="stocks/{html.escape(symbol)}.html">{html.escape(symbol)}</a>')
        sp=start(f'{symbol} — frozen G-2 audit, {day} IST')
        sp.append('<p><a href="../report.html">Back to report</a></p>')
        table(sp,'Movement and coverage',subset(moves.loc[moves.symbol.eq(symbol)],move_cols))
        table(sp,'Every configured setup',subset(slots.loc[slots.symbol.eq(symbol)],slot_cols))
        table(sp,'All reconstructed indicator checks',subset(gates.loc[gates.symbol.eq(symbol)],gate_cols))
        if not cf.empty and 'symbol' in cf and cf.symbol.eq(symbol).any():
            table(sp,'Top-20 counterfactual tests — not live recommendations',cf.loc[cf.symbol.eq(symbol)])
        para(sp, 'Full source paths, file hashes, row hashes and exact consumed-input hashes are retained in the CSV gate audit. Reconstructed passes/failures are not recorded live outcomes. Unreached downstream stages remain unreached.')
        sp.append('<p><a href="../selection/all_indicator_checks.csv">Full gate evidence CSV</a> · <a href="../selection/provenance.json">Frozen strategy and source provenance</a></p>')
        sp.append('</main></body></html>')
        (stocksdir/f'{symbol}.html').write_text('\n'.join(sp),encoding='utf-8')
    parts.append('</div><script>document.querySelector("#stock-search").addEventListener("input",function(){const q=this.value.trim().toUpperCase();document.querySelectorAll("[data-symbol]").forEach(a=>a.hidden=!a.dataset.symbol.includes(q));});</script>')
    parts.append('<h2 id="files">Exact output files and provenance</h2><ul>')
    artifacts=[]
    for p in sorted(root.rglob('*')):
        if not p.is_file() or p.suffix.lower() not in {'.csv','.json','.zip'} or p.name=='report_manifest.json':
            continue
        rel=p.relative_to(root).as_posix()
        parts.append(f'<li><a href="{html.escape(rel)}">{html.escape(rel)}</a></li>')
        artifacts.append(dict(path=rel,sha256=sha(p),bytes=p.stat().st_size))
    parts.append('</ul></main></body></html>')
    (root/'report.html').write_text('\n'.join(parts),encoding='utf-8')
    validation=dict(session_date=day,generated_at=datetime.now().isoformat(),dated_stocks=len(moves),
        qualifying_stocks=len(qualifying),qualifying_slots=len(qslots),stock_pages=len(moves),stock_setup_rows=len(slots),gate_rows=len(gates),
        candidate_counterfactual_rows=len(cf),earliest_counterfactual_rows=len(earliest),protected_baselines=after,
        all_baselines_unchanged=True,report_sha256=sha(root/'report.html'),artifacts=artifacts,
        limitations=['Auction extreme timestamps may be unavailable','Exact hindsight policies unvalidated unless explicitly replayed',
                     'Available history is not an untouched test; gaps are enumerated','Historical full-universe +/-2% capture unavailable where raw daily evidence absent'])
    validation['research_code'] = [dict(path=str(p.resolve()),sha256=sha(p)) for p in
        Path(__file__).parent.glob('g2_audit_*_v2.py')]
    (root/'report_manifest.json').write_text(json.dumps(validation,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in validation.items() if k not in ['artifacts','protected_baselines']},indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-root',type=Path,required=True)
    build(p.parse_args().output_root)
