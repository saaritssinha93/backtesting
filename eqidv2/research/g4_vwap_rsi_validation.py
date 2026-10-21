"""Fixed G4 VWAP 2.5-sigma AND RSI14 weakening experiment; no promotion.

Rebuild all enabled pre-quota candidate pools, reproduce both G3/G4 controls,
then reselect and replay the full portfolio with the existing execution model.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date, datetime
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_g3_mirror_backtest as base
from research.g4_rejection_validation import metrics, REFERENCE, REFERENCE_HASH
from research.g4_full_candidate_pool import build_pool, _cache_code
from research import g4_vwap_rsi_features as features

VERSION = 'V13-V10-G-4-VWAP-RSI-VALIDATION-V1'
SCENARIOS = ('g3_control', 'g4_current', 'g4_vwap_rsi')


def select(pool, limits, enabled):
    eligible = pool.loc[~pool.vwap_rsi_reject.eq(True)].copy() if enabled else pool.copy()
    return base.reselect(eligible, limits)


def pin_trigger_precision(selected, references):
    """Preserve archived pre-entry triggers, allowing at most one binary ULP.

    The first reference has precedence. No fill, exit, or outcome is copied.
    A freshly reconstructed candidate without an archived order is unchanged.
    """
    work = selected.copy()
    archived = pd.concat([frame.assign(trigger_precision_source=name)
                          for name, frame in references]).drop_duplicates('sid', keep='first').set_index('sid')
    audit = []
    for ix, row in work.iterrows():
        if row.sid not in archived.index:
            continue
        pinned = archived.loc[row.sid]
        for key in ('setup_id', 'tradingsymbol', 'side'):
            if row[key] != pinned[key]:
                raise ValueError('Archived trigger identity differs: ' + key)
        if str(row.day)[:10] != str(pinned.day)[:10]:
            raise ValueError('Archived trigger session differs')
        raw, exact = float(row.trigger), float(pinned.trigger)
        if not np.isfinite([raw, exact]).all() or min(raw, exact) <= 0:
            raise ValueError('Invalid archived trigger')
        ulp = max(abs(np.spacing(raw)), abs(np.spacing(exact)))
        if abs(raw-exact) > ulp:
            raise ValueError('Reconstructed trigger differs by more than one ULP')
        if raw != exact:
            audit.append(dict(sid=int(row.sid), day=str(row.day)[:10], setup_id=row.setup_id,
                tradingsymbol=row.tradingsymbol, side=row.side, source=pinned.trigger_precision_source,
                reconstructed_trigger=raw, pinned_trigger=exact, ulp_difference=abs(raw-exact)/ulp))
        work.at[ix, 'trigger'] = exact
    return work, audit


def trade_changes(before, after):
    """Include allocation changes to shared trades, not just additions/removals."""
    left = before.loc[before.portfolio_executed.eq(True)].set_index('sid')
    right = after.loc[after.portfolio_executed.eq(True)].set_index('sid')
    rows = []
    for sid in sorted(set(left.index) | set(right.index)):
        old = float(left.loc[sid, 'portfolio_net_profit_rupees']) if sid in left.index else 0.
        new = float(right.loc[sid, 'portfolio_net_profit_rupees']) if sid in right.index else 0.
        if sid in left.index and sid in right.index and np.isclose(old, new, atol=1e-8, rtol=0):
            continue
        row = right.loc[sid] if sid in right.index else left.loc[sid]
        action = 'ADDED_EXECUTED' if sid not in left.index else 'REMOVED_EXECUTED' if sid not in right.index else 'ALLOCATION_CHANGED'
        rows.append(dict(action=action, sid=int(sid), day=str(row.day), setup_id=row.setup_id,
            tradingsymbol=row.tradingsymbol, side=row.side, exit_reason=row.exit_reason,
            before_net_pnl_rs=old, after_net_pnl_rs=new, delta_net_pnl_rs=new-old))
    return pd.DataFrame(rows, columns=['action','sid','day','setup_id','tradingsymbol','side',
        'exit_reason','before_net_pnl_rs','after_net_pnl_rs','delta_net_pnl_rs'])


def _numeric_same(actual, expected):
    for key, value in actual.items():
        if value is None and pd.isna(expected[key]):
            continue
        if not np.isclose(value, expected[key], atol=1e-6, rtol=0):
            raise ValueError('Accounting mismatch: '+key)


def run(output):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(base.G4_ROOT.resolve()):
        raise ValueError('Requires a fresh directory within the G4 result root')
    if base.sha256(REFERENCE/'manifest.json') != REFERENCE_HASH:
        raise ValueError('Pinned G4 reference changed')
    ref = base.verify(REFERENCE)
    cfg = base.configuration()
    source_paths = [Path(__file__), Path(features.__file__), Path(__file__).with_name('g4_full_candidate_pool.py'),
                    Path(__file__).with_name('g4_rejection_validation.py')]
    code_proof = {**_cache_code(), **{str(p.resolve()):base.sha256(p) for p in source_paths}}
    pointers = {str(p):base.sha256(p) for p in (base.G4_ROOT/'latest/g3_mirror.json',base.G3_ROOT/'latest/current_backtest.json')}
    g3dir = Path(ref['baseline_g3_run'])
    if base.sha256(g3dir/'manifest.json') != ref['baseline_g3_manifest_sha256']:
        raise ValueError('G3 reference changed')
    summary, _ = base.verified_package(g3dir,base.G3_ROOT)
    days = [date.fromisoformat(d) for d in summary['days']]
    from fno_v13_v10_g_3_daily_replay import require_closed_session
    require_closed_session(days[-1])
    original = base.read_frozen_csv(g3dir/'selected_orders.csv')
    original_g4 = base.read_frozen_csv(REFERENCE/'guard_all_ten/selected_orders.csv')
    archived_trigger_comparison = original[['sid','day','setup_id','tradingsymbol','trigger']].merge(
        original_g4[['sid','trigger']],on='sid',suffixes=('_g3','_g4'),validate='one_to_one')
    archived_trigger_comparison = archived_trigger_comparison.loc[
        archived_trigger_comparison.trigger_g3.ne(archived_trigger_comparison.trigger_g4)].copy()
    old_g3 = base.read_frozen_csv(g3dir/'portfolio_trades.csv')
    old_g4 = base.read_frozen_csv(REFERENCE/'guard_all_ten/portfolio_trades.csv')
    prices = base.Prices(summary)
    prepared_cache = base.ROOT/'.codex_tmp/g4_vwap_rsi_prepared_pool'
    pool, pool_proof = build_pool(summary,original,prices,REFERENCE,cache_dir=prepared_cache)
    cache_manifest_digest = base.sha256(prepared_cache/'manifest.json')
    pool_proof['prepared_cache'] = dict(path=str(prepared_cache),manifest_sha256=cache_manifest_digest)
    limits = cfg['scenarios']['guard_all_ten']
    if base.selected_keys(base.reselect(pool,limits)) != base.selected_keys(old_g4):
        raise ValueError('All-setup pool does not reproduce current G4 selection')
    print(f'All enabled setup pools reproduce G3/G4: {len(pool)} candidates',flush=True)
    feature_rows = []
    for n,row in enumerate(pool.itertuples(index=False)):
        raw,key = prices.frame(pd.Timestamp(row.day).date(),row.tradingsymbol)
        feature_rows.append(features.vwap_rsi_features(raw,pd.Series(row._asdict())))
        if (n+1)%500 == 0:
            print(f'Calculated causal VWAP/RSI: {n+1}/{len(pool)}',flush=True)
    pool = pd.concat([pool.reset_index(drop=True),pd.DataFrame(feature_rows)],axis=1)
    # Feature module uses a descriptive flag name; this stable audit column is the selector input.
    pool['vwap_rsi_reject'] = pool['vwap_rsi_flag']
    print(f'Feature status: {pool.vwap_rsi_status.value_counts().to_dict()}; flags={int(pool.vwap_rsi_reject.sum())}',flush=True)
    paths = prices.paths(original)
    ledgers, orders_by, payloads, rows, setup_rows, period_rows = {},{},{},[],[],[]
    precision_rows = []
    for name in SCENARIOS:
        guards = {} if name == 'g3_control' else limits
        enabled = name == 'g4_vwap_rsi'
        selected = select(pool,guards,enabled)
        references = [('g3_selected_orders',original),('g4_selected_orders',original_g4)]
        if name != 'g3_control':
            references.reverse()
        selected, precision = pin_trigger_precision(selected,references)
        precision_rows.extend(dict(scenario=name,**item) for item in precision)
        trial_cfg = copy.deepcopy(cfg)
        trial_cfg.update(variant_id=VERSION,scenarios={name:guards})
        identity = base.Identity(trial_cfg,name)
        identity.payload['vwap_rsi_guard'] = dict(enabled=enabled,band_sigmas=2.5,rsi_period=14,rsi_lag_minutes=3,
            min_session_bars=30,session_first_bar_end='09:16',session_reset=True,vwap_price='HLC3',
            sigma='VOLUME_WEIGHTED_POPULATION_SD_OF_HLC3',rsi='WILDER_SMA_SEED',
            long='close > VWAP + 2.5*sigma AND RSI14(t) < RSI14(t-3)',
            short='close < VWAP - 2.5*sigma AND RSI14(t) > RSI14(t-3)',
            insufficient_warmup='KEEP_BASELINE_RULES_AND_REPORT_NOT_APPLICABLE',invalid_inputs='ABORT_EXPERIMENT',
            setups=sorted(base.donor._ACTIVE_BY_ID) if enabled else [],completed_confirmation_only=True)
        identified = identity.identify(selected)
        if identified.sid.duplicated().any():
            raise ValueError('Duplicate SID')
        paths.update(prices.paths(identified.loc[~identified.sid.isin(paths)]))
        ledger = base._simulate(identified,paths,days,identity)
        ledger['strategy'] = 'V13-V10-G-4'
        shared = ledger.loc[ledger.sid.isin(old_g3.sid)]
        base._same_native(shared,old_g3.loc[old_g3.sid.isin(shared.sid)])
        if name != 'g3_control':
            shared_g4 = ledger.loc[ledger.sid.isin(old_g4.sid)]
            base._same_native(shared_g4,old_g4.loc[old_g4.sid.isin(shared_g4.sid)])
        if name != 'g4_vwap_rsi':
            pinned = old_g3 if name == 'g3_control' else old_g4
            base._same_native(ledger,pinned)
            base._same_allocation(ledger,pinned)
        stat = metrics(ledger,days)
        stat.update(scenario=name,loss_rate_pct=100*stat['losses']/stat['executed_trades'],
            net_delta_vs_current_g4_rs=stat['net_pnl_rs']-metrics(old_g4,days)['net_pnl_rs'])
        rows.append(stat)
        ledgers[name],orders_by[name],payloads[name] = ledger,identified,identity.payload
        for spec in base.donor.ACTIVE_SETUPS:
            setup_rows.append(dict(scenario=name,setup_id=spec.setup_id,signal_end=spec.signal_end,
                side=spec.side,**metrics(ledger.loc[ledger.setup_id.eq(spec.setup_id)],days)))
        periods = {'EARLY_25':days[:25],'LATER_26':days[25:]}
        periods.update({m:[d for d in days if str(d).startswith(m)] for m in sorted({str(d)[:7] for d in days})})
        for label,period_days in periods.items():
            period_rows.append(dict(scenario=name,period=label,**metrics(ledger.loc[ledger.day.isin([str(d) for d in period_days])],period_days)))
        print(f"{name}: {stat['executed_trades']} trades; net={stat['net_pnl_rs']:.2f}; losses={stat['losses']}; stops={stat['stop_exits']}; losingdays={stat['losing_days']}",flush=True)
    comparison = pd.DataFrame(rows)
    changes = trade_changes(ledgers['g4_current'],ledgers['g4_vwap_rsi'])
    delta = comparison.set_index('scenario').loc['g4_vwap_rsi','net_delta_vs_current_g4_rs']
    if not np.isclose(changes.delta_net_pnl_rs.sum(),delta,atol=1e-6,rtol=0):
        raise ValueError('Changed trades do not explain net difference')
    for path,digest in {**code_proof,**pointers,str(prepared_cache/'manifest.json'):cache_manifest_digest}.items():
        if base.sha256(Path(path)) != digest:
            raise ValueError('Code or active pointer changed: '+path)
    for key,proof in prices.proof.items():
        if base.sha256(Path(key)) != proof['sha256']:
            raise ValueError('Price input changed during experiment: '+key)
    output.mkdir(parents=True)
    daily = pd.DataFrame({'day':[str(d) for d in days]})
    for name in SCENARIOS:
        folder=output/name;folder.mkdir()
        orders_by[name].to_csv(folder/'selected_orders.csv',index=False)
        ledgers[name].to_csv(folder/'portfolio_trades.csv',index=False)
        table=base._daily(ledgers[name],days)
        table.to_csv(folder/'daily_results.csv',index=False)
        daily=daily.merge(table[['day','trades','wins','losses','net_pnl']].rename(columns={c:name+'_'+c for c in ('trades','wins','losses','net_pnl')}),on='day',validate='one_to_one')
        (folder/'config.json').write_text(json.dumps(payloads[name],indent=2)+'\n',encoding='utf-8')
    daily['net_delta_vs_current_g4']=daily.g4_vwap_rsi_net_pnl-daily.g4_current_net_pnl
    comparison.to_csv(output/'comparison.csv',index=False)
    daily.to_csv(output/'daily_comparison.csv',index=False)
    changes.to_csv(output/'changed_executed_trades.csv',index=False)
    pool.to_csv(output/'candidate_vwap_rsi_audit.csv',index=False)
    pd.DataFrame(setup_rows).to_csv(output/'setup_results.csv',index=False)
    pd.DataFrame(period_rows).to_csv(output/'period_comparison.csv',index=False)
    pd.DataFrame(precision_rows).to_csv(output/'trigger_precision_audit.csv',index=False)
    archived_trigger_comparison.to_csv(output/'archived_g3_g4_trigger_differences.csv',index=False)
    counts=pool.groupby(['setup_id','vwap_rsi_status'],dropna=False).agg(candidates=('sid','size'),flagged=('vwap_rsi_reject','sum')).reset_index()
    counts.to_csv(output/'feature_coverage.csv',index=False)
    removed=changes.loc[changes.action.eq('REMOVED_EXECUTED')]
    added=changes.loc[changes.action.eq('ADDED_EXECUTED')]
    effect=dict(removed_executed=len(removed),removed_winners=int(removed.before_net_pnl_rs.gt(0).sum()),
        removed_losers=int(removed.before_net_pnl_rs.lt(0).sum()),removed_net_pnl_rs=float(removed.before_net_pnl_rs.sum()),
        added_executed=len(added),added_winners=int(added.after_net_pnl_rs.gt(0).sum()),added_losers=int(added.after_net_pnl_rs.lt(0).sum()),
        added_net_pnl_rs=float(added.after_net_pnl_rs.sum()),allocation_changes=int(changes.action.eq('ALLOCATION_CHANGED').sum()))
    result=dict(version=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),first_date=str(days[0]),last_date=str(days[-1]),sessions=len(days),
        reference=str(REFERENCE),reference_manifest_sha256=REFERENCE_HASH,scenarios=rows,change_effect=effect,
        candidates=len(pool),feature_status_counts=pool.vwap_rsi_status.value_counts().to_dict(),
        flagged_candidates=int(pool.vwap_rsi_reject.sum()),current_g3_g4_reproduced=True,active_pointers_unchanged=True,
        trigger_precision_audit=precision_rows,
        archived_g3_g4_trigger_differences=archived_trigger_comparison.to_dict('records'),
        candidate_source_proof=pool_proof,price_sources=prices.proof,coverage=ref.get('coverage',{}),
        limitations=ref['limitations']+[
            'The additional filter is tested on all 49 enabled setup definitions, but applies only after 30 completed session bars (09:45 confirmation onward). Earlier setups retain current rules.',
            'VWAP and weighted deviation use one-minute HLC3 and volume, a bar approximation rather than trade-tick VWAP.',
            'RSI uses same-session closes, arithmetic seeding over 14 changes, then Wilder smoothing; no prior-day history.',
            'A single fixed 2.5-sigma / RSI14 / 3-minute configuration was tested. No parameter search or automatic promotion.',
            'Known candidates retain archived pre-execution trigger precision after a strict one-ULP equivalence check. Each control uses its own archived orders; the experiment prioritizes current G4 orders with G3 fallback. Newly admitted candidates retain reconstructed triggers. No execution outcomes are copied; see trigger_precision_audit.csv.',
            'All 51 sessions have already influenced strategy development; chronological partitions are diagnostics, not untouched holdouts.',
            'Existing execution assumptions and retrospective opening OI limitations are inherited. No claim of real trading returns or future loss reduction.',
            'The comparison is pinned through 14 October 2026; no 15 October results are included. 1 October remains excluded for incomplete baseline inputs.'])
    (output/'summary.json').write_text(json.dumps(result,indent=2,default=str)+'\n',encoding='utf-8')
    write_report(output,result,comparison,daily,changes,pd.DataFrame(period_rows),counts)
    manifest=dict(version=VERSION,execution_authority=False,code_sha256=code_proof,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in sorted(output.rglob('*')) if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    for name in SCENARIOS:
        saved=base.read_frozen_csv(output/name/'portfolio_trades.csv')
        _numeric_same(metrics(saved,days),comparison.set_index('scenario').loc[name])
        pd.testing.assert_frame_equal(base.read_frozen_csv(output/name/'daily_results.csv'),base._daily(saved,days),check_dtype=False,atol=1e-6,rtol=0)
    print(json.dumps({'output':str(output),'effect':effect,'net_delta':delta}),flush=True)
    return result


def write_report(output,result,comparison,daily,changes,periods,coverage):
    columns=['scenario','executed_trades','wins','losses','win_rate_pct','stop_exits','stop_rate_pct','losing_days','net_pnl_rs','net_delta_vs_current_g4_rs','max_drawdown_rs']
    page='''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 VWAP and RSI comparison</title>
<style>body{font:16px/1.55 system-ui;background:#111827;color:#f3f4f6;margin:28px}a{color:#7dd3fc}h1,h2{color:#fff}table{border-collapse:collapse;font-size:14px}td,th{padding:8px;border-bottom:1px solid #475569;text-align:left;white-space:nowrap}.scroll{overflow:auto}p{max-width:1100px}.note{padding:14px;background:#1e293b;border-left:4px solid #7dd3fc}</style></head><body>
<h1>G4: VWAP deviation + RSI weakening</h1><p>29 July–14 October 2026 · 51 completed sessions · 49 enabled setup definitions. G3 and current G4 controls reproduce their saved selections, native outcomes and portfolio allocation exactly.</p>
<p class="note">One fixed experiment: retain every existing G4 ATR guard. Reject LONG only when its completed confirmation close exceeds session VWAP + 2.5 weighted standard deviations AND RSI14 is lower than three completed minutes earlier. SHORT is mirrored. Evaluate before ranking and quota; allow replacements and replay the whole portfolio. First 30 completed session minutes are required. No settings are promoted.</p>'''
    for title,table in [('Portfolio comparison (INR after existing costs)',comparison[columns]),
            ('Executed changes versus current G4',changes),('Daywise results',daily),
            ('Chronological diagnostics — not untouched validation',periods),('Feature coverage',coverage)]:
        page+='<h2>'+html.escape(title)+'</h2><div class="scroll">'+table.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>'
    page+='<h2>Method and limitations</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in result['limitations'])+'</ul>'
    page+='<p>'+ ' · '.join(f'<a href="{name}.csv">{label}</a>' for name,label in [('comparison','Portfolio totals'),('daily_comparison','Daily comparison'),('setup_results','Setup results'),('candidate_vwap_rsi_audit','Candidate audit'),('changed_executed_trades','Changed trades'),('period_comparison','Period results')])+'</p></body></html>'
    (output/'report.html').write_text(page,encoding='utf-8')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    run(parser.parse_args().output_dir)
