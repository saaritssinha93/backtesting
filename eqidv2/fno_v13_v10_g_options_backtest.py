"""Reproducible three-lot, five-minute ATM options research for retained G.

This module never submits orders or changes the stock/live strategy. Selection
of option exits uses only the chronological training partition. Full-sample
rankings are exported as descriptive research, never as a validation result.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from fno_v13_v10_g_options_execution import simulate_trade
from fno_v13_v10_g_options_signals import make_signals
from fno_v13_v10_g_options_data import map_and_load, build_fetch_plan

DEFAULT_OUTPUT = Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914')
BASELINE = (.175, .225)
GRID = sorted(set([BASELINE] + [(s, t) for s in (.10, .15, .175, .20, .25)
                              for t in (.20, .225, .25, .30, .40) if t >= 1.2*s-1e-10]))
MIN_GLOBAL_TRAIN = 8
MIN_GROUP_TRAIN = 12


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def json_value(v):
    if isinstance(v, dict):
        return {str(k): json_value(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [json_value(x) for x in v]
    if isinstance(v, (pd.Timestamp, Path)):
        return str(v)
    if isinstance(v, np.generic):
        return json_value(v.item())
    if v is pd.NA or v is pd.NaT or isinstance(v, float) and not math.isfinite(v):
        return None
    return v


def write_json(path, data):
    Path(path).write_text(json.dumps(json_value(data), indent=2, allow_nan=False), encoding='utf-8')


def capital_replay(frame, capital):
    """Pay full premium and fees; release proceeds only once exits are observable."""
    out = frame.copy()
    out['portfolio_status'] = 'NOT_ENTERED'
    out['free_cash_before_entry'] = np.nan
    events, pending = [], []
    cash, reserved, peak = float(capital), 0., 0.
    indexes = out.loc[out.status.isin(['CLOSED', 'UNRESOLVED'])].sort_values(
        ['entry_ts', 'setup_id', 'trade_id'], kind='stable').index
    for ix in indexes:
        row = out.loc[ix]
        stamp = pd.Timestamp(row.entry_ts)
        releasable = sorted([x for x in pending if x[0] <= stamp], key=lambda x: (x[0], x[1]))
        for end, tid, receipt, outlay in releasable:
            cash += receipt
            reserved -= outlay
            events.append(dict(timestamp=end, trade_id=tid, event='SELL', cash_change=receipt, free_cash=cash))
        pending = [x for x in pending if x[0] > stamp]
        cost = float(row.entry_price)*int(row.quantity) + float(row.entry_costs)
        out.at[ix, 'free_cash_before_entry'] = cash
        if cost > cash + 1e-8:
            out.at[ix, 'portfolio_status'] = 'REJECTED_PREMIUM_CAPITAL'
            continue
        out.at[ix, 'portfolio_status'] = 'ADMITTED'
        cash -= cost
        reserved += cost
        peak = max(peak, reserved)
        events.append(dict(timestamp=stamp, trade_id=row.trade_id, event='BUY', cash_change=-cost, free_cash=cash))
        if row.status == 'CLOSED':
            observed = pd.Timestamp(row.exit_observed_ts)
            receipt = float(row.exit_price)*int(row.quantity)-float(row.exit_costs)
            pending.append((observed, row.trade_id, receipt, cost))
    for end, tid, receipt, outlay in sorted(pending, key=lambda x: (x[0], x[1])):
        cash += receipt
        reserved -= outlay
        events.append(dict(timestamp=end, trade_id=tid, event='SELL', cash_change=receipt, free_cash=cash))
    return out, dict(initial_capital=capital, ending_free_cash=cash,
                     unresolved_reserved_premium_and_fees=max(0., reserved),
                     peak_reserved_premium_and_fees=peak,
                     capital_rejections=int(out.portfolio_status.eq('REJECTED_PREMIUM_CAPITAL').sum())), events


def replay(mapped, paths, policy, *, capital=1_500_000, slippage_bps=10, participation=.10):
    records, bars = [], []
    for row in mapped.to_dict('records'):
        sid = row['trade_id']
        stop, target = policy.get(row['setup_id'], policy['default'])
        if row.get('signal_status') != 'READY' or not str(row.get('mapping_status', '')).startswith('MAPPED_'):
            records.append({**row, 'stop_pct': stop, 'target_pct': target,
                            'status': 'SKIPPED', 'reason': row.get('mapping_status', row.get('signal_status'))})
            continue
        result, audit = simulate_trade(row, paths.get(sid, pd.DataFrame()), stop, target,
                                      slippage_bps=slippage_bps, participation=participation, lots=3)
        records.append(result)
        bars.extend(audit)
    frame = pd.DataFrame(records)
    for col in ('entry_price', 'entry_costs', 'exit_price', 'exit_costs', 'quantity', 'net_pnl', 'gross_pnl'):
        if col not in frame:
            frame[col] = np.nan
    frame, capacity, events = capital_replay(frame, capital)
    return frame, pd.DataFrame(bars), capacity, pd.DataFrame(events)


def metrics(frame):
    admitted = frame.loc[frame.portfolio_status.eq('ADMITTED')]
    closed = admitted.loc[admitted.status.eq('CLOSED')]
    unresolved = admitted.loc[admitted.status.eq('UNRESOLVED')]
    pnl = pd.to_numeric(closed.net_pnl, errors='coerce')
    wins, losses = pnl[pnl > 0].sum(), -pnl[pnl < 0].sum()
    daily = closed.groupby('day').net_pnl.sum().sort_index().astype(float)
    curve = np.r_[0., daily.cumsum().to_numpy()]
    unresolved_loss = -(unresolved.entry_price*unresolved.quantity+unresolved.entry_costs).sum()
    return dict(attempts=len(frame), entered=len(admitted), closed=len(closed), unresolved=len(unresolved),
                wins=int((pnl > 0).sum()), losses=int((pnl < 0).sum()),
                win_rate_pct=float((pnl > 0).mean()*100) if len(pnl) else None,
                profit_factor=float(wins/losses) if losses > 0 else (math.inf if wins > 0 else None),
                net_pnl=float(pnl.sum()), gross_pnl=float(closed.gross_pnl.sum()),
                costs=float(admitted.entry_costs.sum()+closed.exit_costs.sum()),
                unresolved_entry_costs=float(unresolved.entry_costs.sum()),
                net_pnl_full_premium_loss_bound=float(pnl.sum()+unresolved_loss),
                daily_realized_drawdown=float(np.max(np.maximum.accumulate(curve)-curve)),
                capital_rejections=int(frame.portfolio_status.eq('REJECTED_PREMIUM_CAPITAL').sum()),
                entry_capacity_breaches=int(admitted.get('entry_capacity_breach', pd.Series(dtype=bool)).eq(True).sum()),
                exit_capacity_breaches=int(admitted.get('exit_capacity_breach', pd.Series(dtype=bool)).eq(True).sum()))


def score(frame):
    entered = frame.loc[frame.portfolio_status.eq('ADMITTED')].copy()
    if entered.empty:
        return -math.inf
    pnl = pd.to_numeric(entered.net_pnl, errors='coerce')
    # An entered trade with no trustworthy exit cannot disappear from selection.
    pnl = pnl.fillna(-(entered.entry_price*entered.quantity+entered.entry_costs))
    returns = pnl/(entered.entry_price*entered.quantity)
    return float(returns.mean() - returns.std(ddof=1)/math.sqrt(len(returns))) if len(returns) > 1 else -math.inf


def choose_profile(sweeps, cutoff, setup_ids):
    """All outcome access is restricted to day <= cutoff, including fallback."""
    training = {pair: f.loc[f.day.le(cutoff)] for pair, f in sweeps.items()}
    candidates = []
    for pair, frame in training.items():
        m = metrics(frame)
        n_days = frame.loc[frame.portfolio_status.eq('ADMITTED'), 'day'].nunique()
        if m['entered'] >= MIN_GLOBAL_TRAIN and n_days >= 4:
            candidates.append((score(frame), -pair[0], -pair[1], pair))
    default = max(candidates)[-1] if candidates else BASELINE
    policy, rows = {'default': default}, []
    for setup in setup_ids:
        side = 'LONG' if setup.endswith('LONG') else 'SHORT'
        chosen, scope, support = default, ('TRAIN_GLOBAL' if candidates else 'PREDECLARED_BASELINE_SPARSE_TRAIN'), 0
        for group in ('SIDE', 'SETUP'):
            ranked = []
            for pair, frame in training.items():
                sub = frame.loc[frame.setup_id.eq(setup)] if group == 'SETUP' else frame.loc[frame.side.eq(side)]
                eligible = sub.loc[sub.portfolio_status.eq('ADMITTED')]
                if len(eligible) >= MIN_GROUP_TRAIN and eligible.day.nunique() >= 4:
                    ranked.append((score(sub), -pair[0], -pair[1], pair, len(eligible)))
            if ranked:
                winner = max(ranked)
                chosen, support, scope = winner[-2], winner[-1], 'TRAIN_'+group
        policy[setup] = chosen
        actual = training[chosen]
        rows.append(dict(setup_id=setup, option_type='CE' if side == 'LONG' else 'PE',
                         stop_pct=chosen[0]*100, target_pct=chosen[1]*100, selection_scope=scope,
                         group_training_trades=support,
                         setup_training_trades=int((actual.setup_id.eq(setup)&actual.portfolio_status.eq('ADMITTED')).sum())))
    return policy, pd.DataFrame(rows)


def table(frame):
    return frame.to_markdown(index=False, floatfmt=',.2f') if len(frame) else 'No observations.'


def run(args):
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    spec = dict(schema='V13_V10_G_OPTIONS_5MIN_3LOTS_V1', lots=3, interval_minutes=5,
                baseline=BASELINE, grid=GRID, capital=args.capital, slippage_bps=args.slippage_bps,
                previous_bar_participation=args.participation, train_fraction=.60,
                min_train_global=MIN_GLOBAL_TRAIN, min_train_group=MIN_GROUP_TRAIN,
                score='mean net premium return minus one standard error; unresolved=-full premium and buy fees',
                evidence='EXPLORATORY_REUSED_G_HISTORY_OPTION_EXIT_CHRONOLOGICAL_SPLIT',
                costs_source='https://zerodha.com/charges', costs_checked='2026-09-14')
    write_json(output/'research_spec.json', spec)
    if args.frozen_input:
        frozen = args.frozen_input.resolve()
        frozen_info = json.loads((frozen/'inputs.json').read_text(encoding='utf-8'))
        for name, digest in frozen_info['artifacts'].items():
            if sha(frozen/name) != digest:
                raise RuntimeError(f'Frozen option input drift: {name}')
        signals = pd.read_parquet(frozen/'signals.parquet')
        mapped = pd.read_parquet(frozen/'mapped.parquet')
        paths = {p.stem:pd.read_parquet(p) for p in (frozen/'paths').glob('*.parquet')}
        days, signal_sources, data_sources = frozen_info['days'], frozen_info['signal_sources'], frozen_info['data_sources']
    else:
        signals, days, signal_sources = make_signals()
        mapped, paths, data_sources = map_and_load(signals, extra_root=args.extra_data_root)
    frozen = output/'frozen_input'
    (frozen/'paths').mkdir(parents=True, exist_ok=True)
    signals.to_parquet(frozen/'signals.parquet',index=False)
    mapped.to_parquet(frozen/'mapped.parquet',index=False)
    for tid, candles in paths.items():
        candles.to_parquet(frozen/'paths'/f'{tid}.parquet',index=False)
    write_json(frozen/'inputs.json',dict(days=days,signal_sources=signal_sources,data_sources=data_sources,
        artifacts={str(p.relative_to(frozen)):sha(p) for p in frozen.rglob('*.parquet')}))
    signals.to_csv(output/'g_signal_eligibility.csv', index=False)
    mapped.to_csv(output/'option_mapping_and_coverage.csv', index=False)
    build_fetch_plan(mapped).to_csv(output/'missing_option_fetch_plan.csv', index=False)
    supported = sorted(set(str(row['day']) for row in mapped.to_dict('records')
                           if row['trade_id'] in paths and not paths[row['trade_id']].empty
                           and str(row.get('mapping_status','')).startswith('MAPPED_')))
    if not supported:
        raise RuntimeError('No mapped option history; see saved coverage and fetch plan.')
    train_days = max(1, min(len(supported)-1, math.ceil(.60*len(supported))))
    cutoff = supported[train_days-1]
    spec.update(train_cutoff=cutoff, option_data_sessions=supported)
    write_json(output/'research_spec.json', spec)
    sweeps, summaries = {}, []
    for i, pair in enumerate(GRID, 1):
        frame, _, _, _ = replay(mapped, paths, {'default': pair}, capital=args.capital,
                                 slippage_bps=args.slippage_bps, participation=args.participation)
        sweeps[pair] = frame
        for period, sub in [('ALL_DESCRIPTIVE', frame), ('TRAIN', frame.loc[frame.day.le(cutoff)]),
                            ('CHRONOLOGICAL_EVALUATION', frame.loc[frame.day.gt(cutoff)])]:
            summaries.append(dict(stop_pct=pair[0]*100,target_pct=pair[1]*100,period=period,
                                  selection_score=score(sub), **metrics(sub)))
        if i % 5 == 0 or i == len(GRID):
            print(f'[SWEEP] {i}/{len(GRID)}', flush=True)
    sweep_summary = pd.DataFrame(summaries)
    sweep_summary.to_csv(output/'stop_target_grid.csv', index=False)
    setups = sorted(signals.setup_id.unique())
    policy, trained_slot_rules = choose_profile(sweeps, cutoff, setups)
    write_json(output/'train_selected_challenger_profile.json', {**spec, 'policy': policy})
    challenger, challenger_bars, _, challenger_events = replay(mapped, paths, policy, capital=args.capital,
                                          slippage_bps=args.slippage_bps, participation=args.participation)
    challenger.to_csv(output/'challenger_options_trades.csv',index=False)
    challenger_bars.to_csv(output/'challenger_5min_bar_audit.csv',index=False)
    challenger_events.to_csv(output/'challenger_premium_cash_events.csv',index=False)
    trained_slot_rules.to_csv(output/'train_selected_setup_rules.csv',index=False)
    # The predeclared control is always the primary ledger. The train-selected
    # challenger retains its own complete record, whether evaluation wins or loses.
    baseline_policy = {'default':BASELINE, **{s:BASELINE for s in setups}}
    write_json(output/'selected_option_profile.json', {**spec,'policy':baseline_policy,
        'role':'PREDECLARED_BASELINE_FOR_FURTHER_TESTING_NOT_VALIDATED_LIVE',
        'challenger_policy_file':'train_selected_challenger_profile.json'})
    frame, bars, capacity, events = replay(mapped, paths, baseline_policy, capital=args.capital,
                                          slippage_bps=args.slippage_bps, participation=args.participation)
    if not bars.empty:
        bars = bars.merge(frame[['trade_id','portfolio_status']], on='trade_id',how='left',validate='many_to_one')
    slot_rules = trained_slot_rules.copy()
    slot_rules['stop_pct'],slot_rules['target_pct'] = BASELINE[0]*100,BASELINE[1]*100
    slot_rules['selection_scope'] = 'PREDECLARED_BASELINE'
    frame['evaluation_period'] = np.where(frame.day.le(cutoff), 'TRAIN', 'CHRONOLOGICAL_EVALUATION')
    frame.to_csv(output/'options_trades.csv', index=False)
    bars.to_csv(output/'options_5min_bar_audit.csv', index=False)
    events.to_csv(output/'premium_cash_events.csv', index=False)
    baseline = sweeps[BASELINE]
    baseline.to_csv(output/'baseline_options_trades.csv', index=False)
    comparison = []
    for name, f in [('BASELINE_17.5_SL_22.5_TARGET', baseline), ('TRAIN_SELECTED_CHALLENGER', challenger)]:
        for period, sub in [('ALL_AVAILABLE', f), ('TRAIN', f.loc[f.day.le(cutoff)]),
                            ('CHRONOLOGICAL_EVALUATION', f.loc[f.day.gt(cutoff)]),
                            ('ASOF_METADATA_ONLY', f.loc[f.mapping_status.eq('MAPPED_CAUSAL')])]:
            comparison.append(dict(profile=name, period=period, **metrics(sub)))
    comparison = pd.DataFrame(comparison)
    comparison.to_csv(output/'comparison_summary.csv', index=False)
    daily = pd.DataFrame([dict(day=d, option_data_present=d in supported, **metrics(frame.loc[frame.day.eq(d)])) for d in days])
    daily['cumulative_closed_net_pnl'] = daily.net_pnl.cumsum()
    daily.to_csv(output/'options_daily.csv', index=False)
    monthly = pd.DataFrame([dict(month=m, **metrics(frame.loc[frame.day.str.startswith(m)])) for m in sorted({d[:7] for d in days})])
    monthly.to_csv(output/'options_monthly.csv', index=False)
    from fno_v13_v10_g_options_chart import render
    render(output)
    slot_results = pd.DataFrame([dict(setup_id=s, **metrics(frame.loc[frame.setup_id.eq(s)])) for s in setups])
    slot_rules = slot_rules.merge(slot_results, on='setup_id')
    slot_rules.to_csv(output/'setup_sl_target_results.csv', index=False)
    stress = []
    for bps in sorted(set([args.slippage_bps,25.,50.])):
        for participation in sorted(set([args.participation,.25,1.])):
            f, _, _, _ = replay(mapped, paths, baseline_policy, capital=args.capital, slippage_bps=bps, participation=participation)
            stress.append(dict(slippage_bps=bps,previous_volume_participation=participation, **metrics(f)))
    stress = pd.DataFrame(stress)
    stress.to_csv(output/'execution_sensitivity.csv', index=False)
    admitted = frame.loc[frame.portfolio_status.eq('ADMITTED')]
    skips = frame.loc[~frame.portfolio_status.eq('ADMITTED')].copy()
    skips['exclusion'] = np.where(skips.portfolio_status.eq('REJECTED_PREMIUM_CAPITAL'), skips.portfolio_status, skips.reason)
    skip_counts = skips.exclusion.value_counts().rename_axis('reason').reset_index(name='orders')
    headline = metrics(frame)
    summary = {**headline, **capacity, 'train_cutoff': cutoff, 'signal_sessions':len(days),
               'option_data_sessions':len(supported), 'policy':baseline_policy,'challenger_policy':policy,
               'mapping_counts':mapped.mapping_status.value_counts().to_dict()}
    write_json(output/'summary.json', summary)
    base_eval = metrics(frame.loc[frame.day.gt(cutoff)])
    challenger_eval = metrics(challenger.loc[challenger.day.gt(cutoff)])
    decision = ('Do not promote the train-selected challenger: it underperformed the predeclared baseline on later dates.'
                if challenger_eval['net_pnl_full_premium_loss_bound'] < base_eval['net_pnl_full_premium_loss_bound']
                else 'The train-selected challenger needs additional prospective data before any live promotion.')
    decision += ' Keep the predeclared baseline for further paper testing; this small reused sample does not establish an optimal live stop/target.'
    report = [
        '# V13-V10-G ATM CE/PE options: three lots, five-minute replay', '',
        f"**Closed modeled net: Rs{headline['net_pnl']:,.2f}; {headline['closed']} closed trades; {headline['unresolved']} entered trades with unresolved exits.** "
        f"Source G has {len(signals)} selected orders across {len(days)} observed sessions. Actual mapped option history covers {len(supported)} sessions ({supported[0]} to {supported[-1]}).", '',
        '**Provisional paper-test rule: 17.5% premium stop and 22.5% premium target, all three lots.** '+decision, '',
        '![Baseline and training-selected challenger](OPTIONS_RESULTS_COMPARISON.png)', '',
        'This is an options adaptation of the retained G signals. It is not the stock result multiplied by three, and it is not a full-history or one-year options result. '
        'The source G configuration was already selected on this history; the chronological options evaluation is not an untouched test of the complete strategy.', '',
        '## Execution rules', '',
        '- Preserve G five-minute selection, one-minute confirmation and subsequent underlying trigger. Seven untriggered orders remain unfilled.',
        '- Buy ATM CE for LONG and ATM PE for SHORT. Choose the closest strike to the exact completed underlying minute close at option entry; ties go to the lower strike. Freeze the contract through exit.',
        '- Enter at the first five-minute bar open at or after the underlying trigger minute has completed. Equal-boundary entry assumes zero additional latency; no enclosing-bar open is used. All timestamps are IST.',
        '- Buy exactly three exchange lots. No fractional lots, fivefold leverage, premium compounding or automatic resizing. All three lots use one stop and one target; manually scheduled exit is 15:15 open.',
        '- Check protective orders on every five-minute candle. Open gaps are resolved first, stops gap through at the adverse open, and bars touching both levels use stop first. Target is a resting limit; its modeled fill is at its tick-rounded limit.',
        f'- Require three-lot quantity to be at most {args.participation:.0%} of the previous completed option candle volume. Realized entry/exit candle capacity breaches are flagged, not used to select winners. This is an OHLC fill model, not proof of available bid/ask depth.',
        '- Entry bars must print at least the entire three-lot quantity; exits lacking the required total printed volume remain unresolved (an entry-bar exit requires both buy and sell quantities). Held-path gaps also leave an entered trade unresolved with premium reserved and fees recorded. Unresolved trades are penalized as total premium losses during parameter selection.',
        f'- Reserve actual premium plus entry fees from Rs{args.capital:,.0f} cash. Reuse proceeds only when the exit is observable; intrabar exits release cash at bar end. No separate stock margin allocation is applied.',
        f'- Base adverse market-fill slippage: {args.slippage_bps:g} bps per side. Costs include Rs20 per executed order, sell STT, exchange fees, SEBI, GST, stamp duty and IPFT using the [Zerodha schedule checked September 14, 2026](https://zerodha.com/charges). Tax rounding and historical rate changes beyond the frozen schedule are not reconstructed.', '',
        '## How stops and targets were selected', '',
        f'The predeclared baseline is 17.5% premium SL / 22.5% premium target. {len(GRID)} fixed pairs were compared. '
        f'Training ends {cutoff}; later option-data sessions are evaluated after freezing the profile. '
        'Training-only score is mean net return on entry premium minus one standard error. At least eight entered training trades over four days are required for a global selection. '
        'A CE/PE-specific or individual five-minute setup override needs twelve training trades over four days; sparse groups inherit the global profile or baseline. '
        'The grid file includes descriptive full-sample rankings, which must not be read as prospective performance. '
        'The main trade/bar ledgers always show the predeclared baseline; separate challenger files preserve the training-selected experiment.', '',
        f"Training selected {policy['default'][0]*100:g}% SL / {policy['default'][1]*100:g}% target. Later-date net was Rs{challenger_eval['net_pnl']:,.2f} "
        f"versus Rs{base_eval['net_pnl']:,.2f} for the baseline. {decision}", '',
        table(slot_rules[['setup_id','option_type','stop_pct','target_pct','selection_scope','setup_training_trades','closed','wins','net_pnl']]), '',
        '## Results and chronological evaluation', '',
        table(comparison[['profile','period','closed','unresolved','win_rate_pct','profit_factor','net_pnl','daily_realized_drawdown']]), '',
        f"Peak reserved premium plus entry fees: Rs{capacity['peak_reserved_premium_and_fees']:,.2f}. Capital rejections: {capacity['capital_rejections']}. "
        f"Unresolved premium and fees still reserved: Rs{capacity['unresolved_reserved_premium_and_fees']:,.2f}. "
        f"Closed P&L plus full loss of unresolved premiums: Rs{headline['net_pnl_full_premium_loss_bound']:,.2f}. "
        'Drawdown above uses daily realized closes; it is not intraday mark-to-market drawdown.', '',
        '## Monthly results', '',
        table(monthly[['month','closed','wins','losses','win_rate_pct','profit_factor','net_pnl','costs']]), '',
        '## Coverage and excluded orders', '', table(skip_counts), '',
        table(mapped.groupby('mapping_status').size().rename('orders').reset_index()), '',
        'Historical dated instrument masters are preferred. MAPPED_RETROSPECTIVE_METADATA means the same required monthly expiry was reconstructed from a later snapshot; '
        'listing/strike-universe and lot-size history are not independently proved for those dates. These rows are explicitly separated from ASOF_METADATA_ONLY above. '
        'Missing expired options are not replaced by September contracts. [Kite documents that expired option history is unavailable](https://kite.trade/forum/discussion/3493/historical-data-for-expired-f-o-tokens). '
        'A zero in an uncovered session means no measurable options P&L, not a verified no-trade day.', '',
        '## Execution sensitivity with the baseline fixed', '',
        table(stress[['slippage_bps','previous_volume_participation','closed','unresolved','win_rate_pct','net_pnl','entry_capacity_breaches','exit_capacity_breaches']]), '',
        '## Trade ledger', '',
        table(admitted[[c for c in ['day','setup_id','option_symbol','lot_size','quantity','entry_ts','entry_price','stop_price','target_price','exit_ts','exit_price','reason','net_pnl','status'] if c in admitted]]), '',
        '## Files and replay', '',
        '- `options_trades.csv`: every original order, exact contract/quantity/entry/SL/target/exit/costs/status.',
        '- `options_5min_bar_audit.csv`: each monitored candle with protective levels and execution decisions.',
        '- `option_mapping_and_coverage.csv`, `missing_option_fetch_plan.csv`: mapping evidence and missing history.',
        '- `options_daily.csv`, `options_monthly.csv`, `setup_sl_target_results.csv`: daily, monthly and setup results.',
        '- `stop_target_grid.csv`, `selected_option_profile.json`, `comparison_summary.csv`: parameter study and baseline rules.',
        '- `challenger_options_trades.csv`, `challenger_5min_bar_audit.csv`, `train_selected_challenger_profile.json`: the training-selected experiment.',
        '- `premium_cash_events.csv`, `execution_sensitivity.csv`, `manifest.json`: accounting, stresses and source hashes.', '',
        f'Replay the hashed inputs: `python -B fno_v13_v10_g_options_backtest.py --frozen-input "{frozen}" --output-dir "{output.parent / (output.name + "_replay")}"`.', '',
        'No live strategy, task scheduler or brokerage order configuration was changed.',
    ]
    audit_path = output/'validation/independent_audit.json'
    if audit_path.exists():
        audit = json.loads(audit_path.read_text(encoding='utf-8'))
        if audit.get('passed') and all(sha(output/name)==digest for name,digest in audit['audited_ledger_sha256'].items()):
            report.extend(['', '## Verification', '',
                f"Recorded automated tests: {audit['automated_tests']['passed']} passed. Independent ledger audit: {audit['checks_passed']} checks passed; "
                f"{audit['source_records_verified']} source hashes verified. All audited ledger hashes still match. "
                '[Audit details](validation/independent_audit.json).'])
    (output/'V13_V10_G_OPTIONS_3LOTS_RESULTS.md').write_text('\n'.join(report)+'\n',encoding='utf-8')
    manifest = dict(complete=True, specification=spec, signal_sources=signal_sources, data_sources=data_sources,
                    code_sources={str(p.resolve()):sha(p) for p in Path(__file__).parent.glob('fno_v13_v10_g_options*.py')},
                    artifacts={str(p.relative_to(output)):sha(p) for p in sorted(output.rglob('*')) if p.is_file() and p != output/'manifest.json'})
    write_json(output/'manifest.json',manifest)
    print(json.dumps(json_value(summary),indent=2),flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,default=DEFAULT_OUTPUT)
    parser.add_argument('--extra-data-root',type=Path)
    parser.add_argument('--frozen-input',type=Path,help='Replay the saved, SHA-256 checked normalized input bundle')
    parser.add_argument('--capital',type=float,default=1_500_000.)
    parser.add_argument('--slippage-bps',type=float,default=10.)
    parser.add_argument('--participation',type=float,default=.10)
    args = parser.parse_args()
    if not np.isfinite(args.capital) or args.capital <= 0:
        parser.error('capital must be positive and finite')
    run(args)


if __name__ == '__main__':
    main()
