"""Reports comparing trailing hypotheses with the user-adopted 1.50% baseline."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.loss_control_20261106.reporting import (
    ADDITIVE, compare_days, summarize_days, trade_effects, _day_table, _metrics_table)

BASELINE = 'fixed_1p50'


def _validate_changes(changes, daily, full):
    if set(changes.policy) != set(daily.policy):
        raise ValueError('Trade-effect policies do not cover every daily policy')
    baseline = changes.loc[changes.policy.eq(BASELINE)].set_index('sid').sort_index()
    if baseline.empty or not baseline.index.is_unique:
        raise ValueError('Missing or duplicate fixed1.50 baseline identities')
    if not np.allclose(baseline.baseline_pnl_rs, baseline.candidate_pnl_rs, atol=1e-7, rtol=0):
        raise ValueError('The adopted baseline comparison is not self-identical')
    for name, frame in changes.groupby('policy'):
        part = frame.set_index('sid').sort_index()
        if not part.index.is_unique or not part.index.equals(baseline.index):
            raise ValueError('Candidate entry identities changed: ' + name)
        if not part.day.equals(baseline.day):
            raise ValueError('Candidate entry dates changed: ' + name)
        if not np.allclose(part.baseline_pnl_rs, baseline.baseline_pnl_rs, atol=1e-7, rtol=0):
            raise ValueError('Candidate references a different baseline: ' + name)
        if 'pnl_change_rs' in part and not np.allclose(part.candidate_pnl_rs-part.baseline_pnl_rs,
                                                     part.pnl_change_rs, atol=1e-7, rtol=0):
            raise ValueError('Per-trade P&L changes do not reconcile')
        if not np.isclose(part.candidate_pnl_rs.sum(), full.loc[name, 'net_pnl_rs'], atol=1e-5, rtol=0):
            raise ValueError('Trade outcomes do not reconcile with full totals: ' + name)
        actual = dict(trades=len(part), wins=int(part.candidate_pnl_rs.gt(0).sum()),
            losses=int(part.candidate_pnl_rs.lt(0).sum()),
            gross_loss_rs=float(-part.candidate_pnl_rs.clip(upper=0).sum()))
        for field, value in actual.items():
            if not np.isclose(value, full.loc[name, field], atol=1e-5, rtol=0):
                raise ValueError('Trade outcome distribution differs from full totals: ' + name + '/' + field)
    effects = trade_effects(changes)
    for row in effects.itertuples(index=False):
        if row.compared_trades != int(full.loc[BASELINE, 'trades']):
            raise ValueError('Trade-effect count differs from the adopted baseline')
        if not np.isclose(row.total_pnl_change_rs, full.loc[row.policy, 'net_pnl_rs']-full.loc[BASELINE, 'net_pnl_rs'],
                          atol=1e-5, rtol=0):
            raise ValueError('Trade effects do not reconcile with baseline differences')
    return effects


def _flags(values):
    text = values.astype(str).str.strip().str.lower()
    if not text.isin(['true', 'false']).all():
        raise ValueError('Invalid diagnostic Boolean field')
    return text.eq('true')


def diagnostics(changes):
    rows = []
    for name, frame in changes.groupby('policy', sort=False):
        item = dict(policy=name, trades=len(frame))
        for key in ('activation_count', 'stop_update_count'):
            if key in frame:
                values = pd.to_numeric(frame[key], errors='raise')
                if not np.isfinite(values).all() or values.lt(0).any():
                    raise ValueError('Invalid nonnegative diagnostic count')
                item[key] = int(values.sum())
                item['trades_with_' + key] = int(values.gt(0).sum())
        for key in ('exit_changed', 'premature_exit_then_target'):
            if key in frame:
                item[key] = int(_flags(frame[key]).sum())
        if 'target_mode' in frame:
            item['target_modes'] = ', '.join(sorted(frame.target_mode.astype(str).unique()))
        for key in ('session_mfe_pct', 'session_mfe_capture_ratio'):
            if key in frame:
                values = pd.to_numeric(frame[key], errors='coerce').replace([np.inf, -np.inf], np.nan)
                item['mean_' + key] = float(values.mean()) if values.notna().any() else None
        rows.append(item)
    return pd.DataFrame(rows)


def _number(row, field, decimals=2):
    value = row.get(field)
    return f'{float(value):,.{decimals}f}' if value is not None and np.isfinite(value) else 'n/a'


def write_reports(output, overall, daily, changes, config):
    """Render reconciled reports; this function never selects or refits a policy.

    Required config: baseline_policy='fixed_1p50', selected_policy,
    training_candidate and promotion_pass. Optional comparison_candidate is
    the exploratory arm. Baseline adoption is independent of promotion.
    """
    output = Path(output)
    if config.get('baseline_policy') != BASELINE:
        raise ValueError('The report must use the user-adopted fixed1.50 baseline')
    selected = config['selected_policy']
    training = config['training_candidate'] or BASELINE
    diagnostic = config.get('comparison_candidate') or training
    promoted = config['promotion_pass']
    if not isinstance(promoted, (bool, np.bool_)):
        raise ValueError('Promotion status must be Boolean')
    if not promoted and selected != BASELINE:
        raise ValueError('Failed promotion cannot activate a trailing alternative')
    if promoted and (selected == BASELINE or selected != training):
        raise ValueError('Promoted policy must be the fitting-period candidate')
    required = {BASELINE, selected, training, diagnostic}
    if not required.issubset(set(daily.policy)):
        raise ValueError('Missing adopted baseline or comparison policy')
    if daily.duplicated(['policy', 'day']).any() or overall.duplicated(['policy', 'period']).any():
        raise ValueError('Duplicate policy/day or policy/period rows')
    baseline = daily.loc[daily.policy.eq(BASELINE)].sort_values('day')
    days = baseline.day.tolist()
    if not days or config.get('historydays', days) != days:
        raise ValueError('Daily history differs from the declared inventory')
    full = overall.loc[overall.period.eq(f'full_{len(days)}')].set_index('policy')
    if set(full.index) != set(daily.policy):
        raise ValueError('Full and daily policy inventories differ')
    for name, part in daily.groupby('policy'):
        compare_days(baseline, part)
        values = summarize_days(part)
        for field in (*ADDITIVE, 'win_rate_pct', 'daily_close_drawdown_rs'):
            if not np.isclose(values[field], full.loc[name, field], atol=1e-5, rtol=0):
                raise ValueError('Daily/full reconciliation failed: ' + name + '/' + field)
    effects = _validate_changes(changes, daily, full)
    diag = diagnostics(changes)
    current = compare_days(baseline, daily.loc[daily.policy.eq(selected)])
    candidate = compare_days(baseline, daily.loc[daily.policy.eq(diagnostic)])
    output.mkdir(parents=True, exist_ok=True)
    current.to_csv(output/'day_wise_results.csv', index=False)
    candidate.to_csv(output/'candidate_day_wise_results.csv', index=False)
    effects.to_csv(output/'trade_effect_summary.csv', index=False)
    diag.to_csv(output/'diagnostics.csv', index=False)
    current_note = ('**The comparison baseline is the user-adopted fixed 1.50% SL**, with original targets '
        f'and no timed tightening. Current selected policy: **{selected}**. ')
    current_note += ('The trailing candidate passed the declared retrospective checks.' if promoted else
        'No trailing alternative qualified; the adopted fixed 1.50% baseline remains selected.')
    candidate_note = (f'Comparison policy: **{diagnostic}**, measured against **fixed 1.50% SL**. '
        + ('This is the selected research result.' if diagnostic == selected else
           '**Exploratory comparison only; not the selected current policy.**'))
    (output/'DAY_WISE_RESULTS.md').write_text(_day_table(current,
        'V23 current day-wise results — fixed 1.50% comparison baseline', current_note), encoding='utf-8')
    (output/'CANDIDATE_DAY_WISE_RESULTS.md').write_text(_day_table(candidate,
        'V23 trailing candidate day-wise comparison', candidate_note), encoding='utf-8')
    lines = [f'# V23 fixed 1.50% baseline and trailing study through {days[-1]}', '',
        '**The fixed 1.50% SL is now the V23 backtesting baseline, as requested by the user.** '
        'That adoption does not depend on whether a new trailing alternative qualifies. The baseline '
        'keeps the original per-setup target and the 15:15 time exit, with no automatic two-hour '
        'tightening. LONG stop = actual entry × 0.985; SHORT stop = actual entry × 1.015.', '',
        f'All **{int(full.loc[BASELINE,"trades"])} entries across {len(days)} completed sessions** '
        f'from {days[0]} through {days[-1]} remain. Baseline net profit after modelled costs is '
        f'**Rs {full.loc[BASELINE,"net_pnl_rs"]:,.2f}**. Entry identities, fill prices, times and '
        'exposure are preserved in every comparison.', '']
    if promoted:
        lines += [f'**Qualified trailing candidate: {selected}.** It passed the declared first44 '
            'selection and next15, final7 and full-history checks against the adopted 1.50% baseline. '
            'This is historical evidence, not a guarantee of future profit or unchanged individual winners.', '']
    else:
        lines += ['**No trailing alternative qualified for replacement.** The adopted fixed 1.50% '
            'baseline remains the current V23 backtest policy. Alternatives are retained as research '
            'comparisons; no improvement is claimed merely from selecting a different benchmark.', '']
    if config.get('rejection_reasons'):
        lines += ['Recorded qualification checks: ' + '; '.join(map(str, config['rejection_reasons'])) + '.', '']
    lines += ['## Full-history comparison', ''] + _metrics_table([(name, row) for name, row in full.iterrows()])
    lines += ['', 'Loss amount is the absolute sum of negative after-cost trade P&L before offsetting '
        'winners. Flat trades are neither wins nor losses; win rate uses all fills. Daily-close drawdown '
        'is not intraday drawdown.', '', '## Expectancy and payoff', '',
        '| Policy | Mean net/trade (Rs) | Median net/trade (Rs) | Profit factor | Mean winner (Rs) | Mean losing amount (Rs) |',
        '|:--|--:|--:|--:|--:|--:|']
    for name, row in full.iterrows():
        lines.append(f'| {name} | {_number(row,"average_trade_rs")} | {_number(row,"median_trade_rs")} | '
            f'{_number(row,"profit_factor")} | {_number(row,"average_win_rs")} | {_number(row,"average_loss_rs")} |')
    lines += ['', '## What is being changed', '',
        '- **Target-removal control:** `fixed_1p50_runner` keeps the 1.50% stop and removes the native '
        'profit target from entry. It measures the effect of holding for the rest of the session '
        'without a target. Other runner models also remove the native target from entry; a target '
        'is never cancelled retrospectively after it was hit.',
        '- **Native-target control:** `hybrid_native_target` retains the original target while adding '
        'hybrid management. Original targets below 1R can exit before the 1R breakeven stage activates. '
        'Activation counts below make an inactive or rarely active control visible.',
        '- **R definition:** fixed-risk arms start with R = 1.50% of actual entry. The adaptive hybrid '
        'uses its own initial stop distance as R. The latter changes initial risk as well as trade '
        'management, so it is a separate combined experiment, not evidence about trailing alone.',
        '- **Cost-covered breakeven:** a +0.05% entry-relative stop offsets the modelled 5-bps '
        'round-trip cost at an exact fill. Gaps or additional slippage can still produce a loss.',
        '- **Causal features:** ATR14 uses the completed execution-path true-range prefix, with '
        'confirmed entry ATR during warm-up. EMA indicators are confirmation-path anchored; they '
        'are not asserted to be full-session or overnight indicators. A swing pivot needs two left '
        'and two completed right bars, and its protective stop starts only after confirmation.',
        '- **Execution timing:** stage triggers use completed post-entry closes. Entry-bar extrema '
        'cannot activate a trail. Stops ratchet only in the favorable direction and new decisions '
        'apply from the next bar. All positions still exit by 15:15.', '']
    for name, definition in config.get('definitions', {}).items():
        description = definition if isinstance(definition, str) else json.dumps(definition, sort_keys=True)
        lines.append(f'- **{name}:** {description}')
    lines += ['', '## Period checks', '',
        f'Fitting-period candidate: **{training}**. Current policy: **{selected}**. Diagnostic '
        f'comparison: **{diagnostic}**. Selection uses only the first44 sessions through 5 October: '
        'net profit at least the fixed1.50 baseline, lower total loss amount and no worse daily-close '
        'drawdown. The one selected candidate is checked separately on the next15 sessions through '
        '27 October, final7 through 5 November, and the full history. A failed candidate is not '
        'replaced using later results. All periods were previously observed in research and are '
        'retrospective checks, not an untouched forward holdout.', '']
    wanted = list(dict.fromkeys([BASELINE, training, selected, diagnostic]))
    for period, label in [('fit_44','First44: selection'), ('validation_15','Next15: validation'),
                          ('later_7','Final7: later check'), ('nov05','5 November only')]:
        part = overall.loc[overall.period.eq(period) & overall.policy.isin(wanted)]
        if not part.empty:
            lines += ['### ' + label, ''] + _metrics_table([(r.policy, r._asdict()) for r in part.itertuples(index=False)]) + ['']
    lines += ['## Original winners and losers', '',
        'The labels below are post-replay diagnostics against the fixed1.50 baseline. They never '
        'determine which individual trade receives a trailing rule.', '',
        '| Policy | Baseline winners worsened | Baseline losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|']
    for row in effects.itertuples(index=False):
        lines.append(f'| {row.policy} | {row.baseline_winners_harmed} | {row.baseline_losers_improved} | '
            f'{row.winners_turned_losses} | {row.losers_turned_wins} | {row.winner_pnl_change_rs:+,.2f} | '
            f'{row.loser_pnl_change_rs:+,.2f} |')
    lines += ['', '## Activation and exit diagnostics', '',
        '| Policy | Trades with activation | Stop updates | Changed exits | Exit followed by later native-target touch | Target mode |',
        '|:--|--:|--:|--:|--:|:--|']
    for _, row in diag.iterrows():
        lines.append(f'| {row.policy} | {_number(row,"trades_with_activation_count",0)} | '
            f'{_number(row,"stop_update_count",0)} | {_number(row,"exit_changed",0)} | '
            f'{_number(row,"premature_exit_then_target",0)} | {row.get("target_modes","n/a")} |')
    if 'mean_session_mfe_capture_ratio' in diag:
        lines += ['', '| Policy | Mean session MFE upper bound | Mean opportunity capture |',
                  '|:--|--:|--:|']
        for _, row in diag.iterrows():
            capture = row.get('mean_session_mfe_capture_ratio')
            shown = f'{100*capture:.2f}%' if capture is not None and np.isfinite(capture) else 'n/a'
            lines.append(f'| {row.policy} | {_number(row,"mean_session_mfe_pct")}% | {shown} |')
        if config.get('mfe_definition'):
            lines += ['', str(config['mfe_definition'])]
    lines += ['', 'A later original-target touch is only a retrospective diagnostic. It does not establish '
        'that a trade could safely have waited through the intervening adverse prices. Session MFE '
        'and any capture ratios use future prices only after the replay for research reporting; they '
        'are never inputs to an entry or exit decision. One-minute OHLC gives bounds rather than '
        'exact intrabar opportunity capture. Full diagnostic fields are saved in [diagnostics.csv](diagnostics.csv).', '',
        f'The diagnostic candidate increases daily net P&L on {int(candidate.pnl_change_rs.gt(1e-7).sum())} days '
        f'and decreases it on {int(candidate.pnl_change_rs.lt(-1e-7).sum())} days. Every session remains '
        'listed, including unchanged and zero-trade sessions.', '', '## Scope and limits', '',
        '- Fixed exposure Rs500,000 per trade, Rs100,000 capital at5x, unlimited concurrent capital '
        'and Rs250 modelled round-trip cost per fill. No extra leverage, entries or partial exits '
        'are used to manufacture an improvement. Extra changed-exit slippage is reported separately.',
        '- The 1.50% initial stop permits Rs7,500 nominal price loss before Rs250 costs per fixed-size '
        'trade; an adverse opening gap can exceed that amount. The adaptive arm has a separately '
        'declared lower initial-risk range and a 1.50% invalid-structure fallback.',
        '- All73 configured pairs through14:00 remain enabled, with final confirmation at14:01. '
        'Original pairs retain cap3; new pairs retain cap1 and their existing volume gate.',
        '- Inherited conservative entry-bar stop-first and adverse-open fills remain. One-minute '
        'OHLC does not reveal true intrabar ordering, bid/ask fills or queue position.',
        '- The frozen paths contain OHLC rather than a verified volume/order-flow/VIX series. '
        'This study does not claim to implement VWAP, CVD, OFI or VIX-based management from those paths.',
        '- LIVE execution, approvals, scheduling and broker order handling are unchanged. No claim '
        'about broker-native trailing-stop support is needed for this offline backtest.']
    lines += ['- ' + str(note) for note in config.get('coverage_notes', [])]
    lines += ['', '[Current day-wise results](DAY_WISE_RESULTS.md) · [Current CSV](day_wise_results.csv) · '
        '[Candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · [Candidate CSV](candidate_day_wise_results.csv)', '',
        '[All totals](overall.csv) · [All daily rows](daily.csv) · [Trade changes](trade_changes.csv) · '
        '[Trade effect summary](trade_effect_summary.csv) · [Stop updates](stop_updates.csv) · '
        '[Extra-slippage sensitivity](cost_stress.csv) · [Adopted baseline trades](baseline_trades.csv) · '
        '[Selected trades](selected_trades.csv) · [Configuration](config.json)', '']
    (output/'REPORT.md').write_text('\n'.join(lines), encoding='utf-8')
