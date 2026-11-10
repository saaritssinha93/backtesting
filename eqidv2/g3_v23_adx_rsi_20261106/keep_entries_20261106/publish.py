"""Publish the completed keep-entry study as the current V23 research result."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import run as source
from .daywise import comparison, markdown

PACKAGE = Path(__file__).resolve().parent
EXIT_RESULTS = PACKAGE / 'exit_study/results_through_20261104'
OUTPUT = PACKAGE / 'current_results_through_20261104'


def verify_manifest(folder):
    manifest = json.loads((folder / 'manifest.json').read_text())
    for name, expected in manifest.items():
        path = (folder / name).resolve()
        if not path.is_relative_to(folder.resolve()) or source.sha(path) != expected:
            raise ValueError('Result fingerprint changed: ' + name)


def publish(output=OUTPUT, exit_results=EXIT_RESULTS):
    output = Path(output).resolve()
    exit_results = Path(exit_results).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh result folder inside the V23 keep-entry package')
    original = PACKAGE.parent / 'results_through_20261104'
    risk_results = PACKAGE / 'risk_study/results_through_20261104'
    verify_manifest(original)
    verify_manifest(risk_results)
    if not exit_results.is_relative_to(PACKAGE):
        raise ValueError('Exit results must be inside the V23 keep-entry package')
    verify_manifest(exit_results)
    chosen = json.loads((exit_results / 'config.json').read_text())['selected_policy']
    old_overall = pd.read_csv(original / 'overall.csv')
    old_daily = pd.read_csv(original / 'daily_results.csv')
    exit_overall = pd.read_csv(exit_results / 'overall.csv')
    exit_daily = pd.read_csv(exit_results / 'daily.csv')
    frozen_fills = pd.read_csv(original / 'frozen_inputs/v21_g7_native_trades.csv')
    revised_fills = pd.read_csv(exit_results / 'selected_trades.csv')
    identity = ['sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'entry_ts']
    pd.testing.assert_frame_equal(
        frozen_fills[identity].sort_values('sid').reset_index(drop=True),
        revised_fills[identity].sort_values('sid').reset_index(drop=True), check_exact=True)
    if not np.allclose(frozen_fills.sort_values('sid').entry_price,
                       revised_fills.sort_values('sid').entry_price, atol=1e-10, rtol=0):
        raise ValueError('A native entry price changed beyond CSV round-trip precision')
    baseline = old_daily.loc[old_daily.policy.eq('baseline_v21_g7')]
    revised = exit_daily.loc[exit_daily.policy.eq(chosen)]
    daily = comparison(baseline, revised)
    if len(daily) != 65 or int(daily.trades_v23.sum()) != 771:
        raise ValueError('Current V23 must retain all 771 fills across 65 sessions')
    selected = exit_overall.loc[exit_overall.policy.eq(chosen) & exit_overall.period.eq('full_65')].iloc[0]
    if int(selected.selected_orders) != 871:
        raise ValueError('Current V23 must preserve all 871 G7 selected orders')
    for daily_field, total_field in [('net_pnl_rs_v23', 'net_pnl_rs'), ('trades_v23', 'trades'),
                                    ('wins_v23', 'wins'), ('losses_v23', 'losses'),
                                    ('gross_loss_rs_v23', 'gross_loss_rs')]:
        if not np.isclose(daily[daily_field].sum(), selected[total_field], atol=.01, rtol=0):
            raise ValueError('Published daily result does not reconcile: ' + total_field)
    baseline_summary = old_overall.loc[old_overall.period.eq('full_65') & old_overall.policy.eq('baseline_v21_g7')].iloc[0]
    previous_summary = old_overall.loc[old_overall.period.eq('full_65') & old_overall.policy.eq('adx20_di_rsi')].iloc[0]
    summary = pd.DataFrame([dict(baseline_summary, variant='V21 + G7 baseline'),
                            dict(previous_summary, variant='Previous V23 entry-filter experiment'),
                            dict(selected, variant='Current V23 keep-all-entries')])
    challenger_name = 'protect_mfe0p6_lock0p1'
    challenger_daily = comparison(baseline, exit_daily.loc[exit_daily.policy.eq(challenger_name)])
    challenger = exit_overall.loc[exit_overall.policy.eq(challenger_name) & exit_overall.period.eq('full_65')].iloc[0]
    output.mkdir(parents=True)
    daily.to_csv(output / 'day_wise_results.csv', index=False)
    summary.to_csv(output / 'comparison.csv', index=False)
    title = 'V23 day-wise results — all 771 baseline trades retained'
    (output / 'DAY_WISE_RESULTS.md').write_text(markdown(daily, title), encoding='utf-8')
    challenger_daily.to_csv(output / 'profit_protection_day_wise.csv', index=False)
    challenger_note = ('\nThis is an unselected exit experiment. After a completed post-entry bar reaches '
                       '+0.6% favorable movement, a +0.1% entry-relative stop applies from the next bar. '
                       'The stop can fill worse after a price gap. It preserves all entries but reduces '
                       'total historical profit. It is not the current V23 configuration.\n')
    (output / 'PROFIT_PROTECTION_DAY_WISE.md').write_text(
        markdown(challenger_daily, 'V23 profit-protection experiment — not selected') + challenger_note,
        encoding='utf-8')
    config = {**json.loads((PACKAGE / 'entry_policy.json').read_text()),
              'selected_exit_policy': chosen, 'selected_sizing_policy': 'baseline_fixed_size',
              'exit_results': str(exit_results.relative_to(source.PACKAGE)),
              'daily_trade_count_parity': 'PASS_ALL_65_DAYS',
              'retrospective_only': True, 'live_changed': False,
              'previous_entry_filter': 'ARCHIVED_NOT_CURRENT',
              'current_results': str(output.relative_to(source.PACKAGE))}
    (output / 'config.json').write_text(json.dumps(config, indent=2) + '\n')
    lines = ['# V23 — keep all entries', '',
             '**ADX/RSI entry vetoes are disabled in this revision.** All 871 V21 + G7 selected orders '
             'and 771 native filled trades are retained. The previous 561-trade entry-filter experiment '
             'is archived for comparison and is not the current V23 mode.', '',
             'The old filter removed 72 losing trades and 138 winning trades. Its higher win rate '
             'did not compensate for all of the winners skipped, which is why net profit fell.', '',
             f'Current exit policy: **{chosen}**. Position sizing remains the baseline.', '',
             '| Version | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Gross loss amount (Rs) | Daily-close drawdown (Rs) |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|']
    for r in summary.itertuples():
        lines.append(f'| {r.variant} | {int(r.trades)} | {int(r.wins)} | {int(r.losses)} | '
                     f'{r.win_rate_pct:.2f}% | {r.net_pnl_rs:,.2f} | {r.gross_loss_rs:,.2f} | '
                     f'{r.daily_close_drawdown_rs:,.2f} |')
    if chosen == 'baseline_v21_g7':
        lines += ['', '**No exit overlay met all selection requirements.** The current result therefore '
                  'retains the baseline exits and sizing as well as its entries. No loss reduction is '
                  'claimed for the current mode. The alternative results remain available in the studies.']
    lines += ['', '## Same-trade alternative: profit protection', '',
              f'The +0.6% activation / +0.1% stop experiment retains {int(challenger.trades)} trades, '
              f'with {int(challenger.wins)} wins, {int(challenger.losses)} losses and a '
              f'{challenger.win_rate_pct:.2f}% win rate. Net P&L is Rs {challenger.net_pnl_rs:,.2f}, '
              f'loss amount Rs {challenger.gross_loss_rs:,.2f}, and daily-close drawdown '
              f'Rs {challenger.daily_close_drawdown_rs:,.2f}. It reduces loss amounts and drawdown, '
              'but also cuts profitable trades short. It was not selected because profit falls.', '',
              '[Alternative day-wise comparison](PROFIT_PROTECTION_DAY_WISE.md) · '
              '[Alternative day-wise CSV](profit_protection_day_wise.csv)']
    lines += ['', '## Later six sessions (retrospective)', '',
              '| Policy | Trades | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |',
              '|:--|--:|--:|--:|--:|--:|']
    late = exit_overall.loc[exit_overall.period.eq('later_6') & exit_overall.policy.isin(['baseline_v21_g7', chosen])]
    for r in late.itertuples():
        lines.append(f'| {r.policy} | {int(r.trades)} | {r.win_rate_pct:.2f}% | '
                     f'{r.net_pnl_rs:,.2f} | {r.gross_loss_rs:,.2f} | {r.daily_close_drawdown_rs:,.2f} |')
    lines += ['', '## Day-wise results', '',
              '[All 65 days, formatted table](DAY_WISE_RESULTS.md) · [Day-wise CSV](day_wise_results.csv)', '',
              '## Scope and limits', '',
              '65 sealed sessions from 29 July through 4 November 2026; November 5–6 are not included. '
              'All 73 setup/direction pairs through 14:00 remain configured. Original pairs retain cap 3 '
              'and new pairs cap 1 plus the volume gate. Native entry filters, prices and times are retained.', '',
              'Fixed entry exposure is Rs 500,000 (Rs 100,000 capital at 5x), with unlimited portfolio capital. '
              'These are research results, not LIVE sizing. Entry identities and daily counts are checked '
              'against the baseline. Exit changes use observed prices/entry-time indicators, never future '
              'winner/loser labels. Selection uses the earlier 59 sessions, with later six reported separately. '
              'All history was previously observed, so this is not independent forward validation.', '',
              'Only losing trades cannot be singled out in advance. A causal exit rule can also change '
              'winning trades. Smaller sizing reduces loss amounts without necessarily reducing losing '
              'trade count; the sizing study did not qualify under the profit/loss/drawdown constraints.', '',
              f'[Exit study and later-period results]({Path(os.path.relpath(exit_results / "REPORT.md", output)).as_posix()}) · '
              '[Sizing study](../risk_study/results_through_20261104/REPORT.md) · [Configuration](config.json)', '',
              'LIVE execution, scheduler, approval and position management are unchanged.', '']
    (output / 'REPORT.md').write_text('\n'.join(lines), encoding='utf-8')
    provenance = dict(code_sha256={str(p): source.sha(p) for p in [Path(__file__), PACKAGE / 'daywise.py']},
                      source_manifests={str(p): source.sha(p / 'manifest.json') for p in (original, exit_results, risk_results)})
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    (output / 'manifest.json').write_text(json.dumps({p.name: source.sha(p) for p in output.iterdir()
        if p.is_file()}, indent=2) + '\n')
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    parser.add_argument('--exit-results', type=Path, default=EXIT_RESULTS)
    args = parser.parse_args()
    print(json.dumps(publish(args.output, args.exit_results), indent=2))
