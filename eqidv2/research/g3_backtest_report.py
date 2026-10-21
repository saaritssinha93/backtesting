"""Full-history G3 summaries using the actual portfolio execution ledger."""
from datetime import date
import hashlib
from html import escape
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from research.g3_backtest_profile import contract, production
from research.g3_0920_backtest import _stats


def tables(ledger, days, *, profile=None):
    profile = profile or contract
    added_ids = {spec.setup_id for spec in profile.ACTIVE_SETUPS
                 if spec.setup_id in production.afternoon_setups.SETUP_IDS}
    dates = [date.fromisoformat(d) for d in days]
    setups = []
    for spec in profile.ACTIVE_SETUPS:
        frame = ledger.loc[ledger.setup_id.eq(spec.setup_id)]
        setups.append(dict(setup_id=spec.setup_id, signal_end=spec.signal_end, side=spec.side,
            target_pct=spec.target_pct, added=spec.setup_id in added_ids, **_stats(frame, dates)))
    months = []
    for month in sorted({d[:7] for d in days}):
        cohort = [d for d in days if d.startswith(month)]
        months.append(dict(month=month, **_stats(ledger.loc[ledger.day.astype(str).isin(cohort)],
                                               [date.fromisoformat(d) for d in cohort])))
    periods = []
    for label, cohort in [('original_first_38', days[:38]), ('original_later_11', days[38:49]),
                           ('new_daily_extensions', days[49:]), ('all', days)]:
        if not cohort:
            continue
        for scope, frame in [('full_g3', ledger), ('selected_additions', ledger.loc[ledger.setup_id.isin(added_ids)])]:
            periods.append(dict(period=label, scope=scope, first_date=cohort[0], last_date=cohort[-1],
                **_stats(frame.loc[frame.day.astype(str).isin(cohort)], [date.fromisoformat(d) for d in cohort])))
    return pd.DataFrame(setups), pd.DataFrame(months), pd.DataFrame(periods)


def _saved_profile(summary):
    """Keep an archived report's inventory tied to its verified saved identity."""
    version, fingerprint = summary.get('strategy_version'), summary.get('strategy_fingerprint')
    if not version or not fingerprint:
        return contract
    if production.supported_identity(version, fingerprint):
        return production
    profile = contract.contract_for_identity(version, fingerprint)
    if profile is not None:
        return profile
    if production.supported_identity(version, fingerprint, allow_legacy=True):
        directory = Path(__file__).resolve().parents[1] / 'configs/strategies'
        for path in directory.glob('v13_v10_g_3_production_v*.json'):
            payload = json.loads(path.read_text(encoding='utf-8'))
            if payload.get('strategy_version') != version:
                continue
            digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'),
                                               allow_nan=False).encode()).hexdigest()
            if digest != fingerprint:
                continue
            specs = tuple(SimpleNamespace(**row,
                setup_id=row['confirmation_end'].replace(':', '') + '_' + row['side'])
                for row in payload['active_setups'])
            return SimpleNamespace(STRATEGY_VERSION=version, ACTIVE_SETUPS=specs,
                                   strategy_payload=lambda: payload)
    return None


def write(output, summary, ledger, daily):
    profile = _saved_profile(summary)
    if profile is None:
        raise ValueError('Cannot render a backtest with an unrecognized strategy identity')
    setups, monthly, periods = tables(ledger, summary['days'], profile=profile)
    setup_count = len(profile.ACTIVE_SETUPS)
    payload = profile.strategy_payload()
    target_cutoff = payload.get('scope') == 'BACKTEST_ONLY'
    disabled_setups = payload.get('disabled_setups', [])
    disabled_count = len(disabled_setups)
    production_count = len(production.ACTIVE_SETUPS)
    exceptions = [s for s in profile.ACTIVE_SETUPS if s.target_pct <= .75]
    exception_text = ', '.join(f'{s.signal_end} {s.side} ({s.target_pct:g}%)' for s in exceptions)
    for name, frame in [('setup_results', setups), ('monthly_results', monthly), ('period_results', periods)]:
        frame.to_csv(output / (name + '.csv'), index=False)
    stats = summary['current_g3']
    page = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 full historical backtest: ' + str(setup_count) + ' setups</title><style>body{font:15px/1.5 system-ui,sans-serif;margin:28px;background:#f5f7fa;color:#20344b}h1,h2{line-height:1.2}table{border-collapse:collapse;background:white;width:100%}td,th{padding:9px;text-align:right;border-bottom:1px solid #d6dfe8;white-space:nowrap}th{background:#20344b;color:white}td:first-child,th:first-child{text-align:left}.scroll{overflow:auto;max-height:600px;margin-bottom:26px}.note{background:#fff0cd;padding:14px}a{color:#075dae}</style></head><body>'
    page += f'<h1>G3 historical backtest: {setup_count} setups</h1><p>{summary["first_date"]} through {summary["last_date"]}; {summary["sessions"]} sessions.</p>'
    page += f'<p>Configuration: {escape(summary.get("strategy_version", profile.STRATEGY_VERSION))}. Execution authority: none.</p>'
    page += f'<p><strong>Net P&amp;L: Rs {stats["net_pnl_rs"]:,.2f}; win rate: {stats["win_rate_pct"]:.2f}%; {stats["executed_trades"]} executed trades.</strong> Wins: {stats["wins"]}; losses: {stats["losses"]}; maximum daily realized drawdown: Rs {stats["max_drawdown_rs"]:,.2f}.</p>'
    rule = (f'{setup_count} setups are enabled: targets >0.75% plus the restored exceptions {exception_text}; '
            f'{disabled_count} remaining setups with target <=0.75% are disabled. ' if exceptions else
            f'all {disabled_count} setups with target <=0.75% are disabled; {setup_count} setups with target >0.75% remain. ')
    inventory = ('Backtest-only configuration: ' + rule + f'PAPER/LIVE retain their {production_count}-setup production configuration. '
        if target_cutoff else f'This saved production configuration contains {setup_count} setups. ')
    page += '<p>' + inventory + 'Confirmation is at the exact next minute. Stops remain 1.25%, tightening to 1% after 120 minutes; square-off is 15:15. Afternoon setups use one highest-liquidity qualifying stock per session.</p>'
    page += '<p>Rs100,000 capital per entry, five-times exposure, Rs1,000,000 portfolio capital; five bps round-trip cost. Slippage, market impact and broker-specific charges beyond this estimate are not modeled. Wins are positive net P&amp;L, including profitable time exits.</p>'
    scope = (f'This historical backtest uses its saved {setup_count}-setup configuration; PAPER/LIVE use {production.STRATEGY_VERSION} with {production_count} setups. '
        if target_cutoff else f'This saved {profile.STRATEGY_VERSION} report contains its full {setup_count}-setup decision rules. ')
    page += '<p class="note">Targets and the selected subset were chosen after inspecting the original historical cohort. The first 49 sessions are historical fit results, not untouched validation. October 1 is excluded because immutable input was incomplete. Opening OI is retrospective. Daily extensions use this fixed backtest configuration and disclose stock-day exclusions. ' + scope + 'LIVE retains its separate Rs10,000 margin / Rs50,000 exposure sizing and dated activation gate.</p>'
    if target_cutoff:
        disabled = pd.DataFrame([dict(setup_id=row['confirmation_end'].replace(':', '') + '_' + row['side'],
            signal_end=row['signal_end'], side=row['side'], target_pct=row['target_pct'])
            for row in disabled_setups])
        page += '<h2>Disabled from backtesting</h2><div class="scroll">' + disabled.to_html(
            index=False, border=0, float_format=lambda v:f"{v:,.2f}") + '</div>'
        if summary.get('comparison_sessions') is not None:
            page += (f'<p>Before-and-after comparison period: {escape(str(summary["comparison_first_date"]))} '
                f'through {escape(str(summary["comparison_last_date"]))}; '
                f'{int(summary["comparison_sessions"])} sessions. Later daily extensions appear in the full '
                'backtest totals and are excluded from this matched-period comparison.</p>')
        for filename, title in [('portfolio_comparison.csv', 'Before and after: portfolio replay'),
                                ('newly_admitted_trades.csv', 'New fills from enabled slots and portfolio allocation changes'),
                                ('daily_comparison.csv', 'Before and after: daywise changes')]:
            if (output / filename).exists():
                comparison = pd.read_csv(output / filename)
                if filename == 'newly_admitted_trades.csv':
                    columns = ['day', 'setup_id', 'side', 'tradingsymbol', 'native_target_pct',
                               'entry_price', 'exit_price', 'portfolio_net_profit_rupees']
                    comparison = comparison[[name for name in columns if name in comparison]]
                page += f'<h2>{escape(title)}</h2><div class="scroll">' + comparison.to_html(
                    index=False, border=0, float_format=lambda v:f"{v:,.2f}") + '</div>'
    setup_cols = ['signal_end','side','target_pct','executed_trades','wins','losses','win_rate_pct','net_pnl_rs']
    metric_cols = ['executed_trades','wins','losses','win_rate_pct','net_pnl_rs','max_drawdown_rs']
    for title, frame in [('Enabled afternoon setups', setups.loc[setups.added, setup_cols]),
            ('Monthly totals', monthly[['month',*metric_cols]]),
            ('Period comparison', periods[['period','scope',*metric_cols]]),
            (f'All {setup_count} enabled setups', setups[setup_cols]), ('Daily totals', daily)]:
        page += f'<h2>{escape(title)}</h2><div class="scroll">{frame.to_html(index=False,border=0,float_format=lambda v:f"{v:,.2f}")}</div>'
    if (output / "setup_comparison.csv").exists():
        comparison = pd.read_csv(output / "setup_comparison.csv")
        page += (f'<h2>Before and after: setup results</h2><p>The retained setup rules and targets are unchanged. Portfolio allocation is replayed with {setup_count} enabled setups and {disabled_count} disabled setups.</p><div class="scroll">'
            if target_cutoff else '<h2>Afternoon native and portfolio comparison</h2><p>G3 uses its own existing book and capital allocation. The approved fifteen filters and targets are unchanged; any G3 qualification differences are disclosed.</p><div class="scroll">')
        page += comparison.to_html(index=False, border=0, float_format=lambda v:f"{v:,.2f}") + '</div>'
    ledger_name = 'portfolio_trades.csv'  if (output / 'portfolio_trades.csv').exists() else 'variant_trades.csv'
    names = ['config.json','summary.json','daily_results.csv',ledger_name,'setup_results.csv',
             'monthly_results.csv','period_results.csv','manifest.json']
    names += [name for name in ('setup_comparison.csv', 'disabled_setups.csv', 'portfolio_comparison.csv',
        'daily_comparison.csv', 'newly_admitted_trades.csv', 'baseline_trades.csv', 'selected_orders.csv',
        'enabled_two_slots_selection.csv')
        if (output / name).exists()]
    page += '<p>' + ' | '.join(f'<a href="{name}">{name}</a>' for name in names) + '</p></body></html>'
    (output / 'report.html').write_text(page, encoding='utf-8')
