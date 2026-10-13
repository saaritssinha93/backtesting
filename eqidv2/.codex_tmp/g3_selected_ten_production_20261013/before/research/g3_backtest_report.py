"""Full-history G3 summaries using the actual portfolio execution ledger."""
from datetime import date
from html import escape

import pandas as pd

from research.g3_backtest_profile import contract, ADDED_IDS
from research.g3_0920_backtest import _stats


def tables(ledger, days):
    dates = [date.fromisoformat(d) for d in days]
    setups = []
    for spec in contract.ACTIVE_SETUPS:
        frame = ledger.loc[ledger.setup_id.eq(spec.setup_id)]
        setups.append(dict(setup_id=spec.setup_id, signal_end=spec.signal_end, side=spec.side,
            target_pct=spec.target_pct, added=spec.setup_id in ADDED_IDS, **_stats(frame, dates)))
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
        for scope, frame in [('full_g3', ledger), ('selected_additions', ledger.loc[ledger.setup_id.isin(ADDED_IDS)])]:
            periods.append(dict(period=label, scope=scope, first_date=cohort[0], last_date=cohort[-1],
                **_stats(frame.loc[frame.day.astype(str).isin(cohort)], [date.fromisoformat(d) for d in cohort])))
    return pd.DataFrame(setups), pd.DataFrame(months), pd.DataFrame(periods)


def write(output, summary, ledger, daily):
    setups, monthly, periods = tables(ledger, summary['days'])
    for name, frame in [('setup_results', setups), ('monthly_results', monthly), ('period_results', periods)]:
        frame.to_csv(output / (name + '.csv'), index=False)
    stats = summary['current_g3']
    page = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 full historical backtest: 32 setups</title><style>body{font:15px/1.5 system-ui,sans-serif;margin:28px;background:#f5f7fa;color:#20344b}h1,h2{line-height:1.2}table{border-collapse:collapse;background:white;width:100%}td,th{padding:9px;text-align:right;border-bottom:1px solid #d6dfe8;white-space:nowrap}th{background:#20344b;color:white}td:first-child,th:first-child{text-align:left}.scroll{overflow:auto;max-height:600px;margin-bottom:26px}.note{background:#fff0cd;padding:14px}a{color:#075dae}</style></head><body>'
    page += f'<h1>G3 historical backtest: 32 setups</h1><p>{summary["first_date"]} through {summary["last_date"]}; {summary["sessions"]} sessions.</p>'
    page += f'<p><strong>Net P&amp;L: Rs {stats["net_pnl_rs"]:,.2f}; win rate: {stats["win_rate_pct"]:.2f}%; {stats["executed_trades"]} executed trades.</strong> Wins: {stats["wins"]}; losses: {stats["losses"]}; maximum daily realized drawdown: Rs {stats["max_drawdown_rs"]:,.2f}.</p>'
    page += '<p>Existing 22 setups plus exactly the ten selected G4 additions. Confirmation is at the exact next minute. Stops remain 1.25%, tightening to 1% after 120 minutes; square-off is 15:15. New setups use one highest-liquidity qualifying stock per session.</p>'
    page += '<p>Rs100,000 capital per entry, five-times exposure, Rs1,000,000 portfolio capital; five bps round-trip cost. Slippage, market impact and broker-specific charges beyond this estimate are not modeled. Wins are positive net P&amp;L, including profitable time exits.</p>'
    page += '<p class="note">Targets and the selected subset were chosen after inspecting the original historical cohort. The first 49 sessions are historical fit results, not untouched validation. October 1 is excluded because immutable input was incomplete. Opening OI is retrospective. Daily extensions use this fixed backtest configuration and disclose stock-day exclusions. PAPER/LIVE uses its separate 22-setup profile.</p>'
    setup_cols = ['signal_end','side','target_pct','executed_trades','wins','losses','win_rate_pct','net_pnl_rs']
    metric_cols = ['executed_trades','wins','losses','win_rate_pct','net_pnl_rs','max_drawdown_rs']
    for title, frame in [('Selected ten additions', setups.loc[setups.added, setup_cols]),
            ('Monthly totals', monthly[['month',*metric_cols]]),
            ('Period comparison', periods[['period','scope',*metric_cols]]),
            ('All 32 setups', setups[setup_cols]), ('Daily totals', daily)]:
        page += f'<h2>{escape(title)}</h2><div class="scroll">{frame.to_html(index=False,border=0,float_format=lambda v:f"{v:,.2f}")}</div>'
    names = ['config.json','summary.json','daily_results.csv','portfolio_trades.csv','setup_results.csv',
             'monthly_results.csv','period_results.csv','manifest.json']
    page += '<p>' + ' | '.join(f'<a href="{name}">{name}</a>' for name in names) + '</p></body></html>'
    (output / 'report.html').write_text(page, encoding='utf-8')
