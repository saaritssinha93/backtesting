"""Readable review of a sealed V4 result; does not select or change a strategy."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import os
from pathlib import Path
import pandas as pd
import numpy as np
from research import g7_v4_backtest as base
from research import g7_v4_round2_backtest as round2
from research.g8_vix_selection_research import sha,dump


def run(source):
    source=Path(source).resolve()
    # Check integrity; the computational replay has already passed in round2.run.
    base.verify_members(source)
    summary=base.read(source/'summary.json')
    reference=pd.read_csv(source/'v4_symbol_guard/portfolio_trades.csv')
    rows=[]
    for name in [*round2.model.POLICIES,*round2.ADDITIONAL]:
        book=pd.read_csv(source/name/'portfolio_trades.csv')
        for period,dates in summary['periods'].items():
            r=base.comparison_row(book,reference,dates)
            rows.append(dict(policy=name,period=period,**r))
    incremental=pd.DataFrame(rows)
    out=base.OUT_ROOT/'reviews'/base.FINAL_DAY/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    out.mkdir(parents=True,exist_ok=False)
    incremental.to_csv(out/'incremental_vs_symbol_guard.csv',index=False)
    comparison=pd.read_csv(source/'comparison.csv')
    # Independently reconcile cost sensitivity with the fixed-exposure contract.
    costs=pd.read_csv(source/'cost_sensitivity.csv')
    assert np.allclose(costs.cost_rs,costs.trades*500000*costs.cost_bps/10000,rtol=0,atol=1e-6)
    assert np.allclose(costs.net_pnl_rs,costs.gross_pnl_rs-costs.cost_rs,rtol=0,atol=1e-6)
    link=lambda name:os.path.relpath(source/name,out).replace('\\','/')
    display=lambda f:f.to_html(index=False,float_format=lambda x:f'{x:,.2f}')
    html="<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>G7 V4 results and decision</title><style>body{font:16px/1.5 system-ui;margin:32px;max-width:1400px;color:#182638}table{border-collapse:collapse;font-size:14px}td,th{border:1px solid #ddd;padding:8px}.scroll{overflow:auto}a{color:#1c5caa}.result{padding:18px;background:#f1f4f8;border-left:5px solid #69829e}</style></head><body>"
    html+='<h1>G7 V4: results through 27 October 2026</h1><div class="result"><b>No extreme-entry filter qualified under the fixed development criteria.</b><br>Fourteen candidates were tested in two bounded rounds, using603 selected opportunities across59 available sessions. The V4 default backtest ledger contains only the one-active-position-per-symbol rule. All extreme-entry candidates remain separate research ledgers. No live or paper strategy was activated.</div>'
    html+='<p>The screen required at least5% reduction in losing-trade amount while retaining95% of G7 V2 net profit,70% of trades and85% of winners, without larger daily-close drawdown. The mandatory symbol rule itself reduces profit; its control is shown separately so that cost is not attributed to the extreme filters.</p>'
    chosen=['g7_v2','v4_symbol_guard','vwap25_rsi','dual_extreme_rsi','historical95','conditional_retest']
    cols=['policy','trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']
    html+='<h2>Representative results — all59 sessions</h2><p>All values are backtest rupees after5bps modeled round-trip costs. Drawdown uses daily-close equity.</p><div class="scroll">'+display(comparison.loc[comparison.period.eq('all') & comparison.policy.isin(chosen),cols])+'</div>'
    html+='<h2>What the extreme filter adds beyond duplicate prevention</h2><p>These changes compare each filter with the symbol-guard-only control. Positive net change means added profit; positive loss reduction means less money lost on losing trades. These comparisons do not change the recorded selection decision.</p>'
    cols=['policy','period','net_change_rs','loss_amount_reduction_rs','avoided_baseline_losers','missed_baseline_winners','daily_close_drawdown_rs']
    html+='<div class="scroll">'+display(incremental.loc[incremental.policy.isin(['vwap25_rsi','dual_extreme_rsi']),cols])+'</div>'
    html+='<p>VWAP+RSI improves full-sample net by aboutRs4,196 relative to duplicate prevention alone and removes four additional losing trades. It remains unqualified: its development performance does not meet the95% net-profit-retention threshold against original V2. The full-sample improvement was observed after the selection decision and cannot justify selecting it retrospectively.</p>'
    html+='<h2>26 and27 October</h2><div class="scroll">'+display(comparison.loc[comparison.period.isin(['known_oct26','final_oct27']) & comparison.policy.isin(chosen),['policy','period','trades','losses','net_pnl_rs']])+'</div>'
    html+='<h2>Validation and limitations</h2><p>The original V2 baseline and every saved candidate ledger were replayed and matched.93 relevant automated tests passed. Costs at5,15 and30bps reconcile to fixed exposure. All603 historical percentile references use60 valid prior same-stock/same-clock sessions; the current day is excluded. VWAP/RSI were applicable to499 opportunities after warmup.</p>'
    html+='<p>Development: first35 sessions; validation: next14; assessment: next8. Oct26 was a known diagnostic day; Oct27 was excluded from candidate selection. This is retrospective research, not independent forward validation. Three missing source sessions—24/25 August and1 October—are excluded. Inherited targets and after-close universe eligibility retain the original research limitations.</p>'
    html+='<h2>Files</h2><ul>'
    for title,name in [('Full14-candidate report','report.html'),('V2 vs V4 daily comparison','g7_v4/daywise_comparison.csv'),('V4 default trade ledger','g7_v4/portfolio_trades.csv'),('Every candidate comparison','comparison.csv'),('Every trade change','trade_changes.csv'),('Entry features and decisions','entry_features_and_decisions.csv'),('Cost sensitivity','cost_sensitivity.csv')]:
        html+='<li><a href="'+link(name)+'">'+title+'</a></li>'
    html+='</ul><p><a href="incremental_vs_symbol_guard.csv">Incremental results for all14 filters versus duplicate prevention alone</a></p></body></html>'
    (out/'review.html').write_text(html,encoding='utf-8')
    dump(out/'source.json',dict(source_run=str(source),source_manifest_sha256=sha(source/'manifest.json'),scope='BACKTEST_ONLY',execution_authority=False))
    base.seal(out,code={str(Path(__file__).relative_to(base.ROOT)):sha(Path(__file__))})
    base.verify_members(out)
    print('REVIEW',out/'review.html')
    print(incremental.loc[incremental.policy.isin(['vwap25_rsi','dual_extreme_rsi']),['policy','period','net_change_rs','loss_amount_reduction_rs']].to_string(index=False))
    return out


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('source',type=Path)
    run(parser.parse_args().source)
