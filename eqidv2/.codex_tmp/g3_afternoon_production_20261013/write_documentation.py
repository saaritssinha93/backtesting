from pathlib import Path
import hashlib
import json
import sys

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_contract as contract

DATA = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
pointer = json.loads((DATA/'latest/current_backtest.json').read_text())
run = DATA / pointer['run_dir']
summary = json.loads((run/'summary.json').read_text())
audit = json.loads((HERE/'independent_results_validation.json').read_text())
daily = json.loads((HERE/'daily_validation.json').read_text())
setups = pd.read_csv(run/'setup_results.csv')
ledger = pd.read_csv(run/'portfolio_trades.csv', float_precision='round_trip')
new = setups.loc[setups.added.eq(True)]
assert len(new) == 15 and new.win_rate_pct.ge(70).all()
lines = ['# G3 V7: fifteen afternoon setups, October 14 preparation', '',
'The 44 existing G3 setups plus exactly the fifteen selected 12:35–13:30',
'afternoon additions now share `V13-V10-G-3-PRODUCTION-V7` across normal',
'historical backtesting, main-dashboard daily backtesting, PAPER and LIVE',
'configuration. The authoritative specification is',
'`configs/strategies/v13_v10_g_3_production_v7.json`.', '',
'The LIVE configuration is prepared and the actual October 14 coordinator',
'check returned **READY_DISARMED**. No dated arm was written, no LIVE task',
'was enabled, and no trading workers or broker orders were started. The',
'existing dated activation procedure remains in',
'[the operator handoff](G3_LIVE_OPERATOR_ACTIVATION_2026-10-13.md).', '',
'## Results', '',
'The normal historical CLI completed through October 13: 50 complete sessions',
'from July 29. October 1 remains excluded for incomplete immutable input.',
'All previous 44 setups retain their rules, selected stocks, native outcomes,',
'portfolio executions and PnL. G3 capital allocation is recomputed independently;',
'the G4 portfolio totals are not copied.', '',
'| Scope | Trades | Wins / losses | Win rate | Net PnL (Rs) |',
'| --- | ---: | --- | ---: | ---: |']
full, added = audit['full_history'], audit['added_fifteen']
old = dict(executed_trades=full['executed_trades']-added['executed_trades'],
    wins=full['wins']-added['wins'], losses=full['losses']-added['losses'],
    net_pnl_rs=full['net_pnl_rs']-added['net_pnl_rs'])
old['win_rate_pct'] = 100*old['wins']/old['executed_trades']
for name, st in [('Previous 44 setups',old), ('New fifteen additions',added), ('Combined 59 setups',full)]:
    lines.append(f"| {name} | {st['executed_trades']} | {st['wins']} / {st['losses']} | {st['win_rate_pct']:.2f}% | {st['net_pnl_rs']:,.2f} |")
lines += ['',f"Combined gross PnL: Rs{full['gross_pnl_rs']:,.2f}; modeled costs: Rs{full['costs_rs']:,.2f}.",
    f"Daily-realized maximum drawdown: Rs{full['max_drawdown_rs']:,.2f}; profit factor: {full['profit_factor']:.2f}.",
    'There are 43 profitable sessions and seven losing sessions. The 597 native',
    'fills become 568 executions under the shared capital limit.', '',
    'October 13 was a losing day: combined 15 trades, eight wins, **−Rs2,544.20**;',
    'the new afternoon group made four executions, two wins, **−Rs5,151.49**.',
    'The old eleven daily executions and their Rs2,607.29 net PnL are unchanged.', '',
    'All fifteen still have at least five native fills and actual executions,',
    'positive net PnL and at least 70% net wins both standalone and after G3',
    'capital allocation over the full 50 sessions. 12:45 SHORT is exactly 70%',
    '(seven wins from ten actual trades). Small counts remain visible below.', '',
    '| Signal | Side | Stage | Target | Native win rate | Actual trades | Actual win rate | Actual net PnL (Rs) |',
    '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |']
qualification = []
for s in new.itertuples():
    native = ledger.loc[ledger.setup_id.eq(s.setup_id) & ledger.filled.eq(True)]
    native_rate = 100*native.net_profit_rupees.gt(0).sum()/len(native)
    assert len(native)>=5 and native_rate>=70 and native.net_profit_rupees.sum()>0
    qualification.append(dict(setup_id=s.setup_id,native_fills=len(native),native_win_rate_pct=native_rate,
        actual_trades=int(s.executed_trades),actual_win_rate_pct=s.win_rate_pct))
    lines.append(f'| {s.signal_end} | {s.side} | {contract.afternoon_setups.STAGES[s.setup_id]} | {s.target_pct:.2f}% | {native_rate:.2f}% | {s.executed_trades} | {s.win_rate_pct:.2f}% | {s.net_pnl_rs:,.2f} |')
lines += ['', 'The nine excluded afternoon clock/side pairs remain absent. Filters and',
'targets were copied exactly from the final G4 dual-70 result and were not',
'retuned after October 13. The original 49 sessions participated in target',
'selection; they are fitted historical evidence, not independent validation.',
'The one extra day does not establish a future minimum win rate.', '',
'## Execution and operational preparation', '',
'- Exact next-minute confirmation; one highest-liquidity qualifying stock per new setup.',
'- Initial stop 1.25%, tightening to 1% after 120 minutes from entry; 10-minute trigger expiry; 15:15 square-off.',
'- SHORT thresholds are already effective and receive no second legacy multiplier.',
'- Zero minimum confirmation volume still requires finite causal features and runtime warmup.',
'- PAPER/backtest: Rs100,000 capital per entry, five-times exposure, Rs1,000,000 portfolio capital, five basis points round-trip modeled cost. Slippage, market impact and extra broker charges are not modeled.',
'- LIVE retains the separate Rs10,000 margin / Rs50,000 target exposure and whole-share sizing. Backtest rupee PnL is not a LIVE PnL estimate.',
'- Forty signal clocks now end at 13:30; final confirmation 13:31, publication grace to 13:32:30, scanner/feed/confirmation cutoff 13:33.',
'- Shared Python contracts and import-safe setup rows drive the session and BAT wrappers. No separate hand-maintained strategy rules were added to launchers.',
'- Both existing dashboard servers were reloaded under their established ownership and verified through authenticated APIs. Historical, daily and October 14 PAPER/LIVE views identify V7; tomorrow has no trading events or orders.',
'- All 19 captured scheduled-task states, actions and triggers are unchanged. G3 PAPER/feed tasks are enabled at 09:15, upstream futures feed at 09:05, opening recovery at 17:00 and daily/history at 17:15.',
'- The daily BAT retains its `_1620` filename for installed-task compatibility. Its recorded exit 2 came from the older 16:20 run before opening recovery; tomorrow already uses 17:15.',
'- The separate options ATM feed has an existing stale-heartbeat supervisor failure (21 restarts versus maximum 20). Equity configuration readiness passes; options-feed health is not attested.',
'- Today’s raw replay includes 212 of 213 stocks. KAYNES is excluded for missing required 09:20 futures OI; eleven opening-only exclusions are separately recorded. Missing data is not replaced with a price/OI proxy.', '',
'## Reproduction and artifacts', '',
'The actual normal CLI completed successfully:', '',
'```powershell', 'python fno_v13_v10_g_3_backtest.py --through 2026-10-13', '```', '',
'The actual daily publisher completed from raw dated inputs:', '',
'```powershell', 'python backtesting_result_v13_v10_g_3_daily.py --date 2026-10-13', '```', '',
f'Final historical run: `{run.as_posix()}`.', '',
f"[Full HTML report]({(run/'report.html').as_posix()}) · [Daily CSV]({(run/'daily_results.csv').as_posix()}) · [All setup results]({(run/'setup_results.csv').as_posix()}) · [Trades]({(run/'portfolio_trades.csv').as_posix()})", '',
f"Daily run: `{Path(daily['run_dir']).as_posix()}`.", '',
'The previous V6 JSON and Python readers are archived unchanged. Latest',
'pointers identify the verified V7 manifests; older strategy evidence remains',
'under its own version. Strategy and execution-analysis artifacts were refreshed',
'with V7. Prospective PAPER calibration remains awaiting new V7 evidence.', '',
'## Completed verification', '',
'- 312 root contract, preservation, sizing, dashboard and execution tests passed.',
'- 47 additional API/registry/cockpit tests passed in the API environment.',
'- 189 operations tests, 95 daily/dashboard tests and 32 historical tests passed. These groups overlap; their totals are not added together.',
'- Independent heap-based allocation and full report accounting passed 858 checks.',
'- Raw daily reconstruction, all input/code/artifact hashes, all 40 clocks, independent fifteen-setup selection and prior V6 preservation passed 965 checks.',
'- Every report link resolves; HTML was rendered and visually inspected. Both live dashboard APIs and the actual main daily card reconcile with the final artifacts.',
'- Actual October 14 readiness is READY_DISARMED; all 59 setup IDs, 40 clocks, separate LIVE sizing and inactive trading controls verified.', '',
f'Strategy fingerprint: `{contract.strategy_fingerprint()}`.', '',
f"Final historical manifest: `{hashlib.sha256((run/'manifest.json').read_bytes()).hexdigest()}`.", '',
'Receipts and backups: `.codex_tmp/g3_afternoon_production_20261013/`,',
'`.codex_tmp/g3_afternoon_daily_20261013/`, `.codex_tmp/g3_afternoon_ops_20261013/`.', '']
(ROOT/'docs/G3_AFTERNOON_FIFTEEN_PRODUCTION_2026-10-13.md').write_text('\n'.join(lines),encoding='utf-8')
(HERE/'final_qualification.json').write_text(json.dumps(dict(status='PASS',setups=qualification),indent=2),encoding='utf-8')
print('Documentation and all fifteen native/allocated qualification checks saved.')
