const test = require('node:test');
const assert = require('node:assert/strict');
const {summarize, hasCompleteLedger, runLabel} = require('../dashboard_flow_core.js');
const trade = (date, net) => ({date, net_pnl:net, gross_pnl:net+5, cost:5});
test('filtered summary uses only its recorded dates and starts drawdown from zero', () => {
  const result = summarize([trade('2026-10-01', -20), trade('2026-10-02', 50), trade('2026-10-04', 1000)], ['2026-10-01','2026-10-02','2026-10-03']);
  assert.equal(result.trades, 2); assert.equal(result.sessions, 3);
  assert.equal(result.net_pnl, 30); assert.equal(result.gross_pnl, 40); assert.equal(result.cost, 10);
  assert.equal(result.max_drawdown, 20); assert.equal(result.profit_factor, 2.5); assert.equal(result.win_rate_pct, 50);
});
test('a filtered view with no trades preserves actual sessions and reports zero without inventing a win rate', () => {
  const result=summarize([],['2026-10-09']);
  assert.equal(result.sessions,1); assert.equal(result.net_pnl,0); assert.equal(result.trades,0);
  assert.equal(result.win_rate_pct,null); assert.equal(result.profit_factor,null);
});
test('incomplete or unknown ledger values are not presented as complete filtered totals', () => {
  assert.equal(summarize([trade('2026-10-09',null)],['2026-10-09']).net_pnl,null);
  assert.equal(summarize([trade('2026-10-09',100)],['2026-10-09'],false).net_pnl,null);
  assert.equal(hasCompleteLedger({trades:[trade('2026-10-09',100)],summary:{trades:2,net_pnl:100}}),false);
  assert.equal(hasCompleteLedger({trades:[trade('2026-10-09',100)],summary:{trades:1,net_pnl:100}}),true);
});
test('friendly latest labels use the actual session cutoff, not the folder date', () => {
  const result=runLabel({strategy:'V13-V10-G-3',run_name:'20261010T_through_20261009_pullbacks',period_end:'2026-10-09',kind:'backtest'},true);
  assert.equal(result,'G-3 · Latest · Through 09 Oct 2026');
  assert.ok(!result.includes('pullbacks'));
});
