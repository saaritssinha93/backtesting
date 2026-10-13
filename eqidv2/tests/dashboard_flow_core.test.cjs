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
test('research target is visible in its saved-run label', () => {
  const label=runLabel({strategy:'V13-V10-G-3',run_name:'run_20261012_full49_target075_v1',
    period_end:'2026-10-12',kind:'research_variant',target_pct:0.75},false);
  assert.match(label,/Target 0\.75%/);
  assert.match(label,/Backtest-only/);
});
test('promoted historical target is labelled as the latest G-3 backtest', () => {
  const label=runLabel({strategy:'V13-V10-G-3',run_name:'20261012_0950_long_t075',
    period_end:'2026-10-12',kind:'research_variant',role:'backtest_history',target_pct:0.75},true);
  assert.match(label,/Latest backtest 09:50 LONG/);
  assert.match(label,/Target 0\.75%/);
});

test('full current history and daily replay labels identify their actual scope', () => {
  const base = {strategy:'V13-V10-G-3',run_name:'20261013_current',period_end:'2026-10-12'};
  const full = runLabel({...base,kind:'research_variant',role:'backtest_history',current_full_strategy:true,
    period_start:'2026-07-29',sessions:49},true);
  assert.match(full,/Latest full historical backtest.*29 Jul 2026 to 12 Oct 2026.*49 sessions/);
  assert.doesNotMatch(full,/09:50|undefined|Target/);
  const daily = runLabel({...base,kind:'production_history',strategy_matches_active:true,
    period_start:'2026-10-12',period_end:'2026-10-13',sessions:2},false);
  assert.match(daily,/Daily replay history.*12 Oct 2026 to 13 Oct 2026.*2 sessions/);
  assert.doesNotMatch(daily,/Archive|Full historical/);
  const opening = runLabel({...base,kind:'research_variant',short_0920_enabled:false,target_pct:3},false);
  assert.match(opening,/09:20 LONG comparison.*Target 3%/);
});

test('current backtest profile stays current when PAPER uses a different profile', () => {
  const base = {strategy:'V13-V10-G-3',run_name:'selected_ten',strategy_matches_active:false,
    strategy_matches_backtest:true,period_start:'2026-10-13',period_end:'2026-10-13',sessions:1};
  assert.doesNotMatch(runLabel({...base,kind:'production_daily'},false),/Archived/);
  assert.doesNotMatch(runLabel({...base,kind:'production_history'},false),/Archived/);
});
