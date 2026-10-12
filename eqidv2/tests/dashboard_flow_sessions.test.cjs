const test = require('node:test');
const assert = require('node:assert/strict');
const {sessionSeries} = require('../dashboard_flow_chart.js');

test('filtered session curve respects replay dates and resets its peak at zero', () => {
  const rows = sessionSeries({
    complete: true, dates: ['2026-10-02', '2026-10-03', '2026-10-04'],
    trades: [{date: '2026-10-01', net_pnl: 999}, {date: '2026-10-02', net_pnl: -20}, {date: '2026-10-04', net_pnl: 50}],
  });
  assert.deepEqual(rows.map(row => row.net_pnl), [-20, 0, 50]);
  assert.deepEqual(rows.map(row => row.cumulative), [-20, -20, 30]);
  assert.deepEqual(rows.map(row => row.drawdown), [-20, -20, 0]);
});

test('entire-run scope uses saved daily totals independently of map filters or missing ledger', () => {
  const rows = sessionSeries({scope: 'run', complete: false, dates: ['2026-10-03'], trades: [], daily: [
    {date: '2026-10-01', net_pnl: 100}, {date: '2026-10-02', net_pnl: -30}, {date: '2026-10-03', net_pnl: -20},
  ]});
  assert.deepEqual(rows.map(row => row.cumulative), [100, 70, 50]);
  assert.deepEqual(rows.map(row => row.drawdown), [0, -30, -50]);
});

test('missing daily data cannot turn into a flat cumulative line or a recovered drawdown', () => {
  const rows = sessionSeries({scope: 'run', daily: [
    {date: '2026-10-01', net_pnl: 100}, {date: '2026-10-02', net_pnl: null}, {date: '2026-10-03', net_pnl: 40},
  ]});
  assert.deepEqual(rows.map(row => row.cumulative), [100, null, null]);
  assert.deepEqual(rows.map(row => row.drawdown), [0, null, null]);
  assert.equal(rows[2].net_pnl, 40);
});

test('incomplete filtered ledgers expose unknown totals, including sessions without recorded trades', () => {
  const rows = sessionSeries({complete: false, dates: ['2026-10-01', '2026-10-02'], trades: [{date: '2026-10-01', net_pnl: 100}]});
  assert.ok(rows.every(row => row.net_pnl === null && row.cumulative === null && row.drawdown === null));
});

test('a non-finite trade invalidates its session and all subsequent cumulative observations', () => {
  const rows = sessionSeries({complete: true, dates: ['2026-10-01', '2026-10-02'], trades: [
    {date: '2026-10-01', net_pnl: Infinity}, {date: '2026-10-02', net_pnl: 10},
  ]});
  assert.deepEqual(rows.map(row => row.cumulative), [null, null]);
});
