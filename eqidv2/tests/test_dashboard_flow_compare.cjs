const test = require('node:test');
const assert = require('node:assert/strict');
const {summarize, prepare, latestRuns} = require('../dashboard_flow_compare.js');

const row = (date,net,other={}) => ({date,net_pnl:net,gross_pnl:net+2,cost:2,trades:1,wins:net>0?1:0,...other});
const family = (label,rows) => ({label,data:{daily:rows}});

test('common sessions exclude absent dates rather than inserting zero results', () => {
  const entries=[
    family('G-3',[row('2026-09-30',10),row('2026-10-02',-5)]),
    family('G-2',[row('2026-09-30',20),row('2026-10-01',100),row('2026-10-02',-10)]),
    family('G',[row('2026-09-30',30),row('2026-10-01',200),row('2026-10-02',-15)]),
  ];
  const common=prepare(entries);
  assert.deepEqual(common.common,['2026-09-30','2026-10-02']);
  assert.deepEqual(common.families.map(f=>f.summary.net_pnl),[5,10,15]);
  assert.deepEqual(common.families[0].missing,['2026-10-01']);
  assert.deepEqual(common.families[1].excluded,['2026-10-01']);
  const full=prepare(entries,'full');
  assert.deepEqual(full.families.map(f=>f.summary.sessions),[2,3,3]);
  assert.deepEqual(full.families.map(f=>f.summary.net_pnl),[5,110,215]);
});

test('daily drawdown includes loss from a zero starting balance', () => {
  const result=summarize([row('2026-10-01',-100),row('2026-10-02',30),row('2026-10-03',-80)]);
  assert.equal(result.max_drawdown,150);
  assert.deepEqual(result.points.map(point=>point.value),[-100,-70,-150]);
});

test('monthly aggregation isolates unavailable months and never treats null or infinity as zero', () => {
  const result=summarize([row('2026-09-30',50),row('2026-10-01',null,{gross_pnl:null}),row('2026-10-02',5,{cost:Infinity})]);
  assert.equal(result.net_pnl,null);
  assert.equal(result.gross_pnl,null);
  assert.equal(result.cost,null);
  assert.equal(result.max_drawdown,null);
  assert.deepEqual(result.points.map(point=>point.value),[50,null,null]);
  assert.deepEqual(result.monthly,[{month:'2026-09',net_pnl:50,sessions:1},{month:'2026-10',net_pnl:null,sessions:2}]);
  assert.equal(summarize([row('2026-10-01',0,{trades:0,wins:0,gross_pnl:0,cost:0})]).win_rate_pct,null);
});

test('invalid or unavailable calendars prevent a misleading partial common comparison', () => {
  const shared=[row('2026-10-01',0)];
  const failed=prepare([family('G-3',shared),family('G-2',shared),{label:'G',error:'offline'}]);
  assert.equal(failed.allAvailable,false);
  assert.deepEqual(failed.common,[]);
  assert.equal(failed.families[0].summary.net_pnl,null);
  const duplicates=prepare([family('G-3',[...shared,...shared]),family('G-2',shared),family('G',shared)]);
  assert.match(duplicates.families[0].error,/duplicate/);
  const invalid=prepare([family('G-3',[row('2026-02-30',10)]),family('G-2',shared),family('G',shared)]);
  assert.match(invalid.families[0].error,/invalid/);
});

test('latest run selection retains supplied family and full-history priority', () => {
  const result=latestRuns([
    {id:'g-full-latest',strategy:'V13-V10-G'},
    {id:'g-daily-newer',strategy:'V13-V10-G'},
    {id:'g2-latest',strategy:'V13-V10-G-2'},
    {id:'g3-latest',strategy:'V13-V10-G-3'},
    {id:'g3-archived',strategy:'V13-V10-G-3'},
  ]);
  assert.deepEqual(result.map(f=>f.run.id),['g3-latest','g2-latest','g-full-latest']);
});
