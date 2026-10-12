const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {summarize} = require('../dashboard_flow_core.js');

// Minimal DOM harness: execute the shipped scripts and event handlers without a
// browser. It verifies data wiring; visual layout still requires browser QA.
class Element {
  constructor(tag = 'div') {
    this.tagName = tag.toUpperCase(); this.children = []; this.dataset = {}; this.style = {};
    this.attributes = {}; this.events = {}; this.value = ''; this._text = ''; this.hidden = false;
    this.clientWidth = 800; this.clientHeight = 400; this.isConnected = true;
    this.classList = {toggle() {}, add() {}};
  }
  append(...nodes) { this.children.push(...nodes); }
  appendChild(node) { this.append(node); return node; }
  replaceChildren(...nodes) { this.children = nodes; this._text = ''; }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(node => node.textContent || '').join(''); }
  get lastChild() { return this.children.at(-1); }
  setAttribute(key, value) { this.attributes[key] = String(value); }
  removeAttribute(key) { delete this.attributes[key]; }
  addEventListener(event, handler) { this.events[event] = handler; }
  showModal() { this.open = true; }
  close() { this.open = false; }
}
function fixture(incomplete = false) {
  const trades = [
    {date:'2026-10-01', symbol:'ALPHA', side:'LONG', setup:'Breakout', net_pnl:100},
    {date:'2026-10-02', symbol:'BETA', side:'SHORT', setup:'Breakdown', net_pnl:-40},
    {date:'2026-10-03', symbol:'ALPHA', side:'LONG', setup:'Breakout', net_pnl:20},
  ].map(row => ({...row, gross_pnl:row.net_pnl+5, cost:5, entry_price:100, exit_price:101}));
  const dates = trades.map(row => row.date);
  const run = {id:'g3-fixture', strategy:'V13-V10-G-3', source_label:'Saved G-3 results', run_name:'20261004T_fixture', period_end:'2026-10-03', kind:'backtest'};
  const data = {selected_run:run, runs:[run], trades, daily:trades.map(row => ({date:row.date, net_pnl:row.net_pnl})), summary:summarize(trades, dates), warnings:[]};
  if (incomplete) data.summary.trades = 4;
  return data;
}
async function app(data) {
  const root = path.join(__dirname, '..');
  const html = fs.readFileSync(path.join(root, 'dashboard_flow.html'), 'utf8');
  const elements = new Map([...html.matchAll(/id="([^"]+)"/g)].map(match => [match[1], new Element()]));
  const ranges = ['20','40','all'].map(range => Object.assign(new Element('button'), {dataset:{range}}));
  const views = ['flow','ledger','compare'].map(view => Object.assign(new Element('button'), {dataset:{view}}));
  const document = {body:new Element('body'), getElementById(id) {if (!elements.has(id)) throw Error('Missing element '+id);return elements.get(id);},
    createElement:tag=>new Element(tag), createElementNS:(_,tag)=>new Element(tag), querySelector:()=>new Element('a'),
    querySelectorAll:selector=>selector==='[data-range]'?ranges:selector==='[data-view]'?views:[], addEventListener(){}};
  let failure = false;
  const context = {document, location:{origin:'http://localhost'}, URL, Blob, AbortController, Intl, Date,
    FLOW_API_TOKEN:'', localStorage:{getItem:()=>null,setItem(){}}, addEventListener(){},
    getComputedStyle:()=>({paddingLeft:0,paddingRight:0,paddingTop:0,paddingBottom:0}),
    setTimeout,clearTimeout,setInterval,clearInterval, FlowComparison:{render(){}},
    fetch:async()=>{if(failure)throw Error('Saved results unavailable');return {ok:true,json:async()=>data};}};
  context.window = context;
  vm.createContext(context);
  for (const file of ['dashboard_flow_core.js','dashboard_flow_chart.js','dashboard_flow.js']) vm.runInContext(fs.readFileSync(path.join(root,file),'utf8'),context,{filename:file});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(elements.get('notice').hidden,true, elements.get('notice').textContent);
  return {get:id=>elements.get(id), views, failRefresh:()=>{failure=true;}};
}
const values = app => app.get('session-values-body').children.map(row=>row.children.map(cell=>cell.textContent));

test('initial load wires sourced evidence, provenance and the exact session table', async () => {
  const ui = await app(fixture());
  assert.equal(ui.get('metric-net').textContent,'₹80');
  assert.equal(ui.get('research-state').textContent,'HISTORICAL / LOADED');
  assert.match(ui.get('research-provenance').textContent,/V13-V10-G-3.*03 Oct 2026/);
  assert.match(ui.get('research-loaded').textContent,/Loaded .* IST/);
  assert.equal(ui.get('evidence-cards').children.length,6);
  assert.match(ui.get('evidence-cards').children[0].textContent,/₹120/);
  assert.match(ui.get('evidence-cards').children[1].textContent,/−₹40/);
  assert.match(ui.get('evidence-cards').children[5].textContent,/Reconciled/);
  assert.deepEqual(values(ui).map(row=>row.slice(1)),[['₹20.00','₹80.00','−₹20.00'],['−₹40.00','₹60.00','−₹40.00'],['₹100.00','₹100.00','₹0.00']]);
});

test('direction and replay update session evidence; entire-run scope restores daily totals', async () => {
  const ui = await app(fixture());
  ui.get('side-filter').onchange({target:{value:'SHORT'}});
  assert.equal(ui.get('metric-net').textContent,'−₹40');
  assert.match(ui.get('evidence-cards').children[0].textContent,/₹0/);
  ui.get('replay-slider').oninput({target:{value:'1'}});
  assert.equal(ui.get('metric-net').textContent,'₹0');
  assert.equal(values(ui).length,1);
  ui.get('summary-scope').onchange({target:{value:'run'}});
  assert.equal(ui.get('metric-net').textContent,'₹80');
  assert.equal(ui.get('session-scope').textContent,'ENTIRE RUN');
  assert.equal(values(ui).length,3);
  assert.match(ui.get('evidence-cards').children[0].textContent,/₹120/);
  ui.views[2].onclick();
  assert.equal(ui.get('research-analysis').hidden,true);
  assert.equal(ui.get('source-strip').hidden,true);
});

test('incomplete ledger suppresses filtered evidence while preserving saved daily results', async () => {
  const ui = await app(fixture(true));
  assert.equal(ui.get('metric-net').textContent,'—');
  assert.ok(values(ui).every(row=>row.slice(1).every(value=>value==='—')));
  assert.match(ui.get('evidence-cards').children[5].textContent,/Incomplete/);
  assert.match(ui.get('evidence-cards').children[0].textContent,/Requires a reconciled ledger/);
  ui.get('summary-scope').onchange({target:{value:'run'}});
  assert.equal(ui.get('metric-net').textContent,'₹80');
  assert.equal(values(ui)[0][2],'₹80.00');
  assert.match(ui.get('evidence-cards').children[0].textContent,/Requires a reconciled ledger/);
});

test('failed refresh retains previous evidence and explicitly marks the saved view', async () => {
  const ui = await app(fixture());
  ui.failRefresh();
  await ui.get('refresh').onclick();
  assert.equal(ui.get('research-state').textContent,'REFRESH FAILED / SAVED VIEW');
  assert.equal(ui.get('metric-net').textContent,'₹80');
  assert.equal(ui.get('notice').hidden,false);
  assert.equal(ui.get('refresh').disabled,false);
});
