const test = require('node:test');
const assert = require('node:assert/strict');
const {number, money, outcome, validatePayload, esc, svgChart, tone, mount} = require('../dashboard_cockpit.js');

test('unknown metrics never become zero or valid P&L', () => {
  for (const v of [null, undefined, '', '  ', [], {}, false, true, 'NaN', Infinity]) { assert.equal(number(v), null); assert.equal(money(v), '—'); }
  assert.equal(number(0), 0); assert.equal(money(0), '₹0.00'); assert.equal(number('13.5'), 13.5);
});
test('rejects responses from another mode or session before rendering', () => {
  assert.throws(() => validatePayload({session_date:'2026-10-09', mode:'LIVE'}, '2026-10-09', 'PAPER'), /does not match/);
  assert.throws(() => validatePayload({session_date:'2026-10-08', mode:'PAPER'}, '2026-10-09', 'PAPER'), /does not match/);
  const data = {session_date:'2026-10-09', mode:'PAPER'}; assert.equal(validatePayload(data, data.session_date, data.mode), data);
});
test('missing checks are unknown and selection does not become an indicator failure', () => {
  assert.equal(outcome({}), 'UNKNOWN');
  assert.equal(outcome({checks:[{name:'rsi',status:'PASS'},{name:'final_selection',status:'FAIL'}]}), 'PASS');
  assert.equal(outcome({checks:[{name:'rsi',status:'PASS'},{name:'volume',status:'UNKNOWN'}]}), 'UNKNOWN');
  assert.equal(outcome({checks:[{name:'rsi',status:'FAIL'},{name:'volume',status:'UNKNOWN'}]}), 'FAIL');
});
test('charts distinguish missing prices, real zero, observations and candles', () => {
  assert.match(svgChart({observations:[{price:null}]}), /No price history available/);
  assert.match(svgChart({symbol:'ABC',observations:[{price:0,time:'10:00'}]}), /Recorded price observations/);
  assert.match(svgChart({symbol:'ABC',candles:[{open:100,high:102,low:99,close:101,time:'10:00'}]}), /Recorded candles/);
  assert.doesNotMatch(svgChart({observations:[{price:123,time:'<script>evil</script>'}]}), /<script>/);
});
test('recorded entry and exit markers survive candle rendering and extend real time and price axes', () => {
  const chart = {symbol:'ABC',candles:[
    {timestamp_ist:'2026-10-08T10:00:00+05:30',open:100,high:102,low:99,close:101},
    {timestamp_ist:'2026-10-08T10:01:00+05:30',open:101,high:103,low:100,close:102}
  ], observations:[
    {timestamp_ist:'2026-10-08T09:55:00+05:30',price:98,label:'Entry fill'},
    {timestamp_ist:'2026-10-08T14:00:00+05:30',price:110,label:'Exit fill'}
  ]};
  const html=svgChart(chart);
  assert.equal((html.match(/class="cockpit-candle /g)||[]).length,2);
  assert.equal((html.match(/class="cockpit-observation /g)||[]).length,2);
  assert.match(html,/Entry fill.*09:55:00/); assert.match(html,/Exit fill.*14:00:00/);
  assert.match(html,/>09:55:00<\/text>/); assert.match(html,/>14:00:00 IST<\/text>/);
  assert.match(html,/Gaps are preserved/); assert.doesNotMatch(html,/<polyline|NaN|undefined/);
  const candleXs=[...html.matchAll(/class="cockpit-candle [^"]+"><title>[^<]+<\/title><line x1="([\d.]+)"/g)].map((m)=>Number(m[1]));
  assert.equal(candleXs.length,2); assert.ok(candleXs[1]-candleXs[0]<4,'one-minute candles remain close on the multi-hour fill timeline');
  const exitMatch=html.match(/class="cockpit-observation exit"[^>]*><title>[^<]*<\/title><circle cx="([\d.]+)" cy="([\d.]+)"/);
  assert.ok(exitMatch); assert.ok(Number(exitMatch[1])>600); assert.ok(Number(exitMatch[2])>=22 && Number(exitMatch[2])<240);
});
test('observation labels are escaped and unusable marker timestamps are explained', () => {
  const chart={candles:[{timestamp_ist:'2026-10-08T10:00:00+05:30',open:100,high:102,low:99,close:101}],observations:[
    {timestamp_ist:'2026-10-08T10:01:00+05:30',price:101,label:'<script>bad</script>'},
    {timestamp_ist:'not a date',price:102,label:'Entry fill'}]};
  const html=svgChart(chart);assert.doesNotMatch(html,/<script>/);assert.match(html,/&lt;script&gt;/);assert.match(html,/1 observation markers omitted/);
});
test('all recorded strings escape HTML characters', () => assert.equal(esc('<img src="x" onerror=\'bad\'>&'), '&lt;img src=&quot;x&quot; onerror=&#39;bad&#39;&gt;&amp;'));
test('unknown or incomplete status can never become a healthy badge through substring matching', () => {
  for (const state of ['INCOMPLETE', 'UNAVAILABLE', 'UNKNOWN', 'NO_EVIDENCE', 'HISTORICAL', 'NO_TIMESTAMPED_QUOTE']) assert.equal(tone(state), 'neutral');
  assert.equal(tone('COMPLETE'), 'good'); assert.equal(tone('STALE_OR_PARTIAL'), 'bad'); assert.equal(tone('ENTRY_REJECTED'), 'bad');
});

function mockHost() {
  const nodes = new Map(), listeners = new Map();
  const node = (key) => {
    if (!nodes.has(key)) nodes.set(key, {innerHTML:'', textContent:'', value:'', hidden:false, dataset:{}, classList:{add(){}, toggle(){}}, setAttribute(name, value){this[name]=value;}, scrollIntoView(){}});
    return nodes.get(key);
  };
  const host = {...node('host'), querySelector: node, querySelectorAll: () => [node('paper-button'), node('live-button')], contains: () => true,
    addEventListener: (name, callback) => listeners.set(name, callback), removeEventListener: (name) => listeners.delete(name)};
  node('paper-button').dataset.mode = 'PAPER'; node('live-button').dataset.mode = 'LIVE';
  const send = (type, dataset, value) => { const target = {dataset, value, closest(){return this;}}; listeners.get(type)({target}); };
  return {host, node, send};
}
const tick = () => new Promise((resolve) => setImmediate(resolve));
function minimalPayload(date, mode, total) { return {session_date:date, mode, identity:{label:`${mode} strategy`}, summary:{total_pnl_rs:total}, evidence:{state:'NO_EVIDENCE',rows_5m:[],rows_1m:[],coverage:[]}, stages:[], positions:[], events:[], orders:[], symbols:[], available_dates:[]}; }

test('mount protects mode/date isolation against late responses and retains a failed same-mode snapshot', async () => {
  const previousFetch = global.fetch, requests = [], {host, node, send} = mockHost();
  global.fetch = (url) => new Promise((resolve, reject) => requests.push({url, resolve, reject}));
  const instance = mount({host, apiUrl: (path) => path + '&token=test'});
  try {
    assert.equal(requests.length, 1); assert.match(requests[0].url, /token=test/);
    const today = new URL(requests[0].url, 'http://localhost').searchParams.get('date');
    send('click', {mode:'LIVE'}); assert.equal(requests.length, 2);
    requests[1].resolve({ok:true,json: async () => minimalPayload(today,'LIVE',222)}); await tick();
    requests[0].resolve({ok:true,json: async () => minimalPayload(today,'PAPER',999)}); await tick();
    assert.match(node('[data-zone="metrics"]').innerHTML, /₹222\.00/); assert.doesNotMatch(node('[data-zone="metrics"]').innerHTML, /999/);
    assert.equal(node('[data-zone="strategy-title"]').textContent, 'LIVE strategy');
    instance.refresh(); requests[2].reject(new Error('offline')); await tick();
    assert.match(node('[data-zone="status"]').innerHTML, /LAST SUCCESSFUL SNAPSHOT/); assert.match(node('[data-zone="metrics"]').innerHTML, /₹222\.00/);
    send('change', {field:'date'}, '2026-10-08'); assert.doesNotMatch(node('[data-zone="metrics"]').innerHTML, /₹222\.00/);
    requests[3].reject(new Error('offline')); await tick();
    assert.doesNotMatch(node('[data-zone="metrics"]').innerHTML, /₹222\.00/);
    assert.doesNotMatch(node('[data-zone="status"]').innerHTML, /LAST SUCCESSFUL SNAPSHOT/);
    instance.setPaused(true); instance.refresh(); assert.equal(requests.length,4);
    instance.refresh(true); assert.equal(requests.length,5);
    requests[4].resolve({ok:true,json:async()=>minimalPayload('2026-10-08','LIVE',123)}); await tick();
    assert.match(node('[data-zone="metrics"]').innerHTML,/₹123\.00/);
  } finally {instance.destroy(); global.fetch = previousFetch;}
});

test('paper/live comparison renders flat source fields without inventing missing returns', async () => {
  const previousFetch = global.fetch, {host,node} = mockHost();
  global.fetch = async (url) => { const params = new URL(url,'http://localhost').searchParams, payload = minimalPayload(params.get('date'),'PAPER',0); payload.comparison = [{symbol:'ABC',side:'LONG',paper_status:'CLOSED',live_status:'NO_FILL',paper_qty:500,live_qty:1,paper_return_pct:2.45,live_return_pct:null,entry_difference_bps:null,state:'INCOMPLETE_FILLS',note:'No live fill recorded.'}]; return {ok:true,json:async()=>payload}; };
  const instance=mount({host});
  try {await tick(); const html = node('[data-zone="comparison"]').innerHTML; assert.match(html,/500 \/ 1/); assert.match(html,/2\.45%/); assert.match(html,/NO FILL/); assert.doesNotMatch(html,/0\.00%/); assert.match(html,/No live fill recorded/);} finally {instance.destroy();global.fetch=previousFetch;}
});

const fs = require('node:fs'), path = require('node:path');
const recordedFile = path.join(__dirname, '..', '.codex_tmp', 'dashboard_terminal_qa', 'cockpit_2026-10-08_PAPER.json');
test('recorded October 8 snapshot renders actual metrics, stages, symbols and event schema', {skip: !fs.existsSync(recordedFile)}, async () => {
  const payload = JSON.parse(fs.readFileSync(recordedFile, 'utf8')), previousFetch = global.fetch, {host, node, send} = mockHost();
  global.fetch = async (url) => { const params = new URL(url,'http://localhost').searchParams; return {ok:true,json:async()=>params.get('date') === payload.session_date ? payload : minimalPayload(params.get('date'),params.get('mode'),null)}; };
  const instance = mount({host});
  try {
    await tick(); send('change',{field:'date'},payload.session_date); await tick();
    assert.equal(node('[data-zone="strategy-title"]').textContent,payload.identity.label || payload.identity.profile);
    assert.match(node('[data-zone="metrics"]').innerHTML, new RegExp(esc(money(payload.summary.realized_pnl_rs)).replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
    for (const stage of payload.stages) assert.ok(node('[data-zone="pipeline"]').innerHTML.includes(`data-stage="${stage.key}"`));
    if (payload.chart.candles.length) {assert.match(node('[data-zone="chart"]').innerHTML, /Recorded candles/); assert.doesNotMatch(node('[data-zone="chart"]').innerHTML,/NaN|undefined/);}
    if (payload.events.length) assert.ok(node('[data-zone="events"]').innerHTML.includes(esc(payload.events[payload.events.length-1].event_type.replace(/_/g,' '))));
    instance.setPaused(true); assert.match(node('[data-zone="status"]').innerHTML,/paused/);
  } finally {instance.destroy(); global.fetch = previousFetch;}
});
