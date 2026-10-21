"""Read canonical G3 rules and produce an explanatory HTML/JSON snapshot only."""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from datetime import datetime
import hashlib
from html import escape
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_live_config as runtime
import fno_v13_v10_g_3_live_execution_policy as execution
from research.g3_backtest_profile import contract as backtest, CONFIG_PATH as BACKTEST_CONFIG

contract.load_contract(ROOT / 'configs/strategies/v13_v10_g_3_production_v7.json')
runtime.validate_strategy()
backtest.load_contract(BACKTEST_CONFIG)
backtest_enabled_ids = {s.setup_id for s in backtest.ACTIVE_SETUPS}
assert len(backtest_enabled_ids) == 49 and len(backtest.DISABLED_IDS) == 10
assert backtest_enabled_ids == {s.setup_id for s in contract.ACTIVE_SETUPS if s.target_pct > .75} | set(backtest.RESTORED_IDS)
restored_text = ', '.join(f'{s.signal_end} {s.side} ({s.target_pct:g}%)'
                         for s in backtest.ACTIVE_SETUPS if s.setup_id in backtest.RESTORED_IDS)
deployment = json.loads((ROOT / 'configs/strategies/active_profile.json').read_text())
assert deployment['strategy_fingerprint'] == contract.strategy_fingerprint()
assert deployment['live_execution_policy']['live_execution_policy_fingerprint'] == execution.policy_fingerprint()

groups = [
    ('Late six', contract.late_setups),
    ('Selected ten', contract.selected_setups),
    ('Midday twelve', contract.midday_setups),
    ('Afternoon fifteen', contract.afternoon_setups),
]
rows = []
for s in sorted(contract.ACTIVE_SETUPS, key=lambda x: (x.signal_end, x.side)):
    group, stage = 'Original sixteen', 'Not separately labelled'
    for label, module in groups:
        if s.setup_id in module.SETUP_IDS:
            group = label
            stage = getattr(module, 'STAGES', {}).get(s.setup_id, 'Not separately labelled')
            break
    rows.append({
        **asdict(s), 'setup_id': s.setup_id, 'group': group, 'research_stage': stage,
        'core_price_change_pct': contract._CORE_BY_ID[s.setup_id].price_change_pct,
        'confirmation_volume_ratio_min': contract.confirmation_volume_minimum(s.side, setup_id=s.setup_id),
        'tightened_stop_pct': contract.TIGHTENED_STOP_PCT,
        'tighten_after_minutes': contract.TIGHTEN_AFTER_MINUTES,
        'square_off': contract.SQUARE_OFF,
    })
assert len(rows) == 59 and Counter(r['side'] for r in rows) == {'LONG': 28, 'SHORT': 31}
assert Counter(r['group'] for r in rows) == {
    'Original sixteen': 16, 'Late six': 6, 'Selected ten': 10, 'Midday twelve': 12, 'Afternoon fifteen': 15}
assert len({r['signal_end'] for r in rows}) == 40
assert all(r['stop_pct'] == 1.25 and r['min_traded_value'] == 0 for r in rows)
assert sum(r['max_entries'] for r in rows) == 61

sources = [
    'configs/strategies/active_profile.json',
    'configs/strategies/v13_v10_g_3_production_v7.json',
    'configs/strategies/v13_v10_g_3_live_execution_v1.json',
    'fno_v13_v10_g_3_contract.py', 'fno_v13_v10_g_3_live_config.py',
    'fno_v13_v10_g_3_live_execution_policy.py',
    'fno_v13_v10_g_3_late_setups.py', 'fno_v13_v10_g_3_selected_setups.py',
    'fno_v13_v10_g_3_midday_setups.py', 'fno_v13_v10_g_3_afternoon_setups.py',
    'fno_oi_hybrid_data.py', 'fno_equity_fetch_1min.py', 'fno_v5_live.py',
    'fno_v13_v10_g_paper.py', 'fno_v6_live_kite_session.py', 'fno_live_entry_guard.py',
]
out = ROOT / 'docs/G3_V7_DETAILED_CONFIGURATION_2026-10-13.html'
json_out = out.with_suffix('.json')
snapshot = {
    'schema_version': 'g3_configuration_explanation_snapshot_v1',
    'purpose': 'Read-only description of current configuration; not an execution config or live activation',
    'generated_at_ist': datetime.now(ZoneInfo('Asia/Kolkata')).isoformat(timespec='seconds'),
    'strategy_version': contract.STRATEGY_VERSION,
    'strategy_fingerprint': contract.strategy_fingerprint(),
    'deployment': deployment,
    'canonical_contract': contract.strategy_payload(),
    'live_execution_policy': execution.policy_payload(),
    'setups': rows,
    'source_sha256': {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in sources},
}
if json_out.exists():
    # The backtest availability note is an overlay. Preserve the original V7
    # generated timestamp, detailed values, source hashes and JSON bytes.
    original = json.loads(json_out.read_text(encoding='utf-8'))
    assert original['canonical_contract'] == snapshot['canonical_contract']
    assert original['setups'] == rows
    snapshot = original
else:
    json_out.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')

def n(value):
    return format(value, '.6g')

def table_row(r):
    css = 'long' if r['side'] == 'LONG' else 'short'
    p = 'Liquidity' if r['picker'] == 'max_liquidity' else 'Volume' if r['picker'] == 'max_volume' else 'Move'
    core = n(r['core_price_change_pct'])
    if r['core_price_change_pct'] != r['price_change_pct']:
        core = '<strong>' + core + '</strong>'
    enabled = r['setup_id'] in backtest_enabled_ids
    status = 'Enabled (restored)' if r['setup_id'] in backtest.RESTORED_IDS else 'Enabled' if enabled else 'Disabled'
    availability = '<span class="' + ('long' if enabled else 'short') + '"><b>' + status + '</b></span>'
    vals = [r['signal_end'], r['confirmation_end'], '<span class="side ' + css + '">' + r['side'] + '</span>',
            n(r['price_change_pct']), core, n(r['oi_change_pct']), n(r['volume_ratio']),
            n(r['body_ratio'] * 100), n(r['max_wick_ratio'] * 100), n(r['confirmation_volume_ratio_min']),
            n(r['stop_pct']), n(r['target_pct']), availability, p, str(r['max_entries']), escape(r['research_stage']), escape(r['group'])]
    return '<tr data-side="' + r['side'] + '" data-group="' + escape(r['group']) + '" data-backtest="' + ('enabled' if enabled else 'disabled') + '" data-setup="' + r['setup_id'] + '">' + ''.join('<td>' + v + '</td>' for v in vals) + '</tr>'

table = '\n'.join(table_row(r) for r in rows)
group_options = ''.join('<option>' + g + '</option>' for g in ['Original sixteen', 'Late six', 'Selected ten', 'Midday twelve', 'Afternoon fifteen'])
source_links = ''.join('<li><a href="../' + p + '">' + p + '</a><code>' + snapshot['source_sha256'][p] + '</code></li>' for p in sources)
html = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>G3 V7 · Detailed configuration · 13 October 2026</title>
<style>
:root{--ink:#17283b;--muted:#52667c;--line:#d8e3eb;--paper:#f3f6f9;--blue:#134e78;--green:#16724e;--orange:#985914}
*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:15px/1.6 system-ui,Segoe UI,sans-serif}
main{max-width:1500px;margin:auto;padding:36px 30px 70px}header{background:#102e49;color:#fff;padding:30px 34px;border-radius:16px}
h1{font-size:34px;line-height:1.15;letter-spacing:-.7px;margin:10px 0 12px}h2{font-size:24px;margin:0 0 12px}h3{font-size:17px;margin:22px 0 7px}
p{margin:9px 0}a{color:#096393}header a{color:#bae6ff}code{font-size:12px;overflow-wrap:anywhere}.eyebrow{font-size:12px;letter-spacing:1.5px;text-transform:uppercase;font-weight:700;color:#b9d5e9}
.tag{display:inline-block;background:#dceefc;color:#144f77;border-radius:99px;padding:3px 11px;font-size:12px;font-weight:700}.tag.status{background:#d7f1e4;color:#155b3f}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:15px;margin:22px 0}.stat{background:white;padding:17px 22px;border:1px solid var(--line);border-radius:12px}.stat b{display:block;font-size:28px;line-height:1.2}.stat span{color:var(--muted);font-size:13px}
nav{display:flex;gap:16px;flex-wrap:wrap;margin:20px 0;font-weight:600}section{background:white;padding:27px 30px;border:1px solid var(--line);border-radius:13px;margin:18px 0;scroll-margin-top:20px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:24px}.note{border-left:4px solid #3189b5;background:#edf6fb;padding:13px 18px;margin:16px 0}.caution{border-left-color:#c38b39;background:#fff6e8}
li{margin:5px 0}ul,ol{padding-left:22px}.formula{background:#f1f5f8;padding:9px 12px;border-radius:6px;font-family:Consolas,monospace;font-size:13px}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{border-bottom:1px solid var(--line);padding:9px 10px;text-align:left;vertical-align:top}th{background:#eaf1f6;color:#24445f}tbody tr:nth-child(even){background:#f7fafc}tbody tr:hover{background:#eaf6ff}
.matrix{overflow:auto;max-height:690px;border:1px solid var(--line);border-radius:8px}.matrix table{font-size:12px;white-space:nowrap}.matrix th{position:sticky;top:0;z-index:1}.matrix td:nth-child(n+4):nth-child(-n+12){text-align:right;font-variant-numeric:tabular-nums}
.side{font-size:11px;font-weight:750}.long{color:var(--green)}.short{color:#a64640}.toolbar{display:flex;gap:10px;flex-wrap:wrap;align-items:end;margin:20px 0 12px}.toolbar label{font-size:12px;font-weight:650}input,select,button{display:block;padding:9px 12px;border:1px solid #becdd9;border-radius:6px;background:white;font:inherit;color:var(--ink)}input{width:255px}button{cursor:pointer}.count{margin-left:auto;font-size:13px;color:var(--muted)}.small{font-size:12px;color:var(--muted)}.sources{font-size:13px}.sources code{display:block;color:var(--muted);font-size:10px}footer{font-size:12px;color:var(--muted)}
@media(max-width:800px){main{padding:18px 12px}header,section{padding:22px 18px}.grid{grid-template-columns:1fr}.stats{grid-template-columns:1fr 1fr}h1{font-size:28px}.count{margin-left:0}}
@media print{@page{size:A3 landscape;margin:12mm}body{background:white;font-size:11px}main{max-width:none;padding:0}header{padding:20px}.toolbar,nav,.print{display:none}.matrix{max-height:none;overflow:visible}.matrix table{font-size:9px}.matrix th{position:static}section{break-inside:auto;border-radius:0;padding:15px}tr{break-inside:avoid}a{color:inherit}.stats{margin:12px 0}.stat b{font-size:22px}}
</style></head><body><main>
<header><div class="eyebrow">Configuration reference · 13 October 2026 · IST</div>
<h1>G3 V7: every selection and execution rule</h1>
<p>Production V7 reference for PAPER and the separately gated LIVE path: all 59 setup definitions remain unchanged. Backtesting now uses the __BACKTEST_REVISION__ subset described below.</p>
<p><span class="tag">V13-V10-G-3-PRODUCTION-V7</span> <span class="tag status">PAPER_VALIDATION · LIVE activation not authorized</span></p>
<p class="small" style="color:#c2d5e4">Generated __GENERATED__ · Snapshot only; creating this report did not change strategy or execution settings.</p></header>
<div class="note caution" id="backtest-scope"><b>Backtesting change: __BACKTEST_COUNT__ enabled / __DISABLED_COUNT__ disabled.</b> Normal historical and dashboard daily backtests use <b>__BACKTEST_REVISION__: target &gt;0.75% plus three restored exceptions</b>: <b>__RESTORED__</b>. Their original targets and selection rules are unchanged. The remaining __DISABLED_COUNT__ setups with target &le;0.75% stay disabled for backtesting. <b>PAPER/LIVE remain on the full 59-setup V7 production configuration, where these three setups are already enabled.</b> The matrix preserves all production values and marks each row's backtest status. <a href="__BACKTEST_CONFIG__">Open the __BACKTEST_REVISION__ backtest configuration</a>.</div>
<div class="stats"><div class="stat"><b>59</b><span>28 LONG + 31 SHORT setups</span></div><div class="stat"><b>40</b><span>signal clocks, 09:20–13:30 IST</span></div><div class="stat"><b>1.25% → 1%</b><span>stop tightens after 120 minutes</span></div><div class="stat"><b>15:15</b><span>square-off time, IST</span></div></div>
<nav><a href="#five">5-minute selection</a><a href="#one">1-minute confirmation</a><a href="#matrix">All 59 setups</a><a href="#risk">Stops &amp; sizing</a><a href="#guards">Guards</a><a href="#limits">Limits &amp; differences</a><a href="#sources">Sources</a></nav>

<section id="five"><h2>5-minute selection</h2>
<p>Price, volume, EMAs, entry and exit paths use <b>NSE cash equities</b>. Open interest comes from the corresponding mapped <b>NFO stock future</b>. Setup times are completed candle ends: a 12:35 signal refers to the cash candle covering 12:30–12:35, followed by confirmation ending 12:36.</p>
<div class="grid"><div><h3>Mandatory direction and OI gates</h3><ul>
<li><b>LONG:</b> EMA9 &gt; EMA20 &gt; EMA50 and positive price change at least the setup threshold.</li>
<li><b>SHORT:</b> EMA9 &lt; EMA20 &lt; EMA50 and negative price change with magnitude at least the setup threshold.</li>
<li>Both sides require positive, increasing OI: previous OI &gt; 0, current OI &gt; previous OI.</li>
<li>OI change must be at least the setup minimum and <b>no more than 1%</b>.</li>
<li>Shared floors: price magnitude 0.10%, OI change 0.05%, 5m volume ratio 0.80×. Setup thresholds can be higher.</li>
<li>All required features must be finite. EMA ordering is strict; equality fails. There is no separate close-above/below-EMA gate in this contract.</li></ul></div>
<div><h3>Feature calculations</h3>
<p class="formula">Price change % = (5m close / previous observed 5m close − 1) × 100</p>
<p class="formula">OI change % = (current futures OI / previous observed futures OI − 1) × 100</p>
<p class="formula">5m volume ratio = current volume / mean(previous 20 observed 5m volumes)</p>
<p class="formula">Traded value = cash 5m close × cash 5m volume</p>
<p>The 5m volume denominator excludes the current candle and allows a minimum of 5 prior observations. EMA9/20/50 use pandas <code>ewm(span, adjust=False)</code> on ordered cash closes. Features can use prior-session history; they do not reset at every market open.</p>
<p>Historical 5m cash bars are constructed from five exact, real 1m rows. Gap-filled, opening-snapshot and stale provisional source rows are excluded. Futures OI joins on the exact candle timestamp.</p></div></div>
<h3>Candidate ranking and quotas</h3>
<p>Eligible stocks are ranked within the complete pool for one date, clock and side. <b>Core candidates receive the quota first</b>; eligible expanded candidates fill only the remaining quota. Ranking is descending configured picker, then descending traded value, then ascending trading symbol.</p>
<ul><li><b>Liquidity</b> = greatest 5m traded value; <b>Volume</b> = greatest 5m volume ratio; <b>Move</b> = greatest absolute 5m price change.</li>
<li>Every setup has minimum traded value <b>₹0</b>: liquidity is normally a ranking criterion, not an independent positive absolute cutoff.</li>
<li>Quota is 1 except 09:25 SHORT and 09:35 SHORT, each quota 2. Total possible selections across all 59 setups are 61 per day before trigger/capital rejection; this is not a guaranteed number of trades.</li>
<li>Only seven original SHORT rows have different core and expanded price thresholds: 09:25, 09:30, 09:40, 09:45, 09:50 and 11:20 use 0.20% core / 0.13% expanded; 09:35 uses 0.50% / 0.325%. New additions already contain effective thresholds and receive no further 0.65 multiplier.</li></ul>
<h3>Clock-specific exceptions</h3>
<p><b>09:20 LONG:</b> price change uses 09:15 cash open to 09:20 cash close. OI uses verified dated first-minute futures evidence (09:15–09:16) to completed 09:20 OI: an actual four-minute comparison. LIVE permits verified minute evidence or a timely provisional full quote; baseline receipt must precede 09:20, with 15-second quote window and maximum receipt lag. Missing or late opening evidence excludes that stock from 09:20; all inputs missing blocks the setup. 09:20 SHORT is disabled.</p>
<p><b>09:25 SHORT:</b> dated near-month NIFTY futures first candle ending 09:20 must return ≤ −0.05% from its own open to close. Missing context rejects. No relaxed 09:25 LONG fallback, EMA bypass, morning-slots branch or two-bar continuation is enabled.</p></section>

<section id="one"><h2>1-minute confirmation</h2><p><b>Exactly signal end + 1 minute.</b> A 12:35 signal can only use the candle ending 12:36. Neither a later candle nor a five-minute wait substitutes for a missing confirmation.</p>
<table><thead><tr><th>Rule</th><th>LONG</th><th>SHORT</th></tr></thead><tbody>
<tr><td>Candle direction</td><td>Close &gt; open</td><td>Close &lt; open</td></tr>
<tr><td>Progress beyond signal</td><td>1m close &gt; signal 5m close</td><td>1m close &lt; signal 5m close</td></tr>
<tr><td>Body fraction</td><td colspan="2">abs(close − open) / (high − low) ≥ setup minimum</td></tr>
<tr><td>Wick fraction</td><td>Upper wick / range ≤ setup maximum</td><td>Lower wick / range ≤ setup maximum</td></tr>
<tr><td>Trigger</td><td>Confirmation high</td><td>Confirmation low</td></tr>
<tr><td>1m volume</td><td colspan="2">Confirmation volume / mean(previous 20 observed completed 1m volumes), excluding current, ≥ setup minimum</td></tr>
</tbody></table>
<p>Upper wick = high − max(open, close). Lower wick = min(open, close) − low. Prices must be finite and positive, high &gt; low, and OHLC bounds valid. A doji fails even if body minimum is zero. Body and wick values in the setup table are shown as <b>percent of the candle range</b>; e.g. 50 means code ratio 0.50. Volume values are multiples, not percentages.</p>
<p>Default 1m volume minima are LONG 1.10× and SHORT 1.20×, but many setups override them. The all-setups table shows the actual resolved minimum for each row. A 0× threshold removes the positive minimum; it does not permit missing/invalid volume ratios, missing history or an invalid candle. There is <b>no separate 1m EMA filter</b>.</p>
<div class="note"><b>Research stages are fixed saved configurations.</b> “strict”, “relax_1m” and “relax_5m” record the historical search stage selected for that setup. The production strategy applies the saved row once; it does not retry with progressively looser thresholds intraday. A 70% win rate was a historical selection objective, not a runtime input or guarantee.</div>
<p><b>Warm-up difference:</b> the historical ratio allows 5 prior observations; the current live/PAPER feed producer requires all 20. It requests 7 calendar days initially, then 35 if needed, and fails closed when fewer than 20 are available. Prior-session observed bars may contribute.</p></section>

<section id="matrix"><h2>Complete setup matrix</h2>
<p>All 59 rows remain in production PAPER/LIVE. The <b>Backtest __BACKTEST_REVISION__</b> column applies only to normal historical and dashboard daily backtests: __BACKTEST_COUNT__ enabled rows include targets &gt;0.75% and the three explicitly restored exceptions above; __DISABLED_COUNT__ other rows with target &le;0.75% remain disabled. Each enabled row inherits the shared direction, OI cap, candle validity, timing and execution rules above. <b>Move min</b> is a positive magnitude: LONG needs ≥ +value; SHORT needs ≤ −value. <b>Core move</b> is applied first for selection priority. Initial SL is the same for every row; targets are the saved optimized values.</p>
<div class="toolbar"><label>Find time or setup<input id="search" type="search" placeholder="e.g. 09:50 or 1236_SHORT"></label><label>Side<select id="side"><option value="">Both sides</option><option>LONG</option><option>SHORT</option></select></label><label>Group<select id="group"><option value="">All groups</option>__GROUP_OPTIONS__</select></label><label>Backtest __BACKTEST_REVISION__<select id="backtest"><option value="">All production rows</option><option value="enabled">Enabled (__BACKTEST_COUNT__)</option><option value="disabled">Disabled (__DISABLED_COUNT__)</option></select></label><button type="button" id="reset">Reset</button><button type="button" onclick="window.print()">Print / save PDF</button><span class="count" id="count">59 of 59 setups</span></div>
<div class="matrix"><table id="setups"><thead><tr><th>Signal<br>IST</th><th>Confirm<br>IST</th><th>Side</th><th>Move min<br>%</th><th>Core move<br>%</th><th>OI min<br>%</th><th>5m volume<br>min ×</th><th>1m body<br>min %</th><th>1m wick<br>max %</th><th>1m volume<br>min ×</th><th>Initial SL<br>%</th><th>Target<br>%</th><th>Backtest<br>__BACKTEST_REVISION__</th><th>Picker</th><th>Quota</th><th>Research stage</th><th>Group</th></tr></thead><tbody>__TABLE__</tbody></table></div>
<p class="small">The snapshot contains 16 original + 6 late + 10 selected + 12 midday + 15 afternoon setups. Unlisted clock/side combinations are disabled. Setup IDs use confirmation time, e.g. 1236_SHORT means signal 12:35 / confirmation 12:36 SHORT.</p>
<p><a href="G3_V7_DETAILED_CONFIGURATION_2026-10-13.json" download>Download full JSON snapshot</a> · <a href="../configs/strategies/v13_v10_g_3_production_v7.json">Canonical production JSON</a></p></section>

<section id="risk"><h2>Entry, stops, targets and capital</h2>
<div class="grid"><div><h3>Entry clocks and orders</h3><ul>
<li>Confirmation processing starts after a 3-second boundary buffer. Exact confirmation and immutable publication must finish by <b>confirmation end + 90 seconds</b>.</li>
<li>The pending trigger/fill window ends at <b>confirmation end + 10 minutes</b>; pending orders cancel when the current time exceeds expiry. Pending orders cannot enter at or after square-off.</li>
<li>For 13:30: confirm 13:31; publish by 13:32:30; pipeline cutoff 13:33; pending trigger expiry 13:41. Management session ends 15:32.</li>
<li>LIVE uses NSE equity MIS/DAY orders. Entry is SL-M while the trigger is untouched; it can use MARKET catch-up if price has already crossed the trigger within the allowed window.</li></ul></div>
<div><h3>Stops and exits</h3><ul>
<li>Initial stop <b>1.25%</b> from actual fill price.</li>
<li>After <b>120 minutes from actual fill</b>, tighten to <b>1.00%</b> from entry; target stays fixed.</li>
<li>LONG: stop = fill × 0.9875; tightened = fill × 0.99; target = fill × (1 + target% / 100).</li>
<li>SHORT: stop = fill × 1.0125; tightened = fill × 1.01; target = fill × (1 − target% / 100).</li>
<li>Levels round to instrument tick; fallback tick is ₹0.05. No strategy partial-profit exit, breakeven move or profit-trailing stop.</li>
<li>Square off at <b>15:15 IST</b>; PAPER needs a valid observed quote, LIVE reconciles and exits residual actual position.</li></ul></div></div>
<p>SL and target percentages refer to the <b>underlying share price</b>, before fees, not return on margin. Example: 09:50 LONG filled at ₹1,000 has initial SL ₹987.50, tightened SL ₹990 and target ₹1,007.50 (0.75%). Stop orders and market exits can fill beyond those nominal levels.</p>
<table><thead><tr><th>Setting</th><th>Historical / PAPER</th><th>LIVE execution policy</th></tr></thead><tbody>
<tr><td>Capital allocation per trade</td><td>₹1,00,000</td><td>₹10,000</td></tr>
<tr><td>Exposure multiplier</td><td>5×</td><td>5×</td></tr>
<tr><td>Target exposure</td><td>₹5,00,000</td><td>₹50,000</td></tr>
<tr><td>Units</td><td>PAPER: floor(₹5,00,000 / trigger). Historical uses a fixed-capital model.</td><td>floor(₹50,000 / trigger), equity lot size 1; override can only reduce quantity.</td></tr>
<tr><td>Aggregate capital admission</td><td>₹10,00,000 across LONG and SHORT; effective 10 concurrently filled positions. Pending orders do not reserve capital.</td><td>No equivalent explicit G3 aggregate capital admission was found in the direct executor. Do not infer a 10-position LIVE cap.</td></tr>
<tr><td>Modeled fees</td><td colspan="2">5 bps round trip of entry notional (0.05% total, not per leg). Fixed ₹5L research exposure costs ₹250; PAPER share rounding changes this slightly; LIVE ₹50k is approximately ₹25 modeled. These are not a broker-actual charge statement.</td></tr>
</tbody></table>
<p><code>MAX_POSITIONS=None</code> means no additional position-count setting, not unlimited PAPER capital. Legacy <code>LIVE_QUANTITY_CAP=1</code> and the coordinator name containing <code>qty1</code> do <b>not</b> impose a one-share cap on current G3 LIVE: its separate reviewed exposure policy controls sizing. Actual notional can differ from trigger-based planned exposure after slippage.</p></section>

<section id="guards"><h2>Guards actually present</h2>
<table><thead><tr><th>Layer</th><th>Current requirements</th></tr></thead><tbody>
<tr><td>Futures signal feed</td><td>Exact-slot final marker, attested universe, successful fetch, no API failure/invalid data; ≥99% stock-futures coverage; at most 2 verified no-candle stocks, each with at least 3 clean fetch attempts/observations. Verified missing stocks are skipped.</td></tr>
<tr><td>Cash signal feed</td><td>Exact-slot final complete marker; all expected cash tickers written/complete; no failed symbols; completed FNO-equity quality gate.</td></tr>
<tr><td>Confirmation feed</td><td>All candidate symbols resolved to an exact written bar or verified no-candle; no arbitrary missing/error/invalid candidates. No-candle proof requires 3 clean observations ≥2 seconds apart, at least 15 seconds after candle end, within the 90-second deadline. There is no global confirmation no-candle cap; verified missing candidate is skipped.</td></tr>
<tr><td>Data provenance</td><td>Hashes bind marker, candidate set, scanner snapshot and actual data bytes. Reject flagged gap-filled/opening-snapshot/stale confirmation data. Exact candle and feature clocks checked; supplied feature availability cannot exceed confirmation.</td></tr>
<tr><td>Identity and duplicate detection</td><td>Canonical strategy/date/slot/side/symbol signal identity; current version and fingerprint required; duplicate candidate within one setup/date/symbol rejected. These are not a cross-setup symbol exposure limit.</td></tr>
<tr><td>PAPER capital and state</td><td>Shared lock for LONG/SHORT fill admission; strict book and fingerprint validation; malformed/stale foreign active state blocks new fills. Existing positions can still exit.</td></tr>
<tr><td>LIVE authorization</td><td>Active deployment owner, LIVE_APPROVED and activation-authorized record, exact acknowledgement, enabled current-date arm file bound to strategy and execution policy, kill switches off; coordinator additionally requires explicit activate-live flag.</td></tr>
<tr><td>Entry submission lock</td><td>Shared cross-process broker entry-owner mutex serializes entry submission and rechecks deployment authority. This lock is not a capital, opposite-side or symbol-risk guard.</td></tr>
<tr><td>Protective exits</td><td>Actual filled quantity/price drive broker SL-M stop and LIMIT target. Partial entry cancels the unfilled balance and waits for acknowledgement before protecting acquired shares. Protective-stop failure invokes square-off handling.</td></tr>
<tr><td>Broker reconciliation</td><td>Deterministic order tags help reconcile uncertain submissions; exits reconcile partial fills, cancel outstanding protections, await acknowledgements and close residual quantity. No native broker OCO is assumed. Residual exit retries are limited to 3 before manual-reconciliation state.</td></tr>
<tr><td>Stop tightening</td><td>Modify the existing broker stop and await acknowledgement. If price has crossed the tightened threshold, cancel target before converting the existing stop to market; avoid blindly submitting a second exit.</td></tr>
</tbody></table>
<div class="note">Saved deployment is <b>PAPER_VALIDATION</b>, with <b>live_activation_authorized=false</b>. The readiness list requires three successful reconciled G3 PAPER sessions, broker reconciliation, explicit LIVE approval and a dated strategy/policy-bound acknowledgement and arm record. This report is a source/configuration snapshot; it does not attest current broker positions or live process health.</div></section>

<section id="limits"><h2>Absent guards and historical/runtime differences</h2>
<p>The direct current G3 runtime does <b>not</b> supply the following protections found in some other engines in this repository. They must not be assumed from similarly named options elsewhere:</p>
<ul><li>No numeric quote-age limit or maximum bid/ask-spread filter was found in its quote execution path.</li><li>No numeric maximum price-chase/slippage rejection for market catch-up entries.</li><li>No cross-setup same-stock cap or opposite-side prohibition; different clocks can select the same stock.</li><li>No explicit aggregate LIVE capital/position admission corresponding to PAPER's ₹10 lakh book.</li><li>No automatic daily-loss circuit breaker in the direct G3 runtime; manual kill switches exist.</li></ul>
<p><b>Historical replay is not an exact live-fill simulation.</b> It uses minute OHLC, adverse gap-open handling and stop-first handling of an ambiguous stop/target bar. The stop-tightening historical clock is the first bar open at or after entry-bar end +120 minutes, while runtime uses actual fill +120 minutes. The confirmation volume warm-up differs (minimum 5 historical, 20 in the current feed producer). Fixed modeled costs do not measure actual quote spread or execution slippage.</p>
<p>The previously audited dataset also contained retrospective 09:20 evidence and after-close whole-stock/day eligibility checks. Those limitations were not corrected by producing this configuration report. Optimized targets and a historical ≥70% win rate are research selections on reused history; neither demonstrates forward profitability or live execution parity.</p></section>

<section id="sources"><h2>Source identity and verification</h2><p>All 59 table rows were generated from the Python decision contract, with resolved setup-specific 1m volume minima. The production JSON was checked for exact equality with that contract. Runtime validation checked the production file bytes and separate LIVE policy. No broker endpoint was called and no strategy setting was edited.</p>
<p><b>Strategy fingerprint</b><br><code>__FINGERPRINT__</code></p><p><b>LIVE execution-policy fingerprint</b><br><code>__EXEC_FINGERPRINT__</code></p>
<p>Source locations for manual review: 5m features <code>fno_oi_hybrid_data.py:316–454</code>; base/selection/ranking <code>fno_v13_v10_g_3_contract.py:215–364</code>; timing/brackets/confirmation <code>fno_v13_v10_g_3_live_config.py:116–250</code>; runtime guards and broker lifecycle in <code>fno_v5_live.py</code>; PAPER admission in <code>fno_v13_v10_g_paper.py:88</code>.</p>
<details><summary>Source files and SHA-256 hashes at generation</summary><ul class="sources">__SOURCE_LINKS__</ul></details></section>
<footer>The original G3 V7 source snapshot and generation time (__GENERATED__) are preserved. Backtest availability is overlaid from the separate __BACKTEST_REVISION__ configuration; backtest fingerprint <code>__BACKTEST_FINGERPRINT__</code>.</footer>
</main><script>
const search=document.getElementById('search'),side=document.getElementById('side'),group=document.getElementById('group'),backtest=document.getElementById('backtest');
const rows=[...document.querySelectorAll('#setups tbody tr')];
function filter(){const q=search.value.toLowerCase().trim();let count=0;for(const row of rows){const matches=(!side.value||row.dataset.side===side.value)&&(!group.value||row.dataset.group===group.value)&&(!backtest.value||row.dataset.backtest===backtest.value)&&(!q||(row.textContent+' '+row.dataset.setup).toLowerCase().includes(q));row.hidden=!matches;if(matches)count++;}document.getElementById('count').textContent=count+' of '+rows.length+' setups';}
for(const control of [search,side,group,backtest])control.addEventListener('input',filter);
document.getElementById('reset').addEventListener('click',()=>{search.value='';side.value='';group.value='';backtest.value='';filter();});
</script></body></html>'''
for token, value in {
    '__GENERATED__': escape(snapshot['generated_at_ist']), '__GROUP_OPTIONS__': group_options,
    '__TABLE__': table, '__SOURCE_LINKS__': source_links,
    '__FINGERPRINT__': contract.strategy_fingerprint(), '__EXEC_FINGERPRINT__': execution.policy_fingerprint(),
    '__BACKTEST_CONFIG__': '../' + BACKTEST_CONFIG.relative_to(ROOT).as_posix(),
    '__BACKTEST_FINGERPRINT__': backtest.strategy_fingerprint(),
    '__BACKTEST_REVISION__': backtest.STRATEGY_VERSION.rsplit('-', 1)[-1],
    '__BACKTEST_COUNT__': str(len(backtest_enabled_ids)), '__DISABLED_COUNT__': str(len(backtest.DISABLED_IDS)),
    '__RESTORED__': escape(restored_text),
}.items():
    html = html.replace(token, value)
assert '__TABLE__' not in html
out.write_text(html, encoding='utf-8')
print(json.dumps({'report': str(out), 'snapshot': str(json_out), 'setups': len(rows),
                  'side_counts': dict(Counter(r['side'] for r in rows)),
                  'group_counts': dict(Counter(r['group'] for r in rows)),
                  'strategy_fingerprint': contract.strategy_fingerprint(),
                  'config_validation': 'PASS', 'total_selection_quota': sum(r['max_entries'] for r in rows)}, indent=2))
