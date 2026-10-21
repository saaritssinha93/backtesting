from __future__ import annotations

from datetime import datetime
import hashlib
import html
import io
import json
from pathlib import Path
import pstats
import shutil
from zoneinfo import ZoneInfo

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
OUT = Path('C:/TradingData/eqidv2/diagnostics/g3_opening_selection_20261015')

def read(name):
    return json.loads((HERE/name).read_text(encoding='utf-8-sig'))

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def table(frame):
    return '<div class="scroll">'+frame.to_html(index=False,border=0,escape=True,
        float_format=lambda v:f'{v:,.4f}')+'</div>'

def code(name, line):
    path = ROOT/name
    return f'<code>{html.escape(name)}:{line}</code>'

a, five, oi = read('audit.json'), read('5min_audit.json'), read('oi_pair_integrity_review.json')
source_changes = [dict(path=path,audit_sha256=digest,report_sha256=sha(path))
    for path,digest in a['source_sha256'].items() if sha(path)!=digest]
# Runtime decision/feed evidence must stay unchanged. Concurrent source edits
# are disclosed and copied separately; they must not rewrite morning evidence.
assert all(Path(item['path']).is_relative_to(ROOT) for item in source_changes), 'Recorded runtime evidence changed'
assert len(a['candidates'])==24 and all(s['probe_reproduced'] and s['selected']==0 for s in a['slots'])
assert five['volume_reconciliation']['all_ratio_matches'] and five['volume_reconciliation']['all_history_hash_matches']
assert len(five['slots']['0925']['short_blocked_only_by_nifty_base_gate'])==21
assert oi['synthetic_reproduction']['passes_all_selected_setup_filters'] is True
assert not (OUT/'manifest.json').exists(), 'Completed report already exists'
OUT.mkdir(parents=True,exist_ok=True)
for name in ('audit.json','5min_audit.json','oi_pair_integrity_review.json','confirmation_candidates.csv','candidate5min.csv'):
    shutil.copy2(HERE/name,OUT/name)
(OUT/'concurrent_source_change.json').write_text(json.dumps(source_changes,indent=2)+'\n',encoding='utf-8')

prof = io.StringIO()
pstats.Stats(str(HERE/'cockpit_profile.prof'),stream=prof).sort_stats('cumulative').print_stats(25)
(OUT/'cockpit_profile.txt').write_text(prof.getvalue(),encoding='utf-8')
log_lines = (ROOT/'logs/log_dashboard_server.log').read_text(encoding='utf-8',errors='replace').splitlines()
selected_log = [(n,line) for n,line in enumerate(log_lines,1)
    if '[DASH_READ]' in line or ('15/Oct/2026 14:' in line and ' 503 ' in line)]
(OUT/'dashboard_latency_evidence.txt').write_text('\n'.join(f'{n}: {line}' for n,line in selected_log[-90:])+'\n',encoding='utf-8')

created = datetime.now(ZoneInfo('Asia/Kolkata')).isoformat()
page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>G3 opening selection audit | 15 October 2026</title>
<style>:root{color-scheme:dark}body{margin:0;background:#0b1220;color:#e5edf7;font:16px/1.6 system-ui,sans-serif}main{max-width:1400px;margin:auto;padding:32px}h1,h2,h3{color:#fff;line-height:1.25}h1{font-size:30px}h2{margin-top:36px;font-size:23px}h3{font-size:18px}p{max-width:1150px}a{color:#8bd7ff}code{background:#202d42;padding:2px 5px;border-radius:4px;overflow-wrap:anywhere}.note{padding:18px;background:#14283b;border-left:4px solid #62c6e8}.issue{border:1px solid #40516b;border-radius:8px;padding:16px 20px;margin:16px 0}.risk{border-left:4px solid #ffbe67}.muted{color:#acbcd1}.scroll{overflow-x:auto;margin:16px 0}table{border-collapse:collapse;font-size:14px;width:100%}th{background:#1d2c42;color:#fff}td,th{text-align:left;padding:10px;border-bottom:1px solid #344257;vertical-align:top}tbody tr:nth-child(even){background:#101c2d}summary{cursor:pointer;color:#8bd7ff;font-weight:600}li{margin:8px 0}footer{margin-top:40px;border-top:1px solid #344257;padding-top:16px}@media(max-width:700px){main{padding:18px}h1{font-size:25px}}@media print{body{background:white;color:#111}h1,h2,h3{color:#111}th,td{background:white!important;color:#111}.note,.issue,code{background:#eee}.scroll{overflow:visible}a{color:#145078}}</style></head><body><main>
<h1>G3 opening selection audit</h1><p class="muted">15 October 2026 · 09:20 and 09:25 signals · 09:21 and 09:26 one-minute confirmation · read-only investigation</p>
<div class="note"><strong>Finding: today’s zero final selections reproduce correctly under the V9 rules used in the saved morning decisions.</strong> The scanner produced 17 and 7 LONG base candidates; all 24 one-minute candles arrived successfully. SHORT was disabled at 09:20 this morning, and the NIFTY opening gate prevented every otherwise base-eligible SHORT at 09:25. Separate data-integrity and dashboard/reporting bugs were found below. This investigation changed no trading rules, workers, orders or active configurations.</div>
<p class="note"><strong>Concurrent configuration change:</strong> while this audit was being assembled, separate source edits introduced production V10 and a 09:20 SHORT setup. The replay and morning evidence in this report are V9; they are not a test of the new V10 configuration. Source hashes are recorded separately. Worker reload and deployment of V10 were not verified.</p>
<h2>What happened in each side and slot</h2>'''
page += table(pd.DataFrame([
    ['09:20 → 09:21','LONG','17 base candidates','17 candles checked','0'],
    ['09:20 → 09:21','SHORT','Setup disabled','No short candidates to confirm','0'],
    ['09:25 → 09:26','LONG','7 base candidates','7 candles checked','0'],
    ['09:25 → 09:26','SHORT','21 pass basic stock gates; all fail NIFTY gate','No short candidates to confirm','0'],
],columns=['Signal → confirmation','Side','Five-minute stage','One-minute stage','Final selections']))
page += '<p><strong>Short explanation:</strong> the configured 09:25 SHORT gate requires the first completed NIFTY futures five-minute return to be ≤ −0.05%. The recorded NIFTY26OCTFUT return at 09:20 was <strong>+0.246010%</strong>. No short reached the one-minute stage; this is not evidence of one-minute short confirmation failing.</p>'
page += '<p>The 21 stocks passed the basic scanner conditions apart from NIFTY. They are not 21 proven missed trades: the stricter setup conditions, one-minute confirmation, entry fills and exits were not established for them.</p>'
page += '<h2>Timing and data availability</h2>'
timing = [{ 'Signal':s['signal_end'],'Scanner published':s['scanner_published'][11:19],
    '1m feed published':s['feed_published'][11:23], 'Confirmation published':s['confirmation_published'][11:19],
    'Candles':s['bars'],'Missing/errors':f"{s['missing_bars']}/{s['errors']}"} for s in a['slots']]
page += table(pd.DataFrame(timing))
page += '<p>All 24 confirmation candles arrived on the first attempt, each with 20 prior volume observations and complete history. Both confirmations completed within nine seconds of their candle close. An independent replay from the saved scanner JSON and hash-verified durable feed reproduces zero selections.</p>'
exclusions = ', '.join(x['equity_symbol'] for x in five['slots']['0920']['opening_0920_exclusions'])
page += f'<p>At 09:20, 204 of 213 contracts were evaluated. Eight were explicitly excluded because no qualifying exchange-timestamped opening OI quote was captured within 09:16:00–09:16:15: <strong>{html.escape(exclusions)}</strong>. KAYNES separately lacked the exact 09:20 futures candle. These are disclosed data-availability exclusions; later historical data cannot establish what was tradable at that time.</p>'
page += '<h2>Why the longs did not qualify</h2><p>The scanner publishes broad candidates before the full setup checks. Of the 17 opening candidates, only INFY, JSWSTEEL, KPITTECH, SUPREMEIND and UPL meet the setup’s five-minute price/OI/volume thresholds; all five fail one-minute checks. Of the seven 09:25 candidates, only TATAELXSI meets those five-minute thresholds; its 09:26 candle fails direction, volume and body checks.</p>'
candidates = pd.DataFrame(a['candidates'])
show = candidates.loc[candidates.tradingsymbol.isin(['INFY','JSWSTEEL','KPITTECH','SUPREMEIND','UPL']) & candidates.signal_end.eq('09:20') | (candidates.tradingsymbol.eq('TATAELXSI') & candidates.signal_end.eq('09:25'))].copy()
show['failed_gates'] = show.failed_gates.map(lambda x:', '.join(g.removeprefix('gate_') for g in x))
page += table(show[['signal_end','tradingsymbol','direction_pass','v9_1m_volume_ratio','minimum_confirmation_volume_ratio','body_ratio','required_body_ratio','wick_ratio','maximum_wick_ratio','failed_gates']])
page += '<p>Conversely, INDHOTEL, GODFRYPHLP and HINDPETRO pass basic one-minute direction plus volume, but fail the setup criteria below. They were correctly not selected, despite missing from the short rejection summary.</p>'
page += table(pd.DataFrame([
    ['09:21','INDHOTEL','OI 0.0528% < 0.10%; 5m volume 1.836× < 3×; body 0.656 < 0.75'],
    ['09:26','GODFRYPHLP','OI 0.0590% < 0.10%; 5m volume 2.150× < 3×; body 0.542 < 0.60'],
    ['09:26','HINDPETRO','5m volume 1.285× < 3×'],
],columns=['Confirmation','Stock','Remaining failed setup gates']))
page += '<h2>Confirmed issues and recommended fixes</h2>'
page += f'''<article class="issue risk"><h3>1. Data integrity: a missing preceding futures candle can silently use yesterday’s OI</h3><p><strong>Observed:</strong> KAYNES 09:25 used previous OI 4,228,350 from 14 October 15:30 against current OI 4,227,600, computing −0.017737%. Its 15 October 09:20 futures candle was absent. The raw quality report correctly says BLOCKED / usable:false, but that report is not enforced as an eligibility gate.</p><p><strong>Cause:</strong> {code('fno_oi_hybrid_data.py',422)} uses the previous observed row and checks finite, positive OI without requiring exact five-minute timestamp adjacency. The quality audit in {code('fno_v5_live.py',838)} records the missing candle separately.</p><p><strong>Impact:</strong> KAYNES failed other gates today, so no wrong selection from this defect was established today. A synthetic, read-only reproduction with prior-day OI 100,000 and current OI 100,500 produces +0.50%, admits a LONG and passes the remaining 09:25 setup filters with a valid supplied confirmation. This establishes a real false-eligibility path.</p><p><strong>Fix:</strong> for ordinary slots, require the same contract’s immediately preceding, completed five-minute candle in the same session. Mark missing pairs DATA_UNAVAILABLE and exclude them. Preserve the separate 09:20 opening-evidence rule; test both overnight gaps and intraday missing bars.</p></article>'''
page += f'''<article class="issue"><h3>2. Rejection summaries omit final setup failures</h3><p>{code('fno_v5_live.py',2425)} builds candidate_rejections from failed basic confirmations and missing candles; {code('fno_eq_id_monitor_detail.py',767)} uses that incomplete map for dashboard counts. Thus the 09:21 map lists 16 of 17 candidates and omits INDHOTEL; the 09:26 map lists 5 of 7 and omits GODFRYPHLP and HINDPETRO. Their detailed feature rows contain the correct failures.</p><p><strong>Fix:</strong> give every evaluated candidate one explicit final disposition, with all failed gates, and derive summary counts from those dispositions. Distinguish setup rejection, quota/ranking loss, unavailable data and selected.</p></article>'''
page += f'''<article class="issue"><h3>3. Disabled shorts can have an empty reason that research presents as PASS</h3><p>At 09:20, 27 rows have no base side and both base-pass flags false, but failed_gates=[] and first_failed_gate is blank. The missing disabled-side gate is in {code('ai_platform/observability/feature_ledger.py',302)}; {code('strategy_research.py',652)} and line 661 map an empty reason to PASS.</p><p><strong>Fix:</strong> record setup-side-enabled and opening-data eligibility explicitly. Never infer PASS from an empty reason when the recorded decision is false. The trading selector correctly excludes these rows today.</p></article>'''
page += f'''<article class="issue"><h3>4. Confirmation counters and a single rejection reason are misleading</h3><p>{code('fno_v5_live.py',2431)} reports directional_confirmed=1 and 2, but actual price-direction passes are 6 and 4. The published counter includes the one-minute volume condition. {code('fno_v13_v10_g_3_live_config.py',243)} also overwrites a direction failure with a volume failure when both fail.</p><p><strong>Fix:</strong> publish separate direction-pass, volume-pass, combined-confirmation and final-setup-pass counts; retain every failed condition. The detailed ledger already has enough evidence to do this without changing trade selection.</p></article>'''
page += f'''<article class="issue risk"><h3>5. Dashboard requests exceed their own waiting/timeout limits</h3><p>The live page displayed <strong>HTTP 503</strong> and UNKNOWN/empty panels during this audit. Server logs around 14:29 show cockpit reads of <strong>25.3–31.1 seconds</strong> and a snapshot read of <strong>30.7 seconds</strong>. Concurrent readers wait only 22 seconds ({code('log_dashboard_server.py',97)}); the cockpit browser request aborts at 25 seconds ({code('dashboard_cockpit.js',165)}).</p><p>A single profiled cockpit load took 22.06 seconds, including 18.88 seconds in build_monitor_detail, 80 phase reads, 8,838 feature rows and 106,056 gate transformations. Profiling adds overhead; these figures locate the repeated work rather than predict ordinary latency. The entire session detail is rebuilt before filtering to the selected stock ({code('dashboard_cockpit.py',437)}).</p><p><strong>Fix:</strong> cache unchanged completed-slot projections by their evidence identity/hash, reuse the same session projection across stock/mode requests, and refresh changing order/price state separately. Align server/client wait budgets and return explicitly timestamped last-good evidence while refreshing. Merely increasing timeouts leaves the repeated work unresolved. This display problem is separate from the scanner’s correct zero-selection outcome.</p></article>'''
page += '<h2>Relevant morning V9 configuration and volume meaning</h2>'
page += table(pd.DataFrame([
    ['09:20','LONG','≥ +0.30%','≥ +0.10%','≥ 3.0×','≥ 1.8×','≥ 0.75','≤ 0.35','None for this LONG'],
    ['09:25','LONG','≥ +0.30%','≥ +0.10%','≥ 3.0×','≥ 1.1×','≥ 0.60','≤ 0.60','None for this LONG'],
    ['09:25','SHORT','≤ −0.13%','≥ +0.05%','≥ 1.5×','≥ 1.2×','≥ 0.40','≤ 0.60','Opening NIFTY return ≤ −0.05%'],
],columns=['Signal','Side','5m price move','Futures OI change','5m volume','1m volume','1m body ratio','Opposing wick ratio','Index condition']))
page += '<p>The early production V9 and historical V11 setup fields agreed when compared before the concurrent V10 edit, excluding their version label. No accidental 3× volume configuration mismatch was found. Five-minute volume ratio = current volume ÷ average of the prior 20 observed real completed five-minute bars (minimum five); current-bar volume is excluded. All 24 original ratios were independently reproduced.</p><p>Today’s 09:20 baseline was 14 October 13:40–15:15; the 09:25 baseline was 19 prior-day bars plus today’s 09:20 bar. It is not same-clock relative volume across prior days. The prior day’s 15:20/25/30 zero-volume gap-filled placeholders are excluded; their upstream cause was not investigated here. The one-minute confirmation histories also cross sessions and contained 20 observations each.</p>'
page += '<h2>Complete stock-level evidence</h2><details><summary>All 24 LONG base candidates and their confirmation checks</summary>'
full = candidates.copy()
full['failed_gates'] = full.failed_gates.map(lambda x:', '.join(g.removeprefix('gate_') for g in x))
page += table(full[['signal_end','tradingsymbol','price_change_pct','oi_change_pct','volume_ratio','direction_pass','v9_1m_volume_ratio','body_ratio','wick_ratio','failed_gates','final_selected']])+'</details>'
page += '<details><summary>The 21 otherwise base-eligible 09:25 SHORT stocks blocked by NIFTY</summary>'
page += table(pd.DataFrame(five['slots']['0925']['short_blocked_only_by_nifty_base_gate']))+'</details>'
page += '<details><summary>The 27 rejected 09:20 rows with missing disabled-side explanations</summary><p>'+html.escape(', '.join(five['slots']['0920']['blank_rejection_reason_symbols']))+'</p></details>'
page += '<h2>Recommended order of work</h2><ol><li>Enforce ordinary OI timestamp adjacency and the associated data-quality exclusion, with targeted regression tests.</li><li>Repair missing/overwritten rejection reasons and misleading counters; retain current selection thresholds.</li><li>Remove repeated dashboard projection work and verify refresh latency under simultaneous requests.</li><li>Only after that, evaluate any desired threshold changes as a separate historical experiment. Do not force opening entries or weaken the NIFTY guard to disguise valid rejections.</li></ol>'
page += '<p>PARTIAL scanner snapshots are currently terminal/fail-closed by an explicit existing test. Neither opening slot was PARTIAL today; this is a separate retry-policy choice, not a demonstrated cause of today’s issue. No production fixes or strategy changes were made during this report-only investigation.</p>'
page += '<footer><p>Generated '+html.escape(created)+'. Source timestamps and original saved decisions are used for the opening analysis; the dashboard performance observations were taken later during this audit.</p><p>Concurrent edits changed the live runtime, contract and live configuration files during report assembly, including new V10 setups and opening-exclusion metadata. They were not made by this investigation. The OI and rejection-summary findings were rechecked after the runtime edit and remained present. Original audit hashes and frozen report-time source copies are recorded separately; the report-time code is not asserted to be the morning executable. Source line references describe the audited versions and may shift under further edits.</p><p>'
links = [('confirmation_candidates.csv','All 24 confirmation candidates'),('candidate5min.csv','Five-minute candidate audit'),('audit.json','Confirmation evidence and hashes'),('5min_audit.json','Short/coverage/volume evidence'),('oi_pair_integrity_review.json','OI gap reproduction'),('cockpit_profile.txt','Dashboard profile'),('dashboard_latency_evidence.txt','Server latency evidence'),('manifest.json','Artifact manifest')]
page += ' · '.join(f'<a href="{name}">{label}</a>' for name,label in links)+'</p></footer></main></body></html>'
(OUT/'report.html').write_text(page,encoding='utf-8')
source_proof = {path:sha(path) for path in a['source_sha256']}
for name in ('fno_oi_hybrid_data.py','fno_eq_id_monitor_detail.py','dashboard_cockpit.py','dashboard_cockpit.js','log_dashboard_server.py','ai_platform/observability/feature_ledger.py','ai_platform/observability/strategy_research.py'):
    source_proof[str(ROOT/name)] = sha(ROOT/name)
source_copies = {}
for source_path in list(source_proof):
    path = Path(source_path)
    if path.is_relative_to(ROOT):
        target = OUT/'source_at_report'/path.relative_to(ROOT)
        target.parent.mkdir(parents=True,exist_ok=True)
        data = path.read_bytes()
        target.write_bytes(data)
        source_proof[source_path] = hashlib.sha256(data).hexdigest()
        source_copies[source_path] = target.relative_to(OUT).as_posix()
manifest = dict(session_date='2026-10-15',created_at_ist=created,scope='READ_ONLY_DIAGNOSIS',execution_authority=False,
    active_files_changed_by_this_audit=False,source_sha256=source_proof,source_copies=source_copies,concurrent_source_changes=source_changes,
    files={p.relative_to(OUT).as_posix():sha(p) for p in OUT.rglob('*') if p.is_file()})
(OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
assert all(sha(OUT/source_copies[path] if path in source_copies else path)==digest for path,digest in source_proof.items())
assert all(sha(OUT/name)==digest for name,digest in manifest['files'].items())
print(json.dumps({'report':str(OUT/'report.html'),'source_hashes_verified':len(source_proof),'artifacts':len(manifest['files']),'all_candidates':len(candidates),'shorts_blocked_by_nifty':21}))
