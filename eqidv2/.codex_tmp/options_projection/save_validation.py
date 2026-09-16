"""Collect completed checks and pin the delivered HTML and model artifacts."""
import hashlib
import json
from datetime import datetime,timezone
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from fno_v13_v10_g_options_html import build_html,strip_options

ROOT=Path(__file__).resolve().parents[2]
TMP=Path(__file__).resolve().parent
BASE=Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g')
OUT=BASE/'options_3lots_5min_20260914_sl12p5_target25'/'one_year_scenarios'
HTML=BASE/'V13_V10_G_INTERACTIVE_BACKTEST.html'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
for name in ['browser_validation.json','projection_source_audit.json','source_calendar_audit.csv']:
    (OUT/name).write_bytes((TMP/name).read_bytes())
counts={}
for name in ['browser_validation.json','projection_source_audit.json','independent_projection_audit.json','independent_monthly_sizing_audit.json','validation.json']:
    audit=json.loads((OUT/name).read_text(encoding='utf8'))
    assert audit['passed'],name
    counts[name]=audit.get('checks_count',0)
    if audit.get('payload_sha256'):
        assert audit['payload_sha256']==sha(OUT/'projection_payload.json'),name
test_xml=ET.parse(TMP/'projection_tests.xml')
testcases=test_xml.findall('.//testcase')
assert testcases and not test_xml.findall('.//failure') and not test_xml.findall('.//error') and not test_xml.findall('.//skipped')
counts['model_tests']=len(testcases)
html=HTML.read_bytes()
payload=json.loads((OUT/'projection_payload.json').read_text(encoding='utf8'))
integration=json.loads((OUT/'html_integration_manifest.json').read_text(encoding='utf8'))
assert strip_options(html)==Path(integration['stock_backup']).read_bytes()
assert build_html(html,payload)==html
assert sha(HTML)==integration['html_sha256']
assert sha(OUT/'projection_payload.json')==integration['payload_sha256']
report=f'''# Options projection validation

Completed for the delivered G HTML and its three sizing policies: fixed three lots, monthly equity-based sizing, and profit-based monthly increases. All use the 12.5% SL / 25% target source.

- {counts['model_tests']} focused model tests passed, covering cash admission, observed exit timing, zero-entry sessions, resizing boundaries, path-specific equity, whole-lot quantities, fees and monthly/annual reconciliation.
- {counts['projection_source_audit.json']} independent source checks passed, covering hashes, source coverage, option fees, cash chronology and the 20-trade / 13-session calendar.
- {counts['independent_projection_audit.json']} independent fixed-projection checks passed. A separate calculation recomputed premium fees and scenario cashflows without importing the model and matched annual/daily/monthly values.
- {counts['independent_monthly_sizing_audit.json']} independent sizing audit checks passed; see the audit for scalar replay and comparison details.
- {counts['browser_validation.json']} browser checks passed in headless Chrome. All three sizing policies, four scenarios and four projection measures, history measures, percentile toggles, CSV/SVG exports, light/dark themes, 390/360-pixel mobile layouts and original stock scenario controls worked. No severe browser errors were reported.
- Desktop, dark and mobile screenshots were visually reviewed. Options sections have no horizontal overflow at either tested mobile width.
- Original stock HTML bytes remain exact after removing tagged options additions. Repeated injection is idempotent. The original stock report has a separate hash-checked backup.

The calculation checks establish internal consistency, not predictive validity. The projection reuses only 20 trades over 13 source sessions. Nine executed August trades use reconstructed metadata, and earlier missing August-expiry history is excluded. Percentile bands are conditional simulation ranges and do not cover unseen market regimes. Larger order quantities are not validated against available volume or bid/ask depth. The Rs5 lakh profit milestone and equity-mode 10% fractional-budget increase cap are explicit illustrative rules.

[Source audit](projection_source_audit.json) · [Fixed projection audit](independent_projection_audit.json) · [Monthly sizing audit](independent_monthly_sizing_audit.json) · [Browser checks](browser_validation.json) · [Source calendar](source_calendar_audit.csv) · [HTML integration manifest](html_integration_manifest.json) · [Delivery hashes](completion_manifest.json)
'''
(OUT/'VALIDATION.md').write_text(report,encoding='utf8')
artifacts=[p for p in OUT.iterdir() if p.is_file() and p.name!='completion_manifest.json']
code=[ROOT/'fno_v13_v10_g_options_projection.py',ROOT/'fno_v13_v10_g_options_html.py',ROOT/'tests/test_fno_v13_v10_g_options_projection.py',TMP/'audit_projection_source.py',TMP/'audit_projection_model.py',TMP/'audit_monthly_resizing.py',TMP/'check_browser.py']+list((ROOT/'docs/v13_v10_g_options').glob('options.*'))
manifest=dict(complete=True,generated_at=datetime.now(timezone.utc).isoformat(),html_file=str(HTML),html_sha256=sha(HTML),stock_bytes_unchanged=True,idempotent=True,checks=counts,artifacts={p.name:sha(p) for p in artifacts},code_sha256={str(p):sha(p) for p in code})
(OUT/'completion_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf8')
print(json.dumps({k:v for k,v in manifest.items() if k not in ['artifacts','code_sha256']},indent=2))
