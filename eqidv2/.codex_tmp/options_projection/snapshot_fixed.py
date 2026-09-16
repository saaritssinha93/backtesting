"""Preserve the already delivered fixed-size projection before adding resizing."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json

BASE=Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g')
OUT=BASE/'options_3lots_5min_20260914_sl12p5_target25'/'one_year_scenarios'
DEST=OUT/'fixed_only_snapshot_before_monthly'
ROOT=Path(__file__).resolve().parents[2]
if DEST.exists():
    raise SystemExit('Snapshot exists; preserving it without rewriting.')
DEST.mkdir()
files=[p for p in OUT.iterdir() if p.is_file()]
files+=[BASE/'V13_V10_G_INTERACTIVE_BACKTEST.html']
for p in files:
    (DEST/p.name).write_bytes(p.read_bytes())
code=DEST/'source_code'
code.mkdir()
for p in [ROOT/'fno_v13_v10_g_options_projection.py',ROOT/'fno_v13_v10_g_options_html.py',ROOT/'tests/test_fno_v13_v10_g_options_projection.py']+list((ROOT/'docs/v13_v10_g_options').glob('options.*')):
    (code/p.name).write_bytes(p.read_bytes())
manifest=dict(created_at=datetime.now(timezone.utc).isoformat(),files={str(p.relative_to(DEST)):hashlib.sha256(p.read_bytes()).hexdigest() for p in DEST.rglob('*') if p.is_file()})
(DEST/'snapshot_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf8')
print(str(DEST))
