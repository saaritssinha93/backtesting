"""Compare the dashboard's refreshed families against its captured old results."""
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import dashboard_flow

ROOT = Path(__file__).resolve().parent
BASELINES = {
    "V13-V10-G": "g-full-8d811c14433c7ff1",
    "V13-V10-G-2": "g2-ac37297b75da85cd",
    "V13-V10-G-3": "g3-e209dbc151112070",
}


def compare_records(before, after, fields, key):
    before, after = sorted(before, key=key), sorted(after, key=key)
    assert len(before) == len(after), (len(before), len(after))
    for old, new in zip(before, after):
        for field in fields:
            left, right = old[field], new[field]
            if isinstance(left, (int, float)):
                assert math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-7), (field, left, right)
            else:
                assert left == right, (field, left, right)


before = json.loads((ROOT / "before_20261009_refresh.json").read_text(encoding="utf-8"))
catalogue = dashboard_flow.load_flow_data()
report = {}
for family, old_id in BASELINES.items():
    run = next(r for r in catalogue["runs"] if r["strategy"] == family)
    current = dashboard_flow.load_flow_data(run["id"])
    old = before[old_id]
    summary = current["summary"]
    assert summary["period_end"] == "2026-10-09", (family, summary["period_end"])
    assert current["warnings"] == [], current["warnings"]
    assert len(current["daily"]) == summary["sessions"]
    assert len(current["trades"]) == summary["trades"]
    for metric in ("net_pnl", "gross_pnl", "cost"):
        for records in (current["daily"], current["trades"]):
            assert math.isclose(sum(r[metric] for r in records), summary[metric], abs_tol=1e-6)
    cutoff = old["summary"]["period_end"]
    compare_records(old["daily"], [r for r in current["daily"] if r["date"] <= cutoff],
                    list(old["daily"][0]), lambda r: r["date"])
    trade_fields = [field for field in old["trades"][0] if field != "id"]
    compare_records(old["trades"], [r for r in current["trades"] if r["date"] <= cutoff],
                    trade_fields, lambda r: (r["date"], r["symbol"], r["side"], r["entry_time"], r["setup"]))
    report[family] = {
        "run": run, "summary": summary, "historical_parity": "PASS",
        "new_sessions": [r for r in current["daily"] if r["date"] > cutoff],
    }
(ROOT / "refresh_validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
