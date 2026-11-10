"""Verify replay artifacts, report links and preservation of earlier experiments."""
from __future__ import annotations

import csv
import hashlib
import html
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parent.parent
RESULTS = ROOT / "results_20261102"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def main():
    prior = json.loads((ROOT / "preservation_before.json").read_text(encoding="utf-8"))
    failures = []
    for name, expected in prior.items():
        path = WORKSPACE / name
        if not path.is_file():
            failures.append({"file": name, "reason": "missing"})
        elif digest(path) != expected["sha256"] or path.stat().st_mtime_ns != expected["mtime_ns"]:
            failures.append({"file": name, "reason": "hash_or_modification_time_changed"})
    manifest = json.loads((RESULTS / "manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["files"].items():
        path = RESULTS / name
        if not path.is_file() or digest(path) != expected:
            failures.append({"file": name, "reason": "result_manifest_mismatch"})
    summary = json.loads((RESULTS / "summary.json").read_text(encoding="utf-8"))
    days = summary["days"]
    if len(days) != 62 or days != sorted(set(days)) or days[0] != "2026-07-29" or days[-1] != "2026-10-30":
        failures.append({"reason": "unexpected_replay_calendar"})
    with (RESULTS / "daily_results.csv").open(encoding="utf-8-sig",newline="") as stream:
        daily = list(csv.DictReader(stream))
    for variant in summary["variants"]:
        for cost in (5,9,15):
            actual = sorted(r["day"] for r in daily if r["variant"] == variant and float(r["cost_bps"]) == cost)
            if actual != days:
                failures.append({"reason":"daily_calendar_mismatch", "variant":variant, "cost_bps":cost})
    link_count = 0
    reports = [ROOT / "START_HERE.html", ROOT / "dashboard_20261102/report.html", RESULTS / "report.html"]
    for report in reports:
        content = report.read_text(encoding="utf-8")
        for href in re.findall(r'href=[\"\']([^\"\']+)', content):
            href = html.unescape(href).split("#",1)[0].split("?",1)[0]
            if not href or href.startswith(("https:","http:","mailto:")):
                continue
            destination = (report.parent / href).resolve()
            link_count += 1
            if destination == ROOT / "final_verification.json":
                continue
            if not destination.is_file():
                failures.append({"file":str(report.relative_to(ROOT)),"reason":"broken_link", "link":href})
    if not summary["baseline_parity"]["passed"]:
        failures.append({"reason":"baseline_parity_failed"})
    record = {
        "verified_at_ist":datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "passed":not failures,
        "prior_files_hash_and_mtime_unchanged":len(prior) if not any(f.get("file") in prior for f in failures) else False,
        "result_artifacts_verified":len(manifest["files"]),
        "full_calendar_sessions":len(days),
        "variants":len(summary["variants"]),
        "cost_scenarios":3,
        "daily_result_rows":len(daily),
        "local_report_links_checked":link_count,
        "baseline_parity":summary["baseline_parity"],
        "failures":failures,
        "execution_authority":False,
    }
    (ROOT / "final_verification.json").write_text(json.dumps(record,indent=2)+"\n",encoding="utf-8")
    if failures:
        raise RuntimeError(json.dumps(record))
    published = [ROOT / name for name in ("__init__.py","README.md","START_HERE.html","make_overview.py","verify_publication.py","methodology.json","preservation_before.json","final_verification.json","selection.py","run.py","tests_selection.py","tests_runner.py","dashboard_20261102/report.html","results_20261102/manifest.json")]
    for optional in ("validation_tests.txt","report_preview.png"):
        if (ROOT / optional).is_file():
            published.append(ROOT / optional)
    publication = {"files":{p.relative_to(ROOT).as_posix():digest(p) for p in published},"execution_authority":False}
    (ROOT / "publication_manifest.json").write_text(json.dumps(publication,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(record))


if __name__ == "__main__":
    main()
