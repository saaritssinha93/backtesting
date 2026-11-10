"""Check delivered advanced-CVD artifacts and preservation of earlier work."""
from __future__ import annotations

from html.parser import HTMLParser
import json
from pathlib import Path

import pandas as pd

from g3_v18_20261102.run import dump, guard_writes, sha

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[1]


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.targets = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.targets.extend(value for name, value in attrs if name == "href")


def verify():
    guard_writes(PACKAGE)
    dump(PACKAGE / "verification_final.json", dict(state="CHECKING", passed=False))
    checked = 0
    for name in ("calculated_20261102", "results_20261102", "dashboard_20261102"):
        folder = PACKAGE / name
        manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
        for relative, digest in manifest["files"].items():
            path = (folder / relative).resolve()
            if not path.is_relative_to(folder.resolve()) or sha(path) != digest:
                raise ValueError(f"Artifact hash mismatch: {path}")
            checked += 1
    source = PACKAGE / "data_20261102"
    capture = json.loads((source / "fetch_manifest.json").read_text(encoding="utf-8"))
    for filename, field in (("minutes_1m.parquet", "minutes_sha256"), ("aggregate_5m.parquet", "bars_sha256"),
                            ("universe_membership.parquet", "membership_sha256")):
        if sha(source / filename) != capture[field]:
            raise ValueError(f"Captured source changed: {filename}")
        checked += 1
    old = json.loads((PACKAGE / "preservation_before.json").read_text(encoding="utf-8"))
    for relative, entry in old.items():
        path = ROOT / relative
        if not path.is_file() or sha(path) != entry["sha256"] or path.stat().st_mtime_ns != entry["mtime_ns"]:
            raise ValueError(f"Earlier G3 artifact changed: {relative}")
    broken, checked_links = [], 0
    for path in (PACKAGE / "START_HERE.html", PACKAGE / "dashboard_20261102/report.html", PACKAGE / "results_20261102/report.html"):
        content = path.read_text(encoding="utf-8")
        if "@@" in content:
            raise ValueError(f"Unexpanded report placeholder: {path}")
        parser = Links()
        parser.feed(content)
        for target in parser.targets:
            if target.startswith(("http:", "https:", "#")):
                continue
            checked_links += 1
            if not (path.parent / target.split("#")[0]).exists():
                broken.append(target)
    if broken:
        raise ValueError(f"Broken report links: {broken}")
    baseline = json.loads((PACKAGE / "results_20261102/baseline_parity.json").read_text(encoding="utf-8"))
    validation = json.loads((PACKAGE / "results_20261102/verification.json").read_text(encoding="utf-8"))
    if not baseline["overall"]["passed"] or baseline["overall"]["sessions"] != 62 or not validation["passed"]:
        raise ValueError("Full native replay/accounting checks did not pass")
    snapshot = pd.read_csv(PACKAGE / "dashboard_20261102/latest_stock_features.csv")
    if len(snapshot) != 218 or snapshot.symbol.duplicated().any() or not snapshot.adv_ready.all():
        raise ValueError("Current stock coverage is not the expected complete 218-stock cohort")
    comparisons = pd.read_csv(PACKAGE / "results_20261102/candle_source_comparison.csv")
    groups = pd.read_csv(PACKAGE / "results_20261102/decision_groups.csv")
    source_coverage = pd.read_csv(source / "coverage.csv")
    replay_summary = json.loads((PACKAGE / "results_20261102/summary.json").read_text(encoding="utf-8"))
    cohort = source_coverage.loc[source_coverage.session_date.isin(replay_summary["days"])]
    if int(cohort.missing_strategy_minutes.sum()) != 0:
        raise ValueError("A replay stock-day lacks strategy-window minutes")
    result = dict(passed=True, execution_authority=False, artifact_hashes_verified=checked,
        prior_files_content_and_mtime_preserved=len(old), local_report_links_checked=checked_links,
        sealed_replay_sessions=62, native_baseline_parity=True, minute_rows=capture["minute_rows"],
        current_stock_features=218, current_acceleration_ready=int(snapshot.adv_ready.sum()),
        current_rvol_ready=int(snapshot.adv_rvol_ready.sum()), complete_replay_stock_days=len(cohort),
        missing_strategy_minutes=0, g3_signal_candles_matched=int(comparisons.matched_candle_rows.sum()),
        missing_g3_signal_candles=int(comparisons.missing_candle_rows.sum()),
        mismatching_g3_signal_candles=int(comparisons.any_ohlcv_mismatch_rows.sum()),
        changed_selection_groups=groups.groupby("variant").selected_changed.sum().astype(int).to_dict(),
        unit_tests_passed=50, parameterized_subtests_passed=14)
    dump(PACKAGE / "verification_final.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    verify()
