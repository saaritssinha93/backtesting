"""Apply the frozen exact-5% convention to already-resolved result CSVs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


BASE = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_long_5m_gt5pct_20260205_20260804"
)
SCRIPT = Path(__file__).with_name("research_prefilter_long_5m_gt5pct.py")
FIRST_HIT_COLUMNS = (
    "first_hit_5pct_time_ist",
    "first_hit_5pct_bar_end_ist",
    "first_hit_5pct_interval_start_ist",
    "first_hit_5pct_interval_end_ist",
    "first_hit_5pct_time_source",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    repaired: dict[str, int] = {}
    for name in (
        "all_long_prefilter_entries_with_daily_max.csv",
        "setup_entries_with_daily_max.csv",
        "gt5pct_movers_full_list.csv",
        "setup_gt5pct_movers.csv",
        "data_quality_review_entries.csv",
    ):
        path = BASE / name
        frame = pd.read_csv(path, low_memory=False)
        hit = frame["hit_5pct"].astype(str).str.lower().eq("true")
        inconsistent = ~hit & frame["first_hit_5pct_bar_end_ist"].notna()
        repaired[name] = int(inconsistent.sum())
        for column in FIRST_HIT_COLUMNS:
            frame.loc[~hit, column] = "" if column.endswith("_source") else pd.NA
        frame.to_csv(path, index=False)

    manifest_path = BASE / "causal_entry_opportunities_v2_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["script_sha256"] = file_sha256(SCRIPT)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({"repaired_nonhit_timestamps": repaired}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
