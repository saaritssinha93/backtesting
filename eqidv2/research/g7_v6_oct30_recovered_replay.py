"""Run the dated G3 source reconstruction on isolated recovered October 30 cash bars.

This proves source coverage for later G7 V6 research. It does not itself
produce a G7 V4/V6 portfolio result.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fno_v13_replay_source as source
import fno_v13_v10_g_3_daily_replay as replay

OUT = ROOT / "outputs/g7_v6_20261102"
CASH = OUT / "oct30_retrieved_cash"
OPENING = OUT / "oct30_retrieved_opening"
RUN = OUT / "source_20261030_recovered_with_opening"


def main() -> None:
    provenance_path = CASH / "retrieval_provenance.json"
    retrieval = json.loads(provenance_path.read_text(encoding="utf-8"))
    if retrieval["universe_count"] != 218 or retrieval["accepted"] != 218:
        raise ValueError("Recovered cash universe incomplete")
    opening_path = OPENING / "retrieval_provenance.json"
    opening = json.loads(opening_path.read_text(encoding="utf-8"))
    if opening["universe_count"] != 218 or opening["accepted"] < 1:
        raise ValueError("Recovered futures opening evidence unavailable")
    for record in retrieval["records"]:
        path = Path(record["output_path"])
        if not path.resolve().is_relative_to(CASH.resolve()):
            raise ValueError("Recovered source escaped isolated research root")
        if hashlib.sha256(path.read_bytes()).hexdigest() != record["output_sha256"]:
            raise ValueError("Recovered source hash changed: " + record["symbol"])
    if RUN.exists():
        raise FileExistsError("Use a fresh recovered replay directory")
    roots = source.DataRoots(equity_1m=CASH, opening_observer=OPENING / "opening_observer")
    result = replay.replay_day(date(2026, 10, 30), RUN, roots=roots)
    compact = {"state": result["state"], "complete": result["complete"],
               "session_date": result["session_date"],
               "blocking_problem_count": len(result["coverage"]["blocking_problems"]),
               "retrieval_provenance": str(provenance_path),
               "retrieval_provenance_sha256": hashlib.sha256(provenance_path.read_bytes()).hexdigest(),
               "opening_retrieval_provenance": str(opening_path),
               "opening_retrieval_provenance_sha256": hashlib.sha256(opening_path.read_bytes()).hexdigest(),
               "retrospective_opening_files": opening["accepted"],
               "live_5m_conflicts": retrieval["status_counts"].get("accepted_with_live_5m_conflict", 0)}
    (RUN / "recovery_link.json").write_text(json.dumps(compact, indent=2), encoding="utf-8")
    print(json.dumps(compact, indent=2))


if __name__ == "__main__":
    main()
