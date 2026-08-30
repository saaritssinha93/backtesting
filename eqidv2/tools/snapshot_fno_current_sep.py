"""Create a physical research snapshot of the current persisted SEP FnO universe.

The V8/V10 backtest engine is intentionally pinned to the 2026-08-11 AUG
universe.  This helper overrides only its source-capture binding so an explicit
same-day diagnostic can use the separately persisted 2026-08-27 SEP universe.
It does not alter any strategy parameters or live files.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import fno_oi_common as common
import fno_v10_unified_5m_1m_backtest as unified


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument(
        "--universe-path",
        type=Path,
        default=common.UNIVERSE_DIR / "near_month_2026-08-27.parquet",
    )
    parser.add_argument("--contract-filter", default="26SEP")
    args = parser.parse_args()

    unified.configure_engine()
    engine = unified.engine
    engine.BACKTEST_UNIVERSE_PATH = args.universe_path.resolve()
    engine.BACKTEST_CONTRACT_MONTH_FILTER = str(args.contract_filter).upper()
    engine.OI_INSTRUMENT = "STATIC_CURRENT_26SEP_NFO_FUTURE_TODAY_RESEARCH_ONLY"
    engine.CONFIG_SOURCE += (
        ";CURRENT_DIAGNOSTIC_SOURCE_CAPTURE_ONLY;"
        f"UNIVERSE={engine.BACKTEST_UNIVERSE_PATH};"
        f"CONTRACT_FILTER={engine.BACKTEST_CONTRACT_MONTH_FILTER}"
    )
    mapped, universe_record = engine.provenance.load_backtest_universe(
        universe_path=engine.BACKTEST_UNIVERSE_PATH,
        contract_month_contains=engine.BACKTEST_CONTRACT_MONTH_FILTER,
        require_persisted_mapping=True,
    )
    result = engine.provenance.create_source_snapshot(
        mapped,
        universe_record,
        universe_path=engine.BACKTEST_UNIVERSE_PATH,
        snapshot_root=args.snapshot_root.resolve(),
        require_complete_sources=True,
    )
    manifest_path = Path(result["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    universe = manifest.get("universe", {})
    if universe.get("contract_month_filter") != engine.BACKTEST_CONTRACT_MONTH_FILTER:
        raise AssertionError("snapshot contract filter is not current SEP")
    if int(universe.get("mapped_stock_futures", 0)) < 200:
        raise AssertionError("snapshot current stock-futures universe is unexpectedly small")
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
