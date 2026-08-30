"""V8-Combined: train-selected per-leg V8 configuration on the V8 engine.

This research launcher freezes the ten-leg book chosen by comparing the
existing V8-Strict and retuned-V8 configurations separately on the first 30
sessions of the common 40-session window.  It does not import either strategy
launcher: every setup is literal and hash-pinned below.

Selection lineage
-----------------
* Retuned V8: 09:25 LONG, 09:25 SHORT, 09:30 SHORT.
* V8-Strict: 09:40 SHORT (the less-weak training configuration; it did not
  achieve positive training expectancy and remains diagnostic only).
* Common/identical in both books: the other six legs.

The combined portfolio must be replayed chronologically because duplicate,
margin and concurrency constraints make per-leg P&L non-additive.  All cache,
run, provenance and rolling-report paths are isolated from the parent books.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as engine


STRATEGY_FAMILY = "FNO_V8_COMBINED_BEST_PER_LEG_20260820"
COMBINED_SETUP_BOOK_SHA256 = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)

S = engine.V8Setup
COMBINED_SETUPS: tuple[engine.V8Setup, ...] = (
    # Retuned 09:25 LONG.
    S(
        "09:25", "LONG", 4, "max_move", 0.30, 0.10, 3.0, 0.00, 0.50,
        0.0, 0.40, 1.0, entry_conf_minute=3, entry_buffer_bps=0.0,
        entry_midpoint=False, entry_clv=None,
    ),
    # Retuned 09:25 SHORT.
    S(
        "09:25", "SHORT", 4, "max_move", 0.20, 0.10, 1.5, 0.60, 0.60,
        25_000_000.0, 0.50, 3.0, entry_conf_minute=3,
        entry_buffer_bps=2.0, entry_midpoint=False, entry_clv=None,
    ),
    # Common 09:30 LONG.
    S("09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.50, 0.50, 0.0, 1.00, 2.5),
    # Retuned 09:30 SHORT.
    S(
        "09:30", "SHORT", 4, "max_volume", 0.20, 1.00, 1.0, 0.45, 0.30,
        25_000_000.0, 1.00, 4.0, entry_conf_minute=3,
        entry_buffer_bps=0.0, entry_midpoint=True, entry_clv=0.50,
    ),
    # Common 09:35 LONG/SHORT.
    S("09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.60, 0.50, 0.0, 1.00, 2.5),
    S("09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    # Common 09:40 LONG; strict 09:40 SHORT.
    S("09:40", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.50, 0.50, 0.0, 0.50, 2.5),
    S("09:40", "SHORT", 1, "max_move", 0.20, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    # Common 09:45 LONG/SHORT.
    S("09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.40, 0.30, 0.0, 1.00, 2.0),
)

ROOT = common.FNO_ROOT / "strategy_research" / "v8_combined_best_per_leg_v1"


def launcher_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def setup_payload() -> list[dict[str, object]]:
    return [asdict(setup) for setup in COMBINED_SETUPS]


def validate_launcher_configuration() -> None:
    observed = common.canonical_json_sha256(setup_payload())
    if observed != COMBINED_SETUP_BOOK_SHA256:
        raise AssertionError(
            "V8-Combined setup book changed: "
            f"expected {COMBINED_SETUP_BOOK_SHA256}, observed {observed}"
        )
    if len(COMBINED_SETUPS) != 10:
        raise AssertionError("V8-Combined requires ten setup legs")
    if len({setup.setup_id for setup in COMBINED_SETUPS}) != 10:
        raise AssertionError("V8-Combined setup IDs must be unique")


def configure_engine() -> None:
    """Install the immutable combined book into the neutral V8 engine."""

    validate_launcher_configuration()
    launcher_hash = launcher_sha256()
    engine.STRATEGY_VERSION = f"{STRATEGY_FAMILY}_{launcher_hash[:12]}"
    engine.OBJECTIVE = "TRAIN_SELECTED_PER_LEG_CONFIGURATION_ON_V8_EXECUTION"
    engine.CONFIG_SOURCE = (
        "LITERAL_TRAIN_SELECTED_STRICT_RETUNED_PER_LEG_BOOK;"
        f"LAUNCHER_SHA256={launcher_hash}"
    )
    engine.CACHE_SCHEMA_VERSION = "fno_v8_combined_best_per_leg_cache_manifest_v1"
    engine.PATH_POLICY_VERSION = "fno_v8_combined_same_session_exact_grid_ohlcvt_v1"
    engine.ACTIVE_SETUPS = COMBINED_SETUPS
    engine.V8_SETUP_BOOK_SHA256 = COMBINED_SETUP_BOOK_SHA256
    engine.VARIANT_REGISTRY = {
        "VC": {
            "description": (
                "Train-selected per-leg strict/retuned V8 book; global B0 entry "
                "policy with literal per-leg overrides"
            ),
            "max_confirmation_minute": 1,
            "buffer_bps": 0.0,
            "midpoint_invalidation": False,
            "close_location_min": None,
        }
    }
    engine.V8_ROOT = ROOT
    engine.CACHE_DIR = ROOT / "cache"
    engine.SNAPSHOT_ROOT = ROOT / "snapshots"
    engine.RUN_ROOT = ROOT / "runs"
    engine.PROVENANCE_ROOT = ROOT / "provenance"
    engine.REPORT_PATH = common.LATEST_DIR / "latest_fno_v8_combined_best_per_leg.md"
    engine.CACHE_MANIFEST_PATH = engine.CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = engine.CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = engine.CACHE_DIR / "same_session_minute_paths.parquet"
    engine.DEFAULT_SOURCE_SNAPSHOT = None
    engine.validate_configuration()


def _inject_combined_variant(argv: Sequence[str]) -> list[str]:
    args = list(argv)
    if args and args[0] in {"run", "smoke"} and "--variant" not in args:
        args.extend(["--variant", "VC"])
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_engine()
    args = _inject_combined_variant(sys.argv[1:] if argv is None else argv)
    return engine.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
