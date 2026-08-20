"""V8-Strict: the frozen V6 setup book executed by the independent V8 engine.

This launcher deliberately imports no V6/V7 strategy or cache module.  The ten
setup legs are literal below and hash-pinned.  The shared dependency is only
the independent V8 chronology/execution engine.  All cache, run, provenance
and rolling-report paths are redirected to a V8-Strict-only namespace before
the engine validates or reads any source artifact.

Policy delta versus the retuned V8 B0 book:

* restore the original ten V6 five-minute setup legs, ranks, caps and brackets;
* require the exact S+1 strict direction/displacement/body/wick confirmation;
* use a raw confirmation high/low trigger with no per-leg entry override;
* retain V8 same-session paths, S+5 expiry, gap-aware fills, actual-fill
  brackets, portfolio/duplicate controls, provenance and fail-closed coverage.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as engine


STRATEGY_FAMILY = "FNO_V8_STRICT_V6_LOGIC_20260820"
STRICT_SETUP_BOOK_SHA256 = (
    "5de61f611ad30b52d303b2075ee169421f1208c5026789a78ce4907f35c16919"
)

S = engine.V8Setup
STRICT_SETUPS: tuple[engine.V8Setup, ...] = (
    S("09:25", "LONG", 1, "max_liquidity", 0.30, 0.10, 3.0, 0.60, 0.50, 0.0, 0.50, 3.0),
    S("09:25", "SHORT", 2, "max_volume", 0.20, 0.10, 1.5, 0.40, 0.50, 0.0, 0.75, 3.0),
    S("09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.50, 0.50, 0.0, 1.00, 2.5),
    S("09:30", "SHORT", 1, "max_move", 0.20, 0.25, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.60, 0.50, 0.0, 1.00, 2.5),
    S("09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:40", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.50, 0.50, 0.0, 0.50, 2.5),
    S("09:40", "SHORT", 1, "max_move", 0.20, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.40, 0.30, 0.0, 1.00, 2.0),
)

ROOT = common.FNO_ROOT / "strategy_research" / "v8_strict_v6_logic_v1"


def launcher_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def setup_payload() -> list[dict[str, object]]:
    return [asdict(setup) for setup in STRICT_SETUPS]


def validate_launcher_configuration() -> None:
    observed = common.canonical_json_sha256(setup_payload())
    if observed != STRICT_SETUP_BOOK_SHA256:
        raise AssertionError(
            "V8-Strict setup book changed: "
            f"expected {STRICT_SETUP_BOOK_SHA256}, observed {observed}"
        )
    if len(STRICT_SETUPS) != 10 or len({setup.setup_id for setup in STRICT_SETUPS}) != 10:
        raise AssertionError("V8-Strict requires ten unique literal setup legs")
    if any(setup.overrides_entry_policy for setup in STRICT_SETUPS):
        raise AssertionError("V8-Strict setup legs may not override the global S+1 policy")


def configure_engine() -> None:
    """Install the immutable V8-Strict policy into the neutral V8 engine."""

    validate_launcher_configuration()
    launcher_hash = launcher_sha256()
    engine.STRATEGY_VERSION = f"{STRATEGY_FAMILY}_{launcher_hash[:12]}"
    engine.OBJECTIVE = "V6_STRICT_SIGNAL_QUALITY_ON_V8_CAUSAL_EXECUTION"
    engine.CONFIG_SOURCE = (
        "LITERAL_V6_STRICT_BOOK_ON_V8_ENGINE;"
        f"LAUNCHER_SHA256={launcher_hash}"
    )
    engine.CACHE_SCHEMA_VERSION = "fno_v8_strict_cache_manifest_v1"
    engine.PATH_POLICY_VERSION = "fno_v8_strict_same_session_exact_grid_ohlcvt_v1"
    engine.ACTIVE_SETUPS = STRICT_SETUPS
    engine.V8_SETUP_BOOK_SHA256 = STRICT_SETUP_BOOK_SHA256
    engine.VARIANT_REGISTRY = {
        "VS": {
            "description": (
                "V6 strict exact S+1 confirmation on V8 causal execution, "
                "raw high/low trigger, S+5 expiry"
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
    engine.REPORT_PATH = common.LATEST_DIR / "latest_fno_v8_strict_v6_logic.md"
    engine.CACHE_MANIFEST_PATH = engine.CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = engine.CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = engine.CACHE_DIR / "same_session_minute_paths.parquet"
    engine.DEFAULT_SOURCE_SNAPSHOT = None
    engine.validate_configuration()


def _inject_strict_variant(argv: Sequence[str]) -> list[str]:
    args = list(argv)
    if args and args[0] in {"run", "smoke"}:
        if "--variant" not in args:
            args.extend(["--variant", "VS"])
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_engine()
    args = _inject_strict_variant(sys.argv[1:] if argv is None else argv)
    return engine.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
