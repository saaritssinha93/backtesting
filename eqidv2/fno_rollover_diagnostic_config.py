"""Shared fail-closed configuration for the Aug 24-25 SEP roll diagnostic."""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from types import ModuleType

import fno_oi_common as common


UNIVERSE_PATH = (
    common.FNO_ROOT
    / "strategy_research"
    / "v8_v9_last_10_backtests"
    / "retrospective_rollover_universe"
    / "near_month_2026-08-24.parquet"
)
UNIVERSE_HASHES = {
    "file_sha256": "7444b185bd85f42df68f791228edb5444e9b0e6cfa959722c73c8a0f684e5902",
    "universe_sha256": "4357d6482c04abd692091d18174ebb269d7d5778a71a74db194ef821a269d7c8",
    "mapped_universe_sha256": "4357d6482c04abd692091d18174ebb269d7d5778a71a74db194ef821a269d7c8",
    "mapped_symbol_set_sha256": "308934dbbb8f1f3c400028def1ea0d617dbc38e9f62b50f96df7d381f93c163a",
}

SOURCE_LIMITATIONS = (
    "RETROSPECTIVELY_RECONSTRUCTED_2026_08_24_SEP_ROLLOVER_UNIVERSE",
    "SEP_IDENTITIES_RECOVERED_FROM_2026_08_26_MASTER_NOT_POINT_IN_TIME",
    "DALBHARAT_EXCLUDED_AFTER_FNO_EXIT",
    "LEGACY_EQUITY_1M_HAS_NO_ROW_LINEAGE_FLAGS",
    "SOURCE_SNAPSHOT_IS_PER_FILE_STABLE_NOT_GLOBAL_TRANSACTION",
)
PROMOTION_BLOCKERS = (
    "RETROSPECTIVELY_RECONSTRUCTED_ROLLOVER_DIAGNOSTIC",
    "DATED_2026_08_24_AND_2026_08_25_UNIVERSES_NOT_ARCHIVED",
    "NOT_POINT_IN_TIME_UNIVERSE",
    "LEGACY_EQUITY_ROW_LINEAGE_UNPROVEN",
    "GLOBAL_PORTFOLIO_LEDGER_USES_CONSERVATIVE_NO_BACKFILL_OVERLAY",
    "PROSPECTIVE_20_SESSIONS_AND_100_FILLS_NOT_COMPLETED",
)


def configure(
    engine: ModuleType,
    *,
    root: Path,
    report_name: str,
    launcher_path: Path,
) -> None:
    """Apply the reconstructed SEP identity after a parent launcher configures V8."""

    launcher_hash = hashlib.sha256(launcher_path.read_bytes()).hexdigest()
    engine.BACKTEST_UNIVERSE_DATE = date(2026, 8, 24)
    engine.BACKTEST_UNIVERSE_PATH = UNIVERSE_PATH
    engine.BACKTEST_CONTRACT_MONTH_FILTER = "26SEP"
    engine.BACKTEST_UNIVERSE_HASHES = dict(UNIVERSE_HASHES)
    engine.OI_INSTRUMENT = (
        "RETROSPECTIVELY_RECONSTRUCTED_ROLLING_26SEP_NFO_FUTURE_RESEARCH_ONLY"
    )
    engine.SOURCE_LIMITATION_LABELS = SOURCE_LIMITATIONS
    engine.BASE_PROMOTION_BLOCKER_LABELS = PROMOTION_BLOCKERS
    engine.STRATEGY_VERSION = (
        f"{engine.STRATEGY_VERSION}_ROLLDIAG_{launcher_hash[:12]}"
    )
    engine.OBJECTIVE = (
        f"{engine.OBJECTIVE};RETROSPECTIVE_SEP_ROLLOVER_DIAGNOSTIC_AUG24_25"
    )
    engine.CONFIG_SOURCE = (
        f"{engine.CONFIG_SOURCE};"
        "RETROSPECTIVELY_RECONSTRUCTED_ROLLOVER_DIAGNOSTIC;"
        f"ROLLOVER_LAUNCHER_SHA256={launcher_hash};"
        f"ROLLOVER_UNIVERSE_FILE_SHA256={UNIVERSE_HASHES['file_sha256']}"
    )
    engine.V8_ROOT = root
    engine.CACHE_DIR = root / "cache"
    engine.SNAPSHOT_ROOT = root / "snapshots"
    engine.RUN_ROOT = root / "runs"
    engine.PROVENANCE_ROOT = root / "provenance"
    engine.REPORT_PATH = common.LATEST_DIR / report_name
    engine.CACHE_MANIFEST_PATH = engine.CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = engine.CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = engine.CACHE_DIR / "same_session_minute_paths.parquet"
    engine.DEFAULT_SOURCE_SNAPSHOT = None
    engine.validate_configuration()

