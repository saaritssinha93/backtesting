from __future__ import annotations

import os
from pathlib import Path


RUNTIME_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))

DATA_5M_DIR = Path(
    os.getenv("EQIDV2_DATA_5M_DIR", str(RUNTIME_ROOT / "stocks_indicators_5min_eq_live2"))
)
DATA_15M_DIR = Path(
    os.getenv("EQIDV2_DATA_15M_DIR", str(RUNTIME_ROOT / "stocks_indicators_15min_eq"))
)
DATA_1MIN_DIR = Path(
    os.getenv("EQIDV2_DATA_1MIN_DIR", str(RUNTIME_ROOT / "stocks_indicators_1min_eq"))
)
REPORTS_DIR = Path(os.getenv("EQIDV2_REPORTS_DIR", str(RUNTIME_ROOT / "reports")))
LIVE_SIGNALS_DIR = Path(
    os.getenv("EQIDV2_LIVE_SIGNALS_DIR", str(RUNTIME_ROOT / "live_signals"))
)
CACHE_15M_DIR = Path(
    os.getenv("EQIDV2_CACHE_15M_DIR", str(RUNTIME_ROOT / "stocks_cache_15min_eq"))
)
CACHE_5MIN_DIR = Path(
    os.getenv("EQIDV2_CACHE_5MIN_DIR", str(RUNTIME_ROOT / "stocks_cache_5min_eq"))
)
SLOT_READY_PENDING_DIR = Path(
    os.getenv("EQIDV2_SLOT_READY_PENDING_DIR",
              str(RUNTIME_ROOT / "slot_ready_5m_pending"))
)
# strategy_v2 C1 — NF (NIFTY guard) writes per-slot ready markers here so DE
# can refuse to run detection on a slot whose NIFTYBEES bar has not been
# confirmed. Without this gate, a stale NIFTYBEES parquet silently drives the
# RS gate (LONG >= 0.75%, SHORT <= -0.75%) with last-known NIFTY, not current.
NIFTY_SLOT_READY_DIR = Path(
    os.getenv("EQIDV2_NIFTY_SLOT_READY_DIR",
              str(RUNTIME_ROOT / "nifty_slot_ready_5m"))
)
# M4 (2026-04-22) — NF slot-fail markers. NF bat writes
# nifty_slot_fail_<yyyymmdd>_<HHMM>.json here when the per-slot retry budget
# is exhausted. DE consumes these markers to run detection with RS=neutral
# (allow both long and short) instead of aborting the slot entirely. See
# strategy_v2 §M4 and run_nifty_guard_fetcher_v16_5min.bat.
NIFTY_SLOT_FAIL_DIR = Path(
    os.getenv("EQIDV2_NIFTY_SLOT_FAIL_DIR",
              str(RUNTIME_ROOT / "nifty_slot_fail_5m"))
)
# Audit #2 (2026-04-22) — NF open-slot markers. The 09:15 slot has no prior
# in-day bar (session start), so the regular ready-marker writer would never
# publish for it. NF instead writes nifty_open_slot_<yyyymmdd>_0915.json here
# so DE proceeds with weak_context=True / neutralize=True instead of aborting
# with NF_STALE. Same handling as M4 (neutral RS), different trigger.
NIFTY_OPEN_SLOT_DIR = Path(
    os.getenv("EQIDV2_NIFTY_OPEN_SLOT_DIR",
              str(RUNTIME_ROOT / "nifty_open_slot_5m"))
)
# Runtime status/heartbeat files live off the OneDrive-synced workspace so
# the Python writer (and its atomic-replace) cannot be blocked by OneDrive
# or Defender transient locks. Still env-overridable for tests.
RUNTIME_STATUS_DIR = Path(
    os.getenv("EQIDV2_RUNTIME_STATUS_DIR", str(RUNTIME_ROOT / "runtime_status"))
)


def runtime_dir(*parts: str) -> Path:
    return RUNTIME_ROOT.joinpath(*parts)


def report_subdir(name: str) -> Path:
    return REPORTS_DIR / name


for _path in (
    RUNTIME_ROOT,
    DATA_5M_DIR,
    DATA_15M_DIR,
    DATA_1MIN_DIR,
    REPORTS_DIR,
    LIVE_SIGNALS_DIR,
    CACHE_15M_DIR,
    CACHE_5MIN_DIR,
    SLOT_READY_PENDING_DIR,
    NIFTY_SLOT_READY_DIR,
    NIFTY_SLOT_FAIL_DIR,
    NIFTY_OPEN_SLOT_DIR,
    RUNTIME_STATUS_DIR,
):
    _path.mkdir(parents=True, exist_ok=True)
