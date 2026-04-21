from __future__ import annotations

import os
from pathlib import Path


RUNTIME_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))

DATA_5M_DIR = Path(
    os.getenv("EQIDV2_DATA_5M_DIR", str(RUNTIME_ROOT / "stocks_indicators_5min_eq"))
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
DATA_5M_PENDING_DIR = Path(
    os.getenv("EQIDV2_DATA_5M_PENDING_DIR",
              str(RUNTIME_ROOT / "stocks_indicators_5min_eq_pending"))
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
    DATA_5M_PENDING_DIR,
    SLOT_READY_PENDING_DIR,
    NIFTY_SLOT_READY_DIR,
    RUNTIME_STATUS_DIR,
):
    _path.mkdir(parents=True, exist_ok=True)
