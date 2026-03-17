from __future__ import annotations

import os
from pathlib import Path


RUNTIME_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))

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


def runtime_dir(*parts: str) -> Path:
    return RUNTIME_ROOT.joinpath(*parts)


def report_subdir(name: str) -> Path:
    return REPORTS_DIR / name


for _path in (
    RUNTIME_ROOT,
    DATA_15M_DIR,
    DATA_1MIN_DIR,
    REPORTS_DIR,
    LIVE_SIGNALS_DIR,
    CACHE_15M_DIR,
    CACHE_5MIN_DIR,
):
    _path.mkdir(parents=True, exist_ok=True)
