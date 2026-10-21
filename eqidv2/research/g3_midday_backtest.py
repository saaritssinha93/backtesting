"""Compatibility imports for the archived 44-setup G3 V6 historical study.

The current normal historical run is research.g3_afternoon_backtest (V7).
"""
from research.archived_g3_v6.midday_backtest import FILES, ROOT, verified_package


def run(output):
    raise ValueError("The archived V6 study is verification-only; run fno_v13_v10_g_3_backtest for current V7 history")
