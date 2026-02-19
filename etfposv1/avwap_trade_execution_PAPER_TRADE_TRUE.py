# -*- coding: utf-8 -*-
"""
ETFPOSV1 paper trade entrypoint (legacy filename kept for compatibility).

This strategy is positional and uses the recurring traceable paper trader:
    etf_positional_paper_trader.py

Usage examples:
    python avwap_trade_execution_PAPER_TRADE_TRUE.py
    python avwap_trade_execution_PAPER_TRADE_TRUE.py --run-once
    python avwap_trade_execution_PAPER_TRADE_TRUE.py --interval-sec 30
"""

from __future__ import annotations

from etf_positional_paper_trader import main


if __name__ == "__main__":
    main()
