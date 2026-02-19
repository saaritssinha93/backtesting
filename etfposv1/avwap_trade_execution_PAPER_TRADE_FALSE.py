# -*- coding: utf-8 -*-
"""
ETFPOSV1 execution entrypoint (legacy filename kept for compatibility).

The old AVWAP real-order executor is intentionally retired for ETFPOSV1.
This positional strategy runs through the recurring paper-trading tracker:
    etf_positional_paper_trader.py

Usage examples:
    python avwap_trade_execution_PAPER_TRADE_FALSE.py
    python avwap_trade_execution_PAPER_TRADE_FALSE.py --run-once
    python avwap_trade_execution_PAPER_TRADE_FALSE.py --interval-sec 60
"""

from __future__ import annotations

import argparse

from etf_positional_paper_trader import main as paper_main


def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--allow-real-orders",
        action="store_true",
        help="Not supported for ETFPOSV1 (kept only for explicit safety).",
    )
    args, _ = parser.parse_known_args()
    if args.allow_real_orders:
        raise SystemExit(
            "Real-order execution is disabled for ETFPOSV1 in this script. "
            "Use paper tracking only."
        )
    paper_main()


if __name__ == "__main__":
    main()
