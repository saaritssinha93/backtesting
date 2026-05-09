"""v17D position check — EOD verification that no positions are stuck open.

Run after 15:25 IST. Reads today's fills.log and checks net position per
ticker is zero. Flags any non-zero net (intraday positions should always
flatten by EOD).

Public API:
    check_open_positions(date=None) -> list of stragglers
    main() — CLI entry, prints + exits non-zero if any straggler found
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from eqidv2 import v17D_logging as logger


def check_open_positions(date: datetime | None = None) -> list:
    """Read fills.log for the day; return list of (ticker, net_qty) stragglers."""
    fills = logger.read_stream("fills", date)
    nets: dict = {}
    for ev in fills:
        ticker = ev.get("ticker")
        side = ev.get("side", "").upper()
        qty = int(ev.get("qty", 0) or 0)
        if not ticker or not side:
            continue
        signed = qty if side == "BUY" else -qty
        nets[ticker] = nets.get(ticker, 0) + signed
    stragglers = [(t, q) for t, q in nets.items() if q != 0]
    return stragglers


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None,
                   help="YYYY-MM-DD; defaults to today")
    args = p.parse_args()

    when = datetime.fromisoformat(args.date) if args.date else None
    stragglers = check_open_positions(when)
    print(f"v17D position check — {(when or datetime.now()).strftime('%Y-%m-%d')}")
    print("=" * 60)
    if stragglers:
        print(f"  STRAGGLERS: {len(stragglers)} ticker(s) with non-zero net")
        for t, q in stragglers:
            print(f"    {t}: net qty = {q:+d}")
        print("\n  ACTION: close manually via broker, then run "
              "v17D_manual_exit to update internal ledger.")
        sys.exit(1)
    else:
        print("  CLEAN — all positions flat at EOD")
        sys.exit(0)


if __name__ == "__main__":
    main()
