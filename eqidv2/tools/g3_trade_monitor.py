"""Refresh the isolated read-only G3 entered-trade monitor every 15 seconds."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time as day_time
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import g3_trade_monitor as monitor


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=int, choices=(15,), default=15)
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--end-time", type=day_time.fromisoformat, default=day_time(15, 35))
    parser.add_argument("--output-root", type=Path, default=monitor.DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--market-data", action="store_true", help="Refresh isolated read-only quotes and completed minute evidence")
    args = parser.parse_args(argv)
    if args.end_time.tzinfo is not None:
        parser.error("--end-time is an IST wall-clock time without timezone")
    try:
        with monitor.worker_lock(args.output_root):
            feed = None
            if args.market_data:
                from g3_trade_monitor_market_data import MarketDataFeed
                feed = MarketDataFeed(args.output_root, credential_dir=Path(__file__).resolve().parents[1], app="app1")
            deadline = time.monotonic()
            while True:
                now = datetime.now(monitor.IST)
                inputs = dict(quote_root=args.output_root / "quotes", minute_cache_root=args.output_root / "minutes")
                snapshot = monitor.build_snapshot(args.date, now=now, **inputs)
                if feed is not None and (args.date is None or args.date == now.date()):
                    try:
                        status = feed.refresh(snapshot, now=now)
                    except Exception as exc:
                        # The monitor continues with explicitly stale evidence;
                        # never include credential/client exception messages.
                        status = dict(state="ERROR", error_type=type(exc).__name__)
                    snapshot = monitor.build_snapshot(args.date, now=datetime.now(monitor.IST), **inputs)
                    snapshot["market_data"] = status
                    snapshot["market_data_feed"] = status
                output = monitor.write_snapshot(snapshot, args.output_root)
                print(json.dumps(dict(state=snapshot["state"], session_date=snapshot["session_date"],
                    generated_at_ist=snapshot["generated_at_ist"], summary=snapshot["summary"],
                    execution_authority=False, **output), allow_nan=False), flush=True)
                if (args.once or args.date is not None and args.date != now.date()
                        or now.time().replace(tzinfo=None) >= args.end_time):
                    return 0
                deadline += args.interval
                current = time.monotonic()
                if deadline <= current:
                    deadline = current + args.interval
                time.sleep(deadline - current)
    except monitor.AlreadyRunning:
        print(json.dumps(dict(state="ALREADY_RUNNING", execution_authority=False)), flush=True)
        return 0
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
