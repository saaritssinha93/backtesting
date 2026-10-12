"""Local-only opening evidence audit; emits no broker requests or production writes."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time as wall_time, timedelta
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_flow_observer import DEFAULT_ROOT, IST, load_universe, stamp
from dashboard_flow_observer_history import history_path, missing_minutes
from dashboard_flow_observer_store import read_events, read_json, write_json


def audit(data_root, day):
    universe, universe_source = load_universe(data_root, day)
    events = read_events(data_root, str(day))
    boundary = datetime.combine(day, wall_time(9, 15), IST)
    outcomes = []
    for row in universe:
        observations = [event for event in events if event["symbol"] == row["symbol"]
                        and event["contract"] == row["contract"] and event["kind"] == "future"]
        valid = [event for event in observations if event.get("oi") is not None
                 and (exchange := stamp(event.get("exchange_timestamp"))) is not None
                 and (received := stamp(event.get("received_at"))) is not None
                 and boundary - timedelta(seconds=60) <= exchange <= received <= boundary]
        historical = read_json(data_root, history_path(day, row["contract"])) or {}
        missing = missing_minutes(historical.get("candles", []), day)
        outcomes.append({"symbol": row["symbol"], "contract": row["contract"],
            "raw_observations": len(observations), "opening_oi_available": bool(valid),
            "exact_opening_oi_available": any(stamp(event["exchange_timestamp"]) == boundary for event in valid),
            "historical_minutes": 6 - len(missing), "historical_missing_minutes": [x.strftime("%H:%M") for x in missing],
            "reason": None if valid else "No same-contract fresh OI observed and received by 09:15; minute OI is separate evidence"})
    return {"session_date": str(day), "universe_source": universe_source, "universe_count": len(universe),
            "observations": len(events), "opening_oi_available": sum(x["opening_oi_available"] for x in outcomes),
            "exact_opening_oi_available": sum(x["exact_opening_oi_available"] for x in outcomes),
            "complete_opening_minute_histories": sum(x["historical_minutes"] == 6 for x in outcomes),
            "broker_requests": 0, "production_writes": 0, "outcomes": outcomes}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--save", action="store_true", help="Write audit only into observer coverage")
    args = parser.parse_args(argv)
    result = audit(args.data_root, args.date)
    if args.save:
        write_json(args.data_root, f"coverage/audit_{args.date}.json", result)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
