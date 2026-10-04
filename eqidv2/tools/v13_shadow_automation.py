"""Automatically prepare, seal, or finalize V13-V10-G shadow evidence."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ai_platform.observability.shadow_automation import (  # noqa: E402
    automated_finalize,
    automated_prepare,
    automated_seal,
)
from eqidv2_runtime_paths import runtime_dir  # noqa: E402


DEFAULT_SOURCE_ROOT = runtime_dir(
    "fno_oi", "strategy_research", "v13_v10_g_full_history"
)
DEFAULT_LIVE_ROOT = runtime_dir("fno_oi", "v13_v10_g_live")
DEFAULT_REPLAY_ROOT = runtime_dir("backtesting_result_v13_v10_g")
DEFAULT_OUTPUT_ROOT = runtime_dir("v13_v10_g_strategy_research")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--live-root", type=Path, default=DEFAULT_LIVE_ROOT)
    parser.add_argument("--replay-root", type=Path, default=DEFAULT_REPLAY_ROOT)
    parser.add_argument("command", choices=("prepare", "seal", "finalize"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    today_ist = datetime.now(timezone.utc).astimezone().date()
    # Windows hosts are expected to use IST, while an explicit session date is
    # always supplied by scheduled BATs.  The fallback remains useful for a
    # local operator and is replaced below with a timezone-independent value.
    if args.session_date is None:
        from zoneinfo import ZoneInfo

        today_ist = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata")).date()
    session_date = args.session_date or today_ist
    try:
        if args.command == "prepare":
            result = automated_prepare(
                session_date=session_date,
                source_root=args.source_root,
                output_root=args.output_root,
            )
        elif args.command == "seal":
            result = automated_seal(
                session_date=session_date,
                live_root=args.live_root,
                output_root=args.output_root,
            )
        else:
            result = automated_finalize(
                session_date=session_date,
                live_root=args.live_root,
                replay_root=args.replay_root,
                output_root=args.output_root,
            )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "session_date": session_date.isoformat(),
                    "state": "FAILED_CLOSED",
                    "execution_authority": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
