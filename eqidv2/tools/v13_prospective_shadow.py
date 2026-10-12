"""Capture or verify no-authority V13-V10-G prospective shadow evidence."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fno_strategy_registry import PROFILES, active_profile_name  # noqa: E402


def runtime_dir(*parts: str) -> Path:
    return Path(os.getenv("EQIDV2_RUNTIME_ROOT", "C:/TradingData/eqidv2")).joinpath(*parts)


DEFAULT_OUTPUT_ROOT = runtime_dir("v13_v10_g_strategy_research")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=tuple(PROFILES), default=None)
    parser.add_argument("--output-root", type=Path)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Freeze inputs before 09:15 IST")
    prepare.add_argument("--session-date", required=True, type=date.fromisoformat)
    prepare.add_argument("--dataset", type=Path)
    prepare.add_argument("--model", type=Path)
    prepare.add_argument("--strategy", type=Path)

    decisions = subparsers.add_parser(
        "seal-decisions", help="Seal a complete decision bundle before 15:30 IST"
    )
    decisions.add_argument("--session-date", required=True, type=date.fromisoformat)
    decisions.add_argument("--decisions", type=Path)

    finalize = subparsers.add_parser(
        "finalize", help="Join a complete outcome bundle after 15:30 IST"
    )
    finalize.add_argument("--session-date", required=True, type=date.fromisoformat)
    finalize.add_argument("--outcomes", type=Path)

    verify = subparsers.add_parser("verify", help="Verify final artifact and journal hashes")
    verify.add_argument("--session-date", required=True, type=date.fromisoformat)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    profile = args.profile or active_profile_name()
    args.output_root = args.output_root or runtime_dir(PROFILES[profile].slug + "_strategy_research")
    if profile == "V13_V10_G_3":
        from ai_platform.observability.g3_shadow_gate import pending_g3_shadow
        result = pending_g3_shadow(session_date=args.session_date, command=args.command, output_root=args.output_root)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 2
    required = {"prepare": ("dataset", "model", "strategy"), "seal-decisions": ("decisions",),
                "finalize": ("outcomes",), "verify": ()}[args.command]
    missing = ["--" + name for name in required if getattr(args, name) is None]
    if missing:
        parser.error("the following arguments are required: " + ", ".join(missing))
    from ai_platform.observability.prospective_shadow import (
        finalize_shadow_session, prepare_shadow_session, seal_shadow_decisions, verify_shadow_session)
    if args.command == "prepare":
        result = prepare_shadow_session(
            session_date=args.session_date,
            dataset=args.dataset,
            model=args.model,
            strategy=args.strategy,
            output_root=args.output_root,
        ).as_dict()
    elif args.command == "seal-decisions":
        result = seal_shadow_decisions(
            session_date=args.session_date,
            decisions=args.decisions,
            output_root=args.output_root,
        ).as_dict()
    elif args.command == "finalize":
        result = finalize_shadow_session(
            session_date=args.session_date,
            outcomes=args.outcomes,
            output_root=args.output_root,
        ).as_dict()
    else:
        result = verify_shadow_session(
            session_date=args.session_date,
            output_root=args.output_root,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("state") not in {"INVALID"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
