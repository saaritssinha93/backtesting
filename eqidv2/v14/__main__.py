"""Isolated V14 data preparation, replacement study and frozen-profile replay."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

from . import data, research

DEFAULT_START = "2026-07-29"
DEFAULT_END = "2026-10-09"


def _date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Date must use YYYY-MM-DD") from exc


def _dates(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument("--start", type=_date, default=None if required else DEFAULT_START, required=required)
    parser.add_argument("--end", type=_date, default=None if required else DEFAULT_END, required=required)
    parser.add_argument("--warmup-sessions", type=int, default=30,
                        help="Prior trading sessions staged for feature warmup (default: 30)")
    parser.add_argument("--fetch-missing", action="store_true",
                        help="Read missing cash-minute history through existing local broker credentials")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("prepare", "Stage the MIS minus F&O universe and cash-minute data"),
        ("study", "Compare the preregistered alternatives on already staged data"),
        ("run-all", "Record the protocol, prepare data, then run the full comparison"),
        ("latest", "Show the latest completed V14 report and frozen profile"),
        ("replay-selected", "Replay the frozen chosen profile without comparing or retuning alternatives"),
        ("coverage-study", "Recalculate fixed archived uncapped selections with verified gap evidence"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--root", type=Path, default=data.DEFAULT_ROOT,
                             help="Isolated V14 runtime root (or V14_RUNTIME_ROOT)")
        if name in ("prepare", "run-all"):
            _dates(command)
            command.add_argument("--symbols", help="Comma-separated smoke-test subset; use a separate --root")
            command.add_argument("--probe-mis", action="store_true", help="Also record current broker MIS margin eligibility")
            command.add_argument("--retry-partial", action="store_true", help="Retry already attempted incomplete broker sessions")
        if name in ("study", "run-all", "replay-selected", "coverage-study"):
            command.add_argument("--output", type=Path, help="New output directory; defaults to a unique timestamped run")
        if name == "study":
            command.add_argument("--preregister-only", action="store_true", help="Write the fixed protocol without reading results")
        if name == "latest":
            command.add_argument("--json", action="store_true", help="Print the complete latest-run pointer")
        if name == "replay-selected":
            _dates(command, required=True)
            command.add_argument("--profile", type=Path, help="Frozen selected_profile.json; default is the root's selected profile")
        if name == "coverage-study":
            command.add_argument("--source", type=Path, required=True, help="Completed uncapped source study")
            command.add_argument("--repair-dir", type=Path, required=True, help="Verified minute and independent five-minute repair snapshots")
            command.add_argument("--workers", type=int, default=3)
    return parser


def _output(root: Path, requested: Path | None, prefix: str) -> Path:
    output = requested or root / "runs" / (
        prefix + "_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    if (output / "summary.json").exists():
        raise FileExistsError(f"Completed V14 output is immutable; choose a new directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    return output


def _prepare(args) -> Path:
    symbols = [part.strip().upper() for part in args.symbols.split(",") if part.strip()] if args.symbols else None
    if args.symbols and not symbols:
        raise ValueError("--symbols contains no symbols")
    manifest = data.prepare_data(args.start, args.end, warmup_sessions=args.warmup_sessions,
        fetch_missing=args.fetch_missing, symbols=symbols, root=args.root,
        probe_current_mis=args.probe_mis, retry_partial=args.retry_partial)
    if args.fetch_missing:
        from .index_fetch import refresh_nifty_context
        sessions = json.loads(manifest.read_text(encoding="utf-8"))["study_sessions"]
        refresh_nifty_context(sessions, args.root)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if hasattr(args, "start") and args.start > args.end:
        parser.error("--start must not be after --end")
    if hasattr(args, "warmup_sessions") and args.warmup_sessions < 0:
        parser.error("--warmup-sessions cannot be negative")
    if hasattr(args, "workers") and args.workers < 1:
        parser.error("--workers must be positive")
    try:
        if args.command == "latest":
            path = args.root / "latest.json"
            if not path.is_file():
                raise FileNotFoundError(f"No completed V14 run pointer: {path}")
            latest = json.loads(path.read_text(encoding="utf-8"))
            if args.json:
                print(json.dumps(latest, indent=2))
            else:
                print(f"STATUS {latest.get('status', 'UNKNOWN')}")
                print(f"SELECTED {latest.get('selected_variant', 'See selected_profile.json')}")
                print(f"REPORT {latest.get('report', '')}")
                print(f"PROFILE {args.root / 'selected_profile.json'}")
            return 0
        if args.command == "prepare":
            print(f"MANIFEST {_prepare(args)}", flush=True)
            return 0
        if args.command == "coverage-study":
            from .coverage_study import run
            output = _output(args.root, args.output, "coverage")
            print(f"OUTPUT {output.resolve()}", flush=True)
            run(args.root, args.source, args.repair_dir, output, workers=args.workers)
            print(f"REPORT {output / 'report.html'}", flush=True)
            return 0
        prefix = "replay" if args.command == "replay-selected" else "study"
        output = _output(args.root, args.output, prefix)
        print(f"OUTPUT {output.resolve()}", flush=True)
        if args.command == "replay-selected":
            runner = getattr(research, "replay_selected", None)
            if runner is None:
                raise RuntimeError("Frozen-profile replay is unavailable in this checkout; the comparison study was not rerun")
            runner(args.root, output, start_date=args.start, end_date=args.end,
                   profile_path=args.profile, fetch_missing=args.fetch_missing,
                   warmup_sessions=args.warmup_sessions)
        else:
            research.preregister(output)
            if args.command == "run-all":
                print(f"MANIFEST {_prepare(args)}", flush=True)
            if args.command == "study" and args.preregister_only:
                print(f"PROTOCOL {output / 'protocol.json'}", flush=True)
                return 0
            research.run_study(args.root, output)
        print(f"REPORT {output / 'report.html'}", flush=True)
        return 0
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"V14 ERROR {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
