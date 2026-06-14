"""
v7_qualification_report.py
==========================

Dashboard session harness for P1-22 (qualification_tracker.py).

Replaces the coin-flip "5 clean days, positive cumulative PF" live-enable check
with a card that scores the post-punchlist, mirror-config paper run against
criteria that actually discriminate edge from noise:

  Q1  n >= min_trades (default 150)
  Q2  cumulative NET PF >= 1.15 (from net_pnl_rs, never gross)
  Q3  bootstrap p(mean net > 0) < 0.10
  Q4  zero suspected daily-loss-brake trips (any trip restarts the clock)
  Q5  100% of trades from gate-PROMOTEd setups (accepted_rules.csv)
  Q6  mirror-config attestation (paper == live: positions/capital/brake)

It reuses qualification_tracker.compute_qualification() — the SAME scoring the
CLI uses — so the card can never drift from the live-enable decision.

Verdicts (drive the dashboard chip + JSON):
  QUALIFIED       all criteria pass at full n  (exit 0)
  NOT QUALIFIED   hard fail: brake trip / non-promoted setup / failed at full n
  IN PROGRESS     insufficient n or unattested; shows provisional metrics + ETA

The qualification window start ("the clock") is, in priority order:
  --start-date  >  EQIDV2_V7_QUAL_START_DATE  >  window_start.txt marker  >
  earliest available paper-trade file (with a note to pin it).

Mirror-config attestation (Q6) cannot be derived from CSVs. It is True when
--attest is passed, EQIDV2_V7_QUAL_ATTESTED is truthy, or a
mirror_config_attested.json marker exists in the session root.

Outputs (under EQIDV2_RUNTIME_ROOT/v7_qualification):
  latest/latest_v7_qualification.md / .json
  reports/v7_qualification_<day>.{md,json}
  runtime_status/v7_qualification.{status,heartbeat}
"""

from __future__ import annotations

import argparse
import glob
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir
from qualification_tracker import (
    MIRROR_CHECKLIST,
    NET_COL_CANDIDATES,
    QualConfig,
    QualResult,
    _date_from_name,
    compute_qualification,
    load_trades,
)

SESSION_NAME = "V7 Qualification"
SESSION_SLUG = "v7_qualification"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
LATEST_DIR = SESSION_ROOT / "latest"
REPORTS_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

WINDOW_START_MARKER = SESSION_ROOT / "window_start.txt"
ATTEST_MARKER = SESSION_ROOT / "mirror_config_attested.json"

DEFAULT_TRADES_GLOB = str(
    runtime_dir("live_signals") / "paper_trades_*_id_5min_v7.csv"
)
DEFAULT_ACCEPTED_RULES = Path(
    os.getenv(
        "EQIDV2_V7_ACCEPTED_RULES_PATH",
        r"C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv",
    )
)

for _path in (SESSION_ROOT, LATEST_DIR, REPORTS_DIR, HEARTBEAT_DIR, RUNTIME_STATUS_DIR):
    _path.mkdir(parents=True, exist_ok=True)


def _now_ist() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Kolkata")


def _fmt_ts(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Kolkata")
    else:
        ts = ts.tz_convert("Asia/Kolkata")
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True, default=str))


def _write_status(state: str, **extra: Any) -> None:
    payload = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "status": state,
        "pid": os.getpid(),
        "updated_at_ist": _fmt_ts(_now_ist()),
        **extra,
    }
    text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat.json", payload)


def _resolve_attested(cli_attest: bool) -> tuple[bool, str]:
    if cli_attest:
        return True, "--attest flag"
    env = str(os.getenv("EQIDV2_V7_QUAL_ATTESTED", "")).strip().lower()
    if env in {"1", "true", "yes"}:
        return True, "EQIDV2_V7_QUAL_ATTESTED env"
    if ATTEST_MARKER.exists():
        return True, f"marker {ATTEST_MARKER.name}"
    return False, "not attested"


def _first_net_date(files: list[str]):
    """Earliest paper-file date whose header carries a NET-of-cost column. This is
    the OLD->NEW config boundary: P1-22 scores net_pnl_rs only, and pre-boundary
    files are gross-only (pnl_rs) and inadmissible."""
    dated = []
    for f in files:
        d = _date_from_name(f)
        if d is None:
            continue
        try:
            cols = set(pd.read_csv(f, nrows=0).columns)
        except Exception:
            continue
        if any(c in cols for c in NET_COL_CANDIDATES):
            dated.append(d)
    return min(dated) if dated else None


def _resolve_start(cli_start: str, files: list[str]) -> tuple[date, str]:
    raw = (cli_start or os.getenv("EQIDV2_V7_QUAL_START_DATE", "")).strip()
    if raw:
        return date.fromisoformat(raw), "explicit (flag/env)"
    if WINDOW_START_MARKER.exists():
        marker = WINDOW_START_MARKER.read_text(encoding="utf-8").strip()
        if marker:
            return date.fromisoformat(marker), f"marker {WINDOW_START_MARKER.name}"
    net_boundary = _first_net_date(files)
    if net_boundary is not None:
        return net_boundary, ("net-cost boundary (first net_pnl_rs file; UNPINNED "
                              "— set EQIDV2_V7_QUAL_START_DATE to pin the clock)")
    dates = sorted(d for d in (_date_from_name(f) for f in files) if d is not None)
    if dates:
        return dates[0], "earliest paper file (no net_pnl_rs yet — gross only)"
    return date.today(), "today (no paper files yet)"


def _phase_for(verdict: str) -> str:
    return {"QUALIFIED": "QUALIFIED", "NOT QUALIFIED": "NOT_QUALIFIED",
            "IN PROGRESS": "IN_PROGRESS"}.get(verdict, "IN_PROGRESS")


def _md_daily(daily: pd.DataFrame, tail: int = 25) -> list[str]:
    if daily.empty:
        return ["_No trades in the qualification window yet._"]
    view = daily.tail(tail)
    lines = ["| date | trades | net Rs | worst Rs | brake? |",
             "|---|---:|---:|---:|:--:|"]
    for _, r in view.iterrows():
        flag = "TRIP" if r["suspected_brake_trip"] else ""
        lines.append(
            f"| {r['__date'].date()} | {int(r['trades'])} | "
            f"{r['net']:+,.0f} | {r['worst_trade']:+,.0f} | {flag} |"
        )
    return lines


def _render_md(res: QualResult, *, day: str, cfg: QualConfig,
               start: date, start_src: str, attested_src: str,
               accepted_rules: Path, promoted_n: int | None,
               trades_glob: str, skip_log: list) -> str:
    phase = _phase_for(res.verdict)
    lines = [
        f"# V7 Qualification - {day}",
        "",
        f"**{res.verdict}** — {res.reason}",
        "",
        "Closes P1-22: live enable requires this tracker's QUALIFIED verdict "
        "(plus a human go), not the old coin-flip check. Scored on net_pnl_rs "
        "from the mirror-config paper run.",
        "",
        f"- Generated: {_fmt_ts(_now_ist())}",
        f"- Window start: **{start}** ({start_src})",
        f"- Window: {res.window_start} -> {res.window_end} "
        f"({res.days} trading days, {res.n} trades, {res.rate:.1f}/day)",
        f"- Cumulative net: **Rs {res.total_net:+,.0f}** | win rate: {res.win_rate:.1%}",
        f"- accepted_rules (Q5 source): `{accepted_rules}` "
        f"({promoted_n if promoted_n is not None else 'not loaded'} promoted setups)",
        f"- Paper trades: `{trades_glob}`",
        "",
        "## Criteria",
        "",
        "| criterion | status | detail |",
        "|---|:--:|---|",
    ]
    for name, (ok, detail) in res.criteria.items():
        status = "SKIP" if ok is None else ("PASS" if ok else "FAIL")
        lines.append(f"| {name} | {status} | {detail} |")
    lines.append("")

    lines += ["## Daily Summary", ""]
    lines += _md_daily(res.daily)
    lines.append("")

    if skip_log:
        total_dropped = sum(int(s.get("dropped", 0)) for s in skip_log)
        lines += ["## Data Quality Warning", "",
                  f"{len(skip_log)} paper file(s) had unparseable rows "
                  f"(~{total_dropped} trades dropped) — trade count below is "
                  f"undercounted; fix the paper-trade writer:", ""]
        for s in skip_log:
            detail = s.get("error") or f"{s.get('dropped', 0)} rows dropped"
            lines.append(f"- `{s['file']}` — {detail}")
        lines.append("")

    if res.trips is not None and not res.trips.empty:
        lines += ["## Suspected Brake Trips (hard fail — restart the clock)", ""]
        for _, r in res.trips.iterrows():
            lines.append(
                f"- {r['__date'].date()}: net Rs {r['net']:+,.0f}, "
                f"worst trade Rs {r['worst_trade']:+,.0f}"
            )
        lines.append("")

    if res.offenders:
        lines += ["## Non-Promoted Setups Traded (Q5 leak — hard fail)", "",
                  f"{res.offenders}",
                  "",
                  "Entry config is admitting setups with no PROMOTE row. Fix the "
                  "leak (P0-17 / P1-21) before the clock can advance.",
                  ""]

    attested = res.criteria.get("Q6 mirror-config", (False, ""))[0]
    lines += ["## Mirror-Config Attestation (Q6)", "",
              f"Attestation: **{'CONFIRMED' if attested else 'NOT CONFIRMED'}** "
              f"({attested_src}).",
              "",
              "Paper must mirror live for the whole window (P0-19). Confirm each "
              "item, then attest (`--attest`, EQIDV2_V7_QUAL_ATTESTED=1, or create "
              f"`{ATTEST_MARKER.name}` in the session dir):",
              ""]
    for item in MIRROR_CHECKLIST:
        mark = "x" if attested else " "
        lines.append(f"- [{mark}] {item}")
    lines.append("")

    lines += [
        "## Interpretation",
        "",
        "- IN PROGRESS is expected for ~2-3 weeks at the new (lower) trade "
        "frequency — that is the cost of knowing, not a failure.",
        "- Any brake trip or non-promoted-setup leak is a HARD fail and restarts "
        "the clock; fix the cause, do not wait it out.",
        "- QUALIFIED is necessary, not sufficient: live enable is still a human "
        "decision and should start at minimum size.",
        "",
    ]
    return "\n".join(lines) + "\n"


def run(*, trades_glob: str, accepted_rules: Path, cfg: QualConfig,
        cli_start: str, cli_attest: bool, day: str) -> int:
    _write_status("RUNNING", phase="START", day=day)

    files = sorted(glob.glob(trades_glob))
    attested, attested_src = _resolve_attested(cli_attest)
    start, start_src = _resolve_start(cli_start, files)

    # Q5 source: gate-authored accepted_rules.
    promoted: set[str] | None = None
    promoted_n: int | None = None
    if accepted_rules.exists():
        acc = pd.read_csv(accepted_rules)
        key = "setup" if "setup" in acc.columns else acc.columns[0]
        promoted = set(acc[key].astype(str))
        promoted_n = len(promoted)

    # Are there any paper trades in the window?
    in_window = [f for f in files
                 if (_date_from_name(f) is not None and _date_from_name(f) >= start)]
    if not in_window:
        headline = (f"No paper trades on/after window start {start}. "
                    f"Clock not started.")
        md = (f"# V7 Qualification - {day}\n\n**NO_PAPER_TRADES** — {headline}\n\n"
              f"- Window start: {start} ({start_src})\n"
              f"- Paper glob: `{trades_glob}`\n")
        _write_text_atomic(LATEST_DIR / "latest_v7_qualification.md", md)
        _write_json_atomic(LATEST_DIR / "latest_v7_qualification.json", {
            "session_slug": SESSION_SLUG, "day": day, "phase": "NO_PAPER_TRADES",
            "verdict": "IN PROGRESS", "headline": headline,
            "window_start": str(start), "window_start_source": start_src,
            "trades_glob": trades_glob, "generated_at_ist": _fmt_ts(_now_ist()),
        })
        _write_status("DONE", phase="NO_PAPER_TRADES", day=day,
                      window_start=str(start), report=str(LATEST_DIR / "latest_v7_qualification.md"))
        print(f"[{SESSION_SLUG}] NO_PAPER_TRADES | {headline}", flush=True)
        return 0

    skip_log: list = []
    try:
        trades = load_trades(trades_glob, start, skip_log=skip_log)
    except SystemExit as exc:
        # load_trades exits when in-window files are all empty (no executed rows).
        headline = f"Paper files exist on/after {start} but contain no trades yet."
        md = (f"# V7 Qualification - {day}\n\n**NO_PAPER_TRADES** — {headline}\n\n"
              f"- Window start: {start} ({start_src})\n"
              f"- Paper glob: `{trades_glob}`\n- loader: {exc}\n")
        _write_text_atomic(LATEST_DIR / "latest_v7_qualification.md", md)
        _write_status("DONE", phase="NO_PAPER_TRADES", day=day,
                      window_start=str(start),
                      report=str(LATEST_DIR / "latest_v7_qualification.md"))
        print(f"[{SESSION_SLUG}] NO_PAPER_TRADES | {headline}", flush=True)
        return 0
    res = compute_qualification(trades, cfg, promoted, attested)
    phase = _phase_for(res.verdict)

    # net-coverage: rows with no net-of-cost value are gross-only (pre-NEW-config)
    # and would silently zero PF — surface them.
    net_col = next((c for c in NET_COL_CANDIDATES if c in trades.columns), None)
    net_missing = int(trades[net_col].isna().sum()) if net_col else int(len(trades))
    if net_missing:
        skip_log.append({"file": f"({net_missing} rows)",
                         "error": f"no {net_col or 'net'} value — gross-only; "
                                  f"excluded from PF, inflate trade count"})

    md = _render_md(res, day=day, cfg=cfg, start=start, start_src=start_src,
                    attested_src=attested_src, accepted_rules=accepted_rules,
                    promoted_n=promoted_n, trades_glob=trades_glob, skip_log=skip_log)
    summary = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "day": day,
        "generated_at_ist": _fmt_ts(_now_ist()),
        "phase": phase,
        "verdict": res.verdict,
        "reason": res.reason,
        "exit_code": res.exit_code,
        "window_start": str(start),
        "window_start_source": start_src,
        "window_first_trade": str(res.window_start),
        "window_last_trade": str(res.window_end),
        "trading_days": res.days,
        "n_trades": res.n,
        "trades_per_day": round(res.rate, 3),
        "cumulative_net_rs": round(res.total_net, 2),
        "win_rate": round(res.win_rate, 4),
        "net_pf": (round(res.pf, 4) if res.pf != float("inf") else "inf"),
        "bootstrap_p": round(res.pval, 4),
        "min_trades": cfg.min_trades,
        "min_net_pf": cfg.min_net_pf,
        "max_p_value": cfg.max_p_value,
        "suspected_brake_trips": int(len(res.trips)),
        "non_promoted_offenders": res.offenders,
        "attested": attested,
        "attested_source": attested_src,
        "promoted_setup_count": promoted_n,
        "criteria": {k: {"pass": v[0], "detail": v[1]} for k, v in res.criteria.items()},
        "trades_glob": trades_glob,
        "accepted_rules": str(accepted_rules),
        "net_col": net_col,
        "net_missing_rows": net_missing,
        "data_quality_skips": skip_log,
    }

    latest_md = LATEST_DIR / "latest_v7_qualification.md"
    latest_json = LATEST_DIR / "latest_v7_qualification.json"
    dated_md = REPORTS_DIR / f"v7_qualification_{day}.md"
    dated_json = REPORTS_DIR / f"v7_qualification_{day}.json"
    _write_text_atomic(latest_md, md)
    _write_text_atomic(dated_md, md)
    _write_json_atomic(latest_json, summary)
    _write_json_atomic(dated_json, summary)

    _write_status(
        "DONE", phase=phase, day=day, verdict=res.verdict, n=res.n,
        trading_days=res.days, net_pf=summary["net_pf"],
        cumulative_net_rs=summary["cumulative_net_rs"],
        brake_trips=int(len(res.trips)), attested=attested,
        report=str(latest_md),
    )
    print(f"[{SESSION_SLUG}] {res.verdict} | n={res.n} pf={summary['net_pf']} "
          f"| {res.reason} | wrote {latest_md}", flush=True)
    return 0


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=SESSION_NAME)
    ap.add_argument("--date", default="", help="report day YYYY-MM-DD; defaults to today IST")
    ap.add_argument("--trades-glob", default=DEFAULT_TRADES_GLOB)
    ap.add_argument("--accepted-rules", type=Path, default=DEFAULT_ACCEPTED_RULES)
    ap.add_argument("--start-date", default="",
                    help="qualification window start YYYY-MM-DD "
                         "(else EQIDV2_V7_QUAL_START_DATE / marker / earliest file)")
    ap.add_argument("--attest", action="store_true",
                    help="record that the mirror-config checklist was human-verified")
    ap.add_argument("--min-trades", type=int, default=QualConfig.min_trades)
    ap.add_argument("--min-net-pf", type=float, default=QualConfig.min_net_pf)
    ap.add_argument("--max-p", type=float, default=QualConfig.max_p_value)
    ap.add_argument("--daily-loss-limit", type=float, default=QualConfig.daily_loss_limit_rs)
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    day = str(args.date or _now_ist().strftime("%Y-%m-%d"))
    cfg = QualConfig(min_trades=args.min_trades, min_net_pf=args.min_net_pf,
                     max_p_value=args.max_p, daily_loss_limit_rs=args.daily_loss_limit)
    try:
        return run(
            trades_glob=args.trades_glob, accepted_rules=args.accepted_rules,
            cfg=cfg, cli_start=args.start_date, cli_attest=args.attest, day=day,
        )
    except SystemExit:
        raise
    except Exception as exc:
        _write_status("ERROR", phase="FAILED", day=day,
                      error=f"{type(exc).__name__}: {exc}")
        print(f"[{SESSION_SLUG}] ERROR {type(exc).__name__}: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
