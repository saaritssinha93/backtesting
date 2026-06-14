"""
v7_gate_promotion_report.py
===========================

Dashboard session harness for P0-17 (gate_promotion.py).

The walk-forward gate runs report-only (v7_walkforward_gate_report.py). This
harness closes the loop visibly: every session it reads the latest gate report,
computes what gate_promotion.py *would* do to the production accepted_rules.csv
(NEW / RETAINED / DEMOTED / blocked / floor-failed), and publishes a dashboard
card so the dry-run diff is reviewable before anyone flips --apply.

It reuses gate_promotion.compute_plan() — the SAME read-only decision the writer
uses — so the card can never drift from the enforcement path.

Modes
-----
  default     DRY-RUN. Computes and publishes the plan; writes NOTHING to
              accepted_rules.csv. This is the steady state (P0-17 says run dry
              for 2-3 sessions, diff against incumbent, then --apply).
  --apply     After publishing the plan, invokes gate_promotion.run(--apply) to
              actually author accepted_rules.csv (+ shadow_rules.csv + audit).
              Still subject to gate_promotion's churn guard.

Blocked setups (P1-21): T_TREND_DAY_EMA_STAIR_SHORT and C_OR_BREAKOUT are hard-
blocked by default here, merged with EQIDV2_BLOCKED_SETUPS and --blocked. A hard
block beats a gate PROMOTE.

Outputs (under EQIDV2_RUNTIME_ROOT/v7_gate_promotion):
  latest/latest_v7_gate_promotion.md     <- dashboard card body
  latest/latest_v7_gate_promotion.json   <- machine summary
  reports/v7_gate_promotion_<day>.{md,json,csv}
  runtime_status/v7_gate_promotion.{status,heartbeat}
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir
from gate_promotion import (
    ACCEPTED_RULES_SCHEMA_VERSION,
    PromotionConfig,
    PromotionPlan,
    _blocked_setups,
    compute_plan,
)
from gate_promotion import run as gate_promotion_run

SESSION_NAME = "V7 Gate Promotion"
SESSION_SLUG = "v7_gate_promotion"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
LATEST_DIR = SESSION_ROOT / "latest"
REPORTS_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

# Default source: the report-only walk-forward gate's latest decision CSV.
DEFAULT_GATE_REPORT = (
    runtime_dir("v7_walkforward_gate") / "latest" / "latest_v7_walkforward_gate.csv"
)
GATE_REPORT_JSON = (
    runtime_dir("v7_walkforward_gate") / "latest" / "latest_v7_walkforward_gate.json"
)

# Default production accepted_rules (matches v7_walkforward_gate_report.py).
DEFAULT_ACCEPTED_RULES = Path(
    os.getenv(
        "EQIDV2_V7_ACCEPTED_RULES_PATH",
        r"C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv",
    )
)

# P1-21 hard blocks (risk decisions outrank statistics). Merged with
# EQIDV2_BLOCKED_SETUPS and --blocked.
DEFAULT_BLOCKED = "T_TREND_DAY_EMA_STAIR_SHORT,C_OR_BREAKOUT"

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


def _report_age_days(path: Path) -> float:
    import time

    return (time.time() - path.stat().st_mtime) / 86400.0


def _gate_meta() -> dict[str, Any]:
    if not GATE_REPORT_JSON.exists():
        return {}
    try:
        return json.loads(GATE_REPORT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _phase_for(plan: PromotionPlan, gate_meta: dict[str, Any], applied: bool) -> tuple[str, str]:
    """(phase, headline) — phase drives the dashboard chip; headline is one line."""
    gate_status = str(gate_meta.get("gate_status", "") or "")
    if applied:
        return "APPLIED", f"accepted_rules.csv authored by gate ({len(plan.promoted)} setups)."
    if gate_status and gate_status != "STRICT_GATE_COMPLETE":
        return "WAITING_FOR_GATE", (
            f"Gate not yet conclusive ({gate_status}); accepted_rules unchanged.")
    if plan.promote_in_report == 0:
        return "WAITING_FOR_GATE", (
            "Gate PROMOTEd 0 setups on causal net-of-cost OOS data; "
            "accepted_rules unchanged.")
    if plan.churn_tripped:
        return "CHURN_GUARD", (
            "Churn guard would block the write — review before --force/--apply.")
    return "READY_DRY_RUN", (
        f"Dry-run clean: {len(plan.promoted)} setups would be accepted "
        f"({len(plan.new)} new, {len(plan.demoted)} demoted).")


def _md_table(df: pd.DataFrame, cols: list[str]) -> list[str]:
    cols = [c for c in cols if c in df.columns]
    if df.empty or not cols:
        return []
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for _, row in df.iterrows():
        vals = []
        for c in cols:
            v = row.get(c, "")
            if pd.isna(v):
                v = ""
            vals.append(str(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    return lines


def _render_md(plan: PromotionPlan, gate_meta: dict[str, Any], *,
               gate_report: Path, accepted_rules: Path, report_age: float,
               cfg: PromotionConfig, applied: bool, phase: str, headline: str,
               day: str) -> str:
    incumbent_n = len(plan.incumbent)
    lines = [
        f"# V7 Gate Promotion - {day}",
        "",
        f"**{phase}** — {headline}",
        "",
        "Closes P0-17: the gate stops being report-only. This card shows what "
        "`gate_promotion.py` would write to the production accepted_rules.csv on "
        "causal, net-of-cost, out-of-sample evidence — no setup trades without a "
        "PROMOTE row behind it.",
        "",
        f"- Generated: {_fmt_ts(_now_ist())}",
        f"- Mode: {'APPLY (wrote files)' if applied else 'DRY-RUN (wrote nothing to accepted_rules)'}",
        f"- Gate report: `{gate_report}` ({report_age:.2f} days old)",
        f"- Gate status: {gate_meta.get('gate_status', 'unknown')} "
        f"(source days: {gate_meta.get('source_days', '?')})",
        f"- accepted_rules: `{accepted_rules}` ({incumbent_n} incumbent setups)",
        f"- Schema on write: `{ACCEPTED_RULES_SCHEMA_VERSION}`",
        "",
        "## Promotion Diff",
        "",
        "| metric | value |",
        "|---|---:|",
        f"| PROMOTE in gate report | {plan.promote_in_report} |",
        f"| failed re-verify floors (n>={cfg.min_n_oos}, pf>={cfg.min_net_pf}, fdr, !overfit) | "
        f"{len(plan.floor_failed)} |",
        f"| hard-blocked overrides (P1-21) | {len(plan.blocked_overrides)} |",
        f"| would be ACCEPTED | {len(plan.promoted)} |",
        f"| NEW promotions | {len(plan.new)} |",
        f"| RETAINED | {len(plan.retained)} |",
        f"| DEMOTED -> shadow | {len(plan.demoted)} |",
        f"| churn guard | {'TRIPPED' if plan.churn_tripped else 'ok'} |",
        "",
    ]

    if plan.churn_tripped:
        lines += ["## Churn Guard", "",
                  "Refuses to author a degenerate accepted set (would empty the "
                  "book or demote too much). Resolve the cause; do not --force "
                  "blindly.", ""]
        for m in plan.guard_msgs:
            lines.append(f"- {m}")
        lines.append("")

    lines += [f"## Would Be Accepted ({len(plan.eligible)})", ""]
    if plan.eligible.empty:
        lines.append("_No setups currently clear PROMOTE + re-verification floors._")
    else:
        lines += _md_table(
            plan.eligible,
            ["setup", "n_oos", "net_pf_oos", "net_expectancy_oos",
             "fold_consistency", "p_value", "fdr_significant", "overfit_flag"],
        )
    lines.append("")

    lines += ["## Blocked / Floor-Failed / Demoted", "",
              f"- **Hard-blocked (P1-21)**: {plan.blocked or '-'}",
              f"- **Blocked overrode a PROMOTE**: {plan.blocked_overrides or '-'}",
              f"- **PROMOTE but failed floors**: {plan.floor_failed or '-'}",
              f"- **DEMOTED -> shadow_rules.csv**: {plan.demoted or '-'}",
              f"- **NEW**: {plan.new or '-'}",
              ""]

    lines += [
        "## Interpretation",
        "",
        "- DRY-RUN is the steady state. Run it for 2-3 clean sessions and confirm "
        "the diff is stable before `--apply`.",
        "- `WAITING_FOR_GATE` means the gate has not produced trustworthy PROMOTEs "
        "yet (insufficient causal history or 0 promotes) — accepted_rules is "
        "correctly left untouched.",
        "- A deliberate hand edit to accepted_rules.csv shows up here next run as "
        "an unexplained DEMOTED/NEW delta.",
        "- Demotions are tracked in shadow_rules.csv so the gate can re-admit them "
        "later on evidence.",
        "",
    ]
    return "\n".join(lines) + "\n"


def run(*, gate_report: Path, accepted_rules: Path, cfg: PromotionConfig,
        cli_blocked: str, apply: bool, force: bool, day: str) -> int:
    _write_status("RUNNING", phase="START", day=day)

    if not gate_report.exists():
        _write_status("ERROR", phase="NO_GATE_REPORT", day=day,
                      error=f"gate report not found: {gate_report}")
        _write_text_atomic(
            LATEST_DIR / "latest_v7_gate_promotion.md",
            f"# V7 Gate Promotion - {day}\n\n**NO_GATE_REPORT** — gate report not "
            f"found at `{gate_report}`. Run the V7 Walkforward Gate first.\n",
        )
        print(f"[{SESSION_SLUG}] ERROR gate report not found: {gate_report}", flush=True)
        return 1

    report = pd.read_csv(gate_report)
    existing = pd.read_csv(accepted_rules) if accepted_rules.exists() else None

    # Effective block set = EQIDV2_BLOCKED_SETUPS ∪ --blocked ∪ DEFAULT_BLOCKED.
    merged_cli = ",".join(s for s in (cli_blocked, DEFAULT_BLOCKED) if s)
    blocked = _blocked_setups(merged_cli)

    plan = compute_plan(report, existing, cfg, blocked)
    gate_meta = _gate_meta()
    report_age = _report_age_days(gate_report)

    applied = False
    apply_rc = None
    if apply:
        # Hand off to the SOLE writer. It re-loads, re-checks staleness + churn,
        # backs up, writes atomically, and emits its own audit JSON.
        shadow = accepted_rules.with_name("shadow_rules.csv")
        apply_rc = gate_promotion_run(
            gate_report, accepted_rules, shadow, cfg,
            apply=True, force=force, cli_blocked=merged_cli,
        )
        applied = apply_rc == 0

    phase, headline = _phase_for(plan, gate_meta, applied)

    md = _render_md(
        plan, gate_meta, gate_report=gate_report, accepted_rules=accepted_rules,
        report_age=report_age, cfg=cfg, applied=applied, phase=phase,
        headline=headline, day=day,
    )
    summary = {
        "session": SESSION_NAME,
        "session_slug": SESSION_SLUG,
        "day": day,
        "generated_at_ist": _fmt_ts(_now_ist()),
        "mode": "APPLY" if apply else "DRY_RUN",
        "applied": applied,
        "apply_return_code": apply_rc,
        "phase": phase,
        "headline": headline,
        "gate_report": str(gate_report),
        "gate_report_age_days": round(report_age, 3),
        "gate_status": gate_meta.get("gate_status", ""),
        "gate_source_days": gate_meta.get("source_days", None),
        "accepted_rules": str(accepted_rules),
        "incumbent_count": len(plan.incumbent),
        "promote_in_report": plan.promote_in_report,
        "would_accept_count": len(plan.promoted),
        "new": plan.new,
        "retained": plan.retained,
        "demoted": plan.demoted,
        "blocked": plan.blocked,
        "blocked_overrides": plan.blocked_overrides,
        "floor_failed": plan.floor_failed,
        "churn_tripped": plan.churn_tripped,
        "guard_msgs": plan.guard_msgs,
        "schema_version": ACCEPTED_RULES_SCHEMA_VERSION,
    }

    latest_md = LATEST_DIR / "latest_v7_gate_promotion.md"
    latest_json = LATEST_DIR / "latest_v7_gate_promotion.json"
    latest_csv = LATEST_DIR / "latest_v7_gate_promotion_eligible.csv"
    dated_md = REPORTS_DIR / f"v7_gate_promotion_{day}.md"
    dated_json = REPORTS_DIR / f"v7_gate_promotion_{day}.json"
    dated_csv = REPORTS_DIR / f"v7_gate_promotion_eligible_{day}.csv"

    _write_text_atomic(latest_md, md)
    _write_text_atomic(dated_md, md)
    _write_json_atomic(latest_json, summary)
    _write_json_atomic(dated_json, summary)
    plan.eligible.to_csv(latest_csv, index=False)
    plan.eligible.to_csv(dated_csv, index=False)

    _write_status(
        "DONE", phase=phase, day=day, mode=summary["mode"], applied=applied,
        promote_in_report=plan.promote_in_report,
        would_accept=len(plan.promoted), new=len(plan.new),
        demoted=len(plan.demoted), churn_tripped=plan.churn_tripped,
        report=str(latest_md),
    )
    print(f"[{SESSION_SLUG}] {phase} | {headline} | wrote {latest_md}", flush=True)
    return 0


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=SESSION_NAME)
    ap.add_argument("--date", default="", help="YYYY-MM-DD; defaults to today IST")
    ap.add_argument("--gate-report", type=Path, default=DEFAULT_GATE_REPORT)
    ap.add_argument("--accepted-rules", type=Path, default=DEFAULT_ACCEPTED_RULES)
    ap.add_argument("--apply", action="store_true",
                    help="hand off to gate_promotion.py --apply (actually writes)")
    ap.add_argument("--force", action="store_true",
                    help="pass through to gate_promotion --force (override churn guard)")
    ap.add_argument("--blocked", default="",
                    help="extra comma-separated hard-blocked setups "
                         "(merged with EQIDV2_BLOCKED_SETUPS and P1-21 defaults)")
    ap.add_argument("--min-net-pf", type=float, default=PromotionConfig.min_net_pf)
    ap.add_argument("--min-n-oos", type=int, default=PromotionConfig.min_n_oos)
    ap.add_argument("--min-promoted", type=int, default=PromotionConfig.min_promoted)
    ap.add_argument("--max-demotion-frac", type=float,
                    default=PromotionConfig.max_demotion_frac)
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    day = str(args.date or _now_ist().strftime("%Y-%m-%d"))
    cfg = PromotionConfig(
        min_n_oos=args.min_n_oos, min_net_pf=args.min_net_pf,
        min_promoted=args.min_promoted, max_demotion_frac=args.max_demotion_frac,
    )
    try:
        return run(
            gate_report=args.gate_report, accepted_rules=args.accepted_rules,
            cfg=cfg, cli_blocked=args.blocked, apply=args.apply, force=args.force,
            day=day,
        )
    except SystemExit:
        raise
    except Exception as exc:  # never leave the card stale on a crash
        _write_status("ERROR", phase="FAILED", day=day,
                      error=f"{type(exc).__name__}: {exc}")
        print(f"[{SESSION_SLUG}] ERROR {type(exc).__name__}: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
