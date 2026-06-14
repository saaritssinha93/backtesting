"""
gate_promotion.py
=================

P0-17: the ONLY writer of `accepted_rules.csv`.

Converts the walk-forward gate report (output of `walkforward_gate.run_gate`,
as produced daily by `v7_walkforward_gate_report.py`) into the production
accepted-rules contract. Closes the "report-only" gap: after this is wired,
no setup trades unless the gate PROMOTEd it on causal-VWAP, net-of-cost,
out-of-sample data.

Design rules
------------
1. DRY-RUN BY DEFAULT. Nothing is written without --apply. Run it dry for a
   few sessions and compare the would-be accepted set against the current one
   before flipping.
2. NEVER destroys state. Timestamped backup of the previous accepted_rules,
   atomic replace (temp file + os.replace), and a full JSON audit of every
   decision (promoted / retained / demoted / blocked) per run.
3. CHURN GUARD. Refuses (without --force) to author a file that would demote
   more than `max_demotion_frac` of the incumbent set, or that contains fewer
   than `min_promoted` setups, or from a stale gate report. One bad gate run
   (data glitch, short fold history) must not silently empty the book.
4. HARD BLOCKS BEAT THE GATE. Setups listed in EQIDV2_BLOCKED_SETUPS (or
   --blocked) never enter accepted_rules even if the gate PROMOTEs them.
   Human risk decisions (e.g. T_TREND_DAY_EMA_STAIR_SHORT) outrank statistics.
5. DEMOTIONS GO TO SHADOW. Setups removed from accepted_rules are written to
   `shadow_rules.csv` so the research layer / entry engine can keep tracking
   them in shadow mode and the gate can re-promote them later on evidence.

Integration point
-----------------
`map_to_accepted_contract()` is the ONE function to edit so the output matches
your production accepted_rules column contract. Default behaviour: carry
forward every column of a retained setup's existing row, fill gate-provenance
columns, and warn on brand-new setups whose extra columns will be NaN.

Usage
-----
  # dry run (prints diff, writes nothing)
  python gate_promotion.py --gate-report path\\to\\gate_report.csv ^
      --accepted-rules C:\\TradingData\\eqidv2\\...\\accepted_rules.csv

  # enforce
  python gate_promotion.py --gate-report ... --accepted-rules ... --apply

  # override churn guard after human review
  python gate_promotion.py ... --apply --force
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

ACCEPTED_RULES_SCHEMA_VERSION = "v7_accepted_gate_2026_06_11"

REQUIRED_REPORT_COLS = {
    "setup", "n_oos", "net_pf_oos", "fold_consistency",
    "p_value", "fdr_significant", "overfit_flag", "decision",
}


@dataclass(frozen=True)
class PromotionConfig:
    # Re-verification floors (defense in depth; gate already enforces its own)
    min_n_oos: int = 40
    min_net_pf: float = 1.20
    require_fdr: bool = True
    reject_overfit_flag: bool = True
    # Churn guard
    min_promoted: int = 3
    max_demotion_frac: float = 0.50
    max_report_age_days: float = 3.0
    # Column holding the setup key in the existing accepted_rules file
    key_col: str = "setup"


@dataclass(frozen=True)
class PromotionPlan:
    """Read-only promotion decision. Produced by compute_plan(); consumed by the
    CLI writer (run) AND the dashboard report harness so neither can drift."""
    eligible: pd.DataFrame
    existing: "pd.DataFrame | None"
    incumbent: list
    blocked: list
    blocked_overrides: list
    floor_failed: list
    promote_in_report: int
    new: list
    retained: list
    demoted: list
    guard_msgs: list

    @property
    def promoted(self) -> list:
        return sorted(set(self.eligible["setup"])) if len(self.eligible) else []

    @property
    def churn_tripped(self) -> bool:
        return bool(self.guard_msgs)


# ---------------------------------------------------------------------------
# >>> EDIT HERE: map gate output to YOUR production accepted_rules contract <<<
# ---------------------------------------------------------------------------
def map_to_accepted_contract(eligible: pd.DataFrame,
                             existing: pd.DataFrame | None,
                             cfg: PromotionConfig) -> pd.DataFrame:
    """
    Build the new accepted_rules DataFrame.

    Default contract: one row per promoted setup with
      setup | <carried-forward production columns> | gate provenance columns

    Carried forward: if the setup already exists in the current accepted_rules,
    every column of its existing row is preserved (so V8 exit params, side
    flags, etc. survive promotion refreshes). New setups get NaN in those
    columns -- the run will WARN so you can fill them deliberately.
    """
    prov_cols = ["n_oos", "net_pf_oos", "net_expectancy_oos", "fold_consistency",
                 "p_value", "fdr_significant", "oos_is_ratio"]
    prov = eligible[["setup"] + [c for c in prov_cols if c in eligible.columns]].copy()
    prov = prov.rename(columns={c: f"gate_{c}" for c in prov_cols})

    if existing is not None and len(existing):
        base = existing[existing[cfg.key_col].isin(prov["setup"])].copy()
        base = base.rename(columns={cfg.key_col: "setup"})
        out = prov.merge(base, on="setup", how="left", suffixes=("", "_old"))
        # drop stale provenance columns from previous gate runs
        out = out[[c for c in out.columns if not c.endswith("_old")]]
    else:
        out = prov

    out["schema_version"] = ACCEPTED_RULES_SCHEMA_VERSION
    out["promoted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["promoted_by"] = "gate_promotion.py"
    return out.sort_values("setup").reset_index(drop=True)
# ---------------------------------------------------------------------------


def _load_report(path: Path, cfg: PromotionConfig) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"ERROR: gate report not found: {path}")
    age_days = (time.time() - path.stat().st_mtime) / 86400.0
    if age_days > cfg.max_report_age_days:
        sys.exit(f"ERROR: gate report is {age_days:.1f} days old "
                 f"(max {cfg.max_report_age_days}). Re-run the gate first, or "
                 f"touch the file if this is intentional.")
    df = pd.read_csv(path)
    missing = REQUIRED_REPORT_COLS - set(df.columns)
    if missing:
        sys.exit(f"ERROR: gate report missing columns: {sorted(missing)}")
    return df


def _blocked_setups(cli_blocked: str | None) -> set[str]:
    blocked: set[str] = set()
    env = os.environ.get("EQIDV2_BLOCKED_SETUPS", "")
    for src in (env, cli_blocked or ""):
        blocked |= {s.strip() for s in src.split(",") if s.strip()}
    return blocked


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)  # atomic on Windows and POSIX


def compute_plan(report: pd.DataFrame, existing: "pd.DataFrame | None",
                 cfg: PromotionConfig, blocked: set[str]) -> PromotionPlan:
    """Pure, read-only promotion decision. Writes nothing. This is the single
    source of truth for eligibility + diff + churn guard, shared by the CLI
    writer and the dashboard report harness."""
    # ---- eligibility: PROMOTE + re-verified floors -------------------------
    is_promote = report["decision"].astype(str).str.upper() == "PROMOTE"
    floors = (
        (report["n_oos"] >= cfg.min_n_oos)
        & (report["net_pf_oos"] >= cfg.min_net_pf)
        & (~report["overfit_flag"].astype(bool) if cfg.reject_overfit_flag else True)
        & (report["fdr_significant"].astype(bool) if cfg.require_fdr else True)
    )
    eligible = report[is_promote & floors].copy()
    floor_failed = report[is_promote & ~floors]["setup"].tolist()

    blocked_overrides = sorted(set(eligible["setup"]) & blocked)
    eligible = eligible[~eligible["setup"].isin(blocked)]

    # ---- diff vs incumbent set --------------------------------------------
    incumbent: set[str] = set()
    if existing is not None and len(existing):
        if cfg.key_col not in existing.columns:
            raise KeyError(f"existing accepted_rules has no '{cfg.key_col}' column")
        incumbent = set(existing[cfg.key_col].astype(str))

    promoted = set(eligible["setup"])
    new = sorted(promoted - incumbent)
    retained = sorted(promoted & incumbent)
    demoted = sorted(incumbent - promoted)

    # ---- churn guard --------------------------------------------------------
    guard_msgs: list[str] = []
    if len(promoted) < cfg.min_promoted:
        guard_msgs.append(f"only {len(promoted)} setups would be accepted "
                          f"(min_promoted={cfg.min_promoted})")
    if incumbent and (len(demoted) / len(incumbent)) > cfg.max_demotion_frac:
        guard_msgs.append(f"{len(demoted)}/{len(incumbent)} incumbents would be "
                          f"demoted (max_demotion_frac={cfg.max_demotion_frac})")

    return PromotionPlan(
        eligible=eligible, existing=existing, incumbent=sorted(incumbent),
        blocked=sorted(blocked), blocked_overrides=blocked_overrides,
        floor_failed=floor_failed, promote_in_report=int(is_promote.sum()),
        new=new, retained=retained, demoted=demoted, guard_msgs=guard_msgs,
    )


def run(gate_report: Path, accepted_rules: Path, shadow_rules: Path | None,
        cfg: PromotionConfig, apply: bool, force: bool,
        cli_blocked: str | None) -> int:
    report = _load_report(gate_report, cfg)
    blocked = _blocked_setups(cli_blocked)

    existing: pd.DataFrame | None = None
    if accepted_rules.exists():
        existing = pd.read_csv(accepted_rules)
        if cfg.key_col not in existing.columns:
            sys.exit(f"ERROR: existing accepted_rules has no '{cfg.key_col}' "
                     f"column; pass --key-col to match your contract.")

    plan = compute_plan(report, existing, cfg, blocked)
    eligible = plan.eligible
    incumbent = set(plan.incumbent)
    blocked_overrides = plan.blocked_overrides
    floor_failed = plan.floor_failed
    is_promote_count = plan.promote_in_report
    new, retained, demoted = plan.new, plan.retained, plan.demoted
    guard_msgs = plan.guard_msgs

    # ---- report -------------------------------------------------------------
    print(f"\n=== gate_promotion {'APPLY' if apply else 'DRY-RUN'} "
          f"@ {datetime.now():%Y-%m-%d %H:%M:%S} ===")
    print(f"gate report     : {gate_report}")
    print(f"accepted_rules  : {accepted_rules} "
          f"({'exists, ' + str(len(incumbent)) + ' setups' if incumbent else 'none yet'})")
    print(f"PROMOTE in report          : {is_promote_count}")
    print(f"  failed re-verify floors  : {floor_failed or '-'}")
    print(f"  blocked (env/CLI)        : {blocked_overrides or '-'}")
    print(f"NEW promotions             : {new or '-'}")
    print(f"RETAINED                   : {retained or '-'}")
    print(f"DEMOTED -> shadow          : {demoted or '-'}")

    if guard_msgs and not force:
        print("\nCHURN GUARD TRIPPED -- refusing to write without --force:")
        for m in guard_msgs:
            print(f"  - {m}")
        return 2

    if not apply:
        print("\nDry-run only. Re-run with --apply to write.")
        return 0

    # ---- write --------------------------------------------------------------
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if existing is not None:
        backup = accepted_rules.with_name(f"{accepted_rules.stem}_backup_{ts}.csv")
        existing.to_csv(backup, index=False)
        print(f"\nbacked up previous accepted_rules -> {backup.name}")

    out = map_to_accepted_contract(eligible, existing, cfg)
    nan_new = [s for s in new if existing is not None and
               out.loc[out['setup'] == s].isna().any(axis=1).any()]
    if nan_new:
        print(f"WARN: new setups with unfilled production columns: {nan_new} "
              f"-- fill exit params before next session.")
    accepted_rules.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_csv(out, accepted_rules)
    print(f"wrote {len(out)} setups -> {accepted_rules}")

    if shadow_rules and demoted:
        shadow_df = (existing[existing[cfg.key_col].isin(demoted)].copy()
                     if existing is not None else pd.DataFrame({cfg.key_col: demoted}))
        shadow_df["demoted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _atomic_write_csv(shadow_df, shadow_rules)
        print(f"wrote {len(shadow_df)} demoted setups -> {shadow_rules}")

    audit = {
        "ts": ts, "schema_version": ACCEPTED_RULES_SCHEMA_VERSION,
        "gate_report": str(gate_report), "config": asdict(cfg),
        "blocked": sorted(blocked), "blocked_overrides": blocked_overrides,
        "floor_failed": floor_failed, "new": new, "retained": retained,
        "demoted": demoted, "forced": force,
        "report_rows": report.to_dict(orient="records"),
    }
    audit_path = accepted_rules.with_name(f"promotion_audit_{ts}.json")
    audit_path.write_text(json.dumps(audit, indent=2, default=str))
    print(f"audit -> {audit_path.name}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gate-report", required=True, type=Path)
    ap.add_argument("--accepted-rules", required=True, type=Path)
    ap.add_argument("--shadow-rules", type=Path, default=None,
                    help="where demoted setups are written (default: "
                         "shadow_rules.csv next to accepted_rules)")
    ap.add_argument("--apply", action="store_true", help="actually write files")
    ap.add_argument("--force", action="store_true", help="override churn guard")
    ap.add_argument("--blocked", default=None,
                    help="comma-separated hard-blocked setups "
                         "(merged with EQIDV2_BLOCKED_SETUPS)")
    ap.add_argument("--min-net-pf", type=float, default=PromotionConfig.min_net_pf)
    ap.add_argument("--min-n-oos", type=int, default=PromotionConfig.min_n_oos)
    ap.add_argument("--min-promoted", type=int, default=PromotionConfig.min_promoted)
    ap.add_argument("--max-demotion-frac", type=float,
                    default=PromotionConfig.max_demotion_frac)
    ap.add_argument("--key-col", default=PromotionConfig.key_col)
    a = ap.parse_args()

    cfg = PromotionConfig(min_n_oos=a.min_n_oos, min_net_pf=a.min_net_pf,
                          min_promoted=a.min_promoted,
                          max_demotion_frac=a.max_demotion_frac,
                          key_col=a.key_col)
    shadow = a.shadow_rules or a.accepted_rules.with_name("shadow_rules.csv")
    return run(a.gate_report, a.accepted_rules, shadow, cfg,
               apply=a.apply, force=a.force, cli_blocked=a.blocked)


if __name__ == "__main__":
    raise SystemExit(main())
