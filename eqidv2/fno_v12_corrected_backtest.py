"""V12 CORRECTED - locked V12 strategy on point-in-time near-month OI.

The promoted V12 backtest (``fno_v12_backtest.py``) reads its candidates from
``fno_v10_backtest._load_all_usable_max050_gap2_history()``, whose segments are
built against ONE static universe snapshot pinned to 2026-08-11.  Every
contract in that snapshot is 26AUG, so sessions from 2026-05-27 onward were
scored against 26AUG open interest even on dates when 26AUG was a back-month
that barely traded:

    month     median OI    median 1m volume    26AUG's real role
    2026-05       8,475                   0    3rd month out, untraded
    2026-06      60,088                   0    2nd month out, untraded
    2026-07     867,600                 112    next month, thin
    2026-08  13,512,662               1,850    front month, real

``oi_change_pct`` across that stretch measures a contract ageing into
front-month, not order flow.  V12's late-SHORT volume rule sits downstream of
that same OI gate, so its selection inherits the contamination.

WHAT THIS MODULE CHANGES: the data, and only the data.

  * ROLL POLICY - the near month for session ``d`` is the stored contract with
    the smallest expiry ``>= d``, exactly the policy used by
    ``fno_v6_corrected_backtest`` and inherited by V6-v2 and V13-v5.
  * ELIGIBILITY - a session is replayed only when that contract's bars are
    actually stored for it.  Sessions whose true near month was never captured
    are dropped, never substituted.

WHAT THIS MODULE DOES NOT CHANGE: the strategy.  The setup book, the V12
late-SHORT volume rule, the selection overlay, the V11 and V12 runtime hooks,
the MAX_2_BPS gap guard, the entry policy, exits, square-off, EOD policy,
target exposure and cost scenarios are all imported from the locked V12 module
and executed through its own nested hook stack.  ``fno_v12_backtest.py`` and
``fno_v6_corrected_backtest.py`` are both hash-pinned here and are never
edited; this module refuses to run if either drifts.

RESEARCH ONLY.  The corrected sample is short and shares its window with the
other corrected variants, so nothing here is promotion-grade evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as engine
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_gap_guard_research as gaps
import fno_v11_backtest as v11_backtest
import fno_v11_execution_runtime as v11_execution
import fno_v11_gap_runtime as v11_gap
import fno_v12_backtest as v12
import fno_v12_execution_runtime as v12_execution
import fno_v12_selection_runtime as v12_selection
import fno_v6_corrected_backtest as v6

STRATEGY_VERSION = "FNO_V12_CORRECTED_ROLLING_NEAR_MONTH"
EVIDENCE_STATUS = "RESEARCH_ONLY_NOT_PROMOTED"
CONFIG_SOURCE = "LOCKED_V12_STRATEGY_WITH_POINT_IN_TIME_NEAR_MONTH_OI"
ROLL_POLICY = v6.ROLL_POLICY

# Both upstream modules are inputs, never outputs. If either changes, the
# comparison this module reports would no longer mean what it says.
EXPECTED_V6_SHA256 = (
    "06baf32c33156f21bce1dc786e5687a250b9711a1bca3a186283c824edfcf62d"
)
EXPECTED_V12_SHA256 = (
    "4332cf86a65d8ae9f897600011b205952ecb545a09302c6e7da39d26c43eb971"
)

DEFAULT_THROUGH_DAY = "2026-09-03"
DEFAULT_SPLIT_DAY = "2026-08-14"

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v12_corrected"
SNAPSHOT_DIR = RESULT_DIR / "_snapshots"
TRADES_PATH = RESULT_DIR / "fno_v12_corrected_trades.csv"
DAILY_PATH = RESULT_DIR / "fno_v12_corrected_daily.csv"
SETUPS_PATH = RESULT_DIR / "fno_v12_corrected_setups.csv"
PERIODS_PATH = RESULT_DIR / "fno_v12_corrected_period_metrics.csv"
SCENARIOS_PATH = RESULT_DIR / "fno_v12_corrected_cost_scenarios.csv"
ELIGIBILITY_PATH = RESULT_DIR / "fno_v12_corrected_session_eligibility.csv"
PROVENANCE_PATH = RESULT_DIR / "fno_v12_corrected_provenance.json"
REPORT_PATH = RESULT_DIR / "FNO_V12_CORRECTED_RESULTS.md"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_sources_frozen(pin_v12: bool) -> dict[str, str]:
    v6_path = Path(v6.__file__).resolve()
    v12_path = Path(v12.__file__).resolve()
    v6_hash = _sha256(v6_path)
    v12_hash = _sha256(v12_path)
    if v6_hash != EXPECTED_V6_SHA256:
        raise RuntimeError(
            "Corrected V6 source drifted; V12-corrected refuses to inherit an "
            f"unpinned roll policy. Expected {EXPECTED_V6_SHA256}, got {v6_hash}."
        )
    if EXPECTED_V12_SHA256 and v12_hash != EXPECTED_V12_SHA256 and not pin_v12:
        raise RuntimeError(
            "Locked V12 source drifted; the strategy this module claims to "
            f"replay has changed. Expected {EXPECTED_V12_SHA256}, got {v12_hash}."
        )
    return {"v6_corrected_sha256": v6_hash, "v12_locked_sha256": v12_hash}


# --------------------------------------------------------------------------
# corrected data build
# --------------------------------------------------------------------------
def _rebind_engine_universe(universe_date: date, universe_path: Path, month: str) -> None:
    """Point the V8 engine at a point-in-time near-month universe.

    The engine ships pinned to the 2026-08-11 26AUG snapshot with expected
    file hashes. For a September session that contract has expired and has no
    stored bars, so every symbol-session returns source-incomplete. The pinned
    hashes describe the AUG file and are cleared here; empty values skip the
    comparison (see provenance.load_backtest_universe) and the run records the
    hashes it actually observed in its own provenance.
    """
    engine.BACKTEST_UNIVERSE_DATE = universe_date
    engine.BACKTEST_UNIVERSE_PATH = universe_path
    engine.BACKTEST_CONTRACT_MONTH_FILTER = month.upper()
    engine.OI_INSTRUMENT = f"POINT_IN_TIME_{month.upper()}_NFO_FUTURE_RESEARCH_ONLY"
    engine.BACKTEST_UNIVERSE_HASHES = {
        "file_sha256": "",
        "universe_sha256": "",
        "mapped_universe_sha256": "",
        "mapped_symbol_set_sha256": "",
    }


def _regime_snapshot(month: str, universe_path: Path) -> Path:
    """Freeze this regime's mapped sources once and reuse the manifest."""
    root = SNAPSHOT_DIR / month
    root.mkdir(parents=True, exist_ok=True)
    existing = sorted(root.glob("snapshot_*/manifest.json"))
    if existing:
        return existing[-1]
    mapped, record = provenance.load_backtest_universe(
        universe_path=universe_path,
        contract_month_contains=month,
        require_persisted_mapping=True,
    )
    result = provenance.create_source_snapshot(
        mapped,
        record,
        universe_path=universe_path,
        snapshot_root=root,
        require_complete_sources=True,
    )
    return Path(result["manifest_path"])


def build_corrected_history(
    through_day: date,
    *,
    min_coverage: float,
    rebuild: bool,
):
    """Candidates and one-minute paths on the true near month for each session."""
    regimes = v6.regime_universe_paths()
    eligibility, calendar, _origin = v6.build_eligibility(
        regimes, min_coverage=min_coverage
    )
    eligibility = eligibility.loc[eligibility["day"].le(through_day)].copy()
    ok = eligibility.loc[eligibility["eligible"]]
    if ok.empty:
        raise RuntimeError("V12 corrected has no eligible sessions.")

    days_by_month: dict[str, list[date]] = {}
    for row in ok.to_dict("records"):
        month = str(row["required_contract"])
        if month in regimes:
            days_by_month.setdefault(month, []).append(row["day"])

    candidate_parts: list[pd.DataFrame] = []
    path_parts: list[pd.DataFrame] = []
    regime_records: list[dict[str, Any]] = []

    for month in sorted(days_by_month, key=lambda code: calendar[code]):
        days = sorted(days_by_month[month])
        universe_path = Path(regimes[month]).resolve()
        universe_date = date.fromisoformat(universe_path.stem.replace("near_month_", ""))
        _rebind_engine_universe(universe_date, universe_path, month)
        snapshot = _regime_snapshot(month, universe_path)
        print(
            f"[V12-CORRECTED] regime {month}: {len(days)} sessions "
            f"{days[0]} -> {days[-1]} | universe {universe_path.name}",
            flush=True,
        )
        candidates, minute_paths, coverage, manifest, manifest_path = (
            engine.load_or_build_v8_cache(
                source_snapshot_path=snapshot,
                from_day=days[0],
                through_day=days[-1],
                rebuild=rebuild,
            )
        )
        # build_v8_candidate_tables records the session date as "session_date"
        # (verified against fno_v8_windowed_1m_entry_backtest.py:2457) - the
        # audit frame produced later by the replay engine uses "day" instead,
        # which is why _day_series() below checks both names.
        day_series = pd.to_datetime(candidates["session_date"]).dt.date
        keep = day_series.isin(set(days))
        candidates = candidates.loc[keep].copy()
        ids = set(candidates["candidate_id"].astype(str))
        minute_paths = minute_paths.loc[
            minute_paths["candidate_id"].astype(str).isin(ids)
        ].copy()
        candidates["contract_month"] = month
        candidate_parts.append(candidates)
        path_parts.append(minute_paths)
        regime_records.append(
            {
                "contract_month": month,
                "expiry": str(calendar[month]),
                "universe_path": str(universe_path),
                "universe_sha256": provenance.sha256_file(universe_path),
                "snapshot_manifest": str(snapshot),
                "cache_manifest": str(manifest_path),
                "cache_manifest_sha256": provenance.sha256_file(manifest_path),
                "sessions": len(days),
                "first_day": str(days[0]),
                "last_day": str(days[-1]),
                "candidates": int(len(candidates)),
            }
        )

    candidates = pd.concat(candidate_parts, ignore_index=True)
    minute_paths = pd.concat(path_parts, ignore_index=True)
    if candidates["candidate_id"].astype(str).duplicated().any():
        raise AssertionError(
            "Corrected history contains duplicate candidate_id across regimes."
        )
    if minute_paths.duplicated(["candidate_id", "bar_ts"]).any():
        raise AssertionError("Corrected history contains duplicate minute bars.")
    return candidates, minute_paths, eligibility, regime_records, calendar


# --------------------------------------------------------------------------
# locked V12 strategy, executed unchanged
# --------------------------------------------------------------------------
def run_locked_v12(candidates: pd.DataFrame, minute_paths: pd.DataFrame,
                   *, cost_bps: float, slippage_bps: float):
    """Replay the locked V12 book through its own nested hook stack."""
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    base_setups = tuple(engine.ACTIVE_SETUPS)
    prepared = v12_selection.prepare_variant_selection(
        candidates, base_setups, v12.FIXED_CONFIG
    )
    runtime_spec = v12._runtime_spec(prepared)
    gap = v12._gap_spec()
    policy = experiment._entry_policy_for_variant(
        locked_config.ACTIVE_VARIANT,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        square_off=v12.SQUARE_OFF,
        eod_policy=v12.EOD_POLICY,
    )
    with v11_execution.installed_runtime_hooks(
        v11_backtest.FIXED_RUNTIME_SPEC, allow_composite=True
    ):
        with v12_execution.installed_runtime_hooks(runtime_spec):
            with v11_gap.installed_gap_guard(gap):
                audit = experiment._NEUTRAL_RUN_BACKTEST(
                    prepared.candidates,
                    minute_paths,
                    variant=v12.PROFILE_ID,
                    policy=policy,
                    target_exposure_per_entry_rs=v12.TARGET_EXPOSURE_PER_ENTRY_RS,
                )
    audit = audit.copy()
    audit["v12_variant_id"] = v12.PROFILE_ID
    audit["v12_stage_id"] = v12.STAGE_ID
    audit["v12_family"] = v12.FAMILY
    audit["strategy_version"] = STRATEGY_VERSION
    return audit, prepared


# --------------------------------------------------------------------------
# metrics
# --------------------------------------------------------------------------
def _pnl_column(audit: pd.DataFrame) -> str:
    for name in ("net_return_pct", "net_pnl_rs", "net_return_percentage_points"):
        if name in audit.columns:
            return name
    raise KeyError("No recognised P&L column on the V12 audit frame.")


def _day_series(audit: pd.DataFrame) -> pd.Series:
    for name in ("day", "session_date", "signal_day"):
        if name in audit.columns:
            return pd.to_datetime(audit[name]).dt.date
    raise KeyError("No recognised session-date column on the V12 audit frame.")


def period_metrics(audit: pd.DataFrame, label: str, days: list[date]) -> dict[str, Any]:
    pnl = _pnl_column(audit)
    day = _day_series(audit)
    filled = audit["filled"].astype(bool) if "filled" in audit.columns else pd.Series(
        True, index=audit.index
    )
    subset = audit.loc[day.isin(set(days)) & filled]
    values = subset[pnl].to_numpy(float)
    wins = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    return {
        "period": label,
        "sessions": len(days),
        "orders": int((day.isin(set(days))).sum()),
        "fills": int(len(values)),
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "win_rate_pct": round(100.0 * (values > 0).mean(), 4) if values.size else np.nan,
        "trade_pf": round(wins / losses, 6) if losses > 0 else np.inf,
        "net": round(float(values.sum()), 6),
        "expectancy": round(float(values.mean()), 6) if values.size else np.nan,
        "pnl_column": pnl,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through-day", default=DEFAULT_THROUGH_DAY)
    parser.add_argument("--split-day", default=DEFAULT_SPLIT_DAY)
    parser.add_argument("--min-contract-coverage", type=float, default=0.80)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument(
        "--all-cost-scenarios",
        action="store_true",
        help="run V12's stress scenarios as well as REFERENCE_15_0",
    )
    parser.add_argument(
        "--pin-v12-hash",
        action="store_true",
        help="print the current locked-V12 hash for pinning and continue",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    hashes = _verify_sources_frozen(args.pin_v12_hash)
    if args.pin_v12_hash:
        print(f"[PIN] locked V12 SHA-256 = {hashes['v12_locked_sha256']}", flush=True)

    # Strategy-identity guard: this validates V12's registry, resolved config
    # and variant identity. It is about the strategy, not the data, so it must
    # still pass on corrected input.
    v12.validate_fixed_contract(require_files=True)

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    through_day = pd.Timestamp(args.through_day).date()
    split_day = pd.Timestamp(args.split_day).date()

    candidates, minute_paths, eligibility, regime_records, calendar = (
        build_corrected_history(
            through_day,
            min_coverage=args.min_contract_coverage,
            rebuild=args.rebuild_cache,
        )
    )
    common.atomic_write_csv(eligibility, ELIGIBILITY_PATH)
    ok = eligibility.loc[eligibility["eligible"]]
    print(
        f"[V12-CORRECTED] eligible sessions {len(ok)} of {len(eligibility)} | "
        f"candidates {len(candidates):,}",
        flush=True,
    )
    for reason, group in eligibility.loc[~eligibility["eligible"]].groupby("reason"):
        print(
            f"               dropped {len(group):>3}  {reason}  "
            f"{group['day'].min()} .. {group['day'].max()}",
            flush=True,
        )

    scenarios = (
        gaps.COST_SCENARIOS if args.all_cost_scenarios else gaps.COST_SCENARIOS[:1]
    )
    scenario_rows: list[dict[str, Any]] = []
    headline_audit: pd.DataFrame | None = None
    headline_prepared = None

    for name, cost_bps, slippage_bps in scenarios:
        audit, prepared = run_locked_v12(
            candidates, minute_paths, cost_bps=cost_bps, slippage_bps=slippage_bps
        )
        day = _day_series(audit)
        days = sorted(set(day))
        row = {"scenario": name, "cost_bps": cost_bps, "slippage_bps": slippage_bps}
        row.update({k: v for k, v in period_metrics(audit, "ALL", days).items()
                    if k != "period"})
        scenario_rows.append(row)
        print(
            f"[V12-CORRECTED] {name}: sessions={len(days)} "
            f"fills={row['fills']} PF={row['trade_pf']} net={row['net']}",
            flush=True,
        )
        if headline_audit is None:
            headline_audit, headline_prepared = audit, prepared

    assert headline_audit is not None
    day = _day_series(headline_audit)
    days = sorted(set(day))
    train = [d for d in days if d < split_day]
    test = [d for d in days if d >= split_day]
    periods = pd.DataFrame(
        [
            period_metrics(headline_audit, "TRAIN", train),
            period_metrics(headline_audit, "TEST", test),
            period_metrics(headline_audit, "ALL", days),
        ]
    )

    pnl = _pnl_column(headline_audit)
    filled = headline_audit["filled"].astype(bool) \
        if "filled" in headline_audit.columns else pd.Series(True, index=headline_audit.index)
    daily = (
        headline_audit.loc[filled]
        .assign(day=day.loc[filled])
        .groupby("day", as_index=False)[pnl]
        .sum()
        .rename(columns={pnl: "net"})
    )
    setup_col = "setup_id" if "setup_id" in headline_audit.columns else None
    setups = (
        headline_audit.loc[filled]
        .groupby(setup_col, as_index=False)
        .agg(fills=(pnl, "size"), net=(pnl, "sum"))
        if setup_col
        else pd.DataFrame()
    )

    common.atomic_write_csv(headline_audit, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)
    common.atomic_write_csv(periods, PERIODS_PATH)
    common.atomic_write_csv(pd.DataFrame(scenario_rows), SCENARIOS_PATH)
    if not setups.empty:
        common.atomic_write_csv(setups, SETUPS_PATH)

    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "strategy_version": STRATEGY_VERSION,
            "evidence_status": EVIDENCE_STATUS,
            "config_source": CONFIG_SOURCE,
            "roll_policy": ROLL_POLICY,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "source_hashes": hashes,
            "locked_v12": {
                "profile_id": v12.PROFILE_ID,
                "stage_id": v12.STAGE_ID,
                "family": v12.FAMILY,
                "gap_variant": v12.GAP_VARIANT,
                "eod_policy": v12.EOD_POLICY,
                "square_off": v12.SQUARE_OFF,
                "target_exposure_per_entry_rs": v12.TARGET_EXPOSURE_PER_ENTRY_RS,
                "resolved_config_sha256": v12.EXPECTED_RESOLVED_CONFIG_SHA256,
            },
            "expiry_calendar": {k: str(v) for k, v in calendar.items()},
            "regimes": regime_records,
            "parameters": {
                "through_day": str(through_day),
                "split_day": str(split_day),
                "min_contract_coverage": float(args.min_contract_coverage),
                "scenarios": [list(s) for s in scenarios],
            },
            "sessions_replayed": len(days),
            "sessions_dropped": int((~eligibility["eligible"]).sum()),
            "candidates": int(len(candidates)),
            "selected_candidates": int(len(headline_prepared.candidates))
            if headline_prepared is not None
            else None,
            "period_metrics": periods.to_dict("records"),
            "cost_scenarios": scenario_rows,
        },
    )

    after = _verify_sources_frozen(True)
    if after != hashes:
        raise AssertionError("A pinned source changed during the V12-corrected run.")

    print("", flush=True)
    for row in periods.to_dict("records"):
        print(
            f"[{row['period']:<5}] sessions={row['sessions']:>3} fills={row['fills']:>4} "
            f"PF={row['trade_pf']} net={row['net']}",
            flush=True,
        )
    print(f"[STATUS] {EVIDENCE_STATUS}", flush=True)
    print(f"[WROTE] {RESULT_DIR}", flush=True)
    print(f"[DONE] {time.monotonic() - started:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
