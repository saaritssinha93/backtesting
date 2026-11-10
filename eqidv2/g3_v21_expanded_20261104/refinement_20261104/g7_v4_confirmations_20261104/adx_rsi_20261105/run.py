"""Isolated V21 replay of fixed ADX14 and RSI14 entry checks.

The saved G7 entry checks run first. New checks read only completed
confirmation-minute indicators, without promoting the next ranked stock.
Native V21 fills, exits, sizing and costs are reused without modification.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104.refinement_20261104 import run as finite
from g3_v21_expanded_20261104.refinement_20261104 import unlimited
from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as v21
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import policy as g7
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import run as g7_run
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import run_latest
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.latest_day import DAY, build_expanded_day
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.improvement_20261104 import run as comparison
from . import features


PACKAGE = Path(__file__).resolve().parent
SAVED_G7 = PACKAGE.parent / "results_through_20261104"
SAVED_64 = PACKAGE.parent / "results_full_20261104"
ARMS = (
    "all_g7",
    "adx14_ge25",
    "di_direction",
    "adx14_ge25_and_di_direction",
    "rsi14_directional_50",
    "adx14_ge25_di_direction_rsi14_directional_50",
    "adx14_rising3_di_direction_rsi14_rising3",
)
DESCRIPTIONS = {
    "all_g7": "Existing V21 plus all four G7 entry checks",
    "adx14_ge25": "Additional session-reset Wilder ADX14 >= 25 when ready",
    "di_direction": "Additional +DI14 > -DI14 for LONG, reverse for SHORT",
    "adx14_ge25_and_di_direction": "Both ADX14 >= 25 and directional DI",
    "rsi14_directional_50": "Additional RSI14 >= 50 for LONG, <= 50 for SHORT",
    "adx14_ge25_di_direction_rsi14_directional_50":
        "ADX14 >= 25, DI direction, and directional RSI14 midpoint",
    "adx14_rising3_di_direction_rsi14_rising3":
        "ADX14 not falling over three minutes, DI direction, and RSI14 not weakening over three minutes",
}
G7_COLUMNS = tuple(g7.ARM_GATES[g7.ARMS[-1]])


def _validate_saved_audit(selected: pd.DataFrame) -> pd.DataFrame:
    """Hash-check saved decisions and match every selected native order."""
    comparison._verify_manifest(SAVED_G7)
    comparison._verify_manifest(SAVED_64)
    audit = pd.read_csv(SAVED_G7 / "admission_audit.csv", low_memory=False)
    old = pd.read_csv(SAVED_64 / "admission_audit.csv", low_memory=False)
    ids = set(selected.sid.astype(int))
    if (len(audit) != len(selected) or audit.sid.duplicated().any() or
            set(audit.sid.astype(int)) != ids or
            set(old.sid.astype(int)) != set(audit.loc[
                audit.day.astype(str).ne(DAY), "sid"].astype(int))):
        raise ValueError("Saved G7 audit does not exactly cover V21 selected orders")
    if (not audit.research_policy_fingerprint.eq(g7.fingerprint()).all() or
            not old.research_policy_fingerprint.eq(g7.fingerprint()).all()):
        raise ValueError("Saved G7 decision fingerprint changed")
    for column in ("day", "setup_id", "tradingsymbol", "side", "confirmation_ts"):
        left = selected.set_index("sid")[column].astype(str).sort_index()
        right = audit.set_index("sid")[column].astype(str).sort_index()
        if column == "confirmation_ts":
            x = pd.to_datetime(left, utc=True)
            y = pd.to_datetime(right, utc=True)
            if not x.equals(y):
                raise ValueError("Saved G7 confirmation clock differs from V21")
        elif not left.equals(right):
            raise ValueError(f"Saved G7 identity differs from V21: {column}")
    audit = audit.sort_values("sid", kind="stable").reset_index(drop=True)
    for field in G7_COLUMNS:
        audit[field] = run_latest._flags(audit[field], field)
    audit["all_g7_allowed"] = audit[list(G7_COLUMNS)].all(axis=1)
    if not audit.all_g7_allowed.equals(
            run_latest._flags(audit.all_g7_entry_allowed, "all_g7_entry_allowed")):
        raise ValueError("Saved G7 combined decision differs from four checks")
    return audit


def _admission(selected: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    saved = _validate_saved_audit(selected)
    adx, proofs = features.build_adx_audit(selected)
    if len(adx) != len(selected) or adx.sid.duplicated().any():
        raise ValueError("ADX feature audit did not cover every V21 selected order")
    for field in ("day", "setup_id", "tradingsymbol", "side", "confirmation_ts"):
        lhs = saved.set_index("sid")[field].sort_index()
        rhs = adx.set_index("sid")[field].sort_index()
        if field == "confirmation_ts":
            same = pd.to_datetime(lhs, utc=True).equals(pd.to_datetime(rhs, utc=True))
        else:
            same = lhs.astype(str).equals(rhs.astype(str))
        if not same:
            raise ValueError(f"ADX feature identity differs from V21/G7: {field}")
    fields = ["sid", "adx14", "plus_di14", "minus_di14", "adx_delta3",
              "adx_ready", "adx_delta3_ready", "adx_available_at",
              "adx_session_bars", "adx_stock_source_sha256"]
    audit = saved.merge(adx[fields], on="sid", how="left", validate="one_to_one")
    if len(audit) != len(saved) or audit.sid.isna().any():
        raise ValueError("ADX/G7 feature join lost selected orders")
    for field in ("adx_ready", "adx_delta3_ready", "rsi_ready"):
        audit[field] = run_latest._flags(audit[field], field)
    for field, ready in (("adx14", "adx_ready"), ("plus_di14", "adx_ready"),
                         ("minus_di14", "adx_ready"),
                         ("adx_delta3", "adx_delta3_ready"),
                         ("rsi14", "rsi_ready"),
                         ("signed_rsi_delta3", "rsi_ready")):
        observed = pd.to_numeric(audit[field], errors="coerce")
        if not np.isfinite(observed.loc[audit[ready]]).all():
            raise ValueError(f"Ready indicator is missing: {field}")
        audit[field] = observed
    if (audit.adx_delta3_ready & ~audit.adx_ready).any():
        raise ValueError("ADX delta ready before ADX")
    confirm = pd.to_datetime(audit.confirmation_ts, utc=True)
    adx_time = pd.to_datetime(audit.adx_available_at, utc=True)
    rsi_time = pd.to_datetime(audit.feature_available_at, utc=True)
    if (confirm.isna().any() or adx_time.isna().any() or rsi_time.isna().any()
            or not adx_time.le(confirm).all() or not rsi_time.le(confirm).all()):
        raise ValueError("ADX or RSI observation occurred after confirmation")
    if not audit.side.isin(("LONG", "SHORT")).all():
        raise ValueError("Unknown trade side")
    long = audit.side.eq("LONG")
    adx_pass = ~audit.adx_ready | audit.adx14.ge(25.0)
    di_match = (long & audit.plus_di14.gt(audit.minus_di14)) | (
        ~long & audit.minus_di14.gt(audit.plus_di14))
    di_pass = ~audit.adx_ready | di_match
    rsi_match = (long & audit.rsi14.ge(50.0)) | (~long & audit.rsi14.le(50.0))
    rsi_pass = ~audit.rsi_ready | rsi_match
    adx_rising_pass = ~audit.adx_delta3_ready | audit.adx_delta3.ge(0.0)
    rsi_rising_pass = ~audit.rsi_ready | audit.signed_rsi_delta3.ge(0.0)
    gates = {
        "all_g7": pd.Series(True, index=audit.index),
        "adx14_ge25": adx_pass,
        "di_direction": di_pass,
        "adx14_ge25_and_di_direction": adx_pass & di_pass,
        "rsi14_directional_50": rsi_pass,
        "adx14_ge25_di_direction_rsi14_directional_50":
            adx_pass & di_pass & rsi_pass,
        "adx14_rising3_di_direction_rsi14_rising3":
            adx_rising_pass & di_pass & rsi_rising_pass,
    }
    for key in ARMS:
        audit[f"admit_{key}"] = (audit.all_g7_allowed & gates[key]).astype(bool)
    audit["adx_strength25_pass_or_missing"] = adx_pass.astype(bool)
    audit["di_direction_pass_or_missing"] = di_pass.astype(bool)
    audit["rsi_midpoint_pass_or_missing"] = rsi_pass.astype(bool)
    audit["adx_rising3_pass_or_missing"] = adx_rising_pass.astype(bool)
    audit["rsi_rising3_pass_or_missing"] = rsi_rising_pass.astype(bool)
    return audit, proofs


def _book_metrics(book: pd.DataFrame, days: list[str]) -> dict:
    result = finite._metric(book, days)
    executed = book.loc[book.portfolio_executed.eq(True) &
                        book.day.astype(str).str[:10].isin(days)]
    pnl = executed.portfolio_net_profit_rupees.astype(float)
    daily = (executed.groupby(executed.day.astype(str).str[:10])
             .portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.0))
    result["median_trade_rs"] = float(pnl.median()) if len(pnl) else np.nan
    result["median_daily_pnl_rs"] = float(daily.median())
    result["average_daily_pnl_rs"] = float(daily.mean())
    if len(pnl) != int(result["trades"]):
        raise ValueError("Executed trade denominator changed")
    return result


def _report(output: Path, overall: pd.DataFrame, audit: pd.DataFrame,
            periods: dict[str, list[str]]) -> None:
    full = overall.loc[overall.period.eq("full_65")].set_index("policy")
    later = overall.loc[overall.period.eq("later_6")].set_index("policy")
    lines = [
        "# V21 ADX14 and RSI14 entry check research through 4 November 2026", "",
        "This isolated backtest applies the saved V21 cap/volume policy and all four "
        "G7 entry checks first, then tests a small set of fixed ADX/RSI conditions. "
        "It reuses V21's native entry, exit, cost and per-order size with unlimited "
        "portfolio capital. A rejected order never promotes another stock.", "",
        "ADX14 here is newly calculated from each stock's hash-verified, completed "
        "one-minute session prefix using Wilder's directional movement smoothing. "
        "It is **session-reset**, so it is a different definition from the "
        "continuous EWM ADX column in some source snapshots. RSI14 and its "
        "direction-normalized three-minute change are the existing G7 session-reset "
        "features. ADX is trend strength; the +DI/-DI comparison supplies direction.", "",
        "When ADX or RSI has not warmed up, that new condition passes the existing "
        "G7 decision rather than substituting a future observation. The ADX threshold "
        "25, RSI midpoint 50 and nonnegative three-minute change are fixed research "
        "hypotheses, not established G3 thresholds.", "",
        "## Full 65-session comparison", "",
        "| Policy | Selected | Trades | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Median/day (Rs) | Daily-close DD (Rs) |",
        "|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm in ARMS:
        row = full.loc[arm]
        lines.append(
            f"| {arm} | {int(row.selected_orders)} | {int(row.trades)} | "
            f"{int(row.losses)} | {row.win_rate_pct:.2f}% | "
            f"{row.average_trade_rs:,.2f} | {row.median_trade_rs:,.2f} | "
            f"{row.net_pnl_rs:,.2f} | {row.median_daily_pnl_rs:,.2f} | "
            f"{row.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Later six observed sessions", "",
              "The first 59 sessions end 27 October and the later six span "
              "28 October to 4 November. All have already been observed; this "
              "split is a retrospective stability check.", "",
              "| Policy | Trades | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Daily-close DD (Rs) |",
              "|:---|---:|---:|---:|---:|---:|---:|---:|"]
    for arm in ARMS:
        row = later.loc[arm]
        lines.append(
            f"| {arm} | {int(row.trades)} | {int(row.losses)} | "
            f"{row.win_rate_pct:.2f}% | {row.average_trade_rs:,.2f} | "
            f"{row.median_trade_rs:,.2f} | {row.net_pnl_rs:,.2f} | "
            f"{row.daily_close_drawdown_rs:,.2f} |")
    all_g7 = audit.loc[audit.all_g7_allowed]
    lines += [
        "", "## Coverage and interpretation", "",
        f"- All 73 configured V21 pairs are present in the source; "
        f"{len(audit):,} orders pass the strict V21 selection and "
        f"{len(all_g7):,} pass the saved all-G7 entry checks.",
        f"- Of the all-G7 selected orders, {int(all_g7.adx_ready.sum())} have "
        f"ready ADX14 and {int(all_g7.rsi_ready.sum())} have ready RSI14. "
        "Unavailable early-session indicators are explicit in the audit.",
        "- Mean and median trade P&L are after-cost values from executed trades. "
        "The median per day includes zero-trade days. Daily-close drawdown uses "
        "realized P&L and does not measure intraday lows.",
        "- Entry gates use only confirmation-time prices and indicator decisions. "
        "All seven arms share the same already observed history. Testing several "
        "rules can overfit; no arm is activated for live trading or independently "
        "validated by these results.", "",
        f"Source sessions: {periods['full_65'][0]} through {periods['full_65'][-1]}. "
        "Saved all-G7 baseline metrics and daily P&L were reproduced. Source "
        "result manifests, stock snapshots and output file/code hashes were checked.", "",
        "[Overall results](overall.csv) | [Day-wise results](daily.csv) | "
        "[Per-order decisions](admission_audit.csv) | "
        "[ADX source proofs](adx_source_proofs.json) | "
        "[Configuration](config.json) | [Provenance](provenance.json)", ""
    ]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path) -> dict:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a fresh child of ADX/RSI research package")
    _, expanded64, days64, _ = finite._load_books()
    if len(days64) != 64 or days64[0] != "2026-07-29" or days64[-1] != "2026-11-03":
        raise ValueError("64-session V21 checkpoint inventory changed")
    expanded_latest, latest_proof = build_expanded_day(DAY)
    if set(expanded64.sid.astype(int)) & set(expanded_latest.sid.astype(int)):
        raise ValueError("4 November SID collision")
    expanded = pd.concat((expanded64, expanded_latest), ignore_index=True, sort=False)
    selected = v21.apply(expanded)
    if len(selected) != 1092 or selected.setup_id.nunique() != 73 or selected.sid.duplicated().any():
        raise ValueError("65-day strict V21 selected inventory changed")
    audit, adx_proofs = _admission(selected)
    periods = {
        "full_65": [*days64, DAY],
        "early_59": [day for day in days64 if day <= "2026-10-27"],
        "later_6": [day for day in [*days64, DAY] if day > "2026-10-27"],
        "nov04_only": [DAY],
    }
    if tuple(map(len, periods.values())) != (65, 59, 6, 1):
        raise ValueError("ADX/RSI period inventory changed")
    selected_ids = selected.sid.astype(int)
    overall_rows, daily_rows = [], []
    books = {}
    for arm in ARMS:
        admitted_ids = set(audit.loc[audit[f"admit_{arm}"], "sid"].astype(int))
        subset = selected.loc[selected_ids.isin(admitted_ids)].copy()
        if len(subset) != len(admitted_ids) or subset.sid.duplicated().any():
            raise ValueError(f"Invalid ADX/RSI order subset: {arm}")
        book = unlimited._unlimited(subset)
        books[arm] = book
        for period, days in periods.items():
            overall_rows.append(dict(period=period, policy=arm,
                                     **_book_metrics(book, days)))
        for day in periods["full_65"]:
            daily_rows.append(dict(day=day, policy=arm,
                                   **_book_metrics(book, [day])))
    overall = pd.DataFrame(overall_rows)
    daily = pd.DataFrame(daily_rows)
    saved = pd.read_csv(SAVED_G7 / "overall.csv")
    for period in ("full_65", "nov04_only"):
        actual = overall.loc[overall.period.eq(period) &
                             overall.policy.eq("all_g7")].iloc[0]
        reference = saved.loc[saved.period.eq(period) &
                              saved.policy.eq(g7.ARMS[-1])].iloc[0]
        for field in ("selected_orders", "native_fills", "trades", "wins", "losses",
                      "net_pnl_rs", "average_trade_rs", "daily_close_drawdown_rs",
                      "negative_days"):
            if not np.isclose(float(actual[field]), float(reference[field]),
                              rtol=0, atol=.01):
                raise ValueError(f"Saved all-G7 parity failed: {period} {field}")
    for arm in ARMS:
        full = overall.loc[overall.period.eq("full_65") &
                           overall.policy.eq(arm)].iloc[0]
        per_day = daily.loc[daily.policy.eq(arm)]
        if (abs(float(full.net_pnl_rs - per_day.net_pnl_rs.sum())) > .01 or
                int(full.trades) != int(per_day.trades.sum()) or
                int(full.losses) != int(per_day.losses.sum())):
            raise ValueError(f"ADX/RSI daily metric reconciliation failed: {arm}")
    summary = dict(scope="BACKTEST_ONLY", execution_authority=False,
                   interpretation_of_sdx="Assume ADX pending user clarification; no SDX code found",
                   baseline="Saved all G7 entry confirmations after strict V21 cap/volume",
                   configuration=DESCRIPTIONS, arms=list(ARMS),
                   adx_definition="WILDER14_SESSION_RESET_CONFIRMATION_PREFIX",
                   rsi_definition="SAVED_G7_WILDER14_SESSION_RESET",
                   no_rank_refill=True,
                   unavailable_indicator_policy="PASS_EXISTING_G7_DECISION",
                   source_v21_fingerprint=v21.fingerprint(),
                   source_g7_fingerprint=g7.fingerprint(),
                   all_73_pairs=True, original_pair_cap=3, new_pair_cap=1,
                   new_pair_5m_rvol_increment=.10,
                   source_manifest_hashes_verified=True,
                   saved_all_g7_parity="PASS_FULL65_AND_NOV04",
                   periods={name: dict(first=days[0], last=days[-1],
                                       sessions=len(days))
                            for name, days in periods.items()},
                   latest_source=latest_proof)
    output.mkdir(parents=True)
    overall.to_csv(output / "overall.csv", index=False)
    daily.to_csv(output / "daily.csv", index=False)
    fields = ["sid", "day", "setup_id", "tradingsymbol", "side",
              "confirmation_ts", "all_g7_allowed", "adx14", "plus_di14",
              "minus_di14", "adx_delta3", "adx_ready", "adx_delta3_ready",
              "adx_session_bars", "adx_available_at",
              "adx_stock_source_sha256", "rsi14", "signed_rsi_delta3",
              "rsi_ready", "feature_available_at",
              "adx_strength25_pass_or_missing", "di_direction_pass_or_missing",
              "rsi_midpoint_pass_or_missing", "adx_rising3_pass_or_missing",
              "rsi_rising3_pass_or_missing",
              *(f"admit_{arm}" for arm in ARMS)]
    audit[fields].to_csv(output / "admission_audit.csv", index=False)
    (output / "adx_source_proofs.json").write_text(json.dumps(
        adx_proofs, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    (output / "config.json").write_text(json.dumps(
        summary, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    code = (Path(__file__), Path(features.__file__), Path(v21.__file__),
            Path(g7.__file__), Path(finite.__file__), Path(unlimited.__file__),
            Path(build_expanded_day.__code__.co_filename))
    provenance = dict(
        code_sha256={str(path): g7_run._sha(path) for path in code},
        saved_65_result_manifest=str(SAVED_G7 / "manifest.json"),
        saved_65_result_manifest_sha256=g7_run._sha(SAVED_G7 / "manifest.json"),
        saved_64_result_manifest_sha256=g7_run._sha(SAVED_64 / "manifest.json"),
        checkpoint_sha256={path.name: g7_run._sha(path) for path in
                           sorted(finite.CHECKPOINTS.glob("[0-9][0-9]_*.pkl"))})
    (output / "provenance.json").write_text(json.dumps(
        provenance, indent=2) + "\n", encoding="utf-8")
    _report(output, overall, audit, periods)
    (output / "manifest.json").write_text(json.dumps(
        {path.name: g7_run._sha(path) for path in output.iterdir() if path.is_file()},
        indent=2) + "\n", encoding="utf-8")
    print(overall.loc[overall.period.eq("full_65"),
          ["policy", "selected_orders", "trades", "losses", "win_rate_pct",
           "average_trade_rs", "median_trade_rs", "net_pnl_rs",
           "daily_close_drawdown_rs"]].to_string(index=False), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path,
                        default=PACKAGE / "results_through_20261104")
    args = parser.parse_args()
    run(args.output)


if __name__ == "__main__":
    main()
