"""Replay saved ADX/RSI masks on the post-hoc noon-CHOP V21 candidate.

This is a subset of the hash-verified, unlimited-capital all-G7 native fills.
All admissions were computed at confirmation, and vetoes do not refill ranks.
The noon-CHOP rule was chosen after inspecting history; this overlay cannot
serve as independent validation of that rule or of the ADX/RSI hypotheses.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


PACKAGE = Path(__file__).resolve().parent
ADX_SOURCE = PACKAGE.parent / "results_through_20261104"
G7_PACKAGE = PACKAGE.parents[1]
CHOP_SOURCE = (G7_PACKAGE / "improvement_20261104" /
               "results_final_20261104_labeled")
G7_SOURCE = G7_PACKAGE / "results_through_20261104"
NOON_POLICY = "all_g7_plus_noon_chop14_le50"
ARMS = (
    "all_g7",
    "adx14_ge25",
    "di_direction",
    "adx14_ge25_and_di_direction",
    "rsi14_directional_50",
    "adx14_ge25_di_direction_rsi14_directional_50",
    "adx14_rising3_di_direction_rsi14_rising3",
)


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for part in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def _verified_source(folder: Path) -> dict[str, str]:
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or not manifest:
        raise ValueError(f"Missing source manifest: {folder}")
    for name, expected in manifest.items():
        path = folder / name
        if Path(name).name != name or not path.is_file() or _sha(path) != expected:
            raise ValueError(f"Source manifest mismatch: {path}")
    provenance_path = folder / "provenance.json"
    if provenance_path.is_file():
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        for name, expected in provenance.get("code_sha256", {}).items():
            path = Path(name)
            if not path.is_file() or _sha(path) != expected:
                raise ValueError(f"Source code fingerprint drift: {path}")
    return manifest


def _flag(values: pd.Series, name: str) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        if values.isna().any():
            raise ValueError(f"Missing {name} decision")
        return values.astype(bool)
    text = values.astype(str).str.strip().str.lower()
    if not text.isin(("true", "false")).all():
        raise ValueError(f"Invalid {name} decision")
    return text.eq("true")


def _same_identities(adx: pd.DataFrame, chop: pd.DataFrame,
                     trades: pd.DataFrame) -> None:
    if (len(adx) != 1092 or len(chop) != len(adx) or
            adx.sid.isna().any() or chop.sid.isna().any() or
            adx.sid.duplicated().any() or chop.sid.duplicated().any() or
            set(adx.sid.astype(int)) != set(chop.sid.astype(int))):
        raise ValueError("ADX and noon-CHOP decisions do not cover the same 1,092 orders")
    for key in ("day", "setup_id", "tradingsymbol", "side"):
        left = adx.set_index("sid")[key].astype(str).sort_index()
        right = chop.set_index("sid")[key].astype(str).sort_index()
        if not left.equals(right):
            raise ValueError(f"Saved decision identity mismatch: {key}")
    left = pd.to_datetime(adx.set_index("sid").confirmation_ts.sort_index(), utc=True)
    right = pd.to_datetime(chop.set_index("sid").confirmation_ts.sort_index(), utc=True)
    if not left.equals(right):
        raise ValueError("ADX and CHOP confirmation timestamps differ")
    if (len(trades) != 771 or trades.sid.isna().any() or
            trades.sid.duplicated().any() or not set(trades.sid.astype(int)).issubset(set(adx.sid.astype(int)))):
        raise ValueError("Saved all-G7 native trade inventory changed")
    identified = trades.merge(adx[["sid", "day", "setup_id", "tradingsymbol", "side",
                                   "confirmation_ts"]], on="sid", how="left",
                              suffixes=("", "_audit"), validate="one_to_one")
    for key in ("day", "setup_id", "tradingsymbol", "side"):
        if not identified[key].astype(str).equals(identified[f"{key}_audit"].astype(str)):
            raise ValueError(f"Saved fill identity mismatch: {key}")
    confirmation = pd.to_datetime(identified.confirmation_ts, utc=True)
    entry = pd.to_datetime(identified.entry_ts, utc=True)
    if confirmation.isna().any() or entry.isna().any() or not entry.gt(confirmation).all():
        raise ValueError("An all-G7 native fill is not after its confirmation")


def _metric(fills: pd.DataFrame, selected: int, days: list[str],
            cost_per_fill: float) -> dict:
    part = fills.loc[fills.day.astype(str).isin(days)]
    pnl = pd.to_numeric(part.portfolio_net_profit_rupees, errors="raise")
    if pnl.isna().any() or not np.isfinite(pnl.to_numpy(float)).all():
        raise ValueError("Invalid native after-cost trade result")
    daily = (part.assign(_pnl=pnl).groupby(part.day.astype(str))._pnl.sum()
             .reindex(days, fill_value=0.0))
    curve = np.r_[0.0, daily.cumsum().to_numpy(float)]
    gains = float(pnl.clip(lower=0).sum())
    losses_abs = float(-pnl.clip(upper=0).sum())
    return dict(sessions=len(days), selected_orders=int(selected),
                native_fills=len(part), trades=len(part), wins=int(pnl.gt(0).sum()),
                losses=int(pnl.lt(0).sum()),
                win_rate_pct=100.0 * float(pnl.gt(0).mean()) if len(pnl) else np.nan,
                net_pnl_rs=float(pnl.sum()),
                profit_factor=gains / losses_abs if losses_abs else np.nan,
                average_trade_rs=float(pnl.mean()) if len(pnl) else np.nan,
                median_trade_rs=float(pnl.median()) if len(pnl) else np.nan,
                median_daily_pnl_rs=float(daily.median()),
                average_daily_pnl_rs=float(daily.mean()),
                daily_close_drawdown_rs=float((np.maximum.accumulate(curve) - curve).max()),
                worst_day_rs=float(daily.min()), best_day_rs=float(daily.max()),
                costs_rs=float(cost_per_fill * len(part)),
                negative_days=int(daily.lt(0).sum()),
                positive_days=int(daily.gt(0).sum()))


def _check_parity(overall: pd.DataFrame, daily: pd.DataFrame,
                  source_overall: pd.DataFrame, source_daily: pd.DataFrame) -> None:
    fields = ("sessions", "selected_orders", "native_fills", "trades", "wins",
              "losses", "win_rate_pct", "net_pnl_rs", "profit_factor",
              "average_trade_rs", "median_trade_rs", "median_daily_pnl_rs",
              "average_daily_pnl_rs", "daily_close_drawdown_rs", "worst_day_rs",
              "best_day_rs", "costs_rs", "negative_days", "positive_days")
    for period in ("full_65", "early_59", "later_6", "nov04_only"):
        actual = overall.loc[overall.period.eq(period) & overall.policy.eq(ARMS[0])]
        expected = source_overall.loc[source_overall.period.eq(period) &
                                      source_overall.policy.eq(NOON_POLICY)]
        if len(actual) != 1 or len(expected) != 1:
            raise ValueError(f"Noon-CHOP baseline missing for {period}")
        for field in fields:
            if not np.isclose(float(actual.iloc[0][field]), float(expected.iloc[0][field]),
                              rtol=0, atol=.01, equal_nan=True):
                raise ValueError(f"Noon-CHOP baseline parity failed: {period} {field}")
    expected_daily = source_daily.loc[source_daily.policy.eq(NOON_POLICY)].set_index("day")
    observed_daily = daily.loc[daily.policy.eq(ARMS[0])].set_index("day")
    if set(expected_daily.index) != set(observed_daily.index) or len(expected_daily) != 65:
        raise ValueError("Noon-CHOP daily session inventory changed")
    for day in expected_daily.index:
        for field in fields:
            if not np.isclose(float(observed_daily.loc[day, field]),
                              float(expected_daily.loc[day, field]),
                              rtol=0, atol=.01, equal_nan=True):
                raise ValueError(f"Noon-CHOP daily parity failed: {day} {field}")


def _report(output: Path, overall: pd.DataFrame, summary: dict) -> None:
    full = overall.loc[overall.period.eq("full_65")].set_index("policy")
    later = overall.loc[overall.period.eq("later_6")].set_index("policy")
    baseline = full.loc[ARMS[0]]
    lines = ["# V21 noon-CHOP with ADX/RSI overlay through 4 November 2026", "",
             "Backtest-only replay of the seven saved ADX/RSI admission masks on the "
             "earlier **post-hoc** all-G7 + CHOP14<=50 from noon candidate. The same "
             "native V21 fills and after-cost P&L are used, with unlimited capital "
             "and no promotion of the next ranked stock. Both the noon-CHOP clock "
             "condition and these ADX/RSI combinations were examined on this "
             "history; this is **not a forward validation** or a live setting.", "",
             "## All 65 sealed sessions", "",
             "| Extra ADX/RSI mask | Selected | Fills | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Median/day (Rs) | Daily-close DD (Rs) |",
             "|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for arm in ARMS:
        item = full.loc[arm]
        lines.append(f"| {arm} | {int(item.selected_orders)} | {int(item.trades)} | "
                     f"{int(item.losses)} | {item.win_rate_pct:.2f}% | "
                     f"{item.average_trade_rs:,.2f} | {item.median_trade_rs:,.2f} | "
                     f"{item.net_pnl_rs:,.2f} | {item.median_daily_pnl_rs:,.2f} | "
                     f"{item.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Six later observed sessions", "",
              "28 October through 4 November is a retrospective sensitivity check. "
              "Only two filled trades were vetoed by the noon-CHOP condition in "
              "this period, so it offers very little evidence for that rule.", "",
              "| Extra ADX/RSI mask | Fills | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Daily-close DD (Rs) |",
              "|:---|---:|---:|---:|---:|---:|---:|---:|"]
    for arm in ARMS:
        item = later.loc[arm]
        lines.append(f"| {arm} | {int(item.trades)} | {int(item.losses)} | "
                     f"{item.win_rate_pct:.2f}% | {item.average_trade_rs:,.2f} | "
                     f"{item.median_trade_rs:,.2f} | {item.net_pnl_rs:,.2f} | "
                     f"{item.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Interpretation", "",
              "- The `all_g7` row is the exact noon-CHOP baseline, even though its "
              "name reflects the ADX source arm. Its earlier result matches every "
              "available metric and each daily row within Rs 0.01.",
              f"- Baseline: {int(baseline.trades)} fills, {int(baseline.losses)} losses, "
              f"Rs {baseline.net_pnl_rs:,.2f} net. ADX/RSI masks can raise win "
              "rate and per-trade profit while skipping profitable trades; the "
              "table shows the total-P&L and drawdown tradeoffs.",
              "- ADX14 is a session-reset Wilder feature from exact, hash-verified "
              "stock confirmation prefixes. RSI14 is the saved G7 session-reset "
              "feature. Missing early indicators pass through. Every mask is "
              "causal at confirmation, but thresholds were tested against the "
              "already observed 65 sessions.",
              "- Daily-close drawdown uses realized daily P&L, not intraday lows. "
              "All after-cost trade amounts use the saved V21 unlimited-capital "
              "allocation at unchanged per-entry size and exit rules.", "",
              "[Overall](overall.csv) | [Day-wise](daily.csv) | "
              "[Admission decisions](admission_audit.csv) | "
              "[Trade decisions](trade_decisions.csv) | "
              "[Configuration](config.json) | [Provenance](provenance.json)", ""]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path) -> dict:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a fresh child of the isolated overlay package")
    sources = {"adx": ADX_SOURCE, "noon_chop": CHOP_SOURCE, "all_g7": G7_SOURCE}
    source_manifests = {key: _verified_source(path) for key, path in sources.items()}
    adx = pd.read_csv(ADX_SOURCE / "admission_audit.csv", low_memory=False)
    chop = pd.read_csv(CHOP_SOURCE / "exploratory_admission_audit.csv", low_memory=False)
    trades = pd.read_csv(G7_SOURCE / "trades.csv", low_memory=False)
    _same_identities(adx, chop, trades)
    for field in ("all_g7_allowed", *(f"admit_{arm}" for arm in ARMS)):
        adx[field] = _flag(adx[field], field)
    for field in ("all_g7_admitted", "all_g7_plus_noon_chop14_admitted"):
        chop[field] = _flag(chop[field], field)
    if not adx.all_g7_allowed.equals(chop.all_g7_admitted):
        raise ValueError("Saved all-G7 admission differs between source audits")
    if not adx.admit_all_g7.equals(adx.all_g7_allowed):
        raise ValueError("ADX source baseline differs from all-G7 decisions")
    if (chop.all_g7_plus_noon_chop14_admitted & ~chop.all_g7_admitted).any():
        raise ValueError("Noon-CHOP admitted a non-G7 order")
    base_ids = set(adx.loc[adx.all_g7_allowed, "sid"].astype(int))
    if not set(trades.sid.astype(int)).issubset(base_ids):
        raise ValueError("All-G7 trade ledger includes a rejected order")
    adx = adx.sort_values("sid", kind="stable").reset_index(drop=True)
    chop = chop.sort_values("sid", kind="stable").reset_index(drop=True)
    if not adx.sid.astype(int).equals(chop.sid.astype(int)):
        raise ValueError("Decision SID alignment failed")
    source_overall = pd.read_csv(CHOP_SOURCE / "exploratory_overall.csv")
    source_daily = pd.read_csv(CHOP_SOURCE / "exploratory_daily.csv")
    days = source_daily.loc[source_daily.policy.eq(NOON_POLICY), "day"].astype(str).tolist()
    if (len(days) != 65 or len(set(days)) != 65 or
            days[0] != "2026-07-29" or days[-1] != "2026-11-04" or days != sorted(days)):
        raise ValueError("Saved 65-session inventory changed")
    if not set(adx.day.astype(str)).issubset(set(days)) or not set(trades.day.astype(str)).issubset(set(days)):
        raise ValueError("Order or fill outside sealed sessions")
    periods = {"full_65": days,
               "early_59": [day for day in days if day <= "2026-10-27"],
               "later_6": [day for day in days if day > "2026-10-27"],
               "nov04_only": ["2026-11-04"]}
    if tuple(map(len, periods.values())) != (65, 59, 6, 1):
        raise ValueError("Session split changed")
    source_base = source_overall.loc[source_overall.period.eq("full_65") &
                                      source_overall.policy.eq(NOON_POLICY)]
    if len(source_base) != 1 or int(source_base.iloc[0].trades) != 727:
        raise ValueError("Saved noon-CHOP baseline changed")
    cost_per_fill = float(source_base.iloc[0].costs_rs / source_base.iloc[0].trades)
    if not np.isclose(cost_per_fill, 250.0, rtol=0, atol=.001):
        raise ValueError("V21 fixed per-fill cost changed")
    noon = chop.all_g7_plus_noon_chop14_admitted
    decisions = adx[["sid", "day", "setup_id", "tradingsymbol", "side",
                     "confirmation_ts", "all_g7_allowed", "adx14",
                     "plus_di14", "minus_di14", "adx_delta3", "adx_ready",
                     "adx_delta3_ready", "rsi14", "signed_rsi_delta3",
                     "rsi_ready"]].copy()
    decisions["post_hoc_noon_chop_admitted"] = noon.to_numpy(bool)
    overall_rows, daily_rows = [], []
    trade_decisions = trades.copy()
    for arm in ARMS:
        allowed = noon & adx[f"admit_{arm}"]
        ids = set(adx.loc[allowed, "sid"].astype(int))
        decisions[f"admit_noon_chop_and_{arm}"] = allowed.to_numpy(bool)
        trade_decisions[f"admit_noon_chop_and_{arm}"] = trade_decisions.sid.astype(int).isin(ids)
        fills = trades.loc[trades.sid.astype(int).isin(ids)].copy()
        for name, cohort in periods.items():
            selected = int((allowed & adx.day.astype(str).isin(cohort)).sum())
            overall_rows.append(dict(period=name, policy=arm,
                                     **_metric(fills, selected, cohort, cost_per_fill)))
        for day in days:
            selected = int((allowed & adx.day.astype(str).eq(day)).sum())
            daily_rows.append(dict(day=day, policy=arm,
                                   **_metric(fills, selected, [day], cost_per_fill)))
    overall = pd.DataFrame(overall_rows)
    daily = pd.DataFrame(daily_rows)
    _check_parity(overall, daily, source_overall, source_daily)
    for arm in ARMS:
        full = overall.loc[overall.period.eq("full_65") & overall.policy.eq(arm)].iloc[0]
        part = daily.loc[daily.policy.eq(arm)]
        if (abs(float(part.net_pnl_rs.sum()) - float(full.net_pnl_rs)) > .01 or
                int(part.trades.sum()) != int(full.trades) or
                int(part.losses.sum()) != int(full.losses) or
                int(part.selected_orders.sum()) != int(full.selected_orders)):
            raise ValueError(f"Day-wise metrics failed to reconcile: {arm}")
    summary = dict(scope="BACKTEST_ONLY", execution_authority=False,
                   source_variant="V21_STRICT_CAP_VOLUME_PLUS_ALL_G7_PLUS_POST_HOC_NOON_CHOP",
                   source_adx_variant="SEVEN_FIXED_ADX_RSI_ADMISSION_MASKS",
                   noon_chop_clock_condition="POST_HOC_SELECTED_AFTER_VIEWING_HISTORY",
                   no_independent_forward_validation=True,
                   no_next_rank_promotion=True,
                   all_native_fills_execute=True,
                   fixed_cost_per_fill_rupees=cost_per_fill,
                   source_result_manifests_verified={key: len(items)
                                                     for key, items in source_manifests.items()},
                   baseline_parity="PASS_ALL_METRICS_ALL_PERIODS_AND_65_DAILY_ROWS",
                   periods={key: dict(first=cohort[0], last=cohort[-1],
                                      sessions=len(cohort)) for key, cohort in periods.items()},
                   arms=list(ARMS))
    output.mkdir(parents=True)
    overall.to_csv(output / "overall.csv", index=False)
    daily.to_csv(output / "daily.csv", index=False)
    decisions.to_csv(output / "admission_audit.csv", index=False)
    trade_decisions.to_csv(output / "trade_decisions.csv", index=False)
    (output / "config.json").write_text(json.dumps(
        summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (output / "provenance.json").write_text(json.dumps(dict(
        code_sha256={str(Path(__file__)): _sha(Path(__file__))},
        source_manifest_sha256={key: _sha(folder / "manifest.json")
                                for key, folder in sources.items()},
        source_audit_sha256={"adx": _sha(ADX_SOURCE / "admission_audit.csv"),
                             "noon_chop": _sha(CHOP_SOURCE / "exploratory_admission_audit.csv"),
                             "all_g7_trades": _sha(G7_SOURCE / "trades.csv")}),
        indent=2) + "\n", encoding="utf-8")
    _report(output, overall, summary)
    (output / "manifest.json").write_text(json.dumps(
        {file.name: _sha(file) for file in output.iterdir() if file.is_file()},
        indent=2) + "\n", encoding="utf-8")
    print(overall.loc[overall.period.eq("full_65"),
        ["policy", "selected_orders", "trades", "losses", "win_rate_pct",
         "average_trade_rs", "median_trade_rs", "net_pnl_rs",
         "daily_close_drawdown_rs"]].to_string(index=False), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PACKAGE / "results_through_20261104")
    args = parser.parse_args()
    run(args.output)


if __name__ == "__main__":
    main()
