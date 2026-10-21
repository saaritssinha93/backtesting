"""Entry-indicator sweep for G4 09:20 SHORT on a chronological split.

The first 35 sessions are the only rows used to rank configurations.  The
later 17 sessions and October are opened after that choice.  This is isolated
research: it does not edit a G4 configuration or latest-run pointer.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as paired
from research import g3_0920_joint_70_search as grid_source
from research import g3_0920_parameter_search as coarse
from research import g3_v2_contract as selection_contract
from research import g4_0920_short_parameter_sweep as exit_study
from research import g4_iterative_dataset as dataset
from research import g4_g3_mirror_backtest as base
from research import g4_failed_breakout_research as sealed
from research import g4_trade_monitor_backtest as portfolio
from research.g3_0920_strategy import reference_profile
from research.g3_0920_strategy import selection_audit_0920, setup_specs


VERSION = "G4-0920-SHORT-INDICATOR-SWEEP-V1"
SETUP = "0921_SHORT"
FEATURES = Path("outputs/g4_0920_short_indicator_features_20261016/features.parquet")
DATA = exit_study.DATA
DATA_MANIFEST_SHA256 = exit_study.DATA_MANIFEST_SHA256
SOURCE_MANIFEST_SHA256 = exit_study.SOURCE_MANIFEST_SHA256
EPS = 1e-7
MIN_DEVELOPMENT_TRADES = 5
MIN_LATER_TRADES = 4
TODAY = "2026-10-16"
NIFTY_THRESHOLDS = (-.50, -.30, -.20, -.10, -.05, 0., .10)


def short_tuple(profile) -> tuple[float, ...]:
    rule = profile.short
    return tuple(float(v) for v in (
        rule.five_minute.active_price_pct, rule.five_minute.core_price_pct,
        rule.five_minute.oi_change_pct, rule.five_minute.volume_ratio,
        rule.one_minute.body_ratio, rule.one_minute.max_wick_ratio,
        rule.one_minute.confirmation_volume_ratio))


def profile_grid():
    """Cross 494 unique SHORT configurations with seven NIFTY gates."""
    unique = {}
    for five_name, one_name, profile in grid_source.search_grid():
        unique.setdefault(short_tuple(profile), (five_name, one_name, profile))
    current = reference_profile()
    unique.setdefault(short_tuple(current), ("current", "current", current))
    combinations = [(values, nifty, names) for values, names in unique.items()
                    for nifty in NIFTY_THRESHOLDS]
    rows = []
    for number, (values, nifty, (five_name, one_name, profile)) in enumerate(
            sorted(combinations, key=lambda x:(x[0],x[1])), 1):
        rows.append(dict(policy=f"indicator_{number:04d}", five_tier=five_name,
                         one_tier=one_name, profile=profile, values=values,
                         nifty_threshold=float(nifty)))
    current_key = short_tuple(current)
    for row in rows:
        if row["values"] == current_key and row["nifty_threshold"] == -.05:
            row["policy"] = "baseline"
    return sorted(rows, key=lambda r: (r["policy"] != "baseline", r["policy"]))


def configuration(row: dict) -> dict:
    active, core, oi, volume, body, wick, confirmation_volume = row["values"]
    return dict(active_price_pct=active, core_price_pct=core, oi_change_pct=oi,
                five_minute_volume_ratio=volume, confirmation_body_ratio=body,
                maximum_confirmation_wick_ratio=wick,
                confirmation_volume_ratio=confirmation_volume,
                nifty_first_bar_return_max_pct=row["nifty_threshold"],
                max_entries=2, picker="max_volume", target_pct=1.5,
                initial_stop_pct=1.2, tightened_stop_pct=1.0,
                tighten_after_minutes=120)


class PreparedSelection:
    """Cache validated sources, while allowing the NIFTY gate to be swept."""
    def __init__(self, features: pd.DataFrame):
        self.audit = selection_audit_0920(features, profile=grid_source.broad_profile())
        self.pools = {}
        for row in self.audit.loc[self.audit.side.eq("SHORT")].to_dict("records"):
            reasons = set(filter(None, str(row.get("source_reasons", "")).split(";")))
            if reasons - {"NIFTY_FIRST_BAR_SHORT_GATE"}:
                continue
            self.pools.setdefault(row["day"], []).append(row)

    @staticmethod
    def _passes(row, rule, nifty_threshold):
        fields = ("price_change_pct","oi_change_pct","volume_ratio","body_ratio",
                  "wick_ratio","v9_1m_volume_ratio","ema9","ema20","ema50",
                  "confirm_open","confirm_close","signal_close",
                  "nifty_first_bar_return_pct")
        try:
            values = [float(row.get(k)) for k in fields]
        except (TypeError, ValueError):
            return False
        if not all(np.isfinite(values)):
            return False
        price, oi, volume, body, wick, confirm_volume, ema9, ema20, ema50, opened, closed, signal_close, nifty = values
        return (ema9 < ema20 < ema50 and closed < opened and closed < signal_close
            and price <= -float(rule.five_minute.active_price_pct)
            and float(rule.five_minute.oi_change_pct) <= oi <= selection_contract.MAX_OI_CHANGE_PCT
            and volume >= float(rule.five_minute.volume_ratio)
            and float(rule.one_minute.body_ratio) <= body <= 1.
            and 0 <= wick <= float(rule.one_minute.max_wick_ratio)
            and confirm_volume >= float(rule.one_minute.confirmation_volume_ratio)
            and nifty <= nifty_threshold)

    def select(self, profile, nifty_threshold):
        specs = setup_specs(profile)
        selected, filter_pass = [], 0
        for day, rows in self.pools.items():
            side = "SHORT"
            core, active = specs[side]
            def key(row, setup):
                return (-selection_contract.picker_value(row, setup.picker),
                        -float(row["traded_value"]), str(row["tradingsymbol"]))
            rule = profile.for_side(side)
            eligible = sorted((row for row in rows if self._passes(
                row, rule, nifty_threshold)),
                key=lambda row:key(row, active))
            core_eligible = sorted((row for row in eligible
                if -float(row["price_change_pct"]) >= rule.five_minute.core_price_pct),
                key=lambda row:key(row, core))
            core_chosen = core_eligible[:core.max_entries]
            core_ids = {row["sid"] for row in core_chosen}
            chosen = core_chosen + [row for row in eligible if row["sid"] not in core_ids][
                :active.max_entries-len(core_chosen)]
            ranks = {row["sid"]:rank for rank,row in enumerate(eligible,1)}
            filter_pass += len(eligible)
            selected += [dict(row, v10_g_f_core=row["sid"] in core_ids,
                v9_filter_pass=True, v9_selected=True,
                v9_rank_in_setup_day=ranks[row["sid"]], v9_decision="SELECTED")
                for row in chosen]
        if not selected:
            return self.audit.iloc[:0].copy(), filter_pass
        return pd.DataFrame(selected).sort_values(
            ["day","hhmm_int","side","tradingsymbol"],kind="stable").reset_index(drop=True), filter_pass


def _selection_key(orders: pd.DataFrame) -> tuple:
    if orders.empty:
        return ()
    return tuple((int(r.sid), bool(r.v10_g_f_core), int(r.v9_rank_in_setup_day))
                 for r in orders.sort_values("sid").itertuples())


def _period_stats(book: pd.DataFrame, days: list[str]) -> dict:
    executed = book.loc[book.day.astype(str).isin(days) & book.portfolio_executed.eq(True)]
    pnl = executed.portfolio_net_profit_rupees.astype(float)
    setup = executed.loc[executed.setup_id.eq(SETUP)]
    spnl = setup.portfolio_net_profit_rupees.astype(float)
    return dict(trades=len(executed), wins=int(pnl.gt(EPS).sum()),
                losses=int(pnl.lt(-EPS).sum()),
                win_rate_pct=float(100*pnl.gt(EPS).mean()) if len(pnl) else np.nan,
                net_pnl_rs=float(pnl.sum()), loss_amount_rs=float(-pnl[pnl.lt(-EPS)].sum()),
                setup_trades=len(setup), setup_wins=int(spnl.gt(EPS).sum()),
                setup_losses=int(spnl.lt(-EPS).sum()),
                setup_win_rate_pct=float(100*spnl.gt(EPS).mean()) if len(spnl) else np.nan,
                setup_net_pnl_rs=float(spnl.sum()),
                setup_loss_amount_rs=float(-spnl[spnl.lt(-EPS)].sum()))


def compare(baseline: pd.DataFrame, variant: pd.DataFrame, days: list[str]) -> dict:
    old = _period_stats(baseline, days)
    new = _period_stats(variant, days)
    result = dict(new)
    for field, value in old.items():
        result["baseline_" + field] = value
    result["net_delta_rs"] = new["net_pnl_rs"] - old["net_pnl_rs"]
    result["setup_net_delta_rs"] = new["setup_net_pnl_rs"] - old["setup_net_pnl_rs"]
    result["setup_trade_delta"] = new["setup_trades"] - old["setup_trades"]
    return result


def development_qualifies(row: pd.Series) -> bool:
    return bool(row.setup_trades >= MIN_DEVELOPMENT_TRADES
                and row.setup_win_rate_pct >= 75. - EPS
                and row.setup_net_pnl_rs > EPS
                and row.net_delta_rs > EPS
                and row.win_rate_pct >= row.baseline_win_rate_pct - EPS)


def later_qualifies(row: pd.Series) -> bool:
    return bool(row.setup_trades >= MIN_LATER_TRADES
                and row.setup_win_rate_pct >= 75. - EPS
                and row.setup_net_pnl_rs > EPS)


def choose_development(table: pd.DataFrame) -> tuple[str, str]:
    dev = table.loc[table.period.eq("development35")].copy()
    eligible = dev.loc[dev.apply(development_qualifies, axis=1)]
    if eligible.empty:
        return "baseline", "NO_CONFIGURATION_MET_DEVELOPMENT_GUARDS"
    chosen = eligible.sort_values(
        ["setup_net_pnl_rs", "setup_trades", "setup_loss_amount_rs", "policy"],
        ascending=[False, False, True, True], kind="stable").iloc[0]
    return str(chosen.policy), "FIRST35_SELECTION_ONLY"


def _json(path: Path, value) -> None:
    path.write_text(json.dumps(base.source._json_ready(value), indent=2,
                               allow_nan=False, default=str) + "\n", encoding="utf-8")


def _render(output: Path, comparison: pd.DataFrame, configs: pd.DataFrame,
            summary: dict, october: pd.DataFrame) -> None:
    policies = list(dict.fromkeys(["baseline", summary["development_selected_policy"],
                                   summary["recommended_policy"],
                                   summary["best_exploratory_small_sample_policy"],
                                   summary["best_development_minimum_sample_policy"]]))
    shown = comparison.loc[comparison.policy.isin(policies)]
    cols = ["period", "policy", "setup_trades", "setup_wins", "setup_losses",
            "setup_win_rate_pct", "setup_net_pnl_rs", "setup_net_delta_rs",
            "trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "net_delta_rs"]
    css = "body{font:15px/1.5 system-ui;margin:28px auto;max-width:1500px;background:#101927;color:#edf4ff}a{color:#8ed6ff}table{border-collapse:collapse;font-size:12px}td,th{padding:7px;border-bottom:1px solid #40526b;white-space:nowrap;text-align:right}th{background:#243449}.scroll{overflow:auto}section{margin:18px 0;padding:18px;background:#19283d;border-radius:8px}"
    doc = f'<!doctype html><html><head><meta charset="utf-8"><title>G4 09:20 SHORT indicators</title><style>{css}</style></head><body><h1>G4 09:20 SHORT indicator sweep</h1>'
    doc += '<section><p>'+html.escape(summary["decision"])+"</p><p>Only the first 35 sessions selected the development candidate. Later sessions and October were then inspected.</p></section>"
    doc += '<section><h2>Before and after</h2><div class="scroll">'+shown[cols].to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>Configuration</h2><div class="scroll">'+configs.loc[configs.policy.isin(policies)].to_html(index=False,border=0,float_format=lambda x:f'{x:,.4f}')+'</div></section>'
    doc += '<section><h2>October trades</h2><div class="scroll">'+october.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>Limits</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary["limitations"])+"</ul></section>"
    doc += '<section><a href="comparison.csv">All results</a> | <a href="configurations.csv">All configurations</a> | <a href="october_trades.csv">Recommended October trades</a> | <a href="exploratory_october_trades.csv">Exploratory October trades</a> | <a href="summary.json">Summary</a></section></body></html>'
    (output/"report.html").write_text(doc, encoding="utf-8")


def run(output: Path, features_path: Path = FEATURES) -> Path:
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/"research").resolve()):
        raise ValueError("Require a fresh isolated G4 research output")
    if base.sha256(DATA/"manifest.json") != DATA_MANIFEST_SHA256:
        raise ValueError("Frozen G4 dataset changed")
    native, _, _, proof = dataset.load(DATA)
    if proof["original_manifest_sha256"] != SOURCE_MANIFEST_SHA256:
        raise ValueError("Original G4 source differs")
    days = list(proof["days"])
    features_path = Path(features_path).resolve()
    features = pd.read_parquet(features_path)
    research_days = days + ([TODAY] if TODAY in set(features.session_date.astype(str)) else [])
    features = features.loc[features.session_date.astype(str).isin(research_days)].copy()
    if set(features.session_date.astype(str)) != set(research_days) or TODAY not in research_days:
        raise ValueError("Feature export must cover the frozen calendar and 2026-10-16")
    prepared = PreparedSelection(features)
    definitions = profile_grid()
    records, selections, union = [], {}, {}
    for item in definitions:
        orders, filter_pass = prepared.select(item["profile"], item["nifty_threshold"])
        orders = orders.loc[orders.side.eq("SHORT")].copy().reset_index(drop=True)
        orders["native_target_pct"] = 1.5
        orders["target_pct"] = 1.5
        key = _selection_key(orders)
        selections.setdefault(key, orders)
        for row in orders.to_dict("records"):
            union.setdefault(int(row["sid"]), row)
        records.append({**{k:v for k,v in item.items() if k != "profile"},
                        **configuration(item), "filter_pass":filter_pass,
                        "selection_key":key})
    if not union:
        raise ValueError("No valid 09:20 SHORT candidate")
    candidate_orders = pd.DataFrame(union.values()).sort_values("sid").reset_index(drop=True)
    paths, path_evidence = paired.new_execution_paths(candidate_orders, RUNTIME_ROOT)
    pool = simulator.simulate_staged(candidate_orders, paths,
        cost_bps=contract.ROUND_TRIP_COST_BPS,
        max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    pool["strategy"] = VERSION
    pool["strategy_version"] = VERSION
    pool["scope"] = "BACKTEST_ONLY"
    pool["execution_authority"] = False
    by_sid = pool.set_index("sid", drop=False)
    frozen_baseline = portfolio.allocate(native, "baseline")
    sealed.parity(frozen_baseline, native)
    retained = native.loc[~native.setup_id.eq(SETUP)].copy()
    books = {}
    for key, orders in selections.items():
        fresh = by_sid.loc[orders.sid.astype(int).tolist()].copy().reset_index(drop=True) if len(orders) else pool.iloc[:0].copy()
        for col in ("v10_g_f_core", "v9_rank_in_setup_day", "v9_decision"):
            if len(fresh):
                fresh[col] = orders[col].to_numpy()
        raw = pd.concat((retained, fresh), ignore_index=True, sort=False)
        books[key] = portfolio.allocate(raw, "indicator_entry")
    baseline_definition = next(r for r in records if r["policy"] == "baseline")
    reference = books[baseline_definition["selection_key"]]
    expected = frozen_baseline.loc[frozen_baseline.setup_id.eq(SETUP) & frozen_baseline.portfolio_executed.eq(True),
                            ["day","tradingsymbol","exit_reason","portfolio_net_profit_rupees"]].sort_values(["day","tradingsymbol"]).reset_index(drop=True)
    actual = reference.loc[reference.day.astype(str).isin(days) & reference.setup_id.eq(SETUP) & reference.portfolio_executed.eq(True),
                          ["day","tradingsymbol","exit_reason","portfolio_net_profit_rupees"]].sort_values(["day","tradingsymbol"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(expected, actual, check_dtype=False, atol=.02, rtol=0)
    periods = {"development35":days[:35], "later17":days[35:],
               "october_to_15":[d for d in days if d >= "2026-10-01"], "all52":days,
               "today_2026_10_16":[TODAY],
               "october_through_16":[d for d in research_days if d >= "2026-10-01"],
               "all53":research_days}
    rows = []
    for period, cohort in periods.items():
        for record in records:
            rows.append(dict(period=period, policy=record["policy"],
                             **compare(reference, books[record["selection_key"]], cohort)))
    comparison = pd.DataFrame(rows)
    development = comparison.loc[comparison.period.eq("development35")].copy()
    small_pool = development.loc[development.setup_trades.ge(3)]
    best_small = small_pool.sort_values(
        ["setup_win_rate_pct","setup_net_pnl_rs","setup_trades","policy"],
        ascending=[False,False,False,True],kind="stable").iloc[0]
    supported_pool = development.loc[development.setup_trades.ge(MIN_DEVELOPMENT_TRADES)]
    best_supported = supported_pool.sort_values(
        ["setup_win_rate_pct","setup_net_pnl_rs","setup_trades","policy"],
        ascending=[False,False,False,True],kind="stable").iloc[0]
    selected, selection_reason = choose_development(comparison)
    selected_record = next(r for r in records if r["policy"] == selected)
    later = comparison.loc[comparison.period.eq("later17") & comparison.policy.eq(selected)].iloc[0]
    test_pass = selected != "baseline" and later_qualifies(later)
    recommended = selected if test_pass else "baseline"
    recommended_record = next(r for r in records if r["policy"] == recommended)
    exploratory_record = next(r for r in records if r["policy"] == str(best_small.policy))
    chosen_book = books[recommended_record["selection_key"]]
    exploratory_book = books[exploratory_record["selection_key"]]
    october_trades = chosen_book.loc[
        chosen_book.day.astype(str).ge("2026-10-01") & chosen_book.setup_id.eq(SETUP)
        & chosen_book.portfolio_executed.eq(True),
        ["sid","day","tradingsymbol","exit_reason","portfolio_net_profit_rupees",
         "price_change_pct","oi_change_pct","volume_ratio","body_ratio","wick_ratio",
         "v9_1m_volume_ratio"]].copy()
    exploratory_october = exploratory_book.loc[
        exploratory_book.day.astype(str).ge("2026-10-01") & exploratory_book.setup_id.eq(SETUP)
        & exploratory_book.portfolio_executed.eq(True),
        ["sid","day","tradingsymbol","exit_reason","portfolio_net_profit_rupees",
         "price_change_pct","oi_change_pct","volume_ratio","body_ratio","wick_ratio",
         "v9_1m_volume_ratio"]].copy()
    config_rows = pd.DataFrame([{k:v for k,v in r.items() if k not in ("values","selection_key")}
                                for r in records])
    summary = dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
        feature_path=str(features_path), feature_sha256=base.sha256(features_path),
        frozen_dataset=str(DATA), frozen_dataset_manifest_sha256=DATA_MANIFEST_SHA256,
        original_g4_manifest_sha256=SOURCE_MANIFEST_SHA256,
        declared_joint_grid_profiles=len(grid_source.search_grid()),
        unique_short_configurations=len(records), unique_selected_trade_sets=len(selections),
        unique_short_indicator_profiles=494, nifty_thresholds=list(NIFTY_THRESHOLDS),
        unique_candidate_paths=len(candidate_orders), development_days=days[:35],
        later_days=days[35:], today_included=True, today=TODAY,
        today_feature_rows=int(features.session_date.astype(str).eq(TODAY).sum()),
        today_complete_decision_evidence_rows=int(features.loc[
            features.session_date.astype(str).eq(TODAY), "complete_decision_evidence_by_0922"].fillna(False).astype(bool).sum()),
        development_selected_policy=selected,
        development_selection_reason=selection_reason,
        later_test_passed=test_pass, recommended_policy=recommended,
        best_exploratory_small_sample_policy=str(best_small.policy),
        best_exploratory_small_sample_configuration=configuration(exploratory_record),
        best_development_minimum_sample_policy=str(best_supported.policy),
        best_development_minimum_sample_configuration=configuration(
            next(r for r in records if r["policy"] == str(best_supported.policy))),
        current_configuration=configuration(baseline_definition),
        development_selected_configuration=configuration(selected_record),
        recommended_configuration=configuration(recommended_record),
        decision=(f"Development selected {selected}, but the later-period validation failed; retain the current G4 configuration."
                  if selected != "baseline" and not test_pass else
                  f"Chronological development and later-period guards selected {recommended}."
                  if recommended != "baseline" else
                  "No indicator configuration met the development safeguards; retain the current G4 configuration."),
        selection_rules=dict(development_minimum_trades=MIN_DEVELOPMENT_TRADES,
            development_minimum_win_rate_pct=75., development_positive_setup_net=True,
            development_positive_portfolio_delta=True,
            development_portfolio_win_rate_not_lower=True,
            later_minimum_trades=MIN_LATER_TRADES, later_minimum_win_rate_pct=75.,
            later_positive_setup_net=True),
        path_evidence=path_evidence,
        limitations=[
            "The indicator grid was inherited from the earlier predeclared 702-profile 09:20 study; duplicate SHORT settings reduce it to 494 profiles, crossed with seven declared NIFTY thresholds.",
            "The first 35 sessions choose a candidate. The later 17 sessions and October do not participate in ranking.",
            "This history has been inspected in earlier research, so the later block is chronological validation rather than a pristine untouched holdout.",
            "Historical opening OI is retrospective and does not establish that the signal was executable from live decision-time data.",
            "The NIFTY first-bar gate, entry mechanics, 1.50% target, 1.20% stop, 120-minute tightening, 5 bps cost and portfolio allocation remain fixed.",
            "The official 16 October G4 partial replay marked 0921_SHORT unavailable at its 14:05 cutoff. Today's reconstructed research row is reported separately and does not rewrite that availability decision.",
            "Today's portfolio columns contain only the researched 09:20 SHORT candidates; the setup columns are the relevant today comparison.",
        ])
    output.mkdir(parents=True)
    (output/"code").mkdir()
    shutil.copyfile(__file__, output/"code"/Path(__file__).name)
    comparison.to_csv(output/"comparison.csv", index=False)
    config_rows.to_csv(output/"configurations.csv", index=False)
    candidate_orders.to_csv(output/"union_candidate_orders.csv", index=False)
    pool.to_csv(output/"union_candidate_trades.csv", index=False)
    october_trades.to_csv(output/"october_trades.csv", index=False)
    exploratory_october.to_csv(output/"exploratory_october_trades.csv", index=False)
    chosen_book.to_csv(output/"recommended_portfolio_ledger.csv", index=False)
    _json(output/"summary.json", summary)
    _render(output, comparison, config_rows, summary, october_trades)
    _json(output/"manifest.json", dict(version=VERSION, files={
        p.relative_to(output).as_posix():base.sha256(p)
        for p in output.rglob("*") if p.is_file()}))
    print(output, flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--features", type=Path, default=FEATURES)
    args = parser.parse_args()
    run(args.output_dir, args.features)
