"""Chronological approval review for the 49 original G4 setups.

The fixed one-minute policy catalogue is selected on the first 35 sessions.
Later sessions, October, and the latest complete session are diagnostics only.
This module writes isolated research artifacts and never edits a G4 profile.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path
import secrets

import numpy as np
import pandas as pd

from research import g4_complete_inventory_backtest as complete
from research import g4_g3_mirror_backtest as base


VERSION = "G4-49-SETUP-CHRONOLOGICAL-APPROVAL-REVIEW-V1"
G4_ROOT = base.G4_ROOT
ITERATIVE = G4_ROOT / "research/20261016T162554_29716e_iterative_protection_v2"
ITERATIVE_MANIFEST_SHA256 = "23cd93512ac1a1b6276704bb43cfa6269241b50f35c72b14739b7e365e11eb0d"
DATASET = G4_ROOT / "research/20261016_iterative_sealed_dataset_v1"
DATASET_MANIFEST_SHA256 = "4dbc07968e5c5a0ae313bdd83670ca12b388f3f4f223b35e613952aa25cd80b2"
ENTRY_POOL = base.ROOT / ".codex_tmp/g4_vwap_rsi_prepared_pool"
WIN_RATE_GOAL = 75.0
WINNER_RETENTION_MINIMUM = 90.0
MIN_DEVELOPMENT_TRADES = 5
MIN_LATER_TRADES = 3
EPS = 1e-8

SETUPS = tuple("""0921_LONG 0926_LONG 0926_SHORT 0931_LONG 0931_SHORT
0936_LONG 0936_SHORT 0941_LONG 0941_SHORT 0946_LONG 0946_SHORT 0951_LONG
0951_SHORT 0956_LONG 1001_LONG 1006_LONG 1011_SHORT 1016_LONG 1016_SHORT
1026_SHORT 1031_SHORT 1036_LONG 1036_SHORT 1041_LONG 1046_LONG 1046_SHORT
1051_LONG 1056_SHORT 1111_LONG 1111_SHORT 1121_SHORT 1126_SHORT 1136_LONG
1136_SHORT 1141_LONG 1141_SHORT 1146_SHORT 1206_LONG 1211_SHORT 1216_LONG
1216_SHORT 1226_SHORT 1231_LONG 1231_SHORT 1236_SHORT 1301_SHORT 1321_SHORT
1326_SHORT 1331_LONG""".split())


def _metrics(baseline: pd.Series, candidate: pd.Series) -> dict:
    baseline = baseline.astype(float)
    candidate = candidate.astype(float)
    winners = baseline.gt(EPS)
    profit = float(baseline.loc[winners].sum())
    loss_before = float(-baseline.loc[baseline.lt(-EPS)].sum())
    loss_after = float(-candidate.loc[candidate.lt(-EPS)].sum())
    return dict(
        trades=len(baseline), wins=int(candidate.gt(EPS).sum()),
        losses=int(candidate.lt(-EPS).sum()),
        win_rate_pct=float(100 * candidate.gt(EPS).sum() / len(candidate)) if len(candidate) else 0.0,
        net_pnl_rs=float(candidate.sum()), baseline_net_pnl_rs=float(baseline.sum()),
        net_delta_rs=float((candidate - baseline).sum()),
        loss_amount_rs=loss_after, baseline_loss_amount_rs=loss_before,
        loss_saved_rs=loss_before - loss_after,
        original_winner_profit_retention_pct=(float(100 * candidate.loc[winners].sum() / profit)
                                              if profit > EPS else np.nan),
        original_winners_turned_losses=int((winners & candidate.lt(-EPS)).sum()),
        original_losers_turned_winners=int((baseline.lt(-EPS) & candidate.gt(EPS)).sum()),
        changed_trades=int((candidate - baseline).abs().gt(EPS).sum()),
    )


def _development_eligible(metrics: dict, baseline_losses: int) -> bool:
    retention = metrics["original_winner_profit_retention_pct"]
    return (metrics["win_rate_pct"] >= WIN_RATE_GOAL - EPS
            and metrics["net_delta_rs"] >= -EPS
            and metrics["loss_saved_rs"] > EPS
            and (np.isnan(retention) or retention >= WINNER_RETENTION_MINIMUM - EPS)
            and metrics["original_winners_turned_losses"] == 0
            and metrics["losses"] < baseline_losses
            and metrics["changed_trades"] >= 2)


def _later_pass(metrics: dict) -> bool:
    retention = metrics["original_winner_profit_retention_pct"]
    return (metrics["trades"] >= MIN_LATER_TRADES
            and metrics["win_rate_pct"] >= WIN_RATE_GOAL - EPS
            and metrics["net_delta_rs"] >= -EPS
            and metrics["loss_saved_rs"] > EPS
            and (np.isnan(retention) or retention >= WINNER_RETENTION_MINIMUM - EPS)
            and metrics["original_winners_turned_losses"] == 0)


def choose_policy(matrix: pd.DataFrame, meta: pd.DataFrame, setup_id: str,
                  development_days, later_days) -> dict:
    dev_ids = meta.index[meta.setup_id.eq(setup_id) & meta.day.isin(development_days)]
    later_ids = meta.index[meta.setup_id.eq(setup_id) & meta.day.isin(later_days)]
    before = matrix.loc[dev_ids, "baseline"]
    baseline_losses = int(before.lt(-EPS).sum())
    eligible = []
    if (len(before) >= MIN_DEVELOPMENT_TRADES and before.gt(EPS).sum() >= 2
            and baseline_losses >= 1):
        for policy in matrix.columns:
            if policy == "baseline":
                continue
            metrics = _metrics(before, matrix.loc[dev_ids, policy])
            if _development_eligible(metrics, baseline_losses):
                eligible.append((policy, metrics))
    eligible.sort(key=lambda item: (-item[1]["loss_saved_rs"],
                                    -item[1]["net_delta_rs"], item[0]))
    exploratory = eligible[0][0] if eligible else "baseline"
    dev_metrics = _metrics(before, matrix.loc[dev_ids, exploratory])
    later_before = matrix.loc[later_ids, "baseline"]
    later_metrics = _metrics(later_before, matrix.loc[later_ids, exploratory])
    passed = exploratory != "baseline" and _later_pass(later_metrics)
    if passed:
        recommendation, reason = exploratory, "DEVELOPMENT_AND_LATER_GUARDS_PASSED"
    elif exploratory != "baseline":
        recommendation, reason = "baseline", "DEVELOPMENT_CANDIDATE_FAILED_LATER_GUARDS"
    elif len(before) < MIN_DEVELOPMENT_TRADES:
        recommendation, reason = "baseline", "INSUFFICIENT_DEVELOPMENT_TRADES"
    else:
        recommendation, reason = "baseline", "NO_DEVELOPMENT_POLICY_PASSED_GUARDS"
    return dict(setup_id=setup_id, eligible_development_policies=len(eligible),
        exploratory_policy=exploratory, recommended_policy=recommendation,
        later_test_passed=passed, decision_reason=reason,
        **{"development_" + key: value for key, value in dev_metrics.items()},
        **{"later_" + key: value for key, value in later_metrics.items()})


def _baseline_metrics(book: pd.DataFrame, days) -> dict:
    rows = book.loc[book.day.isin(days) & book.portfolio_executed.astype(bool)]
    pnl = rows.portfolio_net_profit_rupees.astype(float)
    return dict(trades=len(rows), wins=int(pnl.gt(EPS).sum()), losses=int(pnl.lt(-EPS).sum()),
        win_rate_pct=float(100 * pnl.gt(EPS).sum() / len(rows)) if len(rows) else 0.0,
        net_pnl_rs=float(pnl.sum()), loss_amount_rs=float(-pnl.loc[pnl.lt(-EPS)].sum()))


def _verify_entry_pool() -> pd.DataFrame:
    manifest = base.read(ENTRY_POOL / "manifest.json")
    expected = manifest.get("files", {})
    for name in ("pool.pkl", "proof.json", "price_proof.json"):
        if expected.get(name) != base.sha256(ENTRY_POOL / name):
            raise ValueError("Frozen entry-pool member changed: " + name)
    pool = pd.read_pickle(ENTRY_POOL / "pool.pkl")
    if set(pool.setup_id) != set(SETUPS) or pool.duplicated("sid").any():
        raise ValueError("Frozen entry pool does not contain the requested 49 setups")
    return pool


def _indicator_diagnostics(orders: pd.DataFrame, book: pd.DataFrame, days) -> pd.DataFrame:
    fields = ["margin_price_change_pct", "margin_oi_change_pct", "margin_volume_ratio",
              "margin_body_ratio", "margin_wick_ratio", "margin_confirmation_volume_ratio"]
    available = [field for field in fields if field in orders]
    joined = orders.merge(book[["sid", "portfolio_executed", "portfolio_net_profit_rupees"]],
                          on="sid", how="left", validate="one_to_one")
    joined = joined.loc[joined.day.isin(days) & joined.portfolio_executed.fillna(False)].copy()
    rows = []
    for setup_id in SETUPS:
        setup = joined.loc[joined.setup_id.eq(setup_id)]
        for field in available:
            values = pd.to_numeric(setup[field], errors="coerce")
            winner = values.loc[setup.portfolio_net_profit_rupees.gt(EPS)]
            loser = values.loc[setup.portfolio_net_profit_rupees.lt(-EPS)]
            rows.append(dict(setup_id=setup_id, feature=field, trades=len(setup),
                finite_values=int(values.notna().sum()), winners=len(winner), losses=len(loser),
                winner_median=float(winner.median()) if winner.notna().any() else np.nan,
                loser_median=float(loser.median()) if loser.notna().any() else np.nan,
                descriptive_only=True, promoted_threshold_change=False))
    return pd.DataFrame(rows)


def _json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def run(output: Path) -> Path:
    code_sha = base.sha256(Path(__file__))
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((G4_ROOT / "research").resolve()):
        raise ValueError("Fresh isolated G4 research directory required")
    if set(SETUPS) != set(complete.Identity(complete.configuration())._ACTIVE_BY_ID) - complete.ADDED_IDS:
        raise ValueError("Requested setup list no longer matches the original G4 inventory")
    if base.sha256(ITERATIVE / "manifest.json") != ITERATIVE_MANIFEST_SHA256:
        raise ValueError("Fixed exit-policy evidence changed")
    if base.sha256(DATASET / "manifest.json") != DATASET_MANIFEST_SHA256:
        raise ValueError("Sealed one-minute dataset changed")
    pointer_path = G4_ROOT / "latest/bounded_exit.json"
    pointer_sha = base.sha256(pointer_path)
    pointer = base.read(pointer_path)
    source = (G4_ROOT / pointer["run_dir"]).resolve()
    if (pointer.get("variant_id") != complete.VERSION or pointer.get("session_date") != "2026-10-16"
            or base.sha256(source / "manifest.json") != pointer["manifest_sha256"]):
        raise ValueError("Expected the complete 16 October G4 V13 baseline")
    official = base.read_frozen_csv(source / "g4_bounded_exit/portfolio_trades.csv")
    orders = base.read_frozen_csv(source / "selected_orders.csv")
    summary = base.read(source / "summary.json")
    days = summary["days"]
    if len(days) != 53 or days[-1] != "2026-10-16":
        raise ValueError("Unexpected official calendar")
    matrix = base.read_frozen_csv(ITERATIVE / "candidate_net_returns.csv").set_index("sid")
    native = base.read_frozen_csv(DATASET / "native.csv")
    meta = native.loc[native.portfolio_executed.astype(bool)].set_index("sid", drop=False)
    meta["day"] = meta.day.astype(str).str[:10]
    matrix = matrix.loc[meta.index]
    old_days = base.read(DATASET / "summary.json")["days"]
    development_days, later_days = old_days[:35], old_days[35:]
    policies = {item["id"]: item for item in base.read(ITERATIVE / "config.json")["policy_catalog"]}
    pool = _verify_entry_pool()
    pool_counts = pool.groupby("setup_id").size().to_dict()

    decisions = pd.DataFrame([choose_policy(matrix, meta, setup_id, development_days, later_days)
                              for setup_id in SETUPS])
    if decisions.recommended_policy.ne("baseline").any():
        raise ValueError("A setup unexpectedly passed; review the promotion path before requesting approval")
    specs = {row["setup_id"]: row for row in complete.configuration()["active_setups"]}
    confirmation = complete.configuration()["minimum_confirmation_1m_volume_ratio_by_setup"]
    periods = {
        "development35": days[:35], "later18": days[35:],
        "october_through_16": [day for day in days if day >= "2026-10-01"],
        "today_2026_10_16": ["2026-10-16"], "all53": days,
    }
    period_rows = []
    for setup_id in SETUPS:
        setup_book = official.loc[official.setup_id.eq(setup_id)]
        for period, cohort in periods.items():
            period_rows.append(dict(setup_id=setup_id, period=period,
                                    **_baseline_metrics(setup_book, cohort)))
    period_results = pd.DataFrame(period_rows)
    all_metrics = period_results.loc[period_results.period.eq("all53")].set_index("setup_id")
    october = period_results.loc[period_results.period.eq("october_through_16")].set_index("setup_id")
    today = period_results.loc[period_results.period.eq("today_2026_10_16")].set_index("setup_id")

    config_rows = []
    for setup_id in SETUPS:
        spec = dict(specs[setup_id])
        spec["minimum_confirmation_1m_volume_ratio"] = confirmation.get(setup_id)
        config_rows.append(dict(setup_id=setup_id, configuration_changed=False,
            before=json.dumps(spec, sort_keys=True), approved=json.dumps(spec, sort_keys=True),
            recommended_post_entry_policy="baseline"))
    configuration = pd.DataFrame(config_rows)
    indicators = _indicator_diagnostics(orders, official, development_days)

    review = decisions.copy().set_index("setup_id")
    review["candidate_pool_rows"] = pd.Series(pool_counts)
    for prefix, table in (("all", all_metrics), ("october", october), ("today", today)):
        for field in ("trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "loss_amount_rs"):
            review[prefix + "_" + field] = table[field]
    review["current_target_pct"] = pd.Series({key: value["target_pct"] for key, value in specs.items()})
    review["current_stop_pct"] = pd.Series({key: value["stop_pct"] for key, value in specs.items()})
    review["exploratory_policy_parameters"] = [json.dumps(policies[name], sort_keys=True)
        for name in review.exploratory_policy]
    review["recommended_configuration_change"] = False
    review = review.reset_index()
    exploratory = review.loc[review.exploratory_policy.ne("baseline")].copy()

    output.mkdir(parents=True)
    review.to_csv(output / "setup_decisions.csv", index=False)
    period_results.to_csv(output / "period_results.csv", index=False)
    configuration.to_csv(output / "configuration_changes.csv", index=False)
    indicators.to_csv(output / "indicator_diagnostics.csv", index=False)
    exploratory.to_csv(output / "development_candidates_rejected_by_later_test.csv", index=False)
    config = dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        requested_setups=list(SETUPS), development_sessions=35, later_diagnostic_sessions=17,
        official_sessions=53, win_rate_goal_pct=WIN_RATE_GOAL,
        minimum_original_winner_profit_retention_pct=WINNER_RETENTION_MINIMUM,
        minimum_development_trades=MIN_DEVELOPMENT_TRADES,
        minimum_later_trades=MIN_LATER_TRADES, fixed_exit_policy_count=len(matrix.columns),
        selection="First35 only: >=75% win rate, net not below baseline, lower loss money and loss count, >=90% original-winner profit retained, zero original winners turned losses, >=2 changed trades. Later test repeats the economic guards and requires >=3 trades.",
        indicator_analysis="Price, OI, volume, body, wick and confirmation-volume margins are reported descriptively. No threshold is promoted without a replacement-aware chronological replay.")
    _json(output / "config.json", config)
    result = dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now().astimezone().isoformat(), source_run=str(source),
        source_manifest_sha256=pointer["manifest_sha256"], first_date=days[0], last_date=days[-1],
        official_sessions=len(days), reviewed_setups=len(SETUPS), fixed_exit_policies=len(matrix.columns),
        fixed_exit_alternatives=len(matrix.columns) - 1,
        total_setup_policy_evaluations=len(SETUPS) * len(matrix.columns),
        recommended_configuration_changes=int(review.recommended_configuration_change.sum()),
        development_candidates=len(exploratory), later_validated_candidates=int(decisions.later_test_passed.sum()),
        decision="Retain the current configuration for all 49 requested setups; no exit or indicator change passed the chronological safeguards.",
        source_proof=dict(iterative_manifest_sha256=ITERATIVE_MANIFEST_SHA256,
            dataset_manifest_sha256=DATASET_MANIFEST_SHA256,
            current_pointer_sha256=pointer_sha, entry_pool_manifest_sha256=base.sha256(ENTRY_POOL / "manifest.json")),
        code_sha256=code_sha,
        limitations=[
            "The first 35 sessions select candidates; later sessions, October and 16 October do not participate in ranking.",
            "All historical blocks have been inspected in earlier G4 research and are chronological diagnostics rather than pristine future holdouts.",
            "Individual setup samples remain small: no setup has the 30 development trades required by the stricter production selector.",
            "The 1,140 post-entry policies use causal completed-minute decisions and next-open or resting-stop execution with the existing 5bps cost model.",
            "Entry indicator margins are descriptive. A threshold change needs replacement-aware replay because filtering before quota can select a different stock.",
            "No G4 configuration, result pointer, paper profile or live profile is changed by this review.",
        ])
    _json(output / "summary.json", result)

    show = ["setup_id", "development_trades", "later_trades", "all_trades", "all_wins",
            "all_losses", "all_win_rate_pct", "all_net_pnl_rs", "october_trades",
            "october_win_rate_pct", "october_net_pnl_rs", "today_trades", "today_net_pnl_rs",
            "exploratory_policy", "later_test_passed", "recommended_policy", "decision_reason"]
    css = "body{background:#101927;color:#edf4ff;font:14px/1.5 system-ui;padding:24px}table{border-collapse:collapse;width:100%;font-size:12px}td,th{padding:7px;border-bottom:1px solid #476078;text-align:left}th{background:#24364a;position:sticky;top:0}a{color:#8ed6ff}.scroll{overflow:auto;max-height:78vh}"
    report = f'<!doctype html><html><head><meta charset="utf-8"><title>G4 49-setup approval review</title><style>{css}</style></head><body>'
    report += '<h1>G4 49-setup chronological approval review</h1>'
    report += f'<p>{html.escape(result["decision"])} {len(matrix.columns):,} fixed post-entry policies were evaluated per setup. Six development candidates appeared; none passed the later safeguards.</p>'
    report += '<p>Configuration deltas are zero. The 75% goal is a screen, not a promise. October and 16 October are reported separately and were not used to choose a policy.</p>'
    report += '<div class="scroll">' + review[show].to_html(index=False, border=0, float_format=lambda value: f"{value:,.2f}") + '</div>'
    report += '<h2>Development candidates rejected by later testing</h2>' + exploratory[["setup_id", "exploratory_policy", "development_trades", "development_win_rate_pct", "development_net_delta_rs", "development_loss_saved_rs", "later_trades", "later_win_rate_pct", "later_net_delta_rs", "later_loss_saved_rs", "decision_reason"]].to_html(index=False, border=0, float_format=lambda value: f"{value:,.2f}")
    report += '<h2>Method and limitations</h2><ul>' + ''.join('<li>' + html.escape(text) + '</li>' for text in result["limitations"]) + '</ul>'
    report += '<p><a href="setup_decisions.csv">Setup decisions</a> | <a href="period_results.csv">All periods</a> | <a href="configuration_changes.csv">Configuration before/after</a> | <a href="indicator_diagnostics.csv">Indicator diagnostics</a> | <a href="development_candidates_rejected_by_later_test.csv">Rejected candidates</a></p></body></html>'
    (output / "report.html").write_text(report, encoding="utf-8")
    if base.sha256(pointer_path) != pointer_sha:
        raise ValueError("G4 pointer changed during the review")
    if base.sha256(Path(__file__)) != code_sha:
        raise ValueError("Review code changed during the run")
    files = {path.relative_to(output).as_posix(): base.sha256(path)
             for path in output.rglob("*") if path.is_file()}
    _json(output / "manifest.json", dict(version=VERSION, execution_authority=False,
        code_sha256={str(Path(__file__).resolve()): code_sha}, files=files))
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    destination = args.output_dir or (G4_ROOT / "research" /
        f"{datetime.now():%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_all49_approval_review")
    print(run(destination))
