"""Independent accounting, concentration and paired block-bootstrap finalist audit."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "outputs/v13_sl_innovation_20261004/progress_analysis.json"
OUT = SOURCE.with_name("finalist_audit.json")
FINALIST = "TRAIL_A1.25_KEEP0.25_STAGED"
REFERENCES = ("TIGHTEN_1.25_TO_1.00_AFTER_120M", "STATIC_1.00")
KEYS = ["segment", "day", "sid", "setup_id", "tradingsymbol", "side"]
SEED = 20261004
REPLICATES = 30000
BLOCK_LENGTH = 5


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, np.generic):
        return clean(value.item())
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def trades(result):
    df = pd.DataFrame(result["trades_detail"])
    df["day"] = df.day.str.slice(0, 10)
    assert len(df) == 85 and not df.duplicated(KEYS).any()
    assert abs(df.portfolio_net_profit_rupees.sum() - result["net_profit_rupees"]) < 1e-6
    assert int(df.portfolio_net_profit_rupees.gt(1e-8).sum()) == result["wins"]
    byday = df.groupby("day").portfolio_net_profit_rupees.sum()
    for day, net in result["daily_net"].items():
        assert abs(float(byday.get(day, 0)) - net) < 1e-6
    return df


def diagnostics(result, df):
    pnl = df.portfolio_net_profit_rupees.to_numpy(float)
    ambiguous = df.same_bar_ambiguous.fillna(False).astype(bool)
    gaps = df.exit_gap_through.fillna(False).astype(bool)
    fields = KEYS + ["entry_ts", "entry_price", "exit_execution_ts", "exit_bar_end_ts", "exit_event", "exit_reason",
                     "exit_price", "active_stop_pct_at_exit", "portfolio_net_profit_rupees"]
    return dict(trades=len(df), wins=int((pnl > 1e-8).sum()), losses=int((pnl < -1e-8).sum()),
                net_profit_rupees=float(pnl.sum()), same_bar_ambiguous_count=int(ambiguous.sum()),
                exit_gap_through_count=int(gaps.sum()),
                ambiguous_trades=df.loc[ambiguous, fields].to_dict("records"),
                gap_trades=df.loc[gaps, fields].to_dict("records"),
                exit_counts=df.exit_reason.value_counts().to_dict(),
                cost_stress=result["cost_stress"], slices=result["slices"],
                minute_close_drawdown_rupees=result["minute_close_drawdown_rupees"],
                daily_close_drawdown_rupees=result["daily_close_drawdown_rupees"],
                average_loss_magnitude_rupees=result["average_loss_magnitude_rupees"],
                worst_loss_rupees=result["worst_loss_rupees"], positive_sessions=result["positive_sessions"])


def comparison(candidate, reference, cand_trades, ref_trades, indices, days):
    fields = KEYS + ["entry_ts", "entry_price", "exit_execution_ts", "exit_reason", "exit_price", "portfolio_net_profit_rupees"]
    paired = cand_trades[fields].merge(ref_trades[fields], on=KEYS, suffixes=("_candidate", "_reference"), validate="one_to_one")
    assert len(paired) == 85
    assert np.allclose(paired.entry_price_candidate, paired.entry_price_reference, rtol=0, atol=1e-9)
    assert paired.entry_ts_candidate.equals(paired.entry_ts_reference)
    paired["delta_rupees"] = paired.portfolio_net_profit_rupees_candidate - paired.portfolio_net_profit_rupees_reference
    differences = np.array([candidate["daily_net"][d] - reference["daily_net"][d] for d in days])
    assert abs(differences.sum() - paired.delta_rupees.sum()) < 1e-6
    changed = paired.loc[paired.delta_rupees.abs().gt(1e-7)].sort_values("delta_rupees", ascending=False)
    improved, worsened = changed[changed.delta_rupees > 0], changed[changed.delta_rupees < 0]
    positive_delta = float(improved.delta_rupees.sum())
    negative_delta = float(worsened.delta_rupees.sum())
    total = float(differences.sum())
    best_delta = float(improved.delta_rupees.max()) if len(improved) else 0
    best_day = float(differences.max())
    simulations = differences[indices].sum(axis=1)
    quantiles = np.quantile(simulations, [.025, .5, .975])
    by_month = changed.assign(month=changed.day.str.slice(0, 7)).groupby("month").delta_rupees.sum().to_dict()
    stress = []
    for c, r in zip(candidate["cost_stress"], reference["cost_stress"]):
        assert c["total_cost_bps"] == r["total_cost_bps"]
        stress.append(dict(total_cost_bps=c["total_cost_bps"], candidate=c, reference=r,
                           net_delta_rupees=c["net"] - r["net"], wins_delta=c["wins"] - r["wins"]))
    # A separate relative-execution sensitivity: charge only the new variant's
    # P&L-changed exits extra slippage. This is not an execution-cost forecast.
    exposure = 500000.0
    relative_exit_stress = [dict(extra_adverse_bps_per_changed_exit=bps, changed_exit_count=len(changed),
        assumed_exposure_per_trade_rupees=exposure, additional_cost_rupees=bps / 10000 * exposure * len(changed),
        revised_net_delta_rupees=total - bps / 10000 * exposure * len(changed)) for bps in (1, 2, 5)]
    chrono = {}
    for name, current in candidate["slices"].items():
        prev = reference["slices"][name]
        chrono[name] = dict(candidate=current, reference=prev, net_delta_rupees=current["net"] - prev["net"],
                            wins_delta=current["wins"] - prev["wins"])
    return dict(reference=reference["rule"]["name"], same_85_entries_verified=True,
        net_delta_rupees=total, wins_delta=candidate["wins"] - reference["wins"],
        positive_day_delta=candidate["positive_sessions"] - reference["positive_sessions"],
        average_loss_delta_rupees=candidate["average_loss_magnitude_rupees"] - reference["average_loss_magnitude_rupees"],
        worst_loss_delta_rupees=candidate["worst_loss_rupees"] - reference["worst_loss_rupees"],
        minute_close_drawdown_delta_rupees=candidate["minute_close_drawdown_rupees"] - reference["minute_close_drawdown_rupees"],
        daily_close_drawdown_delta_rupees=candidate["daily_close_drawdown_rupees"] - reference["daily_close_drawdown_rupees"],
        improved_trades=len(improved), worsened_trades=len(worsened), unchanged_trades=85-len(changed),
        rescued_winners=int(((paired.portfolio_net_profit_rupees_reference < 0) & (paired.portfolio_net_profit_rupees_candidate > 0)).sum()),
        lost_winners=int(((paired.portfolio_net_profit_rupees_reference > 0) & (paired.portfolio_net_profit_rupees_candidate < 0)).sum()),
        gross_positive_changes_rupees=positive_delta, gross_negative_changes_rupees=negative_delta,
        largest_positive_trade_delta_rupees=best_delta,
        largest_positive_trade_share_of_gross_improvements_pct=100 * best_delta / positive_delta if positive_delta else None,
        net_delta_without_best_improved_trade_rupees=total-best_delta,
        net_delta_without_best_day_rupees=total-best_day,
        all_leave_one_day_out_delta_range_rupees=[float((total-differences).min()), float((total-differences).max())],
        changed_trades=changed.to_dict("records"),
        daily_deltas=[dict(day=d, candidate_net=candidate["daily_net"][d], reference_net=reference["daily_net"][d],
                           delta_rupees=float(v)) for d, v in zip(days, differences)],
        changed_days=int((np.abs(differences) > 1e-7).sum()), month_deltas_rupees=by_month,
        chronological_diagnostics=chrono, cost_stress=stress,
        changed_exit_relative_slippage_sensitivity=dict(
            note="Illustrative relative additional cost charged only to this candidate's P&L-changed exits versus the reference, at fixedRs500000exposure each. Not a forecast, and not an assertion that reference exits have zero slippage.",
            rows=relative_exit_stress,
            extra_bps_per_changed_exit_erasing_net_advantage=total / (exposure * len(changed)) * 10000 if len(changed) else None),
        paired_circular_5session_block_bootstrap=dict(replicates=REPLICATES, seed=SEED, sessions=len(days),
            block_length=BLOCK_LENGTH, observed_net_delta_rupees=total,
            percentile_95_interval_rupees=[float(quantiles[0]), float(quantiles[2])], median_delta_rupees=float(quantiles[1]),
            fraction_resamples_positive=float((simulations > 1e-8).mean()),
            fraction_resamples_nonpositive=float((simulations <= 1e-8).mean()),
            note="Paired circular moving blocks of five recorded sessions; nine blocks sampled then truncated to43. Conditional on the already-selected candidate. Does not adjust for the68-rule search, earlier sweeps, or repeated inspection; not future-profit probability."))


def main():
    source_bytes = SOURCE.read_bytes()
    data = json.loads(source_bytes)
    finalist = next(row for row in data["results"] if row["rule"]["name"] == FINALIST)
    controls = {row["rule"]["name"]: row for row in data["controls"]}
    finalist_trades = trades(finalist)
    ref_trades = {name: trades(controls[name]) for name in REFERENCES}
    days = sorted(finalist["daily_net"])
    assert len(days) == 43
    for name in REFERENCES:
        assert sorted(controls[name]["daily_net"]) == days
    rng = np.random.default_rng(SEED)
    starts = rng.integers(0, len(days), size=(REPLICATES, int(np.ceil(len(days) / BLOCK_LENGTH))))
    indices = ((starts[..., None] + np.arange(BLOCK_LENGTH)) % len(days)).reshape(REPLICATES, -1)[:, :len(days)]
    comparisons = [comparison(finalist, controls[name], finalist_trades, ref_trades[name], indices, days) for name in REFERENCES]
    result = dict(status="INDEPENDENT_ACCOUNTING_AUDIT_REUSED_HISTORY_NOT_OUT_OF_SAMPLE",
        source=str(SOURCE), source_sha256=hashlib.sha256(source_bytes).hexdigest(), source_mtime_ns=SOURCE.stat().st_mtime_ns,
        rule=finalist["rule"], window=[days[0], days[-1]], sessions=43, candidate=diagnostics(finalist, finalist_trades),
        controls={name: diagnostics(controls[name], ref_trades[name]) for name in REFERENCES}, comparisons=comparisons,
        limitations=["No market simulation is rerun here; this independently reconciles existing trade and daily ledgers and performs paired accounting/resampling.",
          "Minute candles cannot reveal exact intrabar chronology. Recorded stop-target ties and gap flags are reported; zero flags do not prove tick-exact execution.",
          "Five-session blocks preserve some short-range dependence but the sample is only43sessions. Confidence intervals condition on a winner selected after68new rules plus prior sweeps.",
          "Incremental cost stress applies the same assumed exposure and trade count to both strategies; it cannot establish equal real slippage across different exits.",
          "October1 remains excluded; chronology slices and this bootstrap reuse already inspected history."],
        checks=["85unique trades, sums, wins and daily reconciliation for candidate and both controls", "identical85entry timestamps and prices", "daily and trade delta totals match", "paired calendar matches43sessions"])
    OUT.write_text(json.dumps(clean(result), indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUT), "candidate": result["candidate"],
                      "comparisons": [{k: v for k, v in r.items() if k not in ("changed_trades", "daily_deltas", "cost_stress", "chronological_diagnostics")} for r in comparisons]}, indent=2))


if __name__ == "__main__":
    main()
