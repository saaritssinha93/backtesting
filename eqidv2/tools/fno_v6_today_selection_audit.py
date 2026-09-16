"""Deep, read-only audit of corrected V6 selection on 2026-09-03.

The utility reuses the immutable corrected-V6 signal caches and native exit
engine.  It does not alter any strategy source or published V6 artifact.  Its
purpose is to separate today's unavoidable eligible pool from picker mistakes,
then test causal selection-only variants across the existing train/test/later
segments rather than optimizing solely on the target day.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

import fno_oi_ema_confirm_sweep as simulator
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as corrected


DAY = date(2026, 9, 3)
TRAIN_END = date(2026, 8, 13)
TEST_END = date(2026, 9, 1)
COST_BPS = 5.0
ROOT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research")
V6_ROOT = ROOT / "v6_corrected"
OUTPUT_ROOT = ROOT / "v6_today_selection_audit_2026-09-03"


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    target_setup: str | None = None
    predicate: Callable[[pd.DataFrame], pd.Series] | None = None
    picker_column: str | None = None
    picker_ascending: bool = False
    cap_override: int | None = None
    remove_setup: bool = False
    nifty_alignment_min: float | None = None
    nifty_target_setups: tuple[str, ...] | None = None
    signal_time_cap_hhmm: int | None = None
    signal_time_cap: int | None = None
    signal_time_rank_column: str | None = None
    signal_time_rank_ascending: bool = False


def _latest_cache(month: str) -> Path:
    candidates = list((V6_ROOT / "_cache").glob(f"{month}_*.parquet"))
    if not candidates:
        raise RuntimeError(f"Missing V6 cache for {month}")
    return max(candidates, key=lambda path: path.stat().st_mtime_ns).with_suffix("")


def _load_signals_and_paths() -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]:
    parts = []
    for month in ("26AUG", "26SEP"):
        signals, paths = corrected._load_cached(_latest_cache(month))
        signals = signals.copy()
        signals["contract_month"] = month
        parts.append((signals, paths))
    signals, paths = corrected.concat_regimes(parts)
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    return signals, paths


def _nifty_context() -> pd.DataFrame:
    raw = Path(r"C:\TradingData\eqidv2\fno_oi\raw_contracts_5m")
    frames = []
    for month in ("26AUG", "26SEP"):
        path = raw / f"NIFTY{month}FUT_5minute.parquet"
        frame = pd.read_parquet(path, columns=["timestamp", "open", "close"])
        frame["timestamp"] = pd.to_datetime(frame["timestamp"])
        frame["day"] = frame["timestamp"].dt.date
        frame["hhmm_int"] = frame["timestamp"].dt.strftime("%H%M").astype(int)
        first_open = frame.groupby("day")["open"].transform("first")
        frame["nifty_return_from_open_pct"] = (frame["close"] / first_open - 1.0) * 100.0
        frame["contract_month"] = month
        frames.append(frame[["contract_month", "day", "hhmm_int", "nifty_return_from_open_pct"]])
    return pd.concat(frames, ignore_index=True).drop_duplicates(
        ["contract_month", "day", "hhmm_int"], keep="last"
    )


def _metric(values: np.ndarray) -> dict[str, float | int]:
    values = values[np.isfinite(values)]
    profit = float(values[values > 0].sum())
    loss = float(-values[values < 0].sum())
    return {
        "fills": int(values.size),
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "pf": profit / loss if loss else (float("inf") if profit else np.nan),
        "net_pct": float(values.sum()),
        "win_rate": float((values > 0).mean()) if values.size else np.nan,
    }


def _period_mask(audit: pd.DataFrame, period: str) -> pd.Series:
    if period == "TRAIN":
        return audit["day"].le(TRAIN_END)
    if period == "TEST":
        return audit["day"].gt(TRAIN_END) & audit["day"].le(TEST_END)
    if period == "SEP2_CHECK":
        return audit["day"].gt(TEST_END)
    if period == "TODAY":
        return audit["day"].eq(DAY)
    return pd.Series(True, index=audit.index)


def _rank(rows: pd.DataFrame, setup, rule: Rule) -> pd.DataFrame:
    picker_columns = {
        "max_oi": "oi_change_pct",
        "max_volume": "volume_ratio",
        "max_move": "abs_price_change_pct",
        "max_body": "body_ratio",
        "max_liquidity": "traded_value",
    }
    picker_column = rule.picker_column or picker_columns[setup.picker]
    if picker_column == "abs_price_change_pct":
        rows[picker_column] = rows["price_change_pct"].abs()
    if picker_column == "body_minus_wick":
        rows[picker_column] = rows["body_ratio"] - rows["wick_ratio"]
    ascending = rule.picker_ascending if rule.picker_column else False
    return rows.sort_values(
        ["day", picker_column, "traded_value", "tradingsymbol"],
        ascending=[True, ascending, False, True],
        kind="stable",
    )


def run_rule(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    rule: Rule,
) -> pd.DataFrame:
    pieces = []
    for setup in corrected.ACTIVE_SETUPS:
        if rule.remove_setup and setup.setup_id == rule.target_setup:
            continue
        rows = replay._eligible(signals, setup).copy()
        if rows.empty:
            continue
        if rule.nifty_alignment_min is not None and (
            rule.nifty_target_setups is None or setup.setup_id in rule.nifty_target_setups
        ):
            alignment = np.where(
                rows["side"].eq("LONG"),
                rows["nifty_return_from_open_pct"],
                -rows["nifty_return_from_open_pct"],
            )
            rows = rows.loc[alignment >= rule.nifty_alignment_min].copy()
        if setup.setup_id == rule.target_setup and rule.predicate is not None:
            rows = rows.loc[rule.predicate(rows)].copy()
        if rows.empty:
            continue
        rows = _rank(rows, setup, rule if setup.setup_id == rule.target_setup else Rule("", ""))
        cap = rule.cap_override if setup.setup_id == rule.target_setup and rule.cap_override is not None else setup.max_entries
        selected = rows.groupby("day", sort=False, as_index=False).head(cap).reset_index(drop=True)
        selected["net_return_pct"] = simulator.simulate_bracket(
            selected,
            paths,
            stop_pct=setup.stop_pct,
            target_pct=setup.target_pct,
            cost_bps=COST_BPS,
        )
        selected["filled"] = selected["net_return_pct"].notna()
        selected["setup_id"] = setup.setup_id
        selected["stop_pct"] = setup.stop_pct
        selected["target_pct"] = setup.target_pct
        pieces.append(selected)
    if not pieces:
        return pd.DataFrame(columns=["day", "net_return_pct", "filled"])
    out = pd.concat(pieces, ignore_index=True)
    if rule.signal_time_cap_hhmm is not None and rule.signal_time_cap is not None:
        target = out.loc[out["hhmm_int"].eq(rule.signal_time_cap_hhmm)].copy()
        other = out.loc[~out["hhmm_int"].eq(rule.signal_time_cap_hhmm)].copy()
        column = rule.signal_time_rank_column or "traded_value"
        if column == "abs_price_change_pct":
            target[column] = target["price_change_pct"].abs()
        target = target.sort_values(
            ["day", column, "traded_value", "tradingsymbol"],
            ascending=[True, rule.signal_time_rank_ascending, False, True],
            kind="stable",
        ).groupby("day", sort=False, as_index=False).head(rule.signal_time_cap)
        out = pd.concat([other, target], ignore_index=True)
    return out.sort_values(
        ["day", "hhmm_int", "side", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


def _assert_baseline(audit: pd.DataFrame) -> None:
    published = pd.read_csv(V6_ROOT / "fno_v6_corrected_trades.csv")
    published["day"] = pd.to_datetime(published["day"]).dt.date
    keys = ["day", "hhmm_int", "side", "tradingsymbol", "setup_id"]
    left = audit.loc[audit["filled"], keys + ["net_return_pct"]].sort_values(keys).reset_index(drop=True)
    right = published.loc[published["filled"].astype(str).str.lower().eq("true"), keys + ["net_return_pct"]].sort_values(keys).reset_index(drop=True)
    if left[keys].to_dict("records") != right[keys].to_dict("records"):
        raise AssertionError("Baseline selection does not reproduce published corrected V6.")
    if not np.allclose(left["net_return_pct"], right["net_return_pct"], atol=1e-12):
        raise AssertionError("Baseline returns do not reproduce published corrected V6.")


def _fail_reasons(rows: pd.DataFrame, setup) -> pd.Series:
    reasons = []
    for row in rows.to_dict("records"):
        failed = []
        if setup.side == "LONG":
            if float(row["price_change_pct"]) < setup.price_change_pct:
                failed.append("price")
        elif float(row["price_change_pct"]) > -setup.price_change_pct:
            failed.append("price")
        if float(row["oi_change_pct"]) < setup.oi_change_pct:
            failed.append("oi")
        if float(row["volume_ratio"]) < setup.volume_ratio:
            failed.append("volume")
        if float(row["body_ratio"]) < setup.body_ratio:
            failed.append("body")
        if float(row["wick_ratio"]) > setup.max_wick_ratio:
            failed.append("wick")
        if float(row["traded_value"]) < setup.min_traded_value:
            failed.append("value")
        reasons.append("PASS" if not failed else "+".join(failed))
    return pd.Series(reasons, index=rows.index)


def today_pool(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    baseline: pd.DataFrame,
) -> pd.DataFrame:
    selected_ids = set(baseline.loc[baseline["day"].eq(DAY), "sid"].astype(int))
    pieces = []
    for setup in corrected.ACTIVE_SETUPS:
        pool = signals.loc[
            signals["day"].eq(DAY)
            & signals["hhmm_int"].eq(int(setup.signal_end.replace(":", "")))
            & signals["side"].eq(setup.side)
        ].copy()
        if pool.empty:
            continue
        pool["gate_result"] = _fail_reasons(pool, setup)
        pool["eligible"] = pool["gate_result"].eq("PASS")
        pool["selected"] = pool["sid"].astype(int).isin(selected_ids)
        pool["native_net_return_pct"] = simulator.simulate_bracket(
            pool, paths, stop_pct=setup.stop_pct, target_pct=setup.target_pct, cost_bps=COST_BPS
        )
        pool["setup_id"] = setup.setup_id
        pieces.append(pool)
    out = pd.concat(pieces, ignore_index=True)
    return out.sort_values(["hhmm_int", "side", "eligible", "traded_value"], ascending=[True, True, False, False])


def _rules() -> list[Rule]:
    target = "0926_SHORT"
    rules = [Rule("BASELINE", "Published corrected V6")]
    rules += [
        Rule("REMOVE_0925_SHORT", "Remove the entire 09:25 SHORT leg", target, remove_setup=True),
        Rule("0925_SHORT_CAP1", "Limit 09:25 SHORT to one entry", target, cap_override=1),
    ]
    for picker, asc in (
        ("oi_change_pct", False), ("price_change_pct", True),
        ("body_ratio", False), ("wick_ratio", True),
        ("traded_value", False), ("body_minus_wick", False),
    ):
        rules.append(Rule(f"0925_SHORT_PICK_{picker}", f"Rank 09:25 SHORT by {picker}", target, picker_column=picker, picker_ascending=asc))
        rules.append(Rule(f"0925_SHORT_CAP1_PICK_{picker}", f"One 09:25 SHORT ranked by {picker}", target, picker_column=picker, picker_ascending=asc, cap_override=1))
    for value in (0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.60, 0.75, 1.00):
        rules.append(Rule(f"0925_SHORT_MIN_MOVE_{value:.2f}", f"09:25 SHORT absolute move >= {value:.2f}%", target, predicate=lambda x, v=value: x["price_change_pct"].le(-v)))
    for value in (0.15, 0.20, 0.25, 0.35, 0.50, 0.75, 1.00):
        rules.append(Rule(f"0925_SHORT_MIN_OI_{value:.2f}", f"09:25 SHORT OI change >= {value:.2f}%", target, predicate=lambda x, v=value: x["oi_change_pct"].ge(v)))
    for value in (0.20, 0.25, 0.35, 0.50, 0.75, 1.00, 1.50):
        rules.append(Rule(f"0925_SHORT_MAX_OI_{value:.2f}", f"09:25 SHORT OI change <= {value:.2f}%", target, predicate=lambda x, v=value: x["oi_change_pct"].le(v)))
    for value in (2.0, 2.5, 3.0, 4.0, 5.0):
        rules.append(Rule(f"0925_SHORT_MIN_VOL_{value:.1f}", f"09:25 SHORT volume ratio >= {value:.1f}", target, predicate=lambda x, v=value: x["volume_ratio"].ge(v)))
    for value in (0.45, 0.50, 0.55, 0.60, 0.70, 0.80):
        rules.append(Rule(f"0925_SHORT_MIN_BODY_{value:.2f}", f"09:25 SHORT body ratio >= {value:.2f}", target, predicate=lambda x, v=value: x["body_ratio"].ge(v)))
    for value in (0.45, 0.40, 0.35, 0.30, 0.25, 0.20, 0.10):
        rules.append(Rule(f"0925_SHORT_MAX_WICK_{value:.2f}", f"09:25 SHORT adverse wick <= {value:.2f}", target, predicate=lambda x, v=value: x["wick_ratio"].le(v)))
    for value in (25e6, 50e6, 100e6, 200e6, 500e6):
        rules.append(Rule(f"0925_SHORT_MIN_VALUE_{int(value/1e6)}M", f"09:25 SHORT traded value >= Rs {int(value/1e6)}m", target, predicate=lambda x, v=value: x["traded_value"].ge(v)))
    for value in (-0.10, -0.05, 0.0, 0.025, 0.05, 0.075, 0.10, 0.15, 0.20):
        rules.append(Rule(f"NIFTY_ALIGN_{value:+.3f}", f"All trades require side-aligned NIFTY return from open >= {value:+.3f}%", nifty_alignment_min=value))
    opening_setups = ("0926_LONG", "0926_SHORT")
    for value in (0.025, 0.05, 0.075, 0.10, 0.15, 0.20):
        rules.append(Rule(
            f"0925_NIFTY_ALIGN_{value:+.3f}",
            f"Only 09:25 trades require side-aligned NIFTY return from open >= {value:+.3f}%",
            nifty_alignment_min=value,
            nifty_target_setups=opening_setups,
        ))
    for signal_hhmm, setup_prefix in ((930, "0931"), (935, "0936"), (940, "0941"), (945, "0946")):
        target_setups = (f"{setup_prefix}_LONG", f"{setup_prefix}_SHORT")
        for value in (0.025, 0.05, 0.075, 0.10, 0.15, 0.20):
            rules.append(Rule(
                f"{signal_hhmm:04d}_NIFTY_ALIGN_{value:+.3f}",
                f"Only {signal_hhmm // 100:02d}:{signal_hhmm % 100:02d} trades require side-aligned NIFTY return from open >= {value:+.3f}%",
                nifty_alignment_min=value,
                nifty_target_setups=target_setups,
            ))
    for cap in (1, 2):
        for column in ("traded_value", "volume_ratio", "abs_price_change_pct", "oi_change_pct", "body_ratio"):
            rules.append(Rule(
                f"0925_TOTAL_CAP{cap}_PICK_{column}",
                f"At most {cap} total 09:25 entries, ranked across sides by {column}",
                signal_time_cap_hhmm=925,
                signal_time_cap=cap,
                signal_time_rank_column=column,
            ))
    return rules


def variant_sweep(signals, paths) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    metric_rows = []
    audits = {}
    for rule in _rules():
        audit = run_rule(signals, paths, rule)
        audits[rule.name] = audit
        for period in ("TRAIN", "TEST", "SEP2_CHECK", "TODAY", "ALL"):
            subset = audit.loc[_period_mask(audit, period) & audit["filled"]]
            metric_rows.append({
                "rule": rule.name,
                "description": rule.description,
                "period": period,
                **_metric(subset["net_return_pct"].to_numpy(float)),
            })
    return pd.DataFrame(metric_rows), audits


def setup_period_metrics(baseline: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for setup_id, setup_rows in baseline.groupby("setup_id"):
        for period in ("TRAIN", "TEST", "SEP2_CHECK", "TODAY", "ALL"):
            subset = setup_rows.loc[_period_mask(setup_rows, period) & setup_rows["filled"]]
            rows.append({"setup_id": setup_id, "period": period, **_metric(subset["net_return_pct"].to_numpy(float))})
    return pd.DataFrame(rows)


def selected_feature_diagnostics(baseline: pd.DataFrame) -> pd.DataFrame:
    filled = baseline.loc[baseline["filled"]].copy()
    filled["abs_price_change_pct"] = filled["price_change_pct"].abs()
    rows = []
    for feature in ("abs_price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "wick_ratio", "traded_value", "nifty_return_from_open_pct"):
        winners = filled.loc[filled["net_return_pct"].gt(0), feature]
        losers = filled.loc[filled["net_return_pct"].lt(0), feature]
        rows.append({
            "feature": feature,
            "winner_median": float(winners.median()),
            "loser_median": float(losers.median()),
            "spearman_with_return": float(filled[[feature, "net_return_pct"]].corr(method="spearman").iloc[0, 1]),
        })
    return pd.DataFrame(rows)


def _table(frame: pd.DataFrame, columns: list[str], floatfmt: str = ".3f") -> str:
    return frame.loc[:, columns].to_markdown(index=False, floatfmt=floatfmt)


def main() -> int:
    signals, paths = _load_signals_and_paths()
    signals = signals.merge(
        _nifty_context(),
        on=["contract_month", "day", "hhmm_int"],
        how="left",
        validate="many_to_one",
    )
    baseline = run_rule(signals, paths, Rule("BASELINE", "Published corrected V6"))
    _assert_baseline(baseline)
    pool = today_pool(signals, paths, baseline)
    variants, audits = variant_sweep(signals, paths)
    setups = setup_period_metrics(baseline)
    features = selected_feature_diagnostics(baseline)

    pivot = variants.pivot(index=["rule", "description"], columns="period", values=["fills", "pf", "net_pct"]).reset_index()
    pivot.columns = ["_".join([str(x) for x in col if str(x)]) if isinstance(col, tuple) else str(col) for col in pivot.columns]
    base = pivot.loc[pivot["rule"].eq("BASELINE")].iloc[0]
    pivot["train_pf_delta"] = pivot["pf_TRAIN"] - base["pf_TRAIN"]
    pivot["test_pf_delta"] = pivot["pf_TEST"] - base["pf_TEST"]
    pivot["all_pf_delta"] = pivot["pf_ALL"] - base["pf_ALL"]
    pivot["today_net_delta"] = pivot["net_pct_TODAY"] - base["net_pct_TODAY"]
    robust = pivot.loc[
        pivot["fills_TRAIN"].ge(max(10, 0.80 * float(base["fills_TRAIN"])))
        & pivot["pf_TRAIN"].gt(float(base["pf_TRAIN"]))
        & pivot["pf_TEST"].gt(float(base["pf_TEST"]))
    ].sort_values(["pf_TEST", "pf_TRAIN"], ascending=False)
    today_positive = pivot.loc[pivot["net_pct_TODAY"].gt(0)].sort_values("pf_TEST", ascending=False)
    decision_names = [
        "BASELINE",
        "0925_NIFTY_ALIGN_+0.025",
        "0925_NIFTY_ALIGN_+0.050",
        "0925_SHORT_MIN_BODY_0.55",
        "NIFTY_ALIGN_+0.100",
        "0925_TOTAL_CAP1_PICK_traded_value",
    ]
    decision = pivot.loc[pivot["rule"].isin(decision_names)].copy()
    decision["sort_order"] = decision["rule"].map({name: i for i, name in enumerate(decision_names)})
    decision = decision.sort_values("sort_order")

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    pool.to_csv(OUTPUT_ROOT / "today_candidate_pool.csv", index=False)
    variants.to_csv(OUTPUT_ROOT / "selection_variant_metrics.csv", index=False)
    pivot.to_csv(OUTPUT_ROOT / "selection_variant_pivot.csv", index=False)
    setups.to_csv(OUTPUT_ROOT / "setup_period_metrics.csv", index=False)
    features.to_csv(OUTPUT_ROOT / "selected_feature_diagnostics.csv", index=False)
    baseline.to_csv(OUTPUT_ROOT / "baseline_reproduction.csv", index=False)

    today_selected = pool.loc[pool["selected"]].copy()
    today_candidates = pool.loc[pool["hhmm_int"].isin([925, 940])].copy()
    columns = ["setup_id", "tradingsymbol", "side", "gate_result", "eligible", "selected", "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "wick_ratio", "traded_value", "nifty_return_from_open_pct", "native_net_return_pct"]
    variant_columns = ["rule", "description", "fills_TRAIN", "pf_TRAIN", "net_pct_TRAIN", "fills_TEST", "pf_TEST", "net_pct_TEST", "pf_SEP2_CHECK", "net_pct_SEP2_CHECK", "net_pct_TODAY", "pf_ALL", "net_pct_ALL"]

    report = [
        f"# Corrected V6 selection audit - {DAY}",
        "",
        "## Verdict",
        "",
        "The published corrected V6 selection was reproduced exactly before any experiment. Today's loss was not caused by a picker choosing the wrong member of a larger eligible pool: SOLARINDS was the only eligible 09:25 LONG, COFORGE and PREMIERENE were the only eligible 09:25 SHORTs and the setup cap was two, and MAHABANK was the only eligible 09:40 LONG. The failure is therefore at the setup/gate/regime level, not the within-pool ranking step.",
        "",
        "Making the day profitable by inventing a threshold after seeing these four outcomes is trivial but invalid. The tables below separate such hindsight rescues from rules that also improve both the original train and test segments.",
        "",
        "## Decision summary",
        "",
        "The largest apparent PF is the all-entry NIFTY-alignment rule at +0.10%: PF rises from 1.925 to 2.942, but fills fall from 70 to 35 and net return falls from +26.006% to +24.826%. This is a selectivity trade-off, not a free improvement.",
        "",
        "The best balanced exploratory result is to apply market alignment only at 09:25. The +0.025% version produces PF 2.272, +28.896% net and 59 fills, and would leave only MAHABANK today for +1.075%. However, today's NIFTY alignment was about +0.0249% for SHORTs, so the winning cutoff is less than 0.0001 percentage point above today's value. Since it was inspected after today's result, it is too close to the boundary to approve for production.",
        "",
        "A cleaner +0.05% neutral-zone rule also makes today +1.075%, but its full-sample net is +25.946% (slightly below V6) and TRAIN PF is 2.004 versus V6's 2.041. The 09:25 minimum-body 0.55 alternative keeps 65 fills and reaches PF 2.154 / +28.749%, but also weakens TRAIN PF and its cutoff sits immediately above both losing shorts today. Both remain hypotheses, not passed changes.",
        "",
        _table(decision, ["rule", "fills_TRAIN", "pf_TRAIN", "fills_TEST", "pf_TEST", "fills_SEP2_CHECK", "pf_SEP2_CHECK", "net_pct_TODAY", "fills_ALL", "pf_ALL", "net_pct_ALL"]),
        "",
        "Recommendation: do not modify corrected V6 from this audit. Freeze the market-regime idea in a separate shadow challenger and collect independent forward sessions. If PF alone is the objective, test the all-entry +0.10% alignment rule; if retaining trades and total return matters, test a predeclared 09:25 neutral zone (prefer a round +0.05% threshold rather than the post-hoc +0.025% boundary). Do not loosen OI or volume gates to capture today's missed winners, and do not add an upper OI cap: those actions are hindsight-driven and the MAHABANK winner had the day's largest selected OI increase.",
        "",
        "## Today's selected trades",
        "",
        _table(today_selected, columns),
        "",
        "## All strict-confirmed candidates around the active 09:25 and 09:40 decisions",
        "",
        _table(today_candidates, columns),
        "",
        "`gate_result` states why a strict-confirmed candidate failed the V6 five-minute setup gate. `native_net_return_pct` is its counterfactual return under that setup's native stop/target and 5 bps cost; it is hindsight and must not be used directly as a live ranker.",
        "",
        "## Baseline setup stability",
        "",
        _table(setups, ["setup_id", "period", "fills", "wins", "losses", "pf", "net_pct", "win_rate"]),
        "",
        "## Pre-entry feature diagnostics on the 70 published V6 fills",
        "",
        _table(features, ["feature", "winner_median", "loser_median", "spearman_with_return"]),
        "",
        "These are weak univariate diagnostics on a small selected sample, not promotion evidence. A large correlation would still require a frozen walk-forward test.",
        "",
        "## Rules that make today positive (ordered by original-test PF)",
        "",
        _table(today_positive.head(20), variant_columns),
        "",
        "## Rules improving PF in both original TRAIN and TEST",
        "",
        _table(robust.head(20), variant_columns) if not robust.empty else "No tested rule improved PF in both original TRAIN and TEST while retaining at least 80% of baseline TRAIN fills.",
        "",
        "## Method and guardrails",
        "",
        "The sweep changes selection only. It retains corrected rolling near-month OI, strict causal confirmation, trigger entries, each setup's existing stop/target and 5 bps cost. Candidate rules cover 09:25 SHORT caps, cross-side opening caps, causal feature thresholds/rankers and a contemporaneous NIFTY-futures direction filter. TRAIN is before 2026-08-14, TEST is 2026-08-14 through 2026-09-01, and SEP2_CHECK contains 2-3 September. The evidence spans only 24 sessions and 70 baseline fills. Because many variants were inspected, even a rule passing both old segments remains research-only until frozen forward evidence accumulates.",
    ]
    report_path = OUTPUT_ROOT / "FNO_V6_TODAY_SELECTION_DEEP_DIVE.md"
    report_path.write_text("\n".join(report), encoding="utf-8")
    print("BASELINE", _metric(baseline.loc[baseline["filled"], "net_return_pct"].to_numpy(float)))
    print("TODAY", _metric(baseline.loc[baseline["filled"] & baseline["day"].eq(DAY), "net_return_pct"].to_numpy(float)))
    print("ROBUST_ROWS", len(robust))
    print(robust[variant_columns].head(10).to_string(index=False) if not robust.empty else "NONE")
    print("TODAY_POSITIVE_ROWS", len(today_positive))
    print(today_positive[variant_columns].head(10).to_string(index=False))
    print("REPORT", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
