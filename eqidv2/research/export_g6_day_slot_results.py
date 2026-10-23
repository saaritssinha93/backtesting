from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


KEYS = ["day", "setup_id", "side"]


def _bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    return series.fillna(False).astype(str).str.lower().isin({"true", "1", "yes"})


def _joined(values: pd.Series) -> str:
    return "|".join(sorted({str(value) for value in values.dropna()}))


def _policy_metrics(
    policy: pd.DataFrame,
    baseline_by_sid: pd.DataFrame,
    policy_name: str,
) -> pd.DataFrame:
    p = policy.copy()
    p["filled"] = _bool(p["filled"])
    p["portfolio_executed"] = _bool(p["portfolio_executed"])
    p["research_veto"] = _bool(p["research_veto"])
    p["portfolio_net_profit_rupees"] = pd.to_numeric(
        p["portfolio_net_profit_rupees"], errors="coerce"
    ).fillna(0.0)
    p = p.merge(baseline_by_sid, on="sid", how="left", validate="one_to_one")

    executed = p["portfolio_executed"]
    pnl = p["portfolio_net_profit_rupees"]
    base_executed = p["baseline_executed"].fillna(False)
    base_pnl = p["baseline_pnl_rs"].fillna(0.0)
    exit_reason = p["exit_reason"].fillna("").astype(str)

    p["m_vetoed_orders"] = p["research_veto"].astype(int)
    p["m_eligible_native_fills"] = (p["filled"] & ~p["research_veto"]).astype(int)
    p["m_executed_trades"] = executed.astype(int)
    p["m_wins"] = (executed & (pnl > 0)).astype(int)
    p["m_losses"] = (executed & (pnl < 0)).astype(int)
    p["m_sl_exits"] = (
        executed & exit_reason.isin(["STOP", "TIGHTENED_STOP"])
    ).astype(int)
    p["m_target_exits"] = (executed & exit_reason.eq("TARGET")).astype(int)
    p["m_time_exits"] = (executed & exit_reason.str.startswith("TIME_EXIT")).astype(int)
    p["m_net_pnl_rs"] = np.where(executed, pnl, 0.0)
    p["m_positive_trade_pnl_rs"] = np.where(executed & (pnl > 0), pnl, 0.0)
    p["m_negative_trade_sum_abs_rs"] = np.where(executed & (pnl < 0), -pnl, 0.0)
    p["m_losing_trades_avoided"] = (
        base_executed & (base_pnl < 0) & ~executed
    ).astype(int)
    p["m_winning_trades_excluded"] = (
        base_executed & (base_pnl > 0) & ~executed
    ).astype(int)
    p["m_loss_value_avoided_rs"] = np.where(
        base_executed & (base_pnl < 0) & ~executed, -base_pnl, 0.0
    )
    p["m_winner_value_excluded_rs"] = np.where(
        base_executed & (base_pnl > 0) & ~executed, base_pnl, 0.0
    )
    p["m_newly_allocated_trades"] = (~base_executed & executed).astype(int)
    p["m_newly_allocated_pnl_rs"] = np.where(~base_executed & executed, pnl, 0.0)

    metric_columns = [column for column in p.columns if column.startswith("m_")]
    grouped = p.groupby(KEYS, sort=False, dropna=False)[metric_columns].sum().reset_index()
    rename = {column: f"{policy_name}__{column[2:]}" for column in metric_columns}
    grouped = grouped.rename(columns=rename)
    prefix = f"{policy_name}__"
    grouped[f"{prefix}win_rate_pct"] = np.where(
        grouped[f"{prefix}executed_trades"] > 0,
        grouped[f"{prefix}wins"] / grouped[f"{prefix}executed_trades"] * 100.0,
        np.nan,
    )
    grouped[f"{prefix}losing_slot_flag"] = (
        grouped[f"{prefix}net_pnl_rs"] < 0
    ).astype(int)
    return grouped


def build(source_dir: Path, output_path: Path) -> pd.DataFrame:
    features = pd.read_csv(source_dir / "trade_features.csv")
    features["day"] = features["day"].astype(str)
    features["sid"] = pd.to_numeric(features["sid"], errors="raise")

    excluded = {
        "sid",
        "day",
        "setup_id",
        "tradingsymbol",
        "side",
        "confirmation_ts",
        "portfolio_executed",
        "portfolio_net_profit_rupees",
        "exit_reason",
    }
    feature_columns = [
        column
        for column in features.columns
        if column not in excluded and not column.startswith("veto_")
    ]

    # Market features must be identical for all stocks selected in the same slot-side.
    uniqueness = features.groupby(KEYS, dropna=False)[feature_columns].nunique(dropna=False)
    if (uniqueness > 1).any().any():
        bad = uniqueness.index[(uniqueness > 1).any(axis=1)].tolist()[:5]
        raise AssertionError(f"Market features differ within slot-side groups: {bad}")

    group = features.groupby(KEYS, sort=False, dropna=False)
    result = group[feature_columns].first().reset_index()
    trace = group.agg(
        selected_orders=("sid", "size"),
        selected_symbols=("tradingsymbol", _joined),
        selected_sids=("sid", lambda s: "|".join(str(int(x)) for x in sorted(s))),
        confirmation_timestamp=("confirmation_ts", "first"),
    ).reset_index()
    result = trace.merge(result, on=KEYS, how="inner", validate="one_to_one")
    result.insert(0, "record_scope", "COMPLETED_G6_BACKTEST")
    result.insert(4, "confirmation_time", result["setup_id"].str[:4].str.replace(
        r"^(\d{2})(\d{2})$", r"\1:\2", regex=True
    ))

    # These inputs were not available historically in the study. Blank means unavailable, not zero.
    unavailable = [
        "historical_intraday_vix_pct",
        "historical_option_skew",
        "historical_cash_sector_membership",
        "historical_orderbook_spread_depth",
        "macro_event_flag",
        "cross_stock_correlation",
    ]
    for column in unavailable:
        result[column] = np.nan

    policy_names = sorted(path.name for path in (source_dir / "policies").iterdir() if path.is_dir())
    if "baseline" in policy_names:
        policy_names.remove("baseline")
        policy_names.insert(0, "baseline")

    baseline = pd.read_csv(source_dir / "policies" / "baseline" / "trades.csv")
    baseline["sid"] = pd.to_numeric(baseline["sid"], errors="raise")
    baseline_by_sid = baseline[["sid", "portfolio_executed", "portfolio_net_profit_rupees"]].copy()
    baseline_by_sid = baseline_by_sid.rename(
        columns={
            "portfolio_executed": "baseline_executed",
            "portfolio_net_profit_rupees": "baseline_pnl_rs",
        }
    )
    baseline_by_sid["baseline_executed"] = _bool(baseline_by_sid["baseline_executed"])
    baseline_by_sid["baseline_pnl_rs"] = pd.to_numeric(
        baseline_by_sid["baseline_pnl_rs"], errors="coerce"
    ).fillna(0.0)

    expected = pd.read_csv(source_dir / "policy_comparison.csv")
    expected = expected[expected["period"].eq("all55")].set_index("policy")

    for policy_name in policy_names:
        policy = pd.read_csv(source_dir / "policies" / policy_name / "trades.csv")
        policy["day"] = policy["day"].astype(str)
        policy["sid"] = pd.to_numeric(policy["sid"], errors="raise")
        metrics = _policy_metrics(policy, baseline_by_sid, policy_name)
        result = result.merge(metrics, on=KEYS, how="left", validate="one_to_one")

        trades = int(metrics[f"{policy_name}__executed_trades"].sum())
        net_pnl = float(metrics[f"{policy_name}__net_pnl_rs"].sum())
        if trades != int(expected.loc[policy_name, "trades"]):
            raise AssertionError(f"{policy_name}: trade reconciliation failed")
        if not np.isclose(net_pnl, float(expected.loc[policy_name, "net_pnl_rs"]), atol=0.01):
            raise AssertionError(f"{policy_name}: P&L reconciliation failed")

    for policy_name in policy_names:
        result[f"{policy_name}__pnl_delta_vs_baseline_rs"] = (
            result[f"{policy_name}__net_pnl_rs"] - result["baseline__net_pnl_rs"]
        )

    result["available_market_parameter_count"] = result[feature_columns].notna().sum(axis=1)
    result["total_market_parameter_count"] = len(feature_columns)

    result = result.sort_values(
        ["day", "confirmation_time", "side", "setup_id"], kind="stable"
    ).reset_index(drop=True)
    if result.duplicated(KEYS).any():
        raise AssertionError("Duplicate day/setup/side rows in output")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False, na_rep="", float_format="%.10g")

    reread = pd.read_csv(output_path, low_memory=False)
    if len(reread) != len(result) or reread.duplicated(KEYS).any():
        raise AssertionError("Saved CSV row/key verification failed")
    if not np.isclose(reread["baseline__net_pnl_rs"].sum(), 1053774.6189516708, atol=0.01):
        raise AssertionError("Saved CSV baseline total does not reconcile")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_dir", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()
    result = build(args.source_dir.resolve(), args.output_csv.resolve())
    print(f"saved={args.output_csv.resolve()}")
    print(f"rows={len(result)} columns={len(result.columns)}")
    print(f"days={result['day'].nunique()} setups={result['setup_id'].nunique()}")
    print(f"baseline_trades={int(result['baseline__executed_trades'].sum())}")
    print(f"baseline_net_pnl_rs={result['baseline__net_pnl_rs'].sum():.2f}")


if __name__ == "__main__":
    main()
