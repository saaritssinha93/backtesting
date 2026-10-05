"""V13-v10-G-2: retained G entries/targets with every stop set to 1.00%.

This is a research-only exit sensitivity.  It reads the immutable, hash-verified
V13-v10-G full-history bundle, reconstructs the retained G selections from the
sealed signals, and replays every path with a 1.00% full-position stop.  Targets,
entry expiry, costs, sizing, leverage and portfolio rules stay unchanged.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from dataclasses import asdict, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g


VERSION = "V13-v10-G-2"
STOP_PCT = 1.0
EVIDENCE = "RETROSPECTIVE_EXIT_SENSITIVITY_REUSED_HISTORY_NOT_PROMOTED"
DEFAULT_SOURCE_BUNDLE = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_full_history"
) / "run_20260925_cutoff_corrected_through_20260923"
DEFAULT_G_CONFIG = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g"
) / "run_20260914_opportunity_expansion/frozen_config.json"
DEFAULT_OUTPUT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_2"
) / "run_20261004_sl100_through_20260923"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def dump_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def transformed_exit(source_exit: dict[str, Any]) -> dict[str, Any]:
    """Keep every G target exactly and replace every configured stop by 1%."""
    result = copy.deepcopy(source_exit)
    result.update(
        version=VERSION,
        source_version=str(source_exit.get("version", "V13-v10-G")),
        rule="Every active setup stop is exactly 1.00%; targets are unchanged from retained G.",
        stop_pct=STOP_PCT,
        target_policy="UNCHANGED_FROM_RETAINED_G",
        evidence=EVIDENCE,
    )
    result["default"]["stop_pct"] = STOP_PCT
    for pair in result["setups"].values():
        pair["stop_pct"] = STOP_PCT
    validate_exit(result, source_exit)
    return result


def validate_exit(candidate: dict[str, Any], source_exit: dict[str, Any]) -> None:
    source_pairs = {"__DEFAULT__": source_exit["default"], **source_exit["setups"]}
    candidate_pairs = {"__DEFAULT__": candidate["default"], **candidate["setups"]}
    if set(candidate_pairs) != set(source_pairs):
        raise ValueError("G-2 exit setup keys must exactly match retained G")
    for setup_id, pair in candidate_pairs.items():
        stop = float(pair["stop_pct"])
        target = float(pair["target_pct"])
        if not np.isfinite(stop) or stop != STOP_PCT:
            raise ValueError(f"G-2 requires an exact 1.00% stop for {setup_id}")
        if not np.isfinite(target) or target != float(source_pairs[setup_id]["target_pct"]):
            raise ValueError(f"G-2 target drift for {setup_id}")
    if candidate.get("partial_exits") is not False or candidate.get("breakeven_stop") is not False:
        raise ValueError("G-2 retains full exits without a breakeven stop")


def config(source_g: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(source_g)
    result.update(
        version=VERSION,
        source_version=str(source_g["version"]),
        exit=transformed_exit(source_g["exit"]),
        stop_change={
            "mode": "ABSOLUTE_ALL_ACTIVE_SETUPS",
            "stop_pct": STOP_PCT,
            "targets": "UNCHANGED",
        },
        evidence=EVIDENCE,
        live_configuration_changed=False,
        execution_authority=False,
    )
    return result


def checked_settings(candidate: dict[str, Any], source_g: dict[str, Any]) -> dict[str, Any]:
    expected = config(source_g)
    if candidate != expected:
        raise ValueError("G-2 settings differ from the fixed 1.00% stop experiment")
    validate_exit(candidate["exit"], source_g["exit"])
    return expected


def verify_bundle(root: Path) -> dict[str, Any]:
    manifest_path = root / "bundle_manifest.json"
    manifest = read_json(manifest_path)
    if manifest.get("state") != "COMPLETE":
        raise ValueError("Source bundle is not COMPLETE")
    if manifest.get("transform", {}).get("strategy_outputs_unchanged") is not True:
        raise ValueError("Source bundle does not attest unchanged strategy outputs")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError("Source bundle has no artifact inventory")
    mismatches = []
    for relative, record in artifacts.items():
        path = root / relative
        observed = sha256(path) if path.is_file() else None
        if observed != record.get("sha256"):
            mismatches.append(relative)
    if mismatches:
        raise RuntimeError(f"Source bundle artifact drift: {', '.join(mismatches)}")
    return manifest


def _selection_keys(frame: pd.DataFrame) -> list[tuple[int, str]]:
    return sorted(zip(frame["sid"].astype(int), frame["setup_id"].astype(str)))


def load_bundle(source: Path, g_config_path: Path) -> dict[str, Any]:
    """Load sealed artifacts without consulting mutable upstream raw files."""
    source = source.resolve()
    manifest = verify_bundle(source)
    run_metadata = read_json(source / "g_backtest/run_metadata.json")
    source_g = read_json(g_config_path)
    recorded_config_hash = str(run_metadata.get("frozen_g_config_sha256") or "")
    if sha256(g_config_path) != recorded_config_hash:
        raise RuntimeError("Retained G configuration does not match the sealed bundle")
    # This validates the complete retained-G contract, not only the exit table.
    g.checked_settings(source_g)

    dataset_manifest = read_json(source / "dataset/dataset_manifest.json")
    signals = pd.read_parquet(source / "dataset/signals.parquet")
    base = replace(
        g.v9.V9Config(),
        portfolio_capital_rupees=float(source_g["portfolio_capital_rupees"]),
        capital_per_entry_rupees=float(source_g["capital_per_entry_rupees"]),
        leverage_factor=float(source_g["leverage_factor"]),
        max_positions=source_g["max_positions"],
        cost_bps=float(source_g["cost_bps"]),
    )
    base.validate()
    change = g.SelectionChange(**source_g["selection_change"])
    orders = g.select_orders(
        signals,
        base,
        change,
        core_first=source_g["core_first"],
        morning_slots=source_g.get("morning_slots", False),
        two_bar_continuation=source_g.get("two_bar_continuation", False),
    )
    sealed_orders = pd.read_csv(source / "g_backtest/selected_trades.csv")
    if _selection_keys(orders) != _selection_keys(sealed_orders):
        raise RuntimeError("Reconstructed G selections differ from the sealed selection ledger")

    paths: dict[int, dict[str, np.ndarray]] = {}
    needed = set(orders["sid"].astype(int))
    with np.load(source / "dataset/paths.npz", allow_pickle=False) as archive:
        for name in archive.files:
            sid_text, field = name.split("_", 1)
            sid = int(sid_text)
            if sid in needed:
                paths.setdefault(sid, {})[field] = archive[name]
    g.v9.validate_paths(orders, paths)
    days = [date.fromisoformat(value) for value in dataset_manifest["days"]]
    return {
        "source": source,
        "g_config_path": g_config_path.resolve(),
        "bundle_manifest": manifest,
        "dataset_manifest": dataset_manifest,
        "source_g": source_g,
        "signals": signals,
        "orders": orders,
        "paths": paths,
        "days": days,
        "v9_config": base,
    }


def evaluate(dataset: dict[str, Any], settings: dict[str, Any]):
    source_g = dataset["source_g"]
    settings = checked_settings(settings, source_g)
    orders = dataset["orders"].copy()
    exits = settings["exit"]
    target_map = {key: float(pair["target_pct"]) for key, pair in exits["setups"].items()}
    orders["native_stop_pct"] = STOP_PCT
    orders["native_target_pct"] = orders["setup_id"].map(target_map).fillna(
        float(exits["default"]["target_pct"])
    )
    if not orders["native_stop_pct"].eq(STOP_PCT).all():
        raise AssertionError("Not every selected order received the 1.00% stop")
    g.v9.validate_paths(orders, dataset["paths"])
    base = dataset["v9_config"]
    trades = g.v9.v5.simulate_native(
        orders,
        dataset["paths"],
        cost_bps=base.cost_bps,
        max_entry_delay_minutes=int(settings["entry_expiry_minutes"]),
    )
    trades = g.v9.v5.apply_fixed_capital_model(
        trades, base.capital_per_entry_rupees, base.leverage_factor
    )
    trades["v10_g_2_stop_pct"] = STOP_PCT
    trades["v10_g_2_target_pct"] = trades["native_target_pct"]
    trades["v10_g_2_reward_risk"] = (
        trades["v10_g_2_target_pct"] / trades["v10_g_2_stop_pct"]
    )
    ledger, summary = g.v9.v6.apply_portfolio_constraints(
        trades, base.portfolio_config()
    )
    summary.update(
        version=VERSION,
        source_version=source_g["version"],
        settings=settings,
        evidence=EVIDENCE,
        stop_pct=STOP_PCT,
        target_policy="UNCHANGED_FROM_RETAINED_G",
        selection_count=len(orders),
        selection_identity_unchanged=True,
        partial_exits=False,
        breakeven_stop=False,
        square_off="15:15 Asia/Kolkata",
        live_configuration_changed=False,
        execution_authority=False,
    )
    return trades, ledger, summary


def _metric_rows(frame: pd.DataFrame, group: str) -> dict[str, Any]:
    pnl = pd.to_numeric(frame["portfolio_net_profit_rupees"], errors="coerce").fillna(0.0)
    gross = pd.to_numeric(frame["portfolio_gross_profit_rupees"], errors="coerce").fillna(0.0)
    cost = pd.to_numeric(frame["portfolio_cost_rupees"], errors="coerce").fillna(0.0)
    positive, negative = pnl[pnl > 1e-9], pnl[pnl < -1e-9]
    return {
        "group": group,
        "trades": len(frame),
        "wins": int((pnl > 1e-9).sum()),
        "losses": int((pnl < -1e-9).sum()),
        "win_rate_pct": float((pnl > 1e-9).mean() * 100) if len(frame) else 0.0,
        "profit_factor": float(positive.sum() / -negative.sum()) if len(negative) else None,
        "gross_pnl_rupees": float(gross.sum()),
        "cost_rupees": float(cost.sum()),
        "net_pnl_rupees": float(pnl.sum()),
        "average_trade_rupees": float(pnl.mean()) if len(frame) else 0.0,
        "median_trade_rupees": float(pnl.median()) if len(frame) else 0.0,
    }


def _executed(ledger: pd.DataFrame) -> pd.DataFrame:
    return ledger.loc[ledger["portfolio_executed"].eq(True)].copy()


def build_breakdowns(ledger: pd.DataFrame, days: list[date]) -> dict[str, pd.DataFrame]:
    executed = _executed(ledger)
    executed["day"] = pd.to_datetime(executed["day"])
    sessions = pd.DatetimeIndex(pd.to_datetime(days))
    daily = pd.DataFrame(index=sessions)
    selected_dates = pd.to_datetime(ledger["day"])
    daily["selected_orders"] = ledger.groupby(selected_dates).size().reindex(sessions, fill_value=0)
    daily["trades"] = executed.groupby("day").size().reindex(sessions, fill_value=0)
    daily["wins"] = executed.groupby("day")["portfolio_net_profit_rupees"].apply(
        lambda values: int((values > 1e-9).sum())
    ).reindex(sessions, fill_value=0)
    daily["losses"] = executed.groupby("day")["portfolio_net_profit_rupees"].apply(
        lambda values: int((values < -1e-9).sum())
    ).reindex(sessions, fill_value=0)
    for target, source in (
        ("gross_pnl_rupees", "portfolio_gross_profit_rupees"),
        ("cost_rupees", "portfolio_cost_rupees"),
        ("net_pnl_rupees", "portfolio_net_profit_rupees"),
    ):
        daily[target] = executed.groupby("day")[source].sum().reindex(sessions, fill_value=0.0)
    daily["cumulative_net_pnl_rupees"] = daily["net_pnl_rupees"].cumsum()
    peak = daily["cumulative_net_pnl_rupees"].cummax().clip(lower=0.0)
    daily["drawdown_rupees"] = peak - daily["cumulative_net_pnl_rupees"]
    daily.index.name = "day"
    daily = daily.reset_index()

    def grouped(column: str) -> pd.DataFrame:
        rows = [_metric_rows(group, str(key)) for key, group in executed.groupby(column, sort=True)]
        return pd.DataFrame(rows)

    month_rows = []
    for month, group in executed.groupby(executed["day"].dt.to_period("M"), sort=True):
        row = _metric_rows(group, str(month))
        row["sessions"] = int((sessions.to_period("M") == month).sum())
        row["selected_orders"] = int(
            ledger.loc[pd.to_datetime(ledger["day"]).dt.to_period("M").eq(month)].shape[0]
        )
        month_rows.append(row)
    return {
        "daily": daily,
        "monthly": pd.DataFrame(month_rows),
        "side": grouped("side"),
        "setup": grouped("setup_id").sort_values("net_pnl_rupees", ascending=False),
        "exit": grouped("exit_reason").sort_values("trades", ascending=False),
    }


def _fmt_money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}Rs {abs(value):,.2f}"


def _fmt_pf(value: Any) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{float(value):.4f}"


def write_report(
    output: Path,
    dataset: dict[str, Any],
    ledger: pd.DataFrame,
    summary: dict[str, Any],
    breakdowns: dict[str, pd.DataFrame],
) -> Path:
    days = dataset["days"]
    control = pd.read_csv(dataset["source"] / "g_backtest/portfolio_trades.csv")
    control_summary = read_json(dataset["source"] / "g_backtest/summary.json")
    control_metric = g.r.metric(control, days)
    candidate_metric = g.r.metric(ledger, days)
    control_detail = _metric_rows(_executed(control), "V13-v10-G")
    candidate_detail = _metric_rows(_executed(ledger), VERSION)
    comparison = pd.DataFrame([
        {"strategy": "V13-v10-G", **control_metric},
        {"strategy": VERSION, **candidate_metric},
    ])
    comparison["net_delta_vs_g_rupees"] = comparison["net_profit_rupees"] - float(
        control_metric["net_profit_rupees"]
    )
    comparison.to_csv(output / "g_vs_g2_comparison.csv", index=False)

    identity = ["sid", "setup_id", "day", "tradingsymbol", "side"]
    outcome = [
        "filled", "exit_reason", "portfolio_executed", "portfolio_net_profit_rupees",
        "native_stop_pct", "native_target_pct",
    ]
    changes = control[identity + outcome].merge(
        ledger[["sid", "setup_id", *outcome]],
        on=["sid", "setup_id"],
        how="outer",
        validate="one_to_one",
        suffixes=("_g", "_g2"),
        indicator=True,
    )
    if not changes["_merge"].eq("both").all():
        raise RuntimeError("G/G-2 trade identity mismatch during comparison")
    changes["net_delta_rupees"] = (
        pd.to_numeric(changes["portfolio_net_profit_rupees_g2"], errors="coerce").fillna(0.0)
        - pd.to_numeric(changes["portfolio_net_profit_rupees_g"], errors="coerce").fillna(0.0)
    )
    changes["exit_changed"] = changes["exit_reason_g"].fillna("").ne(
        changes["exit_reason_g2"].fillna("")
    )
    changes["avoided_g_stop"] = changes["exit_reason_g"].eq("STOP") & ~changes[
        "exit_reason_g2"
    ].eq("STOP")
    material_changes = changes.loc[
        changes["exit_changed"] | changes["net_delta_rupees"].abs().gt(0.01)
    ].sort_values(["day", "sid"], kind="stable")
    material_changes.to_csv(output / "trade_change_analysis.csv", index=False)

    stop_rows = []
    source_exit = dataset["source_g"]["exit"]
    for setup_id, pair in sorted(source_exit["setups"].items()):
        stop_rows.append({
            "setup_id": setup_id,
            "side": setup_id.split("_", 1)[1],
            "g_stop_pct": float(pair["stop_pct"]),
            "g2_stop_pct": STOP_PCT,
            "target_pct_unchanged": float(pair["target_pct"]),
            "g2_reward_risk": float(pair["target_pct"]) / STOP_PCT,
        })
    pd.DataFrame(stop_rows).to_csv(output / "stop_comparison.csv", index=False)

    for name, frame in breakdowns.items():
        frame.to_csv(output / f"{name}_results.csv", index=False)

    delta = candidate_metric["net_profit_rupees"] - control_metric["net_profit_rupees"]
    lines = [
        f"# {VERSION} full backtest — all stops at 1.00%",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Research-only retrospective exit sensitivity. G selections and targets are unchanged; "
        "only every active stop is set to 1.00%. Live and paper configurations were not changed.",
        "",
        "## Headline comparison",
        "",
        "| Metric | V13-v10-G | V13-v10-G-2 |",
        "|---|---:|---:|",
    ]
    fields = [
        ("Selected orders", "selected_orders", ".0f"),
        ("Executed trades", "trades", ".0f"),
        ("Wins", "wins", ".0f"),
        ("Losses", "losses", ".0f"),
        ("Win rate", "win_rate_pct", ".2f"),
        ("Profit factor", "profit_factor", ".4f"),
        ("Net P&L", "net_profit_rupees", ".2f"),
        ("Daily-close maximum drawdown", "daily_close_drawdown_rupees", ".2f"),
        ("Median trade", "median_trade_rupees", ".2f"),
    ]
    for label, key, spec in fields:
        left, right = control_metric[key], candidate_metric[key]
        if key in {"net_profit_rupees", "daily_close_drawdown_rupees", "median_trade_rupees"}:
            left_text, right_text = _fmt_money(float(left)), _fmt_money(float(right))
        elif key == "win_rate_pct":
            left_text, right_text = f"{left:{spec}}%", f"{right:{spec}}%"
        else:
            left_text, right_text = f"{left:{spec}}", f"{right:{spec}}"
        lines.append(f"| {label} | {left_text} | {right_text} |")
    lines.extend([
        f"| Gross P&L | {_fmt_money(control_detail['gross_pnl_rupees'])} | "
        f"{_fmt_money(candidate_detail['gross_pnl_rupees'])} |",
        f"| Modeled costs | {_fmt_money(control_detail['cost_rupees'])} | "
        f"{_fmt_money(candidate_detail['cost_rupees'])} |",
        f"| Average trade | {_fmt_money(control_detail['average_trade_rupees'])} | "
        f"{_fmt_money(candidate_detail['average_trade_rupees'])} |",
        f"| Net return on Rs 10 lakh capital | "
        f"{control_metric['net_profit_rupees'] / 10_000:.2f}% | "
        f"{candidate_metric['net_profit_rupees'] / 10_000:.2f}% |",
        f"| Peak concurrent positions | {int(control_summary['peak_concurrent_positions'])} | "
        f"{int(summary['peak_concurrent_positions'])} |",
        f"| Peak reserved capital | {_fmt_money(float(control_summary['peak_reserved_capital_rupees']))} | "
        f"{_fmt_money(float(summary['peak_reserved_capital_rupees']))} |",
        f"| Peak modeled gross exposure | {_fmt_money(float(control_summary['peak_gross_exposure_rupees']))} | "
        f"{_fmt_money(float(summary['peak_gross_exposure_rupees']))} |",
        f"| Peak open initial risk | {_fmt_money(float(control_summary['peak_open_initial_risk_rupees']))} | "
        f"{_fmt_money(float(summary['peak_open_initial_risk_rupees']))} |",
    ])
    avoided = material_changes.loc[material_changes["avoided_g_stop"]]
    g_stop_count = int(_executed(control)["exit_reason"].eq("STOP").sum())
    became_target = int(avoided["exit_reason_g2"].eq("TARGET").sum())
    became_time = int(avoided["exit_reason_g2"].eq("TIME_EXIT_1515").sum())
    post_mask_g = pd.to_datetime(control["day"]).gt(pd.Timestamp("2026-09-11"))
    post_mask_g2 = pd.to_datetime(ledger["day"]).gt(pd.Timestamp("2026-09-11"))
    post_g = _metric_rows(_executed(control.loc[post_mask_g]), "G post-2026-09-11")
    post_g2 = _metric_rows(_executed(ledger.loc[post_mask_g2]), "G-2 post-2026-09-11")
    daily = breakdowns["daily"]
    best_day = daily.loc[daily["net_pnl_rupees"].idxmax()]
    worst_day = daily.loc[daily["net_pnl_rupees"].idxmin()]
    lines.extend([
        "",
        f"G-2 net change versus G: **{_fmt_money(float(delta))}**.",
        f"Of G's {g_stop_count} stop exits, {len(avoided)} were avoided: {became_target} later hit their "
        f"unchanged targets and {became_time} reached the 15:15 exit. The remaining "
        f"{g_stop_count - len(avoided)} stopped trades generally realized the wider 1.00% loss.",
        "",
        f"Post-2026-09-11 comparison: G produced {post_g['trades']} trades, "
        f"{post_g['win_rate_pct']:.2f}% wins, PF {_fmt_pf(post_g['profit_factor'])}, and "
        f"{_fmt_money(post_g['net_pnl_rupees'])}; G-2 produced {post_g2['trades']} trades, "
        f"{post_g2['win_rate_pct']:.2f}% wins, PF {_fmt_pf(post_g2['profit_factor'])}, and "
        f"{_fmt_money(post_g2['net_pnl_rupees'])}.",
        "",
        f"Best G-2 session: {pd.Timestamp(best_day['day']).date()}, "
        f"{_fmt_money(float(best_day['net_pnl_rupees']))}. Worst G-2 session: "
        f"{pd.Timestamp(worst_day['day']).date()}, "
        f"{_fmt_money(float(worst_day['net_pnl_rupees']))}.",
        "",
        "## Monthly results",
        "",
        "| Month | Sessions | Selected | Trades | W-L | Win rate | PF | Net P&L |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in breakdowns["monthly"].itertuples(index=False):
        lines.append(
            f"| {row.group} | {row.sessions} | {row.selected_orders} | {row.trades} | "
            f"{row.wins}-{row.losses} | {row.win_rate_pct:.2f}% | {_fmt_pf(row.profit_factor)} | "
            f"{_fmt_money(row.net_pnl_rupees)} |"
        )
    for title, key in (("Side results", "side"), ("Setup results", "setup"), ("Exit results", "exit")):
        lines.extend([
            "",
            f"## {title}",
            "",
            "| Group | Trades | W-L | Win rate | PF | Net P&L |",
            "|---|---:|---:|---:|---:|---:|",
        ])
        for row in breakdowns[key].itertuples(index=False):
            lines.append(
                f"| {row.group} | {row.trades} | {row.wins}-{row.losses} | "
                f"{row.win_rate_pct:.2f}% | {_fmt_pf(row.profit_factor)} | "
                f"{_fmt_money(row.net_pnl_rupees)} |"
            )
    lines.extend([
        "",
        "## Assumptions and evidence",
        "",
        f"- Window: {days[0]} through {days[-1]}, {len(days)} eligible sessions.",
        "- Every stop is 1.00%; each retained G target is unchanged.",
        "- Fixed targets make reward:risk less than 1.0 for 0926_LONG (0.97), "
        "0951_SHORT (0.93) and 0956_LONG (0.90).",
        "- Rs 1,00,000 allocated per filled trade, modeled 5x exposure, Rs 10,00,000 portfolio capital.",
        "- Flat 5 bps modeled round-trip cost, 10-minute entry expiry and 15:15 IST square-off.",
        "- Full exits; no partial exit and no breakeven stop.",
        "- Daily-close drawdown is realized end-of-day drawdown, not intraday mark-to-market drawdown.",
        f"- Evidence: `{EVIDENCE}`. This reused history and is not an untouched holdout.",
        "- The immutable source bundle was verified against every artifact hash before replay.",
        "- No live or paper setting was changed; this output has no execution authority.",
        "",
        "## Artifacts",
        "",
        "- `selected_trades.csv`: order-level G-2 replay.",
        "- `portfolio_trades.csv`: chronological portfolio ledger.",
        "- `summary.json`: engine summary and fixed settings.",
        "- `frozen_config.json`: exact G-2 configuration.",
        "- `daily_results.csv`, `monthly_results.csv`, `side_results.csv`, `setup_results.csv`, `exit_results.csv`.",
        "- `g_vs_g2_comparison.csv` and `stop_comparison.csv`.",
        "- `trade_change_analysis.csv`: every materially changed trade and P&L delta.",
        "- `provenance.json`: input and output hashes.",
        "",
    ])
    report = output / "V13_V10_G_2_FULL_RESULTS.md"
    report.write_text("\n".join(lines), encoding="utf-8")
    return report


def save_run(
    output: Path,
    dataset: dict[str, Any],
    settings: dict[str, Any],
    trades: pd.DataFrame,
    ledger: pd.DataFrame,
    summary: dict[str, Any],
) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    g.r.save(output, trades, ledger, summary)
    dump_json(output / "frozen_config.json", settings)
    breakdowns = build_breakdowns(ledger, dataset["days"])
    report = write_report(output, dataset, ledger, summary, breakdowns)
    outputs = sorted(path for path in output.iterdir() if path.is_file() and path.name != "provenance.json")
    provenance = {
        "schema_version": "V13_V10_G_2_BACKTEST_V1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategy": VERSION,
        "source_bundle": str(dataset["source"]),
        "source_bundle_manifest_sha256": sha256(dataset["source"] / "bundle_manifest.json"),
        "source_g_config": str(dataset["g_config_path"]),
        "source_g_config_sha256": sha256(dataset["g_config_path"]),
        "code": str(Path(__file__).resolve()),
        "code_sha256": sha256(Path(__file__).resolve()),
        "selection_identity_unchanged": True,
        "stop_pct": STOP_PCT,
        "targets_unchanged": True,
        "live_configuration_changed": False,
        "execution_authority": False,
        "artifacts": {
            path.name: {"bytes": path.stat().st_size, "sha256": sha256(path)}
            for path in outputs
        },
    }
    dump_json(output / "provenance.json", provenance)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-bundle", type=Path, default=DEFAULT_SOURCE_BUNDLE)
    parser.add_argument("--source-g-config", type=Path, default=DEFAULT_G_CONFIG)
    parser.add_argument("--config-json", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    dataset = load_bundle(args.source_bundle, args.source_g_config)
    settings = config(dataset["source_g"])
    if args.config_json:
        settings = checked_settings(read_json(args.config_json), dataset["source_g"])
    trades, ledger, summary = evaluate(dataset, settings)
    report = save_run(args.output_dir, dataset, settings, trades, ledger, summary)
    print(json.dumps({
        "metrics": g.r.metric(ledger, dataset["days"]),
        "report": str(report),
        "output_dir": str(args.output_dir),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
