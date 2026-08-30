"""Generate a study-grade report for the locked standalone FNO V11 run.

The report is deliberately read-only with respect to the backtest.  It accepts
only a completed, provenance-validated ``fno_v11_backtest.py`` run, recomputes
the reported economics from the sealed CSV artifacts, and writes a Markdown
study plus supporting tables and charts outside the sealed run directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

import fno_oi_common as common
import fno_v10_full_historical_report as study
import fno_v10_gap_guard_research as gaps
import fno_v10_recent_detailed_report as recent
import fno_v11_backtest as v11
import fno_v11_staged_backtest as staged
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v11_full_historical_study_report_v1"
DEFAULT_LINEAGE_ROOT = (
    common.FNO_ROOT / "strategy_research" / "v11_fno_staged_research_v2"
)
BOOTSTRAP_REPLICATES = 10_000
ORDER_REPLICATES = 5_000
RNG_SEED = 11_110


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _numbers(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _bools(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(False, index=frame.index, dtype=bool)
    return recent._bool_series(frame, column)


def _days(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["session_date"], errors="raise").dt.date


def _safe_div(numerator: float, denominator: float) -> float | None:
    return float(numerator / denominator) if denominator else None


def _streak(values: Iterable[int], target: int) -> int:
    best = current = 0
    for value in values:
        if value == target:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _fmt(value: object, column: str) -> str:
    if value is None or value is pd.NA or pd.isna(value):
        return "—"
    if isinstance(value, (bool, np.bool_)):
        return "Yes" if bool(value) else "No"
    if isinstance(value, (float, np.floating)) and not math.isfinite(float(value)):
        return "∞" if float(value) > 0 else "—"
    if isinstance(value, (str, date, pd.Timestamp)):
        return str(value).replace("|", "\\|")
    name = column.lower()
    if any(word in name for word in ("count", "fills", "wins", "losses", "sessions", "candidates", "selected", "confirmed", "rejections", "observations", "replicates", "days")):
        return f"{int(float(value)):,}"
    if name.endswith("_pct") or "rate_pct" in name or "share_pct" in name or "probability_pct" in name:
        return f"{float(value):.2f}%"
    if "pnl_rs" in name or name.endswith("_rs"):
        return f"{float(value):+,.2f}"
    if "return_points" in name or name.endswith("_points") or "net_points" in name:
        return f"{float(value):+.4f}"
    if "profit_factor" in name or name in {"pf", "payoff_ratio", "recovery_factor"}:
        return f"{float(value):.4f}"
    if "spearman" in name or "pearson" in name:
        return f"{float(value):+.3f}"
    if isinstance(value, (int, np.integer)):
        return f"{int(value):,}"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.4f}"
    return str(value).replace("|", "\\|")


def _table(frame: pd.DataFrame, columns: Sequence[str]) -> list[str]:
    if frame.empty:
        return ["_No rows._"]
    view = frame.copy()
    columns = [column for column in columns if column in view.columns]
    headers = [column.replace("_", " ").title() for column in columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in view.to_dict("records"):
        lines.append(
            "| " + " | ".join(_fmt(row.get(column), column) for column in columns) + " |"
        )
    return lines


def _trade_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    metrics = study._trade_metrics(frame)
    gross = _numbers(frame, "gross_pnl_rs")
    costs = _numbers(frame, "estimated_cost_rs")
    notional = _numbers(frame, "position_notional_rs")
    metrics.update(
        {
            "sessions_with_fills": int(_days(frame).nunique()) if len(frame) else 0,
            "gross_pnl_rs": float(gross.sum()),
            "estimated_cost_rs": float(costs.sum()),
            "position_notional_rs": float(notional.sum()),
        }
    )
    return metrics


def _group_metrics(frame: pd.DataFrame, group: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for key, part in frame.groupby(group, dropna=False, sort=False):
        records.append({group: key, **_trade_metrics(part)})
    return pd.DataFrame(records)


def _add_features(audit: pd.DataFrame) -> pd.DataFrame:
    out = study._add_features(audit)
    side_sign = np.where(out["side"].astype(str).eq("LONG"), 1.0, -1.0)
    five_close = _numbers(out, "five_min_close")
    out["directional_five_min_body_pct"] = (
        (_numbers(out, "five_min_close") - _numbers(out, "five_min_open"))
        * side_sign
        / five_close.where(five_close.ne(0))
        * 100.0
    )
    out["directional_close_ema9_pct"] = (
        (five_close - _numbers(out, "ema9"))
        * side_sign
        / five_close.where(five_close.ne(0))
        * 100.0
    )
    out["confirmation_volume_ratio"] = _numbers(out, "confirmation_volume").div(
        (_numbers(out, "five_min_volume") / 5.0).where(_numbers(out, "five_min_volume").gt(0))
    )
    entry = _numbers(out, "entry_price")
    stop = _numbers(out, "stop_price")
    target = _numbers(out, "target_price")
    risk = (entry - stop).abs().div(entry.where(entry.ne(0))).mul(100.0)
    reward = (target - entry).abs().div(entry.where(entry.ne(0))).mul(100.0)
    out["initial_stop_risk_pct"] = risk
    out["initial_target_reward_pct"] = reward
    out["planned_reward_risk"] = reward.div(risk.where(risk.gt(0)))
    out["gross_r_multiple"] = _numbers(out, "gross_return_pct").div(risk.where(risk.gt(0)))
    out["net_r_multiple"] = _numbers(out, "net_return_pct").div(risk.where(risk.gt(0)))
    out["mfe_lower_r"] = _numbers(out, "mfe_pct_ohlc_lower_bound").div(risk.where(risk.gt(0)))
    out["mfe_upper_r"] = _numbers(out, "mfe_pct_ohlc_upper_bound").div(risk.where(risk.gt(0)))
    out["mae_lower_r"] = _numbers(out, "mae_pct_ohlc_lower_bound").div(risk.where(risk.gt(0)))
    out["mae_upper_r"] = _numbers(out, "mae_pct_ohlc_upper_bound").div(risk.where(risk.gt(0)))
    out["cost_drag_points"] = _numbers(out, "gross_return_pct") - _numbers(out, "net_return_pct")
    mfe_upper = _numbers(out, "mfe_pct_ohlc_upper_bound")
    out["gross_mfe_capture_pct"] = (
        _numbers(out, "gross_return_pct").div(mfe_upper.where(mfe_upper.gt(0))).mul(100.0)
    )
    for source, target_name in (("entry_time", "entry_clock"), ("exit_time", "exit_clock")):
        times = pd.to_datetime(out[source], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
        out[target_name] = times.dt.strftime("%H:%M")
    returns = _numbers(out, "net_return_pct")
    out["outcome"] = np.select([returns.gt(0), returns.lt(0)], ["WIN", "LOSS"], default="FLAT")
    return out


def _prepare_daywise(daywise: pd.DataFrame) -> pd.DataFrame:
    out = daywise.copy()
    out["session_date"] = pd.to_datetime(out["session_date"], errors="raise").dt.date
    out = out.sort_values("session_date", kind="stable").reset_index(drop=True)
    out["net_return_pct"] = _numbers(out, "net_return_pct")
    out["net_pnl_rs"] = _numbers(out, "net_pnl_rs")
    out["cumulative_net_return_points"] = out["net_return_pct"].cumsum()
    out["cumulative_net_pnl_rs"] = out["net_pnl_rs"].cumsum()
    points_peak = np.maximum.accumulate(np.r_[0.0, out["cumulative_net_return_points"].to_numpy()])[1:]
    pnl_peak = np.maximum.accumulate(np.r_[0.0, out["cumulative_net_pnl_rs"].to_numpy()])[1:]
    out["drawdown_return_points"] = out["cumulative_net_return_points"] - points_peak
    out["drawdown_pnl_rs"] = out["cumulative_net_pnl_rs"] - pnl_peak
    dates = pd.to_datetime(out["session_date"])
    iso = dates.dt.isocalendar()
    out["month"] = dates.dt.strftime("%Y-%m")
    out["week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    out["weekday"] = dates.dt.day_name()
    return out


def _daily_detail(
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
    daywise: pd.DataFrame,
) -> pd.DataFrame:
    records = [
        {
            "session_date": session,
            **{
                key: value
                for key, value in study._period_funnel(
                    decisions, audit, closed, [session], str(session)
                ).items()
                if key != "period"
            },
        }
        for session in daywise["session_date"]
    ]
    detail = pd.DataFrame(records)
    return detail.merge(
        daywise[
            [
                "session_date",
                "cumulative_net_return_points",
                "cumulative_net_pnl_rs",
                "drawdown_return_points",
                "drawdown_pnl_rs",
            ]
        ],
        on="session_date",
        how="left",
        validate="one_to_one",
    )


def _indicator_bins(audit: pd.DataFrame) -> pd.DataFrame:
    specifications = (
        ("directional_move_pct", [-math.inf, 0.3, 0.5, 0.75, 1, 1.5, math.inf], ["<0.30", "0.30–0.50", "0.50–0.75", "0.75–1.00", "1.00–1.50", "1.50+"]),
        ("oi_change_pct", [-math.inf, 0.1, 0.5, 1, 2, 5, math.inf], ["<0.10", "0.10–0.50", "0.50–1.00", "1.00–2.00", "2.00–5.00", "5.00+"]),
        ("volume_ratio", [-math.inf, 1, 1.5, 2, 3, 5, math.inf], ["<1.00", "1.00–1.50", "1.50–2.00", "2.00–3.00", "3.00–5.00", "5.00+"]),
        ("traded_value_cr", [-math.inf, 2.5, 5, 10, 25, 50, math.inf], ["<2.5cr", "2.5–5cr", "5–10cr", "10–25cr", "25–50cr", "50cr+"]),
        ("five_min_range_pct", [-math.inf, 0.3, 0.5, 0.75, 1, 1.5, math.inf], ["<0.30", "0.30–0.50", "0.50–0.75", "0.75–1.00", "1.00–1.50", "1.50+"]),
        ("five_min_body_ratio", [-math.inf, 0.2, 0.4, 0.6, 0.8, math.inf], ["<0.20", "0.20–0.40", "0.40–0.60", "0.60–0.80", "0.80+"]),
        ("five_min_adverse_wick_ratio", [-math.inf, 0.1, 0.2, 0.3, 0.4, 0.5, math.inf], ["<0.10", "0.10–0.20", "0.20–0.30", "0.30–0.40", "0.40–0.50", "0.50+"]),
        ("five_min_directional_close_location", [-math.inf, 0.5, 0.6, 0.75, 0.9, math.inf], ["<0.50", "0.50–0.60", "0.60–0.75", "0.75–0.90", "0.90+"]),
        ("ema_total_gap_pct", [-math.inf, 0.1, 0.25, 0.5, 1, math.inf], ["<0.10", "0.10–0.25", "0.25–0.50", "0.50–1.00", "1.00+"]),
        ("confirmation_volume_ratio", [-math.inf, 0.5, 0.75, 1, 1.5, 2, math.inf], ["<0.50", "0.50–0.75", "0.75–1.00", "1.00–1.50", "1.50–2.00", "2.00+"]),
        ("confirmation_body_ratio", [-math.inf, 0.4, 0.5, 0.6, 0.75, math.inf], ["<0.40", "0.40–0.50", "0.50–0.60", "0.60–0.75", "0.75+"]),
        ("confirmation_adverse_wick_ratio", [-math.inf, 0.1, 0.2, 0.3, 0.4, 0.5, math.inf], ["<0.10", "0.10–0.20", "0.20–0.30", "0.30–0.40", "0.40–0.50", "0.50+"]),
        ("confirmation_close_location", [-math.inf, 0.5, 0.6, 0.75, 0.9, math.inf], ["<0.50", "0.50–0.60", "0.60–0.75", "0.75–0.90", "0.90+"]),
        ("trigger_distance_c5_bps", [-math.inf, 0, 10, 20, 30, 50, 100, math.inf], ["<0", "0–10", "10–20", "20–30", "30–50", "50–100", "100+"]),
        ("entry_delay_minutes", [-math.inf, 1, 2, 3, 4, math.inf], ["<1", "1–2", "2–3", "3–4", "4+"]),
    )
    frames = [
        study._binned_funnel(audit, indicator=name, bins=bins, labels=labels)
        for name, bins, labels in specifications
    ]
    return pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)


def _quartile_analysis(audit: pd.DataFrame, indicators: Sequence[str]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for indicator in indicators:
        work = audit.loc[_numbers(audit, indicator).notna()].copy()
        if len(work) < 20 or _numbers(work, indicator).nunique() < 4:
            continue
        work["quartile"] = pd.qcut(_numbers(work, indicator), 4, duplicates="drop")
        for index, (interval, part) in enumerate(work.groupby("quartile", observed=True, sort=True), start=1):
            trades = part.loc[_bools(part, "filled")]
            metrics = study._trade_metrics(trades)
            records.append(
                {
                    "indicator": indicator,
                    "quartile": f"Q{index}",
                    "observed_range": str(interval),
                    "selected": len(part),
                    "confirmed": int(_numbers(part, "confirmation_minute").notna().sum()),
                    "fills": len(trades),
                    "fill_rate_pct": float(_bools(part, "filled").mean() * 100.0),
                    **{key: metrics[key] for key in ("wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs")},
                }
            )
    return pd.DataFrame(records)


def _winner_loser_table(closed: pd.DataFrame, indicators: Sequence[str]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    winners = closed.loc[_numbers(closed, "net_return_pct").gt(0)]
    losers = closed.loc[_numbers(closed, "net_return_pct").lt(0)]
    for indicator in indicators:
        win = _numbers(winners, indicator).dropna()
        loss = _numbers(losers, indicator).dropna()
        if win.empty or loss.empty:
            continue
        records.append(
            {
                "indicator": indicator,
                "winner_observations": len(win),
                "loser_observations": len(loss),
                "winner_median": float(win.median()),
                "loser_median": float(loss.median()),
                "median_delta": float(win.median() - loss.median()),
                "winner_mean": float(win.mean()),
                "loser_mean": float(loss.mean()),
            }
        )
    return pd.DataFrame(records)


def _correlations(closed: pd.DataFrame, indicators: Sequence[str]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for indicator in indicators:
        pair = pd.DataFrame(
            {"x": _numbers(closed, indicator), "y": _numbers(closed, "net_return_pct")}
        ).dropna()
        if len(pair) < 10 or pair["x"].nunique() < 3:
            continue
        records.append(
            {
                "indicator": indicator,
                "observations": len(pair),
                "spearman_vs_net_return": float(pair["x"].corr(pair["y"], method="spearman")),
                "pearson_vs_net_return": float(pair["x"].corr(pair["y"], method="pearson")),
            }
        )
    result = pd.DataFrame(records)
    if result.empty:
        return result
    return result.sort_values(
        "spearman_vs_net_return", key=lambda values: values.abs(), ascending=False
    ).reset_index(drop=True)


def _binary_indicator_tests(
    frame: pd.DataFrame,
    indicators: Sequence[str],
    *,
    positive: pd.Series,
    negative: pd.Series,
    comparison: str,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for indicator in indicators:
        positive_values = _numbers(frame.loc[positive], indicator).dropna()
        negative_values = _numbers(frame.loc[negative], indicator).dropna()
        if len(positive_values) < 5 or len(negative_values) < 5:
            continue
        test = mannwhitneyu(
            positive_values, negative_values, alternative="two-sided", method="asymptotic"
        )
        records.append(
            {
                "comparison": comparison,
                "indicator": indicator,
                "positive_observations": len(positive_values),
                "negative_observations": len(negative_values),
                "positive_median": float(positive_values.median()),
                "negative_median": float(negative_values.median()),
                "median_delta": float(positive_values.median() - negative_values.median()),
                "auc_positive_higher": float(
                    test.statistic / (len(positive_values) * len(negative_values))
                ),
                "p_value_two_sided": float(test.pvalue),
            }
        )
    result = pd.DataFrame(records)
    if result.empty:
        return result
    order = result["p_value_two_sided"].sort_values().index
    ranked = result.loc[order, "p_value_two_sided"].to_numpy(float)
    adjusted = ranked * len(ranked) / np.arange(1, len(ranked) + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1].clip(max=1.0)
    result.loc[order, "bh_q_value"] = adjusted
    return result.sort_values(["bh_q_value", "p_value_two_sided"]).reset_index(drop=True)


def _wilson_interval(successes: int, total: int, z: float = 1.95996398454) -> tuple[float, float]:
    if total <= 0:
        return (math.nan, math.nan)
    p = successes / total
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    radius = z * math.sqrt(p * (1.0 - p) / total + z * z / (4.0 * total * total)) / denominator
    return ((center - radius) * 100.0, (center + radius) * 100.0)


def _bootstrap_scenarios(run_dir: Path) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for scenario, _, _ in v11.EXPECTED_SCENARIOS:
        folder = scenario.lower()
        trades = pd.read_csv(run_dir / "scenarios" / folder / "closed_trades.csv", low_memory=False)
        daily = pd.read_csv(run_dir / "scenarios" / folder / "daywise.csv")
        daily["session_date"] = pd.to_datetime(daily["session_date"]).dt.date
        trade_days = _days(trades)
        gains = _numbers(trades, "net_return_pct").where(_numbers(trades, "net_return_pct").gt(0), 0).groupby(trade_days).sum()
        losses = (-_numbers(trades, "net_return_pct").where(_numbers(trades, "net_return_pct").lt(0), 0)).groupby(trade_days).sum()
        sessions = daily["session_date"].tolist()
        gain_values = gains.reindex(sessions, fill_value=0.0).to_numpy(float)
        loss_values = losses.reindex(sessions, fill_value=0.0).to_numpy(float)
        pnl_values = _numbers(daily, "net_pnl_rs").to_numpy(float)
        point_values = _numbers(daily, "net_return_pct").to_numpy(float)
        rng = np.random.default_rng(RNG_SEED + sum(ord(char) for char in scenario))
        indices = rng.integers(0, len(sessions), size=(BOOTSTRAP_REPLICATES, len(sessions)))
        sample_pnl = pnl_values[indices].sum(axis=1)
        sample_points = point_values[indices].sum(axis=1)
        sample_gains = gain_values[indices].sum(axis=1)
        sample_losses = loss_values[indices].sum(axis=1)
        sample_pf = np.divide(sample_gains, sample_losses, out=np.full_like(sample_gains, np.nan), where=sample_losses > 0)
        records.append(
            {
                "scenario": scenario,
                "bootstrap_unit": "SESSION_WITH_REPLACEMENT",
                "bootstrap_replicates": BOOTSTRAP_REPLICATES,
                "probability_positive_total_pnl_pct": float(np.mean(sample_pnl > 0) * 100.0),
                "total_pnl_rs_p025": float(np.quantile(sample_pnl, 0.025)),
                "total_pnl_rs_median": float(np.quantile(sample_pnl, 0.5)),
                "total_pnl_rs_p975": float(np.quantile(sample_pnl, 0.975)),
                "net_points_p025": float(np.quantile(sample_points, 0.025)),
                "net_points_median": float(np.quantile(sample_points, 0.5)),
                "net_points_p975": float(np.quantile(sample_points, 0.975)),
                "pf_p025": float(np.nanquantile(sample_pf, 0.025)),
                "pf_median": float(np.nanquantile(sample_pf, 0.5)),
                "pf_p975": float(np.nanquantile(sample_pf, 0.975)),
            }
        )
    return pd.DataFrame(records)


def _order_drawdown(daywise: pd.DataFrame) -> pd.DataFrame:
    values = _numbers(daywise, "net_pnl_rs").to_numpy(float)
    rng = np.random.default_rng(RNG_SEED + 91)
    depths = np.empty(ORDER_REPLICATES, dtype=float)
    for index in range(ORDER_REPLICATES):
        cumulative = np.cumsum(rng.permutation(values))
        peaks = np.maximum.accumulate(np.r_[0.0, cumulative])[1:]
        depths[index] = float(np.max(peaks - cumulative))
    return pd.DataFrame(
        [
            {
                "method": "RANDOM_SESSION_ORDER",
                "replicates": ORDER_REPLICATES,
                "observed_mdd_pnl_rs": float(-_numbers(daywise, "drawdown_pnl_rs").min()),
                "mdd_pnl_rs_p50": float(np.quantile(depths, 0.50)),
                "mdd_pnl_rs_p75": float(np.quantile(depths, 0.75)),
                "mdd_pnl_rs_p90": float(np.quantile(depths, 0.90)),
                "mdd_pnl_rs_p95": float(np.quantile(depths, 0.95)),
                "mdd_pnl_rs_p975": float(np.quantile(depths, 0.975)),
            }
        ]
    )


def _drawdown_episodes(daywise: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    active: list[int] = []
    for index, drawdown in enumerate(_numbers(daywise, "drawdown_pnl_rs")):
        if drawdown < -1e-9:
            active.append(index)
            continue
        if active:
            trough = min(active, key=lambda value: float(daywise.loc[value, "drawdown_pnl_rs"]))
            records.append(
                {
                    "start_session": daywise.loc[max(active[0] - 1, 0), "session_date"],
                    "trough_session": daywise.loc[trough, "session_date"],
                    "recovery_session": daywise.loc[index, "session_date"],
                    "underwater_sessions": len(active),
                    "depth_pnl_rs": float(-daywise.loc[trough, "drawdown_pnl_rs"]),
                    "depth_return_points": float(-daywise.loc[trough, "drawdown_return_points"]),
                    "recovered": True,
                }
            )
            active = []
    if active:
        trough = min(active, key=lambda value: float(daywise.loc[value, "drawdown_pnl_rs"]))
        records.append(
            {
                "start_session": daywise.loc[max(active[0] - 1, 0), "session_date"],
                "trough_session": daywise.loc[trough, "session_date"],
                "recovery_session": None,
                "underwater_sessions": len(active),
                "depth_pnl_rs": float(-daywise.loc[trough, "drawdown_pnl_rs"]),
                "depth_return_points": float(-daywise.loc[trough, "drawdown_return_points"]),
                "recovered": False,
            }
        )
    result = pd.DataFrame(records)
    return result.sort_values("depth_pnl_rs", ascending=False).reset_index(drop=True) if not result.empty else result


def _daily_regimes(audit: pd.DataFrame, closed: pd.DataFrame, daywise: pd.DataFrame) -> pd.DataFrame:
    selected = audit.assign(session_date=_days(audit)).groupby("session_date").agg(
        selected_candidates=("candidate_id", "size"),
        median_range_pct=("five_min_range_pct", "median"),
        median_directional_move_pct=("directional_move_pct", "median"),
        long_share_pct=("side", lambda values: float(values.astype(str).eq("LONG").mean() * 100.0)),
    ).reset_index()
    work = daywise[["session_date", "net_pnl_rs", "net_return_pct", "fills"]].merge(selected, on="session_date", how="left")
    records: list[dict[str, Any]] = []
    for measure, label in (("selected_candidates", "candidate_activity"), ("median_range_pct", "five_min_range"), ("long_share_pct", "long_share")):
        if work[measure].nunique() < 3:
            continue
        bucket = pd.qcut(work[measure], 3, labels=["LOW", "MID", "HIGH"], duplicates="drop")
        temp = work.assign(regime=bucket)
        for regime, part in temp.groupby("regime", observed=True, sort=True):
            records.append(
                {
                    "regime_dimension": label,
                    "regime": str(regime),
                    "sessions": len(part),
                    "measure_min": float(part[measure].min()),
                    "measure_median": float(part[measure].median()),
                    "measure_max": float(part[measure].max()),
                    "fills": int(_numbers(part, "fills").sum()),
                    "positive_days": int(_numbers(part, "net_pnl_rs").gt(0).sum()),
                    "negative_days": int(_numbers(part, "net_pnl_rs").lt(0).sum()),
                    "net_return_points": float(_numbers(part, "net_return_pct").sum()),
                    "net_pnl_rs": float(_numbers(part, "net_pnl_rs").sum()),
                    "average_daily_pnl_rs": float(_numbers(part, "net_pnl_rs").mean()),
                }
            )
    return pd.DataFrame(records)


def _plots(
    assets_dir: Path,
    daywise: pd.DataFrame,
    setup_metrics: pd.DataFrame,
    monthly: pd.DataFrame,
    scenario_metrics: pd.DataFrame,
    funnel: Mapping[str, int],
) -> list[Path]:
    plt.style.use("seaborn-v0_8-whitegrid")
    paths: list[Path] = []

    path = assets_dir / "equity_and_drawdown.png"
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    axes[0].plot(daywise["session_date"], daywise["cumulative_net_pnl_rs"], color="#1769aa", linewidth=2)
    axes[0].axhline(0, color="#555555", linewidth=0.8)
    axes[0].set_ylabel("Cumulative net P&L (Rs)")
    axes[0].set_title("V11 Stage 10 reference scenario")
    axes[1].fill_between(daywise["session_date"], daywise["drawdown_pnl_rs"], 0, color="#c62828", alpha=0.35)
    axes[1].set_ylabel("Drawdown (Rs)")
    axes[1].set_xlabel("Session")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)
    paths.append(path)

    path = assets_dir / "setup_net_pnl.png"
    ordered = setup_metrics.sort_values("net_pnl_rs")
    colors = ["#2e7d32" if value >= 0 else "#c62828" for value in ordered["net_pnl_rs"]]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(ordered["setup_id"], ordered["net_pnl_rs"], color=colors)
    ax.axvline(0, color="#333333", linewidth=0.8)
    ax.set_xlabel("Net P&L (Rs)")
    ax.set_title("Net contribution by setup")
    for bar, fills in zip(bars, ordered["fills"], strict=True):
        ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2, f" {int(fills)} fills", va="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)
    paths.append(path)

    path = assets_dir / "monthly_net_pnl.png"
    colors = ["#2e7d32" if value >= 0 else "#c62828" for value in monthly["net_pnl_rs"]]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(monthly["period"], monthly["net_pnl_rs"], color=colors)
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.set_ylabel("Net P&L (Rs)")
    ax.set_title("Monthly stability; May and August are partial")
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)
    paths.append(path)

    path = assets_dir / "cost_scenarios.png"
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(scenario_metrics["scenario"], scenario_metrics["net_pnl_rs"], color=["#2e7d32", "#ef6c00", "#c62828"])
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.set_ylabel("Net P&L (Rs)")
    ax.set_title("Cost and slippage sensitivity")
    ax.tick_params(axis="x", rotation=15)
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)
    paths.append(path)

    path = assets_dir / "selection_funnel.png"
    labels = list(funnel)
    values = [funnel[label] for label in labels]
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, values, color=["#455a64", "#1976d2", "#7b1fa2", "#ef6c00", "#2e7d32"])
    ax.set_ylabel("Candidates / trades")
    ax.set_title("5-minute selection to closed winners")
    ax.tick_params(axis="x", rotation=18)
    for bar, value in zip(bars, values, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, value, str(value), ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)
    paths.append(path)
    return paths


def _resolve_latest(root: Path) -> Path | None:
    latest = root / "latest.json"
    if not latest.is_file():
        return None
    payload = json.loads(latest.read_text(encoding="utf-8"))
    run_dir = Path(str(payload.get("run_dir", ""))).expanduser().resolve()
    return run_dir if run_dir.is_dir() else None


def _load_lineage(lineage_run: Path | None, provenance: Mapping[str, Any], headline: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Mapping[str, Any] | None]:
    if lineage_run is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None
    run_dir = lineage_run.expanduser().resolve()
    payload = json.loads((run_dir / "provenance.json").read_text(encoding="utf-8"))
    if payload.get("schema_version") != staged.SCHEMA_VERSION or not payload.get("complete"):
        raise AssertionError("V11 staged lineage run is not complete or has the wrong schema")
    if payload.get("input_binding_sha256") != provenance.get("input_binding_sha256"):
        raise AssertionError("V11 staged lineage input binding differs from standalone V11")
    if not bool(payload.get("determinism_attested")):
        raise AssertionError("V11 staged lineage lacks determinism attestation")
    staged._validate_completed_run_artifact_inventory(run_dir, payload)
    metrics = pd.read_csv(run_dir / "all_period_metrics.csv")
    identifiers = [
        "V10_STAGE0_FROZEN_CONTROL",
        "V11_STAGE3_DETERMINISTIC_GAP_REBASELINE",
        "V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3",
        "V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2",
        v11.PROFILE_ID,
    ]
    comparison = metrics.loc[
        metrics["period"].eq("FULL_USABLE") & metrics["variant_id"].isin(identifiers)
    ].copy()
    observed = comparison.loc[
        comparison["variant_id"].eq(v11.PROFILE_ID)
        & comparison["scenario"].eq("REFERENCE_15_0")
    ].iloc[0]
    for field in ("fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"):
        if not math.isclose(float(observed[field]), float(headline[field]), rel_tol=0.0, abs_tol=1e-9):
            raise AssertionError(f"staged lineage does not reproduce standalone V11: {field}")
    all_daywise = pd.read_csv(run_dir / "all_daywise.csv")
    subset = all_daywise.loc[
        all_daywise["period"].eq("FULL")
        & all_daywise["scenario"].eq("REFERENCE_15_0")
        & all_daywise["variant_id"].isin(["V10_STAGE0_FROZEN_CONTROL", v11.PROFILE_ID])
    ].copy()
    pivot = subset.pivot(index="session_date", columns="variant_id", values=["net_return_pct", "net_pnl_rs", "fills"])
    pivot.columns = [f"{metric}__{variant}" for metric, variant in pivot.columns]
    pivot = pivot.reset_index()
    for metric in ("net_return_pct", "net_pnl_rs", "fills"):
        pivot[f"delta_{metric}"] = pivot[f"{metric}__{v11.PROFILE_ID}"] - pivot[f"{metric}__V10_STAGE0_FROZEN_CONTROL"]
    pivot = pivot.rename(columns={"delta_net_return_pct": "delta_net_return_points"})
    pivot["cumulative_delta_net_pnl_rs"] = pivot["delta_net_pnl_rs"].cumsum()
    gates = pd.read_csv(run_dir / "development_gates.csv")
    gates = gates.loc[gates["variant_id"].isin(identifiers)].copy()
    return comparison, pivot, gates, payload


def _scenario_group_metrics(run_dir: Path, group: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for scenario, _, _ in v11.EXPECTED_SCENARIOS:
        trades = _add_features(
            pd.read_csv(
                run_dir / "scenarios" / scenario.lower() / "closed_trades.csv",
                low_memory=False,
            )
        )
        part = _group_metrics(trades, group)
        part.insert(0, "scenario", scenario)
        frames.append(part)
    return pd.concat(frames, ignore_index=True)


def _component_attribution(
    lineage: Mapping[str, Any] | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if lineage is None:
        return pd.DataFrame(), pd.DataFrame()
    experiments = dict(lineage.get("experiments", {}))
    required = {
        "stage4": "V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3",
        "stage9": "V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2",
        "stage10": v11.PROFILE_ID,
    }
    records: list[dict[str, Any]] = []
    reference_details: list[pd.DataFrame] = []
    for scenario, _, _ in v11.EXPECTED_SCENARIOS:
        trades: dict[str, pd.DataFrame] = {}
        for label, variant in required.items():
            artifact = experiments[variant]["artifacts"][scenario]["closed_trades"]
            trades[label] = pd.read_csv(Path(artifact), low_memory=False)
        stage4_ids = set(trades["stage4"]["candidate_id"].astype(str))
        stage9_ids = set(trades["stage9"]["candidate_id"].astype(str))
        stage10_ids = set(trades["stage10"]["candidate_id"].astype(str))
        slices = {
            "CAP2_ADDED_VS_DELAY_ONLY": trades["stage10"].loc[
                trades["stage10"]["candidate_id"].astype(str).isin(stage10_ids - stage4_ids)
            ],
            "S3_DELAY_REMOVED_VS_CAP2_ONLY": trades["stage9"].loc[
                trades["stage9"]["candidate_id"].astype(str).isin(stage9_ids - stage10_ids)
            ],
        }
        for effect, frame in slices.items():
            metrics = study._trade_metrics(frame)
            records.append(
                {
                    "scenario": scenario,
                    "component_effect": effect,
                    **{key: metrics[key] for key in ("fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs")},
                }
            )
            if scenario == "REFERENCE_15_0":
                reference_details.append(
                    frame.assign(component_effect=effect)[
                        [
                            "component_effect",
                            "candidate_id",
                            "session_date",
                            "setup_id",
                            "side",
                            "symbol",
                            "entry_time",
                            "exit_time",
                            "exit_reason",
                            "net_return_pct",
                            "net_pnl_rs",
                        ]
                    ]
                )
    details = pd.concat(reference_details, ignore_index=True) if reference_details else pd.DataFrame()
    return pd.DataFrame(records), details


def _scenario_transitions(run_dir: Path) -> pd.DataFrame:
    outcomes: dict[str, pd.DataFrame] = {}
    for scenario, _, _ in v11.EXPECTED_SCENARIOS:
        frame = pd.read_csv(
            run_dir / "scenarios" / scenario.lower() / "closed_trades.csv",
            low_memory=False,
        )
        returns = _numbers(frame, "net_return_pct")
        outcomes[scenario] = pd.DataFrame(
            {
                "candidate_id": frame["candidate_id"].astype(str),
                scenario: np.select(
                    [returns.gt(0), returns.lt(0)], ["WIN", "LOSS"], default="FLAT"
                ),
            }
        )
    merged = outcomes["REFERENCE_15_0"]
    for scenario in ("STRESS_20_2", "STRESS_25_5"):
        merged = merged.merge(outcomes[scenario], on="candidate_id", how="outer", validate="one_to_one")
    return (
        merged.groupby(list(outcomes), dropna=False)
        .size()
        .rename("trades")
        .reset_index()
        .sort_values("trades", ascending=False)
        .reset_index(drop=True)
    )


def _source_segment_table(provenance: Mapping[str, Any]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for item in provenance.get("source_segments", []):
        segment = dict(item.get("segment", {}))
        universe = dict(item.get("universe", {}))
        expected = int(item.get("expected_symbol_sessions", 0))
        incomplete = int(item.get("source_incomplete_symbol_sessions", 0))
        records.append(
            {
                "segment_id": segment.get("segment_id"),
                "from_day": segment.get("from_day"),
                "through_day": segment.get("through_day"),
                "contract_month": segment.get("contract_month_filter"),
                "universe_master_date": universe.get("master_date"),
                "mapped_stock_futures": universe.get("mapped_stock_futures"),
                "sessions": item.get("session_count"),
                "candidates": item.get("candidate_count"),
                "minute_path_rows": item.get("minute_path_rows"),
                "expected_symbol_sessions": expected,
                "source_incomplete_symbol_sessions": incomplete,
                "source_incomplete_pct": incomplete / expected * 100.0 if expected else math.nan,
                "headline_source_complete": item.get("headline_source_complete"),
            }
        )
    return pd.DataFrame(records)


def _portfolio_timeline(closed: pd.DataFrame, sessions: Sequence[date]) -> pd.DataFrame:
    work = closed.copy()
    work["_entry"] = pd.to_datetime(work["entry_time"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    work["_exit"] = pd.to_datetime(work["exit_time"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    work["_notional"] = _numbers(work, "position_notional_rs")
    records: list[dict[str, Any]] = []
    work_days = _days(work)
    for session in sessions:
        part = work.loc[work_days.eq(session)]
        minute_index = pd.date_range(
            f"{session} 09:16", f"{session} 15:30", freq="1min", tz="Asia/Kolkata"
        )
        for timestamp in minute_index:
            active = part.loc[part["_entry"].le(timestamp) & part["_exit"].gt(timestamp)]
            records.append(
                {
                    "timestamp": timestamp,
                    "session_date": session,
                    "open_positions": len(active),
                    "deployed_notional_rs": float(active["_notional"].sum()),
                }
            )
    return pd.DataFrame(records)


def _write_frames(assets_dir: Path, frames: Mapping[str, pd.DataFrame]) -> list[Path]:
    outputs: list[Path] = []
    for name, frame in frames.items():
        path = assets_dir / name
        common.atomic_write_csv(frame, path)
        outputs.append(path)
    return outputs


def build_report(
    *,
    source_run: Path,
    report_path: Path,
    assets_dir: Path,
    lineage_run: Path | None,
) -> dict[str, Any]:
    source_run = source_run.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    assets_dir = assets_dir.expanduser().resolve()
    if report_path.is_relative_to(source_run) or assets_dir.is_relative_to(source_run):
        raise ValueError("study outputs must remain outside the sealed V11 run")
    assets_dir.mkdir(parents=True, exist_ok=True)

    provenance_path = source_run / "provenance.json"
    validation = v11.validate_run_provenance(provenance_path)
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    benchmark = json.loads((source_run / "benchmark_verification.json").read_text(encoding="utf-8"))
    if not bool(benchmark.get("verified")):
        raise AssertionError("standalone V11 benchmark verification is false")

    decisions = pd.read_csv(source_run / "selection_decisions.csv", low_memory=False)
    selected = pd.read_csv(source_run / "selected_candidates.csv", low_memory=False)
    audit_raw = pd.read_csv(
        source_run / "scenarios" / "reference_15_0" / "candidate_order_audit.csv",
        low_memory=False,
    )
    closed_file = pd.read_csv(
        source_run / "scenarios" / "reference_15_0" / "closed_trades.csv",
        low_memory=False,
    )
    daywise = _prepare_daywise(
        pd.read_csv(source_run / "scenarios" / "reference_15_0" / "daywise.csv")
    )
    recent._validate_source_tables(decisions, selected, audit_raw, closed_file)
    audit = _add_features(audit_raw)
    closed = audit.loc[_bools(audit, "filled")].copy()
    if set(closed["candidate_id"].astype(str)) != set(closed_file["candidate_id"].astype(str)):
        raise AssertionError("filled audit IDs differ from closed-trade artifact")
    expected_fingerprint = provenance["closed_trade_economic_fingerprints"]["REFERENCE_15_0"]
    if v11._closed_trade_economic_fingerprint(closed_file) != expected_fingerprint:
        raise AssertionError("reference closed-trade fingerprint drifted")

    headline = _trade_metrics(closed)
    expected = benchmark["benchmarks"]["REFERENCE_15_0"]["observed"]
    for field in (
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
    ):
        if not math.isclose(float(headline[field]), float(expected[field]), rel_tol=0.0, abs_tol=1e-9):
            raise AssertionError(f"report reconciliation failed: {field}")

    sessions = daywise["session_date"].tolist()
    expected_span = engine.expected_regular_session_dates(min(sessions), max(sessions))
    missing_sessions = sorted(set(expected_span) - set(sessions))
    if [day.isoformat() for day in missing_sessions] != provenance["missing_regular_session_dates"]:
        raise AssertionError("report calendar gap differs from provenance")

    setup_parameters = recent._setup_parameter_table()
    setup_metrics = study._funnel_by(decisions, audit, closed, "setup_id")
    setup_metrics = setup_metrics.merge(
        setup_parameters[["setup_id", "max_entries", "picker"]],
        on="setup_id",
        how="left",
        validate="one_to_one",
    ).sort_values("setup_id", kind="stable").reset_index(drop=True)
    side_metrics = study._funnel_by(decisions, audit, closed, "side")
    slot_metrics = study._funnel_by(decisions, audit, closed, "signal_end").sort_values("signal_end", kind="stable")
    picker_metrics = study._funnel_by(decisions, audit, closed, "picker")

    audit_rank = _numbers(audit, "frozen_rank")
    audit["rank_bucket"] = np.select(
        [audit_rank.eq(1), audit_rank.eq(2), audit_rank.eq(3), audit_rank.eq(4), audit_rank.eq(5)],
        ["1", "2", "3", "4", "5"],
        default="6+",
    )
    closed = audit.loc[_bools(audit, "filled")].copy()
    rank_metrics = _group_metrics(closed, "rank_bucket")
    rank_funnel = (
        audit.groupby("rank_bucket", as_index=False, sort=False)
        .agg(
            selected=("candidate_id", "size"),
            confirmed=("confirmation_minute", lambda values: pd.to_numeric(values, errors="coerce").notna().sum()),
            fills=("filled", lambda values: recent._bool_series(pd.DataFrame({"filled": values}), "filled").sum()),
        )
        .merge(rank_metrics, on=["rank_bucket", "fills"], how="left")
    )

    month_labels = {session: session.strftime("%Y-%m") for session in sessions}
    week_labels = {session: f"{session.isocalendar().year}-W{session.isocalendar().week:02d}" for session in sessions}
    weekday_labels = {session: session.strftime("%A") for session in sessions}
    monthly = study._calendar_group_table(decisions, audit, closed, sessions, month_labels)
    weekly = study._calendar_group_table(decisions, audit, closed, sessions, week_labels)
    weekday = study._calendar_group_table(decisions, audit, closed, sessions, weekday_labels)
    weekday_order = {name: index for index, name in enumerate(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])}
    weekday = weekday.assign(_order=weekday["period"].map(weekday_order)).sort_values("_order").drop(columns="_order")
    blocks = study._stability_blocks(decisions, audit, closed, sessions)
    rolling = study._rolling_table(daywise, closed, 10)
    periods = pd.DataFrame(
        [
            study._period_funnel(decisions, audit, closed, sessions, "FULL_65"),
            study._period_funnel(decisions, audit, closed, [day for day in sessions if day < date(2026, 8, 20)], "CORE_59"),
            study._period_funnel(decisions, audit, closed, [day for day in sessions if day >= date(2026, 8, 20)], "FORWARD_6"),
            study._period_funnel(decisions, audit, closed, sessions[:32], "FIRST_HALF_32"),
            study._period_funnel(decisions, audit, closed, sessions[32:], "SECOND_HALF_33"),
            study._period_funnel(decisions, audit, closed, sessions[-14:], "LAST_14_USABLE"),
        ]
    )
    daily_detail = _daily_detail(decisions, audit, closed, daywise)

    all_period_metrics = pd.read_csv(source_run / "all_period_metrics.csv")
    scenario_metrics = all_period_metrics.loc[all_period_metrics["period"].eq("FULL_USABLE")].copy()
    scenario_metrics = scenario_metrics.set_index("scenario").loc[[name for name, _, _ in v11.EXPECTED_SCENARIOS]].reset_index()
    reference_net = float(scenario_metrics.loc[scenario_metrics["scenario"].eq("REFERENCE_15_0"), "net_pnl_rs"].iloc[0])
    scenario_metrics["net_pnl_change_vs_reference_rs"] = _numbers(scenario_metrics, "net_pnl_rs") - reference_net
    scenario_metrics["net_pnl_retained_vs_reference_pct"] = _numbers(scenario_metrics, "net_pnl_rs").div(reference_net).mul(100.0)
    scenario_setup = _scenario_group_metrics(source_run, "setup_id")
    scenario_side = _scenario_group_metrics(source_run, "side")
    reference_setup = scenario_setup.loc[scenario_setup["scenario"].eq("REFERENCE_15_0")]
    harsh_setup = scenario_setup.loc[scenario_setup["scenario"].eq("STRESS_25_5")]
    setup_robustness = reference_setup[
        ["setup_id", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]
    ].merge(
        harsh_setup[["setup_id", "profit_factor", "net_return_points", "net_pnl_rs"]],
        on="setup_id",
        suffixes=("_reference", "_harsh"),
        validate="one_to_one",
    ).rename(columns={"fills": "fills_reference"}).sort_values("setup_id")
    scenario_confirmation = _scenario_group_metrics(source_run, "confirmation_minute")
    scenario_entry = _scenario_group_metrics(source_run, "entry_minute")
    scenario_exit = _scenario_group_metrics(source_run, "exit_reason")
    scenario_confirmation["confirmation_minute"] = _numbers(
        scenario_confirmation, "confirmation_minute"
    ).astype("Int64").astype(str)
    scenario_entry["entry_minute"] = _numbers(scenario_entry, "entry_minute").astype(
        "Int64"
    ).astype(str)
    confirmation_robustness = scenario_confirmation.loc[
        scenario_confirmation["scenario"].eq("REFERENCE_15_0")
    ][
        [
            "confirmation_minute",
            "fills",
            "wins",
            "losses",
            "win_rate_pct",
            "profit_factor",
            "net_return_points",
            "net_pnl_rs",
        ]
    ].merge(
        scenario_confirmation.loc[
            scenario_confirmation["scenario"].eq("STRESS_25_5")
        ][["confirmation_minute", "profit_factor", "net_return_points", "net_pnl_rs"]],
        on="confirmation_minute",
        suffixes=("_reference", "_harsh"),
        validate="one_to_one",
    ).rename(columns={"fills": "fills_reference"}).sort_values("confirmation_minute")
    exit_robustness = scenario_exit.loc[
        scenario_exit["scenario"].eq("REFERENCE_15_0")
    ][
        [
            "exit_reason",
            "fills",
            "wins",
            "losses",
            "profit_factor",
            "net_return_points",
            "net_pnl_rs",
        ]
    ].merge(
        scenario_exit.loc[scenario_exit["scenario"].eq("STRESS_25_5")][
            [
                "exit_reason",
                "fills",
                "wins",
                "losses",
                "profit_factor",
                "net_return_points",
                "net_pnl_rs",
            ]
        ],
        on="exit_reason",
        suffixes=("_reference", "_harsh"),
        how="outer",
    ).sort_values("exit_reason")
    outcome_transitions = _scenario_transitions(source_run)

    status_counts = audit["status"].value_counts(dropna=False).rename_axis("status").reset_index(name="count")
    status_counts["share_pct"] = status_counts["count"] / len(audit) * 100.0
    reason_counts = audit["reason"].value_counts(dropna=False).rename_axis("reason").reset_index(name="count")
    reason_counts["share_pct"] = reason_counts["count"] / len(audit) * 100.0
    overlay_rejections = (
        decisions.loc[~_bools(decisions, "selection_passed")]
        .groupby("selection_reason", as_index=False)
        .agg(
            rejections=("candidate_id", "size"),
            affected_sessions=("session_date", "nunique"),
            median_price_change_pct=("price_change_pct", "median"),
        )
    )
    confirmation_checks = recent._confirmation_detail(audit)
    rejection_codes: list[str] = []
    for value in confirmation_checks["rejection_codes"].fillna(""):
        rejection_codes.extend(code for code in str(value).split(" | ") if code)
    one_minute_rejections = pd.Series(rejection_codes, dtype=object).value_counts().rename_axis("reason").reset_index(name="occurrences")

    indicator_names = [
        "directional_move_pct",
        "directional_five_min_body_pct",
        "oi_change_pct",
        "volume_ratio",
        "traded_value_cr",
        "five_min_range_pct",
        "five_min_body_ratio",
        "five_min_adverse_wick_ratio",
        "five_min_directional_close_location",
        "ema_fast_gap_pct",
        "ema_slow_gap_pct",
        "ema_total_gap_pct",
        "directional_close_ema9_pct",
        "confirmation_volume_ratio",
        "confirmation_body_ratio",
        "confirmation_adverse_wick_ratio",
        "confirmation_close_location",
        "trigger_distance_c5_bps",
        "confirmation_minute",
        "entry_minute",
    ]
    indicator_bins = _indicator_bins(audit)
    indicator_cohorts = study._indicator_cohorts(audit)
    quartiles = _quartile_analysis(audit, indicator_names)
    winner_loser = _winner_loser_table(closed, indicator_names)
    correlations = _correlations(closed, indicator_names + ["holding_minutes", "initial_stop_risk_pct"])
    confirmed_mask = _numbers(audit, "confirmation_minute").notna()
    filled_mask = _bools(audit, "filled")
    indicator_tests = pd.concat(
        [
            _binary_indicator_tests(audit, indicator_names[:15], positive=confirmed_mask, negative=~confirmed_mask, comparison="CONFIRMED_VS_NOT_CONFIRMED"),
            _binary_indicator_tests(audit, indicator_names[:15], positive=filled_mask, negative=~filled_mask, comparison="FILLED_VS_NOT_FILLED"),
            _binary_indicator_tests(closed, indicator_names, positive=_numbers(closed, "net_return_pct").gt(0), negative=_numbers(closed, "net_return_pct").lt(0), comparison="WINNER_VS_LOSER"),
        ],
        ignore_index=True,
    )
    oi_quality_anomalies = closed.loc[_numbers(closed, "oi_change_pct").gt(20)].copy()[
        [
            "candidate_id",
            "session_date",
            "setup_id",
            "side",
            "symbol",
            "oi",
            "prev_oi",
            "oi_change_pct",
            "net_return_pct",
            "net_pnl_rs",
        ]
    ].sort_values("oi_change_pct", ascending=False)

    confirmation_metrics = _group_metrics(
        closed.assign(confirmation_minute=_numbers(closed, "confirmation_minute").astype("Int64").astype(str)),
        "confirmation_minute",
    ).sort_values("confirmation_minute", kind="stable")
    entry_metrics = _group_metrics(
        closed.assign(entry_minute=_numbers(closed, "entry_minute").astype("Int64").astype(str)),
        "entry_minute",
    ).sort_values("entry_minute", kind="stable")
    setup_confirmation = _group_metrics(
        closed.assign(
            setup_confirmation=closed["setup_id"].astype(str)
            + " / M"
            + _numbers(closed, "confirmation_minute").astype("Int64").astype(str)
        ),
        "setup_confirmation",
    ).sort_values("setup_confirmation", kind="stable")
    target_0930_short = audit.loc[audit["setup_id"].astype(str).eq("09:30_SHORT")].copy()
    target_0930_short["early_touch_group"] = np.where(
        _bools(target_0930_short, "v11_early_touch_observed"), "EARLY_TOUCH_OBSERVED", "NO_EARLY_TOUCH"
    )
    delay_metrics = []
    for key, part in target_0930_short.groupby("early_touch_group", sort=False):
        trades = part.loc[_bools(part, "filled")]
        delay_metrics.append(
            {
                "early_touch_group": key,
                "selected": len(part),
                "confirmed": int(_numbers(part, "confirmation_minute").notna().sum()),
                "early_fill_checks_skipped": int(_numbers(part, "v11_early_fill_checks_skipped").fillna(0).sum()),
                **study._trade_metrics(trades),
            }
        )
    delay_metrics_frame = pd.DataFrame(delay_metrics)

    closed["gap_group"] = np.where(_bools(closed, "gap_fill"), "GAP_FILL_ACCEPTED", "TRIGGER_TOUCH")
    gap_fill_metrics = _group_metrics(closed, "gap_group")
    audit["gap_guard_path"] = np.select(
        [_bools(audit, "gap_guard_rejected"), _bools(audit, "gap_guard_observed")],
        ["GAP_REJECTED", "GAP_ACCEPTED"],
        default="NO_GAP_OBSERVED",
    )
    gap_path_records: list[dict[str, Any]] = []
    for key, part in audit.groupby("gap_guard_path", sort=False):
        trades = part.loc[_bools(part, "filled")]
        gap_values = _numbers(part, "gap_guard_adverse_bps").dropna()
        gap_path_records.append(
            {
                "gap_guard_path": key,
                "candidates": len(part),
                "fills": len(trades),
                "median_adverse_gap_bps": float(gap_values.median()) if len(gap_values) else math.nan,
                **{key2: value for key2, value in study._trade_metrics(trades).items() if key2 in {"wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"}},
            }
        )
    gap_path_metrics = pd.DataFrame(gap_path_records)
    observed_gaps = audit.loc[_bools(audit, "gap_guard_observed")].copy()
    observed_gaps["adverse_gap_bin"] = pd.cut(
        _numbers(observed_gaps, "gap_guard_adverse_bps"),
        [-math.inf, 0, 1, 2, 3, 5, math.inf],
        labels=["<=0", "0–1", "1–2", "2–3", "3–5", "5+"],
        include_lowest=True,
        right=True,
    ).astype(str)
    gap_bin_records: list[dict[str, Any]] = []
    for key, part in observed_gaps.groupby("adverse_gap_bin", sort=False):
        trades = part.loc[_bools(part, "filled")]
        gap_bin_records.append(
            {
                "adverse_gap_bin": key,
                "observed": len(part),
                "guard_rejections": int(_bools(part, "gap_guard_rejected").sum()),
                **study._trade_metrics(trades),
            }
        )
    gap_bins = pd.DataFrame(gap_bin_records)
    gap_rejections = audit.loc[_bools(audit, "gap_guard_rejected")][
        ["candidate_id", "session_date", "setup_id", "side", "symbol", "gap_guard_bar_open", "gap_guard_trigger", "gap_guard_adverse_bps", "status", "reason"]
    ].copy()

    unconstrained = audit.loc[_numbers(audit, "unconstrained_net_return_pct").notna()].copy()
    for column in (
        "status",
        "net_return_pct",
        "net_pnl_rs",
        "gross_return_pct",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "position_notional_rs",
    ):
        unconstrained[column] = unconstrained[f"unconstrained_{column}"]
    portfolio_comparison = pd.DataFrame(
        [
            {"portfolio_view": "ACTUAL_CAP2_LEDGER", **_trade_metrics(closed)},
            {"portfolio_view": "UNCONSTRAINED_CANDIDATE_OUTCOMES", **_trade_metrics(unconstrained)},
        ]
    )
    portfolio_decisions = audit["portfolio_decision"].value_counts(dropna=False).rename_axis("portfolio_decision").reset_index(name="count")
    portfolio_rejections = audit.loc[audit["portfolio_decision"].astype(str).eq("REJECTED")][
        ["candidate_id", "session_date", "setup_id", "side", "symbol", "portfolio_reject_reason", "unconstrained_status", "unconstrained_net_return_pct", "unconstrained_net_pnl_rs"]
    ].copy()
    closed["active_at_reservation"] = _numbers(closed, "portfolio_active_at_reservation").astype("Int64").astype(str)
    reservation_metrics = _group_metrics(closed, "active_at_reservation")
    symbol_session_counts = (
        closed.groupby(["session_date", "symbol", "side"], as_index=False)
        .agg(fills=("candidate_id", "size"), net_return_points=("net_return_pct", "sum"), net_pnl_rs=("net_pnl_rs", "sum"))
        .sort_values(["fills", "net_pnl_rs"], ascending=[False, False])
    )
    portfolio_timeline = _portfolio_timeline(closed, sessions)
    peak_row = portfolio_timeline.loc[_numbers(portfolio_timeline, "deployed_notional_rs").idxmax()]
    policy = engine.PortfolioPolicy()
    portfolio_exposure = pd.DataFrame(
        [
            {
                "modeled_capital_rs": policy.capital_rs,
                "margin_per_reservation_rs": policy.margin_per_entry_rs,
                "maximum_reservations": policy.max_concurrent_positions,
                "pending_reserves_margin": policy.pending_reserves_margin,
                "target_exposure_per_entry_rs": v11.TARGET_EXPOSURE_PER_ENTRY_RS,
                "same_symbol_same_side_limit": v11.FIXED_RUNTIME_SPEC.same_side_symbol_limit,
                "same_symbol_opposite_side_prohibited": True,
                "peak_open_positions": int(_numbers(portfolio_timeline, "open_positions").max()),
                "peak_deployed_notional_rs": float(peak_row["deployed_notional_rs"]),
                "peak_deployed_timestamp": peak_row["timestamp"],
                "peak_notional_to_modeled_capital": float(peak_row["deployed_notional_rs"] / policy.capital_rs),
                "maximum_active_at_reservation": float(_numbers(audit, "portfolio_active_at_reservation").max()),
                "maximum_reserved_margin_rs": float(_numbers(audit, "portfolio_reserved_margin_rs").max()),
                "mean_time_weighted_open_positions": float(_numbers(portfolio_timeline, "open_positions").mean()),
                "mean_time_weighted_deployed_notional_rs": float(_numbers(portfolio_timeline, "deployed_notional_rs").mean()),
                "mean_trade_notional_rs": float(_numbers(closed, "position_notional_rs").mean()),
                "minimum_trade_notional_rs": float(_numbers(closed, "position_notional_rs").min()),
                "maximum_trade_notional_rs": float(_numbers(closed, "position_notional_rs").max()),
            }
        ]
    )

    closed["exit_reason"] = closed["exit_reason"].astype(str)
    exit_metrics = _group_metrics(closed, "exit_reason")
    closed["holding_bin"] = pd.cut(
        _numbers(closed, "holding_minutes"),
        [-math.inf, 5, 15, 30, 60, 120, math.inf],
        labels=["<5", "5–15", "15–30", "30–60", "60–120", "120+"],
        right=False,
    ).astype(str)
    holding_metrics = _group_metrics(closed, "holding_bin")
    exit_times = pd.to_datetime(closed["exit_time"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    closed["exit_time_bucket"] = np.select(
        [
            exit_times.dt.hour.lt(10),
            exit_times.dt.hour.lt(12),
            exit_times.dt.hour.lt(14),
            exit_times.dt.hour.lt(15),
        ],
        ["BEFORE_10", "10–12", "12–14", "14–15"],
        default="15_PLUS",
    )
    exit_time_metrics = _group_metrics(closed, "exit_time_bucket")
    closed["ambiguity_group"] = np.select(
        [_bools(closed, "excursion_boundary_ambiguous"), _bools(closed, "ambiguous_entry_bar")],
        ["EXCURSION_BOUNDARY_AMBIGUOUS", "ENTRY_BAR_AMBIGUOUS_ONLY"],
        default="NO_RECORDED_BOUNDARY_AMBIGUITY",
    )
    ambiguity_metrics = _group_metrics(closed, "ambiguity_group")
    excursion_by_outcome = (
        closed.groupby("outcome", as_index=False)
        .agg(
            fills=("candidate_id", "size"),
            median_mfe_lower_pct=("mfe_pct_ohlc_lower_bound", "median"),
            median_mfe_upper_pct=("mfe_pct_ohlc_upper_bound", "median"),
            median_mae_lower_pct=("mae_pct_ohlc_lower_bound", "median"),
            median_mae_upper_pct=("mae_pct_ohlc_upper_bound", "median"),
            median_mfe_lower_r=("mfe_lower_r", "median"),
            median_mfe_upper_r=("mfe_upper_r", "median"),
            median_mae_upper_r=("mae_upper_r", "median"),
            median_holding_minutes=("holding_minutes", "median"),
            median_gross_mfe_capture_pct=("gross_mfe_capture_pct", "median"),
        )
    )
    losers = closed.loc[closed["outcome"].eq("LOSS")]
    excursion_thresholds = pd.DataFrame(
        [
            {
                "diagnostic": f"LOSERS_WITH_MFE_LOWER_AT_LEAST_{threshold:.2f}R",
                "trades": int(_numbers(losers, "mfe_lower_r").ge(threshold).sum()),
                "share_of_losses_pct": float(_numbers(losers, "mfe_lower_r").ge(threshold).mean() * 100.0),
            }
            for threshold in (0.25, 0.50, 0.75, 1.00, 2.00)
        ]
    )
    terminal_paths = (
        closed.loc[closed["exit_reason"].eq("LAST_REAL_BAR_SENSITIVITY")]
        .groupby("exit_clock", as_index=False)
        .agg(
            fills=("candidate_id", "size"),
            wins=("net_return_pct", lambda values: pd.to_numeric(values, errors="coerce").gt(0).sum()),
            losses=("net_return_pct", lambda values: pd.to_numeric(values, errors="coerce").lt(0).sum()),
            net_return_points=("net_return_pct", "sum"),
            net_pnl_rs=("net_pnl_rs", "sum"),
        )
    )
    excursion_quality = pd.DataFrame(
        [
            {
                "filled_trades": len(closed),
                "intrabar_trigger_fills": int(_bools(closed, "intrabar_trigger_fill").sum()),
                "ambiguous_entry_bars": int(_bools(closed, "ambiguous_entry_bar").sum()),
                "exit_at_bar_open": int(_bools(closed, "exit_at_bar_open").sum()),
                "excursion_boundary_ambiguous": int(_bools(closed, "excursion_boundary_ambiguous").sum()),
                "median_mfe_bound_width_pct": float((_numbers(closed, "mfe_pct_ohlc_upper_bound") - _numbers(closed, "mfe_pct_ohlc_lower_bound")).median()),
                "maximum_mfe_bound_width_pct": float((_numbers(closed, "mfe_pct_ohlc_upper_bound") - _numbers(closed, "mfe_pct_ohlc_lower_bound")).max()),
                "median_mae_bound_width_pct": float((_numbers(closed, "mae_pct_ohlc_upper_bound") - _numbers(closed, "mae_pct_ohlc_lower_bound")).median()),
                "maximum_mae_bound_width_pct": float((_numbers(closed, "mae_pct_ohlc_upper_bound") - _numbers(closed, "mae_pct_ohlc_lower_bound")).max()),
            }
        ]
    )

    symbol_metrics = _group_metrics(closed, "symbol").sort_values("net_pnl_rs", ascending=False).reset_index(drop=True)
    setup_symbol_metrics = _group_metrics(closed.assign(setup_symbol=closed["setup_id"].astype(str) + " / " + closed["symbol"].astype(str)), "setup_symbol").sort_values("net_pnl_rs", ascending=False)
    best_trades = closed.nlargest(15, "net_pnl_rs").assign(extreme="BEST_15")
    worst_trades = closed.nsmallest(15, "net_pnl_rs").assign(extreme="WORST_15")
    extreme_trades = pd.concat([best_trades, worst_trades], ignore_index=True)
    abs_symbol = _numbers(symbol_metrics, "net_pnl_rs").abs()
    positive_symbol = _numbers(symbol_metrics, "net_pnl_rs").clip(lower=0)
    concentration = pd.DataFrame(
        [
            {
                "unique_symbols": int(closed["symbol"].nunique()),
                "positive_symbols": int(_numbers(symbol_metrics, "net_pnl_rs").gt(0).sum()),
                "negative_symbols": int(_numbers(symbol_metrics, "net_pnl_rs").lt(0).sum()),
                "one_fill_symbols": int(_numbers(symbol_metrics, "fills").eq(1).sum()),
                "top_1_positive_symbol_share_of_net_pct": float(positive_symbol.nlargest(1).sum() / headline["net_pnl_rs"] * 100.0),
                "top_5_positive_symbols_share_of_net_pct": float(positive_symbol.nlargest(5).sum() / headline["net_pnl_rs"] * 100.0),
                "top_10_positive_symbols_share_of_net_pct": float(positive_symbol.nlargest(10).sum() / headline["net_pnl_rs"] * 100.0),
                "top_10_absolute_symbol_share_pct": float(abs_symbol.nlargest(10).sum() / abs_symbol.sum() * 100.0),
                "best_5_days_share_of_net_pct": float(_numbers(daywise, "net_pnl_rs").nlargest(5).sum() / headline["net_pnl_rs"] * 100.0),
                "best_10_days_share_of_net_pct": float(_numbers(daywise, "net_pnl_rs").nlargest(10).sum() / headline["net_pnl_rs"] * 100.0),
                "best_10_trades_share_of_net_pct": float(_numbers(closed, "net_pnl_rs").nlargest(10).sum() / headline["net_pnl_rs"] * 100.0),
            }
        ]
    )

    daily_signs = np.sign(_numbers(daywise, "net_pnl_rs")).astype(int).tolist()
    trade_order = closed.sort_values(["entry_time", "candidate_id"], kind="stable")
    trade_signs = np.sign(_numbers(trade_order, "net_return_pct")).astype(int).tolist()
    total_notional = float(_numbers(closed, "position_notional_rs").sum())
    win_ci = _wilson_interval(int(headline["wins"]), int(headline["fills"]))
    risk_summary = pd.DataFrame(
        [
            {
                "best_pnl_day": daywise.loc[_numbers(daywise, "net_pnl_rs").idxmax(), "session_date"],
                "best_day_pnl_rs": float(_numbers(daywise, "net_pnl_rs").max()),
                "worst_pnl_day": daywise.loc[_numbers(daywise, "net_pnl_rs").idxmin(), "session_date"],
                "worst_day_pnl_rs": float(_numbers(daywise, "net_pnl_rs").min()),
                "average_daily_pnl_rs": float(_numbers(daywise, "net_pnl_rs").mean()),
                "median_daily_pnl_rs": float(_numbers(daywise, "net_pnl_rs").median()),
                "daily_pnl_std_rs": float(_numbers(daywise, "net_pnl_rs").std(ddof=1)),
                "positive_days": int(_numbers(daywise, "net_pnl_rs").gt(0).sum()),
                "negative_days": int(_numbers(daywise, "net_pnl_rs").lt(0).sum()),
                "flat_days": int(_numbers(daywise, "net_pnl_rs").eq(0).sum()),
                "max_consecutive_positive_days": _streak(daily_signs, 1),
                "max_consecutive_negative_days": _streak(daily_signs, -1),
                "max_consecutive_winning_trades": _streak(trade_signs, 1),
                "max_consecutive_losing_trades": _streak(trade_signs, -1),
                "max_drawdown_points": float(-_numbers(daywise, "drawdown_return_points").min()),
                "max_drawdown_pnl_rs": float(-_numbers(daywise, "drawdown_pnl_rs").min()),
                "recovery_factor": float(headline["net_pnl_rs"] / -_numbers(daywise, "drawdown_pnl_rs").min()),
                "win_rate_wilson_95_low_pct": win_ci[0],
                "win_rate_wilson_95_high_pct": win_ci[1],
                "extra_break_even_cost_bps_on_fixed_notional": float(headline["net_pnl_rs"] / total_notional * 10_000.0),
            }
        ]
    )
    drawdown_episodes = _drawdown_episodes(daywise)
    bootstrap = _bootstrap_scenarios(source_run)
    order_drawdown = _order_drawdown(daywise)
    daily_regimes = _daily_regimes(audit, closed, daywise)

    source_segments = _source_segment_table(provenance)
    source_incomplete = int(_numbers(source_segments, "source_incomplete_symbol_sessions").sum())
    source_expected = int(_numbers(source_segments, "expected_symbol_sessions").sum())
    global_parameters = pd.DataFrame(
        [
            {"layer": "Identity", "parameter": "V11 profile", "value": v11.PROFILE_ID, "scope": "locked standalone"},
            {"layer": "Identity", "parameter": "Profile SHA-256", "value": v11.LOCKED_PROFILE_SHA256, "scope": "entire profile"},
            {"layer": "Base", "parameter": "V10 base profile", "value": v11.EXPECTED_V10_PROFILE_ID, "scope": "all setups"},
            {"layer": "5m selection", "parameter": "09:40 LONG directional move floor", "value": "0.40%", "scope": "09:40_LONG only"},
            {"layer": "5m selection", "parameter": "09:35 LONG directional move ceiling", "value": "0.50%", "scope": "09:35_LONG only"},
            {"layer": "1m timing", "parameter": "Earliest trigger-fill minute", "value": "S+3", "scope": "09:30_SHORT only"},
            {"layer": "Gap", "parameter": "Maximum adverse trigger gap", "value": "2 bps", "scope": "strong identity gap events"},
            {"layer": "Gap", "parameter": "Reject every gap", "value": "False", "scope": "gap <= 2 bps can fill"},
            {"layer": "Portfolio", "parameter": "Same symbol + same side concurrent limit", "value": "2", "scope": "all setups"},
            {"layer": "Portfolio", "parameter": "Same symbol + opposite side", "value": "Prohibited", "scope": "all setups"},
            {"layer": "Portfolio", "parameter": "Modeled capital", "value": f"Rs {policy.capital_rs:,.0f}", "scope": "global ledger"},
            {"layer": "Portfolio", "parameter": "Margin reservation per entry", "value": f"Rs {policy.margin_per_entry_rs:,.0f}", "scope": "global ledger"},
            {"layer": "Portfolio", "parameter": "Maximum reservations", "value": str(policy.max_concurrent_positions), "scope": "global ledger"},
            {"layer": "Sizing", "parameter": "Target cash-equivalent exposure per fill", "value": f"Rs {v11.TARGET_EXPOSURE_PER_ENTRY_RS:,.0f}", "scope": "quantity=floor(exposure/entry)"},
            {"layer": "Exit", "parameter": "Dynamic exit overlay", "value": "None", "scope": "base stop/target remains"},
            {"layer": "Exit", "parameter": "Same-bar collision", "value": "STOP_FIRST", "scope": "conservative OHLC rule"},
            {"layer": "Exit", "parameter": "Square-off clock", "value": v11.SQUARE_OFF, "scope": "when a real bar exists"},
            {"layer": "Exit", "parameter": "Terminal policy", "value": v11.EOD_POLICY, "scope": "partial-path sensitivity"},
            {"layer": "Costs", "parameter": "Reference", "value": "15 bps cost + 0 bps slippage", "scope": "headline diagnostic"},
            {"layer": "Costs", "parameter": "Stress", "value": "20 bps cost + 2 bps slippage", "scope": "robustness"},
            {"layer": "Costs", "parameter": "Harsh stress", "value": "25 bps cost + 5 bps slippage", "scope": "robustness"},
        ]
    )
    lineage_metrics, v10_day_difference, development_gates, lineage_payload = _load_lineage(
        lineage_run, provenance, headline
    )
    component_attribution, component_details = _component_attribution(lineage_payload)
    blocked_tests = pd.DataFrame(lineage_payload.get("blocked_tests", [])) if lineage_payload else pd.DataFrame()

    funnel = {
        "Base 5m": len(decisions),
        "After overlays": len(audit),
        "1m confirmed": int(_numbers(audit, "confirmation_minute").notna().sum()),
        "Filled": len(closed),
        "Winners": int(headline["wins"]),
    }
    chart_paths = _plots(assets_dir, daywise, setup_metrics, monthly, scenario_metrics, funnel)

    asset_frames: dict[str, pd.DataFrame] = {
        "daily_performance.csv": daily_detail,
        "period_comparison.csv": periods,
        "monthly_performance.csv": monthly,
        "weekly_performance.csv": weekly,
        "weekday_performance.csv": weekday,
        "ten_session_blocks.csv": blocks,
        "rolling_10_session_metrics.csv": rolling,
        "all_period_all_cost_metrics.csv": all_period_metrics,
        "cost_scenario_summary.csv": scenario_metrics,
        "cost_outcome_transitions.csv": outcome_transitions,
        "setup_metrics.csv": setup_metrics,
        "setup_cost_robustness.csv": setup_robustness,
        "setup_all_scenarios.csv": scenario_setup,
        "side_metrics.csv": side_metrics,
        "side_all_scenarios.csv": scenario_side,
        "confirmation_minute_all_scenarios.csv": scenario_confirmation,
        "entry_minute_all_scenarios.csv": scenario_entry,
        "exit_reason_all_scenarios.csv": scenario_exit,
        "confirmation_minute_cost_robustness.csv": confirmation_robustness,
        "exit_reason_cost_robustness.csv": exit_robustness,
        "slot_metrics.csv": slot_metrics,
        "picker_metrics.csv": picker_metrics,
        "rank_metrics.csv": rank_funnel,
        "candidate_status_counts.csv": status_counts,
        "candidate_reason_counts.csv": reason_counts,
        "selection_overlay_rejections.csv": overlay_rejections,
        "confirmation_checks_expanded.csv": confirmation_checks,
        "one_minute_rejection_codes.csv": one_minute_rejections,
        "indicator_bins.csv": indicator_bins,
        "indicator_cohort_summary.csv": indicator_cohorts,
        "indicator_quartiles.csv": quartiles,
        "indicator_winner_loser.csv": winner_loser,
        "indicator_correlations.csv": correlations,
        "indicator_binary_tests_bh.csv": indicator_tests,
        "oi_low_base_anomalies.csv": oi_quality_anomalies,
        "confirmation_minute_metrics.csv": confirmation_metrics,
        "entry_minute_metrics.csv": entry_metrics,
        "setup_confirmation_minute_metrics.csv": setup_confirmation,
        "stage4_delay_diagnostics.csv": delay_metrics_frame,
        "stage4_0930_short_candidate_detail.csv": target_0930_short,
        "gap_fill_metrics.csv": gap_fill_metrics,
        "gap_guard_path_metrics.csv": gap_path_metrics,
        "gap_adverse_bps_bins.csv": gap_bins,
        "gap_guard_rejections.csv": gap_rejections,
        "portfolio_actual_vs_unconstrained.csv": portfolio_comparison,
        "portfolio_decisions.csv": portfolio_decisions,
        "portfolio_rejections.csv": portfolio_rejections,
        "portfolio_active_at_reservation_metrics.csv": reservation_metrics,
        "symbol_session_side_fill_counts.csv": symbol_session_counts,
        "portfolio_minute_timeline.csv": portfolio_timeline,
        "portfolio_exposure_summary.csv": portfolio_exposure,
        "exit_reason_metrics.csv": exit_metrics,
        "exit_time_metrics.csv": exit_time_metrics,
        "holding_period_metrics.csv": holding_metrics,
        "ambiguity_metrics.csv": ambiguity_metrics,
        "excursion_by_outcome.csv": excursion_by_outcome,
        "loser_mfe_thresholds.csv": excursion_thresholds,
        "terminal_path_summary.csv": terminal_paths,
        "excursion_quality_summary.csv": excursion_quality,
        "symbol_metrics.csv": symbol_metrics,
        "setup_symbol_metrics.csv": setup_symbol_metrics,
        "extreme_trades.csv": extreme_trades,
        "concentration_summary.csv": concentration,
        "risk_summary.csv": risk_summary,
        "drawdown_episodes.csv": drawdown_episodes,
        "session_bootstrap.csv": bootstrap,
        "random_order_drawdown.csv": order_drawdown,
        "daily_regime_diagnostics.csv": daily_regimes,
        "source_segments.csv": source_segments,
        "setup_parameter_reference.csv": setup_parameters,
        "global_parameter_reference.csv": global_parameters,
        "v11_lineage_metrics.csv": lineage_metrics,
        "v11_vs_v10_daywise.csv": v10_day_difference,
        "v11_development_gates.csv": development_gates,
        "v11_component_attribution.csv": component_attribution,
        "v11_component_trade_details.csv": component_details,
        "blocked_validity_tests.csv": blocked_tests,
    }
    output_paths = _write_frames(assets_dir, asset_frames) + chart_paths

    profile = dict(provenance["profile"])
    reference_row = scenario_metrics.loc[scenario_metrics["scenario"].eq("REFERENCE_15_0")].iloc[0]
    harsh_row = scenario_metrics.loc[scenario_metrics["scenario"].eq("STRESS_25_5")].iloc[0]
    forward_row = periods.loc[periods["period"].eq("FORWARD_6")].iloc[0]
    july_row = monthly.loc[monthly["period"].eq("2026-07")].iloc[0]
    eod_row = exit_metrics.loc[exit_metrics["exit_reason"].eq("LAST_REAL_BAR_SENSITIVITY")].iloc[0]
    best_rolling = rolling.loc[_numbers(rolling, "net_pnl_rs").idxmax()]
    worst_rolling = rolling.loc[_numbers(rolling, "net_pnl_rs").idxmin()]
    winner_tests = indicator_tests.loc[indicator_tests["comparison"].eq("WINNER_VS_LOSER")]
    fill_tests = indicator_tests.loc[indicator_tests["comparison"].eq("FILLED_VS_NOT_FILLED")]
    significant_fill_tests = fill_tests.loc[_numbers(fill_tests, "bh_q_value").lt(0.05)]
    all_setups_harsh_positive = bool(_numbers(setup_robustness, "net_return_points_harsh").gt(0).all())
    eod_share = float(eod_row["net_return_points"] / headline["net_return_points"] * 100.0)
    july_share = float(july_row["net_return_points"] / headline["net_return_points"] * 100.0)
    asset_prefix = assets_dir.name if assets_dir.parent == report_path.parent else str(assets_dir)

    five_minute_parameters = setup_parameters.copy()
    five_minute_parameters["effective_move_rule"] = np.where(
        five_minute_parameters["side"].eq("LONG"),
        ">= +" + five_minute_parameters["price_change_pct"].astype(str) + "%",
        "<= -" + five_minute_parameters["price_change_pct"].astype(str) + "%",
    )
    five_minute_parameters.loc[
        five_minute_parameters["setup_id"].eq("09:40_LONG"), "effective_move_rule"
    ] = ">= +0.40% (Stage 7 floor)"
    five_minute_parameters.loc[
        five_minute_parameters["setup_id"].eq("09:35_LONG"), "effective_move_rule"
    ] = ">= +0.20% and <= +0.50%"
    five_minute_parameters["min_traded_value_cr"] = _numbers(
        five_minute_parameters, "min_traded_value"
    ) / 10_000_000.0

    headline_table = pd.DataFrame(
        [
            {
                "sessions": len(sessions),
                "fills": headline["fills"],
                "wins": headline["wins"],
                "losses": headline["losses"],
                "win_rate_pct": headline["win_rate_pct"],
                "profit_factor": headline["profit_factor"],
                "gross_return_points": float(_numbers(closed, "gross_return_pct").sum()),
                "net_return_points": headline["net_return_points"],
                "gross_pnl_rs": headline["gross_pnl_rs"],
                "estimated_cost_rs": headline["estimated_cost_rs"],
                "net_pnl_rs": headline["net_pnl_rs"],
                "max_drawdown_points": float(reference_row["max_daily_drawdown_points"]),
            }
        ]
    )

    lines: list[str] = []
    add = lines.extend
    add(
        [
            "# V11 FNO Stage 10 — full historical deep-study report",
            "",
            f"Generated: {datetime.now(gaps.IST).isoformat()}",
            f"Validated standalone run: `{source_run}`",
            f"Profile: `{v11.PROFILE_ID}`",
            f"Profile SHA-256: `{v11.LOCKED_PROFILE_SHA256}`",
            f"Historical input binding: `{provenance['input_binding_sha256']}`",
            "",
            "> **Research boundary:** this result is explicitly `research_only=true`, "
            "`headline_valid=false`, `promotion_eligible=false`, and has no live/paper authority. "
            "It is the strongest post-hoc configuration observed on this history, not an untouched validation result.",
            "",
            "## Executive conclusion",
            "",
            f"The sealed reference replay covers **{len(sessions)} usable sessions** from **{sessions[0]} through {sessions[-1]}** and records "
            f"**{headline['fills']} fills, {headline['wins']}-{headline['losses']}, WR {headline['win_rate_pct']:.2f}%, PF {headline['profit_factor']:.4f}, "
            f"{headline['net_return_points']:+.4f} net points and Rs {headline['net_pnl_rs']:+,.2f} modeled P&L**, with daily MDD "
            f"{float(reference_row['max_daily_drawdown_points']):.4f} points.",
            "",
            f"The result remains positive in the harsh 25 bps cost + 5 bps slippage case: PF {float(harsh_row['profit_factor']):.4f}, "
            f"{float(harsh_row['net_return_points']):+.4f} points and Rs {float(harsh_row['net_pnl_rs']):+,.2f}. "
            f"Both sides and all ten setup buckets remain positive under that case: **{'yes' if all_setups_harsh_positive else 'no'}**.",
            "",
            f"The evidence is encouraging but concentrated: July contributes **{july_share:.2f}%** of reference net points, and "
            f"last-real-bar exits contribute **{eod_share:.2f}%**. The six-session extension earns {float(forward_row['net_return_points']):+.4f} points, "
            "but it was used in model selection and is not an untouched holdout.",
            "",
            f"Most importantly, none of the {len(winner_tests)} tested numeric features available by entry separates winners from losers at "
            "BH-adjusted q < 0.05. Several activity features predict whether an order gets filled, but fill probability is not accuracy. "
            "The safest conclusion is to freeze V11, repair execution/data validity, and validate prospectively before changing global thresholds.",
            "",
            "## 1. Integrity, data contract and scope",
            "",
            f"- Provenance and all **{validation['artifact_inventory']['artifact_count']}** inventoried run artifacts passed size/hash/set validation.",
            f"- The profile and input were re-bound to `{v11.LOCKED_PROFILE_SHA256}` and `{provenance['input_binding_sha256']}`.",
            f"- Calendar span contains **{len(expected_span)}** expected regular sessions; missing validated session: **{', '.join(map(str, missing_sessions)) or 'none'}**.",
            f"- Strict source completeness failed for **{source_incomplete:,} of {source_expected:,} symbol-sessions ({source_incomplete/source_expected*100:.2f}%)**. "
            "This is universe/path coverage, not the selected-candidate `data_incomplete_candidates` count, which is zero.",
            "- The cache contains base-qualified 5-minute candidates, not every universe symbol that failed base selection. Filter counterfactual P&L is therefore unavailable unless the full stream is replayed.",
            "- Futures OI drives selection, while 5-minute price/EMA/volume and 1-minute execution use NSE cash-equity paths. Quantity is cash-equivalent share sizing, not dated futures-lot sizing.",
            "",
            "### Source segments",
            "",
        ]
    )
    add(
        _table(
            source_segments,
            [
                "segment_id",
                "from_day",
                "through_day",
                "contract_month",
                "universe_master_date",
                "sessions",
                "candidates",
                "expected_symbol_sessions",
                "source_incomplete_symbol_sessions",
                "source_incomplete_pct",
                "headline_source_complete",
            ],
        )
    )
    add(["", "### Validity tests that could not be honestly executed", ""])
    add(_table(blocked_tests, ["stage_id", "test_id", "status", "reason"]))

    add(["", "## 2. Exact locked strategy and parameter values", "", "### Global V11 overlays and economic assumptions", ""])
    add(_table(global_parameters, ["layer", "parameter", "value", "scope"]))
    add(
        [
            "",
            "### Five-minute selection book",
            "",
            "Each row is one side-specific setup for the 5-minute candle ending at `signal_end`. The mixed `max_entries` value is the maximum ranked candidates for that setup/side/slot—not a daily maximum and not the same as the concurrent same-symbol limit. Both LONG and SHORT rows can select on the same slot. `picker` decides ranking inside the eligible bucket; portfolio rules are applied later in chronological order.",
            "",
        ]
    )
    add(
        _table(
            five_minute_parameters,
            [
                "setup_id",
                "signal_end",
                "side",
                "max_entries",
                "picker",
                "five_minute_ema_rule",
                "effective_move_rule",
                "oi_change_pct",
                "volume_ratio",
                "min_traded_value_cr",
            ],
        )
    )
    add(
        [
            "",
            "### One-minute confirmation, entry and exit book",
            "",
            "A candidate monitors setup-relative one-minute bars, requires the side-aware candle checks below, then places a stop trigger at the signal extreme plus any buffer. Entry expires at S+5. The frozen same-bar rule is `STOP_FIRST`. Only `09:30_SHORT` has the V11 S+3 earliest-fill overlay.",
            "",
        ]
    )
    add(
        _table(
            setup_parameters,
            [
                "setup_id",
                "body_ratio",
                "max_wick_ratio",
                "effective_close_location_min",
                "effective_max_confirmation_minute",
                "effective_buffer_bps",
                "effective_midpoint_invalidation",
                "entry_expiry_minute",
                "stop_pct",
                "target_pct",
                "post_confirmation_cancel",
                "allow_cap_reassignment",
                "same_bar_policy",
            ],
        )
    )

    add(["", "## 3. Selection-to-exit funnel", ""])
    funnel_frame = pd.DataFrame([{"step": key, "count": value} for key, value in funnel.items()])
    add(_table(funnel_frame, ["step", "count"]))
    add(
        [
            "",
            f"Retention is **{len(audit)/len(decisions)*100:.2f}%** from the 1,241 cached base candidates to post-overlay selection, "
            f"confirmation is **{_numbers(audit, 'confirmation_minute').notna().mean()*100:.2f}%** of selected, and fills are "
            f"**{len(closed)/len(audit)*100:.2f}%** of selected / **{len(closed)/_numbers(audit, 'confirmation_minute').notna().sum()*100:.2f}%** of confirmed.",
            "",
            "### Five-minute overlay exclusions",
            "",
        ]
    )
    add(_table(overlay_rejections, ["selection_reason", "rejections", "affected_sessions", "median_price_change_pct"]))
    add(
        [
            "",
            "The 107 excluded rows were not replayed through entry/exit. They are **selection exclusions**, not proven avoided losses.",
            "",
            "### Final candidate lifecycle states",
            "",
        ]
    )
    add(_table(status_counts, ["status", "count", "share_pct"]))
    add(["", "### Terminal/rejection reasons", ""])
    add(_table(reason_counts, ["reason", "count", "share_pct"]))
    add(["", "### One-minute failed-check occurrences", ""])
    add(_table(one_minute_rejections, ["reason", "occurrences"]))
    add(["", "Failure codes can overlap within a candle/candidate, so their counts do not sum to candidate totals.", "", f"![Selection funnel]({asset_prefix}/selection_funnel.png)"])

    add(["", "## 4. Headline economics, risk and cost sensitivity", ""])
    add(_table(headline_table, list(headline_table.columns)))
    add(
        [
            "",
            f"Average win is {headline['average_win_points']:+.4f} points, average loss {headline['average_loss_points']:+.4f}, payoff ratio {headline['payoff_ratio']:.4f}, and expectancy {headline['average_return_points']:+.4f} points/fill. "
            f"The 95% Wilson interval around historical WR is {float(risk_summary.iloc[0]['win_rate_wilson_95_low_pct']):.2f}%–{float(risk_summary.iloc[0]['win_rate_wilson_95_high_pct']):.2f}%.",
            "",
            "### Three sealed cost cases",
            "",
        ]
    )
    add(
        _table(
            scenario_metrics,
            [
                "scenario",
                "cost_bps",
                "slippage_bps",
                "fills",
                "wins",
                "losses",
                "win_rate_pct",
                "profit_factor",
                "net_return_points",
                "net_pnl_rs",
                "max_daily_drawdown_points",
                "positive_days",
                "negative_days",
                "net_pnl_change_vs_reference_rs",
                "net_pnl_retained_vs_reference_pct",
            ],
        )
    )
    add(["", f"![Cost scenarios]({asset_prefix}/cost_scenarios.png)", "", "### Outcome changes as costs rise", ""])
    add(_table(outcome_transitions, ["REFERENCE_15_0", "STRESS_20_2", "STRESS_25_5", "trades"]))
    add(
        [
            "",
            f"Reference modeled costs remove {float(_numbers(closed, 'cost_drag_points').sum()):.4f} points / Rs {headline['estimated_cost_rs']:,.2f}. "
            f"The fixed-trade arithmetic break-even cushion is about {float(risk_summary.iloc[0]['extra_break_even_cost_bps_on_fixed_notional']):.2f} additional bps on summed notional; this is not a live capacity estimate because fills and prices can change with friction.",
            "",
            f"![Equity and drawdown]({asset_prefix}/equity_and_drawdown.png)",
        ]
    )

    add(["", "## 5. Stability through time", "", "### Core, extension, halves and recent window", ""])
    add(_table(periods, ["period", "sessions", "post_overlay_selected", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(
        [
            "",
            f"The six-session extension has only {int(forward_row['fills'])} fills and was part of the Stage 10 selection gate. It is a sensitivity slice, not independent evidence.",
            "",
            "### Monthly",
            "",
        ]
    )
    add(_table(monthly, ["period", "sessions", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "positive_days", "negative_days", "flat_days"]))
    add(["", f"![Monthly P&L]({asset_prefix}/monthly_net_pnl.png)", "", "### Weekly", ""])
    add(_table(weekly, ["period", "sessions", "fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Sequential ten-session blocks", ""])
    add(_table(blocks, ["period", "sessions", "fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(
        [
            "",
            f"Best rolling ten-session window: **{best_rolling['from_day']} through {best_rolling['through_day']}**, Rs {float(best_rolling['net_pnl_rs']):+,.2f}, PF {float(best_rolling['profit_factor']):.4f}. "
            f"Worst: **{worst_rolling['from_day']} through {worst_rolling['through_day']}**, Rs {float(worst_rolling['net_pnl_rs']):+,.2f}, PF {float(worst_rolling['profit_factor']):.4f}.",
            "",
            "### Weekday",
            "",
        ]
    )
    add(_table(weekday, ["period", "sessions", "fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "All weekdays are profitable here. That is descriptive; weekday filters would still be post-hoc calendar mining.", "", "### Daily activity/range composition diagnostics", ""])
    add(_table(daily_regimes, ["regime_dimension", "regime", "sessions", "measure_min", "measure_median", "measure_max", "fills", "positive_days", "negative_days", "net_return_points", "net_pnl_rs"]))
    add(["", "Buckets are sample-derived terciles. End-of-day fill count is future information and cannot be used as a live filter; only a predeclared causal opening-breadth proxy could be tested.", "", "### Full 65-session day-wise audit", ""])
    add(_table(daily_detail, ["session_date", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "cumulative_net_pnl_rs", "drawdown_pnl_rs"]))

    add(["", "## 6. V11 lineage and attribution versus frozen V10", ""])
    add(_table(lineage_metrics, ["variant_id", "scenario", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "max_daily_drawdown_points"]))
    if not lineage_metrics.empty:
        baseline = lineage_metrics.loc[lineage_metrics["variant_id"].eq("V10_STAGE0_FROZEN_CONTROL") & lineage_metrics["scenario"].eq("REFERENCE_15_0")].iloc[0]
        delta_points = headline["net_return_points"] - float(baseline["net_return_points"])
        delta_pnl = headline["net_pnl_rs"] - float(baseline["net_pnl_rs"])
        add(
            [
                "",
                f"Against the frozen V10 control, V11 adds **{delta_points:+.4f} points / Rs {delta_pnl:+,.2f} ({delta_points/float(baseline['net_return_points'])*100:+.2f}%)**, "
                f"with {headline['fills']-int(baseline['fills']):+d} net fills and MDD changing from {float(baseline['max_daily_drawdown_points']):.4f} to {float(reference_row['max_daily_drawdown_points']):.4f} points.",
                "",
                "### Component trade-set attribution",
                "",
            ]
        )
        add(_table(component_attribution, ["scenario", "component_effect", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
        add(
            [
                "",
                "`CAP2_ADDED` compares Stage 10 with the delay-only component; `S3_DELAY_REMOVED` reports the early 09:30 SHORT trades present under cap-two without the delay but absent in Stage 10. This is exact replay attribution on the development sample, not prospective causal proof.",
                "",
                "### Day-wise V11 minus V10",
                "",
            ]
        )
        v10_daily_display = v10_day_difference.rename(
            columns={
                "net_return_pct__V10_STAGE0_FROZEN_CONTROL": "v10_net_return_points",
                f"net_return_pct__{v11.PROFILE_ID}": "v11_net_return_points",
                "net_pnl_rs__V10_STAGE0_FROZEN_CONTROL": "v10_net_pnl_rs",
                f"net_pnl_rs__{v11.PROFILE_ID}": "v11_net_pnl_rs",
                "fills__V10_STAGE0_FROZEN_CONTROL": "v10_fills",
                f"fills__{v11.PROFILE_ID}": "v11_fills",
            }
        )
        add(_table(v10_daily_display, ["session_date", "v10_fills", "v11_fills", "v10_net_return_points", "v11_net_return_points", "delta_net_return_points", "v10_net_pnl_rs", "v11_net_pnl_rs", "delta_net_pnl_rs", "cumulative_delta_net_pnl_rs"]))
        add(["", "### Development gates", ""])
        add(_table(development_gates, ["variant_id", "stage_id", "development_gate_passed", "gate_classification", "reference_net_ratio_vs_baseline", "reference_mdd_ratio_vs_baseline", "promotion_gate_passed", "promotion_blocker"]))

    add(["", "## 7. Setup, side, slot, picker and rank", "", "### Setup funnel and contribution", ""])
    add(_table(setup_metrics, ["setup_id", "max_entries", "picker", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "confirmation_rate_pct", "fills", "fill_rate_pct", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", f"![Setup contribution]({asset_prefix}/setup_net_pnl.png)", "", "### Setup survival under harsh costs", ""])
    add(_table(setup_robustness, ["setup_id", "fills_reference", "wins", "losses", "win_rate_pct", "profit_factor_reference", "net_return_points_reference", "profit_factor_harsh", "net_return_points_harsh", "net_pnl_rs_harsh"]))
    add(["", "All ten setups stay net-positive under harsh costs. Deleting a setup based on this same sample would discard diversification without out-of-sample evidence.", "", "### Side", ""])
    add(_table(side_metrics, ["side", "post_overlay_selected", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Signal slot", ""])
    add(_table(slot_metrics, ["signal_end", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Picker", ""])
    add(_table(picker_metrics, ["picker", "post_overlay_selected", "fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Frozen rank", ""])
    add(_table(rank_funnel, ["rank_bucket", "selected", "confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "Rank performance is non-monotonic. In particular, a weak historical rank 3 next to profitable ranks 4–6 is a warning against an exact-rank blacklist; a causal relative-score or breadth hypothesis would be safer to test."])

    add(["", "## 8. Stage 10 mechanism diagnostics", "", "### 09:30 SHORT S+3 earliest-fill rule", ""])
    add(_table(delay_metrics_frame, ["early_touch_group", "selected", "confirmed", "early_fill_checks_skipped", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(
        [
            "",
            f"Within 09:30 SHORT, {int(_numbers(target_0930_short, 'confirmation_minute').notna().sum())} candidates confirmed, "
            f"{int(_numbers(target_0930_short, 'v11_early_fill_checks_skipped').fillna(0).sum())} recorded skipped early-fill checks, and "
            f"{int(_bools(target_0930_short, 'v11_early_touch_observed').sum())} showed an early touch. The lineage table—not this outcome-conditioned subgroup—is the proper component comparison.",
            "",
            "### Same-symbol/same-side maximum two",
            "",
        ]
    )
    add(_table(portfolio_comparison, ["portfolio_view", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(_table(portfolio_rejections, ["candidate_id", "session_date", "setup_id", "side", "symbol", "portfolio_reject_reason", "unconstrained_status", "unconstrained_net_return_pct", "unconstrained_net_pnl_rs"]))
    add(["", "Only three would-be third same-side/symbol reservations were rejected; all three stored unconstrained paths lost here. N=3 is far too small to justify a cap-three conclusion or further cap tuning.", "", "### Portfolio exposure and overlap", ""])
    add(_table(portfolio_exposure, list(portfolio_exposure.columns)))
    add(["", "The exposure figures are internally consistent with the cash-equivalent model, but are not executable futures leverage: dated lots, ticks, margins, spread and front-month rollover are absent.", "", "### Strong-identity 2 bps gap guard", ""])
    add(_table(gap_path_metrics, ["gap_guard_path", "candidates", "fills", "median_adverse_gap_bps", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Observed adverse-gap bins", ""])
    add(_table(gap_bins, ["adverse_gap_bin", "observed", "guard_rejections", "fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "Accepted 1–2 bps gap fills remain profitable in this sample. Tightening to 1 bps would remove observed winners as well as losers. Rejected gaps have no post-rejection execution counterfactual, so the report makes no claim that the guard 'avoided losses.'"])

    add(["", "## 9. Five-minute and confirmation-indicator study", "", "### Cohort distributions", ""])
    add(_table(indicator_cohorts, ["indicator", "cohort", "observations", "mean", "median", "p25", "p75"]))
    add(["", "### Winner-versus-loser medians", ""])
    add(_table(winner_loser, ["indicator", "winner_observations", "loser_observations", "winner_median", "loser_median", "median_delta", "winner_mean", "loser_mean"]))
    add(["", "### Multiple-test-corrected binary comparisons", ""])
    add(_table(indicator_tests, ["comparison", "indicator", "positive_observations", "negative_observations", "positive_median", "negative_median", "auc_positive_higher", "p_value_two_sided", "bh_q_value"]))
    add(
        [
            "",
            f"At q < 0.05, **{len(significant_fill_tests)}** tested features distinguish filled from non-filled candidates, but **{int((_numbers(winner_tests, 'bh_q_value') < 0.05).sum())}** distinguish winners from losers. "
            "AUC near 0.5 means little separation; below 0.5 means the positive group tends to have lower values. These are univariate, post-selection tests—not threshold backtests.",
            "",
            "### Spearman/Pearson association with realized net return",
            "",
        ]
    )
    add(_table(correlations, ["indicator", "observations", "spearman_vs_net_return", "pearson_vs_net_return"]))
    add(["", "### Data-derived quartiles", ""])
    add(_table(quartiles, ["indicator", "quartile", "observed_range", "selected", "confirmed", "fills", "fill_rate_pct", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "Quartiles repeatedly show non-monotonic results. They are hypothesis generators only; choosing a winning quartile after seeing these outcomes would be leakage."])
    for indicator in indicator_bins["indicator"].drop_duplicates().tolist():
        add(["", f"### Fixed bins — `{indicator}`", ""])
        add(_table(indicator_bins.loc[indicator_bins["indicator"].eq(indicator)], ["bin", "selected", "confirmed", "fills", "confirmation_rate_pct", "fill_rate_pct", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))

    add(["", "## 10. One-minute timing and trigger quality", "", "### Confirmation minute", ""])
    add(_table(confirmation_metrics, ["confirmation_minute", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Confirmation minute under harsh costs", ""])
    add(_table(confirmation_robustness, ["confirmation_minute", "fills_reference", "wins", "losses", "win_rate_pct", "profit_factor_reference", "net_return_points_reference", "profit_factor_harsh", "net_return_points_harsh", "net_pnl_rs_harsh"]))
    add(["", "### Confirmation minute by setup", ""])
    add(_table(setup_confirmation, ["setup_confirmation", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Entry minute", ""])
    add(_table(entry_metrics, ["entry_minute", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "Minute 2 is weak under harsh costs specifically in some SHORT setups, while minute-2 LONG remains useful. If tested, use a predeclared minute-2 SHORT quality/reconfirmation rule—not a global minute ban or a retrospective deletion."])

    add(["", "## 11. Exit, holding-time and excursion diagnostics", "", "### Exit reason", ""])
    add(_table(exit_metrics, ["exit_reason", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "average_return_points"]))
    add(["", "### Exit reason under harsh costs", ""])
    add(_table(exit_robustness, ["exit_reason", "fills_reference", "wins_reference", "losses_reference", "profit_factor_reference", "net_return_points_reference", "net_pnl_rs_reference", "fills_harsh", "wins_harsh", "losses_harsh", "profit_factor_harsh", "net_return_points_harsh", "net_pnl_rs_harsh"]))
    add(
        [
            "",
            f"The {int(eod_row['fills'])} last-real-bar exits contribute {float(eod_row['net_return_points']):+.4f} points ({eod_share:.2f}% of total). "
            "Exit reason and holding duration are realized outcomes; they cannot be used as entry filters.",
            "",
            "### Terminal clock under last-real-bar policy",
            "",
        ]
    )
    add(_table(terminal_paths, ["exit_clock", "fills", "wins", "losses", "net_return_points", "net_pnl_rs"]))
    add(["", "### Holding-time buckets", ""])
    add(_table(holding_metrics, ["holding_bin", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Exit-time buckets", ""])
    add(_table(exit_time_metrics, ["exit_time_bucket", "fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### MFE/MAE by outcome", ""])
    add(_table(excursion_by_outcome, list(excursion_by_outcome.columns)))
    add(["", "### Losing trades that first reached favorable R thresholds", ""])
    add(_table(excursion_thresholds, ["diagnostic", "trades", "share_of_losses_pct"]))
    add(["", "### OHLC-boundary quality", ""])
    add(_table(excursion_quality, list(excursion_quality.columns)))
    add(["", "Because most excursion boundaries are ambiguous/incomplete, MFE/MAE can motivate a separately replayed exit hypothesis but cannot support direct trailing-stop optimization yet."])

    add(["", "### OI percentage low-base anomalies", ""])
    add(_table(oi_quality_anomalies, ["session_date", "setup_id", "side", "symbol", "oi", "prev_oi", "oi_change_pct", "net_return_pct", "net_pnl_rs"]))
    add(["", "Very large OI-change percentages can come from a small prior-OI denominator. A minimum prior-OI/data-quality rule is worth testing causally; these rows are not enough to choose its threshold."])

    add(["", "## 12. Symbols, concentration and extremes", ""])
    add(_table(concentration, list(concentration.columns)))
    add(["", "### Top 15 symbols", ""])
    add(_table(symbol_metrics.head(15), ["symbol", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Bottom 15 symbols", ""])
    add(_table(symbol_metrics.tail(15).sort_values("net_pnl_rs"), ["symbol", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "Sixty symbols have only one fill. Symbol blacklists/whitelists would have extreme sampling error and survivorship risk.", "", "### Best 15 trades", ""])
    add(_table(best_trades, ["session_date", "setup_id", "side", "symbol", "entry_time", "exit_time", "exit_reason", "net_return_pct", "net_pnl_rs", "mfe_pct_ohlc_lower_bound", "mae_pct_ohlc_upper_bound"]))
    add(["", "### Worst 15 trades", ""])
    add(_table(worst_trades, ["session_date", "setup_id", "side", "symbol", "entry_time", "exit_time", "exit_reason", "net_return_pct", "net_pnl_rs", "mfe_pct_ohlc_lower_bound", "mae_pct_ohlc_upper_bound"]))

    add(["", "## 13. Statistical uncertainty and drawdown", "", "### Risk summary", ""])
    add(_table(risk_summary, list(risk_summary.columns)))
    add(["", "### Drawdown episodes", ""])
    add(_table(drawdown_episodes, ["start_session", "trough_session", "recovery_session", "underwater_sessions", "depth_pnl_rs", "depth_return_points", "recovered"]))
    add(["", "### IID session bootstrap — conditional on the observed strategy and data", ""])
    add(_table(bootstrap, ["scenario", "bootstrap_unit", "bootstrap_replicates", "probability_positive_total_pnl_pct", "total_pnl_rs_p025", "total_pnl_rs_median", "total_pnl_rs_p975", "net_points_p025", "net_points_median", "net_points_p975", "pf_p025", "pf_median", "pf_p975"]))
    add(["", "### Random ordering of the realized 65 daily P&Ls", ""])
    add(_table(order_drawdown, list(order_drawdown.columns)))
    add(
        [
            "",
            "These resamples quantify conditional sampling/order uncertainty only. They do **not** account for Stage 10 being chosen post-hoc, multiple strategy trials, missing sessions/paths, static-universe bias, or the cash-versus-futures execution mismatch. They are not confirmatory p-values.",
        ]
    )

    add(["", "## 14. What the evidence supports—and what it does not", ""])
    add(
        [
            "- **Supported descriptively:** positive reference and both stress cases; both sides and all ten setup buckets remain positive under harsh costs; Stage 4 and Stage 9 changes reproduce exactly in the locked composite; cap-two rejected three historically losing third reservations.",
            "- **Not established:** live futures profitability, true out-of-sample accuracy, causal benefit of any new indicator threshold, or profitability of the 107 excluded five-minute candidates and 24 rejected gaps.",
            "- **Main structural risk:** Stage 10 is post-hoc and July/day activity dominate returns. The forward six sessions were part of the selection gate.",
            "- **Main execution risk:** cash-equity bars and cash-equivalent quantities stand in for rolling futures lots, margins, spreads and impact.",
            "- **Main path risk:** 71 exits depend on last-real-bar handling and 231 excursion paths have boundary ambiguity.",
            "- **Main indicator result:** activity/range/liquidity can increase fill probability, but no individual tested feature shows corrected winner/loss separation. Global tightening is unsupported.",
        ]
    )

    add(["", "## 15. Safe staged improvement plan", ""])
    add(
        [
            "### Stage A — freeze and register",
            "",
            "1. Keep this exact V11 profile/hash unchanged as the benchmark. Record every future test, including failures, before reading results.",
            "2. Keep V10 Stage 0 and isolated Stage 4 as comparators. Do not use this same history to repeatedly redefine the winner.",
            "",
            "### Stage B — repair validity before parameter tuning",
            "",
            "1. Reconstruct point-in-time daily universes and rolling front-month futures contracts, with dated lots/ticks/margins.",
            "2. Bind complete futures 1-minute price paths, observed spreads, impact assumptions and exact 15:30 bars; repair 26-Aug separately.",
            "3. Re-run V10/V11 parity on the repaired data. Reject any improvement that exists only in cash-equity proxy execution or partial terminal paths.",
            "",
            "### Stage C — genuinely prospective evaluation",
            "",
            "1. Freeze thresholds and collect untouched sessions. Report V11, V10 and Stage 4 each day under all three cost cases.",
            "2. Predeclare acceptance gates for PF, net, drawdown, cost robustness and concentration; do not select on a six-session extension already used during development.",
            "3. Use rolling or nested walk-forward selection when enough history exists, leaving a final untouched block.",
            "",
            "### Stage D — first entry-quality hypothesis",
            "",
            "1. Test only a setup-specific **minute-2 SHORT** reconfirmation/quality rule. The weakness is concentrated in 09:25/09:30 SHORT; a global minute-2 ban discards profitable LONG trades.",
            "2. Prefer a causal relative picker-quality, breadth or liquidity margin over excluding exact frozen rank 3. Rank performance here is non-monotonic.",
            "3. Replay each proposal from the complete pre-overlay candidate stream and preserve setup caps/global ledger ordering. One change per stage.",
            "",
            "### Stage E — execution, gap and portfolio tests",
            "",
            "1. Keep the current 2 bps gap guard as control. The accepted 1–2 bps bucket is profitable; no evidence supports tightening it now.",
            "2. Keep same-symbol/same-side cap two. Do not raise it based on three rejected candidates, even though all three lost historically.",
            "3. Stress sector/side clustering and actual futures capital/margin capacity. Current peak cash-equivalent notional is materially above modeled capital.",
            "",
            "### Stage F — exit research only after path repair",
            "",
            "1. Resolve exact terminal bars first because EOD/last-real-bar exits supply a majority of net points.",
            "2. Only then replay predeclared time-stop, break-even or trailing variants. Never filter on realized exit reason, holding duration, MFE or MAE.",
            "",
            "### Stage G — decision rule",
            "",
            "Promote nothing unless it beats frozen V11 and comparators on untouched/repaired data, stays positive under both stress cases, improves or preserves drawdown, avoids new concentration, and has economically executable futures sizing. Otherwise retain V11 unchanged.",
        ]
    )

    add(["", "## 16. Reproducibility and supporting evidence", ""])
    add(
        [
            "Backtest command used for the sealed result:",
            "",
            "```powershell",
            f'cd "{Path(__file__).resolve().parent}"',
            "python -u fno_v11_backtest.py run --all-usable-history",
            "```",
            "",
            "Report command:",
            "",
            "```powershell",
            f'python -u fno_v11_full_historical_report.py --source-run "{source_run}" --report "{report_path}" --assets-dir "{assets_dir}"',
            "```",
            "",
            f"Supporting tables and charts: `{assets_dir}`.",
            f"The sealed backtest directory was read and validated but not modified. The report contains **{len(asset_frames)} CSV tables** and **{len(chart_paths)} charts**.",
            "",
            "Key evidence files include full daily results, all cost/period rows, expanded confirmation checks, all indicator bins/quartiles/tests, setup and symbol tables, component attribution, portfolio timeline, gap rejections, excursions, bootstrap output, and source-coverage tables.",
            "",
            "## 17. Glossary and interpretation",
            "",
            "- **Net return points:** arithmetic sum of per-trade net percentage returns; it is not compounded portfolio return.",
            "- **PF:** gross positive net-return points divided by absolute gross negative net-return points.",
            "- **MDD:** maximum peak-to-trough drawdown of cumulative daily summed return points unless marked Rs.",
            "- **WR:** winning closed trades divided by closed trades.",
            "- **S+N:** Nth one-minute bar after the 5-minute signal candle closes.",
            "- **MFE/MAE:** favorable/adverse post-entry excursion bounded by available OHLC paths; future information, not an entry feature.",
            "- **BH q-value:** Benjamini–Hochberg multiple-test-adjusted p-value; low q reduces, but does not eliminate, false-discovery risk.",
            "- **Research-only:** useful for hypothesis development; not an authorization or estimate of achievable live returns.",
        ]
    )

    common.atomic_write_text(report_path, "\n".join(lines) + "\n")
    output_paths.append(report_path)
    manifest_path = assets_dir / "report_manifest.json"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_ist": datetime.now(gaps.IST),
        "report": str(report_path),
        "report_sha256": _sha256_file(report_path),
        "generator": str(Path(__file__).resolve()),
        "generator_sha256": _sha256_file(Path(__file__).resolve()),
        "source_run": str(source_run),
        "source_provenance": str(provenance_path),
        "source_provenance_sha256": _sha256_file(provenance_path),
        "profile_id": v11.PROFILE_ID,
        "profile_sha256": v11.LOCKED_PROFILE_SHA256,
        "input_binding_sha256": provenance["input_binding_sha256"],
        "source_validation": validation,
        "lineage_run": str(lineage_run) if lineage_run else None,
        "bootstrap_replicates": BOOTSTRAP_REPLICATES,
        "order_replicates": ORDER_REPLICATES,
        "rng_seed": RNG_SEED,
        "outputs": [
            {"path": str(path), "bytes": path.stat().st_size, "sha256": _sha256_file(path)}
            for path in output_paths
        ],
        "headline": headline,
        "research_only": True,
        "headline_valid": False,
        "promotion_eligible": False,
    }
    common.atomic_write_json(manifest_path, gaps._json_ready(manifest))
    return {
        "report": report_path,
        "assets_dir": assets_dir,
        "manifest": manifest_path,
        "csv_tables": len(asset_frames),
        "charts": len(chart_paths),
        "headline": headline,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path)
    parser.add_argument("--lineage-run", type=Path)
    parser.add_argument("--skip-lineage", action="store_true")
    parser.add_argument("--report", type=Path, default=Path("report_v11.md"))
    parser.add_argument("--assets-dir", type=Path, default=Path("report_v11_assets"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    source_run = args.source_run or _resolve_latest(v11.OUTPUT_ROOT)
    if source_run is None:
        raise FileNotFoundError("no completed standalone V11 run was found")
    lineage_run = None
    if not args.skip_lineage:
        lineage_run = args.lineage_run or _resolve_latest(DEFAULT_LINEAGE_ROOT)
    result = build_report(
        source_run=source_run,
        report_path=args.report,
        assets_dir=args.assets_dir,
        lineage_run=lineage_run,
    )
    print(f"[V11-FULL-REPORT] complete: {result['report']}", flush=True)
    print(
        f"[V11-FULL-REPORT] supporting outputs: {result['csv_tables']} CSV + {result['charts']} charts",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
