from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd


ONE_SIDED_95_Z = 1.6448536269514722


def wilson_lower_bound(successes: int, total: int, z: float = ONE_SIDED_95_Z) -> float:
    if total <= 0:
        return float("nan")
    probability = successes / total
    denominator = 1.0 + z * z / total
    centre = probability + z * z / (2.0 * total)
    margin = z * math.sqrt(
        probability * (1.0 - probability) / total + z * z / (4.0 * total * total)
    )
    return max(0.0, (centre - margin) / denominator)


def _normalise_slot_ticker(
    frame: pd.DataFrame,
    *,
    slot_candidates: tuple[str, ...],
    ticker_candidates: tuple[str, ...] = ("ticker", "symbol"),
) -> pd.DataFrame:
    output = frame.copy()
    slot_column = next((column for column in slot_candidates if column in output.columns), None)
    ticker_column = next((column for column in ticker_candidates if column in output.columns), None)
    if slot_column is None or ticker_column is None:
        raise ValueError(
            f"required slot/ticker columns unavailable: slots={slot_candidates} tickers={ticker_candidates}"
        )
    output["_slot"] = pd.to_datetime(output[slot_column], errors="coerce", utc=True)
    output["_ticker"] = output[ticker_column].astype(str).str.upper().str.strip()
    return output.loc[output["_slot"].notna() & output["_ticker"].ne("")].copy()


def evaluate_budget_grid(
    ranking: pd.DataFrame,
    oracle: pd.DataFrame,
    budgets: Iterable[int],
    *,
    universe_count: int | None = None,
    pnl_column: str = "net_pnl_rs",
) -> pd.DataFrame:
    """Measure shortlist recall against an unchanged full-universe oracle."""

    ranked = _normalise_slot_ticker(
        ranking,
        slot_candidates=("slot_ist", "slot", "signal_time_ist"),
    )
    truth = _normalise_slot_ticker(
        oracle,
        slot_candidates=(
            "slot_ist",
            "signal_bar_close_ist",
            "signal_time_ist",
            "signal_datetime",
            "slot",
        ),
    )
    truth = truth.drop_duplicates(["_slot", "_ticker"], keep="first")
    if pnl_column not in truth.columns:
        truth[pnl_column] = np.nan
    truth[pnl_column] = pd.to_numeric(truth[pnl_column], errors="coerce")
    profitable = truth[pnl_column].gt(0.0)
    total_positive_pnl = float(truth.loc[profitable, pnl_column].sum())
    denominator_universe = int(universe_count or ranked.groupby("_slot")["_ticker"].nunique().max() or 0)
    records: list[dict[str, float | int]] = []

    for raw_budget in sorted({int(value) for value in budgets if int(value) > 0}):
        selected_column = f"selected_k{raw_budget}"
        if selected_column in ranked.columns:
            selected = ranked.loc[ranked[selected_column].fillna(False).astype(bool)]
        elif "universe_rank" in ranked.columns:
            selected = ranked.loc[pd.to_numeric(ranked["universe_rank"], errors="coerce").le(raw_budget)]
        else:
            raise ValueError(
                f"ranking needs {selected_column} or universe_rank for budget evaluation"
            )
        selected_keys = selected[["_slot", "_ticker"]].drop_duplicates()
        matched = truth.merge(selected_keys.assign(_captured=True), on=["_slot", "_ticker"], how="left")
        captured = matched["_captured"].eq(True)
        profitable_mask = matched[pnl_column].gt(0.0)
        total_signals = int(len(matched))
        captured_signals = int(captured.sum())
        profitable_signals = int(profitable_mask.sum())
        captured_profitable = int((captured & profitable_mask).sum())
        captured_positive_pnl = float(matched.loc[captured & profitable_mask, pnl_column].sum())
        records.append(
            {
                "budget": raw_budget,
                "total_oracle_signals": total_signals,
                "captured_oracle_signals": captured_signals,
                "all_signal_recall": captured_signals / total_signals if total_signals else np.nan,
                "all_signal_recall_wilson_lower_95": wilson_lower_bound(captured_signals, total_signals),
                "profitable_oracle_signals": profitable_signals,
                "captured_profitable_signals": captured_profitable,
                "profitable_signal_recall": (
                    captured_profitable / profitable_signals if profitable_signals else np.nan
                ),
                "profitable_signal_recall_wilson_lower_95": wilson_lower_bound(
                    captured_profitable, profitable_signals
                ),
                "total_positive_pnl_rs": total_positive_pnl,
                "captured_positive_pnl_rs": captured_positive_pnl,
                "missed_winner_regret_rs": total_positive_pnl - captured_positive_pnl,
                "positive_pnl_capture_ratio": (
                    captured_positive_pnl / total_positive_pnl if total_positive_pnl > 0 else np.nan
                ),
                "workload_reduction": (
                    1.0 - min(raw_budget, denominator_universe) / denominator_universe
                    if denominator_universe > 0
                    else np.nan
                ),
            }
        )
    return pd.DataFrame.from_records(records)
