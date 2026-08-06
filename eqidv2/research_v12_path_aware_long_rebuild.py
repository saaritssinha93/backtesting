"""Rebuild the prefilter long rule with exact, path-aware V12 economics.

This is a standalone research pipeline.  It never mutates production config.
The June 4--August 3 replay is not used to choose the exit, model, selection
fraction, or score threshold.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import avwap_5min_ID_v12_backtesting as v12
import research_v12_ml_long_entry_backtest as prior_replay
import research_v12_prefilter_train_test_optimizer as optimizer


START_DATE = "2026-02-05"
DEVELOPMENT_END = "2026-06-03"
REPLAY_START = "2026-06-04"
REPLAY_END = "2026-08-03"
REPLAY_MIDPOINT = "2026-07-06"
SETUP = "PATH_AWARE_PREFILTER_LONG_V2"
PRODUCTION_APPROVED = False
DAILY_CAP = 15

SOURCE = Path(
    r"C:\TradingData\eqidv2_experiments\prefilter_long_5m_gt5pct_20260205_20260804"
    r"\causal_entry_opportunities_v2.parquet"
)
SESSION_SOURCE = Path(
    r"C:\TradingData\eqidv2_experiments\prefilter_long_5m_gt5pct_20260205_20260804"
    r"\all_long_daily_summary.csv"
)
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_path_aware_long_rebuild_20260205_20260803"
)

SL_GRID = (0.70, 0.90, 1.10, 1.30, 1.50)
TARGET_GRID = (1.00, 1.50, 2.00, 2.50, 3.00)
SELECTION_FRACTIONS = tuple(np.round(np.arange(0.05, 0.505, 0.025), 3))

FEATURES = (
    "selection_rank", "overall_score", "long_score", "activity_score",
    "signal_minute", "RSI", "ADX", "CCI", "MFI", "atr_pct",
    "vwap_dist_atr", "ema20_dist_atr", "ema50_dist_atr",
    "ema200_dist_atr", "macd_hist_atr", "bb_width_atr", "ret_5m_pct",
    "ret_15m_pct", "ret_30m_pct", "ret_60m_pct",
    "session_return_so_far_pct", "gap_pct", "range_pct", "body_pct",
    "upper_wick_pct", "lower_wick_pct", "close_position_in_bar",
    "volume_ratio20", "traded_value_rs",
    "distance_from_running_session_high_atr",
    "distance_from_session_high_pct", "rebound_from_session_low_pct",
    "ema_long_stack",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def sessions() -> tuple[list[str], list[str]]:
    values = sorted(pd.read_csv(SESSION_SOURCE)["trade_date"].astype(str).unique())
    development = [d for d in values if START_DATE <= d <= DEVELOPMENT_END]
    replay = [d for d in values if REPLAY_START <= d <= REPLAY_END]
    if len(development) != 78 or len(replay) != 41:
        raise RuntimeError(f"unexpected session split: development={len(development)}, replay={len(replay)}")
    return development, replay


def load_base_candidates() -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = list(dict.fromkeys([
        "ticker", "trade_date", "primary_side", "membership_slot_ist",
        "selection_rank", "signal_time_ist", "signal_open", "signal_high",
        "signal_low", "signal_close", "pre_entry_data_invalid",
        "entry_execution_time_ist", "entry_price", *FEATURES,
    ]))
    frame = pd.read_parquet(SOURCE, columns=columns)
    funnel = []

    def retain(stage: str, mask: pd.Series) -> None:
        nonlocal frame
        before = len(frame)
        frame = frame.loc[mask.fillna(False)].copy()
        funnel.append({"stage": stage, "before": before, "after": len(frame), "removed": before - len(frame)})

    retain("research_date_window", frame["trade_date"].astype(str).between(START_DATE, REPLAY_END))
    retain("causal_pre_entry_data_valid", frame["pre_entry_data_invalid"].eq(False))
    retain("prefilter_side_long", frame["primary_side"].astype(str).str.upper().eq("LONG"))
    retain("prefilter_rank_200_300", pd.to_numeric(frame["selection_rank"], errors="coerce").between(200, 300))
    retain("v12_signal_time_09_30_to_14_15", pd.to_numeric(frame["signal_minute"], errors="coerce").between(570, 855))
    retain("original_atr_pct_gte_1_05", pd.to_numeric(frame["atr_pct"], errors="coerce") >= 1.05)
    retain("original_range_pct_gte_1_25", pd.to_numeric(frame["range_pct"], errors="coerce") >= 1.25)
    retain("original_vwap_dist_atr_gte_0_05", pd.to_numeric(frame["vwap_dist_atr"], errors="coerce") >= 0.05)

    frame["setup"] = SETUP
    frame["side"] = "LONG"
    frame["bar_time_ist"] = frame["signal_time_ist"]
    frame["decision_ready_at_ist"] = frame["signal_time_ist"]
    frame["decision_ready_source"] = "completed_5min_signal_bar"
    frame["quality_score"] = 301.0 - pd.to_numeric(frame["selection_rank"], errors="coerce")
    frame["score"] = frame["quality_score"]
    frame["research_source_entry_time_ist"] = frame["entry_execution_time_ist"]
    frame["research_source_entry_price"] = frame["entry_price"]
    frame = frame.sort_values(["trade_date", "signal_time_ist", "selection_rank", "ticker"], kind="mergesort").reset_index(drop=True)
    frame["_optimizer_row_id"] = np.arange(len(frame), dtype=int)
    return frame, pd.DataFrame(funnel)


def select_portfolio(frame: pd.DataFrame, score_column: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    work = frame.copy()
    work["score"] = pd.to_numeric(work[score_column], errors="coerce")
    work["quality_score"] = work["score"]
    selected = v12._select_v7_entry_engine_signals(work)
    accepted, _ = prior_replay.apply_daily_cap(selected)
    return accepted


def attach_outcomes(selected: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    if selected.empty:
        return selected.copy()
    outcome_columns = [
        "_optimizer_row_id", "outcome", "entry_time_ist", "entry_price",
        "quantity", "exit_time_ist", "exit_price", "bars_held",
        "gross_pnl_rs", "cost_rs", "net_pnl_rs", "sl_pct", "tgt_pct",
    ]
    return selected.merge(
        outcomes[outcome_columns], on="_optimizer_row_id", how="left",
        validate="one_to_one", suffixes=("", "_outcome"),
    )


def metric_with_days(trades: pd.DataFrame, day_list: list[str]) -> dict[str, Any]:
    return prior_replay.metrics(trades, day_list)


def choose_exit(
    raw_entries: pd.DataFrame,
    development_days: list[str],
) -> tuple[tuple[float, float], pd.DataFrame, pd.DataFrame]:
    base_selected = select_portfolio(raw_entries, "quality_score")
    pairs = [(sl, target) for sl in SL_GRID for target in TARGET_GRID]
    all_outcomes = optimizer.resolve_exit_grid(
        base_selected, {SETUP: pairs}, v12, progress_label="path-aware-exit-development"
    )
    midpoint = len(development_days) // 2
    first_days = development_days[:midpoint]
    second_days = development_days[midpoint:]
    rows = []
    for (sl_pct, tgt_pct), group in all_outcomes.groupby(["sl_pct", "tgt_pct"], sort=True):
        dev = group.loc[group["trade_date"].le(DEVELOPMENT_END)]
        overall = metric_with_days(dev, development_days)
        first = metric_with_days(dev.loc[dev["trade_date"].isin(first_days)], first_days)
        second = metric_with_days(dev.loc[dev["trade_date"].isin(second_days)], second_days)
        rows.append({
            "sl_pct": float(sl_pct), "tgt_pct": float(tgt_pct),
            **{f"development_{k}": v for k, v in overall.items()},
            "first_half_net_pnl_rs": first["net_pnl_rs"],
            "second_half_net_pnl_rs": second["net_pnl_rs"],
            "worst_half_net_pnl_rs": min(first["net_pnl_rs"], second["net_pnl_rs"]),
        })
    summary = pd.DataFrame(rows)
    summary["robust_rank"] = (
        summary["development_net_pnl_rs"]
        + 0.25 * summary["worst_half_net_pnl_rs"]
        - 0.10 * summary["development_max_drawdown_rs"].abs()
    )
    summary = summary.sort_values(
        ["robust_rank", "development_net_pnl_rs", "development_profit_factor"],
        ascending=False, kind="mergesort",
    ).reset_index(drop=True)
    winner = (float(summary.iloc[0]["sl_pct"]), float(summary.iloc[0]["tgt_pct"]))
    return winner, summary, base_selected


def new_model() -> Any:
    return make_pipeline(
        SimpleImputer(strategy="median"),
        StandardScaler(),
        LogisticRegression(
            C=0.10, class_weight="balanced", max_iter=2000,
            random_state=20260805,
        ),
    )


def row_weights(frame: pd.DataFrame) -> pd.Series:
    keys = frame["ticker"].astype(str) + "|" + frame["trade_date"].astype(str)
    return 1.0 / keys.groupby(keys).transform("size")


def fit_model(frame: pd.DataFrame) -> Any:
    model = new_model()
    model.fit(
        frame[list(FEATURES)], frame["path_positive"],
        logisticregression__sample_weight=row_weights(frame),
    )
    return model


def threshold_for_fraction(scores: np.ndarray, fraction: float) -> float:
    return float(np.quantile(np.asarray(scores, dtype=float), 1.0 - float(fraction)))


def choose_selection_fraction(
    labelled: pd.DataFrame,
    development_days: list[str],
) -> tuple[float, pd.DataFrame, pd.DataFrame]:
    bounds = ((0, 34, 34, 49), (0, 49, 49, 64), (0, 64, 64, len(development_days)))
    fold_frames = []
    fold_contracts = []
    for fold_number, (train_start, train_end, test_start, test_end) in enumerate(bounds, 1):
        train_days = development_days[train_start:train_end]
        test_days = development_days[test_start:test_end]
        train = labelled.loc[labelled["trade_date"].isin(train_days)].copy()
        validation = labelled.loc[labelled["trade_date"].isin(test_days)].copy()
        model = fit_model(train)
        train_scores = model.predict_proba(train[list(FEATURES)])[:, 1]
        validation["oof_score"] = model.predict_proba(validation[list(FEATURES)])[:, 1]
        validation["oof_fold"] = f"F{fold_number}"
        validation["oof_fold_days"] = len(test_days)
        fold_frames.append(validation)
        fold_contracts.append({
            "fold": f"F{fold_number}", "train_start": train_days[0],
            "train_end": train_days[-1], "validation_start": test_days[0],
            "validation_end": test_days[-1], "train_rows": len(train),
            "validation_rows": len(validation), "train_scores": train_scores,
        })
    oof = pd.concat(fold_frames, ignore_index=True)
    oof_days = development_days[34:]
    rows = []
    for fraction in SELECTION_FRACTIONS:
        selected_parts = []
        thresholds = {}
        for contract in fold_contracts:
            fold = contract["fold"]
            threshold = threshold_for_fraction(contract["train_scores"], fraction)
            thresholds[fold] = threshold
            eligible = oof.loc[
                oof["oof_fold"].eq(fold) & oof["oof_score"].ge(threshold)
            ]
            selected_parts.append(select_portfolio(eligible, "oof_score"))
        selected = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
        overall = metric_with_days(selected, oof_days)
        fold_nets = {
            fold: float(selected.loc[selected["oof_fold"].eq(fold), "net_pnl_rs"].sum())
            for fold in ("F1", "F2", "F3")
        }
        rows.append({
            "selection_fraction": float(fraction), **overall,
            "positive_folds": sum(value > 0 for value in fold_nets.values()),
            "worst_fold_net_pnl_rs": min(fold_nets.values()),
            "fold1_net_pnl_rs": fold_nets["F1"],
            "fold2_net_pnl_rs": fold_nets["F2"],
            "fold3_net_pnl_rs": fold_nets["F3"],
            "fold1_threshold": thresholds["F1"],
            "fold2_threshold": thresholds["F2"],
            "fold3_threshold": thresholds["F3"],
        })
    search = pd.DataFrame(rows)
    search["active_trade_days"] = search["sessions"] - search["zero_trade_sessions"]
    eligible = search.loc[(search["trades"] >= 30) & (search["active_trade_days"] >= 20)].copy()
    if eligible.empty:
        raise RuntimeError("no OOF selection fraction met minimum evidence")
    eligible["robust_rank"] = (
        eligible["net_pnl_rs"]
        + 0.25 * eligible["worst_fold_net_pnl_rs"]
        - 0.10 * eligible["max_drawdown_rs"].abs()
    )
    eligible = eligible.sort_values(
        ["positive_folds", "robust_rank", "profit_factor", "trades"],
        ascending=False, kind="mergesort",
    )
    winner = float(eligible.iloc[0]["selection_fraction"])
    return winner, search.sort_values("selection_fraction").reset_index(drop=True), oof


def choose_joint_path_contract(
    raw_entries: pd.DataFrame,
    development_days: list[str],
) -> tuple[tuple[float, float, float], pd.DataFrame, pd.DataFrame]:
    """Jointly choose barrier geometry and gate density on development OOF only."""
    pairs = [(sl, target) for sl in SL_GRID for target in TARGET_GRID]
    all_outcomes = optimizer.resolve_exit_grid(
        raw_entries, {SETUP: pairs}, v12, progress_label="joint-path-outcomes"
    )
    rows = []
    for sl_pct, tgt_pct in pairs:
        pair_outcomes = all_outcomes.loc[
            all_outcomes["sl_pct"].eq(sl_pct) & all_outcomes["tgt_pct"].eq(tgt_pct)
        ]
        labelled = attach_outcomes(raw_entries, pair_outcomes)
        labelled["path_positive"] = (
            labelled["outcome"].eq("TARGET") & labelled["net_pnl_rs"].gt(0)
        ).astype(int)
        development = labelled.loc[labelled["trade_date"].le(DEVELOPMENT_END)].copy()
        fraction, search, _ = choose_selection_fraction(development, development_days)
        winner = search.loc[search["selection_fraction"].eq(fraction)].iloc[0].to_dict()
        robust_rank = (
            float(winner["net_pnl_rs"])
            + 0.25 * float(winner["worst_fold_net_pnl_rs"])
            - 0.10 * abs(float(winner["max_drawdown_rs"]))
        )
        rows.append({
            "sl_pct": float(sl_pct), "tgt_pct": float(tgt_pct),
            "selection_fraction": float(fraction), "robust_rank": robust_rank,
            **{f"oof_{key}": value for key, value in winner.items() if key != "selection_fraction"},
        })
        print(
            f"[joint path] sl={sl_pct:.2f} target={tgt_pct:.2f} "
            f"fraction={fraction:.3f} trades={int(winner['trades'])} "
            f"net={float(winner['net_pnl_rs']):.2f} "
            f"pf={float(winner['profit_factor']):.3f} "
            f"positive_folds={int(winner['positive_folds'])}",
            flush=True,
        )
    joint = pd.DataFrame(rows).sort_values(
        ["oof_positive_folds", "robust_rank", "oof_profit_factor", "oof_trades"],
        ascending=False, kind="mergesort",
    ).reset_index(drop=True)
    winner = joint.iloc[0]
    contract = (
        float(winner["sl_pct"]), float(winner["tgt_pct"]),
        float(winner["selection_fraction"]),
    )
    return contract, joint, all_outcomes


def model_coefficients(model: Any) -> pd.DataFrame:
    values = model.named_steps["logisticregression"].coef_[0]
    out = pd.DataFrame({"feature": FEATURES, "standardised_coefficient": values})
    out["absolute_coefficient"] = out["standardised_coefficient"].abs()
    return out.sort_values("absolute_coefficient", ascending=False).reset_index(drop=True)


def write_config(
    path: Path,
    *,
    sl_pct: float,
    target_pct: float,
    selection_fraction: float,
    probability_threshold: float,
) -> None:
    content = f'''"""Frozen research-only path-aware V12 long entry gate."""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
SETUP = {SETUP!r}
SIDE = "LONG"
PREFILTER_RANK_MIN = 200
PREFILTER_RANK_MAX = 300
SIGNAL_TIMEFRAME = "5min"
SIGNAL_MINUTE_MIN = 570
SIGNAL_MINUTE_MAX = 855
BASE_CONDITIONS = (
    ("atr_pct", ">=", 1.05),
    ("range_pct", ">=", 1.25),
    ("vwap_dist_atr", ">=", 0.05),
)
ENTRY_POLICY = "V12_NEXT_AVAILABLE_1MIN_OPEN_SIGNAL_PLUS_1"
ENTRY_ADVERSE_SLIPPAGE_PCT = 0.0005
SL_PCT = {sl_pct!r}
TARGET_PCT = {target_pct!r}
EXIT_RESOLUTION = "EXACT_1MIN_TARGET_BEFORE_STOP_THEN_EOD"
COST_MODEL = "NSE_STATUTORY_INTRADAY_EQUITY"
MODEL_FILE = "path_aware_logistic_model.joblib"
MODEL_FEATURES = {FEATURES!r}
MODEL_PROBABILITY_THRESHOLD = {probability_threshold!r}
OOF_SELECTION_FRACTION = {selection_fraction!r}
ONE_ENTRY_PER_TICKER_DAY = True
DAILY_ENTRY_CAP = 15
REPLAY_START = {REPLAY_START!r}
REPLAY_END = {REPLAY_END!r}
REPLAY_IS_FRESH_HOLDOUT = False

def base_matches(features):
    try:
        minute = float(features["signal_minute"])
        rank = float(features["selection_rank"])
        return (
            570 <= minute <= 855
            and 200 <= rank <= 300
            and float(features["atr_pct"]) >= 1.05
            and float(features["range_pct"]) >= 1.25
            and float(features["vwap_dist_atr"]) >= 0.05
        )
    except (KeyError, TypeError, ValueError):
        return False

def model_accepts(probability):
    return float(probability) >= MODEL_PROBABILITY_THRESHOLD
'''
    path.write_text(content, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    development_days, replay_days = sessions()
    candidates, funnel = load_base_candidates()

    loader = optimizer.install_windowed_1m_loader(v12, start_date=START_DATE, end_date=REPLAY_END)
    prewarm = optimizer.prewarm_windowed_1m_loader(loader, candidates["ticker"], workers=8)
    optimizer.install_day_1m_adapter(v12, loader)
    v12._V11_EXACT_LIVE_PARITY = False
    v12._V11_COST_MODEL = "statutory"
    v12._V11_SLIPPAGE_BPS = 0.0

    old_exit = v12.v6.SETUP_EXIT_RULES.get(SETUP)
    v12.v6.SETUP_EXIT_RULES[SETUP] = (1.10, 2.50)
    try:
        raw_entries, entry_rejects = v12._v7_entry_engine_raw_rows(candidates)
    finally:
        if old_exit is None:
            v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v12.v6.SETUP_EXIT_RULES[SETUP] = old_exit
    raw_entries["_optimizer_row_id"] = pd.to_numeric(raw_entries["_optimizer_row_id"], errors="raise").astype(int)
    raw_entries["trade_date"] = pd.to_datetime(raw_entries["signal_time_ist"], utc=True).dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d")
    funnel = pd.concat([funnel, pd.DataFrame([{
        "stage": "v12_executable_next_1min_entry", "before": len(candidates),
        "after": len(raw_entries), "removed": len(candidates) - len(raw_entries),
    }])], ignore_index=True)

    (sl_pct, target_pct, selection_fraction), joint_search, all_path_outcomes = (
        choose_joint_path_contract(raw_entries, development_days)
    )
    chosen_outcomes = all_path_outcomes.loc[
        all_path_outcomes["sl_pct"].eq(sl_pct)
        & all_path_outcomes["tgt_pct"].eq(target_pct)
    ].copy()
    labelled = attach_outcomes(raw_entries, chosen_outcomes)
    if labelled["net_pnl_rs"].isna().any():
        raise RuntimeError("path-aware outcome coverage failure")
    labelled["path_positive"] = (
        labelled["outcome"].eq("TARGET") & labelled["net_pnl_rs"].gt(0)
    ).astype(int)

    development = labelled.loc[labelled["trade_date"].le(DEVELOPMENT_END)].copy()
    frozen_fraction, threshold_search, oof = choose_selection_fraction(development, development_days)
    if not math.isclose(frozen_fraction, selection_fraction, abs_tol=1e-12):
        raise RuntimeError("joint selection fraction was not reproducible")
    final_model = fit_model(development)
    development["final_model_score"] = final_model.predict_proba(development[list(FEATURES)])[:, 1]
    probability_threshold = threshold_for_fraction(development["final_model_score"].to_numpy(), selection_fraction)

    replay = labelled.loc[labelled["trade_date"].between(REPLAY_START, REPLAY_END)].copy()
    replay["final_model_score"] = final_model.predict_proba(replay[list(FEATURES)])[:, 1]
    replay_eligible = replay.loc[replay["final_model_score"].ge(probability_threshold)].copy()
    replay_trades = select_portfolio(replay_eligible, "final_model_score")
    replay_trades = replay_trades.sort_values(["trade_date", "entry_time_ist", "ticker"], kind="mergesort").reset_index(drop=True)

    first_days = [d for d in replay_days if d <= REPLAY_MIDPOINT]
    second_days = [d for d in replay_days if d > REPLAY_MIDPOINT]
    replay_metrics = metric_with_days(replay_trades, replay_days)
    split_metrics = {
        "first_half": metric_with_days(replay_trades.loc[replay_trades["trade_date"].le(REPLAY_MIDPOINT)], first_days),
        "second_half": metric_with_days(replay_trades.loc[replay_trades["trade_date"].gt(REPLAY_MIDPOINT)], second_days),
    }

    sensitivity_outcomes = optimizer.resolve_exit_grid(
        replay_trades, {SETUP: [(sl, target) for sl in SL_GRID for target in TARGET_GRID]},
        v12, progress_label="path-aware-replay-sensitivity",
    ) if not replay_trades.empty else pd.DataFrame()
    sensitivity = prior_replay.grid_summary(sensitivity_outcomes, replay_days) if not sensitivity_outcomes.empty else pd.DataFrame()
    daily = prior_replay.daily_summary(replay_trades, replay_days)
    hourly = prior_replay.hourly_summary(replay_trades)
    coefficients = model_coefficients(final_model)

    oof_winner = threshold_search.loc[threshold_search["selection_fraction"].eq(selection_fraction)].iloc[0].to_dict()
    performance_pass = bool(
        replay_metrics["trades"] >= 30
        and replay_metrics["net_pnl_rs"] > 0
        and replay_metrics["profit_factor"] >= 1.15
        and split_metrics["first_half"]["net_pnl_rs"] > 0
        and split_metrics["second_half"]["net_pnl_rs"] > 0
    )
    contract = {
        "production_approved": False,
        "research_only": True,
        "setup": SETUP,
        "development_window": [START_DATE, DEVELOPMENT_END, len(development_days)],
        "replay_window": [REPLAY_START, REPLAY_END, len(replay_days)],
        "replay_used_for_selection": False,
        "replay_is_fresh_holdout": False,
        "base_rule": {
            "side": "LONG", "rank": [200, 300], "signal_minutes": [570, 855],
            "atr_pct_gte": 1.05, "range_pct_gte": 1.25,
            "vwap_dist_atr_gte": 0.05,
        },
        "path_label": {
            "sl_pct": sl_pct, "target_pct": target_pct,
            "definition": "exact V12 target-before-stop with next-1min entry, 5bps adverse entry fill, statutory costs, and EOD resolution",
        },
        "model": {
            "family": "median-imputed standardised L2 logistic regression",
            "C": 0.10, "class_weight": "balanced",
            "features": FEATURES, "selection_fraction": selection_fraction,
            "probability_threshold": probability_threshold,
            "threshold_selection": "three expanding date-block OOF folds; fraction selected without replay P&L; final static probability threshold is the matching development-score quantile",
        },
        "portfolio": {"one_entry_per_ticker_day": True, "daily_cap": DAILY_CAP},
        "promotion_gate": {"minimum_trades": 30, "minimum_pf": 1.15, "net_positive": True, "both_replay_halves_positive": True},
    }
    summary = {
        "contract": contract, "candidate_funnel": funnel.to_dict("records"),
        "prewarm_1m": prewarm, "entry_rejects": int(len(entry_rejects)),
        "development_joint_winner": joint_search.iloc[0].to_dict(),
        "oof_selection_winner": oof_winner,
        "replay_results": replay_metrics, "replay_split_results": split_metrics,
        "performance_gate_passed": performance_pass,
        "production_approved": False,
        "verdict": "RESEARCH_CANDIDATE_REQUIRES_FRESH_HOLDOUT" if performance_pass else "REJECTED_NO_VIABLE_CORRECTED_SETUP",
    }

    config_path = OUTPUT_DIR / "path_aware_long_v2_conf.py"
    write_config(
        config_path, sl_pct=sl_pct, target_pct=target_pct,
        selection_fraction=selection_fraction,
        probability_threshold=probability_threshold,
    )
    joblib.dump(final_model, OUTPUT_DIR / "path_aware_logistic_model.joblib")
    funnel.to_csv(OUTPUT_DIR / "candidate_funnel.csv", index=False)
    joint_search.to_csv(OUTPUT_DIR / "development_exit_search.csv", index=False)
    joint_search.to_csv(OUTPUT_DIR / "development_joint_path_search.csv", index=False)
    threshold_search.to_csv(OUTPUT_DIR / "oof_selection_fraction_search.csv", index=False)
    oof.to_csv(OUTPUT_DIR / "oof_scored_rows.csv", index=False)
    coefficients.to_csv(OUTPUT_DIR / "model_coefficients.csv", index=False)
    replay[["_optimizer_row_id", "ticker", "trade_date", "signal_time_ist", "final_model_score", "path_positive", "outcome", "net_pnl_rs"]].to_csv(OUTPUT_DIR / "replay_scored_candidates.csv", index=False)
    replay_trades.to_csv(OUTPUT_DIR / "replay_trades.csv", index=False)
    daily.to_csv(OUTPUT_DIR / "replay_daily_summary.csv", index=False)
    hourly.to_csv(OUTPUT_DIR / "replay_hourly_summary.csv", index=False)
    sensitivity.to_csv(OUTPUT_DIR / "replay_exit_sensitivity.csv", index=False)
    if entry_rejects.empty:
        entry_rejects = pd.DataFrame(columns=["ticker", "signal_time_ist", "reject_reason"])
    entry_rejects.to_csv(OUTPUT_DIR / "entry_engine_rejects.csv", index=False)
    (OUTPUT_DIR / "contract.json").write_text(json.dumps(json_safe(contract), indent=2), encoding="utf-8")
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(json_safe(summary), indent=2), encoding="utf-8")

    report = f"""# Corrected path-aware V12 long-entry rebuild

## Verdict

**{summary['verdict']}**. Production approval remains false.

The entry label now requires the exact V12 target to occur before the stop, using the V12 next-1-minute entry, 5 bps adverse entry fill, statutory costs, risk sizing, and EOD resolver. June 4--August 3 P&L was not used to select the exit, model, selection fraction, or probability threshold.

## Frozen corrected setup

- Base filter: LONG hourly-prefilter ranks 200--300; completed 5-minute ATR% >= 1.05, range% >= 1.25, VWAP distance/ATR >= 0.05; 09:30--14:15.
- Exit selected on development only: SL {sl_pct:.2f}%, target {target_pct:.2f}%.
- Model: L2 logistic regression over {len(FEATURES)} causal signal-time features.
- OOF selection fraction: {selection_fraction:.3f}; frozen final probability threshold: {probability_threshold:.6f}.
- One entry per ticker/day; daily cap {DAILY_CAP}.

## Requested two-month replay

- Trades: {replay_metrics['trades']}; trades/session: {replay_metrics['trades_per_session']:.2f}; median/day: {replay_metrics['median_trades_per_session']:.1f}.
- Net P&L: Rs {replay_metrics['net_pnl_rs']:,.2f}; PF: {(replay_metrics['profit_factor'] or 0):.3f}; win rate: {replay_metrics['win_rate_pct']:.2f}%.
- Max drawdown: Rs {replay_metrics['max_drawdown_rs']:,.2f}.
- First half net: Rs {split_metrics['first_half']['net_pnl_rs']:,.2f}; second half net: Rs {split_metrics['second_half']['net_pnl_rs']:,.2f}.

## Honesty constraint

This replay is methodologically isolated from the new threshold selection, but it is not a virgin holdout because its dates were already inspected in earlier research. Even a passing result requires a genuinely fresh post-freeze holdout before promotion.
"""
    (OUTPUT_DIR / "RESEARCH_REPORT.md").write_text(report, encoding="utf-8")

    artifacts = []
    for path in sorted(OUTPUT_DIR.iterdir()):
        if path.is_file() and path.name != "integrity_manifest.json":
            artifacts.append({"file": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    manifest = {"production_approved": False, "artifact_count": len(artifacts), "artifacts": artifacts}
    (OUTPUT_DIR / "integrity_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(json_safe(summary), indent=2))


if __name__ == "__main__":
    main()
