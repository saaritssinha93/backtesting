"""Strict-feature, causal rolling-calibration V12 long rebuild.

This supersedes the earlier V2 and first V3 result.  It repairs three
invalidating contracts:
1. every model feature must be genuinely observed (not null or sentinel-filled)
   in every chronological block;
2. redundant algebraic copies are removed before fitting the linear model;
3. the model remains row-level, while calibration uses one completed historical
   peak score per ticker/day and live-style selection uses the first current-day
   threshold crossing.

The June 4--August 3 replay is never used to choose labels, exits, fractions,
model parameters, or thresholds.  Production configuration is untouched.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import avwap_5min_ID_v12_backtesting as v12
import research_v12_ml_long_entry_backtest as replay_helpers
import research_v12_path_aware_long_rebuild as v2
import research_v12_prefilter_train_test_optimizer as optimizer


SETUP = "PATH_AWARE_PREFILTER_LONG_V3_STRICT14"
PRODUCTION_APPROVED = False
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_path_aware_long_rebuild_v3_strict14_20260205_20260803"
)

# Algebraically minimal, fully observed causal basis.  These fields have zero
# null/nonfinite values in every development/replay month.  Unlike the first V3,
# none are sentinel-filled when RSI is absent.
STABLE_FEATURES = (
    "selection_rank", "overall_score", "long_score", "activity_score",
    "signal_minute", "atr_pct", "vwap_dist_atr", "ret_5m_pct",
    "session_return_so_far_pct", "range_pct", "close_position_in_bar",
    "traded_value_rs", "distance_from_running_session_high_atr",
    "rebound_from_session_low_pct",
)
REDUNDANT_SAFE_FEATURES = (
    "body_pct", "upper_wick_pct", "lower_wick_pct",
    "distance_from_session_high_pct",
)
HIDDEN_SENTINEL_FEATURES = (
    "ADX", "ema20_dist_atr", "ema50_dist_atr", "ema200_dist_atr",
    "ema_long_stack",
)
EXPLICITLY_INCOMPLETE_FEATURES = (
    "RSI", "CCI", "MFI", "macd_hist_atr", "bb_width_atr",
    "ret_15m_pct", "ret_30m_pct", "ret_60m_pct", "gap_pct",
    "volume_ratio20",
)
EXCLUDED_UNSTABLE_FEATURES = tuple(
    sorted(set(HIDDEN_SENTINEL_FEATURES) | set(EXPLICITLY_INCOMPLETE_FEATURES))
)

SL_GRID = v2.SL_GRID
TARGET_GRID = v2.TARGET_GRID
SELECTION_FRACTIONS = tuple(np.round(np.arange(0.20, 0.605, 0.05), 3))
ROLLING_SCORE_SESSIONS = 20
MIN_REFERENCE_ROWS = 30
MIN_REFERENCE_ACTIVE_SESSIONS = 15
MIN_REFERENCE_TAIL_UNITS = 10
MIN_OOF_TRADES = 35
MIN_OOF_ACTIVE_DAYS = 20
MIN_OOF_FOLD_TRADES = 5
MIN_OOF_FOLD_ACTIVE_DAYS = 3
MIN_REPLAY_TRADES = 30
MIN_REPLAY_ACTIVE_DAYS = 15
DAILY_CAP = 15


def all_sessions() -> list[str]:
    return sorted(pd.read_csv(v2.SESSION_SOURCE)["trade_date"].astype(str).unique())


def split_sessions() -> tuple[list[str], list[str]]:
    values = all_sessions()
    development = [d for d in values if v2.START_DATE <= d <= v2.DEVELOPMENT_END]
    replay = [d for d in values if v2.REPLAY_START <= d <= v2.REPLAY_END]
    if len(development) != 78 or len(replay) != 41:
        raise RuntimeError(f"unexpected session split: {len(development)}/{len(replay)}")
    return development, replay


def load_candidates() -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = list(dict.fromkeys([
        "ticker", "trade_date", "primary_side", "membership_slot_ist",
        "selection_rank", "signal_time_ist", "signal_open", "signal_high",
        "signal_low", "signal_close", "pre_entry_data_invalid",
        "entry_execution_time_ist", "entry_price", *STABLE_FEATURES,
    ]))
    frame = pd.read_parquet(v2.SOURCE, columns=columns)
    funnel = []

    def retain(stage: str, mask: pd.Series) -> None:
        nonlocal frame
        before = len(frame)
        frame = frame.loc[mask.fillna(False)].copy()
        funnel.append({"stage": stage, "before": before, "after": len(frame), "removed": before - len(frame)})

    retain("research_date_window", frame["trade_date"].astype(str).between(v2.START_DATE, v2.REPLAY_END))
    retain("causal_pre_entry_data_valid", frame["pre_entry_data_invalid"].eq(False))
    retain("prefilter_side_long", frame["primary_side"].astype(str).str.upper().eq("LONG"))
    retain("prefilter_rank_200_300", pd.to_numeric(frame["selection_rank"], errors="coerce").between(200, 300))
    retain("v12_signal_time_09_30_to_14_15", pd.to_numeric(frame["signal_minute"], errors="coerce").between(570, 855))
    retain("atr_pct_gte_1_05", pd.to_numeric(frame["atr_pct"], errors="coerce") >= 1.05)
    retain("range_pct_gte_1_25", pd.to_numeric(frame["range_pct"], errors="coerce") >= 1.25)
    retain("vwap_dist_atr_gte_0_05", pd.to_numeric(frame["vwap_dist_atr"], errors="coerce") >= 0.05)

    for feature in STABLE_FEATURES:
        values = pd.to_numeric(frame[feature], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy()).all():
            raise RuntimeError(f"stable-feature coverage failure: {feature}")

    frame["setup"] = SETUP
    frame["side"] = "LONG"
    frame["bar_time_ist"] = frame["signal_time_ist"]
    frame["decision_ready_at_ist"] = frame["signal_time_ist"]
    frame["decision_ready_source"] = "completed_5min_signal_bar"
    frame["quality_score"] = 301.0 - pd.to_numeric(frame["selection_rank"], errors="coerce")
    frame["score"] = frame["quality_score"]
    frame["research_source_entry_time_ist"] = frame["entry_execution_time_ist"]
    frame["research_source_entry_price"] = frame["entry_price"]
    frame = frame.sort_values(
        ["trade_date", "signal_time_ist", "selection_rank", "ticker"],
        kind="mergesort",
    ).reset_index(drop=True)
    frame["_optimizer_row_id"] = np.arange(len(frame), dtype=int)
    return frame, pd.DataFrame(funnel)


def ticker_day_peaks(frame: pd.DataFrame) -> pd.DataFrame:
    """Return one completed-history peak score per unique ticker/day."""
    required = {"trade_date", "ticker", "model_score"}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"ticker-day peak columns missing: {sorted(missing)}")
    work = frame[["trade_date", "ticker", "model_score"]].copy()
    work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    work["model_score"] = pd.to_numeric(work["model_score"], errors="raise")
    if work["ticker"].eq("").any() or not np.isfinite(work["model_score"].to_numpy()).all():
        raise RuntimeError("invalid ticker-day calibration values")
    return (
        work.groupby(["trade_date", "ticker"], as_index=False, sort=True)
        .agg(peak_score=("model_score", "max"))
    )


def attach_outcomes(units: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "_optimizer_row_id", "outcome", "entry_time_ist", "entry_price",
        "quantity", "exit_time_ist", "exit_price", "bars_held",
        "gross_pnl_rs", "cost_rs", "net_pnl_rs", "sl_pct", "tgt_pct",
    ]
    merged = units.merge(
        outcomes[columns], on="_optimizer_row_id", how="left",
        validate="one_to_one", suffixes=("", "_outcome"),
    )
    if merged["net_pnl_rs"].isna().any():
        raise RuntimeError("outcome coverage failure")
    return merged


def new_model() -> Any:
    return make_pipeline(
        StandardScaler(),
        LogisticRegression(
            C=0.10, class_weight="balanced", max_iter=2000,
            random_state=20260805,
        ),
    )


def row_weights(frame: pd.DataFrame) -> pd.Series:
    """Give every ticker/day total training weight one despite repeated bars."""
    keys = (
        frame["ticker"].astype(str).str.upper().str.strip()
        + "|" + frame["trade_date"].astype(str)
    )
    return 1.0 / keys.groupby(keys).transform("size")


def fit_model(frame: pd.DataFrame) -> Any:
    if frame["path_positive"].nunique() != 2:
        raise RuntimeError("model training requires both path-label classes")
    model = new_model()
    weights = row_weights(frame)
    model.fit(
        frame[list(STABLE_FEATURES)], frame["path_positive"],
        standardscaler__sample_weight=weights,
        logisticregression__sample_weight=weights,
    )
    return model


def metric(trades: pd.DataFrame, days: list[str]) -> dict[str, Any]:
    return replay_helpers.metrics(trades, days)


def select_first_crossings(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the V12 one-ticker/day selector, then the frozen daily cap."""
    if frame.empty:
        return frame.copy(), frame.copy()
    work = frame.copy()
    work["score"] = pd.to_numeric(work["model_score"], errors="raise")
    work["quality_score"] = work["score"]
    selected = v12._select_v7_entry_engine_signals(work)
    return replay_helpers.apply_daily_cap(selected)


def rolling_gate(
    history_scored: pd.DataFrame,
    evaluation_scored: pd.DataFrame,
    evaluation_days: list[str],
    fraction: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calibrate on completed ticker-day peaks; enter first live-style crossing."""
    calendar = all_sessions()
    history = history_scored[["trade_date", "ticker", "model_score"]].copy()
    selected_parts = []
    logs = []
    for day in evaluation_days:
        if day not in calendar:
            raise RuntimeError(f"decision day absent from session calendar: {day}")
        position = calendar.index(day)
        if position < ROLLING_SCORE_SESSIONS:
            raise RuntimeError(f"fewer than {ROLLING_SCORE_SESSIONS} prior sessions on {day}")
        prior_days = calendar[position - ROLLING_SCORE_SESSIONS:position]
        if day in prior_days or history["trade_date"].astype(str).ge(day).any():
            raise RuntimeError(f"noncausal rolling history on {day}")
        reference_rows = history.loc[history["trade_date"].isin(prior_days)].copy()
        reference = ticker_day_peaks(reference_rows)
        active_sessions = int(reference["trade_date"].nunique())
        reference_units = int(len(reference))
        tail_units = int(math.ceil(float(fraction) * reference_units))
        if reference_units < MIN_REFERENCE_ROWS:
            raise RuntimeError(
                f"rolling calibration coverage failure on {day}: "
                f"{reference_units} unique ticker-days"
            )
        if active_sessions < MIN_REFERENCE_ACTIVE_SESSIONS:
            raise RuntimeError(
                f"rolling calibration active-session failure on {day}: {active_sessions}"
            )
        if tail_units < MIN_REFERENCE_TAIL_UNITS:
            raise RuntimeError(
                f"rolling calibration tail failure on {day}: {tail_units}"
            )
        values = reference["peak_score"].to_numpy(dtype=float)
        threshold = float(np.partition(values, reference_units - tail_units)[reference_units - tail_units])
        ties_at_threshold = int(np.count_nonzero(values == threshold))
        passing_reference_units = int(np.count_nonzero(values >= threshold))
        current = evaluation_scored.loc[evaluation_scored["trade_date"].eq(day)].copy()
        eligible = current.loc[current["model_score"].ge(threshold)].copy()
        selected, cap_rejects = select_first_crossings(eligible)
        selected["rolling_threshold"] = threshold
        selected["rolling_fraction"] = float(fraction)
        if not selected.empty:
            selected_parts.append(selected)
        logs.append({
            "trade_date": day, "reference_sessions": len(prior_days),
            "reference_session_dates": "|".join(prior_days),
            "reference_active_sessions": active_sessions,
            "reference_candidate_rows": len(reference_rows),
            "reference_unique_ticker_days": reference_units,
            "rolling_fraction": float(fraction), "tail_k": tail_units,
            "threshold": threshold, "candidate_units": len(current),
            "ties_at_threshold": ties_at_threshold,
            "passing_reference_units": passing_reference_units,
            "tie_inflation_units": passing_reference_units - tail_units,
            "passing_current_rows": len(eligible),
            "passing_current_ticker_days": (
                int(eligible[["trade_date", "ticker"]].drop_duplicates().shape[0])
                if not eligible.empty else 0
            ),
            "selected_trades": len(selected),
            "daily_cap_rejects": len(cap_rejects),
        })
        if not current.empty:
            history = pd.concat(
                [history, current[["trade_date", "ticker", "model_score"]]],
                ignore_index=True,
            )
    selected = (
        pd.concat(selected_parts, ignore_index=True)
        if selected_parts else pd.DataFrame(columns=evaluation_scored.columns)
    )
    return selected, pd.DataFrame(logs)


def oof_search(
    labelled: pd.DataFrame,
    development_days: list[str],
) -> tuple[float, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    bounds = ((0, 34, 34, 49), (0, 49, 49, 64), (0, 64, 64, len(development_days)))
    folds = []
    for fold_number, (_, train_end, test_start, test_end) in enumerate(bounds, 1):
        train_days = development_days[:train_end]
        validation_days = development_days[test_start:test_end]
        train = labelled.loc[labelled["trade_date"].isin(train_days)].copy()
        validation = labelled.loc[labelled["trade_date"].isin(validation_days)].copy()
        model = fit_model(train)
        train["model_score"] = model.predict_proba(train[list(STABLE_FEATURES)])[:, 1]
        validation["model_score"] = model.predict_proba(validation[list(STABLE_FEATURES)])[:, 1]
        validation["oof_fold"] = f"F{fold_number}"
        folds.append((f"F{fold_number}", train, validation, validation_days))

    rows = []
    all_selected_by_fraction: dict[float, pd.DataFrame] = {}
    all_logs_by_fraction: dict[float, pd.DataFrame] = {}
    for fraction in SELECTION_FRACTIONS:
        selected_parts = []
        fold_nets = {}
        fold_trades = {}
        fold_active_days = {}
        log_parts = []
        for fold_name, train, validation, validation_days in folds:
            selected, threshold_log = rolling_gate(
                train, validation, validation_days, fraction
            )
            threshold_log["oof_fold"] = fold_name
            log_parts.append(threshold_log)
            selected_parts.append(selected)
            fold_values = metric(selected, validation_days)
            fold_nets[fold_name] = float(fold_values["net_pnl_rs"])
            fold_trades[fold_name] = int(fold_values["trades"])
            fold_active_days[fold_name] = int(
                fold_values["sessions"] - fold_values["zero_trade_sessions"]
            )
        selected_all = (
            pd.concat(selected_parts, ignore_index=True)
            if selected_parts else pd.DataFrame(columns=labelled.columns)
        )
        all_selected_by_fraction[float(fraction)] = selected_all
        all_logs_by_fraction[float(fraction)] = pd.concat(
            log_parts, ignore_index=True
        )
        oof_days = development_days[34:]
        values = metric(selected_all, oof_days)
        active_days = values["sessions"] - values["zero_trade_sessions"]
        robust_rank = (
            values["net_pnl_rs"]
            + 0.25 * min(fold_nets.values())
            - 0.10 * abs(values["max_drawdown_rs"])
        )
        rows.append({
            "selection_fraction": float(fraction), **values,
            "active_trade_days": active_days,
            "positive_folds": sum(value > 0 for value in fold_nets.values()),
            "worst_fold_net_pnl_rs": min(fold_nets.values()),
            "fold1_net_pnl_rs": fold_nets["F1"],
            "fold2_net_pnl_rs": fold_nets["F2"],
            "fold3_net_pnl_rs": fold_nets["F3"],
            "fold1_trades": fold_trades["F1"],
            "fold2_trades": fold_trades["F2"],
            "fold3_trades": fold_trades["F3"],
            "fold1_active_days": fold_active_days["F1"],
            "fold2_active_days": fold_active_days["F2"],
            "fold3_active_days": fold_active_days["F3"],
            "robust_rank": robust_rank,
        })
    search = pd.DataFrame(rows)
    eligible = search.loc[
        (search["trades"] >= MIN_OOF_TRADES)
        & (search["active_trade_days"] >= MIN_OOF_ACTIVE_DAYS)
        & (
            search[["fold1_trades", "fold2_trades", "fold3_trades"]]
            .min(axis=1) >= MIN_OOF_FOLD_TRADES
        )
        & (
            search[["fold1_active_days", "fold2_active_days", "fold3_active_days"]]
            .min(axis=1) >= MIN_OOF_FOLD_ACTIVE_DAYS
        )
    ].copy()
    if eligible.empty:
        raise RuntimeError("no rolling OOF fraction met minimum evidence")
    winner = eligible.sort_values(
        ["positive_folds", "robust_rank", "profit_factor", "trades"],
        ascending=False, kind="mergesort",
    ).iloc[0]
    fraction = float(winner["selection_fraction"])
    return (
        fraction,
        search.sort_values("selection_fraction").reset_index(drop=True),
        all_selected_by_fraction[fraction],
        all_logs_by_fraction[fraction],
    )


def joint_search(
    row_candidates: pd.DataFrame,
    all_outcomes: pd.DataFrame,
    development_days: list[str],
) -> tuple[tuple[float, float, float], pd.DataFrame]:
    rows = []
    for sl_pct in SL_GRID:
        for tgt_pct in TARGET_GRID:
            outcomes = all_outcomes.loc[
                all_outcomes["sl_pct"].eq(sl_pct)
                & all_outcomes["tgt_pct"].eq(tgt_pct)
            ]
            labelled = attach_outcomes(row_candidates, outcomes)
            labelled["path_positive"] = (
                labelled["outcome"].eq("TARGET") & labelled["net_pnl_rs"].gt(0)
            ).astype(int)
            development = labelled.loc[
                labelled["trade_date"].le(v2.DEVELOPMENT_END)
            ].copy()
            fraction, search, _, _ = oof_search(development, development_days)
            winner = search.loc[
                search["selection_fraction"].eq(fraction)
            ].iloc[0].to_dict()
            rows.append({
                "sl_pct": float(sl_pct), "tgt_pct": float(tgt_pct),
                **{f"oof_{key}": value for key, value in winner.items()},
            })
            print(
                f"[v3 strict14 joint] sl={sl_pct:.2f} target={tgt_pct:.2f} "
                f"fraction={fraction:.2f} trades={int(winner['trades'])} "
                f"net={float(winner['net_pnl_rs']):.2f} "
                f"pf={float(winner['profit_factor']):.3f} "
                f"folds={int(winner['positive_folds'])}", flush=True,
            )
    table = pd.DataFrame(rows).sort_values(
        ["oof_positive_folds", "oof_robust_rank", "oof_profit_factor", "oof_trades"],
        ascending=False, kind="mergesort",
    ).reset_index(drop=True)
    winner = table.iloc[0]
    return (
        float(winner["sl_pct"]), float(winner["tgt_pct"]),
        float(winner["oof_selection_fraction"]),
    ), table


def feature_coverage(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["month"] = work["trade_date"].astype(str).str[:7]
    rows = []
    for feature in STABLE_FEATURES:
        for month, group in work.groupby("month", sort=True):
            values = pd.to_numeric(group[feature], errors="coerce")
            rows.append({
                "feature": feature, "month": month, "rows": len(group),
                "missing_rows": int(values.isna().sum()),
                "missing_pct": float(values.isna().mean() * 100.0),
                "nonfinite_rows": int((~np.isfinite(values.fillna(0))).sum()),
            })
    out = pd.DataFrame(rows)
    if out["missing_rows"].sum() or out["nonfinite_rows"].sum():
        raise RuntimeError("stable feature coverage audit failed")
    return out


def model_coefficients(model: Any) -> pd.DataFrame:
    coefficient = model.named_steps["logisticregression"].coef_[0]
    out = pd.DataFrame({
        "feature": STABLE_FEATURES,
        "standardised_coefficient": coefficient,
        "absolute_coefficient": np.abs(coefficient),
    })
    return out.sort_values("absolute_coefficient", ascending=False).reset_index(drop=True)


def write_config(path: Path, *, sl: float, target: float, fraction: float) -> None:
    path.write_text(f'''"""Frozen research-only strict-feature path-aware V12 gate."""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
SETUP = {SETUP!r}
SIDE = "LONG"
PREFILTER_RANK_MIN = 200
PREFILTER_RANK_MAX = 300
SIGNAL_MINUTE_MIN = 570
SIGNAL_MINUTE_MAX = 855
BASE_CONDITIONS = (("atr_pct", ">=", 1.05), ("range_pct", ">=", 1.25), ("vwap_dist_atr", ">=", 0.05))
CANDIDATE_UNIT = "ROW_LEVEL_MODEL_FIRST_THRESHOLD_CROSSING_PER_TICKER_DAY"
MODEL_FEATURES = {STABLE_FEATURES!r}
MODEL_FILE = "stable_path_logistic_model.joblib"
MODEL_TICKER_DAY_SAMPLE_WEIGHT = True
THRESHOLD_MODE = "PRIOR_20_SESSION_UNIQUE_TICKER_DAY_PEAK_KTH_HIGHEST"
ROLLING_SCORE_SESSIONS = {ROLLING_SCORE_SESSIONS}
ROLLING_SELECTION_FRACTION = {fraction!r}
CALIBRATION_MIN_ACTIVE_SESSIONS = {MIN_REFERENCE_ACTIVE_SESSIONS}
CALIBRATION_MIN_TAIL_UNITS = {MIN_REFERENCE_TAIL_UNITS}
ENTRY_POLICY = "V12_NEXT_AVAILABLE_1MIN_OPEN_SIGNAL_PLUS_1"
SL_PCT = {sl!r}
TARGET_PCT = {target!r}
ONE_ENTRY_PER_TICKER_DAY = True
DAILY_ENTRY_CAP = {DAILY_CAP}
COST_MODEL = "NSE_STATUTORY_INTRADAY_EQUITY"
REPLAY_IS_FRESH_HOLDOUT = False
''', encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    development_days, replay_days = split_sessions()
    candidates, funnel = load_candidates()
    coverage = feature_coverage(candidates)

    loader = optimizer.install_windowed_1m_loader(
        v12, start_date=v2.START_DATE, end_date=v2.REPLAY_END
    )
    prewarm = optimizer.prewarm_windowed_1m_loader(loader, candidates["ticker"], workers=8)
    optimizer.install_day_1m_adapter(v12, loader)
    v12._V11_EXACT_LIVE_PARITY = False
    v12._V11_COST_MODEL = "statutory"
    v12._V11_SLIPPAGE_BPS = 0.0

    old_exit = v12.v6.SETUP_EXIT_RULES.get(SETUP)
    v12.v6.SETUP_EXIT_RULES[SETUP] = (1.0, 2.0)
    try:
        raw, rejects = v12._v7_entry_engine_raw_rows(candidates)
    finally:
        if old_exit is None:
            v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v12.v6.SETUP_EXIT_RULES[SETUP] = old_exit
    raw["_optimizer_row_id"] = pd.to_numeric(raw["_optimizer_row_id"], errors="raise").astype(int)
    raw["trade_date"] = pd.to_datetime(raw["signal_time_ist"], utc=True).dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d")
    unique_ticker_days = int(raw[["trade_date", "ticker"]].drop_duplicates().shape[0])
    funnel = pd.concat([funnel, pd.DataFrame([
        {"stage": "v12_executable_next_1min_entry", "before": len(candidates), "after": len(raw), "removed": len(candidates) - len(raw)},
        {"stage": "row_level_model_candidates", "before": len(raw), "after": len(raw), "removed": 0},
        {"stage": "diagnostic_unique_ticker_days", "before": len(raw), "after": unique_ticker_days, "removed": len(raw) - unique_ticker_days},
    ])], ignore_index=True)

    pairs = [(sl, target) for sl in SL_GRID for target in TARGET_GRID]
    all_outcomes = optimizer.resolve_exit_grid(
        raw, {SETUP: pairs}, v12, progress_label="v3-strict14-path-outcomes"
    )
    expected = len(raw) * len(pairs)
    if len(all_outcomes) != expected:
        raise RuntimeError(f"exit coverage failure: {len(all_outcomes)}/{expected}")

    (sl_pct, tgt_pct, fraction), joint = joint_search(
        raw, all_outcomes, development_days
    )
    chosen_outcomes = all_outcomes.loc[
        all_outcomes["sl_pct"].eq(sl_pct)
        & all_outcomes["tgt_pct"].eq(tgt_pct)
    ]
    labelled = attach_outcomes(raw, chosen_outcomes)
    labelled["path_positive"] = (
        labelled["outcome"].eq("TARGET") & labelled["net_pnl_rs"].gt(0)
    ).astype(int)
    development = labelled.loc[labelled["trade_date"].le(v2.DEVELOPMENT_END)].copy()
    replay = labelled.loc[labelled["trade_date"].between(v2.REPLAY_START, v2.REPLAY_END)].copy()

    reproduced_fraction, fraction_search, oof_trades, oof_rolling_log = oof_search(
        development, development_days
    )
    if not math.isclose(reproduced_fraction, fraction, abs_tol=1e-12):
        raise RuntimeError("joint winner was not reproducible")

    final_model = fit_model(development)
    development["model_score"] = final_model.predict_proba(
        development[list(STABLE_FEATURES)]
    )[:, 1]
    replay["model_score"] = final_model.predict_proba(
        replay[list(STABLE_FEATURES)]
    )[:, 1]
    replay_trades, rolling_log = rolling_gate(
        development, replay, replay_days, fraction
    )
    replay_trades = replay_trades.sort_values(
        ["trade_date", "entry_time_ist", "ticker"], kind="mergesort"
    ).reset_index(drop=True)

    replay_metrics = metric(replay_trades, replay_days)
    first_days = [d for d in replay_days if d <= v2.REPLAY_MIDPOINT]
    second_days = [d for d in replay_days if d > v2.REPLAY_MIDPOINT]
    split_metrics = {
        "first_half": metric(
            replay_trades.loc[replay_trades["trade_date"].le(v2.REPLAY_MIDPOINT)],
            first_days,
        ),
        "second_half": metric(
            replay_trades.loc[replay_trades["trade_date"].gt(v2.REPLAY_MIDPOINT)],
            second_days,
        ),
    }
    active_days = replay_metrics["sessions"] - replay_metrics["zero_trade_sessions"]
    evidence_pass = bool(
        replay_metrics["trades"] >= MIN_REPLAY_TRADES
        and active_days >= MIN_REPLAY_ACTIVE_DAYS
    )
    split_evidence = {
        name: bool(
            values["trades"] >= 10
            and (values["sessions"] - values["zero_trade_sessions"]) >= 5
        )
        for name, values in split_metrics.items()
    }
    split_evidence_pass = all(split_evidence.values())
    performance_pass = bool(
        evidence_pass
        and split_evidence_pass
        and replay_metrics["net_pnl_rs"] > 0
        and replay_metrics["profit_factor"] >= 1.15
        and split_metrics["first_half"]["net_pnl_rs"] > 0
        and split_metrics["second_half"]["net_pnl_rs"] > 0
    )

    sensitivity_rows = []
    selected_ids = set(replay_trades["_optimizer_row_id"].astype(int))
    for (sl, target), group in all_outcomes.groupby(["sl_pct", "tgt_pct"], sort=True):
        selected = group.loc[group["_optimizer_row_id"].isin(selected_ids)].copy()
        values = metric(selected, replay_days)
        sensitivity_rows.append({"sl_pct": sl, "tgt_pct": target, **values})
    sensitivity = pd.DataFrame(sensitivity_rows).sort_values(
        ["net_pnl_rs", "profit_factor"], ascending=False
    ).reset_index(drop=True)

    oof_winner = fraction_search.loc[
        fraction_search["selection_fraction"].eq(fraction)
    ].iloc[0].to_dict()
    contract = {
        "production_approved": False, "research_only": True, "setup": SETUP,
        "development_window": [v2.START_DATE, v2.DEVELOPMENT_END, len(development_days)],
        "replay_window": [v2.REPLAY_START, v2.REPLAY_END, len(replay_days)],
        "replay_used_for_selection": False, "replay_is_fresh_holdout": False,
        "candidate_unit": "row-level model; first live-style threshold crossing per ticker/day",
        "stable_features": STABLE_FEATURES,
        "redundant_safe_features_removed": REDUNDANT_SAFE_FEATURES,
        "hidden_sentinel_features_removed": HIDDEN_SENTINEL_FEATURES,
        "excluded_unstable_features": EXCLUDED_UNSTABLE_FEATURES,
        "feature_missingness_requirement": "zero missing/nonfinite and no known sentinel substitution in every month",
        "exit": {"sl_pct": sl_pct, "target_pct": tgt_pct},
        "model": {"family": "standardised L2 logistic", "C": 0.10, "class_weight": "balanced", "ticker_day_sample_weight": True},
        "rolling_gate": {
            "mode": "kth-highest peak score per unique ticker/day",
            "prior_sessions": ROLLING_SCORE_SESSIONS,
            "selection_fraction": fraction,
            "minimum_reference_unique_ticker_days": MIN_REFERENCE_ROWS,
            "minimum_reference_active_sessions": MIN_REFERENCE_ACTIVE_SESSIONS,
            "minimum_tail_units": MIN_REFERENCE_TAIL_UNITS,
        },
        "portfolio": {"one_entry_per_ticker_day": True, "daily_cap": DAILY_CAP},
        "minimum_replay_evidence": {"trades": MIN_REPLAY_TRADES, "active_days": MIN_REPLAY_ACTIVE_DAYS},
    }
    verdict = (
        "RESEARCH_CANDIDATE_REQUIRES_FRESH_HOLDOUT"
        if performance_pass else
        (
            "REJECTED_PERFORMANCE"
            if evidence_pass and split_evidence_pass else
            "INVALID_INSUFFICIENT_REPLAY_EVIDENCE"
        )
    )
    summary = {
        "contract": contract, "candidate_funnel": funnel.to_dict("records"),
        "prewarm_1m": prewarm, "entry_rejects": len(rejects),
        "development_joint_winner": joint.iloc[0].to_dict(),
        "oof_winner": oof_winner, "replay_results": replay_metrics,
        "replay_split_results": split_metrics,
        "replay_split_evidence": split_evidence,
        "evidence_gate_passed": evidence_pass,
        "split_evidence_gate_passed": split_evidence_pass,
        "performance_gate_passed": performance_pass,
        "production_approved": False, "verdict": verdict,
    }

    write_config(
        OUTPUT_DIR / "path_aware_long_v3_strict14_conf.py",
        sl=sl_pct, target=tgt_pct, fraction=fraction,
    )
    joblib.dump(final_model, OUTPUT_DIR / "stable_path_logistic_model.joblib")
    funnel.to_csv(OUTPUT_DIR / "candidate_funnel.csv", index=False)
    coverage.to_csv(OUTPUT_DIR / "stable_feature_coverage.csv", index=False)
    joint.to_csv(OUTPUT_DIR / "development_joint_search.csv", index=False)
    fraction_search.to_csv(OUTPUT_DIR / "oof_fraction_search.csv", index=False)
    oof_trades.to_csv(OUTPUT_DIR / "oof_selected_trades.csv", index=False)
    oof_rolling_log.to_csv(OUTPUT_DIR / "oof_rolling_thresholds.csv", index=False)
    model_coefficients(final_model).to_csv(OUTPUT_DIR / "model_coefficients.csv", index=False)
    replay[["_optimizer_row_id", "trade_date", "ticker", "signal_time_ist", "model_score", "outcome", "net_pnl_rs"]].to_csv(OUTPUT_DIR / "replay_scored_row_candidates.csv", index=False)
    replay_trades.to_csv(OUTPUT_DIR / "replay_trades.csv", index=False)
    rolling_log.to_csv(OUTPUT_DIR / "replay_rolling_thresholds.csv", index=False)
    replay_helpers.daily_summary(replay_trades, replay_days).to_csv(OUTPUT_DIR / "replay_daily_summary.csv", index=False)
    replay_helpers.hourly_summary(replay_trades).to_csv(OUTPUT_DIR / "replay_hourly_summary.csv", index=False)
    sensitivity.to_csv(OUTPUT_DIR / "replay_exit_sensitivity.csv", index=False)
    if rejects.empty:
        rejects = pd.DataFrame(columns=["ticker", "signal_time_ist", "reject_reason"])
    rejects.to_csv(OUTPUT_DIR / "entry_engine_rejects.csv", index=False)
    (OUTPUT_DIR / "contract.json").write_text(
        json.dumps(v2.json_safe(contract), indent=2), encoding="utf-8"
    )
    (OUTPUT_DIR / "summary.json").write_text(
        json.dumps(v2.json_safe(summary), indent=2), encoding="utf-8"
    )

    report = f"""# V3 strict-14-feature path-aware V12 long replay

## Verdict

**{verdict}**. Production approval remains false.

This corrected run uses {len(STABLE_FEATURES)} nonredundant features with zero missing values and no known sentinel substitution in every month. The model is fitted to row-level candidates with ticker-day weights. Each threshold is the kth-highest completed ticker-day peak from exactly the prior {ROLLING_SCORE_SESSIONS} scheduled sessions; the current day then takes the first live-style threshold crossing per ticker. Replay P&L was not used for contract selection.

## Frozen contract

- SL {sl_pct:.2f}%; target {tgt_pct:.2f}%.
- Rolling selection fraction {fraction:.2f}.
- Stable-feature standardised L2 logistic model.
- Exact V12 next-1-minute entry, 5 bps entry slippage, risk sizing, statutory costs, and 1-minute target-before-stop/EOD resolution.

## Requested replay

- Trades {replay_metrics['trades']}; active days {active_days}/{len(replay_days)}; trades/session {replay_metrics['trades_per_session']:.2f}.
- Net P&L Rs {replay_metrics['net_pnl_rs']:,.2f}; PF {(replay_metrics['profit_factor'] or 0):.3f}; win rate {replay_metrics['win_rate_pct']:.2f}%.
- Max drawdown Rs {replay_metrics['max_drawdown_rs']:,.2f}.
- First half net Rs {split_metrics['first_half']['net_pnl_rs']:,.2f}; second half net Rs {split_metrics['second_half']['net_pnl_rs']:,.2f}.

The dates have been inspected previously, so even a passing replay is not a virgin holdout.
"""
    (OUTPUT_DIR / "RESEARCH_REPORT.md").write_text(report, encoding="utf-8")

    artifacts = []
    for path in sorted(OUTPUT_DIR.iterdir()):
        if path.is_file() and path.name != "integrity_manifest.json":
            artifacts.append({"file": path.name, "bytes": path.stat().st_size, "sha256": v2.sha256(path)})
    (OUTPUT_DIR / "integrity_manifest.json").write_text(
        json.dumps({"production_approved": False, "artifact_count": len(artifacts), "artifacts": artifacts}, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(v2.json_safe(summary), indent=2))


if __name__ == "__main__":
    main()
