import numpy as np
import pandas as pd
import pytest

import fno_v13_v9_diagnostics as diagnostics


def minutes() -> pd.DataFrame:
    stamps = pd.date_range("2026-08-03 09:26", "2026-08-03 15:15", freq="min", tz="Asia/Kolkata")
    frame = pd.DataFrame({"ts": stamps, "open": 99.8, "high": 99.9, "low": 99.7, "close": 99.8})
    frame.loc[0, "high"] = 100.0
    return frame


def candidate(side: str = "LONG") -> pd.DataFrame:
    return pd.DataFrame([{
        "sid": 55, "candidate_id": "SYNTHETIC_20260803_0925_LONG",
        "day": "2026-08-03", "tradingsymbol": "SYNTHETIC",
        "side": side, "setup_id": "0926_LONG", "selection_stage": "RANKED_OUT",
        "confirmation_ts": pd.Timestamp("2026-08-03 09:26", tz="Asia/Kolkata"),
        "trigger": 100.0 if side == "LONG" else 99.7,
    }])


def replay(frame: pd.DataFrame, orders: pd.DataFrame | None = None) -> pd.DataFrame:
    result, _ = diagnostics.fixed_counterfactuals(candidate() if orders is None else orders, minute_loader=lambda _: frame)
    return result


def test_confirmation_candle_touch_never_fills_itself():
    result = replay(minutes()).iloc[0]
    assert not result.filled
    assert result.counterfactual_status == "RESOLVED_NO_TRIGGER_WITHIN_S10"
    assert pd.isna(result.get("net_return_pct"))


@pytest.mark.parametrize("offset,expected", [(10, True), (11, False)])
def test_entry_window_includes_exact_s10_and_rejects_s11(offset, expected):
    frame = minutes()
    frame.loc[offset, "high"] = 100.1
    result = replay(frame).iloc[0]
    assert bool(result.filled) is expected
    if expected:
        assert result.entry_ts == pd.Timestamp("2026-08-03 09:36", tz="Asia/Kolkata")


def test_both_stop_and_target_in_entry_minute_resolve_stop_first():
    frame = minutes()
    frame.loc[1, ["open", "high", "low", "close"]] = [99.9, 103.0, 98.0, 100.0]
    result = replay(frame).iloc[0]
    assert result.filled
    assert result.exit_reason == "STOP"
    assert result.gross_return_pct == pytest.approx(-1.5)
    assert result.net_return_pct == pytest.approx(-1.55)
    assert result.entry_ts == pd.Timestamp("2026-08-03 09:27", tz="Asia/Kolkata")


def test_adverse_later_stop_gap_uses_actual_open():
    frame = minutes()
    frame.loc[1, "high"] = 100.1
    frame.loc[2, ["open", "high", "low", "close"]] = [97.0, 98.0, 96.0, 97.0]
    result = replay(frame).iloc[0]
    assert result.exit_reason == "STOP"
    assert result.exit_price == 97.0
    assert result.gross_return_pct == pytest.approx(-3.0)


@pytest.mark.parametrize("offset", [1, 20, -1])
def test_any_missing_forward_minute_is_unresolved_not_a_zero_return(offset):
    frame = minutes()
    frame = frame.drop(frame.index[offset])
    result = replay(frame).iloc[0]
    assert result.counterfactual_status == "INCOMPLETE_OR_DUPLICATE_FORWARD_PATH"
    assert result.path_missing_minutes == 1
    assert pd.isna(result.get("net_return_pct"))


def test_missing_exact_confirmation_is_unresolved():
    result = replay(minutes().iloc[1:]).iloc[0]
    assert result.counterfactual_status == "MISSING_EXACT_CONFIRMATION_MINUTE"


def test_nonreal_and_duplicate_forward_rows_are_rejected():
    frame = minutes()
    frame["gap_filled"] = False
    frame.loc[20, "gap_filled"] = True
    result = replay(frame).iloc[0]
    assert result.counterfactual_status == "INVALID_OR_NONREAL_FORWARD_PATH"
    assert result.path_flagged_rows == 1
    duplicate = pd.concat([minutes(), minutes().iloc[[20]]], ignore_index=True)
    result = replay(duplicate).iloc[0]
    assert result.counterfactual_status == "INCOMPLETE_OR_DUPLICATE_FORWARD_PATH"
    assert result.path_duplicate_rows == 2


def test_default_raw_loader_preserves_on_disk_duplicates(tmp_path, monkeypatch):
    frame = pd.concat([minutes(), minutes().iloc[[20]]], ignore_index=True).rename(columns={"ts": "date"})
    path = tmp_path / "synthetic.parquet"
    frame.to_parquet(path, index=False)
    monkeypatch.setattr(diagnostics.hybrid, "equity_one_minute_path", lambda *args: path)
    result, _ = diagnostics.fixed_counterfactuals(candidate())
    assert result.iloc[0].counterfactual_status == "INCOMPLETE_OR_DUPLICATE_FORWARD_PATH"
    assert result.iloc[0].path_duplicate_rows == 2


def test_trigger_must_match_observed_confirmation_extreme():
    orders = candidate()
    orders.loc[0, "trigger"] = 95.0
    result = replay(minutes(), orders).iloc[0]
    assert result.counterfactual_status == "TRIGGER_CONFIRMATION_MISMATCH"


def test_feature_bin_tables_cannot_use_later_period_outcomes():
    frame = pd.DataFrame({
        "day": ["2026-08-03", "2026-08-20", "2026-09-04"],
        "setup_id": ["0926_LONG"] * 3, "selection_stage": ["RANKED_OUT"] * 3,
        "body_ratio": [0.7] * 3, "filled": [True] * 3,
        "net_return_pct": [1.0, -1.0, 1000.0],
    })
    before = diagnostics.feature_bin_outcomes(frame)
    frame.loc[2, ["body_ratio", "net_return_pct"]] = [0.1, -1000.0]
    after = diagnostics.feature_bin_outcomes(frame)
    for left, right in zip(before, after):
        pd.testing.assert_frame_equal(left, right)
    assert before[1].sample_status.eq("INSUFFICIENT_SAMPLE").all()
    assert not before[1].same_mean_sign.any()


def test_boolean_ema_features_have_valid_numeric_descriptions():
    frame = pd.DataFrame({"period": ["TRAIN"] * 2, "v9_1m_ema_bull": [True, False]})
    result = diagnostics.feature_descriptions(frame, ["period"])
    assert result.iloc[0]["mean"] == 0.5
    assert result.iloc[0]["median"] == 0.5


def test_overlapping_summary_omits_profit_sum_and_parses_string_false():
    frame = pd.DataFrame({
        "day": ["2026-08-03"] * 2, "period": ["TRAIN"] * 2,
        "filled": ["False", "True"], "net_return_pct": [99.0, 1.0],
    })
    result = diagnostics.outcome_summary(frame, ["period"]).iloc[0]
    assert result.resolved_fills == 1
    assert result.mean_net_return_pct == 1.0
    assert not any("sum" in name or "profit" in name for name in result.index)
    assert result.outcome_limitations == diagnostics.OUTCOME_LIMITATION
    assert diagnostics.as_bool(pd.Series([0, 1, pd.NA], dtype="Int64")).tolist() == [False, True, False]


def test_full_diagnostics_preserves_native_scaleout_separately_from_bracket(tmp_path, monkeypatch):
    import fno_v13_v9_backtest as engine

    frame = minutes()
    frame.loc[1, ["open", "high", "low", "close"]] = [99.9, 103.0, 99.8, 100.1]
    orders = candidate()
    orders["hhmm_int"] = 925
    orders["body_ratio"] = 0.7
    orders["baseline_selected"] = True
    orders["v9_selected"] = True
    _, paths, _ = diagnostics.checked_candidate_paths(orders, minute_loader=lambda _: frame)
    monkeypatch.setattr(engine, "selection_audit", lambda *args: orders.copy())
    monkeypatch.setattr(diagnostics, "v8_coverage_context", lambda *args: {"candidate_rows": 1})
    result = diagnostics.run_diagnostics(
        {"setup_audit": orders, "signals": orders, "paths": {55: paths[0]}},
        tmp_path, minute_loader=lambda _: frame,
    )
    native = pd.read_parquet(tmp_path / "native_scaleout_eligible_counterfactuals.parquet")
    broad_native = pd.read_parquet(tmp_path / "native_scaleout_all_setup_counterfactuals.parquet")
    bracket = pd.read_parquet(tmp_path / "fixed_bracket_counterfactuals.parquet")
    assert result["strict_eligible_counterfactual_rows"] == 1
    assert native.iloc[0].net_return_pct == pytest.approx(0.0575)
    assert broad_native.iloc[0].net_return_pct == pytest.approx(native.iloc[0].net_return_pct)
    assert bracket.iloc[0].net_return_pct == pytest.approx(2.55)
    assert not any("rupees" in column for column in native)
