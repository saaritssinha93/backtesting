import math

import numpy as np
import pandas as pd
import pytest

import research_v12_path_aware_long_rebuild_v3 as rebuild


def _feature_frame(repetitions: int = 1) -> pd.DataFrame:
    rows = []
    for index in range(12):
        row = {
            feature: float(index * 0.7 + position * 0.11)
            for position, feature in enumerate(rebuild.STABLE_FEATURES)
        }
        row.update({
            "ticker": f"T{index:02d}",
            "trade_date": f"2026-03-{index + 2:02d}",
            "path_positive": index % 2,
        })
        rows.extend([row.copy() for _ in range(repetitions)])
    return pd.DataFrame(rows)


def test_strict_feature_allowlist_excludes_null_sentinel_and_redundancy() -> None:
    assert len(rebuild.STABLE_FEATURES) == 14
    assert len(set(rebuild.STABLE_FEATURES)) == len(rebuild.STABLE_FEATURES)
    assert set(rebuild.STABLE_FEATURES).isdisjoint(rebuild.HIDDEN_SENTINEL_FEATURES)
    assert set(rebuild.STABLE_FEATURES).isdisjoint(rebuild.EXPLICITLY_INCOMPLETE_FEATURES)
    assert set(rebuild.STABLE_FEATURES).isdisjoint(rebuild.REDUNDANT_SAFE_FEATURES)
    forbidden_fragments = ("future", "forward", "outcome", "eod", "entry_price", "execution")
    assert not any(
        fragment in feature.lower()
        for feature in rebuild.STABLE_FEATURES
        for fragment in forbidden_fragments
    )


def test_ticker_day_weights_apply_to_scaler_and_model_replication() -> None:
    once = _feature_frame(repetitions=1)
    tripled = _feature_frame(repetitions=3)
    assert rebuild.row_weights(tripled).groupby(
        [tripled["trade_date"], tripled["ticker"]]
    ).sum().eq(1.0).all()

    model_once = rebuild.fit_model(once)
    model_tripled = rebuild.fit_model(tripled)
    x = once[list(rebuild.STABLE_FEATURES)]
    np.testing.assert_allclose(
        model_once.named_steps["standardscaler"].mean_,
        model_tripled.named_steps["standardscaler"].mean_,
        rtol=0.0,
        atol=1e-12,
    )
    np.testing.assert_allclose(
        model_once.predict_proba(x), model_tripled.predict_proba(x),
        rtol=0.0, atol=1e-10,
    )


def test_ticker_day_peaks_are_normalized_and_duplicate_invariant() -> None:
    frame = pd.DataFrame({
        "trade_date": ["2026-03-02"] * 4,
        "ticker": [" aaa ", "AAA", "bbb", "BBB"],
        "model_score": [0.20, 0.80, 0.70, 0.10],
    })
    peaks = rebuild.ticker_day_peaks(frame)
    assert peaks.to_dict("records") == [
        {"trade_date": "2026-03-02", "ticker": "AAA", "peak_score": 0.80},
        {"trade_date": "2026-03-02", "ticker": "BBB", "peak_score": 0.70},
    ]


def test_rolling_gate_uses_exact_prior_sessions_kth_peak_and_not_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = [f"2026-03-{day:02d}" for day in range(1, 22)]
    monkeypatch.setattr(rebuild, "all_sessions", lambda: days)
    monkeypatch.setattr(
        rebuild,
        "select_first_crossings",
        lambda frame: (frame.copy(), frame.iloc[0:0].copy()),
    )
    history_rows = []
    score = 0.0
    for day in days[:20]:
        for ticker in ("AAA", "BBB", "CCC"):
            history_rows.append({
                "trade_date": day, "ticker": ticker, "model_score": score,
            })
            score += 1.0
    history = pd.DataFrame(history_rows)
    current = pd.DataFrame({
        "trade_date": [days[20], days[20]],
        "ticker": ["NOW", "FUTURE_SPIKE"],
        "model_score": [49.0, 1_000_000.0],
    })

    selected, log = rebuild.rolling_gate(history, current, [days[20]], 0.20)
    k = math.ceil(0.20 * 60)
    assert k == 12
    assert log.loc[0, "threshold"] == 48.0
    assert log.loc[0, "tail_k"] == k
    assert log.loc[0, "reference_session_dates"].split("|") == days[:20]
    assert log.loc[0, "reference_active_sessions"] == 20
    assert selected["ticker"].tolist() == ["NOW", "FUTURE_SPIKE"]

    perturbed = current.copy()
    perturbed["model_score"] *= 99.0
    _, perturbed_log = rebuild.rolling_gate(history, perturbed, [days[20]], 0.20)
    assert perturbed_log.loc[0, "threshold"] == 48.0


def test_rolling_gate_rejects_decision_day_in_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = [f"2026-03-{day:02d}" for day in range(1, 22)]
    monkeypatch.setattr(rebuild, "all_sessions", lambda: days)
    history = pd.DataFrame({
        "trade_date": days[:20] + [days[20]],
        "ticker": [f"T{index}" for index in range(21)],
        "model_score": np.arange(21, dtype=float),
    })
    current = history.iloc[0:0].copy()
    with pytest.raises(RuntimeError, match="noncausal rolling history"):
        rebuild.rolling_gate(history, current, [days[20]], 0.50)


def test_selector_enters_first_threshold_crossing_not_ticker_daily_peak() -> None:
    frame = pd.DataFrame({
        "ticker": ["AAA", "AAA", "BBB"],
        "side": ["LONG"] * 3,
        "setup": [rebuild.SETUP] * 3,
        "bar_time_ist": [
            "2026-06-04 10:00:00+05:30",
            "2026-06-04 10:05:00+05:30",
            "2026-06-04 10:05:00+05:30",
        ],
        "v7_signal_entry_time_ist": [
            "2026-06-04 10:01:00+05:30",
            "2026-06-04 10:06:00+05:30",
            "2026-06-04 10:06:00+05:30",
        ],
        "selection_rank": [250, 251, 252],
        "model_score": [0.40, 0.90, 0.85],
        "_optimizer_row_id": [1, 2, 3],
    })
    eligible = frame.loc[frame["model_score"].ge(0.80)]
    selected, rejected = rebuild.select_first_crossings(eligible)
    assert rejected.empty
    assert selected["_optimizer_row_id"].tolist() == [2, 3]
    assert not selected.duplicated(["ticker"]).any()


def test_contract_constants_are_research_only() -> None:
    assert rebuild.PRODUCTION_APPROVED is False
    assert rebuild.SETUP not in rebuild.v12.ENTRY_SHADOW_SETUPS
    assert rebuild.v2.DEVELOPMENT_END < rebuild.v2.REPLAY_START

