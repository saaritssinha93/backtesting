from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import research_v12_prefilter_train_test_optimizer as optimizer


def _anchor() -> dict:
    return {
        "mask_terms": [
            ["quality_score", ">=", 100.0],
            ["vwap_dist_atr", "<=", 1.0],
            ["regime", "!=", "BEAR"],
        ],
        "pre_momentum_terms": [["sig5_adx_calc", ">=", 30.0]],
        "entry_guards": {
            "min_slot": "10:00",
            "max_slot": "12:30",
            "top_n": 2,
        },
    }


def _valid_relaxation() -> dict:
    return {
        "mask_terms": [
            ["quality_score", ">=", 80.0],
            ["vwap_dist_atr", "<=", 1.4],
            ["regime", "!=", "BEAR"],
        ],
        "pre_momentum_terms": [["sig5_adx_calc", ">=", 25.0]],
        "entry_guards": {
            "min_slot": "09:45",
            "max_slot": "13:00",
            "top_n": 4,
        },
    }


def test_fixed_split_is_disjoint_and_boundary_exact() -> None:
    frame = pd.DataFrame(
        {
            "day": [
                "2026-06-03",
                "2026-06-04",
                "2026-07-06",
                "2026-07-07",
                "2026-08-03",
                "2026-08-04",
            ],
            "row": range(6),
        }
    )

    train, test = optimizer.split_frame(frame, date_column="day")

    assert train["day"].tolist() == ["2026-06-04", "2026-07-06"]
    assert test["day"].tolist() == ["2026-07-07", "2026-08-03"]
    assert set(train["row"]).isdisjoint(set(test["row"]))


def test_prefilter_rank_band_is_inclusive_and_missing_ranks_fail_closed() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["A", "B", "C", "D", "E"],
            "prefilter_selection_rank": [199, 200, 250, 300, None],
        }
    )

    selected, audit = optimizer.apply_prefilter_rank_band(
        frame, min_rank=200, max_rank=300
    )

    assert selected["ticker"].tolist() == ["B", "C", "D"]
    assert audit is not None
    assert audit["rows_total"] == 5
    assert audit["rows_active_setup"] == 3
    assert audit["rows_missing_or_non_numeric_rank"] == 1
    assert audit["min_rank"] == 200
    assert audit["max_rank"] == 300


@pytest.mark.parametrize(
    ("minimum", "maximum", "message"),
    [
        (0, 300, "minimum must be at least 1"),
        (1, 0, "maximum must be at least 1"),
        (201, 200, "minimum cannot exceed maximum"),
    ],
)
def test_prefilter_rank_band_rejects_invalid_bounds(
    minimum: int, maximum: int, message: str
) -> None:
    frame = pd.DataFrame({"prefilter_selection_rank": [200]})
    with pytest.raises(optimizer.ContractError, match=message):
        optimizer.apply_prefilter_rank_band(
            frame, min_rank=minimum, max_rank=maximum
        )


def test_prefilter_rank_band_requires_rank_column_when_enabled() -> None:
    with pytest.raises(optimizer.ContractError, match="no prefilter_selection_rank"):
        optimizer.apply_prefilter_rank_band(
            pd.DataFrame({"ticker": ["A"]}), min_rank=200, max_rank=300
        )


def test_split_contract_rejects_overlap() -> None:
    contract = optimizer.SplitContract(
        train_start="2026-06-04",
        train_end="2026-07-07",
        test_start="2026-07-07",
        test_end="2026-08-03",
    )
    with pytest.raises(optimizer.ContractError, match="chronological and disjoint"):
        contract.validate()


def test_relaxation_retains_all_terms_operators_and_guard_keys() -> None:
    optimizer.validate_constraint_relaxation(
        "SAMPLE", _anchor(), _valid_relaxation()
    )


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda cfg: cfg["mask_terms"].pop(), "term count changed"),
        (
            lambda cfg: cfg["mask_terms"][0].__setitem__(1, "<="),
            "feature/operator changed",
        ),
        (
            lambda cfg: cfg["mask_terms"][0].__setitem__(2, 110.0),
            ">= threshold tightened",
        ),
        (
            lambda cfg: cfg["mask_terms"][1].__setitem__(2, 0.8),
            "<= threshold tightened",
        ),
        (
            lambda cfg: cfg["mask_terms"][2].__setitem__(2, "BULL"),
            "categorical value changed",
        ),
        (
            lambda cfg: cfg["entry_guards"].pop("top_n"),
            "entry_guards keys changed",
        ),
        (
            lambda cfg: cfg["entry_guards"].__setitem__("min_slot", "10:30"),
            "min_slot tightened",
        ),
        (
            lambda cfg: cfg["entry_guards"].__setitem__("max_slot", "12:00"),
            "max_slot tightened",
        ),
        (
            lambda cfg: cfg["entry_guards"].__setitem__("top_n", 1),
            "top_n tightened",
        ),
    ],
)
def test_relaxation_contract_fails_closed(mutation, message: str) -> None:
    proposal = _valid_relaxation()
    mutation(proposal)
    with pytest.raises(optimizer.ContractError, match=message):
        optimizer.validate_constraint_relaxation("SAMPLE", _anchor(), proposal)


def test_training_only_threshold_generation_preserves_term_structure() -> None:
    cfg = {
        "side": "LONG",
        "exit": {"sl_pct": 1.0, "tgt_pct": 1.5},
        "mask_terms": [["quality_score", ">=", 80.0]],
        "pre_momentum_terms": [["sig5_adx_calc", ">=", 25.0]],
        "entry_guards": {"top_n": 4},
        "v12_original_constraints": {
            "mask_terms": [["quality_score", ">=", 100.0]],
            "pre_momentum_terms": [["sig5_adx_calc", ">=", 30.0]],
            "entry_guards": {"top_n": 2},
        },
    }
    train = pd.DataFrame(
        {
            "quality_score": [20.0, 60.0, 100.0, 140.0],
            "premom__sl_1p0000__sig5_adx_calc": [10.0, 20.0, 30.0, 40.0],
            "signal_minute": [600, 605, 610, 615],
        }
    )

    trials = optimizer.generate_setup_trials("SAMPLE", cfg, train, [(1.0, 1.5)])

    assert trials
    for proposal in trials:
        optimizer.validate_constraint_relaxation(
            "SAMPLE", optimizer.original_constraints(cfg), proposal
        )
        assert [(term[0], term[1]) for term in proposal["mask_terms"]] == [
            ("quality_score", ">=")
        ]
        assert [
            (term[0], term[1]) for term in proposal["pre_momentum_terms"]
        ] == [("sig5_adx_calc", ">=")]
        assert set(proposal["entry_guards"]) == {"top_n"}


def test_mask_momentum_and_topn_are_applied_before_dedupe() -> None:
    config = {
        "exit": {"sl_pct": 1.0, "tgt_pct": 1.5},
        "mask_terms": [["quality_score", ">=", 50.0]],
        "pre_momentum_terms": [["sig5_adx_calc", ">=", 20.0]],
        "entry_guards": {"top_n": 1},
    }
    frame = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "signal_day": ["2026-06-04"] * 3,
            "signal_minute": [600.0] * 3,
            "quality_score": [100.0, 90.0, 40.0],
            "vwap_dist_atr": [1.0, 2.0, 3.0],
            "premom__sl_1p0000__sig5_adx_calc": [30.0, 25.0, 50.0],
        }
    )

    accepted = optimizer.apply_setup_config(frame, config)

    # C fails the mask before Top-N; B wins Top-N among the remaining A/B rows.
    assert accepted["ticker"].tolist() == ["B"]


def test_portfolio_constraint_does_not_force_a_losing_frequency_target() -> None:
    profitable_near_band = {
        "trades_per_session": 9.0,
        "net_pnl_rs": 1000.0,
        "profit_factor": 1.2,
        "worst_half_net_pnl_rs": 100.0,
        "max_drawdown_rs": -200.0,
        "top_positive_day_share": 0.2,
    }
    losing_in_band = {
        "trades_per_session": 12.0,
        "net_pnl_rs": -100.0,
        "profit_factor": 0.9,
        "worst_half_net_pnl_rs": -100.0,
        "max_drawdown_rs": -300.0,
        "top_positive_day_share": 0.2,
    }
    profitable_in_band = copy.deepcopy(profitable_near_band)
    profitable_in_band["trades_per_session"] = 12.0

    assert optimizer.portfolio_rank(profitable_near_band) > optimizer.portfolio_rank(
        losing_in_band
    )
    assert optimizer.portfolio_rank(profitable_in_band) > optimizer.portfolio_rank(
        profitable_near_band
    )


def test_frequency_target_rejects_trades_concentrated_in_few_sessions() -> None:
    sessions = [f"2026-06-{day:02d}" for day in range(1, 11)]
    concentrated = pd.DataFrame(
        {
            "trade_date": [sessions[0]] * 100,
            "net_pnl_rs": [10.0] * 100,
        }
    )
    regular = pd.DataFrame(
        {
            "trade_date": [day for day in sessions for _ in range(10)],
            "net_pnl_rs": [10.0] * 100,
        }
    )

    concentrated_metrics = optimizer.performance_metrics(concentrated, sessions)
    regular_metrics = optimizer.performance_metrics(regular, sessions)

    assert concentrated_metrics["trades_per_session"] == 10.0
    assert concentrated_metrics["median_trades_per_session"] == 0.0
    assert concentrated_metrics["zero_trade_sessions"] == 9
    assert concentrated_metrics["target_frequency_met"] is False
    assert regular_metrics["median_trades_per_session"] == 10.0
    assert regular_metrics["sessions_in_target_band_pct"] == 100.0
    assert regular_metrics["target_frequency_met"] is True
    assert optimizer.portfolio_rank(regular_metrics) > optimizer.portfolio_rank(
        concentrated_metrics
    )


def test_signal_stop_rounding_matches_entry_engine_before_momentum_and_sizing() -> None:
    assert optimizer._signal_stop_price(100.03, "SHORT", 0.9) == 100.93
    assert optimizer._signal_stop_price(100.03, "LONG", 0.9) == 99.13


def test_trial_exit_sizes_from_unslipped_signal_price() -> None:
    captured: dict[str, float] = {}

    def size(entry: float, stop: float) -> int:
        captured.update(entry=entry, stop=stop)
        return 123

    fake = SimpleNamespace(
        _load_1m_with_open=lambda ticker: pd.DataFrame({"close": [100.0]}),
        _normalise_ts=lambda value: pd.Timestamp(value),
        _risk_based_qty=size,
        _historical_nifty_short_mult=lambda day: 1.0,
        V7_PAPER_SLIPPAGE_PCT=0.0005,
        er=SimpleNamespace(
            resolve=lambda **kwargs: SimpleNamespace(
                exit_price=99.0,
                outcome="TARGET",
                exit_time_ist=pd.Timestamp("2026-06-04 10:05:00+05:30"),
                bars_held=4,
            )
        ),
        _FINAL_CONF_EXIT_POLICIES={},
        _price_pnl_rs=lambda side, entry, exit_price, quantity: (
            entry - exit_price
        )
        * quantity,
    )
    row = pd.Series(
        {
            "_optimizer_row_id": 7,
            "ticker": "TEST",
            "side": "SHORT",
            "setup": "SAMPLE",
            "signal_time_ist": "2026-06-04 10:00:00+05:30",
            "v7_signal_entry_time_ist": "2026-06-04 10:01:00+05:30",
            "v7_signal_entry_price": 100.03,
        }
    )

    resolved = optimizer._resolve_exit_row(
        row, sl_pct=0.9, tgt_pct=1.0, v12=fake
    )

    assert captured == {"entry": 100.03, "stop": 100.93}
    assert resolved is not None
    assert resolved["entry_price"] == 99.98
    assert resolved["quantity"] == 123


def test_attach_outcomes_fails_closed_on_partial_coverage() -> None:
    selected = pd.DataFrame(
        {"_optimizer_row_id": [1], "setup": ["SAMPLE"]}
    )
    outcomes = pd.DataFrame(
        {
            "_optimizer_row_id": [2],
            "setup": ["SAMPLE"],
            "sl_pct": [1.0],
            "tgt_pct": [1.5],
        }
    )
    book = {"SAMPLE": {"exit": {"sl_pct": 1.0, "tgt_pct": 1.5}}}

    with pytest.raises(optimizer.ContractError, match="outcome coverage failure"):
        optimizer.attach_outcomes(selected, book, outcomes)


def test_setup_summary_includes_zero_trade_setups() -> None:
    summary = optimizer._setup_summary(
        pd.DataFrame(
            {
                "setup": ["ACTIVE"],
                "trade_date": ["2026-06-04"],
                "net_pnl_rs": [100.0],
            }
        ),
        ["2026-06-04", "2026-06-05"],
        ["ACTIVE", "ZERO"],
    ).set_index("setup")

    assert list(summary.index) == ["ACTIVE", "ZERO"]
    assert int(summary.loc["ZERO", "trades"]) == 0
    assert int(summary.loc["ZERO", "zero_trade_sessions"]) == 2


def test_complete_source_fingerprint_covers_execution_policies() -> None:
    left = {
        "SAMPLE": {
            "side": "LONG",
            "exit": {"sl_pct": 1.0, "tgt_pct": 1.5},
            "entry_policy": {"model": "next_open"},
            "exit_policy": {"max_hold_minutes": 60},
        }
    }
    right = copy.deepcopy(left)
    right["SAMPLE"]["exit_policy"]["max_hold_minutes"] = 30

    assert optimizer._constraint_fingerprint(left) != optimizer._constraint_fingerprint(
        right
    )


def test_jsonable_serializes_native_infinity_safely() -> None:
    assert optimizer._jsonable(float("inf")) == "inf"
    assert optimizer._jsonable(float("-inf")) == "-inf"


def test_narrow_exact_dedupe_preserves_v12_selection_order() -> None:
    import avwap_5min_ID_v12_backtesting as v12

    frame = pd.DataFrame(
        {
            "_optimizer_row_id": list(range(7)),
            "ticker": ["A", "A", "B", "A", "C", "C", "D"],
            "side": ["LONG"] * 7,
            "setup": ["X", "Y", "X", "X", "Y", "X", "X"],
            "bar_time_ist": [
                "2026-06-04 10:00:00+05:30",
                "2026-06-04 10:00:00+05:30",
                "2026-06-04 10:00:00+05:30",
                "2026-06-04 10:05:00+05:30",
                "2026-06-04 10:05:00+05:30",
                "2026-06-04 10:05:00+05:30",
                "2026-06-05 10:00:00+05:30",
            ],
            "score": [5.0, 6.0, 4.0, 100.0, 3.0, 3.0, 1.0],
            "large_unused_feature": [object() for _ in range(7)],
        }
    )

    expected = v12._select_v7_entry_engine_signals(frame)
    actual = optimizer.exact_dedupe(frame, v12)

    assert actual["_optimizer_row_id"].tolist() == expected[
        "_optimizer_row_id"
    ].tolist()


def test_trial_scoring_reuses_target_independent_filter_and_late_materializes(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "_optimizer_row_id": [1],
            "setup": ["SAMPLE"],
            "ticker": ["TEST"],
            "side": ["LONG"],
            "bar_time_ist": ["2026-06-04 10:00:00+05:30"],
            "score": [1.0],
        }
    )
    common = {
        "side": "LONG",
        "mask_terms": [],
        "pre_momentum_terms": [],
        "entry_guards": {},
    }
    trials = [
        {**copy.deepcopy(common), "exit": {"sl_pct": 1.0, "tgt_pct": target}}
        for target in (1.0, 1.5, 2.0)
    ]
    outcomes = pd.DataFrame(
        {
            "_optimizer_row_id": [1, 1, 1],
            "setup": ["SAMPLE"] * 3,
            "sl_pct": [1.0] * 3,
            "tgt_pct": [1.0, 1.5, 2.0],
            "trade_date": ["2026-06-04"] * 3,
            "entry_time_ist": ["2026-06-04 10:01:00+05:30"] * 3,
            "ticker": ["TEST"] * 3,
            "net_pnl_rs": [10.0, 20.0, 30.0],
        }
    )
    calls = {"apply": 0, "dedupe": 0}

    def apply_once(source, config):
        calls["apply"] += 1
        return source.copy()

    def dedupe_once(source, v12):
        calls["dedupe"] += 1
        return source.copy()

    monkeypatch.setattr(optimizer, "apply_setup_config", apply_once)
    monkeypatch.setattr(optimizer, "exact_dedupe", dedupe_once)
    choices, report = optimizer.score_setup_trials(
        "SAMPLE",
        frame,
        trials,
        outcomes,
        ["2026-06-04"],
        SimpleNamespace(),
    )

    assert calls == {"apply": 1, "dedupe": 1}
    assert len(report) == 3
    assert all(choice.filtered is None for choice in choices)
    assert all(choice.filtered_row_ids == (1,) for choice in choices)
    optimizer.materialize_choice_frames(choices, frame)
    assert all(choice.filtered is not None for choice in choices)


def test_windowed_1m_loader_excludes_rows_outside_fixed_contract(tmp_path: Path) -> None:
    source = tmp_path / "TEST_stocks_indicators_1min.parquet"
    pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2026-06-03 15:20:00+05:30",
                    "2026-06-04 09:15:00+05:30",
                    "2026-08-03 15:20:00+05:30",
                    "2026-08-04 09:15:00+05:30",
                ]
            ),
            "open": [1.0] * 4,
            "high": [1.0] * 4,
            "low": [1.0] * 4,
            "close": [1.0] * 4,
            "volume": [1.0] * 4,
            "ADX": [1.0] * 4,
            "RSI": [1.0] * 4,
        }
    ).to_parquet(source, index=False)

    def normalise(frame: pd.DataFrame, naive_tz: str) -> pd.DataFrame:
        out = frame.copy()
        values = pd.to_datetime(out.pop("date"), errors="coerce")
        if values.dt.tz is None:
            values = values.dt.tz_localize(naive_tz).dt.tz_convert("Asia/Kolkata")
        else:
            values = values.dt.tz_convert("Asia/Kolkata")
        out.index = values
        return out

    fake = SimpleNamespace(
        v6=SimpleNamespace(DATA_1M_DIR=tmp_path),
        _load_1m_with_open=lambda ticker: None,
        _normalise_bars_date_index=normalise,
    )
    loader = optimizer.install_windowed_1m_loader(fake)
    loaded = loader("TEST")
    assert loaded is not None
    assert loaded.index.strftime("%Y-%m-%d").tolist() == [
        "2026-06-04",
        "2026-08-03",
    ]


def test_windowed_1m_prewarm_deduplicates_symbols_and_audits_missing() -> None:
    seen: list[str] = []

    def loader(symbol: str):
        seen.append(symbol)
        return None if symbol == "MISS" else pd.DataFrame({"close": [1.0]})

    audit = optimizer.prewarm_windowed_1m_loader(
        loader, [" aaa ", "AAA", "bbb", "MISS"], workers=2
    )

    assert sorted(seen) == ["AAA", "BBB", "MISS"]
    assert audit == {
        "requested": 3,
        "loaded": 2,
        "missing": 1,
        "failed": 0,
        "workers": 2,
    }


def test_day_1m_adapter_returns_only_requested_session() -> None:
    index = pd.to_datetime(
        [
            "2026-06-04 09:15:00+05:30",
            "2026-06-04 15:20:00+05:30",
            "2026-06-05 09:15:00+05:30",
        ]
    )
    full = pd.DataFrame({"open": [1.0, 2.0, 3.0]}, index=index)
    fake = SimpleNamespace(
        _entry_bars_for_signal=lambda ticker, signal: (full, "old"),
        _normalise_ts=lambda value: pd.Timestamp(value),
        _V11_EXACT_LIVE_PARITY=False,
    )
    loader = optimizer.install_day_1m_adapter(fake, lambda ticker: full)

    day = loader("TEST", "2026-06-04")
    via_entry, source = fake._entry_bars_for_signal(
        "TEST", pd.Timestamp("2026-06-04 10:00:00+05:30")
    )

    assert day is not None and len(day) == 2
    assert via_entry is not None and via_entry.index.equals(day.index)
    assert source == "historical_1min_day_slice"
