from __future__ import annotations

import hashlib
import importlib
import math
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v10_experiment_backtest as experiment
import fno_v10_experiment_compare as compare
import fno_v10_experiment_config as config
import fno_v10_unified_5m_1m_backtest as v10
import fno_v8_windowed_1m_entry_backtest as engine


SIGNAL_TIME = pd.Timestamp("2026-08-03 09:30", tz="Asia/Kolkata")
_ORIGINAL_PROVENANCE_BUILDER = engine.provenance.build_run_provenance


@pytest.fixture(autouse=True)
def restore_engine_after_test() -> None:
    yield
    engine.provenance.build_run_provenance = _ORIGINAL_PROVENANCE_BUILDER
    importlib.reload(engine)
    importlib.reload(v10)
    importlib.reload(experiment)


def _setup(*, signal_end: str = "09:30", side: str = "LONG") -> engine.V8Setup:
    return engine.V8Setup(
        signal_end=signal_end,
        side=side,
        max_entries=1,
        picker="max_liquidity",
        price_change_pct=0.20,
        oi_change_pct=0.10,
        volume_ratio=1.0,
        body_ratio=0.40,
        max_wick_ratio=0.50,
        min_traded_value=0.0,
        stop_pct=1.0,
        target_pct=1.0,
    )


def _candidate(
    symbol: str = "TEST",
    *,
    five_min_volume: float = 5_000.0,
) -> engine.CandidateInput:
    return engine.CandidateInput(
        symbol=symbol,
        signal_time=SIGNAL_TIME,
        five_min_open=99.5,
        five_min_high=101.0,
        five_min_low=99.0,
        five_min_close=100.0,
        price_change_pct=1.0,
        oi_change_pct=1.0,
        volume_ratio=3.0,
        traded_value=10_000_000.0,
        tick_size=0.05,
        five_min_volume=five_min_volume,
    )


def _bar(
    minute: int,
    *,
    open_: float = 99.8,
    high: float = 100.8,
    low: float = 99.7,
    close: float = 100.6,
    volume: float = 1_000.0,
) -> engine.MinuteBar:
    return engine.MinuteBar(
        timestamp=SIGNAL_TIME + pd.Timedelta(minutes=minute),
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def _policy(
    *,
    rv1: float | None,
    max_confirmation: int = 2,
    expiry: int = 5,
) -> experiment.ExperimentEntryPolicy:
    return experiment.ExperimentEntryPolicy(
        buffer_bps=0.0,
        max_confirmation_minute=max_confirmation,
        entry_expiry_minute=expiry,
        close_location_min=None,
        cost_bps=15.0,
        slippage_bps=0.0,
        midpoint_invalidation=False,
        post_confirmation_cancel=True,
        allow_cap_reassignment=True,
        same_bar_policy="STOP_FIRST",
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
        confirmation_volume_ratio_min=rv1,
    )


def _candidate_frame() -> pd.DataFrame:
    rows = [
        ("A", "09:35_LONG", "LONG", 0.25, 50.0, 10_000.0, 1),
        ("B", "09:40_LONG", "LONG", 0.20, 80.0, 20_000.0, 1),
        ("C", "09:40_LONG", "LONG", 0.30, 70.0, 30_000.0, 2),
        ("D", "09:40_LONG", "LONG", 0.40, 60.0, 40_000.0, 3),
        ("E", "09:40_SHORT", "SHORT", -0.40, 55.0, 15_000.0, 1),
    ]
    records = []
    for symbol, setup_id, side, move, picker, traded, rank in rows:
        slot = setup_id[:5]
        records.append(
            {
                "candidate_id": f"2026-08-03|{setup_id}|{symbol}",
                "session_date": pd.Timestamp("2026-08-03").date(),
                "signal_time": pd.Timestamp(
                    f"2026-08-03 {slot}", tz="Asia/Kolkata"
                ),
                "setup_id": setup_id,
                "side": side,
                "symbol": symbol,
                "price_change_pct": move,
                "five_min_volume": 5_000.0,
                "picker": "max_liquidity",
                "picker_value": picker,
                "traded_value": traded,
                "frozen_rank": rank,
            }
        )
    return pd.DataFrame(records)


def test_registry_is_exact_hash_pinned_and_has_unique_configs() -> None:
    config.validate_registry()
    assert config.registry_sha256() == config.EXPECTED_EXPERIMENT_REGISTRY_SHA256
    assert set(config.EXPERIMENT_REGISTRY) == {
        "V10B",
        "RV1_100",
        "EXPIRY_S4",
        "RV1_100_S4",
        "NO_0935_LONG",
        "0940_LONG_MOVE_030",
        "0940_LONG_MOVE_040",
        "SLOT_RVOL_150",
        "SLOT_RVOL_200",
    }
    assert len(
        {config.variant_config_sha256(spec) for spec in config.EXPERIMENT_SPECS}
    ) == len(config.EXPERIMENT_SPECS)
    with pytest.raises(ValueError):
        config.get_spec("ARBITRARY_125")


def test_configuration_is_isolated_and_preserves_frozen_v10_sources() -> None:
    v10_hash_before = hashlib.sha256(Path(v10.__file__).read_bytes()).hexdigest()
    engine_hash_before = engine._module_source_sha256()
    experiment.configure_engine("V10B")
    assert engine.ACTIVE_SETUPS == v10.ACTIVE_SETUPS
    assert engine.V8_SETUP_BOOK_SHA256 == v10.ACTIVE_SETUP_BOOK_SHA256
    assert engine.RUN_ROOT.is_relative_to(experiment.ROOT)
    assert engine.CACHE_DIR.is_relative_to(experiment.ROOT)
    assert engine.PROVENANCE_ROOT.is_relative_to(experiment.ROOT)
    assert not engine.RUN_ROOT.is_relative_to(v10.ROOT)
    assert set(engine.VARIANT_REGISTRY) == set(config.EXPERIMENT_REGISTRY)
    assert hashlib.sha256(Path(v10.__file__).read_bytes()).hexdigest() == (
        v10_hash_before
    )
    assert engine._module_source_sha256() == engine_hash_before


@pytest.mark.parametrize(
    ("variant", "rv1", "expiry"),
    [
        ("V10B", None, 5),
        ("RV1_100", 1.0, 5),
        ("EXPIRY_S4", None, 4),
        ("RV1_100_S4", 1.0, 4),
    ],
)
def test_entry_policy_resolves_predeclared_rv1_and_expiry(
    variant: str,
    rv1: float | None,
    expiry: int,
) -> None:
    experiment.configure_engine(variant)
    policy = engine.entry_policy_for_variant(
        variant,
        cost_bps=15.0,
        slippage_bps=0.0,
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )
    assert isinstance(policy, experiment.ExperimentEntryPolicy)
    assert policy.confirmation_volume_ratio_min == rv1
    assert policy.entry_expiry_minute == expiry
    setup_policy = engine.policy_for_setup(v10.ACTIVE_SETUPS[0], policy)
    assert isinstance(setup_policy, experiment.ExperimentEntryPolicy)
    assert setup_policy.confirmation_volume_ratio_min == rv1
    assert setup_policy.entry_expiry_minute == expiry


def test_v10b_confirmation_adapter_is_exact_noop() -> None:
    experiment.configure_engine("V10B")
    policy = _policy(rv1=None)
    candidate = _candidate()
    bar = _bar(1)
    expected = experiment._NEUTRAL_CONFIRMATION_CHECK(
        _setup(), candidate, bar, policy
    )
    observed = engine._confirmation_check(_setup(), candidate, bar, policy)
    assert observed == expected
    assert "confirmation_volume_ratio" not in observed


@pytest.mark.parametrize(
    ("confirmation_volume", "passes"),
    [(999.0, False), (1_000.0, True), (1_001.0, True)],
)
def test_rv1_boundary_is_inclusive(
    confirmation_volume: float,
    passes: bool,
) -> None:
    experiment.configure_engine("RV1_100")
    check = engine._confirmation_check(
        _setup(),
        _candidate(five_min_volume=5_000.0),
        _bar(1, volume=confirmation_volume),
        _policy(rv1=1.0),
    )
    assert check["confirmation_volume_ratio"] == pytest.approx(
        confirmation_volume / 1_000.0
    )
    assert check["passed"] is passes


@pytest.mark.parametrize("signal_volume", [0.0, -1.0, math.nan])
def test_rv1_invalid_signal_volume_fails_closed(signal_volume: float) -> None:
    experiment.configure_engine("RV1_100")
    check = engine._confirmation_check(
        _setup(),
        _candidate(five_min_volume=signal_volume),
        _bar(1),
        _policy(rv1=1.0),
    )
    assert check["passed"] is False
    assert "CONFIRMATION_VOLUME_RATIO_UNAVAILABLE" in check["rejection_codes"]


def test_rv1_rejected_s1_can_confirm_s2_and_never_fill_same_bar() -> None:
    experiment.configure_engine("RV1_100")
    candidate = _candidate()
    bars = [
        _bar(1, high=101.50, volume=999.0),
        _bar(2, high=101.50, volume=1_000.0),
        _bar(3, open_=101.40, high=102.80, low=101.30, close=102.50),
    ]
    audit = engine.simulate_setup_window(
        _setup(), [candidate], {candidate.symbol: bars}, _policy(rv1=1.0)
    )
    row = audit.iloc[0]
    assert int(row["confirmation_minute"]) == 2
    assert int(row["entry_minute"]) == 3
    assert row["entry_time"] > row["confirmation_time"]
    assert row["confirmation_checks"][0]["passed"] is False
    assert row["confirmation_checks"][1]["passed"] is True


def test_s4_allows_s4_touch_but_never_reads_or_fills_s5() -> None:
    experiment.configure_engine("EXPIRY_S4")
    candidate = _candidate()
    s4_policy = _policy(rv1=None, max_confirmation=1, expiry=4)
    bars_s4 = [
        _bar(1, high=100.80),
        _bar(2, high=100.70, close=100.5),
        _bar(3, high=100.70, close=100.5),
        _bar(4, open_=100.75, high=102.0, low=100.7, close=101.8),
    ]
    filled = engine.simulate_setup_window(
        _setup(), [candidate], {candidate.symbol: bars_s4}, s4_policy
    ).iloc[0]
    assert int(filled["entry_minute"]) == 4

    bars_s5_only = [
        _bar(1, high=100.80),
        _bar(2, high=100.70, close=100.5),
        _bar(3, high=100.70, close=100.5),
        _bar(4, high=100.70, close=100.5),
        _bar(5, open_=100.75, high=102.0, low=100.7, close=101.8),
    ]
    expired = engine.simulate_setup_window(
        _setup(), [candidate], {candidate.symbol: bars_s5_only}, s4_policy
    ).iloc[0]
    assert pd.isna(expired["entry_time"])
    assert str(expired["status"]) == engine.SignalState.WINDOW_EXPIRED.value


def test_policy_rejects_confirmation_window_equal_to_expiry() -> None:
    with pytest.raises(ValueError):
        _policy(rv1=None, max_confirmation=4, expiry=4).validate()


def test_selection_overlays_are_subset_only_and_rerank_before_replay() -> None:
    candidates = _candidate_frame()
    control, control_decisions = experiment.apply_selection_overlay(
        candidates, config.get_spec("V10B")
    )
    assert set(control["candidate_id"]) == set(candidates["candidate_id"])
    assert control_decisions["selection_passed"].all()

    no_0935, decisions = experiment.apply_selection_overlay(
        candidates, config.get_spec("NO_0935_LONG")
    )
    assert not no_0935["setup_id"].eq("09:35_LONG").any()
    rejected = decisions.loc[~decisions["selection_passed"]]
    assert set(rejected["selection_reason"]) == {"SETUP_DISABLED"}

    move_030, _ = experiment.apply_selection_overlay(
        candidates, config.get_spec("0940_LONG_MOVE_030")
    )
    move_040, _ = experiment.apply_selection_overlay(
        candidates, config.get_spec("0940_LONG_MOVE_040")
    )
    ids_base = set(candidates["candidate_id"])
    ids_030 = set(move_030["candidate_id"])
    ids_040 = set(move_040["candidate_id"])
    assert ids_040 < ids_030 < ids_base
    assert any("|09:40_LONG|C" in value for value in ids_030)
    assert any("|09:40_LONG|D" in value for value in ids_040)
    ranks = move_040.loc[
        move_040["setup_id"].eq("09:40_LONG"), "frozen_rank"
    ].tolist()
    assert ranks == list(range(1, len(ranks) + 1))


def test_empty_selection_decisions_keep_valid_provenance_schema() -> None:
    selected, decisions = experiment.apply_selection_overlay(
        pd.DataFrame(), config.get_spec("V10B")
    )
    assert selected.empty
    assert decisions.empty
    assert {
        "candidate_id",
        "session_date",
        "setup_id",
        "symbol",
        "picker_value",
        "traded_value",
        "original_frozen_rank",
        "recalculated_frozen_rank",
        "selection_passed",
        "selection_reason",
        "experiment_variant",
        "schema_version",
    } <= set(decisions.columns)


def test_same_slot_rvol_uses_shift_min10_exact20_and_is_future_causal() -> None:
    official = [value.date() for value in pd.bdate_range("2026-06-01", periods=25)]
    rows = []
    for index, session_day in enumerate(official, start=1):
        rows.append(
            {
                "ts": pd.Timestamp(
                    f"{session_day.isoformat()} 09:25", tz="Asia/Kolkata"
                ),
                "volume": float(index),
            }
        )
        rows.append(
            {
                "ts": pd.Timestamp(
                    f"{session_day.isoformat()} 09:30", tz="Asia/Kolkata"
                ),
                "volume": float(index * 100),
            }
        )
    frame = pd.DataFrame(rows).sample(frac=1.0, random_state=7)
    observed = experiment.compute_same_slot_rvol20(
        frame,
        official_session_dates=official,
        signal_slots=("09:25", "09:30"),
    )
    slot = observed.loc[observed["signal_end"].eq("09:25")].reset_index(drop=True)
    assert slot.loc[9, "prior_slot_observation_count_20"] == 9
    assert not bool(slot.loc[9, "feature_available"])
    assert slot.loc[10, "prior_slot_observation_count_20"] == 10
    assert bool(slot.loc[10, "feature_available"])
    assert slot.loc[24, "prior_slot_volume_median"] == pytest.approx(14.5)
    assert slot.loc[24, "slot_rvol20"] == pytest.approx(25.0 / 14.5)

    mutated = frame.copy()
    last_ts = pd.Timestamp(
        f"{official[-1].isoformat()} 09:25", tz="Asia/Kolkata"
    )
    mutated.loc[mutated["ts"].eq(last_ts), "volume"] = 999_999.0
    changed = experiment.compute_same_slot_rvol20(
        mutated,
        official_session_dates=official,
        signal_slots=("09:25", "09:30"),
    )
    earlier = observed["session_date"].ne(official[-1])
    pd.testing.assert_frame_equal(
        observed.loc[earlier].reset_index(drop=True),
        changed.loc[earlier].reset_index(drop=True),
    )
    slot_0930 = observed.loc[
        observed["signal_end"].eq("09:30")
        & observed["session_date"].eq(official[-1])
    ].iloc[0]
    assert slot_0930["prior_slot_volume_median"] == pytest.approx(1_450.0)


def test_slot_rvol_overlay_fails_closed_and_thresholds_are_nested() -> None:
    candidates = _candidate_frame()
    candidates["current_five_min_volume"] = candidates["five_min_volume"]
    candidates["prior_slot_observation_count_20"] = [9, 10, 20, 20, 20]
    candidates["prior_slot_volume_median"] = [1_000.0] * len(candidates)
    candidates["slot_rvol20"] = [math.nan, 1.49, 1.50, 2.00, 2.50]
    candidates["feature_available"] = [False, True, True, True, True]
    r150, d150 = experiment.apply_selection_overlay(
        candidates, config.get_spec("SLOT_RVOL_150")
    )
    r200, _ = experiment.apply_selection_overlay(
        candidates, config.get_spec("SLOT_RVOL_200")
    )
    assert set(r200["candidate_id"]) < set(r150["candidate_id"])
    reasons = set(d150.loc[~d150["selection_passed"], "selection_reason"])
    assert "SLOT_RVOL_HISTORY_INSUFFICIENT" in reasons
    assert "SLOT_RVOL_BELOW_MINIMUM" in reasons
    assert any("|C" in value for value in r150["candidate_id"])
    assert any("|D" in value for value in r200["candidate_id"])


def test_cli_requires_explicit_variant_for_replay() -> None:
    with pytest.raises(ValueError, match="explicit --variant"):
        experiment.main(["run", "--from-day", "2026-08-01"])


def test_cli_help_does_not_require_variant(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        experiment.main(["run", "--help"])
    assert exc_info.value.code == 0
    assert "--variant" in capsys.readouterr().out


def test_comparison_pack_is_separate_and_can_require_exact_parity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit = pd.DataFrame(
        [
            {
                "candidate_id": "A",
                "filled": True,
                "net_return_pct": 1.0,
            },
            {
                "candidate_id": "B",
                "filled": True,
                "net_return_pct": -0.5,
            },
        ]
    )
    daily = pd.DataFrame(
        [
            {
                "session_date": "2026-08-03",
                "candidates": 2,
                "fills": 2,
                "net_return_pct": 0.5,
                "net_pnl_rs": 250.0,
                "period": "FULL",
            }
        ]
    )
    diagnostics = pd.DataFrame(
        [
            {
                "schema_version": "test",
                "dimension": "side",
                "bucket": "LONG",
                "fills": 2,
            }
        ]
    )
    audit_path = tmp_path / "audit.csv"
    daily_path = tmp_path / "daily.csv"
    diagnostics_path = tmp_path / "diagnostics.csv"
    coverage_path = tmp_path / "coverage.csv"
    setups_path = tmp_path / "setups.csv"
    audit.to_csv(audit_path, index=False)
    daily.to_csv(daily_path, index=False)
    diagnostics.to_csv(diagnostics_path, index=False)
    pd.DataFrame([{"symbol": "TEST", "sessions": 1}]).to_csv(
        coverage_path, index=False
    )
    pd.DataFrame([{"setup_id": "09:30_LONG"}]).to_csv(
        setups_path, index=False
    )
    payload = {
        "parameters": {"variant": "V10B"},
        "backtest_window": {
            "from_day": "2026-08-03",
            "through_day": "2026-08-03",
            "split_day": None,
        },
        "cache_input_fingerprint": "same-cache",
        "backtest_input_fingerprint": "run-fingerprint",
        "results": {
            "sessions": 1,
            "candidates": 2,
            "fills": 2,
            "headline_valid": False,
            "promotion_eligible": False,
            "diagnostic_closed_trade_metrics": {
                "profit_factor": 2.0,
                "net_return_percentage_points": 0.5,
                "net_pnl_rs": 250.0,
                "max_daily_drawdown_percentage_points": 0.5,
            },
        },
        "outputs": {
            "candidate_order_audit": {"path": str(audit_path)},
            "daily": {"path": str(daily_path)},
            "diagnostic_breakdowns": {"path": str(diagnostics_path)},
            "coverage": {"path": str(coverage_path)},
            "setups": {"path": str(setups_path)},
        },
    }
    control_payload = {**payload, "v10_run_schema_version": "test-v10"}
    challenger_payload = {
        **payload,
        "v10_run_schema_version": "test-v10",
        "v10_experiment_run_schema_version": "test-stage1",
    }
    control_provenance = tmp_path / "control.json"
    challenger_provenance = tmp_path / "challenger.json"
    control_provenance.write_text("{}", encoding="utf-8")
    challenger_provenance.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        compare,
        "validate_provenance",
        lambda path: (
            control_payload if Path(path) == control_provenance else challenger_payload
        ),
    )
    output = compare.build_comparison(
        control_provenance=control_provenance,
        challenger_provenance=challenger_provenance,
        output_dir=tmp_path / "comparison",
        require_control_parity=False,
    )
    manifest = compare.load_json(output / "comparison_manifest.json")
    assert manifest["core_audit_parity"]["parity"] is True
    assert (output / "aggregate_comparison.csv").is_file()
    assert (output / "daywise_comparison.csv").is_file()
    assert (output / "artifact_parity.csv").is_file()
    with pytest.raises(AssertionError, match="frozen Stage 0"):
        compare.build_comparison(
            control_provenance=control_provenance,
            challenger_provenance=challenger_provenance,
            output_dir=tmp_path / "strict-comparison",
            require_control_parity=True,
        )


def test_generic_comparison_accepts_stage1_v10b_control() -> None:
    control = {
        "v10_run_schema_version": "test-v10",
        "v10_experiment_run_schema_version": "test-stage1",
        "v10_experiment_variant": "V10B",
        "parameters": {"variant": "V10B"},
    }
    challenger = {
        "v10_run_schema_version": "test-v10",
        "v10_experiment_run_schema_version": "test-stage1",
        "v10_experiment_variant": "RV1_100",
        "parameters": {"variant": "RV1_100"},
    }
    compare.assert_comparison_identity(
        control, challenger, require_control_parity=False
    )
    with pytest.raises(AssertionError, match="non-experiment Stage 0"):
        compare.assert_comparison_identity(
            control, challenger, require_control_parity=True
        )
