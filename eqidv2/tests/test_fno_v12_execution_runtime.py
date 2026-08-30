from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import fno_v10_gap_guard_research as gap_specs
import fno_v11_execution_runtime as v11_runtime
import fno_v11_gap_runtime as v11_gap_runtime
import fno_v12_execution_runtime as runtime_hooks
import fno_v8_windowed_1m_entry_backtest as engine


IST = "Asia/Kolkata"
SIGNAL_TIME = pd.Timestamp("2026-08-18 09:30", tz=IST)


def _setup(
    *,
    signal_end: str = "09:30",
    side: str = "SHORT",
    picker: str = "max_move",
    entry_conf_minute: int | None = 3,
) -> engine.V8Setup:
    return engine.V8Setup(
        signal_end=signal_end,
        side=side,
        max_entries=2,
        picker=picker,
        price_change_pct=0.20,
        oi_change_pct=0.10,
        volume_ratio=1.0,
        body_ratio=0.0,
        max_wick_ratio=1.0,
        min_traded_value=0.0,
        stop_pct=5.0,
        target_pct=0.10,
        entry_conf_minute=entry_conf_minute,
        entry_buffer_bps=0.0,
        entry_midpoint=False,
        entry_clv=None,
    )


def _candidate(symbol: str = "TEST", *, signal_time: pd.Timestamp = SIGNAL_TIME) -> engine.CandidateInput:
    return engine.CandidateInput(
        symbol=symbol,
        signal_time=signal_time,
        five_min_open=100.5,
        five_min_high=101.0,
        five_min_low=99.0,
        five_min_close=100.0,
        price_change_pct=-1.0,
        oi_change_pct=1.0,
        volume_ratio=2.0,
        traded_value=10_000_000.0,
        tick_size=0.05,
        five_min_volume=5_000.0,
    )


def _bar(
    minute: int,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> engine.MinuteBar:
    return engine.MinuteBar(
        timestamp=SIGNAL_TIME + pd.Timedelta(minutes=minute),
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1_000.0,
    )


def _short_window_bars(*, s3_close: float = 99.30) -> list[engine.MinuteBar]:
    return [
        _bar(1, open_=99.70, high=99.90, low=99.60, close=99.80),
        _bar(2, open_=99.80, high=99.90, low=99.40, close=99.50),
        _bar(
            3,
            open_=99.55,
            high=99.60,
            low=min(99.20, s3_close - 0.05),
            close=s3_close,
        ),
        _bar(4, open_=99.40, high=99.45, low=99.00, close=99.10),
        _bar(5, open_=99.10, high=99.20, low=99.00, close=99.05),
    ]


def _policy() -> engine.EntryPolicy:
    return engine.EntryPolicy(
        buffer_bps=0.0,
        max_confirmation_minute=3,
        entry_expiry_minute=5,
        close_location_min=None,
        cost_bps=0.0,
        slippage_bps=0.0,
        midpoint_invalidation=False,
        post_confirmation_cancel=True,
        allow_cap_reassignment=True,
    )


def _candidate_id(setup: engine.V8Setup, candidate: engine.CandidateInput) -> str:
    return (
        f"{candidate.session_date.isoformat()}|{setup.setup_id}|"
        f"{candidate.symbol}"
    )


def test_runtime_spec_validates_modes_legs_expiry_and_score_mapping() -> None:
    neutral = runtime_hooks.RuntimeSpec()
    neutral.validate()
    assert neutral.is_neutral

    active = runtime_hooks.RuntimeSpec(
        m2_short_mode=runtime_hooks.M2_MODE_RECONFIRM_S3,
        m2_short_setup_ids=("09:25_SHORT",),
        long_entry_expiry_minute=4,
        equal_rank_picker_scores={
            "2026-08-18|09:40_SHORT|TEST": 1.25,
        },
    )
    active.validate()
    assert active.active_mechanisms == (
        "M2_SHORT",
        "LONG_ENTRY_EXPIRY",
        "EQUAL_RANK_PICKER",
    )
    payload = active.payload()
    assert payload["equal_rank_picker_score_count"] == 1
    assert len(payload["equal_rank_picker_scores_sha256"]) == 64

    invalid = (
        runtime_hooks.RuntimeSpec(m2_short_mode="UNKNOWN"),
        runtime_hooks.RuntimeSpec(
            m2_short_mode=runtime_hooks.M2_MODE_DELAY_S4,
            m2_short_setup_ids=(),
        ),
        runtime_hooks.RuntimeSpec(
            m2_short_mode=runtime_hooks.M2_MODE_DELAY_S4,
            m2_short_setup_ids=("09:40_SHORT",),
        ),
        runtime_hooks.RuntimeSpec(long_entry_expiry_minute=5),
        runtime_hooks.RuntimeSpec(
            equal_rank_picker_scores={"2026-08-18|09:25_SHORT|TEST": 1.0}
        ),
        runtime_hooks.RuntimeSpec(
            equal_rank_picker_scores={
                "2026-08-18|09:40_SHORT|TEST": float("nan")
            }
        ),
    )
    for spec in invalid:
        with pytest.raises((TypeError, ValueError)):
            spec.validate()


def test_runtime_spec_adapter_does_not_import_registry_types() -> None:
    rule = SimpleNamespace(
        m2_short_mode=runtime_hooks.M2_MODE_DELAY_S4,
        m2_short_setup_ids=("09:30_SHORT",),
        long_entry_expiry_minute=None,
    )
    spec = runtime_hooks.runtime_spec_from_rule(rule)
    assert spec.m2_short_mode == runtime_hooks.M2_MODE_DELAY_S4
    assert spec.m2_short_setup_ids == ("09:30_SHORT",)


def test_delay_s4_suppresses_only_s3_for_an_s2_confirmation() -> None:
    setup = _setup()
    candidate = _candidate()
    trade = engine._CandidateRuntime(
        candidate=candidate,
        confirmation_minute=2,
        trigger=99.50,
    )
    spec = runtime_hooks.RuntimeSpec(
        m2_short_mode=runtime_hooks.M2_MODE_DELAY_S4,
        m2_short_setup_ids=("09:30_SHORT",),
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        s3 = engine._entry_fill(
            setup,
            trade,
            _bar(3, open_=99.40, high=99.60, low=99.20, close=99.30),
            _policy(),
        )
        s4 = engine._entry_fill(
            setup,
            trade,
            _bar(4, open_=99.40, high=99.60, low=99.20, close=99.30),
            _policy(),
        )
        audit = engine._audit_record(setup, trade)

    assert s3 is None
    assert s4 == pytest.approx((99.40, True))
    assert audit["v12_m2_delay_applied"] is True
    assert audit["v12_m2_delay_fill_checks_suppressed"] == 1
    assert audit["v12_m2_delay_touch_observed"] is True


def test_delay_s4_leg_attribution_leaves_the_other_short_leg_unchanged() -> None:
    setup = _setup(signal_end="09:25")
    candidate = _candidate(
        signal_time=pd.Timestamp("2026-08-18 09:25", tz=IST)
    )
    bar = engine.MinuteBar(
        timestamp=pd.Timestamp("2026-08-18 09:28", tz=IST),
        open=99.40,
        high=99.60,
        low=99.20,
        close=99.30,
        volume=1_000.0,
    )
    trade = engine._CandidateRuntime(
        candidate=candidate,
        confirmation_minute=2,
        trigger=99.50,
    )
    spec = runtime_hooks.RuntimeSpec(
        m2_short_mode=runtime_hooks.M2_MODE_DELAY_S4,
        m2_short_setup_ids=("09:30_SHORT",),
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        fill = engine._entry_fill(setup, trade, bar, _policy())
        audit = engine._audit_record(setup, trade)

    assert fill == pytest.approx((99.40, True))
    assert audit["v12_m2_targeted"] is False


def test_reconfirm_s3_defers_confirmation_ranking_trigger_and_fill() -> None:
    setup = _setup()
    candidate = _candidate()
    spec = runtime_hooks.RuntimeSpec(
        m2_short_mode=runtime_hooks.M2_MODE_RECONFIRM_S3,
        m2_short_setup_ids=("09:30_SHORT",),
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        audit = engine.simulate_setup_window(
            setup,
            [candidate],
            {candidate.symbol: _short_window_bars()},
            _policy(),
        )

    row = audit.iloc[0]
    assert int(row["confirmation_minute"]) == 3
    assert int(row["entry_minute"]) == 4
    assert float(row["trigger"]) == pytest.approx(99.20)
    assert row["v12_m2_s2_base_passed"]
    assert row["v12_m2_s3_reconfirmation_evaluated"]
    assert row["v12_m2_s3_base_passed"]
    assert row["events"][0]["event_ts"] == SIGNAL_TIME + pd.Timedelta(minutes=3)
    s2_check = next(
        check for check in row["confirmation_checks"] if check["minute_index"] == 2
    )
    assert not s2_check["passed"]
    assert "S2_PROVISIONAL_REQUIRES_S3" in s2_check["rejection_codes"]


@pytest.mark.parametrize(
    "mode,s3_close,expected_pass,expected_threshold,expected_code",
    [
        (
            runtime_hooks.M2_MODE_RECONFIRM_EXTEND_1TICK,
            99.40,
            True,
            99.45,
            None,
        ),
        (
            runtime_hooks.M2_MODE_RECONFIRM_EXTEND_1TICK,
            99.48,
            False,
            99.45,
            "S3_RECONFIRMATION_EXTENSION_1TICK_NOT_MET",
        ),
        (
            runtime_hooks.M2_MODE_RECONFIRM_EXTEND_2BPS,
            99.47,
            True,
            99.5 * (1.0 - 2.0 / 10_000.0),
            None,
        ),
        (
            runtime_hooks.M2_MODE_RECONFIRM_EXTEND_2BPS,
            99.49,
            False,
            99.5 * (1.0 - 2.0 / 10_000.0),
            "S3_RECONFIRMATION_EXTENSION_2BPS_NOT_MET",
        ),
    ],
)
def test_reconfirmation_extension_is_measured_from_the_s2_close(
    mode: str,
    s3_close: float,
    expected_pass: bool,
    expected_threshold: float,
    expected_code: str | None,
) -> None:
    setup = _setup()
    candidate = _candidate()
    spec = runtime_hooks.RuntimeSpec(
        m2_short_mode=mode,
        m2_short_setup_ids=("09:30_SHORT",),
    )
    s2 = _bar(2, open_=99.80, high=99.90, low=99.40, close=99.50)
    s3 = _bar(
        3,
        open_=99.55,
        high=99.60,
        low=min(99.35, s3_close - 0.01),
        close=s3_close,
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        provisional = engine._confirmation_check(setup, candidate, s2, _policy())
        reconfirmed = engine._confirmation_check(setup, candidate, s3, _policy())
        audit = engine._audit_record(
            setup, engine._CandidateRuntime(candidate=candidate)
        )

    assert provisional["v12_m2_s2_base_passed"] is True
    assert provisional["passed"] is False
    assert reconfirmed["passed"] is expected_pass
    assert reconfirmed["v12_m2_extension_threshold"] == pytest.approx(
        expected_threshold
    )
    assert reconfirmed["v12_m2_extension_passed"] is expected_pass
    assert audit["v12_m2_extension_threshold"] == pytest.approx(expected_threshold)
    if expected_code is None:
        assert not any("EXTENSION" in code for code in reconfirmed["rejection_codes"])
    else:
        assert expected_code in reconfirmed["rejection_codes"]


def test_s3_is_not_treated_as_reconfirmation_when_s2_did_not_pass() -> None:
    setup = _setup()
    candidate = _candidate()
    spec = runtime_hooks.RuntimeSpec(
        m2_short_mode=runtime_hooks.M2_MODE_RECONFIRM_EXTEND_1TICK,
        m2_short_setup_ids=("09:30_SHORT",),
    )
    s2_fail = _bar(2, open_=99.50, high=99.70, low=99.40, close=99.60)
    s3_pass = _bar(3, open_=99.60, high=99.65, low=99.45, close=99.50)

    with runtime_hooks.installed_runtime_hooks(spec):
        assert not engine._confirmation_check(
            setup, candidate, s2_fail, _policy()
        )["passed"]
        result = engine._confirmation_check(setup, candidate, s3_pass, _policy())

    assert result["passed"] is True
    assert "v12_m2_s3_reconfirmation_evaluated" not in result


@pytest.mark.parametrize(
    "expiry,expected_max_confirmation,expected_clamped",
    [(4, 3, False), (3, 2, True)],
)
def test_long_expiry_is_resolved_per_setup_and_s3_clamps_confirmation(
    expiry: int,
    expected_max_confirmation: int,
    expected_clamped: bool,
) -> None:
    long_setup = _setup(side="LONG", signal_end="09:25", entry_conf_minute=3)
    short_setup = _setup(side="SHORT", signal_end="09:25", entry_conf_minute=3)
    base = _policy()
    spec = runtime_hooks.RuntimeSpec(long_entry_expiry_minute=expiry)

    with runtime_hooks.installed_runtime_hooks(spec):
        long_policy = engine.policy_for_setup(long_setup, base)
        short_policy = engine.policy_for_setup(short_setup, base)
        audit = engine._audit_record(
            long_setup, engine._CandidateRuntime(candidate=_candidate())
        )

    assert long_policy.entry_expiry_minute == expiry
    assert long_policy.max_confirmation_minute == expected_max_confirmation
    assert short_policy.entry_expiry_minute == 5
    assert short_policy.max_confirmation_minute == 3
    assert audit["v12_long_expiry_applied"] is True
    assert audit["v12_effective_entry_expiry_minute"] == expiry
    assert audit["v12_effective_max_confirmation_minute"] == expected_max_confirmation
    assert audit["v12_long_confirmation_window_clamped"] is expected_clamped


def test_equal_rank_picker_orders_scores_descending_and_audits_mapping() -> None:
    setup = _setup(
        signal_end="09:40",
        side="SHORT",
        picker=runtime_hooks.EQUAL_RANK_PICKER,
        entry_conf_minute=1,
    )
    signal_time = pd.Timestamp("2026-08-18 09:40", tz=IST)
    first = _candidate("FIRST", signal_time=signal_time)
    second = _candidate("SECOND", signal_time=signal_time)
    scores = {
        _candidate_id(setup, first): 0.25,
        _candidate_id(setup, second): 0.75,
    }
    spec = runtime_hooks.RuntimeSpec(equal_rank_picker_scores=scores)

    with runtime_hooks.installed_runtime_hooks(spec):
        ranked = engine._rank_candidates(
            setup,
            [
                engine._CandidateRuntime(candidate=first),
                engine._CandidateRuntime(candidate=second),
            ],
        )
        audit = engine._audit_record(setup, ranked[0])

    assert [runtime.candidate.symbol for runtime in ranked] == ["SECOND", "FIRST"]
    assert audit["v12_equal_rank_picker_applied"] is True
    assert audit["v12_equal_rank_picker_score"] == pytest.approx(0.75)
    assert audit["v12_equal_rank_picker_score_count"] == 2


def test_equal_rank_picker_fails_closed_for_an_unmapped_target_candidate() -> None:
    setup = _setup(
        signal_end="09:40",
        picker=runtime_hooks.EQUAL_RANK_PICKER,
        entry_conf_minute=1,
    )
    candidate = _candidate(
        "MISSING", signal_time=pd.Timestamp("2026-08-18 09:40", tz=IST)
    )
    spec = runtime_hooks.RuntimeSpec(
        equal_rank_picker_scores={
            "2026-08-18|09:40_SHORT|OTHER": 1.0,
        }
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        with pytest.raises(ValueError, match="no score"):
            engine._picker_value(setup, candidate)


def test_equal_rank_mapping_does_not_override_non_target_native_picker() -> None:
    target_id = "2026-08-18|09:40_SHORT|TARGET"
    spec = runtime_hooks.RuntimeSpec(equal_rank_picker_scores={target_id: 999.0})
    setup = _setup(signal_end="09:35", picker="max_move")
    candidate = _candidate(
        "TARGET", signal_time=pd.Timestamp("2026-08-18 09:35", tz=IST)
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        value = engine._picker_value(setup, candidate)

    assert value == pytest.approx(abs(candidate.price_change_pct))


def test_all_engine_seams_restore_even_when_the_context_raises() -> None:
    originals = {
        "confirmation": engine._confirmation_check,
        "entry": engine._entry_fill,
        "policy": engine.policy_for_setup,
        "picker": engine._picker_value,
        "audit": engine._audit_record,
    }
    spec = runtime_hooks.RuntimeSpec(
        m2_short_mode=runtime_hooks.M2_MODE_RECONFIRM_S3,
        long_entry_expiry_minute=4,
        equal_rank_picker_scores={
            "2026-08-18|09:40_SHORT|TEST": 1.0,
        },
    )

    with pytest.raises(RuntimeError, match="boom"):
        with runtime_hooks.installed_runtime_hooks(spec):
            assert engine._confirmation_check is not originals["confirmation"]
            assert engine._entry_fill is not originals["entry"]
            assert engine.policy_for_setup is not originals["policy"]
            assert engine._picker_value is not originals["picker"]
            assert engine._audit_record is not originals["audit"]
            raise RuntimeError("boom")

    assert engine._confirmation_check is originals["confirmation"]
    assert engine._entry_fill is originals["entry"]
    assert engine.policy_for_setup is originals["policy"]
    assert engine._picker_value is originals["picker"]
    assert engine._audit_record is originals["audit"]


def test_v11_v12_gap2_nesting_preserves_all_audit_layers_and_restores() -> None:
    original_entry = engine._entry_fill
    original_audit = engine._audit_record
    setup = _setup()
    trade = engine._CandidateRuntime(candidate=_candidate())
    v11_spec = v11_runtime.RuntimeSpec(
        entry_setup_id="09:30_SHORT",
        entry_not_before_minute=3,
        same_side_symbol_limit=2,
    )
    v12_spec = runtime_hooks.RuntimeSpec(
        m2_short_mode=runtime_hooks.M2_MODE_RECONFIRM_S3,
        m2_short_setup_ids=("09:30_SHORT",),
    )
    gap2 = gap_specs.GapGuardSpec("MAX_2_BPS", 2.0)

    with v11_runtime.installed_runtime_hooks(v11_spec, allow_composite=True):
        with runtime_hooks.installed_runtime_hooks(v12_spec):
            with v11_gap_runtime.installed_gap_guard(gap2):
                audit = engine._audit_record(setup, trade)
                assert engine._entry_fill is not original_entry
                assert engine._audit_record is not original_audit

    assert audit["v11_runtime_schema_version"]
    assert audit["v12_runtime_schema_version"]
    assert audit["v11_gap_runtime_schema_version"]
    assert audit["gap_guard_variant"] == "MAX_2_BPS"
    assert engine._entry_fill is original_entry
    assert engine._audit_record is original_audit
