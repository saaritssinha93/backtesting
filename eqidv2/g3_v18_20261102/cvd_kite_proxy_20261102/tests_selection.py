"""Native parity, causality, matched coverage, and pre-ranking replacement."""
import numpy as np
import pandas as pd
import pytest

from g3_v18_20261102.tests_selection import candidate
from . import selection as s


def row(symbol="EXAMPLE", sid=1, **changes):
    result = candidate(symbol=symbol, sid=sid)
    result.update(cvd_proxy_ready=True, cvd_proxy_feature_ts=result["signal_ts"],
                  cvd_proxy_session=-100., cvd_proxy_recent15=-30.,
                  cvd_proxy_body_session=-50., cvd_proxy_body_recent15=-15.)
    result.update(changes)
    return result


def test_base_control_is_exact_and_input_unchanged():
    frame = pd.DataFrame([row()])
    before = frame.copy(deep=True)
    expected, audit = s.base.select(frame, "control")
    actual, actual_audit = s.select(frame, "base_control")
    pd.testing.assert_frame_equal(expected, actual)
    pd.testing.assert_frame_equal(audit, actual_audit)
    pd.testing.assert_frame_equal(frame, before)


def test_filter_before_ranking_replaces_leader():
    frame = pd.DataFrame([row("LEADER", 1, price_change_pct=-.8, cvd_proxy_session=100.),
                          row("REPLACEMENT", 2, price_change_pct=-.6)])
    assert s.select(frame, "covered_control")[0].tradingsymbol.tolist() == ["LEADER"]
    orders, audit = s.select(frame, "cvd_session")
    assert orders.tradingsymbol.tolist() == ["REPLACEMENT"]
    assert audit.set_index("tradingsymbol").loc["LEADER", "v9_decision"] == "CVD_PROXY_DIRECTION_REJECTED"


@pytest.mark.parametrize("change", [dict(cvd_proxy_ready=False), dict(cvd_proxy_session=np.nan),
    dict(cvd_proxy_body_recent15=np.inf), dict(cvd_proxy_feature_ts=pd.Timestamp("2026-10-23 09:35", tz="Asia/Kolkata"))])
def test_all_covered_arms_fail_closed_identically(change):
    frame = pd.DataFrame([row(**change)])
    assert len(s.select(frame, "base_control")[0]) == 1
    for variant in s.VARIANTS[1:]:
        assert s.select(frame, variant)[0].empty


def test_zero_has_coverage_but_no_direction():
    frame = pd.DataFrame([row(cvd_proxy_session=0., cvd_proxy_recent15=0.,
                              cvd_proxy_body_session=0., cvd_proxy_body_recent15=0.)])
    assert len(s.select(frame, "covered_control")[0]) == 1
    for variant in s.VARIANTS[2:]:
        assert s.select(frame, variant)[0].empty


def test_symmetric_direction_and_distinct_gates():
    frame = pd.DataFrame([row(side="LONG", cvd_proxy_session=100., cvd_proxy_recent15=-30.,
                             cvd_proxy_body_session=20., cvd_proxy_body_recent15=10.)])
    assert s.gate(frame, "cvd_session").iloc[0]
    assert not s.gate(frame, "cvd_recent15").iloc[0]
    assert not s.gate(frame, "cvd_both").iloc[0]
    assert s.gate(frame, "body_both").iloc[0]


@pytest.mark.parametrize("variant", s.VARIANTS)
def test_empty_and_duplicate_candidates(variant):
    frame = pd.DataFrame([row()])
    assert s.select(frame.iloc[:0], variant)[0].empty
    with pytest.raises(ValueError, match="globally unique"):
        s.select(pd.concat([frame, frame]), variant)


def test_batched_selection_matches_individual():
    frame = pd.DataFrame([row("A", 1), row("B", 2, cvd_proxy_session=20.)])
    results = s.select_all(frame)
    for variant in s.VARIANTS:
        expected = s.select(frame, variant)
        for got, wanted in zip(results[variant], expected):
            pd.testing.assert_frame_equal(got, wanted)
