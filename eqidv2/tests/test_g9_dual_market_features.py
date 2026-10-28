"""Synthetic correctness/causality tests, not strategy-performance evidence."""
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from research import g9_dual_market_features as m


START = datetime(2026, 10, 23, 10, 0, tzinfo=m.IST)
END = START + timedelta(minutes=5)
CONFIG = m.GateConfig(max_oi_age_seconds=60, max_quote_age_seconds=5,
                      max_spread_bps=200, min_history_sessions=2)


def quote(instrument="ABC", seconds=-1, sequence=1, bid=100, bid_size=10,
          ask=101, ask_size=20):
    stamp = START + timedelta(seconds=seconds)
    return m.QuoteUpdate(instrument, stamp, stamp, sequence, bid, bid_size, ask, ask_size)


def trade(instrument="ABC", seconds=299, quantity=10, aggressor=1, trade_id="t1"):
    stamp = START + timedelta(seconds=seconds)
    return m.TradePrint(instrument, stamp, stamp, trade_id, 100.5, quantity, aggressor)


def flow(instrument="ABC", **kwargs):
    args = dict(quotes=[quote(instrument), quote(instrument, seconds=299, sequence=2,
                                               bid_size=20, ask_size=10)],
                trades=[trade(instrument)], window_start=START, window_end=END,
                observed_at=END, subscription_started_at=START - timedelta(minutes=1),
                sequence_complete=True, real_trade_prints=True, max_quote_age_seconds=5)
    args.update(kwargs)
    return m.build_market_flow(**args)


def candidate(symbol="ABC"):
    return dict(symbol=symbol, future_contract=symbol + "26OCTFUT",
                oi_start_contract=symbol + "26OCTFUT", oi_end_contract=symbol + "26OCTFUT",
                window_start=START, window_end=END, available_at=END,
                futures_return_5m=.001, spot_return_5m=.002, oi_delta_5m=10,
                oi_delta_percent=1.0, oi_age_seconds=10,
                futures_flow=flow(symbol + "26OCTFUT"), spot_flow=flow(symbol))


def history(symbol="ABC"):
    return [dict(symbol=symbol, window_end=END - timedelta(days=day),
                 available_at=END - timedelta(days=day), oi_delta_percent=oi,
                 futures_ofi=0, futures_cvd=0, spot_ofi=0, spot_cvd=0)
            for day, oi in ((1, .5), (2, 2))]


@pytest.mark.parametrize("bid,bid_size,ask,ask_size,expected", [
    (100, 15, 101, 15, 10),  # Equal prices: differences in both queues.
    (101, 30, 102, 40, 50),
    (99, 30, 100, 40, -50),
    (100.5, 15, 101, 25, 10),
    (100, 15, 100.5, 25, -20),
])
def test_canonical_ofi_includes_all_four_terms(bid, bid_size, ask, ask_size, expected):
    assert m.canonical_ofi(quote(), quote(seconds=0, sequence=2, bid=bid,
                                        bid_size=bid_size, ask=ask, ask_size=ask_size)) == expected


@pytest.mark.parametrize("change", [dict(sequence=3), dict(sequence=1),
                                     dict(bid=101, ask=101), dict(bid=102, ask=101),
                                     dict(instrument="OTHER"), dict(bid_size=0)])
def test_bad_books_and_nonconsecutive_updates_rejected(change):
    with pytest.raises(ValueError):
        m.canonical_ofi(quote(), replace(quote(seconds=0, sequence=2), **change))


def test_cvd_total_denominator_includes_unknown_volume():
    result = flow(trades=[trade(seconds=10, quantity=10, aggressor=1, trade_id="a"),
                          trade(seconds=20, quantity=4, aggressor=-1, trade_id="b"),
                          trade(seconds=299, quantity=6, aggressor=None, trade_id="c")])
    assert result.cvd_delta == 6
    assert result.cvd_ratio == pytest.approx(.3)
    assert result.classification_coverage == pytest.approx(.7)
    assert result.classified_volume == 14
    assert result.unknown_volume == 6
    assert result.recent_cvd_delta is None
    assert result.recent_classification_coverage == 0


def test_all_unknown_is_missing_not_zero_evidence():
    result = flow(trades=[trade(aggressor=0)])
    assert result.cvd_delta is None and result.cvd_ratio is None
    assert result.classification_coverage == 0
    assert "unclassified_trade_flow" in result.reasons


def test_half_open_window_and_receive_time_exclude_future_evidence():
    base = flow()
    future_quote = quote(seconds=300, sequence=3, bid_size=999)
    late_quote = replace(quote(seconds=299.5, sequence=3, bid_size=999),
                         received_at=END + timedelta(seconds=1))
    result = flow(quotes=[quote(), quote(seconds=299, sequence=2, bid_size=20, ask_size=10),
                          late_quote, future_quote],
                  trades=[trade(), trade(seconds=300, aggressor=-1, quantity=999, trade_id="end"),
                          replace(trade(seconds=299.5, aggressor=-1, quantity=999, trade_id="late"),
                                  received_at=END + timedelta(seconds=1))])
    assert result == base


@pytest.mark.parametrize("kwargs,reason", [
    ({"subscription_started_at": START + timedelta(seconds=1)}, "incomplete_subscription_window"),
    ({"sequence_complete": False}, "incomplete_book_sequence"),
    ({"real_trade_prints": False}, "real_trade_prints_unavailable"),
    ({"quotes": []}, "missing_book_updates"),
    ({"trades": []}, "missing_trade_prints"),
    ({"trades": [trade(), trade()]}, "invalid_trade_prints"),
    ({"max_quote_age_seconds": None}, "quote_freshness_uncalibrated"),
])
def test_missing_or_unreliable_sources_never_qualify(kwargs, reason):
    result = flow(**kwargs)
    assert reason in result.reasons
    row = candidate()
    row["spot_flow"] = result
    assert m.candidate_rejection_reasons(row, CONFIG)


def test_full_candidate_passes_only_with_explicit_quality_thresholds():
    assert not m.candidate_rejection_reasons(candidate(), CONFIG)
    assert "uncalibrated_max_oi_age_seconds" in m.candidate_rejection_reasons(candidate(), m.GateConfig())
    reasons = m.candidate_rejection_reasons({}, CONFIG)
    assert "missing_futures_flow" in reasons and "missing_spot_flow" in reasons


@pytest.mark.parametrize("key,value,reason", [
    ("futures_return_5m", 0, "nonpositive_or_missing_futures_return_5m"),
    ("spot_return_5m", None, "nonpositive_or_missing_spot_return_5m"),
    ("oi_delta_5m", -1, "nonpositive_or_missing_oi_delta_5m"),
    ("oi_age_seconds", 61, "stale_oi"),
    ("oi_end_contract", "NEXT_MONTH", "oi_contract_mismatch"),
])
def test_candidate_positioning_and_contract_gates(key, value, reason):
    row = candidate()
    row[key] = value
    assert reason in m.candidate_rejection_reasons(row, CONFIG)


def test_recent_trade_coverage_gate_is_separate_from_full_window_coverage():
    row = candidate()
    row["spot_flow"] = flow(trades=[trade(seconds=1, quantity=1000, trade_id="early"),
                                     trade(seconds=298, quantity=1, trade_id="known"),
                                     trade(seconds=299, quantity=1, aggressor=None, trade_id="unknown")])
    assert row["spot_flow"].classification_coverage > .95
    assert "insufficient_spot_recent_classification_coverage" in m.candidate_rejection_reasons(row, CONFIG)


def test_delayed_reuse_of_feature_does_not_freeze_book_freshness():
    row = candidate()
    row["available_at"] = END + timedelta(seconds=10)
    assert "stale_or_missing_spot_book" in m.candidate_rejection_reasons(row, CONFIG)


def test_ranking_decision_advances_oi_and_quote_age():
    result = m.score_candidate(candidate(), history(), decision_at=END + timedelta(seconds=61),
                               config=CONFIG)
    assert result.score is None
    assert "stale_oi" in result.reasons
    assert "stale_or_missing_spot_book" in result.reasons


def test_late_pre_window_quote_cannot_hide_observed_sequence_failure():
    result = flow(quotes=[quote(), quote(seconds=60, sequence=2),
                          replace(quote(seconds=-.5, sequence=3),
                                  received_at=START + timedelta(seconds=61)),
                          quote(seconds=299, sequence=4)])
    assert "invalid_book_sequence" in result.reasons


def test_percentiles_and_weights_are_prior_only_same_symbol_same_slot():
    row = candidate()
    base = m.score_candidate(row, history(), decision_at=END, config=CONFIG)
    assert base.eligible
    assert base.percentiles["oi_delta_percent"] == .5
    assert base.score == pytest.approx(87.5)
    contamination = [
        dict(history()[0], symbol="OTHER", oi_delta_percent=10000),
        dict(history()[0], window_end=END - timedelta(minutes=5), oi_delta_percent=10000),
        dict(history()[0], window_end=END + timedelta(days=1), oi_delta_percent=10000),
        dict(history()[0], window_end=END - timedelta(days=3, minutes=5), oi_delta_percent=10000),
        dict(history()[0], window_end=END - timedelta(days=3),
             available_at=END + timedelta(seconds=1), oi_delta_percent=10000),
    ]
    assert m.score_candidate(row, history() + contamination, decision_at=END, config=CONFIG) == base


def test_insufficient_or_duplicate_training_history_is_rejected():
    result = m.score_candidate(candidate(), history()[:1], decision_at=END, config=CONFIG)
    assert result.score is None
    assert "insufficient_prior_history_futures_ofi" in result.reasons
    result = m.score_candidate(candidate(), history() + history()[:1], decision_at=END, config=CONFIG)
    assert result.score is None and "duplicate_training_session_slot" in result.reasons


def test_observability_precedes_ranking_and_ties_are_deterministic():
    rows = [candidate("ZZZ"), candidate("AAA")]
    records = history("ZZZ") + history("AAA")
    ranked = m.rank_candidates(rows, records, decision_at=END, config=CONFIG, top_k=1)
    assert [(r.symbol, r.rank) for r in ranked] == [("AAA", 1)]
    rows[1]["available_at"] = END + timedelta(seconds=1)
    ranked = m.rank_candidates(rows, records, decision_at=END, config=CONFIG, top_k=1)
    assert [r.symbol for r in ranked] == ["ZZZ"]


def test_ranking_cannot_mix_windows_or_duplicate_underlyings():
    with pytest.raises(ValueError, match="decision slot"):
        m.rank_candidates([candidate(), dict(candidate("DEF"), window_end=END - timedelta(minutes=5))],
                          [], decision_at=END, config=CONFIG)
    with pytest.raises(ValueError, match="once"):
        m.rank_candidates([candidate(), candidate()], [], decision_at=END, config=CONFIG)
