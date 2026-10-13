"""The first 09:20 OI comparison is a labelled four-minute bootstrap."""

import pytest

from fno_v13_v10_g_3_opening_oi import (
    bootstrap_0920_oi_change,
    bootstrap_0920_oi_change_from_quote,
    bootstrap_universe_0920_oi_changes,
)


DAY = "2026-10-12"
CONTRACT = "SUZLON26OCTFUT"
BOUNDARY = f"{DAY}T09:20:00+05:30"


def first_minute(**changes):
    return {
        "contract": CONTRACT,
        "instrument": "FUTURES",
        "price_basis": "DATED_NEAR_MONTH_FUTURES",
        "source": "KITE_HISTORICAL_MINUTE",
        "candle_start": f"{DAY}T09:15:00+05:30",
        "candle_end": f"{DAY}T09:16:00+05:30",
        "acquired_at": f"{DAY}T19:00:00+05:30",
        "oi": 413_461_200,
        **changes,
    }


def completed_bar(**changes):
    return {
        "contract": CONTRACT,
        "oi_source": "NEAR_MONTH_FUTURES_5M",
        "received_at": f"{DAY}T09:20:04+05:30",
        "oi": 414_451_800,
        "quality_state": "VALID",
        **changes,
    }


def provisional_quote(**changes):
    return {
        "contract": CONTRACT,
        "source": "KITE_FULL_QUOTE",
        "status": "QUOTE_NEAR_0916_NOT_CANDLE",
        "baseline_kind": "PROVISIONAL_FULL_QUOTE",
        "exact_first_minute_candle": False,
        "exchange_timestamp": f"{DAY}T09:16:08+05:30",
        "received_at": f"{DAY}T09:16:14+05:30",
        "oi": 413_461_200,
        **changes,
    }


def test_research_bootstrap_has_same_0920_change_with_explicit_four_minute_basis():
    feature = bootstrap_0920_oi_change(first_minute(), completed_bar(), boundary=BOUNDARY)
    assert feature["oi_change_pct"] == pytest.approx((414_451_800 / 413_461_200 - 1) * 100)
    assert feature["prev_oi"] == 413_461_200
    assert feature["oi"] == 414_451_800
    assert feature["oi_change_interval_minutes"] == 4
    assert feature["oi_change_from"] == f"{DAY}T09:16:00+05:30"
    assert feature["oi_change_to"] == BOUNDARY
    assert feature["availability"] == "RETROSPECTIVE_RESEARCH"
    assert feature["minute_received_by_0920"] is False


def test_live_bootstrap_requires_both_evidence_receipts_by_decision_time():
    minute = first_minute(acquired_at=f"{DAY}T09:16:03+05:30")
    bar = completed_bar()
    as_of = f"{DAY}T09:20:05+05:30"
    feature = bootstrap_0920_oi_change(minute, bar, boundary=BOUNDARY, as_of=as_of)
    assert feature["availability"] == "LIVE_AS_OF_VERIFIED"
    assert feature["minute_received_by_0920"] is True
    assert bootstrap_0920_oi_change(first_minute(), bar, boundary=BOUNDARY, as_of=as_of) is None
    assert bootstrap_0920_oi_change(minute, completed_bar(received_at=f"{DAY}T09:20:06+05:30"), boundary=BOUNDARY, as_of=as_of) is None
    assert bootstrap_0920_oi_change(minute, bar, boundary=BOUNDARY, as_of=f"{DAY}T09:19:59+05:30") is None


@pytest.mark.parametrize("minute,bar,boundary", [
    (first_minute(), completed_bar(contract="SUZLON26NOVFUT"), BOUNDARY),
    (first_minute(gap_filled=True), completed_bar(), BOUNDARY),
    (first_minute(), completed_bar(quality_state="GAP_FILLED"), BOUNDARY),
    (first_minute(), completed_bar(oi_source="PROXY"), BOUNDARY),
    (first_minute(oi=0), completed_bar(), BOUNDARY),
    (first_minute(), completed_bar(oi=0), BOUNDARY),
    (first_minute(), completed_bar(), f"{DAY}T09:25:00+05:30"),
])
def test_bootstrap_rejects_wrong_or_nonreal_evidence(minute, bar, boundary):
    assert bootstrap_0920_oi_change(minute, bar, boundary=boundary) is None


def test_dated_universe_retains_missing_stock_without_substituting_another_contract():
    universe = [
        {"symbol": "SUZLON", "contract": CONTRACT},
        {"symbol": "TEST", "contract": "TEST26OCTFUT"},
    ]
    rows = bootstrap_universe_0920_oi_changes(
        universe, {CONTRACT: first_minute()}, {CONTRACT: completed_bar()}, boundary=BOUNDARY,
    )
    assert [row["symbol"] for row in rows] == ["SUZLON", "TEST"]
    assert rows[0]["status"] == "VALID_0920_BOOTSTRAP"
    assert rows[0]["oi_change_pct"] == pytest.approx((414_451_800 / 413_461_200 - 1) * 100)
    assert rows[1] == {
        "symbol": "TEST", "contract": "TEST26OCTFUT",
        "status": "MISSING_OR_INVALID_EVIDENCE", "oi_change_pct": None,
    }


def test_dated_universe_rejects_duplicate_mapping():
    with pytest.raises(ValueError, match="duplicate"):
        bootstrap_universe_0920_oi_changes(
            [{"symbol": "SUZLON", "contract": CONTRACT}, {"symbol": "SUZLON", "contract": "SUZLON26NOVFUT"}],
            {}, {}, boundary=BOUNDARY,
        )


def test_timely_provisional_quote_can_bootstrap_future_live_0920_feature():
    value = bootstrap_0920_oi_change_from_quote(
        provisional_quote(), completed_bar(), boundary=BOUNDARY,
        as_of=f"{DAY}T09:20:05+05:30",
    )
    assert value["oi_change_pct"] == pytest.approx((414_451_800 / 413_461_200 - 1) * 100)
    assert value["oi_change_source"] == "PROVISIONAL_FULL_QUOTE"
    assert value["oi_change_interval_seconds"] == 232
    assert value["exact_first_minute_candle"] is False


@pytest.mark.parametrize("quote,bar,as_of", [
    (provisional_quote(contract="SUZLON26NOVFUT"), completed_bar(), f"{DAY}T09:20:05+05:30"),
    (provisional_quote(exchange_timestamp=f"{DAY}T09:16:16+05:30"), completed_bar(), f"{DAY}T09:20:05+05:30"),
    (provisional_quote(received_at=f"{DAY}T09:16:24+05:30"), completed_bar(), f"{DAY}T09:20:05+05:30"),
    (provisional_quote(received_at=f"{DAY}T09:20:00+05:30"), completed_bar(), f"{DAY}T09:20:05+05:30"),
    (provisional_quote(), completed_bar(received_at=f"{DAY}T09:20:06+05:30"), f"{DAY}T09:20:05+05:30"),
    (provisional_quote(oi=0), completed_bar(), f"{DAY}T09:20:05+05:30"),
])
def test_provisional_quote_rejects_wrong_stale_late_or_invalid_evidence(quote, bar, as_of):
    assert bootstrap_0920_oi_change_from_quote(quote, bar, boundary=BOUNDARY, as_of=as_of) is None


def test_universe_prefers_timely_first_minute_and_uses_quote_only_as_live_fallback():
    item = [{"symbol": "SUZLON", "contract": CONTRACT}]
    bar = completed_bar()
    minute = first_minute(acquired_at=f"{DAY}T09:16:03+05:30", oi=413_000_000)
    kwargs = dict(boundary=BOUNDARY, as_of=f"{DAY}T09:20:05+05:30",
                  provisional_quotes={CONTRACT: provisional_quote()})
    with_minute = bootstrap_universe_0920_oi_changes(item, {CONTRACT: minute}, {CONTRACT: bar}, **kwargs)[0]
    assert with_minute["oi_change_source"] == "FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M"
    without_minute = bootstrap_universe_0920_oi_changes(item, {}, {CONTRACT: bar}, **kwargs)[0]
    assert without_minute["oi_change_source"] == "PROVISIONAL_FULL_QUOTE"
