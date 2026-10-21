from datetime import date
import json

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_opening_inputs as inputs

DAY = date(2026, 10, 12)
FUTURE = "EXAMPLE26OCTFUT"


@pytest.fixture
def evidence(tmp_path):
    path = inputs.minute_path(tmp_path, DAY, FUTURE)
    path.parent.mkdir(parents=True)
    raw = dict(session_date=str(DAY), symbol="EXAMPLE", contract=FUTURE,
        source="KITE_HISTORICAL_MINUTE", acquired_at="2026-10-12T16:00:00+05:30",
        candles=[dict(candle_start="2026-10-12T09:15:00+05:30",
                      candle_end="2026-10-12T09:16:00+05:30", oi=100000)])
    path.write_text(json.dumps(raw))
    cash = dict(ts=pd.Timestamp("2026-10-12T09:20:00+05:30"), open=100., close=100.5,
                source_1m_count=5, price_change_pct=999.)
    future = pd.DataFrame([dict(timestamp=cash["ts"], tradingsymbol=FUTURE, expiry="2026-10-27",
                               oi=100200., fetch_timestamp="2026-10-12T09:20:04+05:30")])
    return tmp_path, path, raw, cash, future


def build(evidence, **kwargs):
    root, _, _, cash, future = evidence
    return inputs.build_opening_features(cash, future, day=DAY, symbol="EXAMPLE", contract=FUTURE,
                                        expiry="2026-10-27", root=root, **kwargs)


def quote(root, **changes):
    item = dict(symbol="EXAMPLE", contract=FUTURE, oi=100000, source="KITE_FULL_QUOTE",
        baseline_kind="PROVISIONAL_FULL_QUOTE", status="QUOTE_NEAR_0916_NOT_CANDLE",
        exact_first_minute_candle=False, exchange_timestamp="2026-10-12T09:16:04+05:30",
        received_at="2026-10-12T09:16:05+05:30")
    item.update(changes)
    path = root / "coverage" / f"live_opening_oi_{DAY}.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(dict(schema="dashboard_flow_live_opening_oi_v1", session_date=str(DAY),
                                   universe_source="DATED_UNIVERSE", rows=[item])))


def test_historical_exact_minute_and_open_to_close_leave_input_unchanged(evidence):
    before = evidence[3].copy()
    result = build(evidence)
    assert result["opening_0920_status"] == "VALID"
    assert result["oi_change_pct"] == pytest.approx(.2)
    assert result["price_change_pct"] == pytest.approx(.5)
    assert result["oi_change_interval_minutes"] == 4
    assert result["availability"] == "RETROSPECTIVE_RESEARCH"
    assert evidence[3] == before


def test_recovered_after_close_cannot_be_used_by_runtime(evidence):
    assert build(evidence, as_of="2026-10-12T09:20:10+05:30")["opening_0920_status"] != "VALID"


def test_live_timely_quote_is_provisional_and_matches_oi_since_opening(evidence):
    quote(evidence[0])
    result = build(evidence, as_of="2026-10-12T09:20:10+05:30")
    assert result["opening_0920_status"] == "VALID"
    assert result["oi_change_pct"] == pytest.approx(.2)
    assert result["oi_change_source"] == "PROVISIONAL_FULL_QUOTE"
    assert result["oi_change_interval_seconds"] == 236
    assert result["exact_first_minute_candle"] is False
    # Quotes are not substituted for historical first-minute candles.
    evidence[1].unlink()
    assert build(evidence)["opening_0920_status"] != "VALID"


@pytest.mark.parametrize("changes", [dict(received_at="2026-10-12T09:20:01+05:30"),
    dict(exchange_timestamp="2026-10-12T09:16:16+05:30"), dict(contract="EXAMPLE26NOVFUT"),
    dict(oi=0), dict(gap_filled=True), dict(received_at="2026-10-12T09:17:01+05:30")])
def test_runtime_rejects_late_stale_wrong_or_synthetic_quote(evidence, changes):
    quote(evidence[0], **changes)
    assert build(evidence, as_of="2026-10-12T09:20:10+05:30")["opening_0920_status"] != "VALID"


def test_runtime_requires_completed_bar_receipt_by_decision(evidence):
    quote(evidence[0])
    evidence[4]["fetch_timestamp"] = "2026-10-12T09:20:20+05:30"
    assert build(evidence, as_of="2026-10-12T09:20:10+05:30")["opening_0920_status"] != "VALID"


def test_runtime_cannot_relabel_previous_day_inputs_as_a_current_decision(evidence):
    quote(evidence[0])
    assert build(evidence, as_of="2026-10-13T09:20:10+05:30")["opening_0920_status"] != "VALID"


def test_duplicate_and_wrong_expiry_fail_closed(evidence):
    evidence[2]["candles"] *= 2
    evidence[1].write_text(json.dumps(evidence[2]))
    assert build(evidence)["opening_0920_status"] != "VALID"
    quote(evidence[0])
    evidence[4]["expiry"] = "2026-11-24"
    assert build(evidence, as_of="2026-10-12T09:20:10+05:30")["opening_0920_status"] != "VALID"


def test_contract_preserves_0920_long_and_every_v2_rule():
    from pathlib import Path
    old = json.loads((Path(__file__).resolve().parents[1] / "configs/strategies/v13_v10_g_3_production_v2.json").read_text())
    for family in ("core_setups", "active_setups"):
        retained = [{**item, "source_version": old["strategy_version"]}
                    for item in contract.strategy_payload()[family] if item["signal_end"] != "09:20" and item["signal_end"] not in {"10:05", "10:10", "10:15", "10:25", "10:30"}
                    and item["confirmation_end"].replace(":", "") + "_" + item["side"] not in (contract.selected_setups.SETUP_IDS | contract.midday_setups.SETUP_IDS | contract.afternoon_setups.SETUP_IDS)]
        assert retained == old[family]
    assert contract.setup_for("09:20", "SHORT").target_pct == 1.5
    setup = contract.setup_for("09:20", "LONG")
    assert (setup.target_pct, setup.stop_pct, setup.max_entries, setup.body_ratio, setup.max_wick_ratio) == (3., 1.25, 1, .75, .35)
    assert contract.confirmation_volume_minimum("LONG", signal_end="09:20") == 1.8
    assert contract.SIGNAL_TO_CONFIRMATION["09:20"] == "09:21"
