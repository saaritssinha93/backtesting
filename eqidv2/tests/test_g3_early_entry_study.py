"""Causal order allocation checks for the isolated early-entry experiment."""

import pandas as pd
import pytest

from research.g3_early_entry_study_20261023 import allocate


def stamp(clock):
    return pd.Timestamp(f"2026-10-23 {clock}", tz="Asia/Kolkata")


def candidate(symbol, confirmation="09:21", *, entry="09:22", exit="10:00",
              move=1., profit=1.45, filled=True, open_time=None, side="LONG"):
    record = dict(tradingsymbol=symbol, confirmation_ts=stamp(confirmation),
                  entry_ts=stamp(entry) if filled else pd.NaT,
                  exit_ts=stamp(exit) if filled else pd.NaT,
                  side=side, price_change_pct=move, traded_value=1_000_000.,
                  filled=filled, net_return_pct=profit)
    if open_time is not None:
        record["entry_open_ts"] = stamp(open_time)
    return record


def test_slot_quota_uses_observed_rank_without_looking_at_later_fill_or_profit():
    """The known future winner must not replace a higher-ranked unfilled order."""
    inputs = [candidate("TOP", move=2., filled=False),
              candidate("LOWER", move=1., profit=100.)]
    ledger = allocate(pd.DataFrame(inputs), quota=1)
    assert ledger.tradingsymbol.tolist() == ["TOP"]
    assert ledger.portfolio_status.tolist() == ["UNFILLED_EXPIRED"]
    assert ledger.portfolio_net_profit_rupees.sum() == 0
    inputs[1]["net_return_pct"] = -100.
    pd.testing.assert_frame_equal(ledger, allocate(pd.DataFrame(inputs), quota=1))


def test_pending_order_blocks_intermediate_signal_and_can_retry_at_expiry():
    inputs = [candidate("RETRY", filled=False),
              candidate("RETRY", "09:26", entry="09:27", profit=50.),
              candidate("RETRY", "09:31", entry="09:32", profit=.5)]
    ledger = allocate(pd.DataFrame(inputs), quota=1)
    assert ledger.confirmation_ts.tolist() == [stamp("09:21"), stamp("09:31")]
    assert ledger.portfolio_status.tolist() == ["UNFILLED_EXPIRED", "EXECUTED"]
    assert ledger.portfolio_net_profit_rupees.sum() == pytest.approx(2_500.)


def test_pending_future_fill_blocks_duplicate_before_fill_is_known():
    inputs = [candidate("LATE", entry="09:30", exit="09:35"),
              candidate("LATE", "09:26", entry="09:27", exit="09:29")]
    ledger = allocate(pd.DataFrame(inputs), quota=1)
    assert len(ledger) == 1
    assert ledger.iloc[0].entry_ts == stamp("09:30")
    assert ledger.iloc[0].portfolio_executed


def test_executed_symbol_cannot_reenter_even_after_exit_or_opposite_side():
    inputs = [candidate("ONCE", exit="09:23"),
              candidate("ONCE", "09:26", entry="09:27", side="SHORT", move=-2.)]
    ledger = allocate(pd.DataFrame(inputs), quota=0)
    assert ledger.tradingsymbol.tolist() == ["ONCE"]
    assert ledger.portfolio_executed.sum() == 1


def test_capital_rejects_eleventh_fill_but_allows_retry_after_release():
    inputs = [candidate(f"STOCK{i:02d}", exit="09:25", move=2-i/100)
              for i in range(11)]
    inputs.append(candidate("STOCK10", "09:26", entry="09:27", exit="09:30"))
    ledger = allocate(pd.DataFrame(inputs), quota=0)
    assert ledger.portfolio_executed.sum() == 11
    assert ledger.portfolio_status.eq("CAPITAL_REJECTED").sum() == 1
    retry = ledger.loc[ledger.tradingsymbol.eq("STOCK10")]
    assert retry.portfolio_status.tolist() == ["CAPITAL_REJECTED", "EXECUTED"]


def test_next_open_uses_economic_clock_before_later_minute_exit_frees_capital():
    """At09:21 all capital is occupied even though the09:22 label would free it."""
    occupied = [candidate(f"HELD{i:02d}", "09:16", entry="09:17", exit="09:22")
                for i in range(10)]
    fresh = candidate("MARKET", "09:21", entry="09:22", exit="09:30", open_time="09:21")
    ledger = allocate(pd.DataFrame(occupied + [fresh]), quota=0)
    fresh_order = ledger.loc[ledger.tradingsymbol.eq("MARKET")].iloc[0]
    assert fresh_order.portfolio_status == "CAPITAL_REJECTED"
    assert ledger.portfolio_executed.sum() == 10
    # A breakout genuinely filled at09:22 can use capital released at09:22.
    fresh.pop("entry_open_ts")
    breakout = allocate(pd.DataFrame(occupied + [fresh]), quota=0)
    assert breakout.loc[breakout.tradingsymbol.eq("MARKET"), "portfolio_executed"].item()


def test_quotas_are_per_side_and_use_liquidity_for_equal_moves():
    rows = [candidate("LONG_LOW", move=1.), candidate("LONG_HIGH", move=1.),
            candidate("SHORT", side="SHORT", move=-.5)]
    rows[1]["traded_value"] = 2_000_000.
    ledger = allocate(pd.DataFrame(rows), quota=1)
    assert set(ledger.tradingsymbol) == {"LONG_HIGH", "SHORT"}
    assert ledger.portfolio_executed.sum() == 2


def test_unlimited_capacity_admits_more_than_ten_but_keeps_one_executed_symbol():
    rows = [candidate(f"STOCK{i:02d}", exit="10:00") for i in range(12)]
    rows.append(candidate("STOCK00", "10:01", entry="10:02", exit="10:03",
                          side="SHORT", move=-2.))
    ledger = allocate(pd.DataFrame(rows), quota=0, max_positions=None)
    assert ledger.portfolio_executed.sum() == 12
    assert ledger.tradingsymbol.nunique() == 12
    assert not ledger.portfolio_status.eq("CAPITAL_REJECTED").any()
    assert ledger.tradingsymbol.eq("STOCK00").sum() == 1


def test_unlimited_capacity_does_not_disable_slot_quotas():
    rows = [candidate("LONG_TOP", move=2.), candidate("LONG_OTHER", move=1.),
            candidate("SHORT_TOP", side="SHORT", move=-2.),
            candidate("SHORT_OTHER", side="SHORT", move=-1.)]
    ledger = allocate(pd.DataFrame(rows), quota=1, max_positions=None)
    assert set(ledger.tradingsymbol) == {"LONG_TOP", "SHORT_TOP"}
    assert ledger.portfolio_executed.sum() == 2


@pytest.mark.parametrize("limit", [0, -1, 1.5, "10", True])
def test_invalid_position_limit_is_rejected(limit):
    with pytest.raises((TypeError, ValueError)):
        allocate(pd.DataFrame([candidate("EXAMPLE")]), quota=0, max_positions=limit)
