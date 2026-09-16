from datetime import date, datetime

import pandas as pd

import fno_oi_common as common
import fno_v13_v5_derivative_data as data


def _master() -> pd.DataFrame:
    records = []
    token = 100
    for underlying, expiry in (("ABC", "2026-08-25"), ("ABC", "2026-09-29")):
        for kind in ("CE", "PE"):
            for strike in (90.0, 100.0, 110.0, 120.0):
                records.append(
                    {
                        "instrument_token": token,
                        "exchange_token": token + 1,
                        "tradingsymbol": f"{underlying}{expiry[2:4]}X{int(strike)}{kind}",
                        "name": underlying,
                        "last_price": 0.0,
                        "expiry": expiry,
                        "strike": strike,
                        "tick_size": 0.05,
                        "lot_size": 500,
                        "instrument_type": kind,
                        "segment": "NFO-OPT",
                        "exchange": "NFO",
                    }
                )
                token += 1
    records.append(
        {
            "instrument_token": 900,
            "exchange_token": 901,
            "tradingsymbol": "ABC26SEPFUT",
            "name": "ABC",
            "last_price": 0.0,
            "expiry": "2026-09-29",
            "strike": 0.0,
            "tick_size": 0.05,
            "lot_size": 500,
            "instrument_type": "FUT",
            "segment": "NFO-FUT",
            "exchange": "NFO",
        }
    )
    return data.normalize_nfo_master(records, master_date=date(2026, 9, 4))


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sid": 1,
                "day": "2026-09-01",
                "tradingsymbol": "ABC",
                "side": "LONG",
                "profile": "higher_frequency",
                "strategy_version": "V13V5",
                "contract_month": "26SEP",
                "_trade_id": "long",
                "_entry_ts": pd.Timestamp("2026-09-01 09:27", tz=common.IST),
                "_exit_ts": pd.Timestamp("2026-09-01 10:00", tz=common.IST),
                "_required_expiry": pd.Timestamp("2026-09-29"),
                "_equity_entry_price": 105.0,
                "futures_tradingsymbol": "ABC26SEPFUT",
                "futures_instrument_token": 900,
            },
            {
                "sid": 2,
                "day": "2026-09-01",
                "tradingsymbol": "ABC",
                "side": "SHORT",
                "profile": "higher_frequency",
                "strategy_version": "V13V5",
                "contract_month": "26SEP",
                "_trade_id": "short",
                "_entry_ts": pd.Timestamp("2026-09-01 09:32", tz=common.IST),
                "_exit_ts": pd.Timestamp("2026-09-01 10:10", tz=common.IST),
                "_required_expiry": pd.Timestamp("2026-09-29"),
                "_equity_entry_price": 112.0,
                "futures_tradingsymbol": "ABC26SEPFUT",
                "futures_instrument_token": 900,
            },
        ]
    )


def test_contract_map_uses_direction_same_expiry_and_deterministic_atm() -> None:
    mapped = data.build_option_contract_map(
        _trades(), _master(), master_date=date(2026, 9, 4)
    ).set_index("trade_id")
    assert mapped.loc["long", "required_option_type"] == "CE"
    assert mapped.loc["long", "option_strike"] == 100.0  # 105 tie -> lower strike
    assert mapped.loc["long", "quantity"] == 500
    assert mapped.loc["short", "required_option_type"] == "PE"
    assert mapped.loc["short", "option_strike"] == 110.0
    assert set(mapped["mapping_status"]) == {"MAPPED_ATM"}


def test_expired_option_is_not_silently_remapped_to_live_expiry() -> None:
    trade = _trades().iloc[[0]].copy()
    trade["_required_expiry"] = pd.Timestamp("2026-07-28")
    mapped = data.build_option_contract_map(
        trade, _master(), master_date=date(2026, 9, 4)
    )
    assert mapped.loc[0, "mapping_status"] == "EXPIRED_OPTION_NOT_IN_CURRENT_MASTER"
    assert mapped.loc[0, "option_tradingsymbol"] == ""


def test_strike_ladder_includes_two_neighbors_each_side() -> None:
    mapped = data.build_option_contract_map(
        _trades().iloc[[0]], _master(), master_date=date(2026, 9, 4)
    )
    plan = data.build_option_fetch_plan(mapped, _master(), strike_window=2)
    assert plan["strike"].tolist() == [90.0, 100.0, 110.0, 120.0]
    assert plan["primary_trade_count"].sum() == 1


def test_minute_normalization_preserves_candle_start_and_metadata() -> None:
    contract = {
        "tradingsymbol": "ABC26SEP100CE",
        "instrument_token": 123,
        "underlying": "ABC",
        "expiry": pd.Timestamp("2026-09-29"),
        "strike": 100.0,
        "instrument_type": "CE",
        "lot_size": 500,
        "tick_size": 0.05,
    }
    rows = [
        {
            "date": pd.Timestamp("2026-09-01 09:28", tz=common.IST),
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": 1000,
            "oi": 5000,
        }
    ]
    frame = data.normalize_minute_candles(
        rows, contract, fetched_at=datetime(2026, 9, 4, 16, 0, tzinfo=common.IST)
    )
    assert frame.loc[0, "timestamp"] == pd.Timestamp(
        "2026-09-01 09:28", tz=common.IST
    )
    assert frame.loc[0, "lot_size"] == 500
    assert frame.loc[0, "dataset_version"] == data.DATASET_VERSION


def test_active_futures_plan_excludes_expired_contracts_not_in_live_master() -> None:
    trades = _trades()
    expired = trades.iloc[[0]].copy()
    expired["futures_tradingsymbol"] = "ABC26AUGFUT"
    combined = pd.concat([trades, expired], ignore_index=True)
    plan = data.build_active_futures_fetch_plan(combined, _master())
    assert plan["tradingsymbol"].tolist() == ["ABC26SEPFUT"]
    assert plan.loc[0, "linked_trade_count"] == 2


def test_traded_bar_selector_skips_zero_volume_and_reports_delay() -> None:
    frame = pd.DataFrame(
        {
            "_timestamp": pd.date_range(
                "2026-09-01 09:28", periods=3, freq="min", tz=common.IST
            ),
            "open": [10.0, 10.1, 10.2],
            "volume": [0, 0, 500],
        }
    )
    row, delay = data._first_traded_bar_at_or_after(
        frame, pd.Timestamp("2026-09-01 09:28", tz=common.IST)
    )
    assert row is not None
    assert row["open"] == 10.2
    assert delay == 2.0


def test_peak_option_capital_accounts_for_overlapping_one_lot_positions() -> None:
    coverage = pd.DataFrame(
        [
            {
                "coverage_state": "READY",
                "reference_entry_bar": pd.Timestamp("2026-09-01 09:30", tz=common.IST),
                "reference_exit_bar": pd.Timestamp("2026-09-01 10:00", tz=common.IST),
                "one_lot_premium_outlay_rupees": 10_000.0,
            },
            {
                "coverage_state": "READY",
                "reference_entry_bar": pd.Timestamp("2026-09-01 09:45", tz=common.IST),
                "reference_exit_bar": pd.Timestamp("2026-09-01 10:15", tz=common.IST),
                "one_lot_premium_outlay_rupees": 15_000.0,
            },
            {
                "coverage_state": "ENTRY_LIQUIDITY_DELAY_EXCEEDS_LIMIT",
                "reference_entry_bar": pd.Timestamp("2026-09-01 09:40", tz=common.IST),
                "reference_exit_bar": pd.Timestamp("2026-09-01 10:30", tz=common.IST),
                "one_lot_premium_outlay_rupees": 99_000.0,
            },
        ]
    )
    peak_cash, peak_positions, _ = data.peak_option_capital(coverage)
    assert peak_cash == 25_000.0
    assert peak_positions == 2
