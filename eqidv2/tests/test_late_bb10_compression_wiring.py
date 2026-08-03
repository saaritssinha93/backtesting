from __future__ import annotations

import pandas as pd

import eqidv2_final_conf_live_bootstrap as bootstrap
import eqidv2_late_bb10_compression as setup
import final_setup_conf
import final_setup_conf_v11_working
import v11_exit_policy_resolver


def _bars(rows: list[tuple[str, float, float, float, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        rows, columns=["date", "open", "high", "low", "close"]
    )
    frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize("Asia/Kolkata")
    return frame.set_index("date")


def test_setup_is_active_with_frozen_entry_and_exit_configuration() -> None:
    cfg = final_setup_conf.FINAL_SETUP_CONF[setup.SETUP]
    assert setup.SETUP in final_setup_conf_v11_working.FINAL_SETUP_CONF
    assert cfg["side"] == "LONG"
    assert cfg["exit"] == {"sl_pct": 0.70, "tgt_pct": 0.75}
    assert bootstrap.entry_policy_for_setup(setup.SETUP) == {
        "model": "high_break_trigger",
        "tick_size": 0.05,
        "valid_minutes": 3,
        "max_gap_pct": 0.20,
        "same_bar_cancel_first": True,
    }
    assert bootstrap.exit_policy_for_setup(setup.SETUP)["forced_exit_time"] == "15:15"


def test_breakout_entry_triggers_in_next_three_minutes() -> None:
    bars = _bars(
        [
            ("2026-07-29 14:01", 100.00, 100.04, 99.95, 100.02),
            ("2026-07-29 14:02", 100.02, 100.11, 99.96, 100.08),
            ("2026-07-29 14:03", 100.08, 100.20, 100.00, 100.15),
        ]
    )
    resolved = setup.resolve_entry_1m(
        bars,
        "2026-07-29 14:00+05:30",
        trigger=100.10,
        cancel=99.90,
    )
    assert resolved is not None
    assert resolved[0] == bars.index[1]
    assert resolved[1] == 100.10


def test_breakout_entry_is_cancel_first_and_rejects_large_gap() -> None:
    ambiguous = _bars(
        [("2026-07-29 14:01", 100.00, 100.20, 99.89, 100.15)]
    )
    assert (
        setup.resolve_entry_1m(
            ambiguous,
            "2026-07-29 14:00+05:30",
            trigger=100.10,
            cancel=99.90,
        )
        is None
    )
    gap = _bars(
        [("2026-07-29 14:01", 100.31, 100.35, 100.00, 100.32)]
    )
    assert (
        setup.resolve_entry_1m(
            gap,
            "2026-07-29 14:00+05:30",
            trigger=100.10,
            cancel=99.90,
            max_gap_pct=0.20,
        )
        is None
    )


def test_exit_policy_honours_worse_open_stop_and_1515_cap() -> None:
    gap_bars = _bars(
        [("2026-07-29 14:02", 99.00, 99.20, 98.90, 99.10)]
    )
    stopped = v11_exit_policy_resolver.resolve(
        gap_bars,
        "LONG",
        100.0,
        "2026-07-29 14:02+05:30",
        0.70,
        0.75,
        {"forced_exit_time": "15:15", "stop_gap_mode": "worse_open"},
    )
    assert stopped is not None
    assert stopped.outcome == "SL"
    assert stopped.exit_price == 99.00

    timed_bars = _bars(
        [
            ("2026-07-29 15:14", 100.0, 100.2, 99.9, 100.1),
            ("2026-07-29 15:15", 100.1, 100.2, 100.0, 100.15),
            ("2026-07-29 15:16", 100.2, 101.0, 100.1, 100.9),
        ]
    )
    timed = v11_exit_policy_resolver.resolve(
        timed_bars,
        "LONG",
        100.0,
        "2026-07-29 15:14+05:30",
        0.70,
        2.0,
        {"max_hold_minutes": 60, "forced_exit_time": "15:15"},
    )
    assert timed is not None
    assert timed.outcome == "TIME"
    assert timed.exit_time_ist == timed_bars.index[1]


def test_live_conf_gate_requires_breadth_and_nifty_alignment() -> None:
    frame = pd.DataFrame(
        {
            "setup": [setup.SETUP, setup.SETUP, setup.SETUP],
            "signal_time_ist": [
                "2026-07-29 14:00:00+05:30",
                "2026-07-29 14:00:00+05:30",
                "2026-07-29 14:00:00+05:30",
            ],
            "market_breadth": [0.45, 0.44, 0.60],
            "nifty_ema_up": [1.0, 1.0, 0.0],
        }
    )
    assert bootstrap.conf_mask(frame).tolist() == [True, False, False]
