from __future__ import annotations

import csv
import ctypes
import datetime as dt
import os
import re
import sys
import tempfile
import unittest
from ctypes import wintypes
from pathlib import Path
from unittest import mock

import pandas as pd

import fundamental_price_action_v1_papertrade as papertrade
import fundamental_price_action_v1_session as session
import log_dashboard_server as dashboard


def _open_reader_without_delete_share(path: Path) -> int:
    """Hold `path` open the way a plain reader does: no FILE_SHARE_DELETE.

    MoveFileEx (os.replace) onto such a target fails with WinError 5. This is
    the exact race that killed the 12:50 scan on 2026-08-07.
    """
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.restype = wintypes.HANDLE
    generic_read = 0x80000000
    share_read_write = 0x1 | 0x2
    open_existing = 3
    return kernel32.CreateFileW(
        str(path), generic_read, share_read_write, None, open_existing, 0, None
    )


def _close_handle(handle: int) -> None:
    ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(handle)


def _dashboard_source() -> str:
    return Path(dashboard.__file__).read_text(encoding="utf-8", errors="replace")


def _client_restartable_cards() -> set[str]:
    """The RESTARTABLE_CARDS Set literal the dashboard ships to the browser."""
    match = re.search(
        r"const RESTARTABLE_CARDS = new Set\(\[(.*?)\]\);", _dashboard_source(), re.DOTALL
    )
    if match is None:
        raise AssertionError("client-side RESTARTABLE_CARDS set not found")
    return set(re.findall(r'"([^"]+)"', match.group(1)))


def _client_log_order() -> list[str]:
    match = re.search(r"const LOG_ORDER = \[(.*?)\];", _dashboard_source(), re.DOTALL)
    if match is None:
        raise AssertionError("client-side LOG_ORDER not found")
    return re.findall(r'"([^"]+)"', match.group(1))


def _client_card_titles() -> dict[str, str]:
    match = re.search(r"const LOG_TITLES = \{(.*?)\n    \};", _dashboard_source(), re.DOTALL)
    if match is None:
        raise AssertionError("client-side LOG_TITLES not found")
    return dict(re.findall(r'"([^"]+)":\s*"([^"]*)"', match.group(1)))


def _client_group_ids(group_key: str) -> list[str]:
    match = re.search(
        r'key:\s*"' + re.escape(group_key) + r'".*?ids:\s*\[(.*?)\]',
        _dashboard_source(),
        re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"client-side group {group_key!r} not found")
    return re.findall(r'"([^"]+)"', match.group(1))


class FundamentalPriceActionV1SessionTests(unittest.TestCase):
    def test_marker_requires_full_verified_fetch(self) -> None:
        payload = {
            "complete": True,
            "tickers_expected": 1235,
            "tickers_written": 1235,
            "tickers_complete": 1235,
            "tickers_failed": 0,
            "verification_failed_count": 0,
            "unresolved_symbol_count": 0,
        }
        self.assertTrue(session.marker_is_fully_successful(payload)[0])
        payload["verification_failed_count"] = 1
        self.assertFalse(session.marker_is_fully_successful(payload)[0])

    def test_daily_output_keeps_long_and_short_together(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            slots = root / "sessions" / "2026-08-07" / "slots"
            slots.mkdir(parents=True)
            slot_csv = slots / "fundamental_price_action_v1_1200.csv"
            rows = [
                {"strategy": session.STRATEGY, "side": "LONG", "status": "READY", "symbol": "AAA"},
                {"strategy": session.STRATEGY, "side": "SHORT", "status": "NO_SETUP", "symbol": ""},
            ]
            with slot_csv.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=session.OUTPUT_FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

            latest_csv, latest_md, combined = session.refresh_daily_outputs(
                root,
                dt.date(2026, 8, 7),
                root / "sessions" / "2026-08-07",
                slots,
            )

            self.assertEqual([row["side"] for row in combined], ["LONG", "SHORT"])
            self.assertTrue(latest_csv.exists())
            self.assertIn("AAA", latest_md.read_text(encoding="utf-8"))

    def test_status_files_use_the_dashboard_key_value_format(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "x.heartbeat"
            self.assertTrue(
                session.write_runtime_kv(path, {"state": "RUNNING", "phase": "LOOP"})
            )
            parsed = dashboard.parse_status_file(path)
            self.assertEqual(parsed.get("state"), "RUNNING")
            self.assertEqual(parsed.get("phase"), "LOOP")

    def test_multiline_values_stay_on_one_key_value_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "x.status"
            session.write_runtime_kv(path, {"status": "FAILED", "error": "a\nb\tc"})
            parsed = dashboard.parse_status_file(path)
            self.assertEqual(parsed.get("status"), "FAILED")
            self.assertEqual(parsed.get("error"), "a b c")

    def test_heartbeat_write_retries_then_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "x.heartbeat"
            real_replace = os.replace
            calls = {"n": 0}

            def flaky_replace(src, dst, *a, **kw):
                calls["n"] += 1
                if calls["n"] < 3:
                    raise PermissionError(5, "Access is denied")
                return real_replace(src, dst, *a, **kw)

            with mock.patch.object(session.os, "replace", flaky_replace), \
                    mock.patch.object(session.time, "sleep", lambda _s: None):
                self.assertTrue(session.write_runtime_kv(path, {"state": "RUNNING"}))
            self.assertEqual(calls["n"], 3)
            self.assertEqual(dashboard.parse_status_file(path).get("state"), "RUNNING")

    @unittest.skipUnless(sys.platform == "win32", "Windows sharing semantics")
    def test_heartbeat_survives_a_reader_holding_the_file_open(self) -> None:
        """Regression: 2026-08-07 12:50 scan lost to WinError 5 on the heartbeat."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "x.heartbeat"
            path.write_text("state=OLD\n", encoding="utf-8")
            handle = _open_reader_without_delete_share(path)
            self.assertNotEqual(handle, -1, "could not acquire the test lock")
            try:
                with mock.patch.object(session.time, "sleep", lambda _s: None):
                    ok = session.write_runtime_kv(path, {"state": "RUNNING"})
            finally:
                _close_handle(handle)
            self.assertTrue(ok, "heartbeat write must fall back, not fail")
            self.assertEqual(dashboard.parse_status_file(path).get("state"), "RUNNING")

    @unittest.skipUnless(sys.platform == "win32", "Windows sharing semantics")
    def test_publish_heartbeat_never_raises_under_lock(self) -> None:
        """A locked heartbeat must not abort the trading scan."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "locked.heartbeat"
            path.write_text("state=OLD\n", encoding="utf-8")
            handle = _open_reader_without_delete_share(path)
            self.assertNotEqual(handle, -1, "could not acquire the test lock")
            try:
                # Deny the fallback too: the worst case is still not fatal.
                with mock.patch.object(session, "HEARTBEAT_PATH", path), \
                        mock.patch.object(session.time, "sleep", lambda _s: None), \
                        mock.patch.object(
                            session.Path, "write_text",
                            mock.Mock(side_effect=PermissionError(5, "Access is denied")),
                        ):
                    session.publish_heartbeat("RUNNING", phase="LOOP")
            finally:
                _close_handle(handle)

    def test_live_entry_row_skips_untradable_rows(self) -> None:
        slot = dt.datetime(2026, 8, 7, 12, 25, tzinfo=session.IST)
        self.assertIsNone(
            session.live_entry_row({"side": "SHORT", "status": "NO_SETUP"}, slot)
        )
        self.assertIsNone(
            session.live_entry_row(
                {"side": "LONG", "status": "READY", "symbol": "AAA", "entry_trigger": "0"},
                slot,
            )
        )
        row = session.live_entry_row(
            {
                "side": "LONG",
                "status": "READY",
                "symbol": "AAA",
                "entry_trigger": "100",
                "stop_loss": "99",
                "target_2r": "102",
            },
            slot,
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["ticker"], "AAA")
        self.assertEqual(row["signal_id"], "20260807_1225|LONG|AAA")
        # Quantity follows leveraged exposure (capital x leverage), not the
        # capital itself and not anything on the strategy row.
        self.assertEqual(int(row["quantity"]), int(session.EXPOSURE_PER_TRADE_RS // 100))
        self.assertEqual(float(row["capital_rs"]), session.CAPITAL_PER_TRADE_RS)
        self.assertAlmostEqual(
            float(row["exposure_rs"]),
            session.CAPITAL_PER_TRADE_RS * session.LEVERAGE,
            delta=100.0,
        )

    def test_intraday_leverage_is_five_times_capital(self) -> None:
        self.assertEqual(session.CAPITAL_PER_TRADE_RS, 10000.0)
        self.assertEqual(session.LEVERAGE, 5.0)
        self.assertEqual(session.EXPOSURE_PER_TRADE_RS, 50000.0)

    def test_return_on_capital_reflects_leverage(self) -> None:
        """A ~1% adverse price move on 5x is a ~5% hit to own capital."""
        slot = dt.datetime(2026, 8, 7, 12, 25, tzinfo=session.IST)
        entry_row = session.live_entry_row(
            {"side": "LONG", "status": "READY", "symbol": "AAA", "entry_trigger": "100"},
            slot,
        )
        assert entry_row is not None
        trade = papertrade.build_trade(
            entry_row,
            exit_reason="SL",
            exit_price=99.0,
            exit_time=slot,
            stop_price=99.0,
            target_price=101.0,
            stop_pct=0.01,
            target_pct=0.01,
            cfg=papertrade.CostConfig(),
        )
        self.assertAlmostEqual(float(trade["net_pnl_pct"]), -1.0, delta=0.3)
        roc = float(trade["return_on_capital_pct"])
        self.assertAlmostEqual(roc, -5.0, delta=1.5)
        # Return on capital must be the leveraged multiple of the price move.
        self.assertAlmostEqual(
            roc, float(trade["net_pnl_pct"]) * session.LEVERAGE, places=2
        )

    def test_paper_trade_levels_are_one_percent_both_sides(self) -> None:
        long_stop, long_target = papertrade.resolve_levels("LONG", 100.0, 0.01, 0.01)
        self.assertAlmostEqual(long_stop, 99.0)
        self.assertAlmostEqual(long_target, 101.0)
        short_stop, short_target = papertrade.resolve_levels("SHORT", 100.0, 0.01, 0.01)
        self.assertAlmostEqual(short_stop, 101.0)
        self.assertAlmostEqual(short_target, 99.0)

    def test_paper_trade_takes_the_stop_when_a_bar_spans_both(self) -> None:
        """Five-minute bars hide the path, so the adverse fill must win."""
        bars = pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-08-07 12:30", tz=session.IST)],
                "open": [100.0],
                "high": [101.5],
                "low": [98.5],
                "close": [100.0],
            }
        )
        resolved = papertrade.scan_for_exit("LONG", bars, 99.0, 101.0)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved[0], "SL")

    def test_new_fpa_cards_are_registered_everywhere(self) -> None:
        client_titles = _client_card_titles()
        client_order = _client_log_order()
        forensic_ids = _client_group_ids("forensic-positional")
        expected = {
            "live_signals_csv_fpa_v1_short": "Live Entries CSV FPA v1 Short",
            "live_signals_csv_fpa_v1_long": "Live Entries CSV FPA v1 Long",
            "live_papertrade_result_csv_fpa_v1": "V1 FPA Papertrade Results (Net)",
            "paper_trade_fpa_v1": "V1 FPA Papertrade Runner Log (Net)",
        }
        for card_id, title in expected.items():
            self.assertEqual(client_titles.get(card_id), title, card_id)
            self.assertIn(card_id, client_order, card_id)
            self.assertIn(card_id, forensic_ids, card_id)
            self.assertIn(card_id, dashboard.CARD_TASK_NAMES, card_id)
        # The runner log card is a real session, so it carries live state and
        # a Restart control like the other managed sessions.
        self.assertIn("paper_trade_fpa_v1", dashboard.LOG_IDS)
        self.assertIn("paper_trade_fpa_v1", dashboard.STATUS_FILES)
        self.assertIn("paper_trade_fpa_v1", dashboard.HEARTBEAT_FILES)
        self.assertIn("paper_trade_fpa_v1", dashboard.RESTARTABLE_CARDS)
        self.assertIn("paper_trade_fpa_v1", _client_restartable_cards())

    def test_dashboard_registration_is_complete(self) -> None:
        for card_id in ("fundamental_price_action_v1", "collect_filtered_stock_data"):
            self.assertIn(card_id, dashboard.LOG_FILES)
            self.assertIn(card_id, dashboard.STATUS_FILES)
            self.assertIn(card_id, dashboard.HEARTBEAT_FILES)
            self.assertIn(card_id, dashboard.CARD_TASK_NAMES)
            self.assertIn(card_id, dashboard.RESTARTABLE_CARDS)

    def test_restart_button_renders_for_the_session(self) -> None:
        """The browser gates the button on its own copy of the card list.

        Server-side registration alone renders no button, which is why this
        session had no Restart control despite being restartable.
        """
        card_id = "fundamental_price_action_v1"
        self.assertIn(card_id, dashboard.RESTARTABLE_CARDS)
        self.assertIn(card_id, _client_restartable_cards())
        # The restart path needs both a task to launch and a BAT to identify.
        self.assertTrue(dashboard.CARD_TASK_NAMES[card_id])
        self.assertTrue(dashboard.RESTARTABLE_CARDS[card_id])


if __name__ == "__main__":
    unittest.main()
