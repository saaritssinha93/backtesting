"""Local monitoring cannot manufacture execution, quotes, or trading authority."""
from datetime import datetime, timedelta
import json
from pathlib import Path

import pandas as pd
import pytest

import g3_trade_monitor as monitor
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_live_execution_policy as sizing
from fno_v13_v10_g_identity import canonical_signal_id, _safe_contract_stem

DAY = "2026-10-14"
NOW = datetime.fromisoformat(DAY + "T10:10:15+05:30")


def order(*, symbol="EXAMPLE", mode="PAPER", **overrides):
    setup = contract.setup_for("10:05", "LONG")
    row = dict(strategy_version=contract.STRATEGY_VERSION, strategy_fingerprint=contract.strategy_fingerprint(),
        mode=mode, session_date=DAY, tradingsymbol=symbol, side="LONG", status="OPEN",
        signal_end=setup.signal_end, confirmation_end=setup.confirmation_end, setup_id=setup.setup_id,
        signal_id=canonical_signal_id(contract.STRATEGY_VERSION, NOW.date(), setup.signal_end, setup.confirmation_end, "LONG", symbol),
        updated_at_ist=NOW.isoformat(), entry_at_ist=DAY + "T10:06:10+05:30", entry_price=100.,
        trigger_price=100., quantity=5000, lot_size=1, capital_rs=100000., leverage=5., target_exposure_rs=500000.,
        target_pct=setup.target_pct, stop_pct=setup.stop_pct, stop_price=98.75, target_price=100.56,
        last_price=110., estimated_cost_rs=250., net_pnl_rs=999.)
    if mode == "LIVE":
        row.update(sizing.execution_metadata())
        row.update(capital_rs=10000., quantity=500, sizing_reference_price=100., strategy_sized_quantity=500,
            requested_quantity=500, execution_quantity_override=None)
    row.update(overrides)
    return row


def write_order(root, row, *, namespace="", filename="order.json"):
    directory = root / "live/orders" / row["mode"]
    if namespace:
        directory /= namespace
    directory /= DAY
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_text(json.dumps(row), encoding="utf-8")
    return path


def build(root, **kwargs):
    return monitor.build_snapshot(DAY, now=kwargs.pop("now", NOW), live_root=root / "live",
        minute_root=root / "raw", quote_root=root / "quotes", observer_root=root / "observer",
        minute_cache_root=root / "minutes", **kwargs)


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def quote(symbol="EXAMPLE", *, age=5, **changes):
    row = dict(symbol=symbol, price=99.9, exchange_timestamp=(NOW-timedelta(seconds=age)).isoformat(),
        received_at=NOW.isoformat(), last_trade_time=(NOW-timedelta(seconds=age)).isoformat(), source="KITE_FULL_QUOTE")
    row.update(changes)
    return row


def save_quotes(root, rows):
    save_json(root / "quotes/latest.json", dict(schema_version="g3_trade_monitor_quotes_v1", session_date=DAY, quotes=rows))


def minute_rows(symbol="EXAMPLE", *, end="10:10", count=4):
    stamps = pd.date_range(DAY + "T" + end + ":00+05:30", periods=count, freq="-1min")[::-1]
    return [dict(timestamp=stamp.isoformat(), open=100., high=100.1, low=99.8, close=99.9, volume=100., tradingsymbol=symbol)
        for stamp in stamps]


def save_minutes(root, symbol="EXAMPLE", rows=None, *, private=False):
    rows = rows if rows is not None else minute_rows(symbol)
    if private:
        save_json(root / "minutes" / DAY / (_safe_contract_stem(symbol) + ".json"),
            dict(schema_version="g3_trade_monitor_minutes_v1", symbol=symbol, session_date=DAY, bars=rows, received_at=NOW.isoformat()))
    else:
        path = root / "raw" / DAY / (_safe_contract_stem(symbol) + "_1minute.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)


def test_no_executed_trades_explicit_each_mode(tmp_path):
    result = build(tmp_path)
    assert result["state"] == "NO_EXECUTED_TRADES"
    assert result["summary"] == dict(executed=0, open=0, closed=0, pending=0, invalid=0, conflicts=0)
    assert all(row["no_trades_reason"] for row in result["modes"].values())
    assert result["execution_authority"] is False


def test_migration_session_monitor_preserves_verified_v9_without_accepting_new_setups(tmp_path, monkeypatch):
    import sys
    test_module = sys.modules[__name__]
    monkeypatch.setattr(test_module, "DAY", "2026-10-15")
    monkeypatch.setattr(test_module, "NOW", datetime.fromisoformat("2026-10-15T10:20:15+05:30"))
    identity = monitor.display_identity(DAY, "V13_V10_G_3")
    assert identity.previous_session
    row = order(strategy_version=identity.version, strategy_fingerprint=identity.fingerprint)
    row["signal_id"] = canonical_signal_id(identity.version, NOW.date(), "10:05", "10:06", "LONG", "EXAMPLE")
    path = write_order(tmp_path, row)
    original = path.read_bytes()
    result = build(tmp_path)
    assert result["summary"]["executed"] == 1
    assert result["identity"]["scope"] == "PREVIOUS_SESSION_READ_ONLY"
    monitor.write_snapshot(result, tmp_path / "output")
    latest = monitor.load_latest(tmp_path / "output", day=DAY, now=NOW)
    assert latest["strategy_fingerprint"] == identity.fingerprint
    assert latest["summary"]["executed"] == 1 and path.read_bytes() == original
    row.update(signal_end="10:10", confirmation_end="10:11", setup_id="1011_LONG", target_pct=1.5)
    row["signal_id"] = canonical_signal_id(identity.version, NOW.date(), "10:10", "10:11", "LONG", "EXAMPLE")
    write_order(tmp_path, row)
    rejected = build(tmp_path)
    assert rejected["summary"]["invalid"] == 1 and rejected["trades"] == []
    assert not monitor.display_identity("2026-10-16", "V13_V10_G_3").previous_session


def test_pending_is_not_an_entry_and_old_last_price_never_freshened(tmp_path):
    write_order(tmp_path, order(status="PENDING_ENTRY", entry_price=None, entry_at_ist=None))
    result = build(tmp_path)
    assert result["summary"]["pending"] == 1 and result["trades"] == []
    write_order(tmp_path, order())
    row = build(tmp_path)["trades"][0]
    assert row["mark_price"] is None and row["mark_timestamp"] is None
    assert row["direction"] == "NO_DATA" and row["advisory_flags"] == []


@pytest.mark.parametrize("change", [dict(quantity=2), dict(signal_id="made-up"), dict(strategy_fingerprint="stale"),
    dict(entry_price=-1), dict(entry_at_ist=DAY + "T09:00:00+05:30"), dict(target_pct=2),
    dict(side="<script>"), dict(tradingsymbol="../outside"), dict(updated_at_ist="2026-10-14T10:10:15"),
    dict(entry_at_ist=DAY+"T14:00:00+05:30")])
def test_invalid_execution_omitted_and_disclosed(tmp_path, change):
    write_order(tmp_path, order(**change))
    result = build(tmp_path)
    assert result["trades"] == [] and result["summary"]["invalid"] == 1
    assert result["state"] == "PARTIAL"


def test_paper_live_namespaces_and_partial_fill_disclosure(tmp_path):
    write_order(tmp_path, order())
    write_order(tmp_path, order(mode="LIVE", quantity=250), namespace="live_kite_qty1")
    save_quotes(tmp_path, [quote()])
    result = build(tmp_path)
    assert result["summary"]["executed"] == 2
    live = next(row for row in result["trades"] if row["mode"] == "LIVE")
    assert live["partial_fill"] and live["quantity"] == 250 and live["advisory_flags"] == []
    assert result["modes"]["PAPER"]["executed"] == result["modes"]["LIVE"]["executed"] == 1


def test_live_sizing_mismatch_rejected(tmp_path):
    write_order(tmp_path, order(mode="LIVE", quantity=501), namespace="live_kite_qty1")
    assert build(tmp_path)["summary"]["invalid"] == 1


def test_duplicate_live_conflict_visible_without_action_flags(tmp_path):
    write_order(tmp_path, order(mode="LIVE"))
    write_order(tmp_path, order(mode="LIVE", last_price=99), namespace="live_kite_qty1")
    save_quotes(tmp_path, [quote()])
    result = build(tmp_path)
    assert result["summary"]["executed"] == 1 and result["summary"]["conflicts"] == 1
    assert result["trades"][0]["direction"] == "NO_DATA"
    assert result["trades"][0]["advisory_flags"] == []


def test_real_quote_timestamp_wins_over_completed_minute_and_keeps_ampersand(tmp_path):
    write_order(tmp_path, order(symbol="M&M"))
    save_quotes(tmp_path, [quote("M&M")])
    save_minutes(tmp_path, "M&M")
    row = build(tmp_path)["trades"][0]
    assert row["symbol"] == "M&M" and row["mark_granularity"] == "QUOTE"
    assert row["mark_age_seconds"] == 5 and row["mark_state"] == "FRESH"
    assert row["mark_source"] == "TIMESTAMPED_CASH_QUOTE"


@pytest.mark.parametrize("changes", [dict(last_trade_time=(NOW+timedelta(seconds=1)).isoformat()),
    dict(exchange_timestamp=(NOW+timedelta(seconds=1)).isoformat()), dict(last_trade_time=None),
    dict(received_at=(NOW+timedelta(seconds=1)).isoformat())])
def test_quote_future_or_missing_real_timestamp_rejected(tmp_path, changes):
    write_order(tmp_path, order())
    save_quotes(tmp_path, [quote(**changes)])
    assert build(tmp_path)["trades"][0]["mark_state"] == "NO_DATA"


def test_completed_minute_fallback_explicit_granularity(tmp_path):
    write_order(tmp_path, order())
    save_minutes(tmp_path)
    row = build(tmp_path)["trades"][0]
    assert row["mark_source"] == "COMPLETED_1M_CLOSE" and row["mark_granularity"] == "1m"
    assert row["mark_age_seconds"] == 15 and row["completed_bars"] == 4


def test_private_completed_cache_wins_when_newer_and_future_or_synthetic_omitted(tmp_path):
    write_order(tmp_path, order())
    save_minutes(tmp_path, rows=minute_rows(end="10:07", count=1))
    rows = minute_rows()
    rows += [{**rows[-1], "timestamp": DAY+"T10:11:00+05:30"},
             {**rows[-1], "timestamp": DAY+"T10:06:00+05:30", "gap_filled": True}]
    save_minutes(tmp_path, rows=rows, private=True)
    result = build(tmp_path)
    assert result["trades"][0]["completed_bars"] == 4
    assert result["trades"][0]["mark_timestamp"] == DAY+"T10:10:00+05:30"
    assert any("omitted" in warning for warning in result["warnings"])


def test_conflicting_bars_rejected(tmp_path):
    write_order(tmp_path, order())
    rows = minute_rows()
    rows.append({**rows[-1], "close": 100.})
    save_minutes(tmp_path, rows=rows, private=True)
    result = build(tmp_path)
    assert result["trades"][0]["completed_bars"] == 0
    assert result["warnings"]


def test_historical_uses_real_now_and_never_current_flags(tmp_path):
    write_order(tmp_path, order(last_price_at_ist=NOW.isoformat()))
    result = build(tmp_path, now=NOW+timedelta(days=1))
    assert result["state"] == "HISTORICAL"
    assert result["generated_at_ist"].startswith("2026-10-15")
    assert result["trades"][0]["advisory_flags"] == []
    assert result["trades"][0]["mark_state"] == "HISTORICAL"


def test_closed_trade_keeps_real_exit_and_no_unrealized_pnl(tmp_path):
    write_order(tmp_path, order(status="CLOSED", exit_price=100.56, exit_at_ist=DAY+"T10:09:00+05:30"))
    save_quotes(tmp_path, [quote(price=200)])
    row = build(tmp_path)["trades"][0]
    assert row["mark_price"] == 100.56 and row["mark_source"] == "RECORDED_EXIT"
    assert row["advisory_flags"] == [] and row["metrics"]["gross_unrealized_pnl_rs"] is None


def test_snapshot_immutable_idempotent_atomic_latest_and_staleness(tmp_path):
    output = tmp_path / "output"
    snapshot = build(tmp_path)
    first = monitor.write_snapshot(snapshot, output)
    second = monitor.write_snapshot(snapshot, output)
    assert first == second and len(list((output/"snapshots"/DAY).glob("*.json"))) == 1
    assert len((output/"logs"/(DAY+".jsonl")).read_text().splitlines()) == 1
    assert monitor.load_latest(output, day=DAY, now=NOW)["snapshot_stale"] is False
    assert monitor.load_latest(output, day=DAY, now=NOW+timedelta(seconds=46))["snapshot_stale"] is True
    assert monitor.load_latest(output, day="2026-10-15", now=NOW+timedelta(days=1))["state"] == "NO_SNAPSHOT"


def test_atomic_publish_retries_a_transient_windows_destination_lock(tmp_path, monkeypatch):
    target = tmp_path / "latest" / "latest.json"
    real_replace = monitor.os.replace
    calls, delays = [], []

    def transient_lock(source, destination):
        calls.append((source, destination))
        if len(calls) < 3:
            raise PermissionError(5, "destination is temporarily locked")
        return real_replace(source, destination)

    monkeypatch.setattr(monitor.os, "replace", transient_lock)
    monkeypatch.setattr(monitor.clock_time, "sleep", delays.append)
    monitor._atomic(target, '{"state":"READY"}\n')

    assert target.read_text(encoding="utf-8") == '{"state":"READY"}\n'
    assert len(calls) == 3
    assert delays == [0.05, 0.10]


def test_markdown_escapes_untrusted_text_without_changing_json_identity(tmp_path):
    snapshot = build(tmp_path)
    snapshot["warnings"] = ["<script>alert(1)</script>"]
    assert "<script>" not in monitor.render_markdown(snapshot)
    assert "&lt;script&gt;" in monitor.render_markdown(snapshot)


def test_worker_lock_rejects_concurrent_owner_and_recovers_after_release(tmp_path):
    with monitor.worker_lock(tmp_path):
        with pytest.raises(monitor.AlreadyRunning):
            with monitor.worker_lock(tmp_path):
                pass
    with monitor.worker_lock(tmp_path):
        pass


def test_order_tree_is_unchanged_by_snapshot_and_publication(tmp_path):
    path = write_order(tmp_path, order())
    before = path.read_bytes()
    snapshot = build(tmp_path)
    monitor.write_snapshot(snapshot, tmp_path/"output")
    assert before == path.read_bytes()
    assert set((tmp_path/"live/orders/PAPER"/DAY).glob("*.json")) == {path}


def test_worker_once_after_end_is_day_bounded(tmp_path, monkeypatch):
    from tools import g3_trade_monitor as worker
    class Clock:
        @staticmethod
        def now(tz):
            return NOW
    real_build = monitor.build_snapshot
    monkeypatch.setattr(worker, "datetime", Clock)
    monkeypatch.setattr(worker.monitor, "build_snapshot", lambda day, **kw: real_build(DAY, now=kw["now"],
        live_root=tmp_path/"live", minute_root=tmp_path/"raw", quote_root=tmp_path/"quotes", observer_root=tmp_path/"observer"))
    monkeypatch.setattr(worker.time, "sleep", lambda *args: pytest.fail("Worker should stop after end time"))
    assert worker.main(["--output-root", str(tmp_path/"output"), "--end-time", "10:00"]) == 0
    snapshot = monitor.load_latest(tmp_path/"output", day=DAY, now=NOW)
    assert snapshot["rules_version"] == monitor.RULES_VERSION
    assert snapshot["monitor_config"] == monitor.MONITOR_CONFIG


def test_duplicate_worker_start_does_not_read_or_write_snapshot(tmp_path, monkeypatch):
    from tools import g3_trade_monitor as worker
    monkeypatch.setattr(worker.monitor, "build_snapshot", lambda *a, **kw: pytest.fail("Duplicate worker must not run"))
    with monitor.worker_lock(tmp_path):
        assert worker.main(["--once", "--output-root", str(tmp_path)]) == 0
    assert not (tmp_path/"latest/latest.json").exists()


def test_historical_worker_never_fetches_market_data_or_loops(tmp_path, monkeypatch):
    from tools import g3_trade_monitor as worker
    import g3_trade_monitor_market_data as feed_module
    class Clock:
        @staticmethod
        def now(tz):
            return NOW + timedelta(days=1)
    class Feed:
        def __init__(self, *args, **kwargs):
            pass
        def refresh(self, *args, **kwargs):
            pytest.fail("Historical monitoring must not fetch current market data")
    real_build = monitor.build_snapshot
    monkeypatch.setattr(worker, "datetime", Clock)
    monkeypatch.setattr(feed_module, "MarketDataFeed", Feed)
    monkeypatch.setattr(worker.monitor, "build_snapshot", lambda day, **kw: real_build(day, now=kw["now"],
        live_root=tmp_path/"live", minute_root=tmp_path/"raw", quote_root=tmp_path/"quotes", observer_root=tmp_path/"observer"))
    monkeypatch.setattr(worker.time, "sleep", lambda *args: pytest.fail("Historical worker must not loop"))
    assert worker.main(["--market-data", "--date", DAY, "--output-root", str(tmp_path/"output")]) == 0
    result = monitor.load_latest(tmp_path/"output", day=DAY, now=NOW+timedelta(days=1))
    assert result["state"] == "HISTORICAL"
    assert result["generated_at_ist"].startswith("2026-10-15")


def test_worker_interval_rejects_non_15_second_polling():
    from tools import g3_trade_monitor as worker
    with pytest.raises(SystemExit):
        worker.main(["--interval", "0"])


def test_stale_snapshot_clears_cached_flags_in_all_views(tmp_path):
    write_order(tmp_path, order())
    snapshot = build(tmp_path)
    row = snapshot["trades"][0]
    row["advisory_flags"] = ["TREND_EXIT_REVIEW"]
    row["assessment"]["advisory_flags"] = ["TREND_EXIT_REVIEW"]
    monitor.write_snapshot(snapshot, tmp_path/"output")
    current = monitor.load_latest(tmp_path/"output", day=DAY, now=NOW+timedelta(seconds=46))
    assert current["trades"][0]["advisory_flags"] == []
    assert current["trades"][0]["assessment"]["advisory_flags"] == []
    assert current["trades"][0]["direction"] == "STALE"


def test_malformed_quote_identity_cannot_interrupt_snapshot(tmp_path):
    write_order(tmp_path, order())
    save_quotes(tmp_path, [quote(symbol=["EXAMPLE"]), quote()])
    result = build(tmp_path)
    assert result["trades"][0]["mark_state"] == "FRESH"


def assessed_position(direction="FAVORABLE", *, mode="PAPER", **changes):
    row = dict(mode=mode, symbol="EXAMPLE", side="LONG", status="OPEN", direction=direction, mark_state="FRESH",
        freshness={"price_state": "FRESH"}, metrics={"gross_unrealized_pnl_rs": 125.},
        advisory_flags=[], conflict=False, partial_fill=False, partial_exit=False,
        entry_submission_uncertain=False)
    row.update(changes)
    return row


def summarize_positions(rows, *, invalid=0):
    snapshot = dict(trades=rows, modes={"PAPER": {"invalid": invalid}, "LIVE": {"invalid": 0}}, historical=False)
    monitor._summarize_modes(snapshot)
    return snapshot["modes"]


@pytest.mark.parametrize("incomplete", [
    dict(direction="STALE", mark_state="STALE", freshness={"price_state": "STALE"}),
    dict(direction="NO_DATA", mark_state="NO_DATA", freshness={}),
    dict(direction="WARMING_UP"), dict(partial_fill=True), dict(partial_exit=True),
    dict(conflict=True), dict(entry_submission_uncertain=True)])
def test_overall_direction_never_calls_incomplete_book_favorable(incomplete):
    modes = summarize_positions([assessed_position(), assessed_position(), assessed_position(**incomplete)])
    summary = modes["PAPER"]
    assert summary["overall_direction"] == "INSUFFICIENT_DATA"
    assert summary["eligible_count"] == 2 and summary["open_count"] == 3
    if incomplete.get("direction") != "WARMING_UP":
        assert summary["gross_marked_pnl_rs"] is None
        assert summary["gross_marked_pnl_coverage_count"] == 2


def test_overall_direction_keeps_modes_separate_and_includes_square_off_pending():
    modes = summarize_positions([
        assessed_position(), assessed_position(status="SQUARE_OFF_PENDING"),
        assessed_position("ADVERSE", advisory_flags=["TREND_FAILURE", "PROFIT_GIVEBACK"]),
        assessed_position("ADVERSE", mode="LIVE"),
        assessed_position("ADVERSE", status="CLOSED"), assessed_position(status="PENDING_ENTRY")])
    paper, live = modes["PAPER"], modes["LIVE"]
    assert paper["overall_direction"] == "FAVORABLE" and paper["open_count"] == 3
    assert paper["eligible_count"] == 3 and paper["attention_count"] == 1
    assert paper["open_direction_counts"]["FAVORABLE"] == 2
    assert paper["open_direction_counts"]["ADVERSE"] == 1
    assert paper["gross_marked_pnl_rs"] == 375.
    assert live["overall_direction"] == "ADVERSE" and live["gross_marked_pnl_rs"] == 125.


def test_overall_direction_tie_is_mixed_and_empty_book_not_a_zero_pnl_claim():
    modes = summarize_positions([assessed_position(), assessed_position("ADVERSE")])
    assert modes["PAPER"]["overall_direction"] == "MIXED"
    assert modes["LIVE"]["overall_direction"] == "NO_OPEN_POSITIONS"
    assert modes["LIVE"]["gross_marked_pnl_rs"] is None


def test_omitted_invalid_snapshots_prevent_a_complete_book_claim():
    paper = summarize_positions([assessed_position()], invalid=1)["PAPER"]
    assert paper["overall_direction"] == "INSUFFICIENT_DATA"
    assert paper["gross_marked_pnl_rs"] is None and paper["gross_marked_pnl_coverage_count"] == 1
    assert "completeness is unverified" in paper["overall_reason"]
    assert summarize_positions([], invalid=1)["PAPER"]["overall_direction"] == "INSUFFICIENT_DATA"


def test_warming_book_can_have_marked_pnl_but_no_direction_claim():
    paper = summarize_positions([assessed_position("WARMING_UP")])["PAPER"]
    assert paper["overall_direction"] == "INSUFFICIENT_DATA"
    assert paper["eligible_count"] == 0 and paper["gross_marked_pnl_rs"] == 125.


def test_stale_load_recomputes_overall_direction_and_withholds_marked_pnl(tmp_path):
    snapshot = build(tmp_path)
    snapshot["trades"] = [assessed_position(advisory_flags=["PROFIT_GIVEBACK"])]
    monitor._summarize_modes(snapshot)
    assert snapshot["modes"]["PAPER"]["overall_direction"] == "FAVORABLE"
    monitor.write_snapshot(snapshot, tmp_path/"output")
    result = monitor.load_latest(tmp_path/"output", day=DAY, now=NOW+timedelta(seconds=46))
    paper = result["modes"]["PAPER"]
    assert paper["overall_direction"] == "INSUFFICIENT_DATA"
    assert paper["eligible_count"] == paper["attention_count"] == paper["gross_marked_pnl_coverage_count"] == 0
    assert paper["gross_marked_pnl_rs"] is None
    assert paper["open_direction_counts"]["STALE"] == 1
