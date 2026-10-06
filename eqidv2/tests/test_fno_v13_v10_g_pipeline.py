"""G scanner/feed/confirmation integration using temporary evidence and fake prices."""
from __future__ import annotations

import importlib.util
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import fno_equity_fetch_1min as feed
import fno_oi_common as common
import fno_v13_v10_g_live_config as config


DAY = date(2026, 9, 15)
ROOT = Path(__file__).resolve().parents[1]


class HistoryBroker:
    def __init__(self, records):
        self.records = records
        self.calls = []

    def historical_data(self, instrument_token, from_date, to_date, interval, **kwargs):
        self.calls.append(dict(instrument_token=instrument_token, from_date=from_date,
                               to_date=to_date, interval=interval, **kwargs))
        return deepcopy(self.records)


class ForbiddenConsumerBroker:
    def __getattr__(self, name):
        raise AssertionError(f"Confirmation consumer attempted broker access: {name}")


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G")
    monkeypatch.delenv("FNO_V6_EXECUTION_SESSION_NAMESPACE", raising=False)
    for name, value in {
        "FNO_ROOT": tmp_path / "fno_oi",
        "LATEST_DIR": tmp_path / "latest",
        "EQUITY_1M_RAW_DIR": tmp_path / "raw_1m",
        "EQUITY_1M_SLOT_DIR": tmp_path / "slot_1m",
    }.items():
        monkeypatch.setattr(common, name, value)
    spec = importlib.util.spec_from_file_location("g_pipeline_test_runtime", ROOT / "fno_v5_live.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.config.STRATEGY_VERSION == config.STRATEGY_VERSION
    return module


def scanner_snapshot(runtime, monkeypatch, signal_end, side, *, nifty_return=-.2, feature_overrides=None):
    stamp = config.slot_datetime(DAY, signal_end)
    direction = 1 if side == "LONG" else -1
    featured = pd.DataFrame([dict(ts=pd.Timestamp(stamp), close=100.,
        price_change_pct=.8 * direction, oi_change_pct=.5, oi=100500, prev_oi=100000,
        volume_ratio=10., traded_value=50_000_000.,
        ema9=100 + direction, ema20=100., ema50=100 - direction)])
    for key, value in (feature_overrides or {}).items():
        featured[key] = value
    mapped = pd.DataFrame([dict(underlying="EXAMPLE", futures_tradingsymbol="EXAMPLE26SEPFUT",
        equity_symbol="EXAMPLE", futures_instrument_token=222,
        equity_instrument_token=111, equity_tick_size=.05)])
    dated = pd.DataFrame([dict(underlying="NIFTY", tradingsymbol="NIFTY26SEPFUT")])

    def dated_universe(*, expected_date):
        assert expected_date == DAY
        return dated

    def futures(symbol):
        if symbol == "NIFTY26SEPFUT":
            if nifty_return is None:
                return pd.DataFrame()
            return pd.DataFrame([
                dict(ts=pd.Timestamp(config.slot_datetime(DAY, "09:20")), open=100., close=100 + nifty_return),
                # Later market movement must not affect the opening context.
                dict(ts=pd.Timestamp(config.slot_datetime(DAY, "09:25")), open=100., close=150.),
            ])
        assert symbol == "EXAMPLE26SEPFUT"
        return pd.DataFrame([dict(ts=pd.Timestamp(stamp), oi=100500)])

    monkeypatch.setattr(common, "load_near_month_universe", dated_universe)
    monkeypatch.setattr(runtime.backtest, "load_five_minute", futures)
    monkeypatch.setattr(runtime.hybrid, "load_equity_five_minute", lambda *a, **kw: featured.copy())
    monkeypatch.setattr(runtime.hybrid, "join_equity_price_with_futures_oi", lambda *a: featured.copy())
    snapshot = runtime.scan_five_minute_slot(mapped, DAY, signal_end)
    runtime._write_scanner_snapshot(DAY, signal_end, snapshot)
    return snapshot


def history_records(signal_end, side, *, volume=200., prior_count=20):
    start = config.slot_datetime(DAY, signal_end)
    prior_start = config.slot_datetime(date(2026, 9, 11), "15:10")
    history = [dict(date=prior_start + timedelta(minutes=i), open=100., high=101.,
                    low=99., close=100., volume=100.) for i in range(prior_count)]
    bar = dict(date=start, open=100., high=100.9 if side == "LONG" else 100.1,
               low=99.9 if side == "LONG" else 99.1,
               close=100.8 if side == "LONG" else 99.2, volume=volume)
    future = dict(date=start + timedelta(minutes=1), open=100., high=101.,
                  low=99., close=100., volume=999_999_999.)
    return history + [bar, future]


def produce(runtime, monkeypatch, snapshot, signal_end, side, **history_options):
    conf = config.slot_datetime(DAY, config.SIGNAL_TO_CONFIRMATION[signal_end])
    monkeypatch.setattr(common, "now_ist", lambda: conf + timedelta(seconds=10))
    client = HistoryBroker(history_records(signal_end, side, **history_options))
    marker = feed.produce_slot(snapshot, "v6", DAY, signal_end,
                               [feed.AppRuntime("FAKE_HISTORY", client, pace_seconds=0.)])
    return marker, client


def confirm(runtime, snapshot, signal_end):
    return runtime.process_confirmation_slot(
        snapshot, DAY, signal_end, ForbiddenConsumerBroker(),
        SimpleNamespace(capital=config.CAPITAL_PER_ENTRY_RS, leverage=config.LEVERAGE),
    )


@pytest.mark.parametrize("signal_end,side", [("09:25", "LONG"), ("09:25", "SHORT"), ("11:20", "SHORT")])
def test_g_scanner_through_immutable_feed_to_paper_state(runtime, monkeypatch, signal_end, side):
    snapshot = scanner_snapshot(runtime, monkeypatch, signal_end, side)
    assert snapshot["state"] == "SUCCESS"
    assert len(snapshot["candidates"]) == 1
    candidate = snapshot["candidates"][0]
    assert candidate["strategy_profile"] == "V13_V10_G"
    assert candidate["exchange"] == "NSE"
    assert candidate["futures_instrument_token"] == 222
    marker, broker = produce(runtime, monkeypatch, snapshot, signal_end, side)
    assert marker["state"] == "SUCCESS", marker.get("errors")
    assert marker["strategy_version"] == config.STRATEGY_VERSION
    assert len(broker.calls) == 1
    assert broker.calls[0]["from_date"] == config.slot_datetime(DAY, signal_end) - timedelta(days=7)
    assert broker.calls[0]["oi"] is False
    persisted = pd.read_parquet(marker["slot_data_path"])
    assert persisted.loc[0, "confirmation_prior_volume_count"] == 20
    assert persisted.loc[0, "confirmation_prior_volume_mean"] == 100.
    assert persisted.loc[0, "v9_1m_volume_ratio"] == 2.
    conf = config.slot_datetime(DAY, config.SIGNAL_TO_CONFIRMATION[signal_end])
    assert pd.Timestamp(persisted.loc[0, "v9_1m_feature_ts"]) == pd.Timestamp(conf)
    assert pd.Timestamp(persisted.loc[0, "confirmation_prior_volume_last_ts"]) < pd.Timestamp(conf)
    assert feed.produce_slot(snapshot, "v6", DAY, signal_end, []) == marker
    result = confirm(runtime, snapshot, signal_end)
    assert result["state"] == "SUCCESS", result["errors"]
    assert len(result["_selected_signals"]) == 1
    signal = result["_selected_signals"][0]
    runtime._validate_signal(signal, DAY)
    assert pd.Timestamp(signal["entry_activation_deadline_ist"]) == pd.Timestamp(conf + timedelta(minutes=10))
    assert pd.Timestamp(marker["deadline_ist"]) == pd.Timestamp(conf + timedelta(seconds=90))
    runtime._commit_confirmation_decision(DAY, signal_end, result, result["_selected_signals"])
    loaded = runtime.load_signals(DAY, side)
    assert len(loaded) == 1
    assert loaded[0]["signal_id"] == signal["signal_id"]
    state = runtime.create_order_state(loaded[0], "PAPER")
    runtime._validate_order_state(state, loaded[0], "PAPER")
    assert state["setup_id"] == config.setup_for(signal_end, side).setup_id
    assert state["rank_within_scan"] == 1
    assert state["stop_pct"] == config.setup_for(signal_end, side).stop_pct
    assert state["target_pct"] == config.setup_for(signal_end, side).target_pct
    assert state["quantity"] > 1
    assert state["strategy_fingerprint"] == config.strategy_fingerprint()


@pytest.mark.parametrize("day,expected", [(date(2026, 10, 5), 0), (date(2026, 10, 6), 1)])
def test_dated_relaxed_candidate_reaches_confirmation_and_execution_state(runtime, monkeypatch, day, expected):
    import sys
    monkeypatch.setattr(sys.modules[__name__], "DAY", day)
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "LONG", feature_overrides=dict(
        price_change_pct=.30, oi_change_pct=1.20, oi=101200., prev_oi=100000.,
        volume_ratio=1.75, ema9=99., ema20=100., ema50=101.))
    assert len(snapshot["candidates"]) == expected
    if not expected:
        return
    marker, _ = produce(runtime, monkeypatch, snapshot, "09:25", "LONG")
    assert marker["state"] == "SUCCESS"
    result = confirm(runtime, snapshot, "09:25")
    assert result["state"] == "SUCCESS", result.get("errors")
    signal, = result["_selected_signals"]
    runtime._validate_signal(signal, day)
    assert signal["relaxed_0925_added"] is True
    assert signal["stop_pct"] == 1.25
    assert signal["target_pct"] == config.setup_for("09:25", "LONG").target_pct
    for mode in ("PAPER", "LIVE"):
        state = runtime.create_order_state(signal, mode)
        runtime._validate_order_state(state, signal, mode)
        assert state["stop_pct"] == 1.25
        assert state["tightened_stop_pct"] == 1.0
        assert state["tighten_after_minutes"] == 120


@pytest.mark.parametrize("volume,prior_count,selected", [(120., 20, 1), (119., 20, 0), (200., 4, 0)])
def test_g_volume_boundary_and_missing_denominator_reject_before_selection(
    runtime, monkeypatch, volume, prior_count, selected
):
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "LONG")
    marker, _ = produce(runtime, monkeypatch, snapshot, "09:25", "LONG", volume=volume, prior_count=prior_count)
    assert marker["state"] == "SUCCESS", marker.get("errors")
    result = confirm(runtime, snapshot, "09:25")
    assert result["state"] == "SUCCESS"
    assert len(result["_selected_signals"]) == selected


@pytest.mark.parametrize("nifty_return,expected_shorts", [(-.2, 1), (.2, 0), (None, 0)])
def test_0925_short_uses_dated_nifty_future_first_completed_bar(
    runtime, monkeypatch, nifty_return, expected_shorts
):
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "SHORT", nifty_return=nifty_return)
    assert snapshot["short_candidates"] == expected_shorts
    if nifty_return is not None:
        context = snapshot["index_context"]
        assert context["nifty_futures_tradingsymbol"] == "NIFTY26SEPFUT"
        assert context["nifty_first_bar_return_pct"] == pytest.approx(nifty_return)
        assert context["nifty_feature_timestamp"].endswith("09:20:00+05:30")


def test_late_short_needs_no_opening_nifty_override(runtime, monkeypatch):
    snapshot = scanner_snapshot(runtime, monkeypatch, "11:20", "SHORT", nifty_return=None)
    assert snapshot["short_candidates"] == 1
    assert snapshot["index_context"] == {}
    assert runtime.PIPELINE_DEADLINE.hour >= 11


def test_final_confirmation_uses_immutable_data_and_detects_tampering(runtime, monkeypatch):
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "LONG")
    marker, broker = produce(runtime, monkeypatch, snapshot, "09:25", "LONG")
    assert marker["state"] == "SUCCESS", marker.get("errors")
    raw_path = common.equity_1m_path(DAY, "EXAMPLE")
    raw = pd.read_parquet(raw_path)
    raw["v9_1m_volume_ratio"] = .01
    common.atomic_write_parquet(raw, raw_path)
    assert len(confirm(runtime, snapshot, "09:25")["_selected_signals"]) == 1
    assert len(broker.calls) == 1
    immutable = Path(marker["slot_data_path"])
    altered = pd.read_parquet(immutable)
    altered["volume"] = 1
    common.atomic_write_parquet(altered, immutable)
    result = confirm(runtime, snapshot, "09:25")
    assert result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert result["_selected_signals"] == []
    assert result["error_count"] >= 1


def test_old_v6_scanner_identity_cannot_enter_g_producer(runtime, monkeypatch):
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "LONG")
    snapshot["strategy_version"] = "FNO_V6_BEST_NET_CASH_EQUITY_20260811"
    with pytest.raises(ValueError, match="strategy version mismatch"):
        feed.produce_slot(snapshot, "v6", DAY, "09:25", [])


def test_g_warmup_expands_to_35_days_when_seven_days_are_incomplete(runtime, monkeypatch):
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "LONG")
    conf = config.slot_datetime(DAY, "09:26")
    monkeypatch.setattr(common, "now_ist", lambda: conf + timedelta(seconds=10))

    class SparseHistoryBroker(HistoryBroker):
        def historical_data(self, *args, **kwargs):
            self.records = history_records("09:25", "LONG", prior_count=4 if not self.calls else 20)
            return super().historical_data(*args, **kwargs)

    broker = SparseHistoryBroker([])
    marker = feed.produce_slot(snapshot, "v6", DAY, "09:25", [feed.AppRuntime("FAKE", broker, 0.)])
    assert marker["state"] == "SUCCESS", marker.get("errors")
    assert len(broker.calls) == 2
    assert broker.calls[1]["from_date"] == config.slot_datetime(DAY, "09:25") - timedelta(days=35)
    assert broker.calls[1]["to_date"] == config.slot_datetime(DAY, "09:25")
    assert len(confirm(runtime, snapshot, "09:25")["_selected_signals"]) == 1


def test_g_history_ignores_future_and_outside_session_volume(runtime):
    records = history_records("09:25", "LONG")
    for hhmm in ("08:59", "09:14", "15:30", "17:00"):
        records.append(dict(date=config.slot_datetime(date(2026, 9, 11), hhmm), volume=1_000_000.))
    frame = feed._g_completed_volume_history(records, config.slot_datetime(DAY, "09:26"))
    assert len(frame) == 20
    assert frame["volume"].eq(100.).all()


def test_bad_cached_g_volume_is_refetched_before_publishing_authority(runtime, monkeypatch):
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "LONG")
    start, conf = config.slot_datetime(DAY, "09:25"), config.slot_datetime(DAY, "09:26")
    monkeypatch.setattr(common, "now_ist", lambda: conf + timedelta(seconds=10))
    broker = HistoryBroker(history_records("09:25", "LONG"))
    fake = feed.AppRuntime("FAKE", broker, 0.)
    assert feed._fetch_one(fake, snapshot["candidates"][0], start, conf)["state"] == "WRITTEN"
    raw_path = common.equity_1m_path(DAY, "EXAMPLE")
    corrupted = pd.read_parquet(raw_path)
    corrupted["v9_1m_volume_ratio"] = 200.
    common.atomic_write_parquet(corrupted, raw_path)
    marker = feed.produce_slot(snapshot, "v6", DAY, "09:25", [fake])
    assert marker["state"] == "SUCCESS", marker.get("errors")
    assert len(broker.calls) == 2
    actual = pd.read_parquet(marker["slot_data_path"])
    assert actual.loc[0, "v9_1m_volume_ratio"] == 2.


def test_future_minute_cannot_substitute_for_exact_confirmation(runtime, monkeypatch):
    snapshot = scanner_snapshot(runtime, monkeypatch, "09:25", "LONG")
    start, conf = config.slot_datetime(DAY, "09:25"), config.slot_datetime(DAY, "09:26")
    monkeypatch.setattr(common, "now_ist", lambda: conf + timedelta(seconds=10))
    records = [row for row in history_records("09:25", "LONG") if row["date"] != start]
    broker = HistoryBroker(records)
    marker = feed.produce_slot(snapshot, "v6", DAY, "09:25", [feed.AppRuntime("FAKE", broker, 0.)])
    assert marker["state"] == "WAITING_INCOMPLETE_DATA"
    assert marker["written_count"] == 0
    result = confirm(runtime, snapshot, "09:25")
    assert result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert result["_selected_signals"] == []
