"""Current clocks must govern monitoring without accepting old terminal counts."""
from datetime import datetime

import pytest

import fno_v13_v10_g_3_contract as contract
from ai_platform.observability.runtime_collector import RuntimeMetricsCollector
from ai_platform.observability import live_trust as trust


@pytest.mark.parametrize("clock,previous", [("10:35", "10:30"), ("11:25", "11:20"),
                                           ("11:35", "11:25"), ("12:30", "12:25")])
def test_metrics_require_each_added_signal_slot(tmp_path, clock, previous):
    stamp = datetime.fromisoformat(f"2026-10-14T{clock}:00+05:30")
    from datetime import timedelta
    observed = stamp + timedelta(minutes=3, seconds=1)
    collector = RuntimeMetricsCollector(tmp_path, profile="V13_V10_G_3", cache_ttl_seconds=0,
                                        now=lambda: observed)
    path = tmp_path / "runtime_status/fno_v13_v10_g_3_scanner_5min.heartbeat"
    path.parent.mkdir()
    series = 'trading_pipeline_schedule_overdue{mode="live",pipeline="v13_v10_g_3_scanner"}'
    def write(slot):
        path.write_text(f"state=SUCCESS\nphase=SLOT_DONE\nslot={slot}\nts=2026-10-14T{slot}:20+05:30\n")
    write(previous)
    assert f"{series} 1" in collector.render_prometheus_samples()
    write(clock)
    assert f"{series} 0" in collector.render_prometheus_samples()


@pytest.mark.parametrize("helper", [trust._scanner_schedule_detail,
    trust._confirmation_completion_detail, trust._equity_feed_completion_detail])
def test_terminal_completion_requires_all_30_signal_clocks(helper):
    now = datetime.fromisoformat("2026-10-14T12:34:00+05:30")
    slots = tuple(contract.SIGNAL_TO_CONFIRMATION)
    assert len(slots) == 30 and slots[-1] == "12:30"
    heartbeat = dict(state="DONE", phase="ALL_V6_WINDOWS_DONE", slot="12:30",
                     ts="2026-10-14T12:33:00+05:30", processed_slots="22")
    assert helper(heartbeat, now=now, session_date=now.date(), slots=slots)[0] is False
    heartbeat["processed_slots"] = str(len(slots))
    assert helper(heartbeat, now=now, session_date=now.date(), slots=slots)[0] is True
