from datetime import datetime
from .run import IST, phase, singleton, write_json, read_json


def test_one_day_window_and_cutoffs():
    def at(day, hh, mm):
        return datetime(2026, 11, day, hh, mm, tzinfo=IST)
    assert phase(at(8,14,19)) == 'WAITING_FOR_SESSION'
    assert phase(at(8,14,20)) == 'PAPER_RUNNING'
    assert phase(at(8,15,9)) == 'PAPER_RUNNING'
    assert phase(at(8,15,10)) == 'FINALIZING'
    assert phase(at(8,15,45)) == 'COMPLETE'
    assert phase(at(9,9,15)) == 'EXPIRED'
    assert phase(at(6,14,30)) == 'EXPIRED'


def test_singleton_prevents_duplicate_launch_and_releases(tmp_path):
    path = tmp_path / 'session.lock'
    with singleton(path) as first:
        assert first
        with singleton(path) as duplicate:
            assert not duplicate
    with singleton(path) as restarted:
        assert restarted


def test_atomic_state_preserves_first_seen(tmp_path):
    path = tmp_path / 'state.json'
    assert read_json(path,{}) == {}
    state = {'slots':{'14:25':{'first_seen':'2026-11-08T14:26:08+05:30'}}}
    write_json(path,state)
    assert read_json(path,{}) == state
    assert not path.with_suffix('.json.tmp').exists()


def test_fetch_latency_uses_later_decision_clock_and_request_watermark(tmp_path, monkeypatch):
    import pandas as pd
    from . import run as runner, data, engine
    request = datetime(2026,11,8,14,26,5,tzinfo=IST)
    available = datetime(2026,11,8,14,27,20,tzinfo=IST)
    clocks = iter([request, request, available, available])
    monkeypatch.setattr(runner, 'local_now', lambda: next(clocks))
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    monkeypatch.setattr(runner, 'prepare', lambda: None)
    seen = []

    class FakeCollector:
        def __init__(self, root):
            self.last_health = {'healthy_apps':1}
        def healthcheck(self):
            return self.last_health
        def collect(self, now):
            assert now == request
            return {'healthy_apps':1}
        def load_frames(self):
            return {}

    def evaluate(frames, asof, *, mode, state):
        assert asof == available
        assert frames['source_completed_through'] == pd.Timestamp('2026-11-08T14:26:00+05:30')
        assert mode == 'paper' and state is None
        seen.append(mode)
        return dict(state={},trades=pd.DataFrame(),audit=pd.DataFrame(),candidates=pd.DataFrame(),summary={})

    monkeypatch.setattr(data, 'Collector', FakeCollector)
    monkeypatch.setattr(engine, 'evaluate', evaluate)
    assert runner.run(watch=False) == 0
    assert seen == ['paper']
