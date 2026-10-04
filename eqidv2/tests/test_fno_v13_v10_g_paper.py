"""PAPER capital admission across independent side workers, never broker calls."""
import copy
import importlib.util
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import fno_v13_v10_g_live_config as config
import fno_v13_v10_g_paper as paper
import fno_oi_common as common


def order(identity="CURRENT", status="OPEN", **updates):
    result = dict(signal_id=identity, status=status, mode="PAPER", strategy_version=config.STRATEGY_VERSION,
                  strategy_fingerprint=config.strategy_fingerprint(), session_date="2026-09-11", side="LONG",
                  capital_rs=100000., quantity=1000, entry_price=500.,
                  confirmation_end="09:31", entry_activation_deadline_ist="2026-09-11T09:41:00+05:30",
                  updated_at_ist="2026-09-11T09:32:00+05:30", tradingsymbol=identity,
                  trigger_price=500., stop_pct=.6, target_pct=1.2, tick_size=.05,
                  stop_price=497., target_price=506., round_trip_cost_bps=5., entry_at_ist="",
                  last_price=0.)
    result.update(updates)
    return result


def save(root, state):
    path = root / f"{state['signal_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")


@pytest.fixture
def g_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G")
    monkeypatch.setenv("FNO_V6_EXECUTION_SESSION_NAMESPACE", "")
    monkeypatch.setattr(common, "FNO_ROOT", tmp_path / "fno_oi")
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path / "latest")
    spec = importlib.util.spec_from_file_location("g_paper_integration_runtime", Path(__file__).resolve().parents[1] / "fno_v5_live.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.config.STRATEGY_VERSION == config.STRATEGY_VERSION
    return module


def test_runtime_persists_ten_fills_blocks_eleventh_and_releases_after_exit(g_runtime):
    now = datetime.fromisoformat("2026-09-11T09:32:00+05:30")
    states = []
    for i in range(11):
        pending = order(str(i), "PENDING_ENTRY", entry_price=0., side="LONG" if i % 2 == 0 else "SHORT")
        result = g_runtime._advance_g_paper_state(pending, 500., now)
        states.append(result)
        stored = json.loads(g_runtime._order_path(now.date(), "PAPER", str(i)).read_text())
        assert stored == result
    assert sum(s["status"] == "OPEN" for s in states) == 10
    assert states[-1]["status"] == "BLOCKED_PORTFOLIO"
    assert states[-1]["entry_price"] == 0
    day_root = g_runtime.order_day_dir(now.date(), "PAPER")
    assert paper.capacity_for_fill(order("NEXT"), day_root)["reserved_capital_rs"] == 1e6
    closed = g_runtime._advance_g_paper_state(states[0], 510., now)
    assert closed["status"] == "CLOSED"
    added = g_runtime._advance_g_paper_state(order("NEXT", "PENDING_ENTRY", entry_price=0.), 500., now)
    assert added["status"] == "OPEN"
    assert paper.capacity_for_fill(order("AFTER"), day_root)["reserved_capital_rs"] == 1e6


def test_runtime_reads_latest_locked_state_and_expiry_requires_no_quote(g_runtime, monkeypatch):
    monkeypatch.setattr(g_runtime, "KitePool", lambda *a, **k: pytest.fail("Expiry opened a broker pool"))
    pending = order(status="PENDING_ENTRY", entry_price=0.)
    now = datetime.fromisoformat("2026-09-11T09:41:01+05:30")
    day_root = g_runtime.order_day_dir(now.date(), "PAPER")
    save(day_root, pending)
    result = g_runtime._advance_g_paper_state(pending, None, now)
    assert result["status"] == "CANCELLED"
    assert json.loads(g_runtime._order_path(now.date(), "PAPER", "CURRENT").read_text())["status"] == "CANCELLED"
    # A stale copy cannot reopen a state another worker already cancelled.
    stale = g_runtime._advance_g_paper_state(pending, 501., now)
    assert stale["status"] == "CANCELLED"


def test_worker_expires_existing_pending_before_broker_pool_creation(g_runtime, monkeypatch):
    now = datetime.fromisoformat("2026-09-11T09:41:01+05:30")
    pending = order(status="PENDING_ENTRY", entry_price=0.)
    save(g_runtime.order_day_dir(now.date(), "PAPER"), pending)
    monkeypatch.setattr(g_runtime.common, "now_ist", lambda: now)
    monkeypatch.setattr(g_runtime, "load_signals", lambda *a, **k: [copy.deepcopy(pending)])
    monkeypatch.setattr(g_runtime, "_validate_order_state", lambda *a, **k: None)
    monkeypatch.setattr(g_runtime, "_blocking_pipeline_issue", lambda *a, **k: None)
    monkeypatch.setattr(g_runtime, "_render_worker_report", lambda *a, **k: "isolated test")
    monkeypatch.setattr(g_runtime, "_publish", lambda *a, **k: None)
    monkeypatch.setattr(g_runtime, "KitePool", lambda *a, **k: pytest.fail("Expired pending order initialized KitePool"))
    args = SimpleNamespace(execution_mode="PAPER", once=True, max_apps=1, timeout_sec=.1,
                           live_quantity=None, poll_sec=.01)
    assert g_runtime.run_worker(args, now.date(), "LONG") == 0
    stored = json.loads(g_runtime._order_path(now.date(), "PAPER", "CURRENT").read_text())
    assert stored["status"] == "CANCELLED"


def test_paper_quotes_fail_over_from_primary_to_next_healthy_app(g_runtime):
    calls = []

    class Client:
        def __init__(self, app_name, error=None):
            self.app_name = app_name
            self.error = error

        def ltp(self, keys):
            calls.append((self.app_name, keys))
            if self.error is not None:
                raise self.error
            return {"NSE:OFSS": {"last_price": 10570.0}}

    prices, quote_app, failures = g_runtime._quote_prices_with_failover(
        [
            ("app1", Client("app1", RuntimeError("expired token"))),
            ("app2", Client("app2")),
            ("app3", Client("app3")),
        ],
        ["OFSS"],
    )

    assert prices == {"OFSS": 10570.0}
    assert quote_app == "app2"
    assert failures == [
        {"app": "app1", "error_type": "RuntimeError", "message": "expired token"}
    ]
    assert calls == [
        ("app1", ["NSE:OFSS"]),
        ("app2", ["NSE:OFSS"]),
    ]


def test_paper_quote_failover_reports_all_failed_apps(g_runtime):
    class FailedClient:
        def __init__(self, message):
            self.message = message

        def ltp(self, _keys):
            raise RuntimeError(self.message)

    with pytest.raises(RuntimeError, match="All configured Kite quote apps failed") as exc:
        g_runtime._quote_prices_with_failover(
            [
                ("app1", FailedClient("expired token")),
                ("app2", FailedClient("network timeout")),
            ],
            ["OFSS"],
        )

    assert "app1=RuntimeError: expired token" in str(exc.value)
    assert "app2=RuntimeError: network timeout" in str(exc.value)


def test_runtime_nifty_context_uses_dated_future_exact_end_label(g_runtime, monkeypatch):
    import pandas as pd
    day = datetime.fromisoformat("2026-09-11T09:25:00+05:30").date()
    calls = []
    def universe(*, expected_date):
        assert expected_date == day
        return pd.DataFrame(dict(underlying=["NIFTY", "BANKNIFTY"], tradingsymbol=["NIFTY26SEPFUT", "BANKNIFTY26SEPFUT"]))
    def bars(symbol):
        calls.append(symbol)
        return pd.DataFrame(dict(ts=pd.to_datetime(["2026-09-11T09:20:00+05:30", "2026-09-11T09:25:00+05:30"]),
                                 open=[100., 100.], close=[99.9, 105.]))
    monkeypatch.setattr(g_runtime.common, "load_near_month_universe", universe)
    monkeypatch.setattr(g_runtime.backtest, "load_five_minute", bars)
    observed = g_runtime._g_nifty_first_bar_context(day)
    assert calls == ["NIFTY26SEPFUT"]
    assert observed["nifty_first_bar_return_pct"] == pytest.approx(-.1)
    assert observed["nifty_context_state"] == "READY"


def test_capacity_shared_across_sides_and_pending_does_not_reserve():
    states = [order(str(i), side="LONG" if i % 2 else "SHORT") for i in range(9)]
    states += [order("PENDING", "PENDING_ENTRY"), order("CLOSED", "CLOSED")]
    available = paper.capacity_from_states(states, order())
    assert available["allowed"]
    assert available["reserved_capital_rs"] == 900000.
    assert available["open_positions"] == 9
    blocked = paper.capacity_from_states(states + [order("TENTH", side="SHORT")], order())
    assert not blocked["allowed"]
    assert blocked["available_capital_rs"] == 0
    assert blocked["reason"] == "PORTFOLIO_CAPITAL_LIMIT"


def test_closed_positions_release_capital_and_current_state_is_excluded():
    states = [order(str(i)) for i in range(10)] + [order()]
    assert not paper.capacity_from_states(states, order())["allowed"]
    states[0]["status"] = "CLOSED"
    assert paper.capacity_from_states(states, order())["allowed"]


def test_other_day_and_live_states_do_not_reserve_today_budget():
    states = [order(str(i), session_date="2026-09-10") for i in range(10)]
    states += [order("LIVE", mode="LIVE")]
    observed = paper.capacity_from_states(states, order())
    assert observed["allowed"]
    assert observed["reserved_capital_rs"] == 0


@pytest.mark.parametrize("updates", [
    dict(capital_rs=float("nan")), dict(quantity=0), dict(quantity=1.2), dict(entry_price=0),
    dict(capital_rs=1), dict(strategy_version="OLD"), dict(strategy_fingerprint="STALE"),
    dict(mode=""), dict(session_date=""), dict(side=""), dict(status="UNKNOWN"), dict(signal_id=""),
])
def test_bad_active_state_fails_closed(updates):
    with pytest.raises(paper.PortfolioStateError):
        paper.capacity_from_states([order("OTHER", **updates)], order())


def test_duplicate_active_state_fails_closed():
    with pytest.raises(paper.PortfolioStateError, match="Duplicate"):
        paper.capacity_from_states([order("OTHER"), order("OTHER")], order())


def test_capacity_reader_covers_nested_sides_and_rejects_unreadable_json(tmp_path):
    save(tmp_path / "LONG", order("L"))
    save(tmp_path / "SHORT", order("S", side="SHORT"))
    assert paper.capacity_for_fill(order(), tmp_path)["open_positions"] == 2
    (tmp_path / "bad.json").write_text("{", encoding="utf-8")
    with pytest.raises(paper.PortfolioStateError, match="Unreadable"):
        paper.capacity_for_fill(order(), tmp_path)


def test_denial_reverts_fill_and_corruption_never_leaks_open_state(tmp_path):
    for i in range(10):
        save(tmp_path, order(str(i)))
    previous = order(status="PENDING_ENTRY", entry_price=0., entry_at_ist="", net_pnl_rs=0.)
    proposed = {**previous, "status": "OPEN", "entry_price": 501., "entry_at_ist": "NOW"}
    denied = paper.enforce_fill_capacity(previous, proposed, tmp_path)
    assert denied["status"] == "BLOCKED_PORTFOLIO"
    assert denied["entry_price"] == 0
    assert denied["entry_at_ist"] == ""
    assert previous["status"] == "PENDING_ENTRY"
    (tmp_path / "bad.json").write_text("INVALID", encoding="utf-8")
    denied = paper.enforce_fill_capacity(previous, proposed, tmp_path)
    assert denied["status_reason"] == "PAPER_PORTFOLIO_STATE_INVALID"
    assert denied["entry_price"] == 0
    assert paper.enforce_fill_capacity(order(), order(status="CLOSED"), tmp_path)["status"] == "CLOSED"


def test_successful_fill_records_admission(tmp_path):
    previous = order(status="PENDING_ENTRY", entry_price=0)
    proposed = order()
    assert paper.enforce_fill_capacity(previous, proposed, tmp_path)["paper_portfolio_admission"]["allowed"]


def test_pending_expiry_needs_no_quote_and_boundary_is_inclusive():
    pending = order(status="PENDING_ENTRY", entry_price=0)
    assert not paper.expire_pending(pending, datetime.fromisoformat("2026-09-11T09:41:00+05:30"))
    assert paper.expire_pending(pending, datetime.fromisoformat("2026-09-11T09:41:01+05:30"))
    assert pending["status"] == "CANCELLED"
    assert pending["status_reason"] == "ENTRY_TRIGGER_WINDOW_EXPIRED"
    opened = order()
    assert not paper.expire_pending(opened, datetime.fromisoformat("2026-09-11T15:32:00+05:30"))
    assert opened["status"] == "OPEN"


def test_malformed_expiry_cannot_advance():
    pending = order(status="PENDING_ENTRY", entry_activation_deadline_ist="2026-09-11T10:00:00+05:30")
    with pytest.raises(paper.PortfolioStateError, match="expiry"):
        paper.expire_pending(pending, datetime.fromisoformat("2026-09-11T09:35:00+05:30"))


def test_kernel_lock_excludes_other_process_and_releases_after_error(tmp_path):
    script = (
        "import sys; from fno_v13_v10_g_paper import paper_portfolio_lock; "
        "\nwith paper_portfolio_lock(sys.argv[1]):\n print('LOCKED', flush=True); sys.stdin.readline()\n"
    )
    process = subprocess.Popen([sys.executable, "-B", "-c", script, str(tmp_path)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "LOCKED"
        with pytest.raises(TimeoutError):
            with paper.paper_portfolio_lock(tmp_path, timeout_sec=.05):
                pytest.fail("Second process acquired an occupied portfolio lock")
        process.communicate("release\n", timeout=10)
        assert process.returncode == 0
        with pytest.raises(RuntimeError, match="inner"):
            with paper.paper_portfolio_lock(tmp_path, timeout_sec=.1):
                raise RuntimeError("inner")
        with paper.paper_portfolio_lock(tmp_path, timeout_sec=.1):
            pass
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)


def test_two_independent_fillers_cannot_overallocate_last_lakh(tmp_path):
    for i in range(9):
        save(tmp_path, order(str(i)))
    script = (
        "import json,sys; from pathlib import Path; import fno_v13_v10_g_paper as p; "
        "import fno_v13_v10_g_live_config as c; root=Path(sys.argv[1]); "
        "s=json.loads(sys.argv[2]); "
        "\nwith p.paper_portfolio_lock(root):\n "
        "a=p.capacity_for_fill(s,root); "
        "\n if a['allowed']: (root/(s['signal_id']+'.json')).write_text(json.dumps(s))\n "
        "print(a['allowed'],flush=True)\n"
    )
    children = [subprocess.Popen([sys.executable, "-B", "-c", script, str(tmp_path),
                                 json.dumps(order(name, side=side))],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for name, side in (("ADDED_LONG", "LONG"), ("ADDED_SHORT", "SHORT"))]
    try:
        results = [process.communicate(timeout=15) for process in children]
        assert [p.returncode for p in children] == [0, 0], results
        assert sorted(out.strip() for out, _ in results) == ["False", "True"]
        assert paper.capacity_for_fill(order(), tmp_path)["open_positions"] == 10
    finally:
        for process in children:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=10)
