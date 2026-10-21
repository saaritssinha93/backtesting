"""Refresh only the supervised 8787 dashboard; verify dated read-only identity."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / ".codex_tmp/g3_selected_ten_20261013/helpers"))
import psutil

DAY = "2026-10-15"
V9 = "V13-V10-G-3-PRODUCTION-V9"
V10 = "V13-V10-G-3-PRODUCTION-V10"
SOURCE = Path("C:/TradingData/eqidv2/fno_oi/v13_v10_g_3_live/confirmation_1m") / DAY / "slot_1011.json"
WORKER_SCRIPTS = {"fno_v13_v10_g_3_live.py", "fno_v13_v10_g_3_options_paper.py",
                  "fno_v13_v10_g_3_live_kite_session.py", "g3_trade_monitor.py"}


def worker_pids():
    result = {}
    for process in psutil.process_iter(["pid", "name"]):
        try:
            if "python" not in (process.info["name"] or "").lower():
                continue
            names = {Path(arg).name for arg in process.cmdline()}
            matched = names & WORKER_SCRIPTS
            if matched and Path(process.cwd()).resolve() == ROOT.resolve():
                result[str(process.pid)] = sorted(matched)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return result


def server():
    found = []
    for process in psutil.process_iter(["pid", "name"]):
        try:
            if "python" not in (process.info["name"] or "").lower():
                continue
            args = process.cmdline()
            if not any(Path(arg).name == "log_dashboard_server.py" for arg in args):
                continue
            if Path(process.cwd()).resolve() != ROOT.resolve():
                continue
            if "--port" not in args or args[args.index("--port") + 1] != "8787":
                continue
            found.append(process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if len(found) > 1:
        raise RuntimeError("Multiple matching dashboard children")
    return found[0] if found else None


def fetch(token, endpoint, **parameters):
    url = "http://127.0.0.1:8787" + endpoint + "?" + urlencode(dict(token=token, **parameters))
    try:
        with urlopen(url, timeout=55) as response:
            return json.loads(response.read())
    except Exception:
        raise RuntimeError("Dashboard verification request failed for " + endpoint) from None


def reliance(data):
    return next(row for row in data["rows_1m"] if row.get("symbol") == "RELIANCE"
        and row.get("minute") == "10:11" and row.get("stage") == "1M_CONFIRMATION_SETUP")


def stage(value):
    print(json.dumps(dict(stage=value)), flush=True)


def main():
    before_workers = worker_pids()
    assert before_workers, "No running G3 workers found to protect"
    current = server()
    assert current is not None, "Dashboard child is not running"
    parent = current.parent()
    assert parent is not None and parent.name().lower() == "cmd.exe", "Unexpected dashboard parent"
    assert any("run_log_dashboard_server.bat" in part.lower() for part in parent.cmdline()), "Dashboard supervisor not verified"
    token = current.environ().get("LOG_DASH_TOKEN")
    assert token, "Existing dashboard authentication unavailable"
    source_hash = hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    stage("Verifying current dashboard before refresh")
    before = reliance(fetch(token, "/api/fno-monitor", date=DAY))
    assert before["decision_reason"] == "No LONG setup configured for 10:10."
    old_pid = current.pid
    stage("Refreshing verified dashboard child only")
    current.terminate()
    current.wait(timeout=10)
    ready = None
    for _ in range(60):
        candidate = server()
        if candidate is not None and candidate.pid != old_pid:
            ready = candidate
            break
        time.sleep(1)
    assert ready is not None, "Supervisor did not recreate dashboard child"
    stage("Verifying dated monitoring API")
    monitor = None
    for _ in range(6):
        try:
            monitor = fetch(token, "/api/fno-monitor", date=DAY)
            break
        except RuntimeError:
            time.sleep(1)
    assert monitor is not None, "Refreshed dashboard did not respond"
    row = reliance(monitor)
    assert monitor["identity"]["strategy_version"] == V9
    assert row["evidence_state"] == "RECORDED"
    assert row["decision_reason"] == before["decision_reason"]
    assert row["setup_status"] == "NOT_CONFIGURED"
    assert row["decision"] == "CONFIRMATION_OR_SETUP_REJECTED"
    stage("Verifying dated cockpit API")
    cockpit = fetch(token, "/api/cockpit", date=DAY, mode="PAPER")
    assert cockpit["identity"]["version"] == V9
    assert cockpit["orders"], "Recorded PAPER orders disappeared"
    assert cockpit["identity"]["display_reference"]["scope"] == "PREVIOUS_SESSION_READ_ONLY"
    stage("Verifying existing trade-monitor snapshot")
    trade_monitor = fetch(token, "/api/g3-trade-monitor", date=DAY)
    assert trade_monitor["state"] != "NO_SNAPSHOT"
    assert trade_monitor["strategy_version"] == V9
    stage("Verifying configured and recorded dashboard card identities")
    snapshot = fetch(token, "/api/snapshot", lines=20)
    assert snapshot["active_strategy"]["version"] == V10
    assert snapshot["active_strategy"]["displayed_session_identity"]["strategy_version"] == V9
    expected_cards = {"fno_v13_v10_g_" + role for role in ("scanner_5min", "confirmation_1min", "live_long",
        "live_short", "trade_logger", "net_result", "options_live_long", "options_live_short",
        "options_trade_logger", "options_net_result")}
    cards = {}
    for item in snapshot["items"]:
        if item.get("id") not in expected_cards:
            continue
        status = item.get("status", {})
        assert status.get("strategy_identity_state") in {"MATCH", "DECLARED_MATCH"}, item["id"] + " identity mismatch"
        assert status.get("expected_strategy_version") == V9, item["id"] + " prior version missing"
        cards[item["id"]] = dict(identity=status["strategy_identity_state"], version=status["expected_strategy_version"])
    assert set(cards) == expected_cards, "Expected G3 cards missing from snapshot"
    after_workers = worker_pids()
    assert after_workers == before_workers, "G3 worker PID inventory changed during dashboard refresh"
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest() == source_hash, "Recorded RELIANCE evidence changed"
    receipt = dict(status="PASS", date=DAY, port=8787, previous_dashboard_pid=old_pid,
        current_dashboard_pid=ready.pid, dashboard_supervisor_pid=parent.pid,
        workers_before=before_workers, workers_after=after_workers, workers_untouched=True,
        configured_version=V10, displayed_session_version=V9,
        reliance_reason=row["decision_reason"], original_evidence_unchanged=True,
        paper_order_count=len(cockpit["orders"]), paper_open_count=cockpit["summary"]["open"],
        trade_monitor_state=trade_monitor["state"], trade_monitor_version=trade_monitor["strategy_version"],
        cockpit_orders_incomplete=cockpit["evidence"]["orders_incomplete"], cards=cards)
    (Path(__file__).parent / "dashboard_verification.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
