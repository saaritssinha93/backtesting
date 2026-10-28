"""Provenance, version and inventory guards for same-profile history merging."""
import pandas as pd
import pytest

from research import g46_full_history_20261026 as history


def seal(directory):
    history.write(directory / "manifest.json", dict(files={
        p.relative_to(directory).as_posix(): history.sha(p)
        for p in directory.rglob("*") if p.is_file() and p.name != "manifest.json"}))


@pytest.fixture
def sources(tmp_path, monkeypatch):
    data = tmp_path / "data"
    config = history.g4.configuration()
    calendar = pd.bdate_range(end="2026-10-26", periods=58).strftime("%Y-%m-%d").tolist()
    g3 = data / "g3_calendar"
    g3.mkdir(parents=True)
    history.write(g3 / "summary.json", dict(days=calendar))
    history.write(g3 / "manifest.json", dict(fixture=True))
    root = data / "backtesting_result_v13_v10_g_4"
    paths = [root / "history", root / "daily"]
    for n, (path, cohort) in enumerate(zip(paths, [calendar[:-1], calendar[-1:]])):
        path.mkdir(parents=True)
        history.write(path / "config.json", config)
        history.write(path / "summary.json", dict(variant_id=config["variant_id"],
            selected_policy="g4_bounded_exit", execution_authority=False,
            complete=True, days=cohort))
        day = cohort[-1]
        row = dict(sid=1, day=day, setup_id="1001_LONG", tradingsymbol="STOCK",
                   side="LONG", filled=True, entry_ts=day+" 10:01:00+05:30",
                   exit_ts=day+" 11:01:00+05:30", confirmation_ts=day+" 10:00:00+05:30",
                   entry_price=100., exit_price=101. if n == 0 else 99.5,
                   gross_return_pct=1. if n == 0 else -.5,
                   net_return_pct=.95 if n == 0 else -.55, cost_pct=.05,
                   initial_stop_pct=1.2, exit_reason="TIME_EXIT_1515")
        ledger, _ = history.allocate(pd.DataFrame([row]), config)
        pd.DataFrame([row]).to_csv(path / "selected_orders.csv", index=False)
        ledger_path = path / ("g4_bounded_exit/portfolio_trades.csv" if n == 0 else "portfolio_trades.csv")
        ledger_path.parent.mkdir(exist_ok=True)
        ledger.to_csv(ledger_path, index=False)
        seal(path)
    monkeypatch.setattr(history, "DATA", data)
    monkeypatch.setattr(history, "G3", g3)
    monkeypatch.setattr(history, "G3_MANIFEST", history.sha(g3 / "manifest.json"))
    monkeypatch.setattr(history, "SOURCES", {4: ["history", "daily"]})
    monkeypatch.setattr(history.g4_daily, "verify", lambda path: True)
    return paths, calendar, config


def test_same_profile_packages_merge_all_candidates_and_replay_allocation(sources):
    paths, calendar, config = sources
    before = {str(p / "manifest.json"): history.sha(p / "manifest.json") for p in paths}
    result = history.run(4)
    assert result["days"] == calendar
    assert result["result"]["selected_orders"] == result["result"]["trades"] == 2
    assert result["all_source_allocation_parity"]
    assert result["result"]["net_pnl_rs"] == pytest.approx(2000.)
    output = history.DATA / "backtesting_result_v13_v10_g_4/runs/2026-10-26/20261026_g4_verified_full_history"
    book = pd.read_csv(output / "portfolio_trades.csv")
    assert book.sid.is_unique and book.source_sid.tolist() == [1, 1]
    assert book.source_package.nunique() == 2
    assert history.verify(output)
    assert history.read(output / "config.json") == config
    assert before == {str(p / "manifest.json"): history.sha(p / "manifest.json") for p in paths}


@pytest.mark.parametrize("problem,match", [
    ("version", "different strategy configurations"),
    ("config", "different strategy configurations"),
    ("policy", "entry policy differs"),
    ("overlap", "Overlapping session packages"),
    ("incomplete", "Daily source is incomplete"),
    ("calendar_gap", "complete G3 calendar"),
    ("lost_candidate", "lost selected candidates"),
])
def test_merge_rejects_incompatible_or_incomplete_sources(sources, problem, match):
    paths, calendar, _ = sources
    target = paths[1]
    summary = history.read(target / "summary.json")
    if problem == "version":
        summary["variant_id"] = "ANOTHER_VERSION"
    elif problem == "config":
        config = history.read(target / "config.json")
        config["leverage"] = 4.
        history.write(target / "config.json", config)
    elif problem == "policy":
        summary["selected_policy"] = "g4_unapproved_policy"
    elif problem == "overlap":
        summary["days"] = [calendar[-2]]
    elif problem == "incomplete":
        summary["complete"] = False
    elif problem == "calendar_gap":
        first = history.read(paths[0] / "summary.json")
        first["days"] = calendar[:-2]
        # Retain the first package's existing trade within its revised calendar.
        for filename in ["selected_orders.csv", "g4_bounded_exit/portfolio_trades.csv"]:
            frame = pd.read_csv(paths[0] / filename)
            frame["day"] = calendar[-3]
            frame.to_csv(paths[0] / filename, index=False)
        history.write(paths[0] / "summary.json", first)
        seal(paths[0])
    elif problem == "lost_candidate":
        frame = pd.read_csv(target / "selected_orders.csv")
        frame.iloc[:0].to_csv(target / "selected_orders.csv", index=False)
    history.write(target / "summary.json", summary)
    seal(target)
    with pytest.raises(ValueError, match=match):
        history.run(4)


def test_changed_source_bytes_fail_manifest_validation(sources):
    paths, _, _ = sources
    with (paths[0] / "config.json").open("a", encoding="utf-8") as stream:
        stream.write(" ")
    with pytest.raises(ValueError, match="Source package member changed"):
        history.run(4)


def test_portfolio_parity_detects_changed_money_or_execution(sources):
    paths, _, _ = sources
    original = pd.read_csv(paths[1] / "portfolio_trades.csv")
    altered = original.copy()
    altered.loc[0, "portfolio_net_profit_rupees"] += 1.
    with pytest.raises(AssertionError, match="allocation parity"):
        history.check_allocation(altered, original)
    altered = original.copy()
    altered.loc[0, "portfolio_executed"] = False
    with pytest.raises(ValueError, match="execution allocation differs"):
        history.check_allocation(altered, original)


def test_manifest_member_cannot_escape_package(tmp_path):
    package = tmp_path / "package"
    package.mkdir()
    external = tmp_path / "outside.csv"
    external.write_text("sensitive", encoding="utf-8")
    history.write(package / "manifest.json", dict(files={"../outside.csv": history.sha(external)}))
    with pytest.raises(ValueError, match="Source package member changed"):
        history.package(package)
