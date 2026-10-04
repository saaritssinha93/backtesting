from __future__ import annotations

import json
import re
from pathlib import Path

import log_dashboard_server as dashboard
from ai_platform.observability.strategy_research import OBSERVABILITY_REPORT_FILES


EXPECTED_REPORTS = {
    "fno_v13_v10_g_observability_market_regime":
        "latest_fno_v13_v10_g_observability_market_regime.md",
    "fno_v13_v10_g_observability_selection_funnel":
        "latest_fno_v13_v10_g_observability_selection_funnel.md",
    "fno_v13_v10_g_observability_entry_execution":
        "latest_fno_v13_v10_g_observability_entry_execution.md",
    "fno_v13_v10_g_observability_live_finalized_drift":
        "latest_fno_v13_v10_g_observability_live_finalized_drift.md",
    "fno_v13_v10_g_observability_pnl_attribution":
        "latest_fno_v13_v10_g_observability_pnl_attribution.md",
    "fno_v13_v10_g_observability_regime_profitability":
        "latest_fno_v13_v10_g_observability_regime_profitability.md",
}

EXPECTED_TITLES = {
    "fno_v13_v10_g_observability_market_regime": "V13-V10-G Market Regime",
    "fno_v13_v10_g_observability_selection_funnel": "V13-V10-G Selection Funnel",
    "fno_v13_v10_g_observability_entry_execution": "V13-V10-G Entry and Execution",
    "fno_v13_v10_g_observability_live_finalized_drift": "V13-V10-G Live vs Finalized Drift",
    "fno_v13_v10_g_observability_pnl_attribution": "V13-V10-G P&L Attribution",
    "fno_v13_v10_g_observability_regime_profitability": "V13-V10-G Regime Profitability",
}


def _source() -> str:
    return Path(dashboard.__file__).read_text(encoding="utf-8", errors="strict")


def _javascript_array(source: str, name: str) -> list[str]:
    match = re.search(rf"const\s+{name}\s*=\s*\[(.*?)\];", source, re.DOTALL)
    assert match is not None
    return re.findall(r'"([^"]+)"', match.group(1))


def _javascript_set(source: str, name: str) -> set[str]:
    match = re.search(
        rf"const\s+{name}\s*=\s*new Set\(\[(.*?)\]\);", source, re.DOTALL
    )
    assert match is not None
    return set(re.findall(r'"([^"]+)"', match.group(1)))


def test_exact_six_observability_card_contract_and_resolution(
    tmp_path: Path, monkeypatch,
) -> None:
    assert OBSERVABILITY_REPORT_FILES == EXPECTED_REPORTS
    assert dashboard.V13_V10_G_OBSERVABILITY_CARD_REPORTS == EXPECTED_REPORTS
    latest = tmp_path / "latest"
    latest.mkdir()
    monkeypatch.setattr(dashboard, "V13_V10_G_STRATEGY_RESEARCH_LATEST_DIR", latest)
    for card_id, filename in EXPECTED_REPORTS.items():
        assert dashboard.LOG_IDS.count(card_id) == 1
        resolved, display = dashboard.resolve_log_target(card_id)
        assert resolved == latest / filename
        assert display == str(Path("v13_v10_g_strategy_research") / "latest" / filename)
        missing = dashboard._research_artifact_status(resolved)
        assert missing == {
            "status": "WAITING_OUTPUT",
            "phase": "READ_ONLY_REPORT",
            "view_scope": "ARTIFACT",
            "execution_mode": "RESEARCH_ONLY",
            "execution_authority": False,
            "derived_status": (
                "Run bat\\run_v13_strategy_research_refresh.bat after a complete "
                "historical bundle exists."
            ),
        }
        resolved.write_text("# evidence\n", encoding="utf-8")
        ready = dashboard._research_artifact_status(resolved)
        assert ready["status"] == "READY"
        assert ready["execution_authority"] is False


def test_observability_cards_are_strictly_read_only() -> None:
    source = _source()
    timeline = source[source.index("const SESSION_TIMELINE") : source.index("const API_TOKEN")]
    kill_scope = source[source.index("const KILL_CARD_SCOPE") : source.index("function applyTheme")]
    js_restartable = _javascript_set(source, "RESTARTABLE_CARDS")
    for card_id in EXPECTED_REPORTS:
        assert card_id not in dashboard.LOG_FILES
        assert card_id not in dashboard.STATUS_FILES
        assert card_id not in dashboard.HEARTBEAT_FILES
        assert card_id not in dashboard.CARD_TASK_NAMES
        assert card_id not in dashboard.RESTARTABLE_CARDS
        assert card_id not in js_restartable
        assert card_id not in timeline
        assert card_id not in kill_scope
        assert all(card_id not in ids for _, ids in dashboard.FNO_EQ_ID_MONITOR_GROUPS)
        assert dashboard._restart_card_session(card_id) == {
            "ok": False,
            "message": "Session is not restartable.",
        }


def test_frontend_places_exact_six_cards_under_observability_subheading() -> None:
    source = _source()
    order = _javascript_array(source, "LOG_ORDER")
    titles_match = re.search(r"const LOG_TITLES = (\{.*?\});", source, re.DOTALL)
    assert titles_match is not None
    titles = json.loads(titles_match.group(1))
    markdown_cards = _javascript_set(source, "MD_REPORT_CARDS")
    research_group = source[source.index('key: "research"') : source.index('key: "v16"')]
    research_subgroup = research_group[
        research_group.index('key: "v13-v10-g-strategy-research-prediction"') :
        research_group.index('key: "observability"')
    ]
    observability_subgroup = research_group[research_group.index('key: "observability"') :]
    assert 'title: "Observability"' in observability_subgroup
    assert "6 read-only observability views | no trading controls" in observability_subgroup
    assert 'id.startsWith("fno_v13_v10_g_observability_")' in source
    assert 'scope === "PROFILE" || scope === "ARTIFACT"' in source
    for card_id, title in EXPECTED_TITLES.items():
        assert order.count(card_id) == 1
        assert titles[card_id] == title
        assert card_id in markdown_cards
        assert research_group.count(f'"{card_id}"') == 2
        assert observability_subgroup.count(f'"{card_id}"') == 1
        assert card_id not in research_subgroup


def test_generator_remains_offline_and_non_executing() -> None:
    generator = Path(__file__).parents[1].joinpath(
        "ai_platform", "observability", "strategy_research.py"
    ).read_text(encoding="utf-8")
    assert "import fno_v5_live" not in generator
    assert "place_order" not in generator
    assert "execution_authority\": False" in generator
