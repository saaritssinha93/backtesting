from __future__ import annotations

import json
import re
from pathlib import Path

import log_dashboard_server as dashboard
from ai_platform.observability.strategy_research import REPORT_FILES


CARD_IDS = tuple(REPORT_FILES)


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


def test_exact_eight_card_report_contract_and_latest_only_resolution(
    tmp_path: Path, monkeypatch,
) -> None:
    assert dashboard.V13_V10_G_RESEARCH_CARD_REPORTS == REPORT_FILES
    assert len(CARD_IDS) == 8
    latest = tmp_path / "latest"
    latest.mkdir()
    monkeypatch.setattr(dashboard, "V13_V10_G_STRATEGY_RESEARCH_LATEST_DIR", latest)
    for card_id, filename in REPORT_FILES.items():
        resolved, display = dashboard.resolve_log_target(card_id)
        assert resolved == latest / filename
        assert display == str(Path("v13_v10_g_strategy_research") / "latest" / filename)
        assert not resolved.exists()
        assert dashboard._research_artifact_status(resolved)["status"] == "WAITING_OUTPUT"
        resolved.write_text("# evidence\n", encoding="utf-8")
        status = dashboard._research_artifact_status(resolved)
        assert status["status"] == "READY"
        assert status["view_scope"] == "ARTIFACT"
        assert status["execution_mode"] == "RESEARCH_ONLY"
        assert status["execution_authority"] is False


def test_views_are_strictly_read_only_and_not_operational_sessions() -> None:
    source = _source()
    timeline = source[source.index("const SESSION_TIMELINE") : source.index("const API_TOKEN")]
    js_restartable = _javascript_set(source, "RESTARTABLE_CARDS")
    for card_id in CARD_IDS:
        assert card_id in dashboard.LOG_IDS
        assert card_id not in dashboard.LOG_FILES
        assert card_id not in dashboard.STATUS_FILES
        assert card_id not in dashboard.HEARTBEAT_FILES
        assert card_id not in dashboard.CARD_TASK_NAMES
        assert card_id not in dashboard.RESTARTABLE_CARDS
        assert card_id not in js_restartable
        assert card_id not in timeline
        assert all(card_id not in ids for _, ids in dashboard.FNO_EQ_ID_MONITOR_GROUPS)
        result = dashboard._restart_card_session(card_id)
        assert result["ok"] is False
        assert result["message"] == "Session is not restartable."


def test_frontend_has_named_subheading_titles_filters_and_markdown_views() -> None:
    source = _source()
    order = _javascript_array(source, "LOG_ORDER")
    titles_match = re.search(r"const LOG_TITLES = (\{.*?\});", source, re.DOTALL)
    assert titles_match is not None
    titles = json.loads(titles_match.group(1))
    md_cards = _javascript_set(source, "MD_REPORT_CARDS")
    research_group = source[
        source.index('key: "research"') : source.index('key: "v16"')
    ]
    assert 'title: "V13-V10-G Strategy Research & Prediction"' in research_group
    assert "8 read-only evidence views" in research_group
    assert 'id.startsWith("fno_v13_v10_g_research_")' in source
    assert 'scope === "PROFILE" || scope === "ARTIFACT"' in source
    for card_id in CARD_IDS:
        assert order.count(card_id) == 1
        assert card_id in titles
        assert card_id in md_cards
        assert research_group.count(f'"{card_id}"') == 2


def test_report_bundle_mapping_matches_generator_without_importing_live_code() -> None:
    generator_source = Path(
        __file__
    ).parents[1].joinpath("ai_platform", "observability", "strategy_research.py").read_text(
        encoding="utf-8"
    )
    assert "import fno_v5_live" not in generator_source
    assert "backtesting_result_v13_v10_g_daily" not in generator_source
    assert "place_order" not in generator_source
