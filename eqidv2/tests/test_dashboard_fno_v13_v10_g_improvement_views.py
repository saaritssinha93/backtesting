from __future__ import annotations

import io
import json
import re
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import log_dashboard_server as dashboard


CARD_ID = "fno_v13_v10_g_research_improvements"
REPORT_NAME = "latest_fno_v13_v10_g_research_improvements.md"


def _source() -> str:
    return Path(dashboard.__file__).read_text(encoding="utf-8")


def _html() -> str:
    output = io.BytesIO()
    handler = SimpleNamespace(
        server=SimpleNamespace(api_token=""),
        send_response=lambda *_: None,
        send_header=lambda *_: None,
        end_headers=lambda: None,
        wfile=output,
    )
    dashboard.LogDashboardHandler._send_html(handler)
    return output.getvalue().decode("utf-8")


def test_improvement_artifact_has_its_own_contract_and_latest_path(
    tmp_path: Path, monkeypatch,
) -> None:
    assert dashboard.V13_V10_G_IMPROVEMENT_CARD_REPORTS == {CARD_ID: REPORT_NAME}
    assert len(dashboard.V13_V10_G_RESEARCH_CARD_REPORTS) == 8
    assert len(dashboard.V13_V10_G_OBSERVABILITY_CARD_REPORTS) == 6
    assert len(dashboard.V13_V10_G_ARTIFACT_CARD_REPORTS) == 15
    assert dashboard.LOG_IDS.count(CARD_ID) == 1
    monkeypatch.setattr(dashboard, "V13_V10_G_STRATEGY_RESEARCH_LATEST_DIR", tmp_path)
    report, display = dashboard.resolve_log_target(CARD_ID)
    assert report == tmp_path / REPORT_NAME
    assert display == str(Path("v13_v10_g_strategy_research") / "latest" / REPORT_NAME)
    assert dashboard._research_artifact_status(report)["status"] == "WAITING_OUTPUT"
    report.write_text("# Improvement opportunities\n", encoding="utf-8")
    status = dashboard._research_artifact_status(report)
    assert status["status"] == "READY"
    assert status["view_scope"] == "ARTIFACT"
    assert status["execution_mode"] == "RESEARCH_ONLY"
    assert status["execution_authority"] is False


def test_improvement_card_has_no_process_or_trading_controls() -> None:
    for mapping in (
        dashboard.LOG_FILES, dashboard.STATUS_FILES, dashboard.HEARTBEAT_FILES,
        dashboard.CARD_TASK_NAMES, dashboard.RESTARTABLE_CARDS,
    ):
        assert CARD_ID not in mapping
    assert dashboard._runtime_status_path_for_card(CARD_ID) is None
    assert dashboard._runtime_heartbeat_path_for_card(CARD_ID) is None
    assert dashboard._restart_card_session(CARD_ID) == {
        "ok": False, "message": "Session is not restartable.",
    }
    assert all(CARD_ID not in ids for _, ids in dashboard.FNO_EQ_ID_MONITOR_GROUPS)
    source = _source()
    for start, end in (
        ("const SESSION_TIMELINE", "const API_TOKEN"),
        ("const KILL_CARD_SCOPE", "function applyTheme"),
    ):
        assert CARD_ID not in source[source.index(start):source.index(end)]
    restartable = re.search(
        r"const RESTARTABLE_CARDS = new Set\(\[(.*?)\]\);", source, re.DOTALL,
    )
    assert restartable and CARD_ID not in restartable.group(1)


def test_named_improvement_subgroup_order_title_and_markdown_rendering() -> None:
    html = _html()
    order = re.search(r"const LOG_ORDER = \[(.*?)\];", html, re.DOTALL)
    assert order and order.group(1).count(f'"{CARD_ID}"') == 1
    titles = re.search(r"const LOG_TITLES = (\{.*?\});", html, re.DOTALL)
    assert titles
    assert json.loads(titles.group(1))[CARD_ID] == "V13 Evidence-Based Improvement Opportunities"
    markdown = re.search(r"const MD_REPORT_CARDS = new Set\(\[(.*?)\]\);", html, re.DOTALL)
    assert markdown and CARD_ID in markdown.group(1)
    group = html[html.index('key: "research"'):html.index('key: "v16"')]
    subgroup = group[group.index('key: "v13-improvement-opportunities"'):]
    assert 'title: "V13 Improvement Opportunities"' in subgroup
    assert re.findall(r'"(fno_[^"]+)"', subgroup) == [CARD_ID]
    assert group.count(f'"{CARD_ID}"') == 2
    assert 'id.startsWith("fno_v13_v10_g_research_")' in html
    assert 'id="card-${esc(id)}"' in html


def test_generic_fallback_does_not_mislabel_unrelated_active_cards() -> None:
    source = _source()
    fallback = source[source.index("const otherActive ="):source.index("const otherDisabled =")]
    assert 'label: "Other Active"' in fallback
    assert 'renderSectionBanner("Other Active"' in fallback
    assert "V13 Research and Observability" not in fallback


def test_evidence_markdown_links_only_allow_known_internal_card_anchors() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to evaluate browser Markdown rendering")
    html = _html()
    order = re.search(r"const LOG_ORDER = \[.*?\];", html, re.DOTALL)
    escape = re.search(r"    function esc\(s\) \{.*?\n    \}", html, re.DOTALL)
    inline = re.search(r"    function mdInline\(text\) \{.*?\n    \}", html, re.DOTALL)
    assert order and escape and inline
    evidence_id = "fno_v13_v10_g_observability_entry_execution"
    anchor = f"#card-{evidence_id}"
    inputs = [
        f"[Entry and Execution]({anchor})",
        f"[<img src=x onerror=alert(1)>]({anchor})",
        "[Unknown](#card-not_a_registered_card)",
        "[External](https://example.com)",
        "[Unsafe](javascript:alert(1))",
        "**Evidence** and `read-only`",
    ]
    script = "\n".join((
        order.group(0), escape.group(0), inline.group(0),
        f"process.stdout.write(JSON.stringify({json.dumps(inputs)}.map(mdInline)));",
    ))
    result = subprocess.run(
        [node, "-e", script], capture_output=True, text=True, check=True, timeout=30,
    )
    rendered = json.loads(result.stdout)
    assert rendered[0] == f'<a href="{anchor}">Entry and Execution</a>'
    assert rendered[1] == f'<a href="{anchor}">&lt;img src=x onerror=alert(1)&gt;</a>'
    assert all("<a " not in item for item in rendered[2:5])
    assert rendered[5] == "<strong>Evidence</strong> and <code>read-only</code>"
