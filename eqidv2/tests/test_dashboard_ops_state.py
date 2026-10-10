from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")


def run_javascript(script: str) -> None:
    if not NODE:
        pytest.skip("Node.js is required to exercise dashboard refresh behavior")
    result = subprocess.run(
        [NODE, "-e", script], cwd=ROOT, text=True, capture_output=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr


def function_source(name: str, next_name: str) -> str:
    source = (ROOT / "log_dashboard_server.py").read_text(encoding="utf-8")
    start = source.index(f"    function {name}(")
    return source[start:source.index(f"    function {next_name}(", start)]


def test_pinned_and_problem_order_survives_sections_and_nested_views() -> None:
    run_javascript("""
      const assert = require('node:assert/strict');
      require('./dashboard_ops_state.js');
      const items = { healthy: {status: {status: 'ok'}}, failed: {status: {status: 'bad'}}, pinned: {status: {status: 'ok'}}, view: {status: {status: 'warn'}} };
      const order = DashboardOps.orderIds(['healthy', 'view', 'failed', 'pinned'], items, {pinned: true}, true, s => s);
      assert.deepEqual(order, ['pinned', 'failed', 'view', 'healthy']);
      const group = DashboardOps.inOrder(order, ['healthy', 'view', 'failed', 'pinned']);
      assert.deepEqual(group, order);
      assert.deepEqual(DashboardOps.inOrder(group, ['healthy', 'failed']), ['failed', 'healthy']);
      assert.deepEqual(DashboardOps.orderIds(['healthy', 'failed'], items, {}, false, s => s), ['healthy', 'failed']);
    """)


def test_refresh_restores_reading_position_details_controls_and_focus() -> None:
    run_javascript("""
      const assert = require('node:assert/strict');
      require('./dashboard_ops_state.js');
      function card(id, top, height, open, value) {
        const pre = {tagName: 'PRE', scrollTop: top, scrollHeight: height, clientHeight: 100, scrollLeft: 7};
        const detail = {open};
        const input = {tagName: 'INPUT', type: 'search', value, selectionStart: 1, selectionEnd: 3, selectionDirection: 'forward', focus() { this.focused = true; }, setSelectionRange(start, end) { this.restoredSelection = [start, end]; }};
        const select = {tagName: 'SELECT', value: 'ABC', options: [{value: 'ABC'}]};
        const node = {dataset: {id}, scrollTop: 9, scrollLeft: 2, scrollHeight: 500, clientHeight: 200,
          childNodes: [pre, detail, input, select], contains: el => el === input,
          querySelectorAll: selector => selector === 'details' ? [detail] : selector === 'input, select, textarea' ? [input, select] : [pre]};
        node.childNodes.forEach(child => child.parentNode = node);
        return {node, pre, detail, input, select};
      }
      const reading = card('reading', 75, 1000, true, 'ticker');
      const following = card('following', 900, 1000, false, '');
      const oldCards = {querySelectorAll: () => [reading.node, following.node]};
      const doc = {activeElement: reading.input, getSelection: () => null};
      const saved = DashboardOps.capture(oldCards, doc);
      const replacement = card('reading', 0, 1300, false, '');
      const newFollowing = card('following', 0, 1200, true, '');
      DashboardOps.restore({querySelectorAll: () => [replacement.node, newFollowing.node]}, saved, doc);
      assert.equal(replacement.pre.scrollTop, 75);
      assert.equal(replacement.pre.scrollLeft, 7);
      assert.equal(newFollowing.pre.scrollTop, 1200);
      assert.equal(replacement.detail.open, true);
      assert.equal(newFollowing.detail.open, false);
      assert.equal(replacement.input.value, 'ticker');
      assert.equal(replacement.input.focused, true);
      assert.deepEqual(replacement.input.restoredSelection, [1, 3]);
    """)


def test_old_disabled_completed_closed_and_historical_outputs_are_not_stale_alerts() -> None:
    functions = "\n".join([
        function_source("isReadOnlyProfileView", "renderHealthSummary"),
        function_source("parseLocalDate", "formatAge"),
        function_source("formatAge", "outputFreshness"),
        function_source("outputFreshness", "compactNextRun"),
    ])
    run_javascript(functions + """
      const assert = require('node:assert/strict');
      const old = '2020-01-01T10:00:00';
      for (const status of [{status:'DISABLED'}, {status:'SUCCESS'}, {status:'SCHEDULED'}, {status:'RUNNING', phase:'MARKET_CLOSED'}, {status:'RUNNING', scheduler_state:'DISABLED'}, {status:'SUCCESS', view_scope:'ARTIFACT'}]) {
        const age = outputFreshness({status}, old);
        assert.equal(age.monitored, false);
        assert.equal(age.cls, '');
      }
      assert.equal(outputFreshness({card_kind:'view', status:{status:'RUNNING'}}, old).monitored, false);
      assert.equal(outputFreshness({status:{status:'RUNNING'}}, old).cls, 'bad');
      assert.equal(outputFreshness({status:{status:'RUNNING'}}, new Date().toISOString()).cls, 'ok');
    """)


def test_selected_log_text_survives_appended_output() -> None:
    run_javascript("""
      const assert = require('node:assert/strict');
      require('./dashboard_ops_state.js');
      function card(text) {
        const node = {dataset:{id:'log'}, querySelectorAll: () => [], contains:() => false, scrollTop:0, scrollLeft:0, scrollHeight:100, clientHeight:100};
        const textNode = {nodeType:3, textContent:text, parentNode:node, parentElement:node};
        node.closest = () => node; node.childNodes = [textNode];
        return {node, textNode};
      }
      function range(start, begin, end, finish) {
        return {startContainer:start, startOffset:begin, endContainer:end, endOffset:finish,
          setStart(node, offset) { this.startContainer=node; this.startOffset=offset; },
          setEnd(node, offset) { this.endContainer=node; this.endOffset=offset; },
          toString() { return this.startContainer.textContent.slice(this.startOffset,this.endOffset); }};
      }
      const old = card('before selected after');
      const selected = range(old.textNode, 7, old.textNode, 15);
      const selection = {isCollapsed:false, rangeCount:1, getRangeAt:() => selected, removeAllRanges() {}, addRange(value) {this.restored=value;}};
      const doc = {activeElement:null, getSelection:() => selection, createRange:() => range(null,0,null,0)};
      const saved = DashboardOps.capture({querySelectorAll:() => [old.node]}, doc);
      const updated = card('before selected after NEW OUTPUT');
      DashboardOps.restore({querySelectorAll:() => [updated.node]}, saved, doc);
      assert.equal(selection.restored.toString(), 'selected');
      assert.equal(selection.restored.startContainer, updated.textNode);
    """)


def test_failed_refresh_keeps_last_successful_cards_and_displays_age() -> None:
    source = (ROOT / "log_dashboard_server.py").read_text(encoding="utf-8")
    start = source.index("    let SNAPSHOT_LOAD_IN_FLIGHT = false;")
    end = source.index("\n    applyTheme();", start)
    refresh_source = source[start:end]
    run_javascript("""
      const assert = require('node:assert/strict');
      const elements = {cards: {innerHTML:'last good cards'}, refreshNotice: {hidden:true}, info: {textContent:'last updated'}};
      const document = {getElementById: id => elements[id]};
      let LAST_SUCCESSFUL_REFRESH = '2026-10-10 12:00:00';
      const apiUrl = path => path;
      const fetch = async () => { throw new Error('Network unavailable'); };
    """ + refresh_source + """
      loadNow().then(() => {
        assert.equal(elements.cards.innerHTML, 'last good cards');
        assert.equal(elements.refreshNotice.hidden, false);
        assert.match(elements.refreshNotice.textContent, /2026-10-10 12:00:00/);
        assert.match(elements.refreshNotice.textContent, /last successful snapshot/);
        assert.equal(SNAPSHOT_LOAD_IN_FLIGHT, false);
      });
    """)
