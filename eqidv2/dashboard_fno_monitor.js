/* Read-only stock evidence viewer. No order, strategy or task mutations. */
(function () {
  "use strict";
  const PAGE_SIZE = 50;
  const state = {host: null, token: "", date: "", stock: "", slot: "", side: "", result: "", stage: "5m",
    page: 0, expanded: "", data: null, error: "", loading: false, request: 0, loadedAt: 0, controller: null,
    focus: null, scroll: 0, detailScroll: 0};
  const esc = (value) => String(value == null ? "" : value).replace(/[&<>"']/g, (c) =>
    ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
  const label = (name) => String(name).replace(/_/g, " ");
  function valueText(value) {
    if (value === null || value === undefined || value === "") return "Not recorded";
    if (typeof value === "boolean") return value ? "true" : "false";
    if (typeof value === "number") return Number.isFinite(value) ? String(Number(value.toFixed(5))) : "Not recorded";
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  }
  function pill(value) {
    const name = String(value || "UNKNOWN").toUpperCase();
    const cls = name === "PASS" ? "pass" : name === "FAIL" ? "fail" : "unknown";
    return `<span class="fno-detail-pill ${cls}">${esc(name)}</span>`;
  }
  function checks(row) { return Array.isArray(row.checks) ? row.checks : []; }
  const stageChecks = ["confirmation_stage", "final_selection"];
  const checkStatus = (check) => String(check.status || "UNKNOWN").toUpperCase();
  const checkLabel = (check) => check.label || label(check.name || "Check");
  function checkText(check, formatted, raw) {
    return check[formatted] !== undefined && check[formatted] !== null && check[formatted] !== "" ?
      String(check[formatted]) : valueText(check[raw]);
  }
  function checkMargin(check) {
    if (check.margin_text) return String(check.margin_text);
    return check.margin !== undefined && check.margin !== null ? `Recorded margin: ${valueText(check.margin)}` : "";
  }
  function renderCheckIssues(row) {
    const relevant = checks(row).filter((check) => !stageChecks.includes(check.name));
    const groups = [["FAIL", "Failed", "fail"], ["UNKNOWN", "Missing / unverified evidence", "unknown"],
      ["NOT_EVALUATED", "Not evaluated", "unknown"]];
    const sections = groups.map(([status, heading, style]) => {
      const items = relevant.filter((check) => checkStatus(check) === status);
      if (!items.length) return "";
      return `<div class="fno-check-group ${style}"><strong>${heading} (${items.length})</strong><ul>${items.map((check) => {
        const margin = checkMargin(check);
        const note = [status !== "FAIL" ? check.reason : "", check.evidence_note].filter(Boolean).join(" ");
        return `<li><span class="fno-check-name">${esc(checkLabel(check))}</span>: ${esc(checkText(check, "actual_text", "actual"))}
          <span class="fno-check-required">Required: ${esc(checkText(check, "required_text", "rule"))}</span>
          ${margin ? `<span class="fno-check-margin">${esc(margin)}</span>` : ""}
          ${note ? `<span class="fno-check-note">${esc(note)}</span>` : ""}</li>`;
      }).join("")}</ul></div>`;
    }).filter(Boolean);
    if (!sections.length) sections.push(`<span class="fno-check-note">${relevant.length ?
      "No recorded filter failures or missing checks." : "No per-check evidence recorded."}</span>`);
    const later = checks(row).filter((check) => stageChecks.includes(check.name) &&
      !["PASS", "NOT_APPLICABLE"].includes(checkStatus(check)));
    if (later.length) sections.push(`<div class="fno-check-stage">${later.map((check) =>
      `<div>${esc(checkLabel(check))}: ${esc(check.name === "final_selection" && checkStatus(check) === "FAIL" ?
        "Not selected (separate from filter checks)" : label(checkStatus(check)))}${check.reason ? ` — ${esc(check.reason)}` : ""}</div>`).join("")}</div>`);
    return sections.join("");
  }
  function outcome(row) {
    // A later confirmation and ranking/selection are separate stages, not
    // failed indicator filters. Their recorded outcomes remain in details.
    const statuses = checks(row).filter((check) => !stageChecks.includes(check.name)).map(checkStatus);
    if (statuses.includes("FAIL")) return "FAIL";
    if (!statuses.length || statuses.includes("UNKNOWN")) return "UNKNOWN";
    if (statuses.includes("NOT_EVALUATED")) return "NOT_EVALUATED";
    return statuses.includes("PASS") ? "PASS" : "NOT_APPLICABLE";
  }
  function rows() {
    if (!state.data || state.data.session_date !== state.date) return [];
    const selected = state.stage === "5m" ? state.data.rows_5m : state.data.rows_1m;
    return Array.isArray(selected) ? selected : [];
  }
  function filteredRows() {
    const query = state.stock.trim().toUpperCase();
    return rows().filter((row) => (!query || String(row.symbol || "").toUpperCase().includes(query)) &&
      (!state.slot || row.signal_time === state.slot) && (!state.side || row.side === state.side) &&
      (!state.result || outcome(row) === state.result));
  }
  function selectOptions(values, selected) {
    return values.map(([key, text]) => `<option value="${esc(key)}"${key === selected ? " selected" : ""}>${esc(text)}</option>`).join("");
  }
  function metric(row, patterns) {
    const indicators = row.indicators || {};
    for (const pattern of patterns) {
      const key = Object.keys(indicators).find((name) => name.toLowerCase() === pattern);
      if (key !== undefined) return valueText(indicators[key]);
    }
    return "—";
  }
  function renderControls() {
    const host = state.host;
    if (!host || !host.isConnected) return;
    const focused = document.activeElement;
    const saved = focused && host.contains(focused) && focused.dataset.filter ?
      {filter: focused.dataset.filter, start: focused.selectionStart, end: focused.selectionEnd} : null;
    const slots = [...new Set(rows().map((row) => row.signal_time).filter(Boolean))].sort();
    host.querySelector("[data-zone=controls]").innerHTML = `
      <label>Session date (IST)<input aria-label="Monitoring session date" data-filter="date" type="date" value="${esc(state.date)}"></label>
      <label>Stage<select aria-label="Monitoring stage" data-filter="stage">${selectOptions([["5m", "5-minute signals"], ["1m", "1-minute confirmations & entry events"]], state.stage)}</select></label>
      <label>Stock<input aria-label="Monitoring stock" data-filter="stock" type="search" placeholder="e.g. BHEL" value="${esc(state.stock)}"></label>
      <label>5m signal slot<select aria-label="Monitoring signal slot" data-filter="slot">${selectOptions([["", "All signal slots"], ...slots.map((slot) => [slot, slot])], state.slot)}</select></label>
      <label>Side<select aria-label="Monitoring side" data-filter="side">${selectOptions([["", "Both sides"], ["LONG", "LONG"], ["SHORT", "SHORT"]], state.side)}</select></label>
      <label>Gate result<select aria-label="Monitoring gate result" data-filter="result">${selectOptions([["", "All results"], ["PASS", "All recorded checks pass"], ["FAIL", "One or more checks fail"], ["UNKNOWN", "Unknown / missing evidence"], ["NOT_EVALUATED", "Not evaluated yet"]], state.result)}</select></label>
      <button type="button" data-action="refresh">Refresh evidence</button>
      <button type="button" data-action="reset">Clear filters</button>`;
    if (saved) restoreFocus(saved);
  }
  function renderDetail(row) {
    if (!state.host || !state.host.isConnected) return;
    const host = state.host.querySelector("[data-zone=detail]");
    if (!row) { host.innerHTML = ""; host.hidden = true; return; }
    host.hidden = false;
    const indicatorRows = Object.entries(row.indicators || {}).map(([key, value]) =>
      `<div class="fno-detail-indicator"><span>${esc(label(key))}</span>${esc(valueText(value))}</div>`).join("");
    const checkRows = checks(row).map((check) => {
      const provenance = [check.reason, check.threshold_source ? `Threshold source: ${check.threshold_source}` : "",
        check.margin_source ? `Margin source: ${check.margin_source}` : "", check.evidence_note].filter(Boolean);
      return `<tr><td>${esc(checkLabel(check))}</td><td>${pill(check.status)}</td>
        <td>${esc(checkText(check, "actual_text", "actual"))}</td><td>${esc(checkText(check, "required_text", "rule"))}</td>
        <td>${esc(checkMargin(check) || "Not recorded / not applicable")}</td><td>${provenance.map((text) => `<div>${esc(text)}</div>`).join("")}</td></tr>`;
    }).join("");
    host.innerHTML = `<div class="fno-detail-heading"><h4>${esc(row.symbol)} · ${esc(row.side)} · ${esc(row.setup_id)} · ${esc(row.minute || row.signal_time)} IST</h4>
      <button type="button" data-action="close">Close details</button></div>
      <p class="fno-detail-note">${esc(row.stage)} · Decision: ${esc(row.decision)} · Evidence: ${esc(valueText(row.evidence_state))}<br>Source: ${esc(valueText(row.source))}</p>
      <div class="fno-detail-indicators">${indicatorRows || "No indicator values recorded for this event."}</div>
      <table><thead><tr><th>Indicator / guard / filter</th><th>Recorded result</th><th>Observed value</th><th>Required rule</th><th>Shortfall / margin</th><th>Reason / provenance</th></tr></thead>
      <tbody>${checkRows || '<tr><td colspan="6">No per-check evidence was recorded. This does not mean the guards passed.</td></tr>'}</tbody></table>`;
  }
  function renderRows() {
    if (!state.host || !state.host.isConnected) return;
    const all = rows(), selected = filteredRows();
    const pages = Math.max(1, Math.ceil(selected.length / PAGE_SIZE));
    state.page = Math.min(state.page, pages - 1);
    const start = state.page * PAGE_SIZE, visible = selected.slice(start, start + PAGE_SIZE);
    const counts = {PASS: 0, FAIL: 0, UNKNOWN: 0, NOT_EVALUATED: 0, NOT_APPLICABLE: 0};
    selected.forEach((row) => { counts[outcome(row)] += 1; });
    state.host.querySelector("[data-zone=summary]").innerHTML =
      `<span class="fno-detail-stat">${selected.length} / ${all.length} stock/setup rows</span><span class="fno-detail-stat">${new Set(selected.map((row) => row.symbol).filter(Boolean)).size} stocks</span>` +
      Object.entries(counts).filter(([, count]) => count).map(([status, count]) => `<span class="fno-detail-stat">${pill(status)} ${count}</span>`).join("");
    const mode = state.stage === "5m";
    const headers = mode ? ["Price Δ %", "OI Δ %", "5m volume ratio"] : ["1m volume ratio", "Body ratio", "Wick ratio"];
    const body = visible.map((row, idx) => {
      const metrics = mode ? [metric(row, ["price_change_pct", "price change pct", "price change %"]), metric(row, ["oi_change_pct", "oi change pct", "oi change %"]), metric(row, ["volume_ratio", "5m_volume_ratio", "5m volume ratio"])] :
        [metric(row, ["v9_1m_volume_ratio", "one_minute_volume_ratio", "confirmation_volume_ratio", "1m volume ratio"]), metric(row, ["body_ratio", "v9_1m_body_ratio", "body ratio"]), metric(row, ["wick_ratio", "v9_1m_upper_wick_ratio", "v9_1m_lower_wick_ratio", "wick ratio"])];
      return `<tr><td><button type="button" data-row-index="${start + idx}" aria-label="Inspect ${esc(row.symbol)} ${esc(row.side)} ${esc(row.minute || row.signal_time)}">${esc(row.symbol || "Unknown")}</button></td>
        <td>${esc(row.minute || row.signal_time)}</td><td>${esc(row.side || "Unassigned")}</td><td>${esc(row.setup_id || "Not assigned")}</td><td>${esc(row.stage)}</td>
        <td>${pill(outcome(row))}</td><td class="fno-reasons">${esc(row.decision)}</td>${metrics.map((value) => `<td>${esc(value)}</td>`).join("")}
        <td class="fno-reasons fno-check-issues">${renderCheckIssues(row)}</td></tr>`;
    }).join("");
    state.host.querySelector("[data-zone=rows]").innerHTML = selected.length ?
      `<table><thead><tr>${["Stock / details", "Minute IST", "Side", "Setup", "Stage", "Gate result", "Recorded decision", ...headers, "Failed / missing checks"].map((name) => `<th>${esc(name)}</th>`).join("")}</tr></thead><tbody>${body}</tbody></table>` :
      `<div class="fno-detail-empty">${state.loading ? "Loading recorded stock evidence…" : all.length ? "No rows match these filters." : "No stock-level evidence for this date/stage yet. Missing evidence is not a pass; check slot coverage below."}</div>`;
    state.host.querySelector("[data-zone=pager]").innerHTML = `<button type="button" data-action="previous"${state.page === 0 ? " disabled" : ""}>Previous</button>
      <span>Page ${state.page + 1} / ${pages} · ${PAGE_SIZE} rows per page</span><button type="button" data-action="next"${state.page + 1 >= pages ? " disabled" : ""}>Next</button>`;
    renderDetail(selected.find((row) => row.id === state.expanded));
  }
  function renderStatus() {
    const data = state.data, host = state.host;
    if (!host || !host.isConnected) return;
    host.querySelector("[data-zone=status]").textContent = state.error || (state.loading ? "Refreshing recorded evidence…" :
      data ? `Session ${data.session_date} · ${data.state || "UNKNOWN"} · Updated ${data.generated_at_ist || "not recorded"} · auto-refresh with dashboard` : "Loading evidence…");
    const warnings = data && Array.isArray(data.warnings) ? data.warnings : [];
    host.querySelector("[data-zone=warnings]").innerHTML = warnings.length ?
      `<details class="fno-detail-warnings"><summary>Evidence warnings (${warnings.length})</summary><ul>${warnings.map((warning) => `<li>${esc(valueText(warning))}</li>`).join("")}</ul></details>` : "";
    const coverage = data && Array.isArray(data.coverage) ? data.coverage : [];
    const keys = ["Signal", "Scanner", "5m feature rows", "Missing / no-candle", "Confirmation", "1m feature rows", "Durable feed", "Written / candidates", "Feed published (IST)", "Feed deadline (IST)"];
    const coverageCells = (row) => {
      const scanner = row.scanner || {}, confirmation = row.confirmation || {}, feed = confirmation.durable_feed || {};
      return [row.slot, row.scanner_state, row.scanner_rows,
        `${valueText(scanner.unexpected_missing)} / ${valueText(scanner.skipped_no_candle)}`,
        row.confirmation_state, row.confirmation_rows, feed.state,
        `${valueText(feed.written_count)} / ${valueText(feed.candidate_count)}`, feed.published_at_ist, feed.deadline_ist];
    };
    host.querySelector("[data-zone=coverage]").innerHTML = coverage.length ? `<details class="fno-detail-coverage"><summary>5-minute / 1-minute slot coverage (${coverage.length})</summary><div class="fno-detail-grid"><table>
      <thead><tr>${keys.map((key) => `<th>${esc(key)}</th>`).join("")}</tr></thead><tbody>${coverage.map((row) => `<tr>${coverageCells(row).map((value) => `<td>${esc(valueText(value))}</td>`).join("")}</tr>`).join("")}</tbody></table></div></details>` : "";
  }
  async function load(force) {
    if (state.loading && !force) return;
    if (!force && state.data && state.data.session_date === state.date && Date.now() - state.loadedAt < 10000) return;
    if (state.controller) state.controller.abort();
    const request = ++state.request, date = state.date;
    state.controller = new AbortController();
    state.loading = true; state.error = ""; renderStatus();
    const timeout = setTimeout(() => state.controller && request === state.request && state.controller.abort(), 20000);
    try {
      const url = new URL("/api/fno-monitor", window.location.origin);
      url.searchParams.set("date", date);
      if (state.token) url.searchParams.set("token", state.token);
      const response = await fetch(url, {method: "GET", credentials: "same-origin", cache: "no-store", signal: state.controller.signal});
      const data = await response.json();
      if (request !== state.request || date !== state.date) return;
      if (!response.ok) throw new Error(data.error || `Evidence request failed (${response.status})`);
      if (data.session_date !== date) throw new Error("Evidence session mismatch; refusing to display another day's rows.");
      state.data = data; state.loadedAt = Date.now(); state.loading = false;
      renderControls(); renderStatus(); renderRows();
    } catch (error) {
      if (request !== state.request) return;
      // Do not continue showing a cached PASS table as current after a failed refresh.
      state.data = null; state.error = error.name === "AbortError" ? "Evidence refresh timed out. Use Refresh evidence to retry." : error.message;
      state.loading = false; renderStatus(); renderRows();
    } finally { clearTimeout(timeout); }
  }
  function restoreFocus(saved) {
    if (!state.host || !saved) return;
    const element = state.host.querySelector(`[data-filter="${saved.filter}"]`);
    if (!element) return;
    element.focus({preventScroll: true});
    if (saved.filter === "stock" && saved.start !== null) element.setSelectionRange(saved.start, saved.end);
  }
  function beforeRefresh() {
    const host = state.host, focused = document.activeElement;
    state.focus = host && focused && host.contains(focused) && focused.dataset.filter ?
      {filter: focused.dataset.filter, start: focused.selectionStart, end: focused.selectionEnd} : null;
    if (host && host.isConnected) {
      state.scroll = host.querySelector("[data-zone=rows]").scrollTop;
      state.detailScroll = host.querySelector("[data-zone=detail]").scrollTop;
    }
  }
  function mount(host, token) {
    if (!host) { state.host = null; return; }
    state.host = host; state.token = token || "";
    if (!state.date) state.date = new Intl.DateTimeFormat("en-CA", {timeZone: "Asia/Kolkata", year: "numeric", month: "2-digit", day: "2-digit"}).format(new Date());
    host.innerHTML = `<div class="fno-detail-heading"><h3>Stock-level signals, confirmations & guards</h3><span class="fno-readonly">READ ONLY · V13-V10-G</span></div>
      <p class="fno-detail-note">Recorded live evidence, not a replay or a strategy change. Click a stock for actual indicator values, required thresholds, gate results and source details. A passed filter is not a selected order or a fill.</p>
      <div class="fno-detail-controls" data-zone="controls"></div><div class="fno-detail-note" data-zone="status" role="status" aria-live="polite"></div>
      <div data-zone="warnings"></div><div class="fno-detail-summary" data-zone="summary"></div><div class="fno-detail-grid" data-zone="rows"></div>
      <div class="fno-detail-pager" data-zone="pager"></div><div class="fno-detail-expanded" data-zone="detail" hidden></div><div data-zone="coverage"></div>
      <p class="fno-detail-note">PASS / FAIL are recorded checks; UNKNOWN means evidence is unavailable. NOT EVALUATED is a later stage, not rejection. Entry events are only the published events—not continuous one-minute mark-to-market history. Times are IST.</p>`;
    renderControls(); renderStatus(); renderRows();
    host.querySelector("[data-zone=rows]").scrollTop = state.scroll;
    host.querySelector("[data-zone=detail]").scrollTop = state.detailScroll;
    restoreFocus(state.focus); state.focus = null;
    host.addEventListener("input", (event) => {
      if (event.target.dataset.filter !== "stock") return;
      state.stock = event.target.value; state.page = 0; state.expanded = ""; renderRows();
    });
    host.addEventListener("change", (event) => {
      const filter = event.target.dataset.filter;
      if (!["date", "stage", "slot", "side", "result"].includes(filter)) return;
      state[filter] = event.target.value; state.page = 0; state.expanded = "";
      if (filter === "date") { state.data = null; state.slot = ""; renderControls(); renderRows(); load(true); }
      else { if (filter === "stage") { state.slot = ""; renderControls(); } renderRows(); }
    });
    host.addEventListener("click", (event) => {
      const button = event.target.closest("button");
      if (!button || !host.contains(button)) return;
      if (button.dataset.rowIndex !== undefined) {
        const row = filteredRows()[Number(button.dataset.rowIndex)];
        if (row) { state.expanded = row.id; renderDetail(row); host.querySelector("[data-zone=detail]").scrollIntoView({block: "nearest"}); }
        return;
      }
      const action = button.dataset.action;
      if (action === "refresh") load(true);
      else if (action === "reset") { state.stock = state.slot = state.side = state.result = state.expanded = ""; state.page = 0; renderControls(); renderRows(); }
      else if (action === "close") { state.expanded = ""; renderDetail(null); }
      else if (action === "previous" || action === "next") { state.page += action === "next" ? 1 : -1; state.expanded = ""; renderRows(); }
    });
    if (!host.closest(".is-log-hidden")) load(false);
  }
  window.FnoMonitor = {mount, beforeRefresh};
})();
