/* V13 strategy cockpit: read-only source evidence. No broker or control writes. */
(function (root) {
  "use strict";
  const STAGES = ["data", "scanner", "confirmation", "risk", "execution", "exits"];
  const TITLES = {data: "Market data", scanner: "Scanner", confirmation: "Confirmation", risk: "Risk checks", execution: "Execution", exits: "Exit management"};
  const esc = (value) => String(value == null ? "" : value).replace(/[&<>"']/g, (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
  const array = (value) => Array.isArray(value) ? value : [];
  function number(value) { return !["string", "number"].includes(typeof value) || typeof value === "string" && !value.trim() ? null : Number.isFinite(Number(value)) ? Number(value) : null; }
  function display(value) { return value === null || value === undefined || value === "" ? "Not recorded" : typeof value === "object" ? JSON.stringify(value) : String(value); }
  function money(value) { const n = number(value); return n === null ? "—" : `${n < 0 ? "−" : ""}₹${Math.abs(n).toLocaleString("en-IN", {minimumFractionDigits: 2, maximumFractionDigits: 2})}`; }
  function count(value) { const n = number(value); return n === null ? "—" : n.toLocaleString("en-IN"); }
  function tone(value) { const s = String(value || "UNKNOWN").toUpperCase(); if (["UNKNOWN", "NO_EVIDENCE", "UNAVAILABLE", "INCOMPLETE", "HISTORICAL", "HISTORICAL_SESSION", "NOT_APPLICABLE", "NOT_EVALUATED", "NO_TIMESTAMPED_QUOTE"].includes(s)) return "neutral"; if (["PASS", "HEALTHY", "READY", "COMPLETE", "FILLED", "ACTIVE", "AVAILABLE", "OK", "RECENT", "RECENT_SNAPSHOT", "RECENT_SNAPSHOTS"].includes(s)) return "good"; if (/FAIL|ERROR|REJECT|STALE|BLOCK|MISSING|MISMATCH|CONFLICT/.test(s)) return "bad"; if (/PENDING|WAIT|PART|WARN/.test(s)) return "warn"; return "neutral"; }
  function human(value) { return display(value).replace(/_/g, " "); }
  function outcome(row) { const checks = array(row.checks).filter((c) => !["confirmation_stage", "final_selection"].includes(c.name)); const statuses = checks.map((c) => String(c.status || "UNKNOWN").toUpperCase()); return statuses.includes("FAIL") ? "FAIL" : !statuses.length || statuses.includes("UNKNOWN") ? "UNKNOWN" : statuses.includes("NOT_EVALUATED") ? "NOT_EVALUATED" : statuses.includes("PASS") ? "PASS" : "NOT_APPLICABLE"; }
  function validatePayload(data, date, mode) { if (!data || data.session_date !== date || String(data.mode || "").toUpperCase() !== mode) throw new Error("Response does not match the selected session and trading mode."); return data; }
  function symbolOf(row) { return String(row.symbol || row.ticker || row.tradingsymbol || "").toUpperCase(); }
  function timeOf(row) { return row.timestamp_ist || row.time_ist || row.timestamp || row.at || row.time || row.minute || row.signal_time || row.entry_time || ""; }
  function shortTime(value) { const s = String(value || ""); const m = s.match(/(?:T|\s)(\d\d:\d\d(?::\d\d)?)/); return m ? m[1] : s || "—"; }
  function todayIST() { const p = new Intl.DateTimeFormat("en-CA", {timeZone: "Asia/Kolkata", year: "numeric", month: "2-digit", day: "2-digit"}).formatToParts(new Date()); const get = (k) => p.find((x) => x.type === k).value; return `${get("year")}-${get("month")}-${get("day")}`; }
  function badge(value) { return `<span class="cockpit-badge ${tone(value)}">${esc(human(value || "UNKNOWN"))}</span>`; }
  function empty(title, description) { return `<div class="cockpit-empty"><span class="cockpit-empty-glyph" aria-hidden="true">⌁</span><strong>${esc(title)}</strong><p>${esc(description)}</p></div>`; }
  function svgChart(chart) {
    const candles = array(chart.candles).map((c) => ({t: c.timestamp_ist || c.timestamp || c.time || c.datetime, o: number(c.open), h: number(c.high), l: number(c.low), c: number(c.close)})).filter((c) => [c.o, c.h, c.l, c.c].every((n) => n !== null) && c.h >= c.l);
    const points = array(chart.observations).map((c) => ({t: c.timestamp_ist || c.timestamp || c.time, p: number(c.price ?? c.ltp ?? c.close), label: String(c.label || "Recorded observation")})).filter((c) => c.p !== null).slice(-150);
    const candleMode = candles.length > 0;
    const entries = candleMode ? candles.slice(-100) : points;
    if (!entries.length) return empty("No price history available", chart.message || chart.note || "Select a stock with recorded candle or price evidence. Missing prices are never drawn as a flat market.");
    const entryTimesValid = entries.every((c) => Number.isFinite(Date.parse(c.t)));
    // A fill can sit outside the available candle range. Expand the real time
    // and price axes to include it; never fabricate intervening candles.
    const overlays = candleMode && entryTimesValid ? points.filter((c) => Number.isFinite(Date.parse(c.t))) : [];
    const allEntries = [...entries, ...overlays], times = allEntries.map((e) => Date.parse(e.t));
    const timeScale = times.every(Number.isFinite), startTime = timeScale ? Math.min(...times) : null, endTime = timeScale ? Math.max(...times) : null;
    const values = candleMode ? [...entries.flatMap((c) => [c.l, c.h]), ...overlays.map((c) => c.p)] : entries.map((c) => c.p);
    const lower = Math.min(...values), upper = Math.max(...values), pad = Math.max((upper - lower) * 0.12, upper * 0.0005, 0.01), lo = lower - pad, hi = upper + pad;
    const W = 720, H = 270, left = 14, right = 66, top = 22, bottom = 34, width = W - left - right, height = H - top - bottom;
    const timeX = (t) => endTime > startTime ? left + 5 + (t - startTime) / (endTime - startTime) * (width - 10) : left + width / 2;
    const x = (i) => timeScale ? timeX(times[i]) : left + (i + 0.5) * width / entries.length, y = (v) => top + (hi - v) / (hi - lo) * height;
    const candleSpacing = entries.slice(1).map((_, i) => Math.abs(x(i + 1) - x(i))).filter((gap) => gap > 0);
    const candleWidth = Math.max(0.7, Math.min(10, (candleSpacing.length ? Math.min(...candleSpacing) : width / entries.length) * 0.55));
    let shape = "";
    for (let i = 0; i < 5; i++) { const value = lo + (hi - lo) * i / 4, yy = y(value); shape += `<line x1="${left}" y1="${yy}" x2="${W - right}" y2="${yy}" class="cockpit-chart-grid"/><text x="${W - right + 9}" y="${yy + 4}" class="cockpit-chart-label">${esc(value.toFixed(2))}</text>`; }
    if (candleMode) entries.forEach((c, i) => { const xx = x(i), w = candleWidth, col = c.c >= c.o ? "up" : "down"; shape += `<g class="cockpit-candle ${col}"><title>${esc(display(c.t))} · O ${c.o} H ${c.h} L ${c.l} C ${c.c}</title><line x1="${xx}" y1="${y(c.h)}" x2="${xx}" y2="${y(c.l)}"/><rect x="${xx - w / 2}" y="${Math.min(y(c.o), y(c.c))}" width="${w}" height="${Math.max(1, Math.abs(y(c.o) - y(c.c)))}"/></g>`; });
    else { entries.forEach((c, i) => { shape += `<circle cx="${x(i)}" cy="${y(c.p)}" r="${entries.length > 30 ? 2 : 3}" class="cockpit-price-point"><title>${esc(c.label)} · ${esc(display(c.t))} · ${c.p}</title></circle>`; }); }
    overlays.forEach((point, i) => {
      const kind = /^entry fill$/i.test(point.label) ? "entry" : /^exit fill$/i.test(point.label) ? "exit" : "quote";
      const xx = timeX(Date.parse(point.t)), yy = y(point.p), rightEdge = xx > left + width * 0.78;
      const label = kind === "entry" ? "Entry" : kind === "exit" ? "Exit" : "Quote";
      shape += `<g class="cockpit-observation ${kind}" tabindex="0" aria-label="${esc(point.label)} at ${esc(display(point.t))}, price ${point.p}"><title>${esc(point.label)} · ${esc(display(point.t))} · ${point.p}</title><circle cx="${xx}" cy="${yy}" r="4.5"/><text x="${xx + (rightEdge ? -8 : 8)}" y="${yy + (i % 2 ? 15 : -9)}" text-anchor="${rightEdge ? "end" : "start"}">${label}</text></g>`;
    });
    const first = timeScale ? allEntries[times.indexOf(startTime)] : entries[0], last = timeScale ? allEntries[times.indexOf(endTime)] : entries[entries.length - 1];
    shape += `<text x="${left}" y="${H - 10}" class="cockpit-chart-label">${esc(shortTime(first.t))}</text><text x="${W - right}" y="${H - 10}" text-anchor="end" class="cockpit-chart-label">${esc(shortTime(last.t))} IST</text>`;
    const omitted = candleMode ? points.length - overlays.length : 0;
    return `<div class="cockpit-chart-caption">${candleMode ? "Recorded candles" : "Recorded price observations · irregular sampling"} <span>${entries.length} ${candleMode ? "candles" : "points"}${overlays.length ? ` · ${overlays.length} fill / quote markers` : ""}</span></div><svg class="cockpit-chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(chart.symbol || "Selected stock")} ${candleMode ? "recorded candles with recorded fill and quote markers" : "recorded price observations"}">${shape}</svg>${overlays.length ? '<div class="cockpit-chart-legend"><span class="entry">● Entry fill</span><span class="exit">● Exit fill</span><span class="quote">● Timestamped quote</span><span>Gaps are preserved; markers do not add candles.</span></div>' : ""}${omitted ? `<div class="cockpit-chart-legend">${omitted} observation markers omitted because their timestamps could not be aligned to the candle timeline.</div>` : ""}`;
  }
  function mount(options) {
    const host = typeof options.host === "string" ? document.querySelector(options.host) : options.host;
    if (!host) return {refresh() {}, setPaused() {}, destroy() {}};
    if (host.__cockpit) host.__cockpit.destroy();
    const state = {date: todayIST(), mode: "PAPER", stock: "", query: "", stage: "scanner", data: null, detail: null, selected: null, paused: false, loading: false, error: "", loaded: 0, request: 0, controller: null, destroyed: false, expanded: new Set()};
    const apiUrl = typeof options.apiUrl === "function" ? options.apiUrl : (path) => path;
    host.classList.add("dashboard-cockpit");
    host.innerHTML = `<div class="cockpit-mast"><div><div class="cockpit-eyebrow"><span class="cockpit-brand-mark" aria-hidden="true">◈</span> EQID / STRATEGY TERMINAL <span class="cockpit-readonly">READ ONLY</span></div><h2><b data-zone="strategy-title">Strategy</b> <span>Live & paper cockpit</span></h2><p>Follow the evidence. Understand every trade.</p></div><div class="cockpit-controls"><div class="cockpit-modes" role="group" aria-label="Trading mode"><button type="button" data-mode="PAPER" aria-pressed="true">Paper</button><button type="button" data-mode="LIVE" aria-pressed="false">Live</button></div><label class="cockpit-date-label">Session · IST<input type="date" data-field="date" aria-label="Cockpit session date" value="${state.date}" max="${state.date}"></label><button type="button" class="cockpit-icon-button" data-action="refresh" title="Refresh cockpit" aria-label="Refresh cockpit">↻</button></div></div>
      <div class="cockpit-context" data-zone="context"></div><div class="cockpit-status" data-zone="status" role="status" aria-live="polite"></div><div class="cockpit-metrics" data-zone="metrics"></div>
      <section class="cockpit-panel cockpit-pipeline-panel"><div class="cockpit-panel-head"><div><span class="cockpit-kicker">DECISION PATH</span><h3>Strategy pipeline</h3></div><span class="cockpit-subtle">Select a stage to inspect evidence</span></div><div class="cockpit-pipeline" data-zone="pipeline"></div><div class="cockpit-stage-note" data-zone="stage-note"></div></section>
      <div class="cockpit-workspace"><section class="cockpit-panel cockpit-evidence-panel"><div class="cockpit-panel-head"><div><span class="cockpit-kicker">RECORDED DECISIONS</span><h3 data-zone="evidence-title">Scanner evidence</h3></div><button type="button" class="cockpit-text-button" data-action="load-evidence">Inspect checks ↗</button></div><div class="cockpit-filterbar"><label><span class="cockpit-search-icon" aria-hidden="true">⌕</span><input type="search" data-field="query" aria-label="Filter cockpit stocks" placeholder="Filter by stock…"></label><button type="button" data-action="clear" class="cockpit-text-button">Clear</button></div><div class="cockpit-evidence-list" data-zone="evidence"></div></section>
      <section class="cockpit-panel cockpit-chart-panel"><div class="cockpit-panel-head"><div><span class="cockpit-kicker">PRICE EVIDENCE</span><h3 data-zone="chart-title">Select a stock</h3></div><div class="cockpit-chart-tools"><select data-field="symbol" aria-label="Selected cockpit stock"><option value="">Select a stock</option></select><span class="cockpit-badge neutral" data-zone="chart-state">NOT LOADED</span></div></div><div data-zone="chart"></div><div class="cockpit-chart-note" data-zone="chart-note"></div></section></div>
      <div class="cockpit-bottom"><section class="cockpit-panel cockpit-positions-panel"><div class="cockpit-panel-head"><div><span class="cockpit-kicker">EXPOSURE & EXIT RULES</span><h3>Recorded positions</h3></div><span class="cockpit-subtle" data-zone="position-count"></span></div><div class="cockpit-table-scroll" data-zone="positions"></div></section><section class="cockpit-panel cockpit-events-panel"><div class="cockpit-panel-head"><div><span class="cockpit-kicker">AUDIT TRAIL</span><h3>Execution events</h3></div><span class="cockpit-subtle">Recorded · IST</span></div><div class="cockpit-events" data-zone="events"></div></section></div>
      <section class="cockpit-panel cockpit-comparison" data-zone="comparison"></section><section class="cockpit-panel cockpit-inspector" data-zone="inspector" hidden></section><div class="cockpit-footnote" data-zone="footnote"></div>`;
    const zone = (name) => host.querySelector(`[data-zone="${name}"]`);
    const rowsFor = () => {
      const d = state.data || {}, e = state.detail || d.evidence || {};
      if (state.stage === "scanner") return array(e.rows_5m);
      if (state.stage === "confirmation") return array(e.rows_1m);
      if (state.stage === "risk") return [...array(d.orders).filter((r) => /BLOCK/.test(r.status) || r.last_blocker || r.first_blocker), ...array(e.rows_5m), ...array(e.rows_1m)].filter((r) => r.last_blocker || r.first_blocker || /BLOCK/.test(r.status) || array(r.checks).some((c) => /guard|risk|stop|capital|quantity|exposure|selection/i.test(c.name || c.label || "")));
      if (state.stage === "execution") return array(d.orders).length ? array(d.orders) : array(d.events);
      if (state.stage === "exits") return array(d.orders).filter((r) => ["OPEN", "SQUARE_OFF_PENDING", "CLOSED"].includes(r.status));
      return array(e.coverage);
    };
    const filtered = (rows) => rows.filter((r) => !state.query || symbolOf(r).includes(state.query.toUpperCase()));
    const eventRows = () => filtered(array((state.data || {}).events)).slice().reverse();
    function renderStatus() {
      const d = state.data, evidence = d && d.evidence || {};
      zone("status").classList.toggle("is-error", !!state.error);
      zone("status").innerHTML = `<span class="cockpit-status-dot ${state.error ? "bad" : state.loading ? "loading" : ""}"></span><span>${esc(state.error || (state.loading ? "Refreshing recorded session data…" : state.paused ? "Auto-refresh paused" : d ? `Refreshed ${shortTime(d.generated_at_ist)} IST · Latest order snapshot ${evidence.updated_at_ist ? shortTime(evidence.updated_at_ist) + " IST" : "not recorded"}` : "Waiting for source data"))}</span>${d ? badge(evidence.state) : ""}${d && d.session_clock ? badge(d.session_clock.phase) : ""}${state.error && d ? '<span class="cockpit-badge warn">LAST SUCCESSFUL SNAPSHOT</span>' : ""}`;
      host.querySelector('[data-action="refresh"]').classList.toggle("is-loading", state.loading);
    }
    function renderMetrics() {
      const d = state.data || {}, s = d.summary || {}, pnl = number(s.total_pnl_rs), open = number(s.open);
      const metrics = [
        ["Total P&L", money(s.total_pnl_rs), pnl === null ? "Unavailable until all marks are known" : `${state.mode} · selected session`, pnl === null ? "" : pnl < 0 ? "negative" : "positive"],
        ["Realized P&L", money(s.realized_pnl_rs), `${count(s.closed)} recorded closed trades`, number(s.realized_pnl_rs) < 0 ? "negative" : ""],
        ["Unrealized P&L", money(s.unrealized_pnl_rs), number(s.unrealized_pnl_rs) === null ? "Fresh price evidence required" : "Recorded position marks", number(s.unrealized_pnl_rs) < 0 ? "negative" : ""],
        ["Open positions", count(s.open), open === null ? "Position state unavailable" : `${count(s.pending)} pending · selected mode`, ""],
        ["Signals", count(s.signals), "Recorded signals · not fills", ""],
        ["Win rate", number(s.win_rate_pct) === null ? "—" : `${number(s.win_rate_pct).toFixed(1)}%`, "Closed trades in this mode", ""]];
      zone("metrics").innerHTML = metrics.map(([label, val, note, cls]) => `<div class="cockpit-metric"><div class="cockpit-metric-label">${label}</div><div class="cockpit-metric-value ${cls}">${esc(val)}</div><div class="cockpit-metric-note">${esc(note)}</div></div>`).join("");
    }
    function renderPipeline() {
      const d = state.data || {}, stages = array(d.stages);
      const units = {data: "feature rows", scanner: "candidates", confirmation: "selections", risk: "blockers", execution: "open", exits: "closed"};
      zone("pipeline").innerHTML = STAGES.map((key, i) => { const s = stages.find((x) => (x.key || x.id) === key) || {}; return `<button type="button" class="cockpit-stage ${key === state.stage ? "is-selected" : ""} ${tone(s.status || s.state)}" data-stage="${key}" aria-pressed="${key === state.stage}"><span class="cockpit-stage-top"><span class="cockpit-stage-index">0${i + 1}</span><span class="cockpit-stage-light" aria-hidden="true"></span></span><strong>${TITLES[key]}</strong><span class="cockpit-stage-count">${count(s.count)}<small>${esc(s.unit || units[key])}</small></span><span class="cockpit-stage-state">${esc(human(s.status || s.state || "UNKNOWN"))}</span></button>`; }).join("");
      const stage = stages.find((s) => (s.key || s.id) === state.stage) || {};
      zone("stage-note").innerHTML = `<strong>${TITLES[state.stage]}</strong><span>${esc(stage.detail || stage.note || "No verified stage evidence is available for this session.")}</span>`;
    }
    function renderEvidence() {
      zone("evidence-title").textContent = `${TITLES[state.stage]}${["scanner", "confirmation", "risk"].includes(state.stage) && (state.data || {}).selected_symbol ? ` · ${state.data.selected_symbol}` : " evidence"}`;
      const rows = filtered(rowsFor()), isChecks = ["scanner", "confirmation", "risk"].includes(state.stage);
      if (!rows.length) { zone("evidence").innerHTML = empty(state.query ? "No matching stock evidence" : "No recorded rows", state.loading ? "Loading source evidence…" : isChecks && !state.detail && !(state.data || {}).evidence?.rows_5m ? "Use Inspect checks to load the stock-level indicator and guard evidence." : "Choose another session or stage. An empty record is not a passed check."); return; }
      zone("evidence").innerHTML = rows.slice(0, 70).map((row, idx) => { const sym = symbolOf(row), status = isChecks && Array.isArray(row.checks) ? outcome(row) : row.status || row.state || row.scanner_state || "RECORDED", fail = array(row.checks).filter((c) => String(c.status).toUpperCase() === "FAIL"); return `<button type="button" class="cockpit-evidence-row${sym && sym === state.stock ? " is-selected" : ""}" data-row="${idx}"><span class="cockpit-evidence-row-top"><strong>${esc(sym || row.slot || "Session record")}</strong><time>${esc(shortTime(timeOf(row) || row.slot))}</time></span><span class="cockpit-evidence-row-meta">${badge(status)}<span>${esc(row.side || row.setup_id || row.type || "")}</span></span><span class="cockpit-row-reason">${esc(fail.length ? fail.map((c) => c.label || human(c.name)).join(" · ") : row.decision || row.reason || row.last_blocker || row.message || row.note || "Inspect recorded source values")}</span></button>`; }).join("") + (rows.length > 70 ? `<div class="cockpit-limit-note">Showing 70 of ${rows.length} records. Filter by stock to narrow the view.</div>` : "");
    }
    function renderPositions() {
      const rows = filtered(array((state.data || {}).positions));
      zone("position-count").textContent = `${rows.length} displayed · ${state.mode}`;
      if (!rows.length) { zone("positions").innerHTML = empty("No recorded positions", "Position records will appear when the selected session has execution evidence."); return; }
      zone("positions").innerHTML = `<table><thead><tr><th>Stock / side</th><th>State</th><th>Qty</th><th>Entry</th><th>Recorded mark</th><th>Stop / tightening</th><th>Target</th><th>P&L</th></tr></thead><tbody>${rows.slice(0, 100).map((p, i) => { const pnl = number(p.pnl_rs ?? p.unrealized_pnl_rs ?? p.realized_pnl_rs); return `<tr><td><button type="button" class="cockpit-stock-button" data-position="${i}">${esc(symbolOf(p) || "Unknown")}</button><small>${esc(p.side || "")}</small></td><td>${badge(p.status || p.state || "UNKNOWN")}<small>${esc(human(p.evidence_state || "UNKNOWN"))}</small></td><td>${count(p.partial_exit_pnl_note ? p.remaining_quantity : (p.quantity ?? p.qty))}${p.partial_exit_pnl_note ? "<small>remaining</small>" : ""}</td><td>${money(p.entry_price)}</td><td>${money(p.mark_price ?? p.last_price ?? p.ltp)}<small>${esc(human(p.quote_state || "UNKNOWN"))}</small></td><td>${money(p.stop_price ?? p.stop_loss ?? p.sl_price)}<small>${esc(p.stop_tighten_status || p.stop_note || p.stop_stage || "")}${p.stop_tighten_due_at_ist ? ` · ${esc(shortTime(p.stop_tighten_due_at_ist))} IST` : ""}</small></td><td>${money(p.target_price ?? p.target)}</td><td class="${pnl === null ? "" : pnl < 0 ? "negative" : "positive"}">${money(pnl)}</td></tr>`; }).join("")}</tbody></table>`;
    }
    function renderEvents() {
      const rows = eventRows();
      zone("events").innerHTML = rows.length ? rows.slice(0, 80).map((e, i) => `<button type="button" class="cockpit-event" data-event="${i}"><time>${esc(shortTime(timeOf(e)))}</time><span class="cockpit-event-dot ${tone(e.state_after || e.status || e.state || e.event_type || e.type)}"></span><span><strong>${esc(symbolOf(e) || "Session")} <em>${esc(human(e.event_type || e.type || e.event || e.status || "EVENT"))}</em></strong><span>${esc([e.state_before && e.state_after ? `${e.state_before} → ${e.state_after}` : e.state_after, e.message || e.reason || e.detail || e.to_state].filter(Boolean).join(" · ") || "Recorded execution event")}</span></span></button>`).join("") : empty("No execution events recorded", "Signals, submitted orders and confirmed fills are shown as separate events when the source records them.");
    }
    function renderChart() {
      const chart = (state.data || {}).chart || {};
      const selector = host.querySelector('[data-field="symbol"]'), symbols = array((state.data || {}).symbols);
      const optionsHTML = symbols.length ? symbols.map((s) => `<option value="${esc(s)}"${s === (state.stock || chart.symbol) ? " selected" : ""}>${esc(s)}</option>`).join("") : '<option value="">No recorded stocks</option>';
      if (selector.innerHTML !== optionsHTML) selector.innerHTML = optionsHTML;
      zone("chart-title").textContent = chart.symbol || state.stock || "Select a stock";
      zone("chart-state").textContent = human(chart.state || "UNAVAILABLE");
      zone("chart-state").className = `cockpit-badge ${tone(chart.state)}`;
      zone("chart").innerHTML = svgChart(chart);
      zone("chart-note").textContent = chart.note || chart.source ? [chart.note, chart.source ? `Source: ${display(chart.source)}` : ""].filter(Boolean).join(" · ") : "Select any recorded stock to inspect its available price history. Times are IST.";
    }
    function renderComparison() {
      const comparison = (state.data || {}).comparison, rows = filtered(Array.isArray(comparison) ? comparison : array(comparison && comparison.rows));
      const pct = (value) => number(value) === null ? "—" : `${number(value).toFixed(2)}%`;
      zone("comparison").innerHTML = `<details data-disclosure="compare"${state.expanded.has("compare") ? " open" : ""}><summary><span><span class="cockpit-kicker">PAPER VS LIVE</span><strong>Compare execution on the same signals</strong></span><span class="cockpit-subtle">${rows.length} signals · expand</span></summary><p class="cockpit-comparison-note">Closed-trade price returns use entry and exit prices before costs and account for trade direction. Quantities are shown separately. Entry difference is the raw LIVE minus PAPER quote, in basis points; it is not measured execution slippage or latency.</p>${rows.length ? `<div class="cockpit-table-scroll"><table><thead><tr><th>Stock / side</th><th>Paper state</th><th>Live state</th><th>Qty paper / live</th><th>Paper price return</th><th>Live price return</th><th>Entry difference</th></tr></thead><tbody>${rows.slice(0, 150).map((row) => { const diff = number(row.entry_difference_bps); return `<tr><td><strong>${esc(symbolOf(row) || "Unknown")}</strong><small>${esc(row.side || "")} · ${esc(human(row.state || "UNKNOWN"))}</small></td><td>${badge(row.paper_status || "NO_EVIDENCE")}</td><td>${badge(row.live_status || "NO_EVIDENCE")}</td><td>${count(row.paper_qty)} / ${count(row.live_qty)}</td><td>${pct(row.paper_return_pct)}</td><td>${pct(row.live_return_pct)}</td><td>${diff === null ? "—" : `${diff.toFixed(2)} bps`}<small class="cockpit-comparison-source">${esc(row.note || "")}</small></td></tr>`; }).join("")}</tbody></table></div>` : empty("No comparable execution records", "A matched signal and verified prices are required. A missing live fill remains unknown.")}</details>`;
    }
    function inspect(row, kind) {
      if (!row) return;
      state.selected = {row, kind};
      const symbol = symbolOf(row), checks = array(row.checks), fields = Object.entries(row).filter(([k, v]) => !["checks", "indicators"].includes(k) && v !== null && v !== undefined && v !== "");
      const box = zone("inspector"); box.hidden = false;
      box.innerHTML = `<div class="cockpit-panel-head"><div><span class="cockpit-kicker">SOURCE INSPECTOR</span><h3>${esc(symbol || "Session evidence")} <span class="cockpit-subtle">${esc(kind)}</span></h3></div><button type="button" data-action="close" class="cockpit-text-button">Close ×</button></div><div class="cockpit-inspector-body">${checks.length ? `<div class="cockpit-table-scroll"><table><thead><tr><th>Check</th><th>Result</th><th>Observed</th><th>Required</th><th>Reason / provenance</th></tr></thead><tbody>${checks.map((c) => `<tr><td>${esc(c.label || human(c.name))}</td><td>${badge(c.status)}</td><td>${esc(display(c.actual_text ?? c.actual))}</td><td>${esc(display(c.required_text ?? c.rule))}</td><td>${esc([c.reason, c.margin_text, c.evidence_note, c.threshold_source].filter(Boolean).join(" · ") || "Not recorded")}</td></tr>`).join("")}</tbody></table></div>` : ""}<dl class="cockpit-source-fields">${fields.map(([k, v]) => `<div><dt>${esc(human(k))}</dt><dd>${esc(display(v))}</dd></div>`).join("")}${Object.entries(row.indicators || {}).map(([k, v]) => `<div><dt>${esc(human(k))}</dt><dd>${esc(display(v))}</dd></div>`).join("")}</dl><p class="cockpit-inspector-note">Recorded checks, selections, submitted orders and fills are distinct evidence. Missing values remain unknown.</p></div>`;
      box.scrollIntoView({block: "nearest", behavior: "auto"});
      if (symbol && symbol !== state.stock) { state.stock = symbol; refresh(true); }
      if (typeof options.onInspect === "function") options.onInspect(row);
    }
    function render() {
      const d = state.data || {}, identity = d.identity || {}, sizing = d.sizing || {}, warnings = [...array((d.evidence || {}).warnings), ...array(d.warnings)];
      if (d.comparison_incomplete) warnings.push("Paper/live comparison sources are incomplete or contain invalid records; missing comparisons remain unknown.");
      zone("strategy-title").textContent = identity.label || identity.profile || "Strategy";
      const contextHTML = `<div><span class="cockpit-mode-tag ${state.mode === "LIVE" ? "live" : ""}">${state.mode}</span><span>${esc(identity.label || identity.profile || "Identity awaiting evidence")}</span>${identity.version ? `<span class="cockpit-policy">${esc(identity.version)}</span>` : ""}<span class="cockpit-subtle">${esc(sizing.label || "Sizing from recorded execution state")}</span></div><label class="cockpit-recorded-dates">Recorded sessions <select data-field="recorded-date" aria-label="Recorded cockpit sessions"><option value="">Choose session</option>${array(d.available_dates).map((date) => `<option value="${esc(date)}">${esc(date)}</option>`).join("")}</select></label>`;
      if (zone("context").innerHTML !== contextHTML) zone("context").innerHTML = contextHTML;
      renderStatus(); renderMetrics(); renderPipeline(); renderEvidence(); renderPositions(); renderEvents(); renderChart(); renderComparison();
      zone("footnote").innerHTML = `<div><strong>Evidence, not assumptions.</strong> P&L is reported separately for ${state.mode.toLowerCase()} execution across the selected session; the stock selector changes evidence and chart only. Unknown values appear as —. ${esc(sizing.details || "Paper and live quantities may differ; rupee P&L is not a like-for-like performance comparison.")}</div>${d.session_clock ? `<div>${esc(d.session_clock.note)}</div>` : ""}${identity.policy ? `<details data-disclosure="policy"${state.expanded.has("policy") ? " open" : ""}><summary>Active strategy policy</summary><pre>${esc(typeof identity.policy === "object" ? JSON.stringify(identity.policy, null, 2) : identity.policy)}</pre></details>` : ""}${warnings.length ? `<details class="cockpit-warning-details" data-disclosure="sources"${state.expanded.has("sources") ? " open" : ""}><summary>Source notes (${warnings.length})</summary><ul>${warnings.map((w) => `<li>${esc(display(w))}</li>`).join("")}</ul></details>` : ""}`;
      if (identity.configured_live_execution_policy) {
        zone("footnote").innerHTML += `<details data-disclosure="live-policy"${state.expanded.has("live-policy") ? " open" : ""}><summary>Configured LIVE sizing policy</summary><p>This separate execution policy sets LIVE quantities. The recorded strategy policy above preserves decision and backtest provenance.</p><pre>${esc(JSON.stringify(identity.configured_live_execution_policy, null, 2))}</pre></details>`;
      }
    }
    async function refresh(force) {
      if (state.destroyed || (state.paused && !force) || (state.loading && !force)) return;
      if (state.controller) state.controller.abort();
      const controller = new AbortController(), request = ++state.request, date = state.date, mode = state.mode;
      state.controller = controller; state.loading = true; state.error = ""; renderStatus();
      const timeout = setTimeout(() => controller.abort(), 25000);
      try {
        const query = new URLSearchParams({date, mode}); if (state.stock) query.set("symbol", state.stock);
        const response = await fetch(apiUrl(`/api/cockpit?${query}`), {credentials: "same-origin", cache: "no-store", signal: controller.signal});
        if (!response.ok) throw new Error(response.status === 401 ? "Dashboard authentication required." : `Cockpit source unavailable (HTTP ${response.status}).`);
        const data = validatePayload(await response.json(), date, mode);
        if (request !== state.request || state.destroyed) return;
        state.data = data; state.loaded = Date.now(); state.error = "";
        if (!state.stock && data.chart && data.chart.symbol) state.stock = data.chart.symbol;
      } catch (error) {
        if (request !== state.request || state.destroyed) return;
        state.error = error.name === "AbortError" ? "Refresh timed out. Retry to reconnect to recorded data." : error.message;
      } finally { clearTimeout(timeout); if (request === state.request && !state.destroyed) { state.loading = false; render(); } }
    }
    function resetSelection() { state.data = null; state.detail = null; state.stock = ""; state.selected = null; zone("inspector").hidden = true; render(); refresh(true); }
    function click(event) {
      const button = event.target.closest("button"); if (!button || !host.contains(button)) return;
      if (button.dataset.mode) { if (state.mode === button.dataset.mode) return; state.mode = button.dataset.mode; host.querySelectorAll("[data-mode]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.mode === state.mode))); resetSelection(); return; }
      if (button.dataset.stage) { state.stage = button.dataset.stage; renderPipeline(); renderEvidence(); return; }
      if (button.dataset.row !== undefined) { inspect(filtered(rowsFor())[Number(button.dataset.row)], TITLES[state.stage]); return; }
      if (button.dataset.position !== undefined) { inspect(filtered(array((state.data || {}).positions))[Number(button.dataset.position)], "Position evidence"); return; }
      if (button.dataset.event !== undefined) { inspect(eventRows()[Number(button.dataset.event)], "Execution event"); return; }
      if (button.dataset.action === "refresh") refresh(true);
      if (button.dataset.action === "load-evidence") { const first = filtered(rowsFor())[0]; if (first) inspect(first, TITLES[state.stage]); else refresh(true); }
      if (button.dataset.action === "close") { state.selected = null; zone("inspector").hidden = true; }
      if (button.dataset.action === "clear") { state.query = ""; host.querySelector('[data-field="query"]').value = ""; renderEvidence(); renderPositions(); renderEvents(); renderComparison(); }
    }
    function change(event) {
      const field = event.target.dataset.field, value = event.target.value;
      if (field === "symbol") { state.stock = value; state.selected = null; zone("inspector").hidden = true; refresh(true); return; }
      if (!["date", "recorded-date"].includes(field) || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return;
      state.date = value; host.querySelector('[data-field="date"]').value = value; resetSelection();
    }
    function input(event) { if (event.target.dataset.field === "query") { state.query = event.target.value; renderEvidence(); renderPositions(); renderEvents(); renderComparison(); } }
    function toggle(event) { const key = event.target.dataset.disclosure; if (key) { if (event.target.open) state.expanded.add(key); else state.expanded.delete(key); } }
    host.addEventListener("click", click); host.addEventListener("change", change); host.addEventListener("input", input); host.addEventListener("toggle", toggle, true);
    const instance = {refresh: (force = false) => refresh(force), setPaused: (paused) => { state.paused = !!paused; renderStatus(); }, destroy: () => { state.destroyed = true; state.request++; if (state.controller) state.controller.abort(); host.removeEventListener("click", click); host.removeEventListener("change", change); host.removeEventListener("input", input); host.removeEventListener("toggle", toggle, true); delete host.__cockpit; }};
    host.__cockpit = instance; render(); refresh(true); return instance;
  }
  const api = {mount};
  if (typeof module === "object" && module.exports) module.exports = {number, money, count, outcome, validatePayload, esc, svgChart, tone, mount};
  if (root) root.DashboardCockpit = api;
})(typeof window === "undefined" ? null : window);
