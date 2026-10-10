/* Dashboard Flow: actual cumulative realized P&L, rendered without dependencies. */
(function () {
  'use strict';
  const NS = 'http://www.w3.org/2000/svg';
  const GREEN = 'var(--chart-green)', ROSE = 'var(--chart-rose)', INK = 'var(--ink)';
  const MUTED = 'var(--muted)';
  const chartObservers = new WeakMap();
  let chartSequence = 0;
  function node(tag, attributes, text) {
    const element = document.createElementNS(NS, tag);
    Object.entries(attributes || {}).forEach(([key, value]) => {
      if (value !== undefined && value !== null) element.setAttribute(key, String(value));
    });
    if (text !== undefined) element.textContent = text;
    return element;
  }
  function money(value) {
    const n = Math.abs(value), sign = value < 0 ? '−' : '';
    return sign + '₹' + (n >= 1e7 ? (n / 1e7).toFixed(1) + 'Cr' : n >= 1e5 ? (n / 1e5).toFixed(1) + 'L' : n >= 1e3 ? (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + 'k' : n.toFixed(0));
  }
  function shortDate(value) {
    const text = String(value || '');
    const match = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!match) return text.slice(0, 10);
    return match[3] + ' ' + ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][Number(match[2]) - 1];
  }
  function finite(value) { return value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)); }
  function render(container, stocks, dates, options) {
    if (!container) return;
    options = options || {};
    stocks = Array.isArray(stocks) ? stocks : [];
    dates = Array.isArray(dates) ? dates : [];
    const compact = Boolean(options.compact), selected = options.selected || null;
    // Use screen-size coordinates so labels stay readable at laptop widths.
    // The main chart scrolls inside its panel on narrow screens.
    const styles = getComputedStyle(container);
    const contentWidth = container.clientWidth - parseFloat(styles.paddingLeft || 0) - parseFloat(styles.paddingRight || 0);
    const contentHeight = container.clientHeight - parseFloat(styles.paddingTop || 0) - parseFloat(styles.paddingBottom || 0);
    if (typeof ResizeObserver !== 'undefined') {
      let observed = chartObservers.get(container);
      if (!observed) {
        observed = { pending: false };
        const observer = new ResizeObserver(() => {
          if (observed.pending || !container.clientWidth || !container.clientHeight) return;
          const current = getComputedStyle(container);
          const nextWidth = container.clientWidth - parseFloat(current.paddingLeft || 0) - parseFloat(current.paddingRight || 0);
          const nextHeight = container.clientHeight - parseFloat(current.paddingTop || 0) - parseFloat(current.paddingBottom || 0);
          if (Math.abs(nextWidth - observed.width) < 1 && Math.abs(nextHeight - observed.height) < 1) return;
          observed.pending = true;
          requestAnimationFrame(() => {
            observed.pending = false;
            if (container.isConnected && container.clientWidth) render(container, observed.stocks, observed.dates, observed.options);
          });
        });
        chartObservers.set(container, observed);
        observer.observe(container);
      }
      Object.assign(observed, { width: contentWidth, height: contentHeight, stocks, dates, options });
    }
    const width = Math.max(compact ? 280 : 540, contentWidth || (compact ? 740 : 760));
    const height = Math.max(compact ? 180 : 365, contentHeight || (compact ? 220 : 395));
    const left = 64, right = compact ? width - 30 : width - 185, top = compact ? 37 : 42, bottom = height - 48;
    const spine = right + 20, branchEnd = right + 81, labelLeft = right + 86;
    const axisFont = 11;
    const interactive = typeof options.onSelect === 'function';
    const series = stocks.filter(stock => stock && typeof stock === 'object').map(stock => ({ ...stock, points: Array.isArray(stock.points) ? stock.points : [] }));
    const values = series.flatMap(stock => stock.points.filter(finite).map(Number));
    const svg = node('svg', { viewBox: '0 0 ' + width + ' ' + height, preserveAspectRatio: 'none', role: compact && !interactive ? 'img' : 'group', 'aria-label': compact ? 'Cumulative realized net profit and loss over time' : 'Stock performance flow. Cumulative realized net profit and loss connects to ranked stock outcomes.', style: 'display:block;width:100%;height:100%;min-height:180px;overflow:visible;font-family:inherit;' });
    svg.appendChild(node('rect', { width, height, fill: 'var(--chart-bg)', rx: 12 }));
    container.replaceChildren(svg);
    if (!values.length) {
      svg.appendChild(node('text', { x: width / 2, y: height / 2 - 20, 'text-anchor': 'middle', fill: MUTED, 'font-size': 14 }, 'No trade results in this selection'));
      svg.appendChild(node('text', { x: width / 2, y: height / 2 + 7, 'text-anchor': 'middle', fill: MUTED, 'font-size': 12 }, 'Choose another period or broaden the filters.'));
      return;
    }
    let low = 0, high = 0;
    values.forEach(value => { low = Math.min(low, value); high = Math.max(high, value); });
    const range = high - low || 1;
    high += range * 0.09; low -= range * 0.07;
    const y = value => bottom - ((Number(value) - low) / (high - low)) * (bottom - top);
    const maxLength = Math.max(dates.length, ...series.map(stock => stock.points.length), 1);
    const x = index => left + (maxLength === 1 ? 0.5 : index / (maxLength - 1)) * (right - left);
    const zero = y(0), clipId = 'flow-clip-' + (++chartSequence);
    const defs = node('defs');
    const clip = node('clipPath', { id: clipId });
    clip.appendChild(node('rect', { x: left - 4, y: top - 4, width: right - left + 8, height: bottom - top + 8 }));
    defs.appendChild(clip); svg.appendChild(defs);
    svg.appendChild(node('text', { x: left, y: 23, fill: MUTED, 'font-size': 11, 'letter-spacing': .6, 'font-weight': 600 }, 'CUMULATIVE NET P&L'));
    if (!compact) svg.appendChild(node('text', { x: labelLeft + 37, y: 23, fill: MUTED, 'font-size': 10, 'letter-spacing': .4, 'font-weight': 600, 'text-anchor': 'middle' }, 'OUTCOME FLOW'));
    for (let i = 0; i <= 5; i++) {
      const value = low + ((high - low) * i / 5), yy = y(value);
      svg.appendChild(node('line', { x1: left, x2: right, y1: yy, y2: yy, stroke: 'var(--chart-grid)', 'stroke-width': 0.8, 'stroke-dasharray': '3 5' }));
      svg.appendChild(node('text', { x: left - 10, y: yy + 4, 'text-anchor': 'end', fill: MUTED, 'font-size': axisFont }, money(value)));
    }
    svg.appendChild(node('line', { x1: left, x2: compact ? right : spine + 30, y1: zero, y2: zero, stroke: 'var(--chart-zero)', 'stroke-width': 1.1 }));
    svg.appendChild(node('text', { x: left - 10, y: zero + 4, 'text-anchor': 'end', fill: INK, 'font-size': axisFont, 'font-weight': 600 }, '₹0'));
    const dateCount = Math.min(6, Math.max(2, Math.floor((right - left) / 58)), maxLength);
    const dateIndices = [...new Set(Array.from({ length: dateCount }, (_, i) => Math.round(i * (maxLength - 1) / Math.max(1, dateCount - 1))))];
    dateIndices.forEach(index => {
      const xx = x(index);
      svg.appendChild(node('line', { x1: xx, x2: xx, y1: top, y2: bottom, stroke: 'var(--chart-grid)', 'stroke-width': 0.7, 'stroke-dasharray': '2 6' }));
      svg.appendChild(node('text', { x: xx, y: bottom + 23, 'text-anchor': 'middle', fill: MUTED, 'font-size': axisFont }, shortDate(dates[index])));
    });
    const plotted = series.map(stock => {
      const valid = stock.points.map((value, index) => finite(value) ? { value: Number(value), index } : null).filter(Boolean);
      return { ...stock, valid, last: valid[valid.length - 1] };
    }).filter(stock => stock.last);
    const labelLimit = Math.min(24, Math.floor((bottom - top) / 16) + 1);
    const labelCandidates = plotted.length <= labelLimit ? [...plotted] : [...plotted.slice(0, Math.ceil(labelLimit / 2)), ...plotted.slice(-Math.floor(labelLimit / 2))];
    if (selected && !labelCandidates.some(stock => stock.symbol === selected)) {
      const focused = plotted.find(stock => stock.symbol === selected);
      if (focused) labelCandidates.splice(Math.floor(labelCandidates.length / 2), 1, focused);
    }
    labelCandidates.sort((a, b) => b.last.value - a.last.value);
    const labelPositions = new Map(labelCandidates.map((stock, index) => [stock.symbol, top + 5 + (labelCandidates.length === 1 ? 0.5 : index / (labelCandidates.length - 1)) * (bottom - top - 10)]));
    if (!compact) {
      svg.appendChild(node('rect', { x: spine - 5, y: top - 7, width: 38, height: bottom - top + 14, rx: 6, fill: 'var(--chart-spine)' }));
      svg.appendChild(node('line', { x1: spine, x2: spine, y1: top - 3, y2: bottom + 3, stroke: 'var(--chart-zero)', 'stroke-width': 1 }));
    }
    const lines = node('g', { 'clip-path': 'url(#' + clipId + ')' });
    const endings = node('g');
    const branches = node('g');
    const labels = node('g');
    const maxAbs = Math.max(...plotted.map(stock => Math.abs(stock.last.value)), 1);
    const activate = stock => { if (typeof options.onSelect === 'function') options.onSelect(stock.symbol); };
    const makeInteractive = (element, stock, description) => {
      if (!interactive) return;
      element.setAttribute('tabindex', '0'); element.setAttribute('role', 'button');
      element.setAttribute('aria-label', description);
      element.style.cursor = 'pointer';
      element.addEventListener('click', () => activate(stock));
      element.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); activate(stock); } });
      element.addEventListener('mouseenter', () => { if (typeof options.onHover === 'function') options.onHover(stock.symbol); });
      element.addEventListener('mouseleave', () => { if (typeof options.onHover === 'function') options.onHover(null); });
      element.addEventListener('focus', () => { if (typeof options.onHover === 'function') options.onHover(stock.symbol); });
      element.addEventListener('blur', () => { if (typeof options.onHover === 'function') options.onHover(null); });
    };
    // Focused trajectories are painted last so their actual values stay legible.
    plotted.sort((a, b) => Number(a.symbol === selected) - Number(b.symbol === selected)).forEach(stock => {
      const focus = stock.symbol === selected, color = stock.last.value >= 0 ? GREEN : ROSE;
      const opacity = selected ? (focus ? 1 : 0.13) : plotted.length > 45 ? 0.37 : 0.57;
      let path = '', penDown = false;
      stock.points.forEach((value, index) => {
        if (!finite(value)) { penDown = false; return; }
        path += (penDown ? ' L ' : ' M ') + x(index).toFixed(2) + ' ' + y(value).toFixed(2);
        penDown = true;
      });
      const group = node('g', { opacity });
      const curve = node('path', { d: path.trim(), fill: 'none', stroke: color, 'stroke-width': focus ? 2.7 : 1.05, 'vector-effect': 'non-scaling-stroke', 'stroke-linejoin': 'round', 'stroke-linecap': 'round' });
      const summary = stock.symbol + ': ' + money(stock.last.value) + ' cumulative net P&L, ' + (stock.trades || 0) + ' trades, as of ' + (dates[stock.last.index] || 'last observation');
      const hitArea = node('path', { d: path.trim(), fill: 'none', stroke: 'transparent', 'stroke-width': 10, 'vector-effect': 'non-scaling-stroke' });
      hitArea.appendChild(node('title', {}, summary)); makeInteractive(hitArea, stock, summary);
      group.appendChild(curve); group.appendChild(hitArea); lines.appendChild(group);
      const yy = y(stock.last.value), xx = x(stock.last.index);
      endings.appendChild(node('circle', { cx: xx, cy: yy, r: focus ? 4 : 2.1, fill: color, opacity: selected && !focus ? 0.2 : 0.85, stroke: focus ? 'var(--surface)' : 'none', 'stroke-width': 1.5 }));
      if (compact) return;
      branches.appendChild(node('path', { d: 'M ' + xx + ' ' + yy + ' L ' + spine + ' ' + yy, fill: 'none', stroke: color, 'stroke-width': focus ? 1.8 : 0.65, opacity: selected && !focus ? 0.09 : 0.3 }));
      branches.appendChild(node('rect', { x: spine, y: yy - (focus ? 1.5 : 0.65), width: 4 + Math.abs(stock.last.value) / maxAbs * 26, height: focus ? 3 : 1.3, fill: color, opacity: selected && !focus ? 0.11 : 0.47 }));
      if (!labelPositions.has(stock.symbol)) return;
      const labelY = labelPositions.get(stock.symbol);
      branches.appendChild(node('path', { d: 'M ' + (spine + 30) + ' ' + yy + ' C ' + (spine + 50) + ' ' + yy + ', ' + (spine + 50) + ' ' + labelY + ', ' + branchEnd + ' ' + labelY, fill: 'none', stroke: color, 'stroke-width': focus ? 2 : 0.8, opacity: selected && !focus ? 0.08 : 0.31 }));
      const label = node('g');
      label.appendChild(node('rect', { x: labelLeft - 5, y: labelY - 8, width: width - labelLeft, height: 16, rx: 4, fill: focus ? (stock.last.value >= 0 ? 'var(--selected-green)' : 'var(--selected-rose)') : 'transparent', stroke: focus ? color : 'none', 'stroke-width': 0.7 }));
      label.appendChild(node('circle', { cx: labelLeft, cy: labelY, r: focus ? 3 : 2, fill: color }));
      label.appendChild(node('text', { x: labelLeft + 7, y: labelY + 3.5, fill: focus ? INK : MUTED, 'font-size': 11, 'font-weight': focus ? 700 : 550 }, String(stock.symbol).slice(0, 13)));
      label.appendChild(node('title', {}, summary)); makeInteractive(label, stock, 'Inspect ' + summary);
      labels.appendChild(label);
    });
    svg.appendChild(lines); svg.appendChild(branches); svg.appendChild(endings); svg.appendChild(labels);
    svg.appendChild(node('text', { x: left, y: height - 7, fill: MUTED, 'font-size': 10 }, 'REALIZED RESULTS · ' + plotted.length + ' SYMBOL' + (plotted.length === 1 ? '' : 'S')));
    if (!compact) svg.appendChild(node('text', { x: width - 10, y: height - 7, fill: MUTED, 'font-size': 10, 'text-anchor': 'end' }, 'Select a path or symbol to inspect'));
  }
  window.FlowChart = { render };
}());
