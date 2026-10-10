/* Pure data helpers shared by the Flow controls and their accounting tests. */
(function (root) {
  'use strict';
  const known = value => typeof value === 'number' && Number.isFinite(value);
  const sum = values => values.every(known) ? values.reduce((total, value) => total + value, 0) : null;
  function hasCompleteLedger(data) {
    const total = sum(data.trades.map(trade => trade.net_pnl));
    return data.summary.trades === data.trades.length && known(total) && known(data.summary.net_pnl)
      && Math.abs(total - data.summary.net_pnl) <= Math.max(0.02, Math.abs(total) * 1e-9);
  }
  function summarize(trades, dates, complete = true) {
    const calendar = [...new Set(dates)].sort();
    const calendarSet = new Set(calendar);
    const rows = trades.filter(trade => calendarSet.has(trade.date));
    const knownNet = complete && rows.every(trade => known(trade.net_pnl));
    const wins = knownNet ? rows.filter(trade => trade.net_pnl > 0).length : null;
    const losses = knownNet ? rows.filter(trade => trade.net_pnl < 0).length : null;
    let cumulative = 0, peak = 0, drawdown = 0;
    const daily = new Map(calendar.map(day => [day, 0]));
    if (knownNet) {
      rows.forEach(trade => daily.set(trade.date, daily.get(trade.date) + trade.net_pnl));
      for (const value of daily.values()) { cumulative += value; peak = Math.max(peak, cumulative); drawdown = Math.max(drawdown, peak - cumulative); }
    }
    const gain = knownNet ? sum(rows.map(trade => Math.max(0, trade.net_pnl))) : null;
    const loss = knownNet ? -sum(rows.map(trade => Math.min(0, trade.net_pnl))) : null;
    return {
      sessions: calendar.length, trades: complete ? rows.length : null,
      wins, losses, net_pnl: knownNet ? sum(rows.map(trade => trade.net_pnl)) : null,
      gross_pnl: complete ? sum(rows.map(trade => trade.gross_pnl)) : null,
      cost: complete ? sum(rows.map(trade => trade.cost)) : null,
      win_rate_pct: knownNet && rows.length ? wins / rows.length * 100 : null,
      profit_factor: knownNet && loss > 0 ? gain / loss : null,
      max_drawdown: knownNet ? drawdown : null,
      period_start: calendar[0] || null, period_end: calendar.at(-1) || null,
    };
  }
  function shortStrategy(strategy) { return String(strategy || '').replace(/^V13-V10-/, ''); }
  function runLabel(run, latest, index = 0) {
    const shortDate = value => value ? new Date(value + 'T12:00:00').toLocaleDateString('en-GB', {day: '2-digit', month: 'short', year: 'numeric'}) : 'date unavailable';
    const match = run.run_name.match(/(?:^run_|^)(\d{4})(\d{2})(\d{2})/);
    const saved = match ? shortDate(`${match[1]}-${match[2]}-${match[3]}`) : '';
    const kind = run.kind === 'daily' ? 'Daily replay' : run.kind === 'full_history' ? 'Full history' : 'Backtest';
    return `${shortStrategy(run.strategy)} · ${latest ? 'Latest' : 'Archive ' + index} · Through ${shortDate(run.period_end)}${latest ? '' : saved ? ' · Saved ' + saved : ''}${run.kind === 'daily' ? ' · ' + kind : ''}`;
  }
  const api = {hasCompleteLedger, summarize, shortStrategy, runLabel};
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.FlowCore = api;
})(typeof window !== 'undefined' ? window : globalThis);
