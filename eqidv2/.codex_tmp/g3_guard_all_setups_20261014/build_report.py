from pathlib import Path
import hashlib
import html
import json
import shutil
import pandas as pd

OUT = Path(r'C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/research/guard_all_setups_20261014')
summary = json.loads((OUT / 'study_summary.json').read_text())
base = summary['baseline']
scenarios = pd.read_csv(OUT / 'remaining_selected_reallocation.csv')
guard = scenarios.loc[scenarios.threshold.eq(3)].iloc[0]
setups = pd.read_csv(OUT / 'per_setup.csv')
setups = setups.loc[setups.threshold.eq(3) & setups.chronology.eq('ALL')].copy()
current = setups.loc[setups.scope.eq('CURRENT_HISTORICAL_49')].copy()
extra = setups.loc[setups.scope.ne('CURRENT_HISTORICAL_49')].copy()
exits = pd.read_csv(OUT / 'exit_reason_3atr.csv')
groups = pd.read_csv(OUT / 'groups.csv')
groups = groups.loc[groups.scope.eq('CURRENT_HISTORICAL_49') & groups.threshold.eq(3)]
daily = pd.read_csv(OUT / 'daily.csv')
daily = daily.loc[daily.threshold.eq(3)].copy()

def money(value):
    return f'₹{float(value):,.2f}'

def table(frame, money_columns=()):
    return frame.to_html(index=False, border=0, classes='data', escape=True,
                         formatters={c: money for c in money_columns}, na_rep='—')

def setup_table(frame):
    frame = frame.copy()
    frame['Signal setup'] = frame.setup_id.map(lambda s: f'{int(s[:2]):02d}:{int(s[2:4])-1:02d} {s.split("_")[1]}')
    frame['Assessment'] = frame.apply(lambda r: 'Already guarded; unchanged' if r.setup_id in ('1036_LONG','1036_SHORT')
        else 'Small positive screen; research only' if r.fixed_allocation_delta_rs > 0
        else 'No observed P&L benefit', axis=1)
    frame = frame.rename(columns={'executed':'Executed','blocked_executed':'Removed','winners_removed':'Winners removed',
        'losers_removed':'Losers removed','fixed_allocation_delta_rs':'Fixed-allocation P&L effect'})
    cols = ['Signal setup','Executed','Removed','Winners removed','Losers removed','Fixed-allocation P&L effect','Assessment']
    return table(frame[cols], ['Fixed-allocation P&L effect'])

metrics = pd.DataFrame([
    ['Executed trades', int(base['executed_trades']), int(guard.executed_trades)],
    ['Wins / losses', f"{base['wins']} / {base['losses']}", f'{int(guard.wins)} / {int(guard.losses)}'],
    ['Win rate', f"{base['win_rate_pct']:.2f}%", f'{guard.win_rate_pct:.2f}%'],
    ['Net P&L', money(base['net_pnl_rs']), money(guard.net_pnl_rs)],
    ['Net P&L per executed trade', money(base['net_pnl_rs']/base['executed_trades']), money(guard.net_pnl_rs/guard.executed_trades)],
    ['Maximum daily-close drawdown', money(base['max_drawdown_rs']), money(guard.max_drawdown_rs)],
], columns=['Metric','Current G3','3 ATR on other setups: selected-order screen'])
sensitivity = scenarios[['threshold','executed_trades','win_rate_pct','net_pnl_rs','net_delta_rs']].copy()
sensitivity.win_rate_pct = sensitivity.win_rate_pct.map(lambda x:f'{x:.2f}%')
sensitivity.columns = ['Threshold (ATR)','Executed trades','Win rate','Net P&L','Change from current']
cohorts = groups.loc[groups.group.isin(['side','chronology']), ['group','label','blocked_executed','winners_removed','losers_removed','fixed_allocation_delta_rs']].copy()
cohorts.columns = ['Grouping','Cohort','Removed executions','Winners removed','Losers removed','Fixed-allocation P&L effect']
day_table = daily[['day','before_pnl','after_pnl','delta']].copy()
day_table.columns = ['Day','Current net','Screen net after reallocation','Change']

page = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>G3: Should the one-minute ATR guard apply to other setups?</title>
<style>
:root {{color-scheme:light dark;--bg:#f3f5f7;--card:#fff;--fg:#17212d;--muted:#475569;--line:#cbd5e1;--accent:#155e75;--note:#fff5dd}}
@media(prefers-color-scheme:dark){{:root{{--bg:#111827;--card:#1f2937;--fg:#f3f4f6;--muted:#cbd5e1;--line:#475569;--accent:#67e8f9;--note:#423519}}}}
body{{font:16px/1.6 system-ui,sans-serif;background:var(--bg);color:var(--fg);margin:0}}main{{max-width:1200px;margin:auto;padding:28px}}
section{{background:var(--card);padding:22px;margin:20px 0;border:1px solid var(--line);border-radius:10px}}h1{{line-height:1.2}}h2{{font-size:1.3rem}}a{{color:var(--accent)}}
.note{{background:var(--note);padding:16px;border-left:4px solid #c48b16}}.scroll{{overflow-x:auto}}table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{padding:9px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}}th{{font-weight:700}}.muted{{color:var(--muted)}}code{{font-size:.95em}}li{{margin:8px 0}}
</style></head><body><main>
<p class="muted">14 October 2026 · G3 research · Current configuration unchanged</p>
<h1>Do not apply the one-minute 3×ATR guard to every setup yet</h1>
<p>The guard provides useful information about entry extension. The current evidence does not support a mandatory global veto. G3 already clears the 70% historical win-rate objective; further increasing win rate can remove profitable opportunities.</p>
<section><h2>What was analysed</h2>
<p>All 530 selected orders in the verified 51-session current historical run, across 49 setups, from 29 July to 14 October 2026. Each indicator was reconstructed using completed one-minute OHLC only through the exact confirmation timestamp. All confirmation OHLC matched the frozen order evidence. Existing 10:35 LONG and SHORT guards were kept unchanged.</p>
<p class="note"><strong>This is a selected-order screen, not a full strategy reselection backtest.</strong> After rejecting selected orders, capital was reallocated among the remaining originally selected orders. Alternative candidates that might replace a rejected top-ranked stock were not selected. Native entry/exit outcomes were reused because this screen changes only admission; the allocator first reproduced the original execution flags and P&L exactly.</p>
<div class="scroll">{table(metrics)}</div>
<p><strong>Screen net change: {money(guard.net_delta_rs)}.</strong> Trading frequency falls sharply. Average net per remaining trade improves modestly, and daily-close drawdown falls, but total net profit nearly halves. This is a trade-off in activity and risk, not proof of better entries at equal exposure. Daily-close drawdown is not intraday equity drawdown.</p>
<p>The backtest uses ₹100,000 capital per entry and 5× exposure, with 5 bps round-trip costs. Current live configuration specifies ₹10,000 capital and 5× exposure. These rupee results are historical-model figures, not a live income estimate; brokerage, spread, impact and fill timing can differ.</p></section>
<section><h2>It avoids stops, but also removes winners</h2>
<p>At 3 ATR, 278 selections are rejected, removing 242 previously executed trades: 175 winners and 67 losers. It removes 32 of the 47 stop exits, but also 97 target winners worth ₹642,650 net. The removed winners contributed ₹862,711, while the avoided losing trades lost ₹249,600.</p>
<div class="scroll">{table(exits, ['removed_net_pnl_rs'])}</div>
<p>After reallocation, nine days improve, 41 worsen and one is unchanged. Both LONG and SHORT, every time band, and both chronological halves show negative aggregate P&L effects. The chronological split is diagnostic, not an independent holdout.</p>
<div class="scroll">{table(cohorts, ['Fixed-allocation P&L effect'])}</div></section>
<section><h2>Why the same ceiling is not appropriate everywhere</h2><ul>
<li><strong>Trend strength is not exhaustion.</strong> G3 deliberately selects directional price, OI, EMA ordering and a directional confirmation candle. A persistent trend can remain more than 3 ATR from EMA20. A hypothetical steady ₹1/min rise with ₹1.20 true ranges has an eventual EMA20 gap near ₹9.50: roughly 7.92 ATR, despite no reversal.</li>
<li><strong>ATR measures volatility, not the chance of a reversal.</strong> Three ATR is not three standard deviations or a statistical confidence level. A price can reverse within the limit or keep trending outside it. <a href="https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr">Fidelity ATR guide</a>.</li>
<li><strong>Opening and later setups are different.</strong> Early EMA/ATR history spans the prior session and overnight gap. Later clocks have more current-session observations. A uniform limit does not have a uniform meaning.</li>
<li><strong>The current formula includes the confirmation candle.</strong> Its close moves EMA20 and its true range changes ATR14. G3 uses a simple 14-bar mean of true range, not Wilder smoothing; a large range rolling out of that window can also change the ratio.</li>
<li><strong>Confirmation is not the actual fill.</strong> The check uses the one-minute confirmation close. LONG triggers at confirmation high and SHORT at confirmation low; the entry can occur up to ten minutes later. The guard is not recomputed at the later touch. A passing check does not ensure the actual fill is within 3 ATR.</li>
<li><strong>Expansion needs data-path work.</strong> Live EMA/ATR capture and persisted-bar validation are currently specific to 10:36. Adding setup IDs without matching indicator capture at every new confirmation would fail closed on missing live data.</li></ul></section>
<section><h2>Where further research may be worthwhile</h2>
<p>Only four of the 47 newly screened setups show positive fixed-allocation removal effects at 3 ATR: 09:45 SHORT (+₹6,500 from one removed loser), 09:45 LONG (+₹4,600 from three removals), 12:35 SHORT (+₹2,603 from six removals), and 13:25 SHORT (+₹240 from three removals). These are too small to justify deployment.</p>
<p>12:35 SHORT is the most coherent candidate to investigate because both chronological halves are positive, but its effect turns negative at 4 ATR. Across all 49 setups, median executed count is only eight and 48 setups have fewer than twenty executions. Those samples do not justify declaring a best threshold for each setup.</p>
<p>The global sensitivity points below all reduce total P&L. They are diagnostics, not an optimization or a recommendation to choose the least damaging threshold.</p>
<div class="scroll">{table(sensitivity, ['Net P&L','Change from current'])}</div>
<p><strong>Recommended next step:</strong> record extension as a shadow advisory across setups, assess the opening and later groups separately, and fully replay pre-quota reselection for any nominated group. Freeze the candidate rule before forward paper validation. Judge net expectancy, losses, drawdown, turnover and missed target winners together with win rate. Do not choose per-setup parameters from these tiny samples. Multiple strategy trials can manufacture apparent historical advantages: <a href="https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf">Bailey et al., The Probability of Backtest Overfitting</a>.</p>
<p>The existing 10:35 guard also remains experimental: its earlier full 50-session comparison raised win rate but reduced portfolio net P&L by ₹8,101.35. Its current presence is not evidence that it should be copied everywhere.</p></section>
<section><h2>Source and scope limits</h2>
<p>All 530 current orders and all 49 setups had valid matched confirmation inputs. Of these, 133 orders used immutable day snapshots (128 files); 397 used current historical parquets (166 files). All 166 older whole-file hashes differ from their original sealed hashes. The confirmation bar matches, but this does not certify every earlier EMA/ATR input against the original dataset. Both source cohorts independently show negative aggregate screen effects.</p>
<p>Production/PAPER/LIVE has 59 setups; normal historical/daily V11 has 49. The ten additional production setups were screened separately using 146 archived V7 selections, 116 of which were executed. At 3 ATR this supplemental screen removes 37 winners and four losers, reducing their fixed-allocation net by ₹36,115.25. That is older-configuration evidence, not a current-production59 replay.</p>
<p>No active configuration, live authority, or dashboard result pointer was changed by this study. The threshold was already proposed after inspecting UPL; this study and its historical splits are not independent out-of-sample qualification.</p></section>
<section><h2>All 49 current historical setups</h2><p>Effects below remove existing executed trades at their original allocation; they do not include replacement candidates or other-setup capital effects. Setup labels refer to the signal clock; the one-minute confirmation is one minute later.</p><div class="scroll">{setup_table(current)}</div></section>
<section><h2>Supplemental ten production-only setups</h2><p>Archived V7 evidence only, kept separate from the current 49-setup analysis.</p><div class="scroll">{setup_table(extra)}</div></section>
<section><h2>Daily portfolio screen</h2><div class="scroll">{table(day_table, ['Current net','Screen net after reallocation','Change'])}</div></section>
<section><h2>Audit files</h2><p><a href="selected_order_extension_audit.csv">Order-level indicator evidence</a> · <a href="per_setup.csv">All setup thresholds and chronology</a> · <a href="remaining_selected_reallocation.csv">Scenario totals</a> · <a href="daily.csv">Daily comparison</a> · <a href="study_summary.json">Method and source hashes</a> · <a href="artifact_manifest.json">Artifact hashes</a></p>
<p class="muted">Baseline: {html.escape(summary['baseline_run'])}</p></section>
</main></body></html>'''
(OUT / 'report.html').write_text(page, encoding='utf-8')
shutil.copyfile(Path(__file__).with_name('screen.py'), OUT / 'screen_source.py.txt')
hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(OUT.iterdir()) if p.is_file() and p.name!='artifact_manifest.json'}
(OUT / 'artifact_manifest.json').write_text(json.dumps(hashes,indent=2)+'\n')
print(OUT / 'report.html')
