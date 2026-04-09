# -*- coding: utf-8 -*-
"""
generate_v17b_reference_pdf.py
================================
Generates: v17b_Complete_Strategy_Reference.pdf
Run:  python generate_v17b_reference_pdf.py
"""
from __future__ import annotations
from fpdf import FPDF
from pathlib import Path

OUT_PATH = Path(__file__).resolve().parent / "v17b_Complete_Strategy_Reference.pdf"

# ─── palette ───────────────────────────────────────────────────────────────
C_DARK  = (15,  23,  42)   # near-black text
C_HEAD  = (30,  58, 138)   # deep blue headings
C_SUB   = (37, 99, 235)    # section sub-title blue
C_KEY   = (79,  70, 229)   # indigo for parameter names
C_LONG  = (5, 150, 105)    # green for LONG
C_SHORT = (220,  38,  38)  # red for SHORT
C_BOTH  = (124,  58, 237)  # purple for shared/both
C_WARN  = (202, 138,   4)  # amber highlight
C_LIGHT = (241, 245, 249)  # light gray row
C_WHITE = (255, 255, 255)
C_RULE  = (203, 213, 225)  # divider


class Doc(FPDF):
    title_text = "V17b 5-min — Complete Strategy & Parameter Reference"

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*C_HEAD)
        self.cell(0, 6, self.title_text, align="L")
        self.set_text_color(*C_RULE)
        self.ln(1)
        self.set_draw_color(*C_RULE)
        self.set_line_width(0.3)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "", 7)
        self.set_text_color(*C_RULE)
        self.cell(0, 6, f"Page {self.page_no()}", align="C")

    # ── helpers ──────────────────────────────────────────────────────────────
    def rule(self):
        self.set_draw_color(*C_RULE)
        self.set_line_width(0.2)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2)

    def h1(self, text: str, bg=C_HEAD):
        self.set_fill_color(*bg)
        self.set_text_color(*C_WHITE)
        self.set_font("Helvetica", "B", 13)
        self.cell(0, 9, f"  {text}", fill=True, ln=True)
        self.ln(2)
        self.set_text_color(*C_DARK)

    def h2(self, text: str, color=C_SUB):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*color)
        self.cell(0, 7, text, ln=True)
        self.set_draw_color(*color)
        self.set_line_width(0.5)
        self.line(self.l_margin, self.get_y(), self.l_margin + 60, self.get_y())
        self.set_line_width(0.2)
        self.ln(2)
        self.set_text_color(*C_DARK)

    def h3(self, text: str, color=C_DARK):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*color)
        self.cell(0, 6, text, ln=True)
        self.set_text_color(*C_DARK)

    def para(self, text: str, size=8.5):
        self.set_font("Helvetica", "", size)
        self.set_text_color(*C_DARK)
        self.multi_cell(0, 5, text)
        self.ln(1)

    def badge(self, text: str, color):
        """Inline colored badge label."""
        self.set_font("Helvetica", "B", 7.5)
        self.set_text_color(*color)
        self.cell(self.get_string_width(text) + 2, 5, text)
        self.set_text_color(*C_DARK)

    def param_row(self, name: str, value: str, note: str = "",
                  shade: bool = False, side_color=None):
        """One parameter table row."""
        if shade:
            self.set_fill_color(*C_LIGHT)
        else:
            self.set_fill_color(*C_WHITE)

        x0 = self.get_x()
        y0 = self.get_y()

        # name col  (62 mm)
        self.set_font("Courier", "B", 8)
        sc = side_color or C_KEY
        self.set_text_color(*sc)
        self.cell(62, 6, name, fill=True)

        # value col (30 mm)
        self.set_font("Courier", "", 8)
        self.set_text_color(*C_DARK)
        self.cell(30, 6, value, fill=True)

        # note col (rest)
        self.set_font("Helvetica", "", 7.5)
        note_w = self.w - self.r_margin - self.get_x()
        self.multi_cell(note_w, 6, note, fill=True)

        # ensure we advanced at least one row
        if self.get_y() <= y0 + 1:
            self.ln(6)

        self.set_text_color(*C_DARK)

    def section_table(self, rows, title: str | None = None,
                      side_color=None, note_col_label: str = "Notes / Rationale"):
        """Draw a labeled table with alternating shading."""
        if title:
            self.h3(title, color=side_color or C_DARK)

        # header row
        self.set_fill_color(*C_HEAD)
        self.set_text_color(*C_WHITE)
        self.set_font("Helvetica", "B", 7.5)
        self.cell(62, 6, "Parameter", fill=True)
        self.cell(30, 6, "Value", fill=True)
        self.cell(0,  6, note_col_label, fill=True, ln=True)
        self.set_text_color(*C_DARK)

        for i, (name, val, note) in enumerate(rows):
            self.param_row(name, val, note, shade=(i % 2 == 0), side_color=side_color)

        self.ln(3)


def build_cover(doc: Doc):
    doc.add_page()
    doc.set_fill_color(*C_HEAD)
    doc.rect(0, 0, doc.w, 60, "F")

    doc.set_font("Helvetica", "B", 20)
    doc.set_text_color(*C_WHITE)
    doc.set_y(14)
    doc.cell(0, 10, "V17b — 5-min Backtest", align="C", ln=True)
    doc.set_font("Helvetica", "B", 14)
    doc.cell(0, 8, "Complete Strategy & Parameter Reference", align="C", ln=True)

    doc.set_fill_color(*C_WARN)
    doc.set_y(38)
    doc.cell(0, 8, "  ATR-Normalized Short RS  +  V16 Long RS  (Hybrid)  ", align="C", fill=True, ln=True)

    doc.set_text_color(*C_DARK)
    doc.set_y(68)

    doc.set_font("Helvetica", "B", 9)
    doc.set_text_color(*C_HEAD)
    doc.cell(0, 6, "What is V17b?", ln=True)
    doc.set_font("Helvetica", "", 8.5)
    doc.set_text_color(*C_DARK)
    doc.multi_cell(0, 5,
        "V17b is a thin monkey-patch over V16 Run-14 canonical.  It inherits every parameter "
        "from V16 unchanged except for the SHORT-side NIFTY relative-strength filter, which is "
        "replaced with an ATR-normalised version.\n\n"
        "  SHORT side  — raw RS%  /  stock ATR%  →  dimensionless ratio (ATR-norm RS)\n"
        "                eliminates signals where 'weakness' is just normal volatility noise\n\n"
        "  LONG  side  — original V16 raw RS% filter, completely unchanged\n\n"
        "All other scan logic, signal windows, entry setups, exit rules, risk management, "
        "NIFTY context modes, and V16 anti-exhaustion post-scan filters are identical to V16 Run-14."
    )

    doc.ln(4)
    doc.rule()
    doc.ln(2)

    # Quick stats box
    doc.h2("V16 Run-14 Benchmark (reference baseline)", color=C_BOTH)
    stats = [
        ("Trades", "141", "43 SHORT + 98 LONG"),
        ("Trading Days", "13", ""),
        ("Short PF",  "2.442", "pessimistic"),
        ("Long PF",   "1.391", "pessimistic"),
        ("Combined PF", "1.624", "pessimistic"),
        ("Day-win%",  "76.92%", "13 days"),
        ("Sum PnL%",  "+119.19%", "pessimistic"),
        ("Max DD%",   "21.39%", "pessimistic"),
    ]
    for i, (k, v, n) in enumerate(stats):
        doc.set_fill_color(*(C_LIGHT if i % 2 == 0 else C_WHITE))
        doc.set_font("Helvetica", "B", 8)
        doc.cell(50, 5.5, k, fill=True)
        doc.set_font("Courier", "B", 8)
        doc.set_text_color(*C_HEAD)
        doc.cell(28, 5.5, v, fill=True)
        doc.set_font("Helvetica", "", 7.5)
        doc.set_text_color(*C_DARK)
        doc.cell(0, 5.5, n, fill=True, ln=True)

    doc.ln(4)
    doc.rule()

    doc.set_font("Helvetica", "", 7.5)
    doc.set_text_color(*C_RULE)
    doc.cell(0, 5, "Generated 2026-04-08  |  eqidv2 backtesting project", align="C", ln=True)


def build_toc(doc: Doc):
    doc.add_page()
    doc.h1("TABLE OF CONTENTS")
    sections = [
        ("1", "Strategy Architecture Overview"),
        ("2", "V17b-Specific Parameters  (SHORT ATR-Norm RS)"),
        ("3", "Signal Windows & Timing"),
        ("4", "SHORT-Side Indicator Parameters"),
        ("5", "LONG-Side Indicator Parameters"),
        ("6", "Entry Setup Controls"),
        ("7", "Risk Management  (SL / TGT / BE / Trail)"),
        ("8", "NIFTY Context Filter"),
        ("9", "V16 Anti-Exhaustion Post-Scan Filters"),
        ("10", "Portfolio & Execution Settings"),
        ("11", "VIX Dynamic Scaling  (disabled in V17b)"),
        ("12", "Parallelism & Chart Settings"),
        ("13", "Env-Var Override Reference"),
        ("14", "How Parameters Changed from V15 → V16 → V17b"),
    ]
    for num, title in sections:
        doc.set_font("Helvetica", "", 9)
        doc.cell(12, 6, num + ".")
        doc.set_font("Helvetica", "B", 9)
        doc.set_text_color(*C_HEAD)
        doc.cell(0, 6, title, ln=True)
        doc.set_text_color(*C_DARK)
    doc.ln(3)
    doc.rule()
    doc.para(
        "Note: V17b re-uses avwap_combined_runner_v16_5min.py as a base module via Python "
        "import. All parameters below are active at runtime for V17b unless explicitly "
        "overridden by the V17b monkey-patch section (§2)."
    )


def build_architecture(doc: Doc):
    doc.add_page()
    doc.h1("1. Strategy Architecture Overview")

    doc.h2("Signal Flow")
    doc.para(
        "5-min OHLCV parquet data\n"
        "  └─ Parallel ticker scan (ProcessPoolExecutor)\n"
        "       ├─ SHORT scanner  (avwap_short_strategy_v11)\n"
        "       └─ LONG  scanner  (avwap_long_strategy_v9_sweep)\n"
        "            ↓\n"
        "  Merge all tickers → raw signal DataFrames\n"
        "            ↓\n"
        "  NIFTY intraday context build\n"
        "    → mode_map  {ts_key: 'LONG_ONLY' | 'SHORT_ONLY' | 'BOTH'}\n"
        "    → nifty_ret_map  {ts_key: 4-bar return %}\n"
        "            ↓\n"
        "  V17b NIFTY Context Filter  ← KEY CHANGE FROM V16\n"
        "    SHORT: ATR-normalised RS gate (rs_norm = raw_RS / ATR%)\n"
        "    LONG : V16 raw RS% gate (unchanged)\n"
        "            ↓\n"
        "  V16 Anti-Exhaustion Post-Scan Filters\n"
        "    SHORT: block RSI 35-40 dead zone\n"
        "    LONG : block QS 7.5-8.0 dead zone + QS>10.0 exhausted\n"
        "           block AVWAP dist >3.0 ATR + dead zone 1.0-1.5 ATR\n"
        "           block entry vol >4x day-avg (>=3 bars from open)\n"
        "            ↓\n"
        "  (Optional) OR gate — disabled in Run-14 canonical\n"
        "            ↓\n"
        "  Exit realism: 1-min SL-first resolver + pessimistic stress band\n"
        "            ↓\n"
        "  Portfolio simulation → metrics → CSV + charts"
    )
    doc.ln(2)
    doc.rule()

    doc.h2("Data Inputs")
    rows = [
        ("5-min OHLCV parquets",   "C:\\TradingData\\eqidv2\\...", "Primary signal data — stocks + NIFTYBEES"),
        ("15-min parquets",         "same root",                    "ATR values for v17b ATR-norm RS"),
        ("1-min parquets",          "same root",                    "Intrabar exit resolution"),
        ("india_vix.parquet",       "project root",                 "VIX scaling (disabled in v17b)"),
    ]
    doc.section_table(rows, title="Data Sources", note_col_label="Purpose")

    doc.h2("Output Directory")
    doc.para("outputs_v17b_5min/   (runtime_dir redirected from v16_5min → v17b_5min by monkey-patch)")


def build_v17b_params(doc: Doc):
    doc.add_page()
    doc.h1("2. V17b-Specific Parameters  (SHORT ATR-Norm RS)", bg=C_SHORT)

    doc.h2("Core Concept", color=C_SHORT)
    doc.para(
        "Instead of testing raw RS% against a fixed percentage threshold, V17b divides the "
        "raw RS by the stock's ATR% (from 15-min data).  This produces a dimensionless ratio "
        "that is comparable across low- and high-volatility stocks.\n\n"
        "  raw_RS  = stock_4bar_return%  −  nifty_4bar_return%\n"
        "  ATR%    = ATR(14) from 15-min parquet  /  close  ×  100\n"
        "  rs_norm = raw_RS  /  ATR%\n\n"
        "A SHORT requires  rs_norm  ≤  −threshold  (i.e. stock underperforms by at least\n"
        "  'threshold' multiples of its own ATR).  This filters out noise-level weakness."
    )

    doc.h2("V17b Constants & Env-Var Overrides", color=C_SHORT)
    rows = [
        ("V17B_RS_ATR_NORM_ENABLED",          "True",  "Master on/off for ATR-norm mode"),
        ("V17B_RS_ATR_NORM_THRESH_SHORT_BOTH","0.50",  "BOTH-mode threshold — stock must underperform by ≥ 0.50× its own ATR; env: EQIDV17B_RS_ATR_NORM_THRESH_SHORT_BOTH"),
        ("V17B_RS_ATR_DIRECTIONAL_THRESH_SHORT","0.35","Directional mode (SHORT_ONLY) — lower bar; env: EQIDV17B_RS_ATR_DIRECTIONAL_THRESH_SHORT"),
        ("V17B_RS_ATR_FALLBACK_PCT_SHORT",    "0.40%", "Fallback raw RS% threshold when ATR data unavailable; env: EQIDV17B_RS_ATR_FALLBACK_PCT_SHORT"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("ATR Map Builder Logic", color=C_SHORT)
    doc.para(
        "Source: 15-min parquet file for each ticker.\n"
        "  1. Load 'atr' column (case-insensitive)\n"
        "  2. Compute  atr_pct = atr / close × 100\n"
        "  3. Index by  ts_key = YYYY-MM-DD HH:MM (IST, tz-aware)\n"
        "  4. Cache per ticker (_v17b_atr_cache) to avoid re-loading\n\n"
        "If the 15-min parquet does not exist or has no ATR column, the fallback "
        "raw RS% threshold (0.40%) is used instead — same as V16 directional mode."
    )

    doc.rule()
    doc.h2("Filter Logic (SHORT only)", color=C_SHORT)
    doc.para(
        "For each SHORT candidate at timestamp ts_key in mode M:\n\n"
        "  1. Look up nifty_ret_map[ts_key]  → nifty 4-bar return\n"
        "  2. Look up stock_ret_map[ts_key]  → stock 4-bar return\n"
        "  3. raw_RS = stock_return − nifty_return\n"
        "  4. Look up atr_map[ts_key]  → stock ATR%\n\n"
        "  If ATR% is valid (finite, >0):\n"
        "    rs_norm = raw_RS / ATR%\n"
        "    threshold = 0.50 if M=='BOTH' else 0.35\n"
        "    keep  =  (rs_norm  ≤  −threshold)\n\n"
        "  If ATR% is missing:\n"
        "    keep  =  (raw_RS  ≤  −0.40%)\n\n"
        "  LONG_ONLY mode: always rejected (mode filter, not RS filter)\n"
        "  SHORT_ONLY / BOTH: apply above test"
    )


def build_signal_windows(doc: Doc):
    doc.add_page()
    doc.h1("3. Signal Windows & Timing")

    doc.h2("SHORT Signal Windows", color=C_SHORT)
    rows = [
        ("Window 1",  "09:15 – 11:00", "Morning — 60-70% win rate (9:xx=60.8%, 10:xx=70.6%)"),
        ("Window 2",  "12:00 – 13:30", "Afternoon — 12:xx=80% win, 13:00-13:30=83% win"),
        ("Excluded",  "11:00 – 12:00", "Dead zone — 25% win; excluded from v15 onwards"),
        ("Excluded",  "13:30 – 15:30", "25% win, tail end; entry_cutoff gates this hard"),
        ("entry_cutoff", "13:30",      "Hard entry cutoff: no SHORT entries after 13:30 IST"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("LONG Signal Windows", color=C_LONG)
    rows = [
        ("Window 1",  "09:15 – 11:00", "Morning — best quality (Run10 confirmed)"),
        ("Window 2",  "12:00 – 13:00", "Afternoon — Run13 addition: +DayWin 76.56%, +PnL 546% vs 523%"),
        ("Excluded",  "11:00 – 12:00", "Dead zone — confirmed bad in Run15 (DayWin 67%, MaxDD 56%)"),
        ("Excluded",  "13:00 – 15:30", "Run14 tested 13:00-14:15: worse; Run15 tested continuous: worse"),
    ]
    doc.section_table(rows, side_color=C_LONG)

    doc.h2("Other Timing Constants", color=C_BOTH)
    rows = [
        ("EOD exit time",              "15:20 IST",  "V15_EOD_EXIT_TIME — force-exit all open positions"),
        ("NIFTY OR end time",          "09:30 IST",  "Opening-range window for NIFTY context — 5-min bars before this"),
        ("NIFTY confirm time",         "09:30 IST",  "Context activates at 09:30 (v15: shifted from 10:30)"),
        ("OR gate time",               "09:30 IST",  "Upper bound for opening-range bar selection"),
    ]
    doc.section_table(rows, side_color=C_BOTH)

    doc.h2("Signal → Entry Lag (bars)", color=C_BOTH)
    rows = [
        ("SHORT Setup-A  mod break C1-low",       "1 bar  (5 min)",  ""),
        ("SHORT Setup-A  pullback C2 break C2-low","2 bars (10 min)", ""),
        ("SHORT Setup-B  huge failed bounce",      "dynamic (-1)",    "First valid bar after signal bar"),
        ("LONG  Setup-A  mod break C1-high",       "1 bar  (5 min)",  ""),
        ("LONG  Setup-A  pullback C2 break C2-high","1 bar  (5 min)", ""),
        ("LONG  Setup-A  close continuation break", "2 bars (10 min)",""),
        ("LONG  Setup-B  huge C1 close reclaim",   "2 bars (10 min)", ""),
        ("LONG  Setup-B  huge pullback hold break", "999 bars",        "Effectively disabled — weak setup"),
    ]
    doc.section_table(rows, side_color=C_BOTH)


def build_short_indicators(doc: Doc):
    doc.add_page()
    doc.h1("4. SHORT-Side Indicator Parameters", bg=C_SHORT)

    doc.h2("Trend / Momentum Indicators", color=C_SHORT)
    rows = [
        ("adx_min",                    "30.0",   "Minimum ADX for valid trend (Pack C short — stricter than LONG)"),
        ("adx_slope_min",              "0.40",   "Minimum ADX slope — rising trend required"),
        ("rsi_max_short",              "58.0",   "RSI must be ≤ 58 — avoids late/overbought entries"),
        ("stochk_max",                 "90.0",   "Stochastic K maximum — momentum cap"),
        ("mod_impulse_min_atr",        "0.30",   "Min impulse bar size in ATR units (V16 Run6: relaxed 0.45→0.30)"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("AVWAP / Structure Indicators", color=C_SHORT)
    rows = [
        ("avwap_min_consec_closes",    "1",      "Min consecutive closes below AVWAP (V16 Run6: relaxed 2→1)"),
        ("signal_avwap_dist_atr_max",  "2.10",   "Max AVWAP distance at signal time in ATR units — anti-chase"),
        ("enable_avwap_no_trade_zone", "False",  "Disabled — no dead-zone rejection by signal scanner"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("Volume Indicators", color=C_SHORT)
    rows = [
        ("volume_min_ratio",           "0.90",   "Entry bar vol must be ≥ 90% of day-average — confirms impulse"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("Structure / Mode Filters", color=C_SHORT)
    rows = [
        ("enable_mode_selector",       "True",   "Day-mode selection enabled (AVWAP structure check)"),
        ("reversal_requires_sweep",    "True",   "Reversal entry requires prior liquidity sweep"),
        ("enable_liquidity_sweep_filter","False", "Global liquidity-sweep pre-filter disabled"),
        ("require_vwap_side_persistence","False", "VWAP-side persistence check off"),
        ("vwap_side_lookback_bars",    "5",      "Lookback for VWAP-side persistence (unused — filter off)"),
        ("vwap_side_min_count",        "3",      "Min bars on VWAP side (unused — filter off)"),
        ("require_structure_filter",   "False",  "Structure filter off"),
        ("structure_lookback_bars",    "30",     "Lookback bars for structure (unused — filter off)"),
        ("enable_ema200_filter",       "False",  "EMA-200 filter disabled"),
        ("use_prev_close_for_day_mode","False",  "Day mode uses intraday price, not prev close"),
        ("min_opening_range_width_pct","1.00%",  "OR width must be ≥ 1% of price — filters low-vol days"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("Scanning Setup Types (SHORT)", color=C_SHORT)
    rows = [
        ("ENABLE_PLAYBOOK_V11_PROFILE","True",   "Activates the full Pack-C SHORT playbook (v11 strategy)"),
        ("Setup A — mod break C1-low", "Enabled","Moderate-size candle breaks C1 low"),
        ("Setup A — pullback C2 break","Enabled","Pullback to C2 then breaks lower"),
        ("Setup B — huge failed bounce","Enabled","V16 Run6: huge candle failed bounce short (PACK2_ENABLE=True)"),
        ("max_trades_per_ticker/day",   "6",      "V16 Run3: raised 4→6 for volume"),
    ]
    doc.section_table(rows, side_color=C_SHORT)


def build_long_indicators(doc: Doc):
    doc.add_page()
    doc.h1("5. LONG-Side Indicator Parameters", bg=C_LONG)

    doc.h2("Trend / Momentum Indicators", color=C_LONG)
    rows = [
        ("adx_min",                    "24.0",   "Min ADX — slightly lower than SHORT for more longs"),
        ("adx_slope_min",              "0.50",   "Min ADX slope"),
        ("rsi_min_long",               "52.0",   "Min RSI — momentum floor (Pack C: above midline)"),
        ("stochk_min",                 "15.0",   "Stochastic K minimum"),
        ("stochk_max",                 "95.0",   "Stochastic K maximum"),
        ("atr_pct_min",                "0.0025", "Min ATR% — filters extremely low-vol stocks"),
        ("quality_score_min",          "4.5",    "Pack C: higher-quality entries only (V16 raised from 4.0)"),
    ]
    doc.section_table(rows, side_color=C_LONG)

    doc.h2("AVWAP / Structure Indicators", color=C_LONG)
    rows = [
        ("avwap_min_consec_closes",    "1",      "Min consecutive closes above AVWAP (V16 Run6: relaxed 2→1)"),
        ("require_entry_close_confirm","True",   "Entry bar must close above/near entry trigger"),
        ("enable_setup_a_pullback_c2_break","True","Pullback-to-C2 then break C2-high enabled"),
        ("enable_setup_a_close_continuation_break","True","Close-continuation break enabled (v15 fix)"),
        ("enable_setup_b_huge_c1_close_reclaim_break","True","Huge candle C1 close reclaim enabled"),
    ]
    doc.section_table(rows, side_color=C_LONG)

    doc.h2("Volume Indicators", color=C_LONG)
    rows = [
        ("volume_min_ratio",           "0.90",   "Entry bar vol ≥ 90% day-avg"),
        ("max_vix_for_entries",        "13.0",   "Max India VIX for LONG entries — avoids volatile days"),
    ]
    doc.section_table(rows, side_color=C_LONG)

    doc.h2("Scanning Setup Types (LONG)", color=C_LONG)
    rows = [
        ("ENABLE_V9_LONG_PROFILE",     "True",   "Activates V9 s5 LONG playbook"),
        ("Setup A — mod break C1-high","Enabled","Moderate-size candle breaks C1 high"),
        ("Setup A — pullback C2 break","Enabled","Pullback to C2 then breaks higher"),
        ("Setup A — close continuation","Enabled","Close continuation break"),
        ("Setup B — huge C1 close reclaim","Enabled","V14: huge candle C1 close reclaim break (lag=2)"),
        ("Setup B — huge pullback hold","Disabled","Lag=999 — effectively disabled (weak setup)"),
        ("max_trades_per_ticker/day",   "5",      "V16 Run3: raised 3→5"),
    ]
    doc.section_table(rows, side_color=C_LONG)


def build_risk(doc: Doc):
    doc.add_page()
    doc.h1("7. Risk Management  (SL / TGT / BE / Trail)")

    doc.h2("SHORT Risk Parameters", color=C_SHORT)
    rows = [
        ("stop_pct",              "0.75%",  "Stop-loss distance from entry (0.0075)"),
        ("target_pct",            "0.80%",  "Profit target (0.00800) — slightly asymmetric R:R"),
        ("be_trigger_pct",        "0.42%",  "Break-even trigger — move SL to entry after +0.42%"),
        ("trail_pct",             "0.23%",  "Trail step after BE trigger activated"),
        ("enable_partial_exit",   "False",  "V16: no partial exits — SL / TARGET / EOD only"),
        ("partial_exit_fraction", "0.50",   "Would exit 50% at first target (disabled)"),
        ("partial_target_fraction","0.50",  "Partial target at 50% of target_pct (disabled)"),
        ("enable_risk_based_sizing","False","Risk-% position sizing disabled"),
        ("risk_per_trade_pct",    "0.35%",  "Capital risk per trade if sizing enabled (disabled)"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("LONG Risk Parameters", color=C_LONG)
    rows = [
        ("stop_pct",              "0.75%",  "Same as SHORT — unified V16 stop"),
        ("target_pct",            "0.80%",  "Same as SHORT — unified V16 target"),
        ("be_trigger_pct",        "0.55%",  "Break-even trigger at +0.55% (LONG needs more room)"),
        ("trail_pct",             "0.28%",  "Trail step — slightly wider than SHORT"),
    ]
    doc.section_table(rows, side_color=C_LONG)

    doc.h2("Exit Realism Settings", color=C_BOTH)
    rows = [
        ("EXIT_REALISM_BAND_ENABLED",       "True",  "Enable pessimistic/optimistic stress bands"),
        ("EXIT_REALISM_USE_STRESSED_BASE",  "True",  "Promote pessimistic result to main output"),
        ("STOP_EXIT_EXTRA_SLIPPAGE_BPS",    "3.0 bps","Extra slippage on stop exits"),
        ("1-min resolver",                  "Active", "Uses 1-min bars when available; falls back to 5-min"),
    ]
    doc.section_table(rows, side_color=C_BOTH)

    doc.h2("Position Sizing", color=C_BOTH)
    rows = [
        ("POSITION_SIZE_RS_SHORT",          "₹20,000",  "Capital/margin per SHORT trade"),
        ("POSITION_SIZE_RS_LONG",           "₹20,000",  "Capital/margin per LONG trade"),
        ("INTRADAY_LEVERAGE_SHORT",         "5.0×",     "Notional = 20,000 × 5 = ₹1,00,000"),
        ("INTRADAY_LEVERAGE_LONG",          "5.0×",     "Notional = 20,000 × 5 = ₹1,00,000"),
        ("PORTFOLIO_START_CAPITAL_RS",      "₹10,00,000","Starting portfolio capital"),
        ("ENABLE_CASH_CONSTRAINED_SIM",     "False",    "All trades taken regardless of capital state"),
        ("DISALLOW_BOTH_SIDES_SAME_TICKER", "False",    "V16 Run3: allow LONG + SHORT same ticker same day"),
    ]
    doc.section_table(rows, side_color=C_BOTH)


def build_nifty_context(doc: Doc):
    doc.add_page()
    doc.h1("8. NIFTY Context Filter")

    doc.h2("Context Mode Builder", color=C_BOTH)
    doc.para(
        "At startup a mode_map is built from NIFTYBEES / NIFTY50 5-min data.  "
        "Each 5-min timestamp is assigned one of: LONG_ONLY | SHORT_ONLY | BOTH.\n\n"
        "Logic:\n"
        "  1. Identify NIFTY tickers in data (NIFTYBEES, NIFTY50, NIFTY_50, NIFTY)\n"
        "  2. Compute opening range (OR) high/low from bars before OR_END_TIME (09:30)\n"
        "  3. For each subsequent bar:\n"
        "       if NIFTY close > OR_high  → LONG_ONLY\n"
        "       if NIFTY close < OR_low   → SHORT_ONLY\n"
        "       else                       → BOTH\n"
        "  4. Require NIFTY_CONTEXT_MIN_DAYMOVE_PCT ≥ 0.35% total day move to activate\n"
        "     (if day move < 0.35%, all timestamps default to BOTH)"
    )

    rows = [
        ("NIFTY_CONTEXT_ENABLED",          "True",   "Master switch"),
        ("NIFTY_CONTEXT_TICKERS",          "NIFTYBEES, NIFTY50, NIFTY_50, NIFTY", "Tries each until data found"),
        ("NIFTY_CONTEXT_OR_END_TIME",      "09:30",  "OR computed from bars before this time"),
        ("NIFTY_CONTEXT_CONFIRM_TIME",     "09:30",  "Context activates after this time"),
        ("NIFTY_CONTEXT_MIN_DAYMOVE_PCT",  "0.35%",  "Min NIFTY day move to apply directional mode"),
        ("NIFTY_RS_FILTER_ENABLED",        "True",   "Apply RS filter in addition to mode filter"),
        ("NIFTY_RS_LOOKBACK_BARS",         "4",      "4-bar (20-min) RS lookback window"),
        ("NIFTY_RS_THRESHOLD_PCT",         "0.20%",  "Directional mode threshold (raw RS%)"),
        ("NIFTY_RS_BOTH_MODE_ENABLED",     "True",   "Apply RS in BOTH mode too"),
        ("NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT",  "0.75%","LONG must beat NIFTY by ≥ 0.75% in BOTH mode"),
        ("NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT", "0.75%","SHORT must lag NIFTY by ≥ 0.75% in BOTH mode  (V16 Run3: relaxed from 1.00%)"),
    ]
    doc.section_table(rows, side_color=C_BOTH)

    doc.h2("V17b Override — SHORT Side Only", color=C_SHORT)
    doc.para(
        "In V17b the BOTH-mode RS gate for SHORT is replaced by ATR-norm RS (see §2).  "
        "The NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT constant (0.75%) is NOT used for SHORT "
        "when V17B_RS_ATR_NORM_ENABLED=True.  It is replaced by:\n\n"
        "  rs_norm = raw_RS / ATR%  ≤  −0.50  (BOTH mode)\n"
        "  rs_norm = raw_RS / ATR%  ≤  −0.35  (SHORT_ONLY mode)\n\n"
        "The LONG side uses NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT = 0.75% unchanged."
    )


def build_anti_exhaustion(doc: Doc):
    doc.add_page()
    doc.h1("9. V16 Anti-Exhaustion Post-Scan Filters")

    doc.para(
        "Applied after the NIFTY context filter, before portfolio simulation.  "
        "These are the core V16 additions over V15."
    )

    doc.h2("SHORT Filters", color=C_SHORT)
    rows = [
        ("V16_SHORT_RSI_DEAD_ZONE_LO",  "35.0",  "Block RSI ≥ 35 AND < 40 only — the dead zone (38.5% win)"),
        ("V16_SHORT_RSI_DEAD_ZONE_HI",  "40.0",  "RSI < 35 = 66.7% win → keep;  RSI ≥ 40 = 75%+ win → keep"),
        ("V16_SHORT_RS_EXHAUSTION_CAP", "−2.0%", "RS extremity guard (implemented via NIFTY threshold)"),
        ("OR gate SHORT",               "Disabled","V16 Run11: OR gate disabled for SHORT (cost −90 trades)"),
    ]
    doc.section_table(rows, side_color=C_SHORT)

    doc.h2("LONG Filters", color=C_LONG)
    rows = [
        ("V16_LONG_QS_DEAD_LO",         "7.5",   "Quality-score dead zone lower bound (50% win)"),
        ("V16_LONG_QS_DEAD_HI",         "8.0",   "Quality-score dead zone upper bound → block 7.5–8.0"),
        ("V16_LONG_QS_ABS_MAX",         "10.0",  "Absolute QS cap — QS > 10 = 30% win (exhausted momentum)"),
        ("V16_LONG_AVWAP_DIST_ATR_MAX", "3.0 ATR","Anti-chase cap: dist > 3.0 ATR = 50% win → block"),
        ("V16_LONG_AVWAP_DIST_DEAD_LO", "1.0 ATR","AVWAP dead zone low (59% TGT, avg +0.16%)"),
        ("V16_LONG_AVWAP_DIST_DEAD_HI", "1.5 ATR","AVWAP dead zone high — block 1.0–1.5 ATR band"),
        ("V16_LONG_ENTRY_VOL_EXHAUST_ENABLED","True","Volume exhaustion filter (Run14 addition)"),
        ("V16_LONG_ENTRY_VOL_EXHAUST_MULT","4.0×","Entry bar vol > 4× day-avg = climax signal → reject"),
        ("V16_LONG_ENTRY_VOL_EXHAUST_MIN_BARS","3","Only apply when ≥ 3 bars from open (skip OR-bar entries)"),
    ]
    doc.section_table(rows, side_color=C_LONG)

    doc.h2("OR Gate", color=C_BOTH)
    rows = [
        ("V16_OR_GATE_ENABLED",        "False",  "Run12: disabled — was blocking high-quality LONG trades"),
        ("V16_OR_GATE_SHORT_ENABLED",  "False",  "Run11: disabled for SHORT"),
        ("V16_OR_GATE_FIRST_BAR_ONLY", "True",   "Use only 09:15 bar for OR range (not 09:15-09:30)"),
        ("V16_OR_GATE_TIME",           "09:30",  "Upper bound for OR bar selection"),
    ]
    doc.section_table(rows, side_color=C_BOTH)


def build_portfolio(doc: Doc):
    doc.add_page()
    doc.h1("10. Portfolio & Execution Settings")

    doc.h2("General Settings", color=C_BOTH)
    rows = [
        ("FORCE_LIVE_PARITY_MIN_BARS_LEFT",  "True",  "Forces min_bars_left_after_entry=0 both sides"),
        ("FORCE_LIVE_PARITY_DISABLE_TOPN",   "True",  "Disables Top-N per-day pruning"),
        ("SYNC_CURRENT_DAY_WITH_LIVE_PARITY","False", "Live replay disabled for 5-min variant"),
        ("ENABLE_CASH_CONSTRAINED_SIM",       "False", "No capital constraint — all trades taken"),
        ("DISALLOW_BOTH_SIDES_SAME_TICKER",   "False", "V16: allow both LONG + SHORT for same ticker on same day"),
    ]
    doc.section_table(rows, side_color=C_BOTH)

    doc.h2("Pack-2 (Setup-B) Controls", color=C_BOTH)
    rows = [
        ("PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE","True","V16 Run6: enabled huge failed-bounce SHORT"),
        ("PACK2_SHORT_MAX_VIX_FOR_ENTRIES",  "0.0",   "No VIX cap for SHORT (0.0 = disabled)"),
        ("PACK2_LONG_MAX_VIX_FOR_ENTRIES",   "0.0",   "LONG VIX cap overridden to 13.0 in profile"),
    ]
    doc.section_table(rows, side_color=C_BOTH)

    doc.h2("Parallelism & Execution", color=C_BOTH)
    rows = [
        ("DEFAULT_MAX_WORKERS",        "min(4, cpu_count)", "Auto: capped at 4 workers"),
        ("MAX_WORKERS",                "env override",      "EQIDV16_5MIN_MAX_WORKERS env var"),
        ("EXECUTOR_MODE",              "auto",              "EQIDV16_5MIN_EXECUTOR: 'auto'|'process'|'thread'"),
        ("min_bars_left_after_entry",  "0",                 "Forced to 0 (live parity)"),
    ]
    doc.section_table(rows, side_color=C_BOTH)

    doc.h2("Chart Generation", color=C_BOTH)
    rows = [
        ("ENABLE_LEGACY_CHARTS",  "True (default)",   "env: EQIDV16_5MIN_ENABLE_LEGACY_CHARTS=0 to disable"),
        ("ENABLE_ENHANCED_CHARTS","True (default)",   "env: EQIDV16_5MIN_ENABLE_ENHANCED_CHARTS=0 to disable"),
    ]
    doc.section_table(rows, side_color=C_BOTH)


def build_vix(doc: Doc):
    doc.add_page()
    doc.h1("11. VIX Dynamic Scaling  (Disabled in V17b)")

    doc.para(
        "V16/V17b include a VIX-scaling module that adjusts SL and target proportionally "
        "to India VIX.  It is DISABLED (VIX_SCALE_ENABLED = False) in both V16 Run-14 and V17b.  "
        "Parameters documented here for completeness."
    )

    rows = [
        ("VIX_SCALE_ENABLED",   "False",    "Master switch — OFF in v16/v17b"),
        ("VIX_BASELINE",        "11.5",     "Actual dataset median VIX — neutral point (scale = 1.0×)"),
        ("VIX_SCALE_MIN",       "0.75×",    "Floor multiplier at VIX ≈ 8.6"),
        ("VIX_SCALE_MAX",       "1.50×",    "Cap multiplier at VIX ≈ 17.25"),
        ("VIX_SCALE_TARGET",    "True",     "Would scale target_pct proportionally"),
        ("VIX_SCALE_SL",        "True",     "Would scale stop_pct proportionally (R:R preserved)"),
        ("Data source",         "india_vix.parquet","Fetch once with fetch_india_vix.py"),
    ]
    doc.section_table(rows, side_color=C_BOTH)

    doc.h2("VIX Dataset Statistics (Aug 2025 – Mar 2026)", color=C_BOTH)
    rows = [
        ("Actual VIX range",   "9.15 – 21.14", "132 trading days"),
        ("Median VIX",         "11.41",          "Used as baseline"),
        ("Mean VIX",           "11.61",          ""),
        ("Best zone",          "12.0 – 12.7",    "Win=66.7%, AvgPnL=+1.26% → scale 1.04-1.12×"),
        ("Worst zone",         "11.1 – 11.9",    "Win=57.9%, AvgPnL=+0.74% → dead zone (~1.0×)"),
        ("Low VIX zone",       "9.2 – 10.1",     "Win=64.2%, AvgPnL=+1.19% → scale 0.80-0.86×"),
    ]
    doc.section_table(rows, side_color=C_BOTH)


def build_env_vars(doc: Doc):
    doc.add_page()
    doc.h1("13. Environment Variable Override Reference")

    doc.para(
        "All env vars listed here can be set before running the script to override defaults.  "
        "V17b adds the EQIDV17B_* vars; all others are inherited from V16."
    )

    doc.h2("V17b-Specific (SHORT ATR-Norm RS)", color=C_SHORT)
    rows = [
        ("EQIDV17B_RS_ATR_NORM_THRESH_SHORT_BOTH",   "default 0.50", "BOTH-mode dimensionless threshold"),
        ("EQIDV17B_RS_ATR_DIRECTIONAL_THRESH_SHORT",  "default 0.35", "SHORT_ONLY mode threshold"),
        ("EQIDV17B_RS_ATR_FALLBACK_PCT_SHORT",         "default 0.40", "Raw RS% fallback when ATR missing"),
    ]
    doc.section_table(rows, side_color=C_SHORT, note_col_label="Default / Meaning")

    doc.h2("V16 Execution / IO", color=C_BOTH)
    rows = [
        ("EQIDV2_RUNTIME_ROOT",                "C:\\TradingData\\eqidv2", "Root for all data paths"),
        ("EQIDV16_5MIN_MAX_WORKERS",           "auto",    "Override worker pool size"),
        ("EQIDV16_5MIN_EXECUTOR",              "auto",    "'auto'|'process'|'thread'"),
        ("EQIDV16_5MIN_ENABLE_LEGACY_CHARTS",  "1",       "0 = disable legacy charts (~5s faster)"),
        ("EQIDV16_5MIN_ENABLE_ENHANCED_CHARTS","1",       "0 = disable enhanced charts"),
        ("PYTHONUNBUFFERED",                   "1",       "Flush logs immediately"),
        ("PYTHONIOENCODING",                   "utf-8",   "Encoding for console output"),
    ]
    doc.section_table(rows, side_color=C_BOTH, note_col_label="Default / Meaning")


def build_changelog(doc: Doc):
    doc.add_page()
    doc.h1("14. Parameter Evolution: V15 → V16 → V17b")

    changes = [
        # (label, what changed, from, to, reason)
        ("NIFTY OR end time",          "V14→V16", "10:15", "09:30", "Earlier live participation"),
        ("NIFTY confirm time",         "V14→V16", "10:30", "09:30", "Context activates sooner"),
        ("NIFTY min day move",         "V14→V16", "0.20%", "0.35%", "Filter noise-level NIFTY moves"),
        ("NIFTY RS lookback",          "V14→V16", "3 bars","4 bars","60-min window; more stable"),
        ("NIFTY RS directional thresh","V14→V16", "0.15%", "0.20%","Above round-trip cost"),
        ("NIFTY RS BOTH mode",         "V14→V16", "Off",   "On",    "New: apply RS in neutral days too"),
        ("BOTH-mode LONG thresh",      "V15→V16", "0.08%", "0.75%","Run2: RS 1.5-2% = 75% win"),
        ("BOTH-mode SHORT thresh",     "V15→V16", "1.00%", "0.75%","Run3: RS -0.75 to -1.0 = 76% win"),
        ("SHORT adx_min",              "V15→V16", "28",    "30",    "Stricter trend for cleaner shorts"),
        ("SHORT mod_impulse_min_atr",  "V16 Run6","0.45",  "0.30",  "More C1 qualify (relaxed)"),
        ("SHORT avwap_min_consec",     "V16 Run6","2",     "1",     "Relaxed; 1 close sufficient"),
        ("LONG  avwap_min_consec",     "V16 Run6","2",     "1",     "Relaxed; 1 close sufficient"),
        ("SHORT max_trades/ticker",    "V16 Run3","4",     "6",     "Volume increase"),
        ("LONG  max_trades/ticker",    "V16 Run3","3",     "5",     "Volume increase"),
        ("LONG  signal windows",       "V16 Run13","09:15-11:00","+ 12:00-13:00","Afternoon window added"),
        ("LONG  vol exhaustion filter","V16 Run14","Off",  "On",    "Entry vol >4× day-avg = climax"),
        ("SHORT RSI dead zone",        "V16",     "Off",   "35-40", "38.5% win → block; RSI<35 keep"),
        ("LONG  QS dead zone",         "V16",     "Off",   "7.5-8.0","50% win dead zone; 8-10 keep"),
        ("LONG  QS abs max",           "V16",     "Off",   "10.0",  "QS>10 = 30% win; block"),
        ("LONG  AVWAP dist max",       "V16",     "Off",   "3.0 ATR","Anti-chase; 3.0+ = 50% win"),
        ("LONG  AVWAP dead zone",      "V16",     "Off",   "1.0-1.5 ATR","59% TGT dead zone"),
        ("SHORT RS gate",              "V16→V17b","raw RS% ≤ −0.75%","rs_norm ≤ −0.50","ATR-normalised; removes noise"),
    ]

    doc.set_fill_color(*C_HEAD)
    doc.set_text_color(*C_WHITE)
    doc.set_font("Helvetica", "B", 7.5)
    for col, w in [("Parameter", 52), ("Change", 20), ("From", 22), ("To", 22), ("Reason", 0)]:
        doc.cell(w, 6, col, fill=True)
    doc.ln()
    doc.set_text_color(*C_DARK)

    for i, (param, change, frm, to, reason) in enumerate(changes):
        if doc.get_y() > 250:
            doc.add_page()
        shade = (i % 2 == 0)
        if shade:
            doc.set_fill_color(*C_LIGHT)
        else:
            doc.set_fill_color(*C_WHITE)
        doc.set_font("Courier", "B", 7.5)
        doc.set_text_color(*C_KEY)
        doc.cell(52, 5.5, param, fill=True)
        doc.set_font("Helvetica", "", 7.5)
        doc.set_text_color(*C_BOTH)
        doc.cell(20, 5.5, change, fill=True)
        doc.set_text_color(*C_SHORT)
        doc.cell(22, 5.5, frm, fill=True)
        doc.set_text_color(*C_LONG)
        doc.cell(22, 5.5, to, fill=True)
        doc.set_text_color(*C_DARK)
        note_w = doc.w - doc.r_margin - doc.get_x()
        doc.multi_cell(note_w, 5.5, reason, fill=True)


# ─── MAIN ──────────────────────────────────────────────────────────────────
def main():
    doc = Doc(orientation="P", unit="mm", format="A4")
    doc.set_auto_page_break(auto=True, margin=15)
    doc.set_margins(15, 15, 15)

    build_cover(doc)
    build_toc(doc)
    build_architecture(doc)
    build_v17b_params(doc)
    build_signal_windows(doc)
    build_short_indicators(doc)
    build_long_indicators(doc)
    build_risk(doc)
    build_nifty_context(doc)
    build_anti_exhaustion(doc)
    build_portfolio(doc)
    build_vix(doc)
    build_env_vars(doc)
    build_changelog(doc)

    doc.output(str(OUT_PATH))
    print(f"[OK] PDF saved: {OUT_PATH}")


if __name__ == "__main__":
    main()
