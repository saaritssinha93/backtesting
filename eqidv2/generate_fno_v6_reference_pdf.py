# -*- coding: utf-8 -*-
"""
generate_fno_v6_reference_pdf.py
=================================
Generates: FnO_V6_Strategy_and_Flows.pdf

Print-oriented reference for the FnO V6 strategy: greyscale only, compact type,
and a complete inventory of every module, runner, input path and output
artefact the strategy touches.

Run:  python generate_fno_v6_reference_pdf.py

Note: core PDF fonts are latin-1 only, so every string here is ASCII and all
diagrams are drawn with rectangles/polygons instead of box-drawing glyphs.
"""
from __future__ import annotations

from pathlib import Path

from fpdf import FPDF
from fpdf.enums import XPos, YPos

OUT_PATH = Path(__file__).resolve().parent / "FnO_V6_Strategy_and_Flows.pdf"

# --- greyscale palette (print-safe) ---------------------------------------
BLACK = (0, 0, 0)
INK = (20, 20, 20)          # body text
HEAD_BG = (38, 38, 38)      # section bar
MUTE = (105, 105, 105)      # secondary text
RULE = (170, 170, 170)      # hairlines
SHADE = (237, 237, 237)     # alternating table row
BOX = (224, 224, 224)       # diagram box - data
TINT = (246, 246, 246)      # callout / code background
WHITE = (255, 255, 255)

TITLE = "FnO V6 - Strategy, Backtest Flow and Live Flow"
GENERATED = "2026-08-13"


class Doc(FPDF):
    # ---------------------------------------------------------------- chrome
    def header(self) -> None:
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "B", 6.8)
        self.set_text_color(*MUTE)
        self.cell(0, 5, TITLE, align="L", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*RULE)
        self.set_line_width(0.25)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2.4)
        self.set_text_color(*INK)

    def footer(self) -> None:
        self.set_y(-10)
        self.set_font("Helvetica", "", 6.4)
        self.set_text_color(*MUTE)
        self.cell(
            0, 5,
            f"FnO V6 reference  |  generated {GENERATED}  |  page {self.page_no()}",
            align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )

    # --------------------------------------------------------------- helpers
    @property
    def usable(self) -> float:
        return self.w - self.l_margin - self.r_margin

    def rule(self) -> None:
        self.set_draw_color(*RULE)
        self.set_line_width(0.2)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(1.6)

    def h1(self, text: str) -> None:
        self.set_fill_color(*HEAD_BG)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 10.5)
        self.cell(0, 7.5, f"  {text}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)
        self.set_text_color(*INK)

    def h2(self, text: str) -> None:
        self.ln(0.8)
        self.set_font("Helvetica", "B", 8.5)
        self.set_text_color(*BLACK)
        self.cell(0, 5.4, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*BLACK)
        self.set_line_width(0.4)
        self.line(self.l_margin, self.get_y(), self.l_margin + 46, self.get_y())
        self.set_line_width(0.2)
        self.ln(1.8)
        self.set_text_color(*INK)

    def h3(self, text: str) -> None:
        self.set_font("Helvetica", "B", 7.6)
        self.set_text_color(*BLACK)
        self.cell(0, 4.8, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*INK)

    def para(self, text: str, size: float = 7.1) -> None:
        self.set_font("Helvetica", "", size)
        self.set_text_color(*INK)
        self.multi_cell(0, 3.9, text, align="L")
        self.ln(0.9)

    def bullets(self, items, size: float = 7.1) -> None:
        for item in items:
            y0 = self.get_y()
            self.set_font("Helvetica", "B", size)
            self.set_text_color(*BLACK)
            self.set_x(self.l_margin + 1.5)
            self.cell(3.5, 3.9, "-")
            self.set_font("Helvetica", "", size)
            self.set_text_color(*INK)
            self.multi_cell(self.usable - 5, 3.9, item, align="L")
            if self.get_y() <= y0 + 1:
                self.ln(3.9)
        self.ln(0.9)

    def code(self, text: str, size: float = 6.6) -> None:
        lines = text.strip("\n").split("\n")
        height = len(lines) * 3.5 + 2.4
        if self.will_page_break(height):
            self.add_page()
        x0, y0 = self.l_margin, self.get_y()
        self.set_fill_color(*TINT)
        self.set_draw_color(*RULE)
        self.set_line_width(0.2)
        self.rect(x0, y0, self.usable, height, style="DF")
        self.set_draw_color(*BLACK)
        self.set_line_width(0.9)
        self.line(x0, y0, x0, y0 + height)
        self.set_line_width(0.2)
        self.set_xy(x0 + 2.5, y0 + 1.2)
        self.set_font("Courier", "", size)
        self.set_text_color(*INK)
        for line in lines:
            self.set_x(x0 + 2.5)
            self.cell(self.usable - 5, 3.5, line, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_y(y0 + height)
        self.ln(1.8)

    def callout(self, title: str, text: str) -> None:
        self.set_font("Helvetica", "", 7.0)
        body = self.multi_cell(
            self.usable - 8, 3.7, text, align="L", dry_run=True, output="LINES"
        )
        height = 5.4 + len(body) * 3.7 + 2.2
        if self.will_page_break(height):
            self.add_page()
        x0, y0 = self.l_margin, self.get_y()
        self.set_fill_color(*TINT)
        self.set_draw_color(*RULE)
        self.set_line_width(0.2)
        self.rect(x0, y0, self.usable, height, style="DF")
        self.set_fill_color(*BLACK)
        self.rect(x0, y0, 1.5, height, style="F")
        self.set_xy(x0 + 4, y0 + 1.1)
        self.set_font("Helvetica", "B", 7.3)
        self.set_text_color(*BLACK)
        self.cell(0, 4.2, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font("Helvetica", "", 7.0)
        self.set_text_color(*INK)
        for line in body:
            self.set_x(x0 + 4)
            self.cell(self.usable - 8, 3.7, line, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_y(y0 + height)
        self.ln(2.2)

    def table(self, headers, widths, rows, aligns=None, size: float = 6.5,
              mono_cols=(), bold_cols=()) -> None:
        total = sum(widths)
        if total > self.usable + 0.01:
            raise ValueError(
                f"table wider than the text block: {total:.1f} > {self.usable:.1f}mm "
                f"(headers={headers})"
            )
        aligns = aligns or ["L"] * len(headers)

        def draw_header() -> None:
            self.set_fill_color(*HEAD_BG)
            self.set_text_color(*WHITE)
            self.set_font("Helvetica", "B", size - 0.2)
            for header, width, align in zip(headers, widths, aligns):
                self.cell(width, 5, f" {header}", fill=True, align=align)
            self.ln(5)
            self.set_text_color(*INK)

        if self.will_page_break(12):
            self.add_page()
        draw_header()

        for index, row in enumerate(rows):
            heights = []
            for col, (cell_text, width) in enumerate(zip(row, widths)):
                self.set_font("Courier" if col in mono_cols else "Helvetica", "", size)
                lines = self.multi_cell(
                    width - 1.6, 3.5, str(cell_text), align="L",
                    dry_run=True, output="LINES",
                )
                heights.append(max(1, len(lines)))
            row_height = max(heights) * 3.5 + 1.1
            if self.will_page_break(row_height):
                self.add_page()
                draw_header()
            self.set_fill_color(*(SHADE if index % 2 == 0 else WHITE))
            y0 = self.get_y()
            x = self.l_margin
            for col, (cell_text, width, align) in enumerate(zip(row, widths, aligns)):
                self.set_xy(x, y0)
                font = "Courier" if col in mono_cols else "Helvetica"
                style = "B" if col in bold_cols else ""
                self.set_font(font, style, size)
                self.set_text_color(*(BLACK if col in bold_cols else INK))
                self.multi_cell(width, row_height, f" {cell_text}", align=align,
                                fill=True, max_line_height=3.5)
                x += width
            self.set_xy(self.l_margin, y0 + row_height)
        self.set_text_color(*INK)
        self.ln(2.2)

    def files_table(self, rows, first_label="File", second_label="Role / contents",
                    first_w=None) -> None:
        first_w = first_w or 74.0
        self.table([first_label, second_label], [first_w, self.usable - first_w],
                   rows, size=6.4, mono_cols=(0,), bold_cols=())

    # ------------------------------------------------------------- diagrams
    def node(self, x, y, w, h, text, fill=BOX, bold=True, size=6.6,
             border=RULE, border_w=0.35, sub=None) -> None:
        self.set_fill_color(*fill)
        self.set_draw_color(*border)
        self.set_line_width(border_w)
        self.rect(x, y, w, h, style="DF", round_corners=True, corner_radius=1.3)
        self.set_text_color(*INK)
        if sub:
            self.set_font("Helvetica", "B" if bold else "", size)
            self.set_xy(x, y + h / 2 - 3.4)
            self.cell(w, 3.4, text, align="C")
            self.set_font("Helvetica", "", size - 0.9)
            self.set_text_color(*MUTE)
            self.set_xy(x, y + h / 2 - 0.1)
            self.cell(w, 3.4, sub, align="C")
        else:
            self.set_font("Helvetica", "B" if bold else "", size)
            self.set_xy(x, y + h / 2 - 1.7)
            self.cell(w, 3.4, text, align="C")
        self.set_text_color(*INK)
        self.set_line_width(0.2)

    def arrow_down(self, x, y0, y1, label=None) -> None:
        self.set_draw_color(*MUTE)
        self.set_line_width(0.4)
        self.line(x, y0, x, y1 - 1.4)
        self.set_fill_color(*MUTE)
        self.polygon([(x - 1.2, y1 - 1.6), (x + 1.2, y1 - 1.6), (x, y1 + 0.3)], style="F")
        if label:
            self.set_font("Helvetica", "", 5.8)
            self.set_text_color(*MUTE)
            self.set_xy(x + 1.8, (y0 + y1) / 2 - 1.8)
            self.cell(60, 3.4, label)
            self.set_text_color(*INK)
        self.set_line_width(0.2)

    def arrow_right(self, x0, x1, y, label=None) -> None:
        self.set_draw_color(*MUTE)
        self.set_line_width(0.4)
        self.line(x0, y, x1 - 1.4, y)
        self.set_fill_color(*MUTE)
        self.polygon([(x1 - 1.6, y - 1.2), (x1 - 1.6, y + 1.2), (x1 + 0.3, y)], style="F")
        if label:
            self.set_font("Helvetica", "", 5.8)
            self.set_text_color(*MUTE)
            self.set_xy(x0, y - 4.4)
            self.cell(x1 - x0, 3.4, label, align="C")
            self.set_text_color(*INK)
        self.set_line_width(0.2)

    def elbow_arrow(self, x0, y0, x1, y1) -> None:
        mid = (y0 + y1) / 2
        self.set_draw_color(*MUTE)
        self.set_line_width(0.4)
        self.line(x0, y0, x0, mid)
        self.line(x0, mid, x1, mid)
        self.line(x1, mid, x1, y1 - 1.4)
        self.set_fill_color(*MUTE)
        self.polygon([(x1 - 1.2, y1 - 1.6), (x1 + 1.2, y1 - 1.6), (x1, y1 + 0.3)], style="F")
        self.set_line_width(0.2)

    def legend(self, items) -> None:
        self.set_font("Helvetica", "", 6.2)
        for label, fill, border_w in items:
            self.set_fill_color(*fill)
            self.set_draw_color(*BLACK)
            self.set_line_width(border_w)
            self.rect(self.get_x(), self.get_y() + 1, 2.8, 2.8, style="DF")
            self.set_line_width(0.2)
            self.set_x(self.get_x() + 4)
            self.set_text_color(*MUTE)
            self.cell(self.get_string_width(label) + 5, 4.8, label)
        self.ln(6)
        self.set_text_color(*INK)


# =========================================================================
# pages
# =========================================================================
def build_cover(doc: Doc) -> None:
    doc.add_page()
    doc.set_fill_color(*HEAD_BG)
    doc.rect(0, 0, doc.w, 50, "F")

    doc.set_font("Helvetica", "B", 18)
    doc.set_text_color(*WHITE)
    doc.set_y(12)
    doc.cell(0, 9, "FnO V6", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    doc.set_font("Helvetica", "B", 11)
    doc.cell(0, 6.5, "Strategy, Backtest Flow and Live Flow",
             align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    doc.set_font("Courier", "", 7.4)
    doc.set_text_color(*RULE)
    doc.cell(0, 5, "FNO_V6_BEST_NET_CASH_EQUITY_20260811   |   objective BEST_NET",
             align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    doc.set_font("Helvetica", "B", 7.4)
    doc.set_text_color(*WHITE)
    doc.cell(0, 6, "NSE cash equity executes every trade  -  the future contributes OI only",
             align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    doc.set_y(56)
    doc.set_text_color(*INK)
    doc.h2("What the strategy does")
    doc.para(
        "In the first thirty minutes of the session the strategy looks for F&O stocks whose cash "
        "price is trending (EMA9/20/50 stacked), moving hard on a 5-minute bar, on expanding "
        "volume, while open interest in the mapped near-month future is rising - a fresh "
        "directional move backed by new positions rather than short covering.\n"
        "The 5-minute bar raises a candidate; the very next 1-minute candle must close in the "
        "same direction to confirm it. Entry is a stop order at that confirmation candle's "
        "extreme, so price has to continue through the confirmation high (LONG) or low (SHORT) "
        "before any risk is taken. Each entry carries a fixed percentage stop and target and is "
        "squared off at 15:30 regardless.\n"
        "Five scan slots are traded - 09:25, 09:30, 09:35, 09:40 and 09:45 - with at most one "
        "LONG and one or two SHORT names per slot, at Rs 10,000 capital x 5x = Rs 50,000 "
        "exposure per entry."
    )

    doc.h2("Frozen backtest attestation")
    doc.para(
        "Checked at every live start-up against the separately versioned 20260818_V1 "
        "current-source selected CSV and protected dated-universe/source provenance. The "
        "legacy 20260811 curve is preserved as historical evidence, not overwritten. These "
        "numbers are an in-sample fit - see section 8."
    )
    stats = [
        ("Protected window", "2026-05-27 .. 2026-08-11", "53 sessions"),
        ("Orders / fills", "210 / 209", "one order never filled"),
        ("Trade PF", "2.811", "profit factor across fills"),
        ("Day PF", "6.062", "profit factor across session totals"),
        ("Net return sum", "+146.711%", "additive, equal notional per trade"),
        ("Setup legs", "10", "5 LONG (cap 1) + 5 SHORT (cap 1-2)"),
        ("Round-trip cost", "5 bps", "same figure in backtest and live"),
    ]
    for index, (key, value, note) in enumerate(stats):
        doc.set_fill_color(*(SHADE if index % 2 == 0 else WHITE))
        doc.set_font("Helvetica", "B", 7.0)
        doc.set_text_color(*INK)
        doc.cell(42, 4.8, f" {key}", fill=True)
        doc.set_font("Courier", "B", 7.0)
        doc.set_text_color(*BLACK)
        doc.cell(42, 4.8, f" {value}", fill=True)
        doc.set_font("Helvetica", "", 6.6)
        doc.set_text_color(*MUTE)
        doc.cell(0, 4.8, f" {note}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    doc.set_text_color(*INK)
    doc.ln(3)

    doc.h2("Contents")
    sections = [
        ("1", "Architecture and data contract"), ("10", "Feed-readiness gates"),
        ("2", "Scan slots and the base gate"), ("11", "Scanner and confirmation logic"),
        ("3", "Confirmation gate and ranking"), ("12", "Entry workers - PAPER and LIVE"),
        ("4", "The V6 setup book"), ("13", "Reporting roles"),
        ("5", "Entry, exits, sizing and cost"), ("14", "Backtest vs live parity"),
        ("6", "Backtest pipeline"), ("15", "Safety rails"),
        ("7", "Fill simulation and attestation"), ("16", "Operations"),
        ("8", "How the V6 book was selected"), ("17", "File and artefact inventory"),
        ("9", "Live architecture and schedule"), ("", ""),
    ]
    col_w = doc.usable / 2
    for i in range(0, len(sections), 2):
        for number, title in sections[i:i + 2]:
            if not number:
                doc.cell(col_w, 4.4, "")
                continue
            doc.set_font("Helvetica", "B", 7.0)
            doc.set_text_color(*BLACK)
            doc.cell(7, 4.4, number)
            doc.set_font("Helvetica", "", 7.0)
            doc.set_text_color(*INK)
            doc.cell(col_w - 7, 4.4, title)
        doc.ln(4.4)

    doc.ln(2)
    doc.rule()
    doc.set_font("Helvetica", "", 6.4)
    doc.set_text_color(*MUTE)
    doc.multi_cell(
        0, 3.6,
        f"Generated {GENERATED}  |  eqidv2 backtesting project  |  regenerate with "
        "generate_fno_v6_reference_pdf.py  |  markdown companion FNO_V6_STRATEGY_AND_FLOWS.md\n"
        "Source of truth: fno_v6_live_config.py and fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py. "
        "If they disagree with this document, they win.",
        align="C",
    )
    doc.set_text_color(*INK)


def build_architecture(doc: Doc) -> None:
    doc.add_page()
    doc.h1("1. Architecture and Data Contract")
    doc.para(
        "The strategy reasons about a futures-market fact (open interest building) but expresses "
        "every decision and every trade in the cash equity. The separation is enforced by "
        "contract validators on both the backtest and the live path: a violation raises rather "
        "than degrades. Data contract version: fno_v5_equity_real_5m_futures_oi_v4."
    )

    doc.h2("Who supplies what")
    doc.table(
        ["Concern", "Source", "Detail"],
        [54, 36, doc.usable - 90],
        [
            ["Price, volume, OHLC, EMA9/20/50, traded value", "NSE cash equity",
             "5-minute bars, end-labelled"],
            ["Confirmation candle, trigger, entry, exits, LTP", "NSE cash equity",
             "1-minute bars and quotes"],
            ["oi, prev_oi, oi_change_pct", "NFO near-month future",
             "joined on the exact bar-end timestamp; nothing else is admitted"],
            ["Universe", "live latest; V6 dated 2026-08-11",
             "promoted replay verifies dated file plus full and mapped semantic hashes; scanner maps stocks and excludes indexes"],
        ],
    )
    doc.para(
        "Mapping future to equity is done by hybrid.ensure_equity_mapping(). Any stock future "
        "that cannot be mapped to a cash symbol is a hard failure - the scanner raises. Only "
        "index futures may be dropped. LTM -> LTIM is the single alias."
    )

    doc.h2("Bar quality rules")
    doc.para(
        "A 5-minute equity bar is usable for a decision only if it survives all of these "
        "(completed_real_equity_five_minute_bars):"
    )
    doc.bullets([
        "not the 09:15 opening snapshot - end-labelled 5-minute data starts at 09:20,",
        "not flagged gap_filled, opening_snapshot or provisional_stale,",
        "built from exactly five 1-minute rows when source_1m_count is present,",
        "not an exact OHLCV copy of the adjacent prior bar, unless both are proven 5x1m.",
    ])
    doc.para(
        "OI is admitted only when both oi and prev_oi are positive and finite; otherwise "
        "oi_change_pct is NaN and the row cannot signal at all."
    )
    doc.callout(
        "Known data limitation",
        "The historical OI cache uses 26AUG futures OI across the whole backtest period. It is "
        "not a rolling near-month OI series. This is carried in the report header of the backtest "
        "itself and should temper any conclusion drawn from OI thresholds fitted on that history.",
    )

    doc.h2("2. Scan slots and the base candidate gate")
    doc.para(
        "Timestamps are candle-end labelled throughout. The 09:25 signal bar covers 09:20-09:25; "
        "its confirmation candle covers 09:25-09:26. 09:20 is never a signal bar because it has "
        "no 5-minute predecessor to diff against."
    )
    y = doc.get_y() + 0.5
    slots = [("09:25", "09:26"), ("09:30", "09:31"), ("09:35", "09:36"),
             ("09:40", "09:41"), ("09:45", "09:46")]
    box_w = 28.0
    gap = (doc.usable - 5 * box_w) / 4
    x = doc.l_margin
    for signal, confirm in slots:
        doc.node(x, y, box_w, 11, signal, sub=f"confirm {confirm}",
                 fill=WHITE, border=BLACK, border_w=0.4, size=7.4)
        x += box_w + gap
    doc.set_y(y + 14)

    doc.para(
        "The base gate is applied per contract on the signal bar, identically in the backtest "
        "(build_signal_table) and live (_base_signal_side):"
    )
    doc.code(
        "LONG   :  ema9 > ema20 > ema50   AND   price_change_pct >= +0.10\n"
        "SHORT  :  ema9 < ema20 < ema50   AND   price_change_pct <= -0.10\n"
        "BOTH   :  oi > prev_oi   AND   oi_change_pct >= 0.05   AND   volume_ratio >= 0.80"
    )
    doc.table(
        ["Field", "Definition"],
        [42, doc.usable - 42],
        [
            ["price_change_pct", "close versus the previous 5-minute close, in percent"],
            ["volume_ratio",
             "bar volume / 20-bar prior-volume mean (min 5 periods, shifted - no look-ahead)"],
            ["traded_value", "close x volume; the liquidity tie-break and the max_liquidity picker"],
            ["oi_change_pct",
             "(oi / prev_oi - 1) x 100 from the mapped future; NaN unless both are positive"],
        ],
        mono_cols=(0,),
    )
    doc.para(
        "Any row with a NaN in a required column is rejected. This superset is deliberately "
        "loose: every tradable setup is a subset of it, so signals are computed once and reused "
        "across the whole parameter search - which is what makes a full grid search affordable."
    )
    doc.callout(
        "Why OI has to be rising",
        "A hard price move on rising open interest means new positions are being opened in the "
        "direction of the move. The same move on falling OI is more likely an unwind - existing "
        "positions closing - which historically continues far less reliably. The gate oi > "
        "prev_oi separates the two, and is applied before any setup-level filter.",
    )


def build_confirmation(doc: Doc) -> None:
    doc.add_page()
    doc.h1("3. Confirmation Gate and Ranking")

    doc.h2("The 1-minute confirmation candle")
    doc.code(
        "range       = high - low                        (must be > 0)\n"
        "body_ratio  = |close - open| / range\n"
        "wick_ratio  = (upper wick if LONG else lower wick) / range\n"
        "trigger     = high if LONG else low\n"
        "\n"
        "LONG  confirmed  <=>  close > open  AND  close > signal-bar close\n"
        "SHORT confirmed  <=>  close < open  AND  close < signal-bar close"
    )
    doc.para(
        "A candidate that fails the direction test is dropped as direction_rejected and never "
        "reaches setup filtering. body_ratio demands a decisive candle rather than a doji; "
        "wick_ratio caps rejection against the direction of the trade - a long upper wick on a "
        "LONG confirm means sellers were already defending that level."
    )

    doc.h2("Ranking and selection")
    doc.code(
        "sort by   picker value      DESC\n"
        "then      traded_value      DESC     (liquidity tie-break)\n"
        "then      tradingsymbol     ASC      (deterministic)\n"
        "take      top `max_entries`"
    )
    doc.table(
        ["Picker", "Ranks by", "Selects the name that is..."],
        [30, 38, doc.usable - 68],
        [
            ["max_oi", "oi_change_pct", "adding the most open interest"],
            ["max_volume", "volume_ratio", "most unusual against its own recent volume"],
            ["max_move", "abs(price_change_pct)", "moving hardest on the signal bar"],
            ["max_body", "body_ratio", "showing the most decisive confirmation candle"],
            ["max_liquidity", "traded_value", "largest in rupee turnover - the safest to trade"],
        ],
        mono_cols=(0, 1),
    )
    doc.para(
        "The backtest (select_setup_rows) groups by day and takes the head of the same ordering; "
        "the live path (config.rank_candidates) is already inside one session. Both share the "
        "traded_value and symbol tie-breaks, so a replay of the same slot always picks the same "
        "names."
    )
    doc.callout(
        "Entry caps are structural, not preference",
        "validate_strategy() enforces a maximum of 1 LONG and 2 SHORT entries per slot, exactly "
        "10 legs, one leg per (slot, side), and confirmation times matching the canonical map. A "
        "config edit that breaks any of these raises at import time - before the scanner runs, "
        "before an order can exist. The daily ceiling is 5 LONG + 7 SHORT = 12 orders.",
    )

    doc.h1("4. The V6 Setup Book")
    doc.para(
        "The ten frozen legs, exactly as they appear in ACTIVE_SETUPS. All are FILTERED mode with "
        "min_traded_value = 0. For a LONG leg the price filter reads price_change_pct >= +X; for "
        "a SHORT leg it reads price_change_pct <= -X (the table stores the magnitude). Stop and "
        "target are percentages of the entry price."
    )
    rows = [
        ["09:25", "09:26", "LONG", "1", "max_liquidity", "0.30", "0.10", "3.0", "0.6", "0.5", "0.50", "3.00"],
        ["09:25", "09:26", "SHORT", "2", "max_volume", "0.20", "0.10", "1.5", "0.4", "0.5", "0.75", "3.00"],
        ["09:30", "09:31", "LONG", "1", "max_move", "0.65", "0.10", "1.0", "0.5", "0.5", "1.00", "2.50"],
        ["09:30", "09:31", "SHORT", "1", "max_move", "0.20", "0.25", "1.0", "0.4", "0.5", "1.00", "3.00"],
        ["09:35", "09:36", "LONG", "1", "max_liquidity", "0.20", "0.10", "1.0", "0.6", "0.5", "1.00", "2.50"],
        ["09:35", "09:36", "SHORT", "2", "max_liquidity", "0.50", "1.00", "1.0", "0.4", "0.5", "1.00", "3.00"],
        ["09:40", "09:41", "LONG", "1", "max_liquidity", "0.20", "0.10", "2.0", "0.5", "0.5", "0.50", "2.50"],
        ["09:40", "09:41", "SHORT", "1", "max_move", "0.20", "0.10", "1.0", "0.4", "0.5", "1.00", "3.00"],
        ["09:45", "09:46", "LONG", "1", "max_move", "0.65", "0.10", "1.0", "0.4", "0.5", "1.00", "3.00"],
        ["09:45", "09:46", "SHORT", "1", "max_volume", "0.20", "0.75", "1.0", "0.4", "0.3", "1.00", "2.00"],
    ]
    doc.table(
        ["Signal", "Confirm", "Side", "Max", "Picker", "Price %", "OI %", "Vol x",
         "Body >=", "Wick <=", "Stop %", "Tgt %"],
        [13, 14, 15, 9, 24, 13, 11, 11, 12, 12, 12, 13],
        rows,
        aligns=["L", "L", "L", "R", "L", "R", "R", "R", "R", "R", "R", "R"],
        size=6.4,
        mono_cols=(5, 6, 7, 8, 9, 10, 11),
        bold_cols=(2,),
    )
    doc.bullets([
        "Targets are wide and stops tight across the whole book: nine of ten legs target 2.5-3.0% "
        "against stops of 0.5-1.0%, and the tenth targets 2.0%.",
        "Reward-to-risk therefore runs from 1:2 (09:45 SHORT) to 1:6 (09:25 LONG). That implies a "
        "low expected hit rate - the current-source attested history fills 209 orders for a trade PF of 2.81, "
        "driven by target-sized winners rather than win frequency.",
        "The SHORT side carries more capacity: two legs (09:25 and 09:35) may take two names, so "
        "seven of the twelve daily order slots are short.",
        "The 09:25 LONG carries the book's highest volume requirement (3x) and its widest bracket "
        "(0.50% stop against a 3.0% target) - one name only, chosen by rupee turnover.",
        "The 09:35 SHORT demands the highest OI build (1.00%); the 09:45 SHORT is the only leg "
        "with a tightened wick cap (0.3) and the only one with a 2.0% target.",
    ])
    doc.callout(
        "This book is frozen",
        "ACTIVE_SETUPS is not tuned at runtime and is not read from a CSV. It is a literal tuple "
        "in the module, hashed into the strategy fingerprint, and cross-checked against a "
        "SHA-256-pinned daily curve at every live start-up. Changing a single threshold changes "
        "the fingerprint and invalidates the day's artefacts rather than silently mixing books.",
    )


def build_execution(doc: Doc) -> None:
    doc.add_page()
    doc.h1("5. Entry, Exits, Sizing and Cost")
    doc.table(
        ["Item", "Value", "Detail"],
        [30, 30, doc.usable - 60],
        [
            ["Entry order", "STOP", "Stop-market at the confirmation candle extreme (trigger), tick-rounded"],
            ["Stop", "entry x (1 -/+ s%)", "s = leg stop_pct; below entry for LONG, above for SHORT"],
            ["Target", "entry x (1 +/- t%)", "t = leg target_pct"],
            ["Time exit", "15:30", "Square-off regardless of position state"],
            ["Tie-break", "stop wins", "If stop and target are reachable in the same bar, the stop is taken"],
            ["Capital", "Rs 10,000", "Per entry, locked"],
            ["Leverage", "5.0x", "Locked - Rs 50,000 target exposure per entry"],
            ["Quantity", "floor(50000/px)", "LIVE additionally floors to a lot multiple"],
            ["Cost", "5 bps", "Round-trip, charged on entry notional"],
            ["Product", "MIS", "Intraday"],
        ],
        mono_cols=(1,),
    )
    doc.para(
        "Capital and leverage are hard-locked: passing any other --capital or --leverage raises "
        "immediately, so a fat-fingered scheduled task cannot quietly trade a different size than "
        "the one that was backtested."
    )

    doc.h2("Why a stop-entry rather than a market entry")
    doc.para(
        "The confirmation candle proves direction; the stop-entry proves continuation. Price must "
        "trade through the confirmation extreme before any risk is taken, discarding every "
        "candidate that confirmed and then stalled. The cost of that discipline shows up in the "
        "frozen history as unfilled orders - an untouched stop-entry expires at square-off as "
        "NO_FILL, having risked nothing."
    )

    doc.h2("Sizing states")
    doc.table(
        ["State", "Meaning"],
        [52, doc.usable - 52],
        [
            ["PAPER_EXPOSURE_SIZED", "Paper quantity = floor(exposure / price), no lot rounding"],
            ["LIVE_LOT_SIZED", "Live quantity floored to a whole lot multiple"],
            ["BLOCKED_PRICE_EXCEEDS_BUDGET", "Paper: one share costs more than Rs 50,000"],
            ["BLOCKED_LOT_EXCEEDS_BUDGET", "Live: one lot costs more than Rs 50,000 - no order is placed"],
        ],
        mono_cols=(0,),
    )
    doc.para(
        "A blocked sizing state is terminal: the order state is created as BLOCKED_SIZING and the "
        "worker never sends anything to the broker."
    )


def build_backtest_flow(doc: Doc) -> None:
    doc.add_page()
    doc.h1("6. Backtest Pipeline")

    stages = [
        ("near_month_2026-08-11.parquet", "frozen dated universe", BOX, 0.35),
        ("verify + map universe", "file/full/mapped hashes; no fallback", BOX, 0.9),
        ("load futures 5m  +  equity 1m", "raw_contracts_5m  |  stocks_indicators_1min_eq", BOX, 0.35),
        ("aggregate 1m -> 5m", "exactly five real 1-minute rows, end-labelled", WHITE, 0.35),
        ("join_equity_price_with_futures_oi", "cash OHLCV/features + oi, prev_oi, oi_change_pct", WHITE, 0.35),
        ("base gate (loose superset)", "EMA stack + price + OI rising + volume", WHITE, 0.35),
        ("1-minute confirmation bar", "body / wick / trigger / direction", WHITE, 0.35),
        ("signal cache", "artifacts + exact source inventory manifest", BOX, 0.35),
        ("validate_cash_equity_signal_contract", "rejects any cache that cannot prove cash execution", WHITE, 0.9),
        ("replay_setups", "filter -> rank -> top-N per day per leg", WHITE, 0.35),
        ("simulate_bracket", "stop-entry fill, stop / target / square-off", WHITE, 0.35),
        ("daily curve, setup summary, stats", "trade PF, day PF, net %", BOX, 0.35),
        ("attest_selected_history", "frozen metrics must match exactly", WHITE, 0.9),
        ("CSV + markdown outputs", "strategy_research/ and latest/", BOX, 0.35),
    ]
    box_w, box_h, gap = 108.0, 7.6, 3.0
    x = doc.l_margin + (doc.usable - box_w) / 2
    y = doc.get_y() + 0.5
    for i, (label, sub, fill, bw) in enumerate(stages):
        doc.node(x, y, box_w, box_h, label, sub=sub, fill=fill,
                 border=BLACK if bw > 0.5 else RULE, border_w=bw, size=6.4)
        if i < len(stages) - 1:
            doc.arrow_down(x + box_w / 2, y + box_h, y + box_h + gap)
        y += box_h + gap
    doc.set_y(y)
    doc.legend([
        ("data / artefact", BOX, 0.35),
        ("computation", WHITE, 0.35),
        ("hard gate - raises on failure", WHITE, 0.9),
    ])

    doc.h2("The signal cache")
    doc.para(
        "load_signals() owns a cache under strategy_research/"
        "_signal_cache_equity_1m_aggregated_5m_futures_oi_v4/ holding signals.parquet, paths.npz "
        "and manifest.json. The manifest pins a dated universe and SHA-256 inventories every "
        "mapped futures-5m and equity-1m file. It also verifies the two cache artifacts. Any "
        "source-byte, universe, contract or artifact drift rebuilds or fails closed. Promoted "
        "V6 refuses mutable latest aliases, missing sources and mapping fallback."
    )
    doc.para(
        "Backtest 5-minute equity bars are constructed rather than read: "
        "NSE_EQUITY_1M_CAUSAL_5X_AGGREGATION groups exactly five real 1-minute rows (offsets "
        "1..375 from 09:15) into an end-labelled candle and drops any group that is not exactly "
        "five rows spanning slot_end-4min .. slot_end. Each signal also stores its forward "
        "1-minute path - high, low and close arrays - starting from the bar AFTER the "
        "confirmation bar and truncated at square-off. A 09:25 signal confirms on the 09:26 "
        "candle and can first fill on the candle ending 09:27. There is no same-bar fill."
    )


def build_backtest_detail(doc: Doc) -> None:
    doc.add_page()
    doc.h1("7. Fill Simulation, Metrics and Attestation")

    doc.h2("simulate_bracket")
    doc.code(
        "1. Walk the forward path; the first bar whose high >= trigger (LONG)\n"
        "   or low <= trigger (SHORT) is the entry bar.  No touch => NaN => NO_FILL.\n"
        "2. From that bar onward, find the first stop hit and first target hit.\n"
        "3. Neither hit                 => exit at the last close (square-off).\n"
        "   stop index <= target index  => exit at the stop (ties resolve to the stop).\n"
        "   otherwise                   => exit at the target.\n"
        "4. net_return_pct = (gross_return - 5bps) x 100"
    )
    doc.callout(
        "Two deliberate biases, pulling in opposite directions",
        "The simulator fills exactly at the trigger, which is optimistic - a real stop-market "
        "order slips. It also resolves same-bar stop/target ambiguity in favour of the stop, "
        "which is pessimistic. Do not net these off mentally; compare live paper against the "
        "backtest on order counts and hit rates first, and on rupee P&L only afterwards.",
    )

    doc.h2("Metrics")
    doc.table(
        ["Metric", "Definition"],
        [34, doc.usable - 34],
        [
            ["Trade PF", "sum of positive trade returns / absolute sum of negative trade returns"],
            ["Day PF", "the same ratio computed over per-day summed returns"],
            ["Net %", "additive sum of per-trade net return % - equal notional per trade, not compounded"],
            ["TRAIN / TEST", "labels only, from --split-day (default 2026-07-17); never a selection input"],
        ],
        mono_cols=(0,),
    )

    doc.h2("Attestation")
    doc.para(
        "Live start-up runs attest_selected_backtest(), which verifies the versioned "
        "current-source selected-daily CSV and protected provenance hash/input fingerprint. "
        "The old 206/205 curve remains unchanged with an explicit mismatch audit. Each V6 run "
        "records its dated-universe "
        "hashes, exact source inventory, cache/output hashes, arguments and date window. The "
        "post-control recreation is labelled honestly; it does not claim to recover the original "
        "August 11 source-byte inventory. A drifted file stops live before it can trade."
    )
    doc.table(
        ["Attested metric", "Expected"],
        [52, doc.usable - 52],
        [
            ["Sessions", "53"],
            ["Orders / fills", "210 / 209"],
            ["Trade PF", "2.811435346898863"],
            ["Day PF", "6.061863909031509"],
            ["Net %", "+146.71089469102625"],
            ["Protected window", "2026-05-27 .. 2026-08-11"],
            ["Protected curve SHA-256", "7ba3426c16497f4d...  (...selected_current_source_20260818_v1.csv)"],
        ],
        mono_cols=(1,),
    )

    doc.add_page()
    doc.h1("8. How the V6 Book Was Selected")
    doc.para(
        "V6 is the BEST_NET portfolio produced by the V5 full-history optimizer "
        "(fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py --mode full-history), in four stages: "
        "leg candidates over a full grid, robustness guards, beam-searched portfolios, then one "
        "argmax per objective (BEST_TRADE_PF, BEST_DAY_PF, BEST_NET)."
    )
    doc.table(
        ["Knob", "Grid"],
        [36, doc.usable - 36],
        [
            ["price_change_pct", "0.20  0.30  0.40  0.50  0.65  0.80"],
            ["oi_change_pct", "0.10  0.25  0.40  0.50  0.75  1.00"],
            ["volume_ratio", "1.0  1.5  2.0  3.0"],
            ["body_ratio", "0.40  0.50  0.60"],
            ["max_wick_ratio", "0.30  0.50"],
            ["min_traded_value", "0  1e7"],
            ["picker", "max_oi  max_volume  max_move  max_body  max_liquidity"],
            ["stop_pct", "0.30  0.40  0.50  0.75  1.00"],
            ["target_pct", "0.50  0.75  1.00  1.50  2.00  2.50  3.00"],
        ],
        mono_cols=(0, 1),
    )
    doc.h3("Robustness guards applied to every candidate")
    doc.bullets([
        "minimum fills per leg and per portfolio, and a minimum count of active days,",
        "day-win rate >= 0.45, and a cap on top-day profit share - one heroic session cannot "
        "carry the book,",
        "robust PF: trade PF and day PF recomputed with the single best whole day removed, both "
        "of which must still exceed 1.0,",
        "3 of 3 positive folds with worst-fold PF >= 0.80.",
    ])
    doc.callout(
        "The headline numbers are an in-sample fit",
        "Full-history mode deliberately fits on every session and keeps TRAIN/TEST as diagnostic "
        "labels only - the V6 report says so in its own header. The +144% and PF 2.80 figures are "
        "a parameter-search ceiling, not out-of-sample evidence. The guards make the fit harder "
        "to game but do not turn it into a walk-forward result. Judge the live book on order "
        "count and fill rate first (those depend on the gates, which are shared code); a live PF "
        "above 1.0 with the expected order flow is the realistic success case.",
    )


def build_live_architecture(doc: Doc) -> None:
    doc.add_page()
    doc.h1("9. Live Architecture and Schedule")
    doc.para(
        "fno_v6_live.py sets FNO_LIVE_GENERATION=v6 and delegates to the shared runtime "
        "fno_v5_live.py, which loads fno_v6_live_config. Six roles run independently, each with "
        "its own scheduled task, status file and markdown report - so a failure localises to one "
        "role instead of taking down a monolith."
    )

    left = doc.l_margin
    width = doc.usable
    y = doc.get_y() + 1
    feed_w = (width - 12) / 3
    feeds = [("fno_oi_universe", "08:50"),
             ("fno_oi_fetch_5min", "futures 5m + marker"),
             ("cash 5m live feed", "slot_ready_5m marker")]
    x = left
    centres = []
    for label, sub in feeds:
        doc.node(x, y, feed_w, 10, label, sub=sub, fill=BOX, size=6.4)
        centres.append(x + feed_w / 2)
        x += feed_w + 6
    y_bottom = y + 10

    y_scan = y_bottom + 8
    spine = left + width / 2
    for centre in centres:
        doc.elbow_arrow(centre, y_bottom, spine, y_scan)

    doc.node(left + 16, y_scan, width - 32, 10, "scanner-5m",
             sub="waits for both markers, emits the candidate superset per slot",
             fill=WHITE, border=BLACK, border_w=0.5, size=7)
    y = y_scan + 10
    doc.arrow_down(spine, y, y + 5.0, label="  immutable scanner evidence")
    y += 5.0

    doc.node(left + 16, y, width - 32, 9, "equity-1min-feed",
             sub="prewarms apps; +3s exact bars; fsync + create-once slot data",
             fill=WHITE, border=BLACK, border_w=0.5, size=6.7)
    y += 9
    doc.arrow_down(spine, y, y + 5.0, label="  final marker + slot parquet")
    y += 5.0

    doc.node(left + 16, y, width - 32, 10, "confirmation-1m",
             sub="read-only durable-feed consumer; confirms, ranks, selects",
             fill=WHITE, border=BLACK, border_w=0.5, size=7)
    y += 10
    doc.arrow_down(spine, y, y + 7.5, label="  signals/<date>/*.json")
    y += 7.5

    worker_w = (width - 24) / 2
    doc.node(left + 4, y, worker_w, 10, "long-entry", sub="PAPER or LIVE",
             fill=WHITE, border=BLACK, border_w=0.5, size=7)
    doc.node(left + width - 4 - worker_w, y, worker_w, 10, "short-entry",
             sub="PAPER or LIVE", fill=WHITE, border=BLACK, border_w=0.5, size=7)
    y += 10
    doc.elbow_arrow(left + 4 + worker_w / 2, y, spine, y + 7.5)
    doc.elbow_arrow(left + width - 4 - worker_w / 2, y, spine, y + 7.5)
    y += 7.5

    doc.node(left + 26, y, width - 52, 9, "orders/<MODE>/<date>/*.json",
             sub="immutable per-signal order state", fill=BOX, size=6.4)
    y += 9
    doc.elbow_arrow(spine, y, left + 4 + worker_w / 2, y + 7.5)
    doc.elbow_arrow(spine, y, left + width - 4 - worker_w / 2, y + 7.5)
    y += 7.5
    doc.node(left + 4, y, worker_w, 9, "trade-logger",
             sub="consolidated CSV + markdown", fill=BOX, size=6.4)
    doc.node(left + width - 4 - worker_w, y, worker_w, 9, "net-result",
             sub="realized / unrealized / ROC", fill=BOX, size=6.4)
    doc.set_y(y + 12)

    doc.h2("Weekday schedule (Mon-Fri)")
    doc.table(
        ["Time", "Task", "Role"],
        [15, 60, doc.usable - 75],
        [
            ["08:50", "run_fno_oi_universe.bat", "Near-month universe"],
            ["09:05", "run_fno_oi_fetch_5min.bat", "Futures 5-minute fetch + slot markers"],
            ["09:15", "run_fno_oi_feature_ranker.bat", "Feature ranking (observability only)"],
            ["09:15", "run_fno_v6_scanner_5min.bat", "scanner-5m"],
            ["09:15", "run_fno_v6_equity_1min_feed.bat", "durable completed 1m producer"],
            ["09:15", "run_fno_v6_confirmation_1min.bat", "confirmation-1m"],
            ["09:15", "run_fno_v6_live_long.bat / _short.bat", "long-entry / short-entry"],
            ["09:15", "run_fno_v6_trade_logger.bat / _net_result.bat", "trade-logger / net-result"],
            ["15:40", "run_fno_oi_eod_qc.bat", "End-of-day data QC"],
        ],
        mono_cols=(0, 1),
    )
    doc.para(
        "The installer disables the equivalent V5 tasks - V6 replaced V5 in production. Every "
        ".bat runner exports FNO_V6_EXECUTION_MODE=PAPER. Artefact root: <FNO_ROOT>/v6_live/ "
        "(see section 17)."
    )


def build_gates(doc: Doc) -> None:
    doc.add_page()
    doc.h1("10. Feed-Readiness Gates")
    doc.para(
        "The scanner will not read a slot until both upstream feeds have published a final, "
        "complete marker for that exact slot timestamp. This is the most important correctness "
        "gate in the live path: scanning a partially written feed produces a plausible-looking "
        "but wrong candidate list."
    )

    doc.h3("Start-up gate - every role")
    doc.code(
        "config.validate_strategy()         -> 10 legs, entry caps, confirmation map\n"
        "config.attest_selected_backtest()  -> frozen CSV + provenance hashes + metrics\n"
        "_write_manifest()                  -> strategy_manifest.json (payload + fingerprint)\n"
        "capital / leverage                 -> must equal 10,000 and 5.0\n"
        "trading-day check                  -> else SKIPPED_NON_TRADING_DAY"
    )
    doc.para(
        "The strategy fingerprint is the SHA-256 of the full strategy payload - setup book, "
        "gates, slots, cost, sizing and the locked futures-readiness policy. It is stamped on every scanner snapshot, confirmation "
        "snapshot, entry signal and order state, and re-checked at every handoff."
    )

    doc.h2("Futures marker  -  fno_oi/slot_ready/slot_<YYYYMMDD_HHMM>.json")
    doc.bullets([
        "schema v2, locked readiness policy, source == final, complete == true, exact slot,",
        "full and symbol-set hashes match the scanner's exact mapped stock-futures universe,",
        "recomputed stock coverage >= 99%; at most two named stock omissions,",
        "each admitted omission has at least three clean NO_CANDLE observations,",
        "API failures, invalid candles, foreign, unverified and unlisted missing symbols block,",
        "index-future NO_CANDLE outcomes do not reduce mapped-stock coverage.",
    ])

    doc.h2("Cash 5-minute marker  -  slot_ready_5m/slot_<YYYYMMDD_HHMM>.json")
    doc.bullets([
        "source == \"final\", complete == true, slot matches,",
        "tickers_written == tickers_complete == tickers_expected and tickers_failed == 0,",
        "fno_equity_quality_complete == true,",
        "fno_equity_ready == fno_equity_expected and fno_equity_failed == 0,",
        "fno_equity_universe_sha256 matches the scanner's own mapped equity set - so the feed "
        "cannot be complete for yesterday's universe.",
    ])
    doc.callout(
        "The 09:50 pipeline deadline",
        "If the scanner or confirmation role has not finished all five slots by 09:50 it "
        "publishes BLOCKED and exits. Downstream workers detect that state, publish "
        "UPSTREAM_BLOCKED and exit too - so a broken feed surfaces as one clear cause rather than "
        "six roles quietly reporting success while trading nothing.",
    )

    doc.h1("11. Scanner and Confirmation Slot Logic")
    doc.h3("scanner-5m")
    doc.para(
        "Waits until slot end + 3s and checks both markers. A marker-attested absent stock "
        "future is written as SKIPPED_NO_CANDLE before any data load; it is never synthesized, "
        "forward-filled or restored by a later backfill. Every other contract loads futures 5m "
        "and live equity 5m, joins OI, takes the exact slot row and applies the base gate. The "
        "snapshot is SUCCESS when only verified skips are absent; every unexpected absence or "
        "invalid contract remains PARTIAL."
    )
    doc.h3("confirmation-1m")
    doc.para(
        "A dedicated producer waits for each completed boundary, consumes the immutable scanner "
        "candidate set, fetches exact bars, persists and re-reads them, then atomically publishes "
        "a marker and compact slot parquet bound to scanner, candidate and bar-set hashes."
    )
    doc.bullets([
        "the marker carries exact written, verified/unverified no-candle, invalid, API-failed and "
        "unexpected symbol lists plus attempts and publication time;",
        "three clean empty observations are admitted only after a minimum publication age; a "
        "verified candidate becomes INELIGIBLE_NO_CANDLE, never a synthetic bar;",
        "confirmation is a read-only filesystem consumer and never calls historical_data;",
        "API/invalid/unverified/unexpected gaps block; complete written candidates are ranked.",
    ])
    doc.callout(
        "Durable evidence and no retroactive entries",
        "The producer and consumer both enforce confirmation_end + 90s. Scheduled, --once and "
        "explicit --slot paths check the deadline before publishing any signal. Equality is "
        "accepted; one microsecond late is BLOCKED_STALE_ACTIVATION. A verified no-trade "
        "candidate may be ineligible, but every unexplained data gap blocks the slot.",
    )
    doc.para(
        "Only signal IDs listed in a valid confirmation snapshot count. load_signals() reads that "
        "list first, ignores stray JSON files on disk, raises if a listed file is missing, and "
        "re-validates every field of every signal - deterministic ID, setup fields, sizing, rank "
        "within cap, instrument tokens, activation deadline - before a worker may act."
    )


def build_workers(doc: Doc) -> None:
    doc.add_page()
    doc.h1("12. Entry Workers - PAPER and LIVE")
    doc.para(
        "Poll loop at 1s: load authoritative signals for the side, create or load order state, "
        "quote LTPs in one batched call, advance each state."
    )

    left = doc.l_margin
    y = doc.get_y() + 1
    bw, bh, gx = 38.0, 9.5, 18.0
    x1 = left + 4
    x2 = x1 + bw + gx
    x3 = x2 + bw + gx
    doc.node(x1, y, bw, bh, "PENDING_ENTRY", sub="stop-entry armed",
             fill=WHITE, border=BLACK, border_w=0.5, size=6.6)
    doc.node(x2, y, bw, bh, "OPEN", sub="filled, bracket live",
             fill=WHITE, border=BLACK, border_w=0.5, size=6.6)
    doc.node(x3, y, bw, bh, "CLOSED", sub="stop / target / 15:30",
             fill=BOX, size=6.6)
    doc.arrow_right(x1 + bw, x2, y + bh / 2, label="LTP crosses trigger")
    doc.arrow_right(x2 + bw, x3, y + bh / 2, label="exit condition")

    y2 = y + bh + 11
    doc.node(x1, y2, bw, bh, "NO_FILL", sub="15:30, never touched", fill=BOX, size=6.6)
    doc.arrow_down(x1 + bw / 2, y + bh, y2)
    doc.node(x2, y2, bw, bh, "CANCELLED", sub="late start", fill=BOX, size=6.6)
    doc.node(x3, y2, bw, bh, "BLOCKED_SIZING", sub="quantity is zero", fill=BOX, size=6.6)
    doc.set_y(y2 + bh + 2)
    doc.para(
        "CANCELLED and BLOCKED_SIZING are entry-time terminal states: the signal was first seen "
        "after the activation deadline, or one unit already exceeds the Rs 50,000 budget. On "
        "fill, stop and target are recomputed from the observed fill price rather than from the "
        "trigger; on close the state charges entry notional x 5bps and records "
        "net_return_exposure_pct and return_on_capital_pct."
    )

    doc.h2("LIVE - four independent arming conditions")
    doc.bullets([
        "--execution-mode LIVE - the default is PAPER, and every .bat pins PAPER,",
        "environment variable FNO_V6_LIVE_ACK set to the exact acknowledgement string,",
        "live_arm.json with enabled: true AND today's session_date,",
        "kill_switch.json not enabled.",
    ])

    doc.h2("LIVE broker lifecycle")
    doc.table(
        ["Phase", "Action"],
        [42, doc.usable - 42],
        [
            ["PENDING_ENTRY", "Place SL-M at trigger_price, product MIS, tagged FV6<sha1[:14] of signal_id>"],
            ["Recovery", "Before placing anything, look for an existing order with the same tag, symbol, side and type - restarts adopt working orders instead of duplicating them"],
            ["Entry COMPLETE", "Recompute stop/target from average_price, adopt filled_quantity"],
            ["OPEN", "Place protective SL-M stop and LIMIT target, both tagged"],
            ["Stop filled", "Cancel the target, close at the stop's average price"],
            ["Target filled", "Cancel the stop, close at the target's average price"],
            ["Stop/target rejected", "Cancel siblings, send a MARKET square-off"],
            ["Kill switch, or 15:30", "Cancel siblings, send a MARKET square-off (SQUARE_OFF_PENDING)"],
            ["Disarmed while pending", "Cancel the entry order; if it filled meanwhile, adopt the fill"],
        ],
        mono_cols=(0,),
    )
    doc.para("Terminal states: CLOSED, NO_FILL, ENTRY_REJECTED, BLOCKED_SIZING, CANCELLED.")

    doc.h1("13. Reporting Roles")
    doc.para(
        "Both poll every 5 seconds until 15:32. trade-logger merges PAPER and LIVE order states "
        "into a 32-column CSV at consolidated/fno_v6_trades_<date>.csv plus a markdown table; it "
        "carries strategy_version and strategy_fingerprint on every row, so a day's trades can "
        "always be tied back to the exact book that produced them."
    )
    doc.table(
        ["net-result field", "Meaning"],
        [48, doc.usable - 48],
        [
            ["signals / pending / open / closed", "Counts by order state"],
            ["no_fill / cancelled / blocked", "Non-traded outcomes, split by cause"],
            ["capital_deployed_rs", "Sum of capital across signals that actually filled"],
            ["realized_net_rs", "Net P&L of CLOSED positions, after 5 bps"],
            ["unrealized_net_rs", "Marked net P&L of OPEN positions, after 5 bps"],
            ["total_net_rs / return_on_capital_pct", "realized + unrealized, and that over capital deployed"],
        ],
        mono_cols=(0,),
    )


def build_parity(doc: Doc) -> None:
    doc.add_page()
    doc.h1("14. Backtest vs Live Parity")
    doc.para("These differences are structural, not bugs.")
    doc.table(
        ["Aspect", "Backtest", "Live", "Impact"],
        [28, 42, 42, doc.usable - 112],
        [
            ["5m equity bars", "built from 5x1m aggregation", "read from the live 5m store",
             "Construction differs; both admit completed real bars only"],
            ["Fill detection", "first forward 1m bar through the trigger",
             "PAPER: polled LTP crossing. LIVE: broker SL-M",
             "PAPER can fill at a worse or better print than the trigger"],
            ["Bracket basis", "stop/target from the trigger", "stop/target from the actual fill",
             "Live brackets shift with slippage"],
            ["First fillable bar", "the bar after the confirmation candle",
             "any tick after the worker sees the signal", "Live can engage sooner"],
            ["Same-bar stop+target", "stop wins (pessimistic)", "whichever is hit first",
             "Live may be luckier than the backtest"],
            ["Cost", "5 bps on gross return", "5 bps on entry notional", "Equivalent to ~0.05%"],
            ["Sizing", "not modelled - equal-weight % returns",
             "floor(50000/price), lot-rounded in LIVE",
             "Rupee P&L is not the additive % sum when prices differ widely"],
            ["Missing futures bar", "skip that contract; process the rest",
             "3 clean NO_CANDLE checks -> SKIPPED_NO_CANDLE; other absence blocks",
             "Contract-level parity without fabricating a bar"],
        ],
        size=6.3,
    )
    doc.callout(
        "How to compare the two honestly",
        "The backtest is optimistic on fills and pessimistic on same-bar ties. Compare order "
        "counts and fill rates first - they depend on the gates, which are shared code, and "
        "should track closely. Compare hit rate second. Compare rupee P&L last, and only across "
        "enough sessions that a single 3% target does not dominate the sample.",
    )
    doc.callout(
        "Completeness/deadline parity replay",
        "Live archives append-only marker, scanner, durable 1-minute feed and confirmation "
        "revisions. Observed mode selects the earliest causally available immutable evidence "
        "and fails on gaps; counterfactual mode may use the latest repaired revision but is "
        "always labelled. The 2026-08-17 SAIL repair is counterfactual because the original "
        "as-seen futures marker was overwritten before this evidence control existed.",
    )

    doc.h1("15. Safety Rails")
    rails = [
        ("Fingerprint chaining",
         "scanner -> confirmation -> signal -> order state all carry strategy_version and "
         "strategy_fingerprint; a mismatch aborts rather than degrades."),
        ("Backtest attestation",
         "dated universe, source inventory, cache/output hashes, protected provenance, frozen CSV and metrics."),
        ("Feed-readiness gate",
         "exact-slot markers, mapped-stock hashes, at least 99% coverage, at most two named "
         "omissions and three clean checks per omission."),
        ("Durable confirmation evidence",
         "fingerprinted +3-second completed boundary; fsync'd create-once exact bars; verified "
         "no-trade is ineligible, while unexplained gaps block."),
        ("90-second activation deadline",
         "no retroactive entries after a late start; early/manual retries remain WAITING without "
         "poisoning the slot."),
        ("09:50 pipeline deadline",
         "an incomplete pipeline blocks instead of trading a half-built book."),
        ("PAPER by default",
         "LIVE needs mode plus the exact ack environment variable plus a same-day arm file plus "
         "no kill switch."),
        ("Kill switch",
         "flips open LIVE positions to a market square-off on the next poll."),
        ("Tag-based order recovery",
         "restarts adopt working broker orders instead of duplicating them."),
        ("Locked sizing",
         "Rs 10,000 x 5x only; any other capital or leverage raises immediately."),
    ]
    doc.table(
        ["#", "Rail", "What it does"],
        [7, 42, doc.usable - 49],
        [[str(i), title, body] for i, (title, body) in enumerate(rails, start=1)],
        size=6.4,
        bold_cols=(1,),
    )
    doc.callout(
        "The rails encode one principle",
        "Every gate in this list fails closed. A missing bar, a stale marker, a drifted config or "
        "a late restart all produce fewer trades or no trades - never a different trade. That is "
        "the property worth protecting when changing this code: a change that turns a hard "
        "failure into a warning has removed a rail, whatever else it improved.",
    )


def build_operations(doc: Doc) -> None:
    doc.add_page()
    doc.h1("16. Operations")

    doc.h3("Backtest")
    doc.code(
        "python fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py\n"
        "python fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py --through-day 2026-08-11\n"
        "python fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py --rebuild-cache"
    )
    doc.para(
        "Arguments: --split-day (TRAIN/TEST labels, default 2026-07-17), --cost-bps (5.0), "
        "--square-off (1530), --max-forward-bars (400), --rebuild-cache (required after a "
        "data-contract change)."
    )

    doc.h3("Live and paper roles")
    doc.code(
        "bat\\run_fno_v6_scanner_5min.bat          bat\\run_fno_v6_live_long.bat\n"
        "bat\\run_fno_v6_equity_1min_feed.bat      bat\\run_fno_v6_live_short.bat\n"
        "bat\\run_fno_v6_confirmation_1min.bat\n"
        "bat\\run_fno_v6_trade_logger.bat          bat\\run_fno_v6_net_result.bat"
    )

    doc.h3("Single-slot live diagnostics (deadline still enforced)")
    doc.code(
        "python fno_v6_live.py --role scanner-5m      --slot 0925 --session-date 2026-08-13\n"
        "python fno_v6_live.py --role confirmation-1m --slot 0926 --session-date 2026-08-13"
    )
    doc.h3("Evidence-only completeness/deadline replay")
    doc.code(
        "python fno_v6_parity_replay.py --session-date 2026-08-18 --mode observed --slot all --strict\n"
        "python fno_v6_parity_replay.py --session-date 2026-08-17 --mode counterfactual --slot all"
    )

    doc.h3("Schedule installation (also disables the V5 tasks)")
    doc.code("powershell -ExecutionPolicy Bypass -File bat\\schedule_fno_oi_weekday.ps1")

    doc.h2("Live runtime arguments")
    doc.table(
        ["Argument", "Default", "Effect"],
        [40, 22, doc.usable - 62],
        [
            ["--role", "(required)", "scanner-5m | confirmation-1m | long-entry | short-entry | trade-logger | net-result"],
            ["--execution-mode", "PAPER", "PAPER or LIVE; env FNO_V6_EXECUTION_MODE sets the default"],
            ["--session-date", "today", "ISO date; used for replay of a past session"],
            ["--slot", "(all)", "Process one slot only; forces --once for scanner and confirmation"],
            ["--once", "off", "Single pass instead of the poll loop"],
            ["--poll-sec", "1.0", "Worker/scanner poll interval"],
            ["--reporting-poll-sec", "5.0", "Trade-logger and net-result interval"],
            ["--boundary-buffer-sec", "3.0", "Wait after a slot boundary before reading"],
            ["--confirmation-max-wait-sec", "90.0", "Staleness cut-off for a confirmation slot"],
            ["--max-apps", "8", "Kite credential pool size"],
            ["--max-retries / --timeout-sec", "3 / 8.0", "Per-bar fetch retries and HTTP timeout"],
            ["--capital / --leverage", "10000 / 5.0", "Locked - any other value raises"],
            ["--allow-non-trading-day", "off", "Bypass the holiday check"],
            ["--ignore-fetch-marker", "off", "DIAGNOSTIC ONLY - bypasses the feed-readiness gate"],
        ],
        size=6.3,
        mono_cols=(0, 1),
    )
    doc.callout(
        "Flag to use with care",
        "--ignore-fetch-marker bypasses the gate described in section 10. It exists for offline "
        "diagnosis of a single slot. Never leave it in a scheduled task - it removes the check "
        "that stops the scanner from reading a half-written feed.",
    )


def build_inventory_code(doc: Doc) -> None:
    doc.add_page()
    doc.h1("17. File and Artefact Inventory")
    doc.para(
        "Everything the strategy loads, runs, reads or writes. Python modules live in the project "
        "directory (...\\backtesting\\eqidv2\\); runners live in its bat\\ subdirectory; runtime "
        "data lives under C:\\TradingData\\eqidv2\\ (env EQIDV2_RUNTIME_ROOT)."
    )

    doc.h2("17.1  Live-path modules  (loaded during a trading session)")
    doc.files_table([
        ["fno_v6_live.py", "Entry point. Sets FNO_LIVE_GENERATION=v6 and delegates to the shared runtime."],
        ["fno_v5_live.py", "Shared runtime for all six roles: scanning, confirmation, order state machines, reports."],
        ["fno_equity_fetch_1min.py", "Dedicated exact completed candidate-bar producer; writes immutable marker + slot parquet."],
        ["fno_live_evidence.py", "Append-only as-observed evidence envelopes for live/replay gates."],
        ["fno_v6_parity_replay.py", "Evidence-only observed/counterfactual completeness and deadline replay."],
        ["fno_v6_live_config.py", "V6 live config: setup book re-export, slots, sizing, fingerprint, backtest attestation."],
        ["fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py", "Frozen ACTIVE_SETUPS (the setup book) and the V6 replay."],
        ["fno_v5_live_config.py", "Imported by V6 for the SetupSpec and PositionSize dataclasses; also the V5 book."],
        ["fno_oi_hybrid_data.py", "Data contract: equity mapping, bar-quality filters, 1m->5m aggregation, OI join."],
        ["fno_oi_common.py", "Paths, IST clock, atomic writes, status/heartbeat, Kite credentials, holidays, universe loader."],
        ["fno_oi_ema_confirm_backtest.py", "load_five_minute() for the futures raw 5-minute parquet."],
        ["eqidv2_runtime_paths.py", "Resolves the runtime root and every data directory from environment variables."],
    ])

    doc.h2("17.2  Backtest-path modules")
    doc.files_table([
        ["fno_oi_ema_confirm_optimize.py", "Owns the signal cache and its manifest invalidation (load_signals)."],
        ["fno_oi_backtest_provenance.py", "Dated-universe verification, source inventory, cache/output hashes and immutable run provenance."],
        ["fno_oi_ema_confirm_sweep.py", "build_signal_table() - candidate superset + forward paths; simulate_bracket()."],
        ["fno_v5_hybrid_backtest.py", "replay_setups, select_setup_rows, build_daily_curve, summary_stats."],
        ["fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py", "validate_cash_equity_signal_contract + the full-history optimizer that selected V6."],
        ["fno_v5_hybrid_optimize.py", "Leg grid search, OptimizerGuards, beam_portfolios - the selection engine."],
    ])

    doc.h2("17.3  Transitive imports  (pulled in at import time, not called directly)")
    doc.files_table([
        ["fno_oi_ema_confirm_0925_0930_0935_v4.py", "V4 leg-choice helpers reused by the V5 optimizer."],
        ["fno_oi_ema_confirm_0925_0930_pf_v3.py", "V3 optimiser primitives (Candidate, scoring) used by V4 and V5."],
        ["fno_oi_ema_confirm_0925_0930_best_combo_v3.py", "Imported by V4."],
    ])

    doc.h2("17.4  Upstream data producers  (separate scheduled jobs)")
    doc.files_table([
        ["fno_oi_universe.py", "Near-month universe + instrument master; writes latest_near_month.parquet."],
        ["fno_oi_fetch_5min.py", "Futures 5-minute fetch -> raw_contracts_5m + fno_oi/slot_ready markers."],
        ["eqidv2_eod_scheduler_for_5mins_data_live_minimal.py", "Cash 5-minute feed scheduler; writes slot_ready_5m markers incl. the fno_equity_* fields."],
        ["trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py", "Core cash fetch / indicator writer driven by that scheduler."],
        ["fno_oi_feature_ranker.py", "OI rankings and leaderboard. Observability only - V6 does not read its output."],
        ["fno_oi_eod_qc.py", "End-of-day data quality check, 15:40."],
    ])

    doc.h2("17.5  Backfill and repair utilities  (ad-hoc, not scheduled)")
    doc.files_table([
        ["fno_oi_backfill_5min.py", "Backfills futures 5-minute history into raw_contracts_5m."],
        ["fno_oi_backfill_daily.py", "Backfills daily futures/OI history."],
        ["fno_oi_fetch_1m_history.py", "Fetches 1-minute futures history."],
        ["fno_equity_1m_backfill.py", "Backfills cash-equity 1-minute history - the backtest's price source."],
        ["fno_equity_5m_repair.py", "Repairs gaps in the cash-equity 5-minute store."],
    ])

    doc.h2("17.6  Research modules outside the live path")
    doc.files_table([
        ["fno_v5_0926_day_pf_optimize.py", "Train-only day-PF search restricted to the 09:25 -> 09:26 leg."],
        ["fno_v5_0926_all_history_day_pf_optimize.py", "All-history ceiling for the same leg - explicitly not out-of-sample."],
        ["fno_v5_hybrid_optimize.py", "Also runnable standalone as the train-only optimizer."],
        ["generate_fno_v6_reference_pdf.py", "Generates this document."],
        ["FNO_V6_STRATEGY_AND_FLOWS.md", "Markdown companion to this document."],
    ])


def build_inventory_data(doc: Doc) -> None:
    doc.add_page()
    doc.h1("17. File and Artefact Inventory  (continued)")

    doc.h2("17.7  Runners and scheduling  (bat\\)")
    doc.table(
        ["Runner", "Starts"],
        [58, doc.usable - 58],
        [
            ["run_fno_v6_scanner_5min.bat", "fno_v6_live.py --role scanner-5m (sets EQIDV2_DATA_5M_DIR to ...5min_eq_live)"],
            ["run_fno_v6_equity_1min_feed.bat", "fno_equity_fetch_1min.py --generation v6"],
            ["run_fno_v6_confirmation_1min.bat", "fno_v6_live.py --role confirmation-1m"],
            ["run_fno_v6_live_long.bat", "fno_v6_live.py --role long-entry"],
            ["run_fno_v6_live_short.bat", "fno_v6_live.py --role short-entry"],
            ["run_fno_v6_trade_logger.bat", "fno_v6_live.py --role trade-logger"],
            ["run_fno_v6_net_result.bat", "fno_v6_live.py --role net-result"],
            ["run_fno_oi_universe.bat", "fno_oi_universe.py (08:50)"],
            ["run_fno_oi_fetch_5min.bat", "fno_oi_fetch_5min.py (09:05)"],
            ["run_fno_oi_feature_ranker.bat", "fno_oi_feature_ranker.py (09:15)"],
            ["run_fno_oi_eod_qc.bat", "fno_oi_eod_qc.py (15:40)"],
            ["run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat", "Cash 5-minute live feed"],
            ["schedule_fno_oi_weekday.ps1 / .bat", "Installs all weekday tasks; disables the V5 tasks"],
            ["harden_scheduled_task.ps1", "Applies task hardening after each schtasks create"],
        ],
        size=6.3,
        mono_cols=(0,),
    )

    doc.h2("17.8  Credentials and static config  (project directory)")
    doc.table(
        ["File", "Purpose"],
        [58, doc.usable - 58],
        [
            ["api_key.txt / access_token.txt", "Kite app 1 credentials"],
            ["api_key2..8.txt / access_token2..8.txt", "Additional apps for the parallel 1-minute fetch pool"],
            ["nse_holidays.csv", "Trading-day calendar; a missing file means no holidays are known"],
            ["stocks_tokens_cache.json", "Legacy equity instrument-token fallback for the mapping layer"],
        ],
        size=6.3,
        mono_cols=(0,),
    )

    doc.h2("17.9  Environment variables")
    doc.table(
        ["Variable", "Default / value used", "Effect"],
        [52, 46, doc.usable - 98],
        [
            ["EQIDV2_RUNTIME_ROOT", "C:\\TradingData\\eqidv2", "Root of every runtime data directory"],
            ["EQIDV2_DATA_5M_DIR", "...\\stocks_indicators_5min_eq_live", "Live 5m equity store; the scanner .bat overrides the ..._live2 default"],
            ["EQIDV2_DATA_1MIN_DIR", "...\\stocks_indicators_1min_eq", "Backtest 1-minute equity source"],
            ["EQIDV2_RUNTIME_STATUS_DIR", "...\\runtime_status", "Status and heartbeat files, off the OneDrive-synced tree"],
            ["EQIDV2_FNO_V5_BACKTEST_EQUITY_1M_DIR", "...\\stocks_indicators_1min_eq", "Overrides the backtest 1m source"],
            ["EQIDV2_FNO_V5_BACKTEST_EQUITY_5M_DIR", "...\\stocks_indicators_5min_eq_live2", "Overrides the backtest 5m dir (unused when aggregating from 1m)"],
            ["FNO_LIVE_GENERATION", "v6", "Selects the config module; set by fno_v6_live.py"],
            ["FNO_V6_EXECUTION_MODE", "PAPER", "Default --execution-mode; every .bat pins PAPER"],
            ["FNO_V6_LIVE_ACK", "(unset)", "Must equal I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS to arm LIVE"],
        ],
        size=6.2,
        mono_cols=(0, 1),
    )

    doc.h2("17.10  Input data read by the strategy, and who writes it")
    doc.table(
        ["Path (under C:\\TradingData\\eqidv2\\)", "Read by", "Written by"],
        [70, 54, doc.usable - 124],
        [
            ["fno_oi\\universe\\latest_near_month.parquet", "Live scanner - rolling tradable universe", "fno_oi_universe.py"],
            ["fno_oi\\universe\\near_month_2026-08-11.parquet", "Promoted V6 backtest - frozen dated universe", "fno_oi_universe.py"],
            ["fno_oi\\universe\\latest_universe_summary.json", "Operator / QC", "fno_oi_universe.py"],
            ["fno_oi\\instrument_master\\", "Universe builder - contract registry", "fno_oi_universe.py"],
            ["fno_oi\\raw_contracts_5m\\<CONTRACT>_5minute.parquet", "Futures OI 5m bars (live + backtest)", "fno_oi_fetch_5min.py"],
            ["fno_oi\\slot_ready\\slot_<YYYYMMDD_HHMM>.json", "Scanner - futures feed marker", "fno_oi_fetch_5min.py"],
            ["slot_ready_5m\\slot_<YYYYMMDD_HHMM>.json", "Scanner - cash feed marker", "eqidv2_eod_scheduler_..._live_minimal.py"],
            ["stocks_indicators_5min_eq_live\\<SYM>_..._5min.parquet", "Scanner - live 5m equity bars", "eqidv2_eod_scheduler_..._live_minimal.py"],
            ["stocks_indicators_1min_eq\\<SYM>_..._1min.parquet", "Backtest - 5m source + forward paths", "cash 1m feed / fno_equity_1m_backfill.py"],
            ["fno_oi\\raw_equity_1m\\<date>\\<SYM>_1minute.parquet", "Durable confirmation producer/consumer", "fno_equity_fetch_1min.py"],
            ["fno_oi\\equity_1m_slot_ready\\v6\\<date>\\slot_*.json/.parquet", "Confirmation gate + parity replay", "fno_equity_fetch_1min.py"],
            ["stocks_indicators_5min_eq_live2\\", "Default backtest 5m dir - bypassed", "cash 5m feed"],
        ],
        size=6.1,
        mono_cols=(0,),
    )


def build_inventory_outputs(doc: Doc) -> None:
    doc.add_page()
    doc.h1("17. File and Artefact Inventory  (continued)")

    doc.h2("17.11  Backtest outputs")
    doc.table(
        ["Path (under C:\\TradingData\\eqidv2\\fno_oi\\)", "Contents"],
        [92, doc.usable - 92],
        [
            ["strategy_research\\_signal_cache_equity_1m_aggregated_5m_futures_oi_v4\\", "signals.parquet + paths.npz with verified artifact hashes; manifest pins dated universe and exact source-file SHA inventory"],
            ["strategy_research\\ema_confirm_0925_0930_0935_0940_0945_v6_best_net_daily.csv", "Per-session curve: selections, fills, day %, cumulative PF"],
            ["  ..._v6_best_net_trades.csv", "Per-order audit: symbol, trigger, filled, net_return_pct, setup_id"],
            ["  ..._v6_best_net_setups.csv", "Per-leg summary: orders, fills, PF, net %"],
            ["  ..._v6_best_net_selected_20260811.csv", "Legacy historical curve; preserved unchanged, source inventory unavailable"],
            ["  ..._selected_current_source_20260818_v1.csv", "Protected versioned current-source curve read at live start-up"],
            ["  ..._selected_current_source_20260818_v1.provenance.json", "Pinned strategy/input/output provenance; explicitly not the original source inventory"],
            ["  fno_v6_legacy_selected_mismatch_audit_20260818.json", "Immutable three-session legacy/current mismatch record"],
            ["latest\\latest_fno_oi_ema_confirm_v6_best_net.md", "Human-readable backtest report incl. day-wise entries"],
            ["latest\\latest_fno_v6_0926_only.md", "09:26-entries-only slice of the trades CSV (no script in the repo writes this)"],
        ],
        size=6.1,
        mono_cols=(0,),
    )

    doc.h2("17.12  Live outputs  (under fno_oi\\v6_live\\ unless noted)")
    doc.table(
        ["Path", "Contents"],
        [80, doc.usable - 80],
        [
            ["strategy_manifest.json", "Strategy payload + fingerprint + attestation, written at every start-up"],
            ["live_arm.json", "OPERATOR INPUT - enables LIVE for one session date"],
            ["kill_switch.json", "OPERATOR INPUT - forces market square-off of open LIVE positions"],
            ["scanner_5m\\<date>\\slot_HHMM.json", "Candidate superset per slot + state SUCCESS / PARTIAL"],
            ["confirmation_1m\\<date>\\slot_HHMM.json", "Confirmation result, selected_signal_ids, errors, state"],
            ["evidence\\<date>\\slot_HHMM\\*.json", "Append-only as-observed marker/scanner/confirmation revisions for parity replay"],
            ["signals\\<date>\\<signal_id>.json", "One authoritative entry signal: trigger, stop, target, sizing"],
            ["orders\\PAPER\\<date>\\<signal_id>.json", "Paper order state machine"],
            ["orders\\LIVE\\<date>\\<signal_id>.json", "Live order state machine incl. broker order IDs"],
            ["..\\equity_1m_slot_ready\\v6\\<date>\\slot_*.json/.parquet", "Immutable durable confirmation marker and exact completed bar set"],
            ["consolidated\\fno_v6_trades_<date>.csv", "32-column merged PAPER+LIVE trade log"],
            ["fno_oi\\latest\\latest_fno_v6_scanner_5min.md  /  _confirmation_1min.md", "Scanner slot states; confirmation results + selected entries"],
            ["fno_oi\\latest\\latest_fno_v6_live_long.md  /  _live_short.md", "Per-side order tables"],
            ["fno_oi\\latest\\latest_fno_v6_trade_logger.md  /  _net_result.md", "Continuous trade log; net result by execution mode"],
        ],
        size=6.1,
        mono_cols=(0,),
    )

    doc.h2("17.13  Status, heartbeat and log files")
    doc.table(
        ["Path", "Contents"],
        [80, doc.usable - 80],
        [
            ["runtime_status\\fno_v6_scanner_5min.status  /  _confirmation_1min.status", "Role state, phase, reason, PID, fingerprint (each with a .heartbeat twin)"],
            ["runtime_status\\fno_v6_live_long.status  /  _live_short.status", "Per-side worker state and order counts (+ .heartbeat)"],
            ["runtime_status\\fno_v6_trade_logger.status  /  _net_result.status", "Reporter state and summary counters (+ .heartbeat)"],
            ["<project>\\logs\\fno_v6_scanner_5min.log  /  _confirmation_1min.log", "stdout/stderr of the scanner and confirmation .bat runners"],
            ["<project>\\logs\\fno_v6_live_long.log  /  _live_short.log", "Worker logs"],
            ["<project>\\logs\\fno_v6_trade_logger.log  /  _net_result.log", "Reporter logs"],
        ],
        size=6.1,
        mono_cols=(0,),
    )

    doc.h2("17.14  Observability artefacts  (written by upstream jobs, never read by V6)")
    doc.table(
        ["Path", "Written by"],
        [80, doc.usable - 80],
        [
            ["fno_oi\\rankings\\, ranking_ready\\, latest\\latest_fno_oi_rankings.csv", "fno_oi_feature_ranker.py"],
            ["fno_oi\\latest\\latest_fno_oi_leaderboard.md, latest_fno_oi_candidates_shadow.csv", "fno_oi_feature_ranker.py"],
            ["fno_oi\\eod_qc\\, latest\\latest_fno_oi_eod_qc.md / .csv", "fno_oi_eod_qc.py"],
            ["fno_oi\\latest\\latest_fno_oi_fetch.md, latest_fno_oi_universe.md", "Fetch and universe jobs"],
        ],
        size=6.1,
        mono_cols=(0,),
    )

    doc.ln(1)
    doc.rule()
    doc.set_font("Helvetica", "", 6.2)
    doc.set_text_color(*MUTE)
    doc.multi_cell(
        0, 3.4,
        "Regenerate this PDF with:  python generate_fno_v6_reference_pdf.py    |    "
        "Markdown companion: FNO_V6_STRATEGY_AND_FLOWS.md",
        align="C",
    )
    doc.set_text_color(*INK)


def main() -> None:
    doc = Doc(orientation="P", unit="mm", format="A4")
    doc.set_auto_page_break(auto=True, margin=14)
    doc.set_margins(left=12, top=10, right=12)
    doc.set_title(TITLE)
    doc.set_author("eqidv2 backtesting project")
    doc.set_subject("FnO V6 EMA/OI opening-window strategy - specification, flows and file inventory")

    build_cover(doc)
    build_architecture(doc)
    build_confirmation(doc)
    build_execution(doc)
    build_backtest_flow(doc)
    build_backtest_detail(doc)
    build_live_architecture(doc)
    build_gates(doc)
    build_workers(doc)
    build_parity(doc)
    build_operations(doc)
    build_inventory_code(doc)
    build_inventory_data(doc)
    build_inventory_outputs(doc)

    doc.output(str(OUT_PATH))
    print(f"[DONE] {OUT_PATH}  ({OUT_PATH.stat().st_size / 1024:.1f} KB, {doc.page_no()} pages)")


if __name__ == "__main__":
    main()
