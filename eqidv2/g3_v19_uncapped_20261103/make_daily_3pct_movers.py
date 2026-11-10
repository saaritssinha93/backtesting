"""Build an observed intraday mover catalogue from sealed G3 minute history.

Read-only with respect to source data; no broker or market calls.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

from g3_v18_20261102.data import available_days, load_day


OUTPUT = Path(__file__).with_name("FULL_HISTORY_DAILY_3PCT_MOVERS.md")
THROUGH = "2026-11-03"


def _display(table: pd.DataFrame, side: str) -> list[str]:
    if table.empty:
        return ["No stock reached 3% in this direction."]
    lines = ["| Rank | Stock | Move from 09:16 open | 09:16 open | Extreme price | Time of extreme | Close vs open |",
             "|---:|:---|---:|---:|---:|:---:|---:|"]
    for i, row in enumerate(table.itertuples(), 1):
        move = row.up_pct if side == "long" else row.down_pct
        extreme = row.high if side == "long" else row.low
        when = row.high_time if side == "long" else row.low_time
        lines.append(f"| {i} | {row.tradingsymbol} | {move:+.2f}% | {row.open:.2f} | "
                     f"{extreme:.2f} | {when:%H:%M} | {row.close_pct:+.2f}% |")
    return lines


def main() -> None:
    days = available_days(THROUGH)
    if not days:
        raise ValueError("No sealed G3 sessions available")
    sections = []
    covered = 0
    long_days = short_days = 0
    qualifiers = Counter()
    source_types = Counter()
    coverage_notes = []
    for day in days:
        features, minutes, proof = load_day(day)
        source_types[proof["loader_source"]] += 1
        part = minutes.sort_values(["tradingsymbol", "ts"])
        grouped = part.groupby("tradingsymbol", sort=True)
        summary = grouped.agg(open=("open", "first"), high=("high", "max"),
                              low=("low", "min"), close=("close", "last"),
                              count=("ts", "size")).reset_index()
        if not summary["count"].eq(360).all():
            raise ValueError(f"Incomplete minute data in {day}")
        high_index = grouped["high"].idxmax()
        low_index = grouped["low"].idxmin()
        high_times = part.loc[high_index, ["tradingsymbol", "ts"]].set_index("tradingsymbol")["ts"]
        low_times = part.loc[low_index, ["tradingsymbol", "ts"]].set_index("tradingsymbol")["ts"]
        summary["high_time"] = summary.tradingsymbol.map(high_times)
        summary["low_time"] = summary.tradingsymbol.map(low_times)
        summary["up_pct"] = (summary.high / summary.open - 1) * 100
        summary["down_pct"] = (summary.low / summary.open - 1) * 100
        summary["close_pct"] = (summary.close / summary.open - 1) * 100
        longs = summary.loc[summary.up_pct >= 3].sort_values(
            ["up_pct", "tradingsymbol"], ascending=[False, True]).head(5)
        shorts = summary.loc[summary.down_pct <= -3].sort_values(
            ["down_pct", "tradingsymbol"], ascending=[True, True]).head(5)
        eligible_longs = int(summary.up_pct.ge(3).sum())
        eligible_shorts = int(summary.down_pct.le(-3).sum())
        assert len(longs) == min(5, eligible_longs)
        assert len(shorts) == min(5, eligible_shorts)
        covered += len(summary)
        long_days += bool(eligible_longs)
        short_days += bool(eligible_shorts)
        qualifiers["long"] += eligible_longs
        qualifiers["short"] += eligible_shorts
        breadth = proof.get("breadth_universe", {}).get("count")
        if breadth is not None and breadth != len(summary):
            coverage_notes.append(f"{day}: {len(summary)} complete minute histories; "
                                  f"{breadth} stocks in the dated/research universe")
        date_label = datetime.strptime(day, "%Y-%m-%d").strftime("%d %b %Y (%a)")
        lines = [f"## {date_label}", "",
                 f"Complete 1-minute stock histories: **{len(summary)}**. "
                 f"Reached +3%: **{eligible_longs}**; reached -3%: **{eligible_shorts}**.", "",
                 "### Top 5 long moves", ""]
        lines += _display(longs, "long")
        lines += ["", "### Top 5 short moves", ""]
        lines += _display(shorts, "short")
        sections.append("\n".join(lines))

    header = ["# G3 full-history daily stock movers: 3% or greater", "",
              f"**Coverage:** {len(days)} sealed G3 sessions, {days[0]} to {days[-1]}; "
              f"{covered:,} stock-session histories. No complete sealed session for 2026-11-03.", "",
              "**Definition:** For each stock, take the first available **09:16 IST 1-minute open**. "
              "Long move = (day's highest 1-minute high / that open - 1) x 100. "
              "Short move = (day's lowest 1-minute low / that open - 1) x 100. "
              "Rank each direction separately by absolute move, show up to five stocks reaching "
              "+3% or -3%, and break ties by symbol. The extreme time is the first minute when "
              "that price occurred. Close vs open uses the last available 15:15 minute close.", "",
              "**Scope:** All stocks with 360 complete, validated 09:16-15:15 minute bars "
              "in each day's sealed G3 source; this is the G3 historical stock cohort, "
              "not every NSE-listed security. The 09:15 cash-opening minute is absent "
              "from this replay source. Early cached cohorts are historically selected "
              "and not strictly point-in-time universes. These are full-day observed "
              "extremes, not prices known at entry time or realized trading returns. "
              "A stock may appear in both directions if it crossed both thresholds.", "",
              f"**Threshold counts:** +3% on {long_days}/{len(days)} days "
              f"({qualifiers['long']:,} stock-days); -3% on {short_days}/{len(days)} days "
              f"({qualifiers['short']:,} stock-days).", "",
              "**Source:** Hash-verified `g3_v18_20261102.data.load_day()` sealed G3 "
              "minute history; generated by `g3_v19_uncapped_20261103/make_daily_3pct_movers.py`. "
              "No Kite fetch or interpolation was used.", ""]
    if coverage_notes:
        header += ["**Coverage differences:** The minute source and the dated/research "
                   "cohort differ on these sessions:", ""]
        header += [f"- {note}" for note in coverage_notes]
        header += [""]
    header += ["---", ""]
    OUTPUT.write_text("\n".join(header) + "\n\n---\n\n".join(sections) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT}: {len(days)} days, {covered} stock-days; "
          f"long qualifiers {qualifiers['long']}, short qualifiers {qualifiers['short']}; "
          f"coverage notes {len(coverage_notes)}; source types {dict(source_types)}")


if __name__ == "__main__":
    main()
