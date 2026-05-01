"""Per-ticker ASCII timeline tree for V16 5min.

Reads timeline_wide_<date>.csv produced by timeline_v16_5min.py and emits
per_ticker_tree_<date>.txt — one block per ticker showing every signal's
lifecycle stages with timestamps and per-stage lag.

Usage:
  python timeline_per_ticker_tree.py --date 2026-04-27
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_REPORTS_DIR = SCRIPT_DIR / "reports"


def _to_ts(s) -> Optional[pd.Timestamp]:
    if s is None or (isinstance(s, float) and pd.isna(s)) or s == "":
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce", utc=False)
        if pd.isna(ts):
            return None
        if ts.tz is None:
            ts = ts.tz_localize("Asia/Kolkata")
        else:
            ts = ts.tz_convert("Asia/Kolkata")
        return ts
    except Exception:
        return None


def _hms(ts: Optional[pd.Timestamp]) -> str:
    return ts.strftime("%H:%M:%S") if ts is not None else "        "


def _short_sid(sid: str) -> str:
    s = str(sid or "").strip()
    if not s:
        return "-"
    return s[:8] + "…" if len(s) > 8 else s


def _short_synth(synth: str) -> str:
    s = str(synth or "")
    if not s.startswith("PREPOOL::"):
        return ""
    return "prepool"


def _fmt_lag(seconds) -> str:
    if seconds is None or seconds == "" or pd.isna(seconds):
        return ""
    try:
        s = float(seconds)
    except Exception:
        return ""
    if s == 0:
        return "+0s"
    if abs(s) < 60:
        return f"{'+' if s >= 0 else ''}{s:.0f}s"
    if abs(s) < 3600:
        return f"{'+' if s >= 0 else ''}{s/60:.1f}m"
    return f"{'+' if s >= 0 else ''}{s/3600:.2f}h"


def _fmt_money(v) -> str:
    if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
        return ""
    try:
        f = float(v)
    except Exception:
        return ""
    return f"Rs.{f:+.2f}"


def _stage_rows(row: pd.Series) -> list[tuple[Optional[pd.Timestamp], str, str]]:
    """Return populated stages as (ts, label, detail)."""
    out: list[tuple[Optional[pd.Timestamp], str, str]] = []

    bar = _to_ts(row.get("signal_bar_close"))
    slot = _to_ts(row.get("entry_slot"))
    if bar is not None:
        out.append((bar, "BAR_CLOSE", f"5min bar close{(' | entry_slot=' + _hms(slot)) if slot is not None else ''}"))

    if (ts := _to_ts(row.get("pool_written_ts"))) is not None:
        out.append((ts, "POOL_WRITTEN", "SE pushed to pool"))

    if (ts := _to_ts(row.get("pf_verified_ts"))) is not None:
        out.append((ts, "PF_VERIFIED", "data fetcher confirmed bar"))

    if (ts := _to_ts(row.get("de_passed_ts"))) is not None:
        delag = row.get("de_lag_entry_slot_sec", "")
        detail = f"DE entry-slot lag={delag}s" if delag not in ("", None) else "DE accepted"
        out.append((ts, "DE_PASSED", detail))

    if (ts := _to_ts(row.get("dispatched_ts"))) is not None:
        out.append((ts, "DISPATCHED", "written to signals_*.csv"))

    if (ts := _to_ts(row.get("entry_filled_ts"))) is not None:
        fp = row.get("filled_price", "")
        out.append((ts, "ENTRY_FILLED", f"@{fp}" if fp not in ("", None) else ""))

    if (ts := _to_ts(row.get("late_skipped_ts"))) is not None:
        lag = row.get("late_skip_lag_sec", "")
        thr = row.get("late_skip_threshold_sec", "")
        out.append((ts, "LATE_SKIPPED", f"lag={lag}s vs threshold={thr}s"))

    if (ts := _to_ts(row.get("exit_ts"))) is not None:
        outcome = row.get("outcome", "")
        xp = row.get("exit_price", "")
        pnl_rs = row.get("pnl_rs", "")
        pnl_pct = row.get("pnl_pct", "")
        bits = [str(outcome)] if outcome not in ("", None) else []
        if xp not in ("", None):
            bits.append(f"@{xp}")
        if pnl_rs not in ("", None):
            bits.append(_fmt_money(pnl_rs))
        if pnl_pct not in ("", None):
            try:
                bits.append(f"({float(pnl_pct):+.2f}%)")
            except Exception:
                pass
        out.append((ts, "EXIT", " ".join(b for b in bits if b)))

    if (ts := _to_ts(row.get("paper_entry_filled_ts"))) is not None:
        out.append((ts, "PAPER_ENTRY", "paper-trade fill"))

    if (ts := _to_ts(row.get("paper_exit_ts"))) is not None:
        po = row.get("paper_outcome", "")
        ppnl = row.get("paper_pnl_rs", "")
        bits = [str(po)] if po not in ("", None) else []
        if ppnl not in ("", None):
            bits.append(_fmt_money(ppnl))
        out.append((ts, "PAPER_EXIT", " ".join(b for b in bits if b)))

    if (ts := _to_ts(row.get("dropped_event_ts"))) is not None and not str(row.get("signal_id", "")).strip():
        out.append((ts, "PREPOOL_DROP", str(row.get("dropped_event_reason", ""))))

    out.sort(key=lambda x: (x[0] is None, x[0] or pd.Timestamp.min))
    return out


def render_signal_block(row: pd.Series, signal_index: int) -> list[str]:
    sid = str(row.get("signal_id", "") or "").strip()
    side = str(row.get("side", "") or "").strip()
    setup = str(row.get("setup", "") or "").strip()
    dropped_at = str(row.get("dropped_at", "") or "").strip()

    if sid:
        head_id = f"signal_id={_short_sid(sid)}"
    else:
        head_id = _short_synth(row.get("synth_id", "")) or "PREPOOL_DROP"

    bar = _to_ts(row.get("signal_bar_close"))
    bar_str = bar.strftime("%H:%M") if bar is not None else "--:--"
    head = f"  [#{signal_index}] {head_id} | {side or '-'} {setup or '-'} | bar={bar_str}"
    if dropped_at and dropped_at != "(reached EXIT)":
        head += f"  ⟶  {dropped_at}"

    stages = _stage_rows(row)
    out: list[str] = [head]

    if not stages:
        out.append("        (no populated stage timestamps)")
        out.append("")
        return out

    for i, (ts, label, detail) in enumerate(stages):
        connector = "└─" if i == len(stages) - 1 else "├─"
        if i == 0:
            lag_part = ""
        else:
            prev_ts = stages[i - 1][0]
            lag_seconds = (ts - prev_ts).total_seconds() if (ts is not None and prev_ts is not None) else None
            lag_part = f"  [{_fmt_lag(lag_seconds)}]" if lag_seconds is not None else ""
        line = f"   {connector} {_hms(ts)}  {label:<13}{lag_part}"
        if detail:
            line += f"  | {detail}"
        out.append(line)
    out.append("")
    return out


def render_ticker_block(ticker: str, group: pd.DataFrame) -> list[str]:
    n = len(group)
    counts = group["dropped_at"].fillna("").value_counts()
    outcomes = group["outcome"].fillna("").value_counts()

    summary_bits = []
    for label, key in [
        ("TARGET", "TARGET"),
        ("SL", "SL"),
        ("STALE", "ENTRY_SKIPPED_STALE_DETECTION"),
        ("DUPE", "ENTRY_SKIPPED_TICKER_ALREADY_TRADED_TODAY"),
        ("TIMEOUT", "ENTRY_SKIPPED_NEAR_ENTRY_TIMEOUT"),
        ("RISK", "ENTRY_SKIPPED_RISK_LIMIT"),
        ("OPEN", "OPEN"),
        ("PRE", "PREPOOL_DROP"),
    ]:
        c = int(outcomes.get(key, 0))
        if c:
            summary_bits.append(f"{label}:{c}")

    pnl_rs_total = pd.to_numeric(group.get("pnl_rs", pd.Series()), errors="coerce").fillna(0).sum()
    pnl_part = f" | live PnL={_fmt_money(pnl_rs_total)}" if pnl_rs_total else ""

    bar = "═" * 67
    header = [
        bar,
        f" {ticker:<25} {n} signal(s){pnl_part}",
        bar,
        f"  ── outcomes: {' '.join(summary_bits) or '(no outcomes)'}",
        "",
    ]

    sort_key = pd.to_datetime(
        group["signal_bar_close"].where(
            group["signal_bar_close"].astype(str).str.len() > 0,
            group["dispatched_ts"].where(
                group["dispatched_ts"].astype(str).str.len() > 0,
                group["dropped_event_ts"].where(
                    group["dropped_event_ts"].astype(str).str.len() > 0,
                    group["entry_filled_ts"],
                ),
            ),
        ),
        errors="coerce",
        utc=True,
    )
    group = group.assign(__sort=sort_key).sort_values("__sort", na_position="last").drop(columns="__sort")

    blocks: list[str] = []
    blocks.extend(header)
    for i, (_, row) in enumerate(group.iterrows(), start=1):
        blocks.extend(render_signal_block(row, i))
    return blocks


def render_day_summary(df: pd.DataFrame) -> list[str]:
    out: list[str] = []
    out.append("V16 5MIN PER-TICKER TIMELINE TREE")
    out.append("=" * 67)
    out.append(f"total rows: {len(df)}")
    if "ticker" in df.columns:
        per_ticker = df.groupby(df["ticker"].fillna("")).size()
        ticker_cnt = (per_ticker.index.astype(str).str.len() > 0).sum()
        out.append(f"distinct tickers: {ticker_cnt}")
    if "outcome" in df.columns:
        outcomes = df["outcome"].fillna("").value_counts().head(10)
        out.append("outcomes:")
        for name, n in outcomes.items():
            label = name if name else "(none)"
            out.append(f"  {int(n):>4}  {label}")
    out.append("")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", required=True)
    ap.add_argument("--reports-dir", default=str(DEFAULT_REPORTS_DIR))
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    in_dir = Path(args.reports_dir) / f"timeline_v16_5min_{args.date}"
    wide_path = in_dir / f"timeline_wide_{args.date}.csv"
    if not wide_path.exists():
        print(f"[ERROR] not found: {wide_path}", file=sys.stderr)
        return 2

    out_path = Path(args.out) if args.out else (in_dir / f"per_ticker_tree_{args.date}.txt")

    df = pd.read_csv(wide_path, dtype=str, keep_default_na=False)
    if df.empty:
        print(f"[INFO] wide CSV is empty: {wide_path}")
        return 0

    df["ticker"] = df["ticker"].fillna("").astype(str).str.strip()
    df = df[df["ticker"] != ""].copy()

    lines: list[str] = []
    lines.extend(render_day_summary(df))

    for ticker in sorted(df["ticker"].unique()):
        group = df[df["ticker"] == ticker].copy()
        lines.extend(render_ticker_block(ticker, group))

    body = "\n".join(lines)
    out_path.write_text(body, encoding="utf-8")
    print(f"[INFO] wrote {out_path} ({len(df)} signals across {df['ticker'].nunique()} tickers)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
