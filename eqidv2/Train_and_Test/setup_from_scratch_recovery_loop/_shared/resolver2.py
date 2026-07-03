r"""resolver2.py — research exit resolver v2 for the from-scratch recovery loop.

Extends the production first-touch resolver (v17D_exit_resolver.resolve) with the
exit modes the campaign spec asks for but the repo tuner never supported:

  break_even_at_pct : once price moves +X% in favour (bar high/low touch), the
                      stop moves to ENTRY — effective from the NEXT bar
                      (conservative: no same-bar benefit).
  trail_pct         : trailing stop anchored to the best CLOSE seen so far
                      (conservative: closes only, no intrabar peaks), effective
                      from the NEXT bar. Stop only ever tightens.
  time_stop_min     : if still open N minutes after entry, exit at that bar's
                      close (models a live market-order flatten).

Intrabar conventions (all pessimistic, matching production):
  - Stop checked BEFORE target within each bar; same-bar stop+target -> stop.
  - BE/TRAIL state updates happen AFTER a bar completes, so a stop raised by
    bar i can only fire from bar i+1 onward.
  - EOD forced close at 15:20 IST bar close (production convention).

Also provides fill_retest_limit(): a resting limit entry at a pullback level
after the 5-min signal (cancel if untouched within K minutes) — live-
implementable as a plain limit order.

With all extras None/off, resolve2() is validated to REPRODUCE the production
resolver exactly (see validate_against_production()).

RESEARCH-ONLY. No live execution.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

EOD_CUTOFF_HOUR = 15
EOD_CUTOFF_MIN = 20


@dataclass(frozen=True)
class Res2:
    outcome: str          # TARGET | SL | BE | TRAIL | TIME | EOD
    exit_price: float
    exit_time_ist: pd.Timestamp
    bars_held: int


def _ist(ts) -> pd.Timestamp:
    t = pd.to_datetime(ts)
    return t.tz_localize("Asia/Kolkata") if t.tz is None else t.tz_convert("Asia/Kolkata")


def resolve2(bars: pd.DataFrame | None, side: str, entry_price: float, entry_time_ist,
             sl_pct: float, tgt_pct: float,
             be_at_pct: float | None = None,
             trail_pct: float | None = None,
             time_stop_min: int | None = None) -> Res2 | None:
    if bars is None or bars.empty:
        return None
    side = side.upper()
    et = _ist(entry_time_ist)
    eod = et.normalize() + pd.Timedelta(hours=EOD_CUTOFF_HOUR, minutes=EOD_CUTOFF_MIN)
    sub = bars[(bars.index >= et) & (bars.index <= eod)]
    if sub.empty:
        return None
    long = side == "LONG"
    e = float(entry_price)
    stop = e * (1 - sl_pct / 100.0) if long else e * (1 + sl_pct / 100.0)
    tgt = e * (1 + tgt_pct / 100.0) if long else e * (1 - tgt_pct / 100.0)
    be_trigger = None
    if be_at_pct is not None:
        be_trigger = e * (1 + be_at_pct / 100.0) if long else e * (1 - be_at_pct / 100.0)
    stop_kind = "SL"
    t_deadline = et + pd.Timedelta(minutes=int(time_stop_min)) if time_stop_min else None

    h = sub["high"].to_numpy()
    l = sub["low"].to_numpy()
    c = sub["close"].to_numpy()
    times = sub.index

    for i in range(len(sub)):
        hi, lo, cl = float(h[i]), float(l[i]), float(c[i])
        # 1) stop first (pessimistic)
        if (long and lo <= stop) or ((not long) and hi >= stop):
            return Res2(stop_kind, stop, times[i], i + 1)
        # 2) target
        if (long and hi >= tgt) or ((not long) and lo <= tgt):
            return Res2("TARGET", tgt, times[i], i + 1)
        # 3) time stop (bar close at/after deadline)
        if t_deadline is not None and times[i] >= t_deadline:
            return Res2("TIME", cl, times[i], i + 1)
        # 4) update BE/trail state for NEXT bar (only ever tighten)
        if be_trigger is not None and ((long and hi >= be_trigger) or ((not long) and lo <= be_trigger)):
            if (long and e > stop) or ((not long) and e < stop):
                stop, stop_kind = e, "BE"
            be_trigger = None
        if trail_pct is not None:
            cand = cl * (1 - trail_pct / 100.0) if long else cl * (1 + trail_pct / 100.0)
            if (long and cand > stop) or ((not long) and cand < stop):
                stop, stop_kind = cand, "TRAIL"
    return Res2("EOD", float(c[-1]), times[-1], len(sub))


def fill_retest_limit(bars: pd.DataFrame | None, side: str, limit_price: float,
                      from_ts, within_min: int) -> tuple[pd.Timestamp, float] | None:
    """Resting limit entry: LONG fills when a 1-min low <= limit; SHORT when a
    1-min high >= limit. Fill price = limit (no price improvement claimed).
    Returns (fill_time, limit_price) or None if untouched within the window."""
    if bars is None or bars.empty:
        return None
    ft = _ist(from_ts)
    w = bars[(bars.index >= ft) & (bars.index < ft + pd.Timedelta(minutes=int(within_min)))]
    if w.empty:
        return None
    long = side.upper() == "LONG"
    ok = (w["low"] <= limit_price) if long else (w["high"] >= limit_price)
    if not bool(ok.any()):
        return None
    ts = w.index[ok.to_numpy().argmax()]
    return ts, float(limit_price)


def validate_against_production(n_check: int = 300, seed: int = 7) -> dict:
    """resolve2 with extras off must equal v17D_exit_resolver.resolve on real
    trades (same outcome, exit price, exit time)."""
    import sys
    from pathlib import Path
    here = Path(__file__).resolve()
    repo = here.parents[3]
    tt_dir = here.parents[2]
    for p in (str(repo), str(tt_dir)):
        if p not in sys.path:
            sys.path.insert(0, p)
    import numpy as np
    import setup_train_test as tt
    import avwap_5min_ID_v11_backtesting as v11
    import v17D_exit_resolver as er

    pool_dir = here.parent.parent / "B_HUGE_FAILED_BOUNCE" / "pools" / "B_HUGE_FAILED_BOUNCE_enriched"
    tt.POOL_DIRS = [pool_dir]
    tt.POOL_DIR = pool_dir
    pool = tt.load_pool()
    pool = pool.sample(n=min(n_check, len(pool)), random_state=seed)
    sub = tt.attach_entries(pool)
    mismatches = []
    checked = 0
    rng = np.random.default_rng(seed)
    for r in sub.itertuples():
        bars = v11._load_1m_with_open(r.ticker)
        if bars is None or bars.empty:
            continue
        sl = float(rng.choice([0.7, 0.9, 1.2]))
        tg = float(rng.choice([1.0, 1.5, 2.0]))
        a = er.resolve(bars=bars, side=r.side, entry_price=float(r.tt_fill),
                       entry_time_ist=pd.Timestamp(r.tt_entry_iso), sl_pct=sl, tgt_pct=tg)
        b = resolve2(bars, r.side, float(r.tt_fill), pd.Timestamp(r.tt_entry_iso), sl, tg)
        if (a is None) != (b is None):
            mismatches.append((r.ticker, r.tt_entry_iso, "presence", a, b))
            continue
        if a is None:
            continue
        checked += 1
        if (a.outcome != b.outcome or abs(a.exit_price - b.exit_price) > 1e-9
                or a.exit_time_ist != b.exit_time_ist):
            mismatches.append((r.ticker, r.tt_entry_iso, sl, tg,
                               (a.outcome, a.exit_price, str(a.exit_time_ist)),
                               (b.outcome, b.exit_price, str(b.exit_time_ist))))
    return {"checked": checked, "mismatches": len(mismatches), "detail": mismatches[:5]}


if __name__ == "__main__":
    out = validate_against_production()
    print(f"[resolver2] validation vs production resolver: checked={out['checked']} "
          f"mismatches={out['mismatches']}")
    if out["mismatches"]:
        for d in out["detail"]:
            print("  MISMATCH:", d)
        raise SystemExit(1)
    print("[resolver2] PASS — identical to production resolver with extras off")
