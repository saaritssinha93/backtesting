"""
qualification_tracker.py
========================

P1-22: replaces the "5 clean days, positive cumulative PF" live-enable check,
which simulation shows a ZERO-EDGE system passes ~49% of the time and the
measured PF-0.82 config passes ~26% of the time. This tracker scores the
post-punchlist, mirror-config paper run against criteria that actually
discriminate:

  Q1  n >= min_trades              (default 150; ~2-3 weeks at new frequency)
  Q2  cumulative NET PF >= 1.15    (from net_pnl_rs -- never gross)
  Q3  bootstrap p(mean net > 0) < 0.10
  Q4  zero daily-loss-brake trips  (any trip => NOT QUALIFIED, restart clock)
  Q5  100% of trades from gate-PROMOTEd setups (accepted_rules.csv)
  Q6  mirror-config attestation    (paper == live: positions/capital/brake)

Verdicts
--------
  QUALIFIED       all criteria pass at n >= min_trades
  NOT QUALIFIED   hard fail (brake trip, non-promoted setup traded, or
                  criteria failed with sufficient n)
  IN PROGRESS     insufficient n, no hard fails; shows provisional metrics
                  and a projection of days remaining

Q6 cannot be verified from trade CSVs. The tracker prints the mirror-config
checklist and accepts --attest to record that a human confirmed it. Without
--attest the best verdict is IN PROGRESS.

Usage
-----
  python qualification_tracker.py ^
      --trades-glob "C:\\TradingData\\eqidv2\\live_signals\\paper_trades_*_id_5min_v7.csv" ^
      --start-date 2026-06-11 ^
      --accepted-rules C:\\...\\accepted_rules.csv ^
      --attest
"""

from __future__ import annotations

import argparse
import glob
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

NET_COL_CANDIDATES = ["net_pnl_rs", "net_pnl"]
SETUP_COL_CANDIDATES = ["setup", "setup_name", "setup_id"]
DATE_IN_NAME = re.compile(r"paper_trades_(\d{4}-\d{2}-\d{2}|\d{8})")

MIRROR_CHECKLIST = [
    "max open positions: paper == live (20)",
    "max capital: paper == live (Rs 500,000)",
    "daily loss brake: same limit AND same semantics (realized + open MTM)",
    "per-trade loss limit: Rs 5,000 active in paper",
    "gross SHORT notional cap: same (Rs 1,500,000)",
    "risk sizing / regime gate / ADV cap / F&O filter: same env values",
]


@dataclass(frozen=True)
class QualConfig:
    min_trades: int = 150
    min_net_pf: float = 1.15
    max_p_value: float = 0.10
    daily_loss_limit_rs: float = 10_000.0   # for suspected-trip detection
    per_trade_loss_limit_rs: float = 5_000.0
    n_bootstrap: int = 10_000
    random_seed: int = 7


@dataclass(frozen=True)
class QualResult:
    """Read-only qualification verdict. Produced by compute_qualification();
    consumed by the CLI (evaluate) AND the dashboard report harness so neither
    can drift."""
    n: int
    days: int
    rate: float
    total_net: float
    win_rate: float
    pf: float
    pval: float
    window_start: object
    window_end: object
    daily: pd.DataFrame
    trips: pd.DataFrame
    offenders: list
    criteria: dict          # name -> (ok: bool|None, detail: str)
    verdict: str            # "QUALIFIED" | "NOT QUALIFIED" | "IN PROGRESS"
    exit_code: int          # 0 | 1 | 3
    reason: str
    promoted_given: bool
    attested: bool


def _pick_col(df: pd.DataFrame, candidates: list[str], what: str) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    sys.exit(f"ERROR: no {what} column found (looked for {candidates}); "
             f"columns present: {list(df.columns)[:20]}")


def _date_from_name(path: str):
    m = DATE_IN_NAME.search(Path(path).name)
    if not m:
        return None
    raw = m.group(1).replace("-", "")
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))


def load_trades(pattern: str, start: date,
                skip_log: list | None = None) -> pd.DataFrame:
    """Load + concat in-window paper-trade CSVs. Resilient to a malformed daily
    file (bad rows are skipped, not fatal). When `skip_log` is provided, any file
    with dropped rows is recorded as {"file","dropped"} so callers can SURFACE the
    data issue rather than silently undercounting trades."""
    files = sorted(glob.glob(pattern))
    if not files:
        sys.exit(f"ERROR: no files match {pattern}")
    frames = []
    for f in files:
        d = _date_from_name(f)
        if d is None or d < start:
            continue
        try:
            df = pd.read_csv(f, on_bad_lines="skip")
        except Exception as exc:
            if skip_log is not None:
                skip_log.append({"file": Path(f).name, "error": str(exc)[:120]})
            continue
        if skip_log is not None:
            try:
                with open(f, "r", encoding="utf-8", errors="replace") as fh:
                    raw_rows = max(0, sum(1 for _ in fh) - 1)  # minus header
                if raw_rows > len(df):
                    skip_log.append({"file": Path(f).name,
                                     "dropped": int(raw_rows - len(df))})
            except Exception:
                pass
        if len(df) == 0:
            continue
        df["__date"] = pd.Timestamp(d)
        frames.append(df)
    if not frames:
        sys.exit(f"ERROR: no trade files on/after {start} matched {pattern}")
    return pd.concat(frames, ignore_index=True)


def _profit_factor(net: np.ndarray) -> float:
    gains = net[net > 0].sum()
    losses = -net[net < 0].sum()
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def _bootstrap_p(net: np.ndarray, n_boot: int, seed: int) -> float:
    if len(net) < 5:
        return 1.0
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(net), size=(n_boot, len(net)))
    return float((net[idx].mean(axis=1) <= 0).mean())


def compute_qualification(trades: pd.DataFrame, cfg: QualConfig,
                          promoted: set[str] | None, attested: bool) -> QualResult:
    """Pure, side-effect-free qualification scoring. The single source of truth
    for the Q1-Q6 criteria and the verdict, shared by the CLI and the dashboard
    report harness."""
    net_col = _pick_col(trades, NET_COL_CANDIDATES, "net P&L")
    setup_col = _pick_col(trades, SETUP_COL_CANDIDATES, "setup")
    net = trades[net_col].astype(float).to_numpy()
    n = len(net)

    # ---- daily table + suspected brake trips --------------------------------
    daily = (trades.groupby("__date")
             .agg(trades=(net_col, "size"), net=(net_col, "sum"),
                  worst_trade=(net_col, "min"))
             .reset_index())
    daily["suspected_brake_trip"] = (
        (daily["net"] <= -cfg.daily_loss_limit_rs)
        | (daily["worst_trade"] <= -cfg.per_trade_loss_limit_rs))
    trips = daily[daily["suspected_brake_trip"]]

    # ---- non-promoted setups -------------------------------------------------
    offenders: list[str] = []
    if promoted is not None:
        traded = set(trades[setup_col].astype(str))
        offenders = sorted(traded - promoted)

    # ---- core stats ----------------------------------------------------------
    pf = _profit_factor(net)
    pval = _bootstrap_p(net, cfg.n_bootstrap, cfg.random_seed)
    total_net = float(net.sum())
    win_rate = float((net > 0).mean()) if n else 0.0
    days = len(daily)
    rate = n / days if days else 0.0

    q = {
        "Q1 trades": (n >= cfg.min_trades, f"{n}/{cfg.min_trades}"),
        "Q2 net PF": (pf >= cfg.min_net_pf, f"{pf:.3f} vs >= {cfg.min_net_pf}"),
        "Q3 bootstrap p": (pval < cfg.max_p_value, f"{pval:.4f} vs < {cfg.max_p_value}"),
        "Q4 brake trips": (len(trips) == 0, f"{len(trips)} suspected"),
        "Q5 promoted-only": (len(offenders) == 0 if promoted is not None else None,
                             "no accepted_rules given" if promoted is None
                             else (f"offenders: {offenders}" if offenders else "all promoted")),
        "Q6 mirror-config": (attested, "attested" if attested else
                             "NOT attested (--attest after checklist)"),
    }

    # ---- verdict -------------------------------------------------------------
    hard_fail = False
    reason = ""
    if len(trips) > 0:
        hard_fail = True
        reason = "suspected brake trip(s) -- investigate, fix, RESTART the clock"
    elif offenders:
        hard_fail = True
        reason = f"non-promoted setups traded: {offenders} -- entry config leaks"
    elif n >= cfg.min_trades:
        # Q6 is a human attestation, not a statistical criterion: its absence
        # holds the verdict at IN PROGRESS rather than failing the run.
        fails = [k for k, (ok, _) in q.items()
                 if ok is False and k != "Q6 mirror-config"]
        if fails:
            hard_fail = True
            reason = f"criteria failed at full n: {fails}"

    if hard_fail:
        verdict, exit_code = "NOT QUALIFIED", 1
    elif n < cfg.min_trades or (promoted is None) or not attested:
        verdict, exit_code = "IN PROGRESS", 3
        remaining = max(0, cfg.min_trades - n)
        eta = f"~{remaining / rate:.0f} more trading days at current rate" if rate else "n/a"
        why = []
        if n < cfg.min_trades:
            why.append(f"{remaining} trades to go ({eta})")
        if not attested:
            why.append("mirror-config not attested")
        if promoted is None:
            why.append("accepted_rules not provided")
        reason = "; ".join(why)
    else:
        verdict, exit_code = "QUALIFIED", 0
        reason = "all criteria pass"

    return QualResult(
        n=n, days=days, rate=rate, total_net=total_net, win_rate=win_rate,
        pf=pf, pval=pval,
        window_start=daily["__date"].min().date() if days else None,
        window_end=daily["__date"].max().date() if days else None,
        daily=daily, trips=trips, offenders=offenders, criteria=q,
        verdict=verdict, exit_code=exit_code, reason=reason,
        promoted_given=promoted is not None, attested=attested,
    )


def evaluate(trades: pd.DataFrame, cfg: QualConfig,
             promoted: set[str] | None, attested: bool) -> int:
    res = compute_qualification(trades, cfg, promoted, attested)

    print(f"\n=== qualification_tracker @ {datetime.now():%Y-%m-%d %H:%M:%S} ===")
    print(f"window: {res.window_start} -> {res.window_end}"
          f"  ({res.days} trading days, {res.n} trades, {res.rate:.1f}/day)")
    print(f"cumulative net: Rs {res.total_net:+,.0f}   win rate: {res.win_rate:.1%}\n")
    print(f"{'criterion':<20}{'status':<10}detail")
    for name, (ok, detail) in res.criteria.items():
        status = "SKIP" if ok is None else ("PASS" if ok else "FAIL")
        print(f"{name:<20}{status:<10}{detail}")

    print("\ndaily summary:")
    for _, r in res.daily.iterrows():
        flag = "  << SUSPECTED BRAKE TRIP" if r["suspected_brake_trip"] else ""
        print(f"  {r['__date'].date()}  trades={int(r['trades']):>3}  "
              f"net=Rs {r['net']:>+9,.0f}  worst=Rs {r['worst_trade']:>+8,.0f}{flag}")

    if res.verdict == "NOT QUALIFIED":
        print(f"\nVERDICT: NOT QUALIFIED -- {res.reason}")
    elif res.verdict == "IN PROGRESS":
        print(f"\nVERDICT: IN PROGRESS -- {res.reason}")
        print("\nmirror-config checklist (confirm, then pass --attest):")
        for item in MIRROR_CHECKLIST:
            print(f"  [ ] {item}")
    else:
        print("\nVERDICT: QUALIFIED -- all criteria pass. Live enable is a human "
              "decision; start at minimum size.")
    return res.exit_code


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--trades-glob", required=True)
    ap.add_argument("--start-date", required=True,
                    help="qualification window start, YYYY-MM-DD (post-punchlist, "
                         "post-mirror-config)")
    ap.add_argument("--accepted-rules", type=Path, default=None,
                    help="gate-authored accepted_rules.csv for Q5")
    ap.add_argument("--attest", action="store_true",
                    help="record that the mirror-config checklist was verified")
    ap.add_argument("--min-trades", type=int, default=QualConfig.min_trades)
    ap.add_argument("--min-net-pf", type=float, default=QualConfig.min_net_pf)
    ap.add_argument("--max-p", type=float, default=QualConfig.max_p_value)
    ap.add_argument("--daily-loss-limit", type=float,
                    default=QualConfig.daily_loss_limit_rs)
    a = ap.parse_args()

    cfg = QualConfig(min_trades=a.min_trades, min_net_pf=a.min_net_pf,
                     max_p_value=a.max_p, daily_loss_limit_rs=a.daily_loss_limit)
    start = date.fromisoformat(a.start_date)

    promoted: set[str] | None = None
    if a.accepted_rules:
        if not a.accepted_rules.exists():
            sys.exit(f"ERROR: accepted_rules not found: {a.accepted_rules}")
        acc = pd.read_csv(a.accepted_rules)
        key = "setup" if "setup" in acc.columns else acc.columns[0]
        promoted = set(acc[key].astype(str))

    trades = load_trades(a.trades_glob, start)
    return evaluate(trades, cfg, promoted, attested=a.attest)


if __name__ == "__main__":
    raise SystemExit(main())
