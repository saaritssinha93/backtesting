"""V13 corrected - exact V6-corrected replay behind an explicit OI-policy layer.

STATUS OF THE EVIDENCE (2026-09-03)
-----------------------------------
Three research rounds looked for an OI rule better than corrected V6's
per-setup thresholds on the 23 point-in-time rolling-near-month sessions:

  1. Threshold sweeps (fno_v6_corrected_oi_threshold_research.py):
     74 one-at-a-time / uniform / per-side overlays. Train-optimised book
     collapsed out of sample (TRAIN PF 4.180 -> TEST PF 0.662). The
     "shadow two-LONG" book's TEST segment is byte-identical to baseline;
     its whole improvement is one deleted TRAIN trade.
  2. Ungated ablation (fno_v13_corrected_ungated_oi_research.py):
     8 shared policies on 1,825 pre-gate candidates. NO_OI collapses to
     PF 1.022 over 142 trades; the mirror-image gate (OI falling) loses at
     PF 0.531; CURRENT_V6 wins every metric (PF 2.041 TRAIN / 2.428 TEST).
  3. Advanced round (v13_advanced_oi_policies): cross-sectional ranks,
     dOI/volume positioning share, 10-minute persistence, and V6
     unions/intersections. None beat CURRENT_V6 in either segment; the
     V6 AND top-half-rank policy changed ZERO trades - V6's absolute
     thresholds already imply top-half relative OI rank.

  4. Blocker analytics (v13_advanced_oi_policies/ungated_enriched_blockers):
     per-candidate bracket outcomes for all 1,825 ungated candidates,
     classified by the single V6 gate that rejects them (TRAIN sessions).
     Every rejected class is a net loser picker-free: ONLY_OI PF 0.457,
     ONLY_PRICE PF 0.514, ONLY_VOL PF 0.729, MULTI PF 0.764; even
     passed-everything-but-picker runners-up manage only PF 1.287 vs 2.04
     for the picked book. High-volume OI-rejects are the WORST bucket
     (2.5-4x vol: PF 0.258). There is no gate whose relaxation adds
     positive-expectancy trades on this sample.

  5. Trade-count round (marginal picks, dead setups, new slots): raising
     per-setup entry caps is uniformly negative - rank-2 picks lose on TRAIN
     for every setup that ships cap=1, and the two zero-trade legs (09:35 and
     09:45 SHORT) stay negative at every lower OI threshold tested. The ONLY
     lever that adds trades without diluting the book is a NEW slot, because
     it changes no existing trade. Of 19 slot/side combinations at V6's modal
     gate, 09:55 LONG is the one plausible candidate (see ADD_0955_LONG) and
     11:05 LONG is obvious noise (PF inf on 4 trades) - a reminder that this
     filter admits noise at this sample size.

  6. 1-minute confirmation window (v13_confirmation_window): V6 confirms on
     exactly ONE candle (S+1) and discards the candidate otherwise, which is
     the strategy's largest single source of attrition. V8's engine defaults
     max_confirmation_minute to 4 and its B1-B5 variants use "first strict
     confirmation S+1..S+4"; V10 forced it to 1 on the CONTAMINATED data, so
     it was re-tested here on the corrected set with a parameterised replica
     of the builder (parity-checked: at K=1 it reproduces published V6 exactly,
     66/66 orders). Widening adds trades and costs PF in BOTH segments:

         K   trades   PF TRAIN   PF TEST   PF ALL   net ALL
         1       66      2.041     2.045    2.043     27.08
         2       90      1.580     1.279    1.442     18.56
         3      101      1.461     1.531    1.492     22.73
         4      112      1.413     1.325    1.372     19.56

     Two causes. The rescued trades are poor (net by confirm_step at K=4:
     S+2 -6.90, S+3 +6.38, S+4 -2.57 - non-monotonic, i.e. noise rather than
     structure). And widening is NOT additive: more candidates compete for the
     same per-day picker caps, so later confirmations evict V6's own S+1
     trades, whose net decays 27.08 -> 26.23 -> 24.83 -> 22.65 as K rises.
     V10's S+1 choice was made for the wrong reason but is correct.

Under the predeclared promotion gate (beat V6's PF in BOTH chronological
segments, >=8 changed trades, no worse concentration or drawdown), nothing
qualifies. V13 therefore defaults to EXACT V6 PARITY and offers the failed
candidates only as clearly-labelled research books. Corrected V6 itself is
never edited by this module; its SHA-256 is verified before and after.

Promotion path: rerun the alternatives after enough genuinely new sessions
(target ~40 sessions / two expiries); a book may replace V6_PARITY as the
default only with the owner's explicit approval.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path

import pandas as pd

import fno_oi_common as common
import fno_v5_hybrid_backtest as replay
import fno_v6_corrected_backtest as v6

STRATEGY_VERSION = "FNO_V13_CORRECTED_POLICY_LAYER_V1"
EXPECTED_V6_SHA256 = (
    "06baf32c33156f21bce1dc786e5687a250b9711a1bca3a186283c824edfcf62d"
)
# The signal cache admits candidates down to this OI floor; a policy book may
# not set any threshold below it or the cache would silently hide candidates.
CACHE_OI_FLOOR = 0.05

RESULT_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected"

POLICY_BOOKS: dict[str, dict[str, object]] = {
    "V6_PARITY": {
        "description": "Byte-identical to corrected V6. The only validated book.",
        "validated": True,
        "overrides": {},
    },
    "ADD_0955_LONG": {
        "description": (
            "RESEARCH - V6 book plus one additive 09:55 LONG leg at V6's modal "
            "gate (price 0.20 / OI 0.10 / vol 1.0 / body 0.4 / wick 0.5, "
            "max_liquidity, cap 1, stop 1.00 / target 3.00). Additive: it "
            "changes no existing trade. On 24 sessions it takes the book from "
            "66 trades PF 2.043 to 77 trades PF 2.116 (net 27.08 -> 33.67), "
            "positive in BOTH segments independently (TRAIN 5 trades PF 1.993, "
            "TEST 6 trades PF 3.144). NOT default: it was 1 of 19 slot/side "
            "combinations tested, which is chance-level discovery on 11 trades, "
            "and the adjacent 09:50 LONG is a loser (TRAIN 1.114 / TEST 0.000) "
            "so the edge has no neighbourhood stability. V9 previously recorded "
            "09:50/09:55 as negative lineage, though at V6's LOOSEST gate "
            "rather than this one. Promote only if it holds on ~40 sessions "
            "across two expiries."
        ),
        "validated": False,
        "overrides": {},
        "extra_setups": (
            {
                "signal_end": "09:55",
                "confirmation_end": "09:56",
                "side": "LONG",
                "max_entries": 1,
                "picker": "max_liquidity",
                "price_change_pct": 0.20,
                "oi_change_pct": 0.10,
                "volume_ratio": 1.0,
                "body_ratio": 0.4,
                "max_wick_ratio": 0.5,
                "stop_pct": 1.00,
                "target_pct": 3.00,
            },
        ),
    },
    "SHADOW_TWO_LONG": {
        "description": (
            "RESEARCH ONLY - 09:35 LONG OI 0.10->0.15, 09:40 LONG OI "
            "0.10->0.075. Failed the promotion gate: TEST segment identical "
            "to baseline, improvement is one deleted TRAIN trade."
        ),
        "validated": False,
        "overrides": {("09:35", "LONG"): 0.15, ("09:40", "LONG"): 0.075},
    },
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_v6_frozen() -> str:
    digest = _sha256(Path(v6.__file__).resolve())
    if digest != EXPECTED_V6_SHA256:
        raise RuntimeError(
            "Corrected V6 source drifted; V13 refuses to run against an "
            f"unpinned comparator. Expected {EXPECTED_V6_SHA256}, got {digest}."
        )
    return digest


def _policy_setups(policy: str):
    book = POLICY_BOOKS[policy]
    overrides: dict = book["overrides"]  # type: ignore[assignment]
    setups = []
    for setup in v6.ACTIVE_SETUPS:
        new_thr = overrides.get((setup.signal_end, setup.side))
        if new_thr is None:
            setups.append(setup)
            continue
        if new_thr < CACHE_OI_FLOOR:
            raise AssertionError(
                f"{policy}: {setup.signal_end} {setup.side} threshold "
                f"{new_thr} is below the cache floor {CACHE_OI_FLOOR}."
            )
        setups.append(replace(setup, oi_change_pct=float(new_thr)))
    template = v6.ACTIVE_SETUPS[0]
    for extra in book.get("extra_setups", ()) or ():
        if float(extra["oi_change_pct"]) < CACHE_OI_FLOOR:
            raise AssertionError(
                f"{policy}: additive leg {extra['signal_end']} {extra['side']} "
                f"OI threshold {extra['oi_change_pct']} is below the cache "
                f"floor {CACHE_OI_FLOOR}."
            )
        if any(
            s.signal_end == extra["signal_end"] and s.side == extra["side"]
            for s in setups
        ):
            raise AssertionError(
                f"{policy}: additive leg {extra['signal_end']} {extra['side']} "
                "duplicates an existing setup; it would not be additive."
            )
        setups.append(replace(template, **extra))
    return tuple(setups)


def _parity_check(audit: pd.DataFrame) -> dict[str, object]:
    """Compare a V6_PARITY audit against V6's published trades, if present."""
    published = v6.AUDIT_OUTPUT_PATH
    if not published.is_file():
        return {"published_found": False}
    ref = pd.read_csv(published)
    ref["day"] = ref["day"].astype(str)
    ours = audit.copy()
    ours["day"] = ours["day"].astype(str)
    shared_days = sorted(set(ref["day"]) & set(ours["day"]))
    key = ["day", "tradingsymbol", "setup_id", "side"]
    left = (
        ours.loc[ours["day"].isin(shared_days)]
        .sort_values(key)[key + ["net_return_pct"]]
        .reset_index(drop=True)
    )
    right = (
        ref.loc[ref["day"].isin(shared_days)]
        .sort_values(key)[key + ["net_return_pct"]]
        .reset_index(drop=True)
    )
    keys_equal = left[key].equals(right[key])
    returns_equal = bool(
        keys_equal
        and len(left) == len(right)
        and (left["net_return_pct"] - right["net_return_pct"]).abs().le(1e-12).all()
    )
    return {
        "published_found": True,
        "published_path": str(published),
        "shared_sessions": len(shared_days),
        "v13_orders_on_shared": int(len(left)),
        "v6_orders_on_shared": int(len(right)),
        "trade_keys_equal": bool(keys_equal),
        "returns_equal_at_1e_12": returns_equal,
        "passed": bool(keys_equal and returns_equal),
    }


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--oi-policy",
        choices=sorted(POLICY_BOOKS),
        default="V6_PARITY",
        help="Setup book to replay. Only V6_PARITY is validated.",
    )
    parser.add_argument("--split-day", default="2026-08-14")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--min-contract-coverage", type=float, default=0.80)
    parser.add_argument("--rebuild-cache", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    hash_before = _verify_v6_frozen()
    book = POLICY_BOOKS[args.oi_policy]
    if not book["validated"]:
        print(
            f"[V13][WARNING] {args.oi_policy} is RESEARCH ONLY and is not a "
            "validated improvement. Its output must not be treated as a "
            "promoted strategy change.",
            flush=True,
        )
    out_dir = RESULT_ROOT / args.oi_policy.lower()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Identical pipeline to corrected V6, reusing its cache.
    regimes = v6.regime_universe_paths()
    eligibility, calendar, _origin = v6.build_eligibility(
        regimes, min_coverage=args.min_contract_coverage
    )
    ok = eligibility.loc[eligibility["eligible"]]
    if ok.empty:
        raise RuntimeError("V13 has no eligible sessions.")
    days_by_month: dict[str, list[date]] = {}
    for row in ok.to_dict("records"):
        days_by_month.setdefault(str(row["required_contract"]), []).append(row["day"])

    parts = []
    for month in sorted(days_by_month, key=lambda m: calendar[m]):
        days = sorted(days_by_month[month])
        signals, paths, _record = v6.build_regime_signals(
            month,
            regimes[month],
            days,
            square_off=args.square_off,
            max_forward_bars=args.max_forward_bars,
            rebuild=args.rebuild_cache,
        )
        parts.append((signals, paths))
    signals, paths = v6.concat_regimes(parts)
    days = sorted(set(signals["day"]))

    setups = _policy_setups(args.oi_policy)
    audit = replay.replay_setups(
        signals, paths, cost_bps=args.cost_bps, setups=setups
    )
    if audit.empty:
        raise RuntimeError("V13 selected no orders.")
    audit["strategy_version"] = STRATEGY_VERSION
    audit["oi_policy"] = args.oi_policy
    split_day = pd.Timestamp(args.split_day).date()
    daily = replay.build_daily_curve(audit, days, split_day=split_day)
    stats = replay.summary_stats(daily, audit)

    parity = (
        _parity_check(audit) if args.oi_policy == "V6_PARITY" else {"skipped": True}
    )
    hash_after = _verify_v6_frozen()

    common.atomic_write_csv(audit, out_dir / "fno_v13_trades.csv")
    common.atomic_write_csv(daily, out_dir / "fno_v13_daily.csv")
    common.atomic_write_json(
        out_dir / "fno_v13_provenance.json",
        {
            "strategy_version": STRATEGY_VERSION,
            "oi_policy": args.oi_policy,
            "policy_description": book["description"],
            "policy_validated": book["validated"],
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "v6_sha256_before": hash_before,
            "v6_sha256_after": hash_after,
            "sessions": [str(d) for d in days],
            "active_setups": [asdict(s) for s in setups],
            "parameters": {
                "split_day": str(args.split_day),
                "cost_bps": float(args.cost_bps),
                "square_off": str(args.square_off),
                "max_forward_bars": int(args.max_forward_bars),
            },
            "stats": {
                k: (float(x) if isinstance(x, (int, float)) else x)
                for k, x in stats.items()
            },
            "parity_vs_v6": parity,
        },
    )

    print(f"[V13] policy={args.oi_policy} sessions={stats['sessions']} "
          f"orders={stats['orders']} fills={stats['fills']}", flush=True)
    for key in ("trade_pf", "day_pf", "net_pct"):
        if key in stats:
            print(f"      {key}={stats[key]}", flush=True)
    if args.oi_policy == "V6_PARITY" and parity.get("published_found"):
        print(f"[PARITY] identical to published V6 on shared sessions: "
              f"{parity['passed']}", flush=True)
    print(f"[WROTE] {out_dir}", flush=True)
    print(f"[DONE] {time.monotonic() - started:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
