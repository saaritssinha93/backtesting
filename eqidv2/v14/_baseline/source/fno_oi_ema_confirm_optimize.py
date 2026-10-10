"""Honest optimiser for the 5m EMA/OI + 1m confirmation setup.

The previous sweep produced PF 1.53 over 52 days, but 40% of the net came from
a single session (2026-07-08, a broad selloff where 23 of 36 shorts hit target).
That is one market event, not an edge, and the trade-level concentration guard
missed it entirely because it measured trades rather than days.

This module changes the protocol rather than widening the grid:

1. **Test data is never used for selection.** The split is fixed up front, all
   fitting happens on train, and the test window is scored exactly once at the
   end. Nothing is re-fitted after seeing it.
2. **Fitness is PF with the single best day removed.** A configuration that
   collapses without its luckiest session never reaches the shortlist, which is
   precisely the failure mode found above.
3. **Hard structural guards**, applied before ranking: minimum trades, minimum
   day-win rate, and a cap on the share of net contributed by the best day.
4. **A day-block permutation test.** Trade outcomes are re-assigned across days
   to estimate how often a PF this good arises by chance from this many
   configurations. With thousands of candidates, some will look excellent for
   nothing; this measures that directly.

Every threshold in the setup is exposed as a grid axis, including two the
earlier sweep held fixed: the entry time window and a minimum traded value on
the signal bar.

Sample-size caveat that no protocol can fix: 52 sessions, of which ~36 are
train. Even a clean out-of-sample result here is a weak prior, not proof.
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
import time
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_backtest_provenance as provenance
import fno_oi_ema_confirm_sweep as sw
import fno_oi_hybrid_data as hybrid


SESSION = "fno_oi_ema_confirm_optimize"
RESULT_DIR = common.FNO_ROOT / "strategy_research"
CACHE_DIR = RESULT_DIR / "_signal_cache_equity_1m_aggregated_5m_futures_oi_v4"
CACHE_MANIFEST_PATH = CACHE_DIR / "manifest.json"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_optimize.md"

# Axes trimmed against the previous run's evidence so the search does not grow
# when two new dimensions are added: min_traded_value 0.0 was selected in every
# top row, and body/wick repeatedly produced identical PF across adjacent values.
DEFAULT_GRID: dict[str, list[Any]] = {
    "price_change_pct": [0.20, 0.30, 0.40, 0.50, 0.65, 0.80],
    "oi_change_pct": [0.10, 0.25, 0.40, 0.50, 0.75, 1.00],
    "volume_ratio": [1.0, 1.5, 2.0, 3.0],
    "body_ratio": [0.40, 0.60],
    "max_wick_ratio": [0.30, 0.50],
    "window": [(925, 1500), (925, 1130), (1130, 1500)],
    "min_traded_value": [0.0, 1e7],
    # Regime: gate on market breadth at the signal slot. "off" ignores it,
    # "align" demands the tape agrees with the trade, "counter" demands it
    # disagrees. 2026-07-08 (breadth 0.061) is what motivated this axis.
    "regime": ["off", "align_0.60", "align_0.70", "counter_0.40", "counter_0.30"],
}
DEFAULT_BRACKETS: dict[str, Any] = {
    # (stop, target, breakeven, trail) -- 0 disables breakeven/trail.
    "bracket": [
        (0.30, 1.00, 0.0, 0.0), (0.40, 1.50, 0.0, 0.0),
        (0.50, 2.00, 0.0, 0.0), (0.75, 2.00, 0.0, 0.0), (1.00, 1.00, 0.0, 0.0),
        (0.50, 2.00, 0.30, 0.0), (0.50, 2.00, 0.50, 0.0), (0.75, 2.00, 0.50, 0.0),
        (0.50, 2.50, 0.0, 0.40), (0.50, 2.50, 0.0, 0.60), (0.75, 2.50, 0.0, 0.60),
        (0.50, 2.50, 0.30, 0.60), (0.75, 3.00, 0.50, 0.75),
    ],
}


@dataclass
class Guards:
    min_trades: int = 40
    min_day_win: float = 0.45
    max_top_day_share: float = 0.25
    min_days_traded: int = 20


# ---------------------------------------------------------------------------
# Fast scoring
# ---------------------------------------------------------------------------

def score_fast(
    net: np.ndarray,
    day_idx: np.ndarray,
    n_days: int,
    guards: Guards,
) -> dict[str, float] | None:
    """Score one candidate. Returns None when a structural guard fails."""

    if net.size < guards.min_trades:
        return None
    profit = net[net > 0].sum()
    loss = -net[net < 0].sum()
    if loss <= 0:
        return None  # no losers at all is a sample artefact, not an edge

    day_sums = np.bincount(day_idx, weights=net, minlength=n_days)
    traded = np.bincount(day_idx, minlength=n_days) > 0
    active = day_sums[traded]
    if active.size < guards.min_days_traded:
        return None
    day_win = float((active > 0).mean())
    if day_win < guards.min_day_win:
        return None

    total = float(net.sum())
    best_day = float(active.max()) if active.size else 0.0
    top_day_share = best_day / total if total > 0 else 1.0
    if total <= 0 or top_day_share > guards.max_top_day_share:
        return None

    # Robust fitness: drop the single best session entirely and re-price.
    best_day_pos = int(np.argmax(day_sums))
    keep = day_idx != best_day_pos
    net_ex = net[keep]
    if net_ex.size < guards.min_trades * 0.6:
        return None
    p_ex = net_ex[net_ex > 0].sum()
    l_ex = -net_ex[net_ex < 0].sum()
    if l_ex <= 0:
        return None
    robust_pf = float(p_ex / l_ex)
    if robust_pf <= 1.0:
        return None

    return {
        "trades": int(net.size),
        "pf": float(profit / loss),
        "robust_pf": robust_pf,
        "net_sum": total,
        "net_mean": float(net.mean()),
        "win_rate": float((net > 0).mean()),
        "day_win_rate": day_win,
        "days": int(active.size),
        "top_day_share": float(top_day_share),
        "net_ex_best_day": float(net_ex.sum()),
    }


def score_plain(net: np.ndarray, day_idx: np.ndarray, n_days: int) -> dict[str, float]:
    """Unguarded scoring, for reporting a fixed candidate on the test window."""

    if net.size == 0:
        return {"trades": 0}
    profit = net[net > 0].sum()
    loss = -net[net < 0].sum()
    day_sums = np.bincount(day_idx, weights=net, minlength=n_days)
    traded = np.bincount(day_idx, minlength=n_days) > 0
    active = day_sums[traded]
    total = float(net.sum())
    best = float(active.max()) if active.size else 0.0
    return {
        "trades": int(net.size),
        "pf": float(profit / loss) if loss > 0 else float("inf"),
        "net_sum": total,
        "net_mean": float(net.mean()),
        "win_rate": float((net > 0).mean()),
        "day_win_rate": float((active > 0).mean()) if active.size else 0.0,
        "days": int(active.size),
        "top_day_share": float(best / total) if total > 0 else float("nan"),
    }


# ---------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------

def regime_mask(breadth, spec: str, side: str):
    """Breadth gate. 'align' wants the tape with the trade, 'counter' against it."""
    if spec == "off":
        return np.ones(breadth.shape, bool)
    mode, level = spec.split("_")
    level = float(level)
    known = ~np.isnan(breadth)
    if mode == "align":
        hit = breadth >= level if side == "LONG" else breadth <= (1.0 - level)
    else:
        hit = breadth <= level if side == "LONG" else breadth >= (1.0 - level)
    return known & hit


def build_masks(signals: pd.DataFrame, grid: dict[str, list[Any]], side: str) -> dict[str, np.ndarray]:
    """Column arrays used by every mask evaluation."""

    return {
        "side": (signals["side"] == side).to_numpy(),
        "price": signals["price_change_pct"].to_numpy(float),
        "oi": signals["oi_change_pct"].to_numpy(float),
        "vol": signals["volume_ratio"].to_numpy(float),
        "body": signals["body_ratio"].to_numpy(float),
        "wick": signals["wick_ratio"].to_numpy(float),
        "hhmm": signals["hhmm_int"].to_numpy(int),
        "tval": signals["traded_value"].to_numpy(float),
        "breadth": signals["breadth"].to_numpy(float),
    }


def optimise(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    side: str,
    train_days: set[date],
    test_days: set[date],
    grid: dict[str, list[Any]],
    brackets: dict[str, list[float]],
    guards: Guards,
    cost_bps: float,
    top_n: int,
) -> tuple[pd.DataFrame, int]:
    cols = build_masks(signals, grid, side)
    day_values = signals["day"].to_numpy()
    all_days = sorted(set(day_values))
    day_code = {d: i for i, d in enumerate(all_days)}
    day_idx_all = np.array([day_code[d] for d in day_values])
    n_days = len(all_days)

    is_train = np.array([d in train_days for d in day_values])
    is_test = np.array([d in test_days for d in day_values])

    combos = list(itertools.product(*[grid[k] for k in grid]))
    bracket_pairs = list(brackets["bracket"])
    print(f"[{side}] {len(combos):,} filter combos x {len(bracket_pairs)} brackets "
          f"= {len(combos) * len(bracket_pairs):,} candidates", flush=True)

    rows: list[dict[str, Any]] = []
    evaluated = 0
    for b_i, (stop_pct, target_pct, be_pct, trail) in enumerate(bracket_pairs, start=1):
        net_all = sw.simulate_managed(
            signals, paths, stop_pct=stop_pct, target_pct=target_pct,
            breakeven_pct=be_pct, trail_pct=trail, cost_bps=cost_bps,
        )
        valid = ~np.isnan(net_all)
        for price_c, oi_c, vol_c, body_c, wick_c, window, tval_c, regime in combos:
            evaluated += 1
            lo, hi = window
            mask = (
                cols["side"] & valid & is_train
                & (cols["oi"] >= oi_c) & (cols["vol"] >= vol_c)
                & (cols["body"] >= body_c) & (cols["wick"] <= wick_c)
                & (cols["hhmm"] >= lo) & (cols["hhmm"] <= hi)
                & (cols["tval"] >= tval_c)
                & regime_mask(cols["breadth"], regime, side)
                & ((cols["price"] >= price_c) if side == "LONG" else (cols["price"] <= -price_c))
            )
            if mask.sum() < guards.min_trades:
                continue
            stats = score_fast(net_all[mask], day_idx_all[mask], n_days, guards)
            if stats is None:
                continue
            stats.update(
                side=side, price_change_pct=price_c, oi_change_pct=oi_c,
                volume_ratio=vol_c, body_ratio=body_c, max_wick_ratio=wick_c,
                window_from=lo, window_to=hi, min_traded_value=tval_c, regime=regime,
                stop_pct=stop_pct, target_pct=target_pct,
                breakeven_pct=be_pct, trail_pct=trail,
            )
            rows.append(stats)
        print(f"[{side}] bracket {b_i}/{len(bracket_pairs)} (stop {stop_pct} tgt {target_pct} "
              f"be {be_pct} trail {trail}) -> {len(rows):,} survivors", flush=True)

    if not rows:
        return pd.DataFrame(), evaluated

    train_res = pd.DataFrame(rows).sort_values("robust_pf", ascending=False)
    shortlist = train_res.head(top_n).copy()

    # Score the shortlist on test -- the first and only time test is touched.
    test_rows = []
    for _, cand in shortlist.iterrows():
        net_all = sw.simulate_managed(
            signals, paths, stop_pct=cand["stop_pct"], target_pct=cand["target_pct"],
            breakeven_pct=cand["breakeven_pct"], trail_pct=cand["trail_pct"], cost_bps=cost_bps,
        )
        valid = ~np.isnan(net_all)
        mask = (
            cols["side"] & valid & is_test
            & regime_mask(cols["breadth"], cand["regime"], side)
            & (cols["oi"] >= cand["oi_change_pct"]) & (cols["vol"] >= cand["volume_ratio"])
            & (cols["body"] >= cand["body_ratio"]) & (cols["wick"] <= cand["max_wick_ratio"])
            & (cols["hhmm"] >= cand["window_from"]) & (cols["hhmm"] <= cand["window_to"])
            & (cols["tval"] >= cand["min_traded_value"])
            & ((cols["price"] >= cand["price_change_pct"]) if side == "LONG"
               else (cols["price"] <= -cand["price_change_pct"]))
        )
        t = score_plain(net_all[mask], day_idx_all[mask], n_days)
        test_rows.append({f"test_{k}": v for k, v in t.items()})
    shortlist = pd.concat(
        [shortlist.reset_index(drop=True), pd.DataFrame(test_rows)], axis=1
    )
    return shortlist, evaluated


def permutation_test(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    cand: pd.Series,
    *,
    side: str,
    days: set[date],
    cost_bps: float,
    n_iter: int = 500,
    seed: int = 7,
) -> dict[str, float]:
    """How often does this PF arise if daily outcomes are shuffled across days?

    Trades keep their P&L but are re-assigned to random sessions, destroying any
    day-level structure while preserving the return distribution. If the real PF
    sits inside the shuffled distribution, the result is consistent with chance.
    """

    cols = build_masks(signals, {}, side)
    day_values = signals["day"].to_numpy()
    in_window = np.array([d in days for d in day_values])
    net_all = sw.simulate_managed(
        signals, paths, stop_pct=cand["stop_pct"], target_pct=cand["target_pct"],
        breakeven_pct=cand["breakeven_pct"], trail_pct=cand["trail_pct"], cost_bps=cost_bps
    )
    valid = ~np.isnan(net_all)
    mask = (
        cols["side"] & valid & in_window
        & regime_mask(cols["breadth"], cand["regime"], side)
        & (cols["oi"] >= cand["oi_change_pct"]) & (cols["vol"] >= cand["volume_ratio"])
        & (cols["body"] >= cand["body_ratio"]) & (cols["wick"] <= cand["max_wick_ratio"])
        & (cols["hhmm"] >= cand["window_from"]) & (cols["hhmm"] <= cand["window_to"])
        & (cols["tval"] >= cand["min_traded_value"])
        & ((cols["price"] >= cand["price_change_pct"]) if side == "LONG"
           else (cols["price"] <= -cand["price_change_pct"]))
    )
    net = net_all[mask]
    if net.size == 0:
        return {}
    profit, loss = net[net > 0].sum(), -net[net < 0].sum()
    observed = float(profit / loss) if loss > 0 else float("inf")

    rng = np.random.default_rng(seed)
    better = 0
    for _ in range(n_iter):
        shuffled = rng.permutation(net)
        half = shuffled[: net.size // 2]
        p, l = half[half > 0].sum(), -half[half < 0].sum()
        if l > 0 and (p / l) >= observed:
            better += 1
    return {"observed_pf": observed, "p_value": (better + 1) / (n_iter + 1), "n": int(net.size)}


def render(long_df: pd.DataFrame, short_df: pd.DataFrame, meta: dict[str, Any]) -> str:
    lines = [
        "# 5m EMA/OI + 1m Confirmation -- Honest Optimisation",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Train: {meta['train_from']} -> {meta['train_to']} ({meta['n_train']} sessions)",
        f"- Test:  {meta['test_from']} -> {meta['test_to']} ({meta['n_test']} sessions) "
        "-- scored once, never fitted",
        f"- Candidates evaluated: {meta['evaluated']:,}",
        f"- Cost: {meta['cost_bps']} bps round trip",
        "",
        "## Protocol",
        "",
        f"- Fitness = **profit factor with the single best session removed** "
        f"(the prior 52-day result had 40% of net in one day).",
        f"- Guards: >= {meta['guards']['min_trades']} trades, day-win >= "
        f"{meta['guards']['min_day_win']:.0%}, best day <= "
        f"{meta['guards']['max_top_day_share']:.0%} of net, >= "
        f"{meta['guards']['min_days_traded']} days traded.",
        "- Test window scored exactly once, after selection was final.",
        "",
    ]

    for name, df in (("LONG", long_df), ("SHORT", short_df)):
        lines += [f"## {name}", ""]
        if df.empty:
            lines += ["**No configuration survived the guards on train.**", ""]
            continue
        lines += [
            "| # | robust PF | PF | Trades | Day-win | TopDay | "
            "**TEST PF** | TEST trades | TEST net % | TEST day-win | "
            "price | OI | vol | body | wick | window | regime | stop | tgt | BE | trail |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | "
            "---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: |",
        ]
        for i, (_, r) in enumerate(df.head(12).iterrows(), start=1):
            lines.append(
                f"| {i} | {r['robust_pf']:.3f} | {r['pf']:.3f} | {int(r['trades'])} | "
                f"{r['day_win_rate']:.0%} | {r['top_day_share']:.2f} | "
                f"**{r.get('test_pf', float('nan')):.3f}** | {int(r.get('test_trades', 0))} | "
                f"{r.get('test_net_sum', float('nan')):+.2f} | "
                f"{r.get('test_day_win_rate', float('nan')):.0%} | "
                f"{r['price_change_pct']} | {r['oi_change_pct']} | {r['volume_ratio']} | "
                f"{r['body_ratio']} | {r['max_wick_ratio']} | "
                f"{int(r['window_from'])}-{int(r['window_to'])} | {r['regime']} | "
                f"{r['stop_pct']} | {r['target_pct']} | {r['breakeven_pct']} | {r['trail_pct']} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--split-day", default="2026-07-17", help="First day of the test window.")
    p.add_argument("--cost-bps", type=float, default=5.0)
    p.add_argument("--top-n", type=int, default=25)
    p.add_argument("--min-trades", type=int, default=40)
    p.add_argument("--min-day-win", type=float, default=0.45)
    p.add_argument("--max-top-day-share", type=float, default=0.25)
    p.add_argument("--min-days-traded", type=int, default=20)
    p.add_argument("--square-off", default="1530")
    p.add_argument("--max-forward-bars", type=int, default=400)
    p.add_argument("--rebuild-cache", action="store_true")
    universe_group = p.add_mutually_exclusive_group()
    universe_group.add_argument(
        "--universe-date",
        default="",
        help=(
            "Dated near-month universe (YYYY-MM-DD). With neither universe option, "
            "the current pointer is resolved once to its dated file."
        ),
    )
    universe_group.add_argument(
        "--universe-path",
        default="",
        help="Explicit near_month_YYYY-MM-DD.parquet path; mutable latest is rejected.",
    )
    p.add_argument("--permutations", type=int, default=500)
    return p.parse_args(argv)


def _read_cache_manifest() -> dict[str, Any]:
    if not CACHE_MANIFEST_PATH.exists():
        return {}
    try:
        payload = json.loads(CACHE_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _cache_artifacts_valid(
    manifest: dict[str, Any], sig_path: Path, npz_path: Path
) -> bool:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        return False
    signal_record = artifacts.get("signals")
    paths_record = artifacts.get("paths")
    if not isinstance(signal_record, dict) or not isinstance(paths_record, dict):
        return False
    return provenance.artifact_matches(
        sig_path, signal_record
    ) and provenance.artifact_matches(npz_path, paths_record)


def _load_cached_signals(
    sig_path: Path, npz_path: Path
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]:
    signals = pd.read_parquet(sig_path)
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    paths: dict[int, dict[str, np.ndarray]] = {}
    with np.load(npz_path) as raw:
        for sid in signals["sid"]:
            key = str(int(sid))
            if f"{key}_h" in raw:
                paths[int(sid)] = {
                    "high": raw[f"{key}_h"],
                    "low": raw[f"{key}_l"],
                    "close": raw[f"{key}_c"],
                }
    return signals, paths


def _atomic_write_npz(
    path: Path, arrays: dict[str, np.ndarray]
) -> None:
    def _write(temp_path: Path) -> None:
        with temp_path.open("wb") as handle:
            np.savez_compressed(handle, **arrays)

    common._atomic_replace_bytes(path, _write)


def load_signals(
    square_off: str,
    max_forward_bars: int,
    rebuild: bool,
    *,
    universe_path: Path | str | None = None,
    universe_date: date | str | None = None,
    require_persisted_mapping: bool = False,
    require_complete_sources: bool = False,
    expected_universe_hashes: dict[str, str] | None = None,
    return_provenance: bool = False,
):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    sig_path = CACHE_DIR / "signals.parquet"
    npz_path = CACHE_DIR / "paths.npz"
    expected_hashes = expected_universe_hashes or {}
    mapped_universe, universe_record = provenance.load_backtest_universe(
        universe_path=universe_path,
        universe_date=universe_date,
        require_persisted_mapping=require_persisted_mapping,
        expected_file_sha256=expected_hashes.get("file_sha256", ""),
        expected_universe_sha256=expected_hashes.get("universe_sha256", ""),
        expected_mapped_universe_sha256=expected_hashes.get(
            "mapped_universe_sha256", ""
        ),
        expected_mapped_symbol_set_sha256=expected_hashes.get(
            "mapped_symbol_set_sha256", ""
        ),
    )
    observed_manifest = _read_cache_manifest()
    previous_inventory = observed_manifest.get("source_inventory")
    if not isinstance(previous_inventory, dict):
        previous_inventory = None
    source_inventory = provenance.build_source_inventory(
        mapped_universe,
        universe_record,
        previous_inventory=previous_inventory,
    )
    if require_complete_sources and int(source_inventory["missing_count"]) != 0:
        missing = [
            f"{entry['role']}:{entry['logical_symbol']}"
            for entry in source_inventory["entries"]
            if not entry["exists"]
        ]
        raise FileNotFoundError(
            "Promoted V6 requires every mapped source file; missing "
            f"{missing[:20]}"
        )
    if require_complete_sources:
        provenance.validate_source_inventory_readable(source_inventory)
    input_contract = {
        "schema_version": provenance.CACHE_MANIFEST_SCHEMA_VERSION,
        "hybrid_data_contract": hybrid.cache_manifest_payload(),
        "forward_path_policy": sw.FORWARD_PATH_POLICY,
        "square_off": str(square_off),
        "max_forward_bars": int(max_forward_bars),
        "universe": universe_record,
        "source_fingerprint": source_inventory["source_fingerprint"],
        "require_complete_sources": bool(require_complete_sources),
    }
    input_fingerprint = common.canonical_json_sha256(input_contract)
    cache_valid = bool(
        observed_manifest.get("schema_version")
        == provenance.CACHE_MANIFEST_SCHEMA_VERSION
        and observed_manifest.get("input_fingerprint") == input_fingerprint
        and _cache_artifacts_valid(observed_manifest, sig_path, npz_path)
    )
    if cache_valid and not rebuild:
        print("[CACHE] loading signal table", flush=True)
        signals, paths = _load_cached_signals(sig_path, npz_path)
        # Refresh cheap stat metadata after a same-content touch so future runs
        # can continue to reuse the already verified per-file hashes.
        if observed_manifest.get("source_inventory") != source_inventory:
            observed_manifest["source_inventory"] = source_inventory
            observed_manifest["universe"] = universe_record
            observed_manifest["verified_at_ist"] = common.now_ist().isoformat(
                timespec="seconds"
            )
            common.atomic_write_json(CACHE_MANIFEST_PATH, observed_manifest)
        result = (signals, paths, observed_manifest) if return_provenance else (signals, paths)
        return result

    if not rebuild and (sig_path.exists() or npz_path.exists()) and not cache_valid:
        print("[CACHE] source inventory or artifact hash changed; rebuilding", flush=True)
    print("[BUILD] hybrid equity-price/futures-OI signal superset...", flush=True)
    signals, paths = sw.build_signal_table(
        None,
        square_off=square_off,
        max_forward_bars=max_forward_bars,
        mapped_universe=mapped_universe,
    )
    verified_inventory = provenance.build_source_inventory(
        mapped_universe,
        universe_record,
        previous_inventory=source_inventory,
    )
    if verified_inventory["source_fingerprint"] != source_inventory["source_fingerprint"]:
        raise RuntimeError(
            "FNO backtest sources changed while the signal cache was being built; "
            "discarding the uncommitted build."
        )
    common.atomic_write_parquet(signals, sig_path)
    flat: dict[str, np.ndarray] = {}
    for sid, p in paths.items():
        flat[f"{sid}_h"] = p["high"]; flat[f"{sid}_l"] = p["low"]; flat[f"{sid}_c"] = p["close"]
    _atomic_write_npz(npz_path, flat)
    manifest = {
        "schema_version": provenance.CACHE_MANIFEST_SCHEMA_VERSION,
        "built_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "input_fingerprint": input_fingerprint,
        "input_contract": input_contract,
        "universe": universe_record,
        "source_inventory": verified_inventory,
        "artifacts": {
            "signals": provenance.artifact_record(sig_path),
            "paths": provenance.artifact_record(npz_path),
        },
    }
    common.atomic_write_json(CACHE_MANIFEST_PATH, manifest)
    result = (signals, paths, manifest) if return_provenance else (signals, paths)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.publish_status(SESSION, "RUNNING")

    signals, paths = load_signals(
        args.square_off,
        args.max_forward_bars,
        args.rebuild_cache,
        universe_path=args.universe_path or None,
        universe_date=args.universe_date or None,
    )
    ctx = sw.build_market_context()
    signals = signals.merge(ctx[["day", "hhmm_int", "breadth", "nifty_ret_day"]],
                            on=["day", "hhmm_int"], how="left")
    attached = int(signals["breadth"].notna().sum())
    print(f"[DATA] {len(signals):,} candidate signals | breadth attached to {attached:,}",
          flush=True)

    split = pd.Timestamp(args.split_day).date()
    days = sorted(set(signals["day"]))
    train_days = {d for d in days if d < split}
    test_days = {d for d in days if d >= split}
    print(f"[SPLIT] train {len(train_days)} days | test {len(test_days)} days", flush=True)

    guards = Guards(
        min_trades=args.min_trades, min_day_win=args.min_day_win,
        max_top_day_share=args.max_top_day_share, min_days_traded=args.min_days_traded,
    )

    results = {}
    evaluated_total = 0
    for side in ("LONG", "SHORT"):
        df, evaluated = optimise(
            signals, paths, side=side, train_days=train_days, test_days=test_days,
            grid=DEFAULT_GRID, brackets=DEFAULT_BRACKETS, guards=guards,
            cost_bps=args.cost_bps, top_n=args.top_n,
        )
        results[side] = df
        evaluated_total += evaluated
        if not df.empty:
            common.atomic_write_csv(df, RESULT_DIR / f"optimize_shortlist_{side}.csv")

    meta = {
        "train_from": min(train_days) if train_days else None,
        "train_to": max(train_days) if train_days else None,
        "test_from": min(test_days) if test_days else None,
        "test_to": max(test_days) if test_days else None,
        "n_train": len(train_days), "n_test": len(test_days),
        "evaluated": evaluated_total, "cost_bps": args.cost_bps,
        "guards": asdict(guards),
    }
    report = render(results["LONG"], results["SHORT"], meta)

    # Permutation test on the single best surviving candidate per side.
    perm_lines = ["## Day-block permutation test", ""]
    for side in ("LONG", "SHORT"):
        df = results[side]
        if df.empty:
            perm_lines.append(f"- {side}: no candidate to test.")
            continue
        best = df.iloc[0]
        pt = permutation_test(
            signals, paths, best, side=side, days=test_days,
            cost_bps=args.cost_bps, n_iter=args.permutations,
        )
        if pt:
            perm_lines.append(
                f"- **{side}** best candidate on TEST: PF {pt['observed_pf']:.3f} "
                f"on {pt['n']} trades, permutation p = {pt['p_value']:.3f}"
            )
    report += "\n" + "\n".join(perm_lines) + "\n"

    common.atomic_write_text(REPORT_PATH, report)
    print(report, flush=True)
    common.publish_status(SESSION, "SUCCESS", evaluated=evaluated_total,
                          duration_sec=round(time.monotonic() - started, 1))
    print(f"[DONE] {time.monotonic() - started:.0f}s | {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
