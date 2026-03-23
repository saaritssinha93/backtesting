# -*- coding: utf-8 -*-
"""
eqidv2_eod_scheduler_for_5mins_data_live_minimal.py
=================================================
Live-only 5-minute scheduler using the minimal indicator fetch core.

Why this exists:
- keeps the original scheduler/core intact
- computes only the indicator set used by the live/backtest v7/v15 path
- adds a dedicated 5m live fetch session without disturbing the 15m path

Inherited fixes from the base scheduler:

1) Your core loader can treat filtered_stocks_MIS.selected_stocks (dict) as a TOKEN MAP
   if values are numeric, then int() casts weights like 0.6 -> 0, leading to:
      "No token for XYZ, skipping."
   This scheduler monkey-patches core.load_stocks_universe to IGNORE such "small-value" token maps
   and force a real token fetch via kite.instruments().

2) The old scheduler ran too close to the candle boundary. Kite can lag a bit.
   This version runs exactly once per slot and uses a configurable buffer
   (default 2s; override via --buffer-sec / EQIDV2_5M_BUFFER_SEC).

3) The old scheduler referenced core.HOLIDAYS_FILE (not present). Core exposes HOLIDAYS_FILE_DEFAULT.

Run:
    python eqidv2_eod_scheduler_for_5mins_data_live_minimal.py

Optional:
    python eqidv2_eod_scheduler_for_5mins_data_live_minimal.py --buffer-sec 20 --max-workers 16
    The scheduler treats --max-workers as a total worker budget across all 4 app partitions.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import sys
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Callable, Optional

import pytz
from eqidv2_runtime_paths import DATA_5M_DIR as RUNTIME_DATA_5M_DIR
from eqidv2_runtime_paths import REPORTS_DIR as RUNTIME_REPORTS_DIR

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------
# Locate eqidv2 folder (robust even if your repo is nested oddly)
# ---------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
CORE_FILENAME = "trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py"

def _find_core_dir(start: Path, max_up: int = 6) -> Path:
    cur = start
    for _ in range(max_up + 1):
        if (cur / CORE_FILENAME).exists():
            return cur
        cur = cur.parent
    # fallback: use script dir
    return start

EQIDV2_DIR = _find_core_dir(SCRIPT_DIR)
if str(EQIDV2_DIR) not in sys.path:
    sys.path.insert(0, str(EQIDV2_DIR))

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal as core  # noqa: E402


# ---------------------------------------------------------------------
# Monkey-patch kite session to read api_key.txt / access_token.txt from EQIDV2_DIR
# (core.setup_kite_session reads relative files from CWD) fileciteturn36file10L31-L38
# ---------------------------------------------------------------------
def _read_first_token(p: Path) -> str:
    txt = p.read_text(encoding="utf-8").strip()
    if not txt:
        raise RuntimeError(f"Auth file is empty: {p}")
    return txt.split()[0].strip()

def setup_kite_session_from_eqidv2_dir():
    from kiteconnect import KiteConnect  # imported here to avoid import costs on module import
    api_key = _read_first_token(EQIDV2_DIR / "api_key.txt")
    access_token = _read_first_token(EQIDV2_DIR / "access_token.txt")
    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(access_token)
    return kc

def _setup_kite_session_n_from_eqidv2_dir(app_idx: int):
    """
    Additional app session (app2/app3/app4):
    - request_tokenN.txt is validated for presence (operational sanity check)
    - access_tokenN.txt is used for auth
    - api_keyN.txt is preferred; fallback to api_key.txt if absent
    """
    from kiteconnect import KiteConnect  # imported here to avoid import costs on module import

    if app_idx not in (2, 3, 4):
        raise ValueError(f"Unsupported app index: {app_idx}")

    request_token_n = EQIDV2_DIR / f"request_token{app_idx}.txt"
    access_token_n = EQIDV2_DIR / f"access_token{app_idx}.txt"
    api_key_n = EQIDV2_DIR / f"api_key{app_idx}.txt"
    api_key1 = EQIDV2_DIR / "api_key.txt"

    if not request_token_n.exists():
        raise FileNotFoundError(f"Missing app{app_idx} auth file: {request_token_n}")
    if not access_token_n.exists():
        raise FileNotFoundError(f"Missing app{app_idx} auth file: {access_token_n}")

    _ = _read_first_token(request_token_n)
    api_key_path = api_key_n if api_key_n.exists() else api_key1
    if api_key_path == api_key1:
        print(f"[WARN] api_key{app_idx}.txt not found; app{app_idx} will use api_key.txt.")

    api_key = _read_first_token(api_key_path)
    access_token = _read_first_token(access_token_n)

    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(access_token)
    return kc

def setup_kite_session2_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(2)

def setup_kite_session3_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(3)

def setup_kite_session4_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(4)

core.setup_kite_session = setup_kite_session_from_eqidv2_dir


# ---------------------------------------------------------------------
# Monkey-patch universe loader: if token_map looks like weights/flags (all small),
# ignore it so core will fetch real instrument tokens.
#
# Core currently can treat selected_stocks dict numeric values as tokens fileciteturn43file2L95-L103
# ---------------------------------------------------------------------
_orig_load_universe = getattr(core, "load_stocks_universe", None)

def _looks_like_real_tokens(token_map: dict) -> bool:
    try:
        vals = [int(v) for v in token_map.values()]
        if not vals:
            return False
        # Instrument tokens are typically 5+ digits. If everything is tiny, it's likely weights/flags.
        return max(vals) >= 1000
    except Exception:
        return False

def load_stocks_universe_fixed(*args, **kwargs):
    if _orig_load_universe is None:
        raise RuntimeError("core.load_stocks_universe not found")
    tickers, token_map = _orig_load_universe(*args, **kwargs)
    if token_map and not _looks_like_real_tokens(token_map):
        # Force core to fetch tokens from kite.instruments()
        try:
            logger = args[0] if args else None
            if logger is not None:
                logger.warning("Token map from filtered_stocks_MIS looks like weights/flags (small ints). Ignoring it and forcing token fetch.")
        except Exception:
            pass
        token_map = {}
    return tickers, token_map

core.load_stocks_universe = load_stocks_universe_fixed

# ---------------------------------------------------------------------
# IMPORTANT FIX: core.ticker_is_fresh() has a +/- one-step tolerance that can
# wrongly treat a file that is ONE candle behind as "fresh".
# For 5m, that can make updates happen every 10 minutes (it skips every alternate slot).
# So we patch it to be STRICT: last_ts must be >= expected_ts (minus a tiny tol).
# ---------------------------------------------------------------------
_orig_ticker_is_fresh = getattr(core, 'ticker_is_fresh', None)

def ticker_is_fresh_strict(mode: str, out_path: str, now_ist: datetime, holidays: set, intraday_ts: str) -> bool:
    existing_path = core._resolve_existing_store_path(out_path)
    if not os.path.exists(existing_path):
        return False
    last_ts = core._read_last_ts_from_store(existing_path)
    if last_ts is None:
        return False
    # normalize tz
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(core.IST_TZ)
    else:
        last_ts = last_ts.tz_convert(core.IST_TZ)
    spec = core.expected_last_stamp(mode, now_ist, holidays, intraday_ts)
    exp_ts = spec['value']
    if exp_ts.tzinfo is None:
        exp_ts = core.IST_TZ.localize(exp_ts)
    tol = timedelta(seconds=1)
    return last_ts >= (exp_ts - tol)

# Apply patch only if core exposes expected_last_stamp (newer core); otherwise keep original.
if hasattr(core, 'expected_last_stamp') and _orig_ticker_is_fresh is not None:
    core.ticker_is_fresh = ticker_is_fresh_strict


# ---------------------------------------------------------------------
# Scheduler logic
# ---------------------------------------------------------------------
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 35)  # keep scheduler alive long enough to process the 15:30 close bar
HARD_STOP = dtime(15, 50)  # exit after this
FIRST_5M_CLOSE = dtime(9, 20)  # first completed 5m candle close timestamp
DEFAULT_MAX_WORKERS = int(os.getenv("EQIDV2_5M_MAX_WORKERS", "16"))
DEFAULT_MAX_WORKERS_PER_APP = int(os.getenv("EQIDV2_5M_MAX_WORKERS_PER_APP", "4"))
DEFAULT_BUFFER_SEC = int(os.getenv("EQIDV2_5M_BUFFER_SEC", "2"))
DEFAULT_QUARTER_HOUR_BUFFER_SEC = int(os.getenv("EQIDV2_5M_QUARTER_HOUR_BUFFER_SEC", "75"))
DEFAULT_REFRESH_TOKENS = str(os.getenv("EQIDV2_5M_REFRESH_TOKENS", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_ENABLE_OPENING_SLOT_FETCH = str(
    os.getenv("EQIDV2_5M_ENABLE_OPENING_SLOT_FETCH", "1")
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# ---------------------------------------------------------------------
# Opening-slot expected stamp override:
# For 09:15 slot we intentionally allow a "start-ts" snapshot fetch.
# Core expected_last_stamp(start) normally resolves to 09:10 at 09:15,
# which suppresses opening updates. We force expected to 09:15 for this slot.
# ---------------------------------------------------------------------
_orig_expected_last_stamp = getattr(core, "expected_last_stamp", None)

def expected_last_stamp_opening_fix(mode: str, now_ist_dt: datetime, holidays: set, intraday_ts: str):
    if _orig_expected_last_stamp is None:
        raise RuntimeError("core.expected_last_stamp not found")

    mode_l = str(mode).lower().strip()
    ts_mode = str(intraday_ts).lower().strip()

    if now_ist_dt.tzinfo is None:
        now_ist_dt = IST.localize(now_ist_dt)

    t = now_ist_dt.time()
    in_opening_window = (MARKET_OPEN <= t < FIRST_5M_CLOSE)
    if mode_l == "5min" and ts_mode == "start" and in_opening_window:
        open_anchor = IST.localize(
            datetime(
                now_ist_dt.year,
                now_ist_dt.month,
                now_ist_dt.day,
                MARKET_OPEN.hour,
                MARKET_OPEN.minute,
                0,
            )
        )
        return {"kind": "ts", "value": open_anchor, "step_min": 5}

    return _orig_expected_last_stamp(mode, now_ist_dt, holidays, intraday_ts)

if _orig_expected_last_stamp is not None:
    core.expected_last_stamp = expected_last_stamp_opening_fix


def now_ist() -> datetime:
    return datetime.now(IST)

def _floor_to_5m(dt: datetime) -> datetime:
    # dt is tz-aware
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)

def _next_boundary(dt: datetime) -> datetime:
    flo = _floor_to_5m(dt)
    if dt == flo:
        return flo + timedelta(minutes=5)
    return flo + timedelta(minutes=5)


def _slot_buffer_seconds(slot_end: datetime, base_buffer_sec: int, quarter_hour_buffer_sec: int) -> int:
    if int(slot_end.minute) % 15 == 0:
        return max(int(base_buffer_sec), int(quarter_hour_buffer_sec))
    return int(base_buffer_sec)

def _is_trading_time(dt: datetime) -> bool:
    t = dt.time()
    return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)

def _is_trading_day(dt: datetime, holidays: set) -> bool:
    if dt.weekday() >= 5:
        return False
    return dt.date() not in holidays

def _next_trading_day_open(dt: datetime, holidays: set) -> datetime:
    probe = dt
    while True:
        if _is_trading_day(probe, holidays):
            return IST.localize(
                datetime(
                    probe.year,
                    probe.month,
                    probe.day,
                    MARKET_OPEN.hour,
                    MARKET_OPEN.minute,
                    0,
                )
            )
        probe = probe + timedelta(days=1)

def _read_holidays_set() -> set:
    # Core exposes HOLIDAYS_FILE_DEFAULT fileciteturn36file2L54-L56
    hf = getattr(core, "HOLIDAYS_FILE_DEFAULT", "holidays.csv")
    try:
        return set(core._read_holidays(str(Path(hf))))
    except Exception:
        return set()

def _split_tickers_for_four_apps(tickers: list[str]) -> tuple[list[str], list[str], list[str], list[str]]:
    ordered = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    q = len(ordered) // 4
    return ordered[:q], ordered[q:(2 * q)], ordered[(2 * q):(3 * q)], ordered[(3 * q):]

def _partition_worker_budget(total_budget: int, active_partitions: int, per_app_cap: int) -> int:
    total_budget = max(1, int(total_budget))
    active_partitions = max(1, int(active_partitions))
    per_app_cap = max(1, int(per_app_cap))
    workers_per_partition = (total_budget + active_partitions - 1) // active_partitions
    return max(1, min(per_app_cap, workers_per_partition))

def _run_partition(
    mode: str,
    partition_name: str,
    partition_tickers: list[str],
    partition_token_map: dict[str, int],
    setup_kite_fn: Callable[[], object],
    *,
    max_workers: int,
    report_dir: str,
    holidays: set,
    refresh_tokens: bool,
    intraday_ts: str,
    skip_if_fresh: bool,
) -> None:
    if not partition_tickers:
        print(f"[INFO] {partition_name}: no tickers assigned; skipping.")
        return

    current_loader = core.load_stocks_universe
    current_setup = core.setup_kite_session

    def _partition_loader(logger):
        logger.info("[%s] universe override for %s: %d symbols", mode.upper(), partition_name, len(partition_tickers))
        return list(partition_tickers), dict(partition_token_map)

    try:
        core.load_stocks_universe = _partition_loader
        core.setup_kite_session = setup_kite_fn
        core.run_mode(
            mode,
            max_workers=max_workers,
            skip_if_fresh=bool(skip_if_fresh),
            intraday_ts=str(intraday_ts),
            holidays=holidays,
            report_dir=report_dir,
            refresh_tokens=refresh_tokens,
            print_missing_rows=False,
            print_missing_rows_max=5,
        )
    finally:
        core.load_stocks_universe = current_loader
        core.setup_kite_session = current_setup

def _run_partition_worker(
    mode: str,
    partition_name: str,
    partition_tickers: list[str],
    partition_token_map: dict[str, int],
    setup_kind: str,
    *,
    max_workers: int,
    report_dir: str,
    holidays: set,
    refresh_tokens: bool,
    intraday_ts: str,
    skip_if_fresh: bool,
    result_queue,
) -> None:
    # Use stream-only logger in child process to avoid concurrent file truncation.
    logger = core.logging.getLogger("stocks_fetcher")
    logger.setLevel(core.logging.INFO)
    if not logger.handlers:
        fmt = core.logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        sh = core.logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    setup_fn_map = {
        "app1": setup_kite_session_from_eqidv2_dir,
        "app2": setup_kite_session2_from_eqidv2_dir,
        "app3": setup_kite_session3_from_eqidv2_dir,
        "app4": setup_kite_session4_from_eqidv2_dir,
    }
    setup_fn = setup_fn_map.get(setup_kind, setup_kite_session_from_eqidv2_dir)
    try:
        _run_partition(
            mode,
            partition_name,
            partition_tickers,
            partition_token_map,
            setup_fn,
            max_workers=max_workers,
            report_dir=report_dir,
            holidays=holidays,
            refresh_tokens=refresh_tokens,
            intraday_ts=intraday_ts,
            skip_if_fresh=skip_if_fresh,
        )
        result_queue.put((partition_name, True, ""))
    except Exception as e:
        result_queue.put((partition_name, False, str(e)))

def run_update_5m_once(
    max_workers: int,
    max_workers_per_app: int,
    report_dir: str,
    buffer_sec: int,
    refresh_tokens: bool,
    opening_slot: bool = False,
) -> None:
    holidays = _read_holidays_set()
    logger = core.logging.getLogger("stocks_fetcher")
    all_tickers, pre_token_map = core.load_stocks_universe(logger)
    token_map = {str(k).strip().upper(): int(v) for k, v in dict(pre_token_map).items()}

    intraday_ts_mode = "start" if opening_slot else "end"
    skip_if_fresh_mode = False if opening_slot else True
    if opening_slot:
        print(
            "[INFO] Opening-slot mode active: intraday_ts=start, skip_if_fresh=False "
            "(attempting 09:15 opening snapshot fetch)."
        )

    app1_tickers, app2_tickers, app3_tickers, app4_tickers = _split_tickers_for_four_apps(all_tickers)
    app1_token_map = {t: token_map[t] for t in app1_tickers if t in token_map}
    app2_token_map = {t: token_map[t] for t in app2_tickers if t in token_map}
    app3_token_map = {t: token_map[t] for t in app3_tickers if t in token_map}
    app4_token_map = {t: token_map[t] for t in app4_tickers if t in token_map}

    print(
        "[INFO] 5min split:",
        f"app1={len(app1_tickers)} tickers (api_key.txt/access_token.txt),",
        f"app2={len(app2_tickers)} tickers (request_token2.txt/access_token2.txt),",
        f"app3={len(app3_tickers)} tickers (request_token3.txt/access_token3.txt),",
        f"app4={len(app4_tickers)} tickers (request_token4.txt/access_token4.txt)",
    )

    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    partitions = [
        ("app1", app1_tickers, app1_token_map),
        ("app2", app2_tickers, app2_token_map),
        ("app3", app3_tickers, app3_token_map),
        ("app4", app4_tickers, app4_token_map),
    ]
    active_partition_count = sum(1 for _, ptickers, _ in partitions if ptickers)
    partition_max_workers = _partition_worker_budget(
        total_budget=max_workers,
        active_partitions=active_partition_count,
        per_app_cap=max_workers_per_app,
    )

    print(
        "[INFO] 5min worker budget:",
        f"total={max_workers},",
        f"active_apps={active_partition_count},",
        f"per_app_cap={max_workers_per_app},",
        f"effective_per_app={partition_max_workers}",
    )

    workers: list[tuple[str, object]] = []
    for pname, ptickers, ptoken_map in partitions:
        proc = ctx.Process(
            target=_run_partition_worker,
            args=(
                "5min",
                pname,
                ptickers,
                ptoken_map,
                pname,
            ),
            kwargs={
                "max_workers": partition_max_workers,
                "report_dir": os.path.join(report_dir, pname),
                "holidays": holidays,
                "refresh_tokens": refresh_tokens,
                "intraday_ts": intraday_ts_mode,
                "skip_if_fresh": skip_if_fresh_mode,
                "result_queue": result_queue,
            },
        )
        workers.append((pname, proc))

    for _, proc in workers:
        proc.start()

    for _, proc in workers:
        proc.join()

    result_map: dict[str, tuple[bool, str]] = {}
    for _ in workers:
        try:
            pname, ok, msg = result_queue.get(timeout=1.0)
            result_map[str(pname)] = (bool(ok), str(msg))
        except Exception:
            break

    failures: list[str] = []
    for pname, proc in workers:
        ok, msg = result_map.get(pname, (proc.exitcode == 0, f"worker_exit={proc.exitcode}"))
        if (not ok) or (proc.exitcode not in (0, None)):
            failures.append(f"{pname}: {msg}")

    if failures:
        raise RuntimeError("Parallel partition run failed: " + " | ".join(failures))

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Total worker budget across all 4 app partitions.",
    )
    ap.add_argument(
        "--max-workers-per-app",
        type=int,
        default=DEFAULT_MAX_WORKERS_PER_APP,
        help="Hard cap for workers inside each app partition.",
    )
    ap.add_argument("--buffer-sec", type=int, default=DEFAULT_BUFFER_SEC, help="How long after boundary to run (Kite can lag).")
    ap.add_argument(
        "--quarter-hour-buffer-sec",
        type=int,
        default=DEFAULT_QUARTER_HOUR_BUFFER_SEC,
        help="How long after :00/:15/:30/:45 boundaries to run 5-minute slots to avoid colliding with the 15-minute fetch.",
    )
    ap.add_argument("--refresh-tokens", dest="refresh_tokens", action="store_true", help="Force refresh kite instrument token cache.")
    ap.add_argument("--no-refresh-tokens", dest="refresh_tokens", action="store_false", help="Do not refresh kite instrument token cache.")
    ap.set_defaults(refresh_tokens=DEFAULT_REFRESH_TOKENS)
    ap.add_argument(
        "--enable-opening-slot-fetch",
        dest="enable_opening_slot_fetch",
        action="store_true",
        help="Process 09:15 slot using opening snapshot mode.",
    )
    ap.add_argument(
        "--disable-opening-slot-fetch",
        dest="enable_opening_slot_fetch",
        action="store_false",
        help="Skip 09:15 slot (old behavior).",
    )
    ap.set_defaults(enable_opening_slot_fetch=DEFAULT_ENABLE_OPENING_SLOT_FETCH)
    ap.add_argument(
        "--report-dir",
        default=str(RUNTIME_REPORTS_DIR / "stocks_missing_reports_5m"),
    )
    args = ap.parse_args()

    # Ensure core INFO logs (including timing/verification summaries) are emitted.
    try:
        core.setup_logger()
        print("[INFO] stocks_fetcher logger initialized.")
    except Exception as e:
        print(f"[WARN] Failed to initialize stocks_fetcher logger: {e}")

    print("[LIVE] EQIDV2 5m scheduler started.")
    print(f"       Using EQIDV2_DIR: {EQIDV2_DIR}")
    print(f"       Output dir (5m): {getattr(core, 'DIRS', {}).get('5min', {}).get('out', str(RUNTIME_DATA_5M_DIR))}")
    print(f"       Runs every 5 mins between {MARKET_OPEN.strftime('%H:%M')} and {MARKET_CLOSE.strftime('%H:%M')} IST (trading days).")
    print(f"       Buffer after boundary: {args.buffer_sec}s")
    print(f"       Quarter-hour buffer after boundary: {args.quarter_hour_buffer_sec}s")
    print(f"       Max workers (total budget): {args.max_workers}")
    print(f"       Max workers per app cap: {args.max_workers_per_app}")
    print(f"       Refresh tokens: {args.refresh_tokens}")
    print(f"       Opening slot fetch (09:15): {args.enable_opening_slot_fetch}")
    print(f"       Process will exit at {HARD_STOP.strftime('%H:%M')} IST.")
    holidays = _read_holidays_set()
    print(f"       Holidays loaded: {len(holidays)}")

    last_run_slot: Optional[datetime] = None

    while True:
        dt = now_ist()
        if dt.time() >= HARD_STOP:
            print("[DONE] Hard stop reached. Exiting.")
            return

        if not _is_trading_day(dt, holidays):
            nxt = _next_trading_day_open(dt + timedelta(days=1), holidays)
            sleep_s = max(30.0, (nxt - dt).total_seconds())
            print(
                f"[WAIT] Non-trading day ({dt.strftime('%Y-%m-%d')}). "
                f"Sleeping {int(min(sleep_s, 300))}s..."
            )
            time.sleep(min(sleep_s, 300))
            continue

        if not _is_trading_time(dt):
            nxt = IST.localize(datetime(dt.year, dt.month, dt.day, MARKET_OPEN.hour, MARKET_OPEN.minute, 0))
            if dt.time() > MARKET_OPEN:
                nxt = nxt + timedelta(days=1)
            sleep_s = max(30.0, (nxt - dt).total_seconds())
            print(f"[WAIT] Outside market hours. Sleeping {int(min(sleep_s, 300))}s...")
            time.sleep(min(sleep_s, 300))
            continue

        # Determine the slot we should process: last completed 5m boundary
        slot_end = _floor_to_5m(dt)

        opening_slot = slot_end.time() < FIRST_5M_CLOSE
        if opening_slot and (not args.enable_opening_slot_fetch):
            if last_run_slot != slot_end:
                print(
                    f"[SKIP] Opening slot {slot_end.strftime('%H:%M')} disabled by config. "
                    "First actionable slot is 09:20."
                )
            last_run_slot = slot_end
            nxt = _next_boundary(dt) + timedelta(seconds=int(args.buffer_sec))
            time.sleep(max(2.0, (nxt - now_ist()).total_seconds()))
            continue

        # Don't run until buffer has passed for this slot_end
        slot_buffer_sec = _slot_buffer_seconds(
            slot_end,
            int(args.buffer_sec),
            int(args.quarter_hour_buffer_sec),
        )
        if dt < (slot_end + timedelta(seconds=slot_buffer_sec)):
            wake = slot_end + timedelta(seconds=slot_buffer_sec)
            time.sleep(max(1.0, (wake - dt).total_seconds()))
            continue

        if last_run_slot == slot_end:
            # Sleep until next slot buffer
            nxt = _next_boundary(dt) + timedelta(seconds=int(args.buffer_sec))
            time.sleep(max(2.0, (nxt - now_ist()).total_seconds()))
            continue

        tag = "OPEN" if opening_slot else "RUN "
        if slot_buffer_sec != int(args.buffer_sec):
            print(
                f"[INFO] Quarter-hour slot {slot_end.strftime('%H:%M')} using staggered buffer "
                f"{slot_buffer_sec}s (base={int(args.buffer_sec)}s)."
            )
        print(f"[{tag}] Updating EQIDV2 5m for slot {slot_end.strftime('%H:%M')} at {dt.strftime('%Y-%m-%d %H:%M:%S%z')}")
        try:
            run_update_5m_once(
                max_workers=int(args.max_workers),
                max_workers_per_app=int(args.max_workers_per_app),
                report_dir=str(args.report_dir),
                buffer_sec=slot_buffer_sec,
                refresh_tokens=bool(args.refresh_tokens),
                opening_slot=bool(opening_slot),
            )
        except Exception as e:
            print(f"[ERROR] Update failed: {e}", file=sys.stderr)

        last_run_slot = slot_end

        # Sleep until next slot + buffer
        nxt = _next_boundary(dt) + timedelta(seconds=int(args.buffer_sec))
        print(f"[INFO] Done slot {slot_end.strftime('%H:%M')}. Next at {nxt.strftime('%H:%M:%S')}.")
        time.sleep(max(2.0, (nxt - now_ist()).total_seconds()))

if __name__ == "__main__":
    main()
