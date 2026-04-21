from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from kite_testing import load_all_sessions


ROOT = Path(__file__).resolve().parent
INSTRUMENTS_CSV = ROOT / "Merged_NSE_BSE_Instruments.csv"
FULL_UNIVERSE_FILE = ROOT / "filtered_stocks_NSE_cash_full.py"
INTRADAY_5X_FILE = ROOT / "filtered_stocks_NSE_cash_intraday_5x.py"
STOCK_UNIVERSE_FILE = ROOT / "filtered_stocks_NSE_stock_universe.py"
STOCK_INTRADAY_5X_FILE = ROOT / "filtered_stocks_NSE_stock_intraday_5x.py"
MIS_V2_FILE = ROOT / "filtered_stocks_MIS_v2.py"
DEFAULT_BATCH_SIZE = 64
DEFAULT_LTP_BATCH_SIZE = 250
DEFAULT_QUOTE_BATCH_SIZE = 200
STOCK_EXCLUDE_SYMBOL_PARTS = ("ETF", "INAV")
STOCK_EXCLUDE_NAME_PARTS = (
    "ETF",
    "INAV",
    "GOLDBONDS",
    "LIQUID",
    "GILT",
    "TREASURY",
    "SDL ",
    " SDL",
    "G-SEC",
    "GSEC",
)


def _now_ist_text() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def load_full_nse_cash_universe(csv_path: Path) -> List[str]:
    symbols = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            exchange = _normalize_symbol(row.get("exchange"))
            segment = _normalize_symbol(row.get("segment"))
            symbol = _normalize_symbol(row.get("tradingsymbol"))
            if exchange != "NSE":
                continue
            if segment != "NSE":
                continue
            if symbol:
                symbols.add(symbol)
    return sorted(symbols)


def _is_nse_stock_row(row: dict[str, Any]) -> bool:
    exchange = _normalize_symbol(row.get("exchange"))
    segment = _normalize_symbol(row.get("segment"))
    instrument_type = _normalize_symbol(row.get("instrument_type"))
    symbol = _normalize_symbol(row.get("tradingsymbol"))
    name = _normalize_symbol(row.get("name"))
    if exchange != "NSE" or segment != "NSE" or instrument_type != "EQ":
        return False
    if not symbol or not name:
        return False
    if symbol.endswith("-GB"):
        return False
    # Most debt/NCD cash symbols are digit-led and hyphenated; genuine stocks
    # like 20MICRONS or 360ONE remain included because they are not hyphenated.
    if symbol[:1].isdigit() and "-" in symbol:
        return False
    if any(part in symbol for part in STOCK_EXCLUDE_SYMBOL_PARTS):
        return False
    if any(part in name for part in STOCK_EXCLUDE_NAME_PARTS):
        return False
    return True


def load_full_nse_stock_universe(csv_path: Path) -> List[str]:
    symbols = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if _is_nse_stock_row(row):
                symbol = _normalize_symbol(row.get("tradingsymbol"))
                if symbol:
                    symbols.add(symbol)
    return sorted(symbols)


def _choose_session(preferred_app: str):
    sessions = load_all_sessions()
    if preferred_app:
        preferred = preferred_app.strip().lower()
        matching = [item for item in sessions if item[0].lower() == preferred]
        if not matching:
            available = ", ".join(name for name, _client, _profile in sessions)
            raise RuntimeError(f"Requested app {preferred_app!r} not available. Available: {available}")
        return matching[0]
    return sessions[0]


def _chunked(items: Sequence[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(items), size):
        yield list(items[index:index + size])


def _build_margin_payload(symbols: Sequence[str], quantity: int) -> List[dict[str, Any]]:
    payload = []
    for symbol in symbols:
        payload.append(
            {
                "exchange": "NSE",
                "tradingsymbol": symbol,
                "transaction_type": "BUY",
                "variety": "regular",
                "product": "MIS",
                "order_type": "MARKET",
                "quantity": int(quantity),
                "price": 0.0,
                "trigger_price": 0.0,
            }
        )
    return payload


def _extract_leverage(row: dict[str, Any]) -> float:
    leverage = row.get("leverage")
    if leverage is None:
        return 0.0
    try:
        return float(leverage)
    except (TypeError, ValueError):
        return 0.0


def scan_intraday_leverage(
    preferred_app: str,
    symbols: Sequence[str],
    min_leverage: float,
    batch_size: int,
    quantity: int,
) -> Tuple[str, List[str], List[Tuple[str, float]], int]:
    app_name, client, _profile = _choose_session(preferred_app)
    eligible: List[str] = []
    ineligible: List[Tuple[str, float]] = []
    api_calls = 0

    for batch in _chunked(symbols, batch_size):
        response = client.order_margins(_build_margin_payload(batch, quantity))
        api_calls += 1
        if len(response) != len(batch):
            raise RuntimeError(
                f"Unexpected order_margins response length for batch starting {batch[0]!r}: "
                f"expected {len(batch)}, got {len(response)}"
            )
        for symbol, row in zip(batch, response):
            leverage = _extract_leverage(row)
            if leverage >= min_leverage:
                eligible.append(symbol)
            else:
                ineligible.append((symbol, leverage))

    return app_name, eligible, ineligible, api_calls


def fetch_ltp_map(
    preferred_app: str,
    symbols: Sequence[str],
    batch_size: int,
) -> Tuple[str, Dict[str, float], List[str], int]:
    app_name, client, _profile = _choose_session(preferred_app)
    ltp_map: Dict[str, float] = {}
    missing: List[str] = []
    api_calls = 0

    for batch in _chunked(symbols, batch_size):
        instruments = [f"NSE:{symbol}" for symbol in batch]
        payload = client.ltp(instruments) or {}
        api_calls += 1
        for symbol in batch:
            item = payload.get(f"NSE:{symbol}", {}) if isinstance(payload, dict) else {}
            try:
                last_price = float(item.get("last_price", 0.0) or 0.0)
            except (TypeError, ValueError):
                last_price = 0.0
            if last_price > 0:
                ltp_map[symbol] = last_price
            else:
                missing.append(symbol)

    return app_name, ltp_map, missing, api_calls


def fetch_quote_metrics(
    preferred_app: str,
    symbols: Sequence[str],
    batch_size: int,
) -> Tuple[str, Dict[str, Dict[str, float]], List[str], int]:
    app_name, client, _profile = _choose_session(preferred_app)
    metrics: Dict[str, Dict[str, float]] = {}
    missing: List[str] = []
    api_calls = 0

    for batch in _chunked(symbols, batch_size):
        instruments = [f"NSE:{symbol}" for symbol in batch]
        payload = client.quote(instruments) or {}
        api_calls += 1
        for symbol in batch:
            row = payload.get(f"NSE:{symbol}", {}) if isinstance(payload, dict) else {}
            try:
                last_price = float(row.get("last_price", 0.0) or 0.0)
            except (TypeError, ValueError):
                last_price = 0.0
            try:
                volume = float(row.get("volume", 0.0) or 0.0)
            except (TypeError, ValueError):
                volume = 0.0
            try:
                average_price = float(row.get("average_price", 0.0) or 0.0)
            except (TypeError, ValueError):
                average_price = 0.0
            ref_price = average_price if average_price > 0 else last_price
            traded_value = float(volume * ref_price) if volume > 0 and ref_price > 0 else 0.0
            if last_price > 0:
                metrics[symbol] = {
                    "last_price": float(last_price),
                    "volume": float(volume),
                    "average_price": float(average_price),
                    "traded_value": float(traded_value),
                }
            else:
                missing.append(symbol)

    return app_name, metrics, missing, api_calls


def _price_tag(value: float) -> str:
    text = f"{float(value):g}"
    return text.replace(".", "p")


def build_price_filtered_path(min_price: float) -> Path:
    return ROOT / f"filtered_stocks_NSE_stock_intraday_5x_price_ge_{_price_tag(min_price)}.py"


def build_liquid_filtered_path(min_price: float) -> Path:
    return ROOT / f"filtered_stocks_NSE_stock_intraday_5x_price_ge_{_price_tag(min_price)}_liquid.py"


def _py_set_literal(symbols: Sequence[str]) -> str:
    if not symbols:
        return "set()"
    lines = ["{"]
    for symbol in symbols:
        lines.append(f"    {symbol!r},")
    lines.append("}")
    return "\n".join(lines)


def write_universe_module(
    target_path: Path,
    generated_at_ist: str,
    header_lines: Sequence[str],
    symbols: Sequence[str],
) -> None:
    header = [f"# Auto-generated at IST: {generated_at_ist}"]
    header.extend(f"# {line}" for line in header_lines)
    body = [
        *header,
        "",
        f"selected_stocks = {_py_set_literal(symbols)}",
        "",
    ]
    target_path.write_text("\n".join(body), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate full NSE cash and intraday >=5x MIS universe Python files."
    )
    parser.add_argument("--app", default="", help="Optional fixed Kite app name, for example app1.")
    parser.add_argument("--min-leverage", type=float, default=5.0, help="Minimum leverage required.")
    parser.add_argument(
        "--min-price",
        type=float,
        default=0.0,
        help="Optional live LTP cutoff for the stock intraday universe, for example 25.",
    )
    parser.add_argument("--quantity", type=int, default=1, help="Probe quantity for MIS margin preview.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Symbols per order_margins call.")
    parser.add_argument(
        "--ltp-batch-size",
        type=int,
        default=DEFAULT_LTP_BATCH_SIZE,
        help="Symbols per Kite ltp() call when applying a min-price filter.",
    )
    parser.add_argument(
        "--quote-batch-size",
        type=int,
        default=DEFAULT_QUOTE_BATCH_SIZE,
        help="Symbols per Kite quote() call when applying live liquidity filters.",
    )
    parser.add_argument(
        "--min-live-volume",
        type=float,
        default=0.0,
        help="Optional live quote volume cutoff, for example 100000.",
    )
    parser.add_argument(
        "--min-live-traded-value-rs",
        type=float,
        default=0.0,
        help="Optional live quote traded-value cutoff in rupees, for example 50000000 for Rs 5 crore.",
    )
    parser.add_argument(
        "--exclude-series-suffixes",
        default="",
        help="Optional comma-separated symbol suffixes to exclude, for example -SM,-ST,-BE.",
    )
    parser.add_argument(
        "--skip-live-scan",
        action="store_true",
        help="Only build the full NSE cash universe file and skip the live Kite leverage scan.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    generated_at = _now_ist_text()

    full_universe = load_full_nse_cash_universe(INSTRUMENTS_CSV)
    stock_universe = load_full_nse_stock_universe(INSTRUMENTS_CSV)
    write_universe_module(
        FULL_UNIVERSE_FILE,
        generated_at,
        [
            "Source: Merged_NSE_BSE_Instruments.csv",
            "Rule: exchange == NSE and segment == NSE",
            f"Count: {len(full_universe)}",
        ],
        full_universe,
    )
    print(f"[OK] Wrote {FULL_UNIVERSE_FILE.name} with {len(full_universe)} symbols.")
    write_universe_module(
        STOCK_UNIVERSE_FILE,
        generated_at,
        [
            "Source: Merged_NSE_BSE_Instruments.csv",
            "Rule: exchange == NSE, segment == NSE, instrument_type == EQ",
            "Stock-only exclusions: ETF/INAV markers, sovereign gold bonds, debt-like digit-led hyphenated symbols, and obvious debt/liquid/gilt name markers",
            f"Count: {len(stock_universe)}",
        ],
        stock_universe,
    )
    print(f"[OK] Wrote {STOCK_UNIVERSE_FILE.name} with {len(stock_universe)} symbols.")

    if args.skip_live_scan:
        print("[SKIP] Live Kite leverage scan skipped.")
        return 0

    app_name, eligible, ineligible, api_calls = scan_intraday_leverage(
        preferred_app=args.app,
        symbols=full_universe,
        min_leverage=float(args.min_leverage),
        batch_size=max(1, int(args.batch_size)),
        quantity=max(1, int(args.quantity)),
    )
    write_universe_module(
        INTRADAY_5X_FILE,
        generated_at,
        [
            "Source universe: filtered_stocks_NSE_cash_full.py",
            f"Kite app used: {app_name}",
            "Probe: BUY 1 share, NSE regular MIS MARKET dry-run via order_margins()",
            f"Rule: leverage >= {float(args.min_leverage):.2f}x",
            f"Count: {len(eligible)}",
            f"Ineligible: {len(ineligible)}",
            f"API calls: {api_calls}",
        ],
        eligible,
    )
    print(f"[OK] Wrote {INTRADAY_5X_FILE.name} with {len(eligible)} symbols.")
    stock_intraday = sorted(set(stock_universe) & set(eligible))
    write_universe_module(
        STOCK_INTRADAY_5X_FILE,
        generated_at,
        [
            "Source universe: filtered_stocks_NSE_stock_universe.py",
            f"Kite app used: {app_name}",
            "Probe: BUY 1 share, NSE regular MIS MARKET dry-run via order_margins()",
            f"Rule: leverage >= {float(args.min_leverage):.2f}x",
            f"Count: {len(stock_intraday)}",
        ],
        stock_intraday,
    )
    print(f"[OK] Wrote {STOCK_INTRADAY_5X_FILE.name} with {len(stock_intraday)} symbols.")
    if float(args.min_price) > 0:
        ltp_app_name, ltp_map, missing_ltp, ltp_api_calls = fetch_ltp_map(
            preferred_app=args.app,
            symbols=stock_intraday,
            batch_size=max(1, int(args.ltp_batch_size)),
        )
        min_price = float(args.min_price)
        stock_intraday_price_filtered = sorted(
            symbol for symbol in stock_intraday if float(ltp_map.get(symbol, 0.0)) >= min_price
        )
        price_filtered_path = build_price_filtered_path(min_price)
        write_universe_module(
            price_filtered_path,
            generated_at,
            [
                f"Source universe: {STOCK_INTRADAY_5X_FILE.name}",
                f"Kite app used: {ltp_app_name}",
                "Probe: live NSE LTP via Kite ltp()",
                f"Rule: price >= Rs {min_price:g}",
                f"Count: {len(stock_intraday_price_filtered)}",
                f"Missing LTP: {len(missing_ltp)}",
                f"API calls: {ltp_api_calls}",
            ],
            stock_intraday_price_filtered,
        )
        print(f"[OK] Wrote {price_filtered_path.name} with {len(stock_intraday_price_filtered)} symbols.")
        if missing_ltp:
            print(f"[WARN] Missing LTP sample: {', '.join(missing_ltp[:20])}")
        raw_suffixes = [part.strip().upper() for part in str(args.exclude_series_suffixes).split(",")]
        excluded_suffixes = tuple(part for part in raw_suffixes if part)
        if (
            float(args.min_live_volume) > 0
            or float(args.min_live_traded_value_rs) > 0
            or excluded_suffixes
        ):
            quote_app_name, quote_metrics, missing_quote, quote_api_calls = fetch_quote_metrics(
                preferred_app=args.app,
                symbols=stock_intraday_price_filtered,
                batch_size=max(1, int(args.quote_batch_size)),
            )
            min_live_volume = float(args.min_live_volume)
            min_live_traded_value_rs = float(args.min_live_traded_value_rs)
            liquid_symbols = []
            for symbol in stock_intraday_price_filtered:
                if excluded_suffixes and symbol.endswith(excluded_suffixes):
                    continue
                row = quote_metrics.get(symbol, {})
                volume = float(row.get("volume", 0.0))
                traded_value = float(row.get("traded_value", 0.0))
                if min_live_volume > 0 and volume < min_live_volume:
                    continue
                if min_live_traded_value_rs > 0 and traded_value < min_live_traded_value_rs:
                    continue
                liquid_symbols.append(symbol)
            liquid_path = build_liquid_filtered_path(min_price)
            write_universe_module(
                liquid_path,
                generated_at,
                [
                    f"Source universe: {price_filtered_path.name}",
                    f"Kite app used: {quote_app_name}",
                    "Probe: live NSE quote() metrics via Kite",
                    f"Rule: volume >= {min_live_volume:g}" if min_live_volume > 0 else "Rule: no minimum live volume",
                    (
                        f"Rule: traded_value >= Rs {min_live_traded_value_rs:g}"
                        if min_live_traded_value_rs > 0
                        else "Rule: no minimum live traded value"
                    ),
                    (
                        f"Rule: exclude suffixes {','.join(excluded_suffixes)}"
                        if excluded_suffixes
                        else "Rule: no excluded series suffixes"
                    ),
                    f"Count: {len(liquid_symbols)}",
                    f"Missing quote rows: {len(missing_quote)}",
                    f"API calls: {quote_api_calls}",
                ],
                liquid_symbols,
            )
            print(f"[OK] Wrote {liquid_path.name} with {len(liquid_symbols)} symbols.")
            write_universe_module(
                MIS_V2_FILE,
                generated_at,
                [
                    f"Source universe: {price_filtered_path.name}",
                    f"Kite app used: {quote_app_name}",
                    "Probe: live NSE quote() metrics via Kite",
                    (
                        f"Rule: volume >= {min_live_volume:g}"
                        if min_live_volume > 0
                        else "Rule: no minimum live volume"
                    ),
                    (
                        f"Rule: traded_value >= Rs {min_live_traded_value_rs:g}"
                        if min_live_traded_value_rs > 0
                        else "Rule: no minimum live traded value"
                    ),
                    (
                        f"Rule: exclude suffixes {','.join(excluded_suffixes)}"
                        if excluded_suffixes
                        else "Rule: no excluded series suffixes"
                    ),
                    f"Alias file: {MIS_V2_FILE.name}",
                    f"Count: {len(liquid_symbols)}",
                ],
                liquid_symbols,
            )
            print(f"[OK] Wrote {MIS_V2_FILE.name} with {len(liquid_symbols)} symbols.")
            if missing_quote:
                print(f"[WARN] Missing quote sample: {', '.join(missing_quote[:20])}")
    if ineligible:
        sample = ", ".join(f"{symbol}:{leverage:g}" for symbol, leverage in ineligible[:20])
        print(f"[WARN] Ineligible sample: {sample}")
    else:
        print("[OK] All scanned symbols met the leverage threshold.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
