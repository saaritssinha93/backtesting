from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

from kiteconnect import KiteConnect


ROOT = Path(__file__).resolve().parent

APP_SPECS: List[Tuple[str, str, str]] = [
    ("app1", "api_key.txt", "access_token.txt"),
    ("app2", "api_key2.txt", "access_token2.txt"),
    ("app3", "api_key3.txt", "access_token3.txt"),
    ("app4", "api_key4.txt", "access_token4.txt"),
    ("app5", "api_key5.txt", "access_token5.txt"),
    ("app6", "api_key6.txt", "access_token6.txt"),
    ("app7", "api_key7.txt", "access_token7.txt"),
    ("app8", "api_key8.txt", "access_token8.txt"),
]


def _get_public_ip() -> str:
    try:
        req = Request("https://api.ipify.org?format=json", headers={"User-Agent": "kite-testing/1.0"})
        with urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return str(payload.get("ip", "")).strip() or "unknown"
    except Exception as exc:
        return f"unavailable ({exc})"


def _load_kite_session(app_name: str, key_file: str, token_file: str) -> Optional[Tuple[str, KiteConnect, Dict[str, Any]]]:
    key_path = ROOT / key_file
    token_path = ROOT / token_file
    if not (key_path.exists() and token_path.exists()):
        return None

    key_parts = key_path.read_text(encoding="utf-8").split()
    if len(key_parts) < 2:
        raise RuntimeError(f"{key_path.name} must contain at least: api_key api_secret")

    access_token = token_path.read_text(encoding="utf-8").strip()
    if not access_token:
        raise RuntimeError(f"{token_path.name} is empty")

    client = KiteConnect(api_key=key_parts[0])
    client.set_access_token(access_token)
    profile = client.profile()
    return app_name, client, profile


def load_all_sessions() -> List[Tuple[str, KiteConnect, Dict[str, Any]]]:
    sessions: List[Tuple[str, KiteConnect, Dict[str, Any]]] = []
    for app_name, key_file, token_file in APP_SPECS:
        try:
            session = _load_kite_session(app_name, key_file, token_file)
            if session is not None:
                sessions.append(session)
        except Exception as exc:
            print(f"[WARN] {app_name} unavailable: {exc}")
    if not sessions:
        raise RuntimeError("No valid Kite sessions found. Check api_key*.txt and access_token*.txt files.")
    return sessions


def _ip_restriction_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "not allowed to place orders for this app" in text or "update allowed ips" in text


def _choose_sessions(
    sessions: List[Tuple[str, KiteConnect, Dict[str, Any]]],
    preferred_app: Optional[str],
) -> List[Tuple[str, KiteConnect, Dict[str, Any]]]:
    if not preferred_app:
        return sessions
    preferred = preferred_app.strip().lower()
    matching = [item for item in sessions if item[0].lower() == preferred]
    if not matching:
        available = ", ".join(name for name, _, _ in sessions)
        raise RuntimeError(f"Requested app {preferred_app!r} not available. Available: {available}")
    return matching


def _build_order_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    order_type = args.order_type.upper()
    kwargs: Dict[str, Any] = {
        "variety": args.variety,
        "exchange": args.exchange,
        "tradingsymbol": args.symbol.upper(),
        "transaction_type": args.side.upper(),
        "quantity": int(args.quantity),
        "product": args.product.upper(),
        "order_type": order_type,
        "validity": args.validity.upper(),
        "tag": args.tag,
    }
    if args.price is not None:
        kwargs["price"] = float(args.price)
    if args.trigger_price is not None:
        kwargs["trigger_price"] = float(args.trigger_price)
    if args.market_protection is not None:
        kwargs["market_protection"] = int(args.market_protection)
    elif order_type in {"MARKET", "SL-M"}:
        # Kite currently requires market protection for API market/SL-M orders.
        # -1 lets the broker apply automatic protection.
        kwargs["market_protection"] = -1
    return kwargs


def _preview_order(client: KiteConnect, order_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    margin_payload = [{
        "exchange": order_kwargs["exchange"],
        "tradingsymbol": order_kwargs["tradingsymbol"],
        "transaction_type": order_kwargs["transaction_type"],
        "variety": order_kwargs["variety"],
        "product": order_kwargs["product"],
        "order_type": order_kwargs["order_type"],
        "quantity": order_kwargs["quantity"],
        "price": float(order_kwargs.get("price", 0.0) or 0.0),
        "trigger_price": float(order_kwargs.get("trigger_price", 0.0) or 0.0),
    }]
    return client.order_margins(margin_payload)


def _place_order_with_fallback(
    sessions: List[Tuple[str, KiteConnect, Dict[str, Any]]],
    order_kwargs: Dict[str, Any],
) -> Tuple[str, str]:
    last_error: Optional[Exception] = None
    for app_name, client, _profile in sessions:
        try:
            order_id = str(_place_order(client, order_kwargs))
            return app_name, order_id
        except Exception as exc:
            last_error = exc
            print(f"[WARN] {app_name} place_order failed: {exc}")
            if _ip_restriction_error(exc):
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("No session was available for place_order")


def _place_order(client: KiteConnect, order_kwargs: Dict[str, Any]) -> str:
    payload = dict(order_kwargs)
    market_protection = payload.pop("market_protection", None)

    if market_protection is None:
        return str(client.place_order(**payload))

    params = dict(payload)
    params["market_protection"] = int(market_protection)
    response = client._post(
        "order.place",
        url_args={"variety": params["variety"]},
        params=params,
    )
    return str(response["order_id"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Small Kite session tester. Dry-run by default; live order only with --confirm-live."
    )
    parser.add_argument("--app", default="", help="Optional fixed app name, for example app1 or app4.")
    parser.add_argument("--symbol", required=True, help="Trading symbol, for example SBIN.")
    parser.add_argument("--side", required=True, choices=["BUY", "SELL", "buy", "sell"])
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--product", default="MIS")
    parser.add_argument("--order-type", default="MARKET", choices=["MARKET", "LIMIT", "SL", "SL-M", "market", "limit", "sl", "sl-m"])
    parser.add_argument("--variety", default="regular")
    parser.add_argument("--validity", default="DAY")
    parser.add_argument("--price", type=float, default=None)
    parser.add_argument("--trigger-price", type=float, default=None)
    parser.add_argument(
        "--market-protection",
        type=int,
        default=None,
        help="Kite market protection. Use -1 for broker auto protection, 0 for none, or 1-100 for custom percent.",
    )
    parser.add_argument("--tag", default="kite_testing")
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Actually place the order. Without this flag the script only previews margins.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    sessions = load_all_sessions()
    chosen_sessions = _choose_sessions(sessions, args.app)
    order_kwargs = _build_order_kwargs(args)

    print(f"Current public IPv4: {_get_public_ip()}")
    print("Available sessions:")
    for app_name, _client, profile in chosen_sessions:
        user = profile.get("user_name", "N/A")
        user_id = profile.get("user_id", "N/A")
        print(f"  - {app_name}: user={user} user_id={user_id}")

    first_app, first_client, _first_profile = chosen_sessions[0]
    print("\nOrder payload:")
    print(json.dumps(order_kwargs, indent=2))

    print("\nMargin preview:")
    preview = _preview_order(first_client, order_kwargs)
    print(json.dumps(preview, indent=2, default=str))

    quote_key = f"{order_kwargs['exchange']}:{order_kwargs['tradingsymbol']}"
    try:
        ltp_data = first_client.ltp(quote_key)
        print("\nLTP snapshot:")
        print(json.dumps(ltp_data, indent=2, default=str))
    except Exception as exc:
        print(f"\n[WARN] LTP fetch failed for {quote_key}: {exc}")

    if not args.confirm_live:
        print("\nDry-run only. No live order was placed.")
        print("To place the order intentionally, rerun with --confirm-live.")
        return 0

    print("\nLive order requested. Sending place_order()...")
    app_name, order_id = _place_order_with_fallback(chosen_sessions, order_kwargs)
    print(f"[OK] Order placed via {app_name}. order_id={order_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
