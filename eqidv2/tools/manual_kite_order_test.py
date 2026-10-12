"""Operator-run, single LIMIT order tool. Preview is the default.

This module is separate from the physically read-only readiness validator.
Submission requires --submit, a unique intent ID and an interactive exact-text
confirmation. There is no broker retry, app fallback, cancellation or token refresh.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener
from uuid import uuid4
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.test_kite_order_readiness import NoRedirect, ProbeError, ReadOnlyKite, credential_value, error_code, number

IST = ZoneInfo("Asia/Kolkata")
ORDER_URL = "https://api.kite.trade/orders/regular"
ORDER_KEYS = frozenset({"exchange", "tradingsymbol", "transaction_type", "quantity", "product", "order_type", "validity", "price"})


def positive_price(value):
    try:
        price = Decimal(str(value))
        if not price.is_finite() or price <= 0 or price.quantize(Decimal("0.01")) != price:
            raise ValueError
    except (InvalidOperation, ValueError):
        raise argparse.ArgumentTypeError("price must be positive with at most two decimal places") from None
    return format(price, ".2f")


def validated_order(order):
    if (set(order) != ORDER_KEYS or order.get("exchange") != "NSE" or order.get("order_type") != "LIMIT"
            or order.get("validity") != "DAY" or order.get("product") not in {"MIS", "CNC"}
            or order.get("transaction_type") not in {"BUY", "SELL"}
            or type(order.get("quantity")) is not int or order["quantity"] <= 0
            or not isinstance(order.get("tradingsymbol"), str)
            or re.fullmatch(r"[A-Z0-9&_.-]{1,40}", order["tradingsymbol"]) is None):
        raise ProbeError("LIMIT_ORDER_CONTRACT_INVALID")
    return {**order, "price": positive_price(order["price"])}


class SingleOrderWriter:
    """One bounded request per instance; no GETs or alternate mutation paths."""
    def __init__(self, api_key, access_token, *, timeout=10, opener=None):
        self._authorization = f"token {api_key}:{access_token}"
        self._opener = opener or build_opener(NoRedirect())
        self._timeout = timeout
        self._attempted = False

    def request(self, method, path, *, payload):
        if method != "POST" or path != "/orders/regular":
            raise ProbeError("ENDPOINT_NOT_ALLOWED")
        if self._attempted:
            raise ProbeError("SUBMISSION_ALREADY_ATTEMPTED")
        tag = payload.get("tag")
        if not isinstance(tag, str) or re.fullmatch(r"[A-Za-z0-9]{1,20}", tag) is None:
            raise ProbeError("ORDER_TAG_INVALID")
        order = validated_order({key: value for key, value in payload.items() if key != "tag"})
        request = Request(ORDER_URL, data=urlencode({**order, "tag": tag}).encode("ascii"), method="POST",
            headers={"Authorization": self._authorization, "X-Kite-Version": "3",
                     "Content-Type": "application/x-www-form-urlencoded", "User-Agent": "eqidv2-manual-single-limit/1.0"})
        self._attempted = True
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                raw = response.read(32769)
            if len(raw) > 32768:
                return {"state": "UNKNOWN_ACCEPTANCE", "reason": "RESPONSE_TOO_LARGE"}
            value = json.loads(raw)
        except HTTPError as exc:
            try:
                value = json.loads(exc.read(32768))
            except (OSError, ValueError, UnicodeError):
                value = {}
            definitive = exc.code in {400, 401, 403, 404, 405, 422} and isinstance(value, dict) and value.get("status") == "error"
            return {"state": "BROKER_REQUEST_REJECTED" if definitive else "UNKNOWN_ACCEPTANCE",
                    "reason": error_code(value, exc.code)}
        except (URLError, TimeoutError, OSError, HTTPException, ProbeError):
            return {"state": "UNKNOWN_ACCEPTANCE", "reason": "TRANSPORT_RESULT_UNCERTAIN"}
        except (ValueError, UnicodeError):
            return {"state": "UNKNOWN_ACCEPTANCE", "reason": "INVALID_BROKER_RESPONSE"}
        if not isinstance(value, dict):
            return {"state": "UNKNOWN_ACCEPTANCE", "reason": "INVALID_BROKER_RESPONSE"}
        if value.get("status") == "error":
            return {"state": "BROKER_REQUEST_REJECTED", "reason": error_code(value)}
        data = value.get("data")
        order_id = data.get("order_id") if isinstance(data, dict) else None
        if value.get("status") != "success" or not isinstance(order_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", order_id) is None:
            return {"state": "UNKNOWN_ACCEPTANCE", "reason": "ORDER_ACKNOWLEDGEMENT_MISSING"}
        return {"state": "BROKER_ACKNOWLEDGED", "order_id": order_id,
                "reason": "Order ID received; exchange acceptance, status and fill require review in Kite."}


def preview_order(client, order):
    profile = client.request("GET", "/user/profile")
    if (not isinstance(profile, dict) or any(not isinstance(profile.get(key), list)
            or any(not isinstance(value, str) for value in profile[key]) for key in ("exchanges", "products", "order_types"))):
        raise ProbeError("INVALID_PROFILE_RESPONSE")
    if not ("NSE" in profile["exchanges"] and order["product"] in profile["products"] and "LIMIT" in profile["order_types"]):
        raise ProbeError("ACCOUNT_CAPABILITY_MISMATCH")
    funds = client.request("GET", "/user/margins/equity")
    if not isinstance(funds, dict) or funds.get("enabled") is not True or not isinstance(funds.get("available"), dict):
        raise ProbeError("EQUITY_FUNDS_UNVERIFIED")
    balance = number(funds["available"].get("live_balance"))
    proposed = {**order, "variety": "regular", "trigger_price": 0, "price": float(order["price"])}
    proposed.pop("validity")
    margins = client.request("POST", "/margins/orders", payload=[proposed])
    if not isinstance(margins, list) or len(margins) != 1 or not isinstance(margins[0], dict):
        raise ProbeError("INVALID_MARGIN_RESPONSE")
    required = number(margins[0].get("total"))
    if required is None or required < 0:
        raise ProbeError("INVALID_MARGIN_RESPONSE")
    return {"state": "PREVIEW_COMPLETE", "required_margin_rupees": required,
            "live_balance_covers_estimate": balance >= required if balance is not None else None,
            "order_acceptance": "NOT_TESTED", "static_ip_order_permission": "NOT_VERIFIED"}


def _atomic_json(path, value):
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    # Windows scanners can briefly hold the closed temporary file. Retry only
    # this local atomic rename (175 ms total), never the exclusive intent claim
    # or any broker request. A persistent failure still consumes the intent.
    delays = (0.025, 0.05, 0.1)
    for attempt in range(len(delays) + 1):
        try:
            os.replace(temporary, path)
            break
        except PermissionError:
            if attempt == len(delays):
                raise
            time.sleep(delays[attempt])


def intent_directory(root, intent_id):
    if not isinstance(intent_id, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{7,63}", intent_id) is None:
        raise ProbeError("INTENT_ID_INVALID")
    root = Path(root).resolve()
    path = (root / "logs/manual_kite_order_test/intents" / intent_id).resolve()
    if not path.is_relative_to(root):
        raise ProbeError("INTENT_PATH_OUTSIDE_WORKSPACE")
    return path


def claim_intent(root, intent_id, record):
    directory = intent_directory(root, intent_id)
    directory.parent.mkdir(parents=True, exist_ok=True)
    # Atomic exclusive claim. A crash before the final JSON still consumes this
    # ID, so a rerun cannot resubmit after an uncertain local/process failure.
    directory.mkdir(exist_ok=False)
    _atomic_json(directory / "intent.json", record)
    return directory


def confirmation_text(app, order, intent_id):
    return f"SUBMIT {app} {order['transaction_type']} {order['quantity']} {order['tradingsymbol']} NSE {order['product']} LIMIT {order['price']} DAY {intent_id}"


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", required=True, choices=[f"app{i}" for i in range(1, 9)])
    parser.add_argument("--symbol", required=True, type=str.upper)
    parser.add_argument("--side", required=True, type=str.upper, choices=["BUY", "SELL"])
    parser.add_argument("--quantity", required=True, type=int)
    parser.add_argument("--price", required=True, type=positive_price)
    parser.add_argument("--exchange", choices=["NSE"], default="NSE")
    parser.add_argument("--product", choices=["MIS", "CNC"], default="MIS")
    parser.add_argument("--order-type", choices=["LIMIT"], default="LIMIT")
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--intent-id", help="Unique 8-64 character alphanumeric/hyphen/underscore label; required for --submit.")
    return parser


def run(args, *, root=ROOT, reader_factory=ReadOnlyKite, writer_factory=SingleOrderWriter,
        confirm=None, interactive=None, emit=print):
    order = validated_order(dict(exchange=args.exchange, tradingsymbol=args.symbol,
        transaction_type=args.side, quantity=args.quantity, product=args.product,
        order_type=args.order_type, validity="DAY", price=args.price))
    base = dict(schema_version="manual_kite_single_limit_v1", app=args.app, order=order,
                created_at_ist=datetime.now(IST).isoformat(), strategy_controls_changed=False,
                automatic_retry=False, app_fallback=False, submission_attempts=0)
    if args.submit:
        directory = intent_directory(root, args.intent_id)
        if directory.exists():
            return {**base, "state": "INTENT_ALREADY_EXISTS", "reason": "Review this intent and Kite manually. This intent ID cannot be reused."}, 2
        if not (sys.stdin.isatty() if interactive is None else interactive):
            return {**base, "state": "INTERACTIVE_CONFIRMATION_REQUIRED"}, 2
    suffix = "" if args.app == "app1" else args.app[3:]
    try:
        key = credential_value(Path(root) / f"api_key{suffix}.txt", "API_KEY")
        token = credential_value(Path(root) / f"access_token{suffix}.txt", "ACCESS_TOKEN")
        reader = reader_factory(key, token)
        preview = preview_order(reader, order)
    except (OSError, UnicodeError, HTTPException, ProbeError):
        return {**base, "state": "PREVIEW_FAILED", "reason": "Credentials, read access or margin response could not be verified."}, 2
    base["preview"] = preview
    emit(json.dumps({"app": args.app, "order": order, "preview": preview}, indent=2))
    if not args.submit:
        return {**base, "state": "PREVIEW_ONLY", "order_acceptance": "NOT_TESTED"}, 0
    if preview["live_balance_covers_estimate"] is not True:
        return {**base, "state": "MARGIN_COVERAGE_NOT_CONFIRMED"}, 2
    text = confirmation_text(args.app, order, args.intent_id)
    emit("This submits one real LIMIT order. It does not place protective exits or change G3 controls.")
    emit("Type this exact line to confirm:\n" + text)
    try:
        answer = (confirm or input)("> ")
    except (EOFError, KeyboardInterrupt):
        return {**base, "state": "CONFIRMATION_CANCELLED"}, 2
    if answer != text:
        return {**base, "state": "CONFIRMATION_CANCELLED"}, 2
    tag = "MT" + uuid4().hex[:18]
    intent = {**base, "state": "INTENT_COMMITTED", "intent_id": args.intent_id, "tag": tag,
              "human_confirmation_received": True,
              "recovery": "Never retry this intent. Inspect Kite for this tag, symbol and quantity before any further action."}
    try:
        writer = writer_factory(key, token)
        directory = claim_intent(root, args.intent_id, intent)
    except FileExistsError:
        return {**base, "state": "INTENT_ALREADY_EXISTS"}, 2
    except (OSError, ProbeError):
        return {**base, "state": "INTENT_SAVE_FAILED_NO_SUBMISSION"}, 3
    # The intent is durable before the sole mutation attempt. No failure path
    # below may call request again, even when the server result is unknowable.
    try:
        outcome = writer.request("POST", "/orders/regular", payload={**order, "tag": tag})
    except KeyboardInterrupt:
        # Ctrl+C can interrupt the wait after the broker has accepted the
        # request. Preserve the same consumed intent and uncertain outcome;
        # cancelling the local wait never establishes that no order exists.
        outcome = {"state": "UNKNOWN_ACCEPTANCE", "reason": "Submission wait interrupted; review Kite manually."}
    except Exception:
        outcome = {"state": "UNKNOWN_ACCEPTANCE", "reason": "Unexpected submission result; review Kite manually."}
    result = {**intent, **outcome, "submission_attempts": 1,
              "completed_at_ist": datetime.now(IST).isoformat(),
              "review": "Review order status and any fills in Kite. Never rerun this intent or assume an error means no order exists."}
    try:
        _atomic_json(directory / "result.json", result)
    except (OSError, KeyboardInterrupt):
        try:
            emit("RESULT_SAVE_FAILED: a submission was attempted. Review Kite and the saved intent; do not retry.")
        except (OSError, KeyboardInterrupt):
            # A closed Windows console/pipe must not escape into main's
            # pre-submission setup-error handler and claim no order was sent.
            pass
        return {**result, "result_saved": False}, 3
    return {**result, "result_saved": True}, 0 if result["state"] == "BROKER_ACKNOWLEDGED" else 2


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.quantity <= 0 or re.fullmatch(r"[A-Z0-9&_.-]{1,40}", args.symbol) is None:
        parser.error("symbol must be an NSE trading symbol and quantity must be positive")
    if args.submit and (args.intent_id is None or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{7,63}", args.intent_id) is None):
        parser.error("--submit requires a unique --intent-id of 8-64 alphanumeric/hyphen/underscore characters")
    try:
        result, code = run(args)
    except (OSError, ProbeError, argparse.ArgumentTypeError):
        print("SETUP_FAILED: no submission was initiated by this invocation.")
        return 3
    print(json.dumps(result, indent=2, allow_nan=False))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
