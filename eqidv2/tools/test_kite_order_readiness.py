"""Validate existing Kite apps without ever submitting, modifying or cancelling orders.

Uses standard-library HTTPS and a closed endpoint allowlist. An optional proposed
order only calls the broker's margin calculator. No authentication refresh,
strategy engine, scheduler, arming control or execution-capable helper is loaded.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import datetime, time
from http.client import HTTPException
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
IST = ZoneInfo("Asia/Kolkata")
API_BASE = "https://api.kite.trade"
ALLOWED = frozenset({("GET", "/user/profile"), ("GET", "/user/margins/equity"),
                     ("GET", "/orders"), ("GET", "/portfolio/positions"),
                     ("GET", "/quote/ltp"), ("POST", "/margins/orders")})


class ProbeError(Exception):
    """Only a sanitized code is retained; broker messages may contain secrets."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ProbeError("REDIRECT_REFUSED")


def error_code(payload, status=0):
    kind = payload.get("error_type") if isinstance(payload, dict) else None
    if kind == "TokenException" or status == 401:
        return "SESSION_INVALID_OR_EXPIRED"
    if kind == "PermissionException" or status == 403:
        return "ACCESS_DENIED"
    if status == 429:
        return "RATE_LIMITED"
    if kind in {"InputException", "MarginException", "OrderException", "NetworkException", "DataException", "GeneralException"}:
        return kind.upper()
    return f"HTTP_ERROR_{int(status)}" if status else "INVALID_BROKER_RESPONSE"


class ReadOnlyKite:
    def __init__(self, api_key, access_token, *, timeout=10, opener=None):
        self._authorization = f"token {api_key}:{access_token}"
        self._opener = opener or build_opener(NoRedirect())
        self._timeout = timeout

    def request(self, method, path, *, payload=None, query=None):
        if (method, path) not in ALLOWED:
            raise ProbeError("ENDPOINT_NOT_ALLOWED")
        if (method == "GET" and payload is not None) or (query and path != "/quote/ltp"):
            raise ProbeError("REQUEST_NOT_ALLOWED")
        url = API_BASE + path + ("?" + urlencode(query) if query else "")
        data = json.dumps(payload, allow_nan=False).encode() if payload is not None else None
        request = Request(url, data=data, method=method, headers={
            "Authorization": self._authorization, "X-Kite-Version": "3",
            "Content-Type": "application/json", "User-Agent": "eqidv2-readiness-read-only/1.0"})
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                raw = response.read(8 * 1024 * 1024 + 1)
            if len(raw) > 8 * 1024 * 1024:
                raise ProbeError("RESPONSE_TOO_LARGE")
            value = json.loads(raw)
        except HTTPError as exc:
            try:
                value = json.loads(exc.read(32768))
            except (ValueError, OSError):
                value = {}
            raise ProbeError(error_code(value, exc.code)) from None
        except (URLError, TimeoutError, OSError, HTTPException):
            raise ProbeError("NETWORK_UNAVAILABLE") from None
        except (ValueError, UnicodeError):
            raise ProbeError("INVALID_BROKER_RESPONSE") from None
        if not isinstance(value, dict) or value.get("status") != "success" or "data" not in value:
            raise ProbeError(error_code(value))
        return value["data"]


def credential_value(path, field):
    # API key files may also contain secret/password/TOTP fields; only use the key.
    content = path.read_text(encoding="utf-8-sig")
    lines = [line.strip() for line in content.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    value = None
    for line in lines:
        if "=" in line and line.split("=", 1)[0].strip().upper() == field:
            value = line.split("=", 1)[1].strip()
            break
    if value is None and lines:
        value = lines[0].split()[0]
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]{1,1024}", value):
        raise ProbeError("CREDENTIAL_FORMAT_INVALID")
    return value


def market_schedule(now, root=ROOT):
    if now.tzinfo is None or now.utcoffset() is None:
        return dict(state="TIMEZONE_UNVERIFIED", scheduled_trading_day=None,
                    exchange_open_verified=False, schedule_only=True)
    now = now.astimezone(IST)
    result = dict(date=now.date().isoformat(), time_ist=now.isoformat(),
                  state="CALENDAR_UNVERIFIED", scheduled_trading_day=None,
                  exchange_open_verified=False, schedule_only=True)
    try:
        calendar = json.loads((root / f"configs/markets/nse_equity_calendar_{now.year}.json").read_text())
        if calendar["schema_version"] != "nse_equity_regular_session_calendar_v1" or calendar["year"] != now.year:
            return result
        result.update(calendar_checked_on=calendar["checked_on"], source=calendar["source"],
                      regular_open_ist=calendar["regular_open_ist"], regular_close_ist=calendar["regular_close_ist"])
        day = now.date().isoformat()
        if day in calendar.get("special_sessions_unverified", []):
            result["state"] = "SPECIAL_SESSION_HOURS_UNVERIFIED"
        elif now.weekday() >= 5 or day in calendar["holidays"]:
            result.update(state="CLOSED_BY_PUBLISHED_CALENDAR", scheduled_trading_day=False)
        else:
            clock = now.time().replace(tzinfo=None)
            start, end = map(time.fromisoformat, (calendar["regular_open_ist"], calendar["regular_close_ist"]))
            state = "BEFORE_REGULAR_SESSION" if clock < start else "AFTER_REGULAR_SESSION" if clock >= end else "WITHIN_SCHEDULED_REGULAR_SESSION"
            result.update(state=state, scheduled_trading_day=True)
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return result


def deployment_gate(root=ROOT):
    result = dict(state="DEPLOYMENT_RECORD_UNVERIFIED", live_activation_authorized=False,
                  runtime_arm_and_kill_gates_checked=False, scheduler_checked=False)
    try:
        value = json.loads((root / "configs/strategies/active_profile.json").read_text(encoding="utf-8-sig"))
        authorized = (value.get("profile") == "V13_V10_G_3" and value.get("deployment_stage") == "LIVE_APPROVED"
                      and value.get("live_activation_authorized") is True)
        result.update(profile=value.get("profile"), deployment_stage=value.get("deployment_stage"),
                      state="DEPLOYMENT_APPROVED_OTHER_GATES_UNVERIFIED" if authorized else "G3_LIVE_DEPLOYMENT_BLOCKED",
                      live_activation_authorized=authorized)
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return result


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def inspect_app(app, *, root=ROOT, proposed_order=None, client_factory=ReadOnlyKite, delay=.35, timeout=10):
    suffix = "" if app == "app1" else app[3:]
    key_path, token_path = root / f"api_key{suffix}.txt", root / f"access_token{suffix}.txt"
    record = dict(app=app, credential_files_present=key_path.is_file() and token_path.is_file(),
                  state="CREDENTIALS_MISSING", checks={}, order_acceptance="NOT_TESTED", orders_submitted=0)
    if not record["credential_files_present"]:
        return record
    try:
        client = client_factory(credential_value(key_path, "API_KEY"), credential_value(token_path, "ACCESS_TOKEN"), timeout=timeout)
    except (OSError, UnicodeError, ProbeError):
        record["state"] = "CREDENTIALS_UNREADABLE_OR_INVALID"
        return record
    endpoints = [("profile", "/user/profile"), ("margins", "/user/margins/equity"),
                 ("orders", "/orders"), ("positions", "/portfolio/positions")]
    responses = {}
    for name, path in endpoints:
        try:
            payload = client.request("GET", path)
            if ((name in {"profile", "margins", "positions"} and not isinstance(payload, dict))
                    or (name == "profile" and any(not isinstance(payload.get(key), list)
                        or any(not isinstance(value, str) for value in payload[key])
                        for key in ("exchanges", "products", "order_types")))
                    or (name == "margins" and (type(payload.get("enabled")) is not bool
                        or not isinstance(payload.get("available"), dict)))
                    or (name == "orders" and (not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload)))
                    or (name == "positions" and (not isinstance(payload.get("net"), list)
                        or any(not isinstance(row, dict) or number(row.get("quantity")) is None for row in payload["net"])))):
                raise ProbeError("INVALID_BROKER_RESPONSE")
            responses[name] = payload
            record["checks"][name] = "PASS"
        except ProbeError as exc:
            record["checks"][name] = str(exc)
            if name == "profile":
                record["state"] = str(exc)
                return record
        if delay:
            sleep(delay)
    profile = responses.get("profile", {})
    # Persist capability flags only, never profile names/IDs/email or raw ledgers.
    record["capabilities"] = dict(nse="NSE" in profile.get("exchanges", []), mis="MIS" in profile.get("products", []),
                                  cnc="CNC" in profile.get("products", []),
                                  limit="LIMIT" in profile.get("order_types", []), market="MARKET" in profile.get("order_types", []),
                                  sl="SL" in profile.get("order_types", []), sl_m="SL-M" in profile.get("order_types", []))
    margins = responses.get("margins", {})
    available = margins.get("available") or {}
    balance = number(available.get("live_balance")) if isinstance(available, dict) else None
    record["equity_segment_enabled"] = margins.get("enabled") is True
    record["funds_positive"] = balance > 0 if balance is not None else None
    if "orders" in responses:
        orders = responses["orders"]
        record["existing_orders_today"] = len(orders)
        record["nonterminal_orders"] = sum(row.get("status") not in {"COMPLETE", "CANCELLED", "REJECTED"} for row in orders)
    if "positions" in responses:
        record["open_net_positions"] = sum(number(row.get("quantity")) not in {None, 0} for row in responses["positions"]["net"])
    record["state"] = "READ_ACCESS_CONFIRMED" if all(record["checks"].get(key) == "PASS" for key, _ in endpoints) else "PARTIAL_READ_ACCESS"
    if proposed_order:
        supported = (proposed_order["exchange"] in profile.get("exchanges", [])
                     and proposed_order["product"] in profile.get("products", [])
                     and proposed_order["order_type"] in profile.get("order_types", []))
        record["order_preview"] = dict(state="ACCOUNT_CAPABILITY_MISMATCH", order_acceptance="NOT_TESTED")
        if supported:
            try:
                preview = client.request("POST", "/margins/orders", payload=[proposed_order])
                if not isinstance(preview, list) or len(preview) != 1 or not isinstance(preview[0], dict):
                    raise ProbeError("INVALID_MARGIN_RESPONSE")
                required = number(preview[0].get("total"))
                if required is None or required < 0:
                    raise ProbeError("INVALID_MARGIN_RESPONSE")
                record["order_preview"] = dict(state="MARGIN_ESTIMATE_RECEIVED", required_margin_rupees=required,
                    live_balance_covers_estimate=balance >= required if balance is not None else None,
                    order_acceptance="NOT_TESTED")
            except ProbeError as exc:
                record["order_preview"]["state"] = str(exc)
    return record


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", choices=["all", *[f"app{i}" for i in range(1, 9)]], default="all")
    parser.add_argument("--symbol", help="Optional proposed NSE equity symbol; calculates margins only.")
    parser.add_argument("--side", type=str.upper, choices=["BUY", "SELL"])
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--exchange", choices=["NSE"], default="NSE")
    parser.add_argument("--product", choices=["MIS", "CNC"], default="MIS")
    parser.add_argument("--order-type", choices=["MARKET", "LIMIT", "SL", "SL-M"], default="LIMIT")
    parser.add_argument("--price", type=float)
    parser.add_argument("--trigger-price", type=float)
    return parser


def proposed_order(args, parser):
    if args.quantity <= 0:
        parser.error("quantity must be positive")
    if not args.symbol:
        if args.side or args.price is not None or args.trigger_price is not None:
            parser.error("provide a symbol for the optional margin preview")
        return None
    if not args.side or not re.fullmatch(r"[A-Za-z0-9&_.-]{1,40}", args.symbol):
        parser.error("a valid symbol and --side BUY/SELL are required")
    for name in ("price", "trigger_price"):
        val = getattr(args, name)
        if val is not None and (not math.isfinite(val) or val <= 0):
            parser.error(f"{name} must be finite and positive")
    if args.order_type in {"LIMIT", "SL"} and args.price is None:
        parser.error("LIMIT and SL previews require --price")
    if args.order_type in {"SL", "SL-M"} and args.trigger_price is None:
        parser.error("SL and SL-M previews require --trigger-price")
    if args.order_type in {"MARKET", "SL-M"} and args.price is not None:
        parser.error("MARKET and SL-M previews do not take a limit price")
    if args.order_type in {"MARKET", "LIMIT"} and args.trigger_price is not None:
        parser.error("this order type does not take a trigger price")
    if args.order_type == "SL" and ((args.side == "BUY" and args.price < args.trigger_price) or (args.side == "SELL" and args.price > args.trigger_price)):
        parser.error("SL limit must be >= trigger for BUY and <= trigger for SELL")
    return dict(exchange=args.exchange, tradingsymbol=args.symbol.upper(), transaction_type=args.side,
                quantity=args.quantity, product=args.product, order_type=args.order_type,
                variety="regular", price=args.price or 0, trigger_price=args.trigger_price or 0)


def save_report(report, root=ROOT):
    output = root / "logs/kite_order_readiness"
    output.mkdir(parents=True, exist_ok=True)
    run = output / (datetime.now(IST).strftime("%Y%m%dT%H%M%S%f") + "_" + uuid4().hex[:8] + ".json")
    body = json.dumps(report, indent=2, allow_nan=False) + "\n"
    run.write_text(body, encoding="utf-8")
    temp = output / ("latest." + uuid4().hex + ".tmp")
    temp.write_text(body, encoding="utf-8")
    os.replace(temp, output / "latest.json")
    return run


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    order = proposed_order(args, parser)
    apps = [f"app{i}" for i in range(1, 9)] if args.app == "all" else [args.app]
    now = datetime.now(IST)
    results = []
    for app in apps:
        result = inspect_app(app, proposed_order=order)
        results.append(result)
        print(f"{app}: {result['state']}", flush=True)
    healthy = [row["app"] for row in results if row["state"] == "READ_ACCESS_CONFIRMED"]
    report = dict(schema_version="kite_order_readiness_validation_only_v1", checked_at_ist=now.isoformat(),
                  completed_at_ist=datetime.now(IST).isoformat(), mode="VALIDATE_ONLY", orders_submitted=0,
                  order_mutations_available=False, strategy_controls_changed=False,
                  state="READ_ACCESS_CONFIRMED" if healthy else "NO_FULL_READ_ACCESS",
                  readable_apps=healthy, apps=results, proposed_order=order,
                  market=market_schedule(now), g3_deployment=deployment_gate(),
                  order_acceptance="NOT_TESTED", static_ip_order_permission="NOT_VERIFIED",
                  limitations=["Read APIs and margin estimates cannot prove live order acceptance or static-IP order permission.",
                               "This file cannot submit, modify or cancel an order, refresh a token, or activate G3.",
                               "Margin preview is an estimate only." if order else "No symbol supplied; instrument-specific margin was not checked.",
                               "Published market schedule is not a real-time exchange-status check."])
    try:
        saved = save_report(report)
    except OSError:
        print("REPORT_WRITE_FAILED; no orders were submitted.")
        return 3
    print(f"Market schedule: {report['market']['state']}")
    print(f"G3 deployment: {report['g3_deployment']['state']}")
    print("Order acceptance: NOT_TESTED. Orders submitted: 0.")
    print(f"Report: {saved}")
    return 0 if healthy else 2


if __name__ == "__main__":
    raise SystemExit(main())
