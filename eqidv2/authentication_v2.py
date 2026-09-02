# -*- coding: utf-8 -*-
"""
authentication_v2.py

Design goals:
1) Single auth pass per IST day (typically scheduler at ~08:55).
2) Generate/reuse request/access tokens for all configured apps (1..8).
3) No intraday 15-minute renewal loop in this script.

Files used:
- api_key.txt
- api_key2.txt (optional second Kite app)
- api_key3.txt (optional third Kite app)
- api_key4.txt (optional fourth Kite app)
- api_key5.txt (optional fifth Kite app)
- api_key6.txt (optional sixth Kite app)
- api_key7.txt (optional seventh Kite app)
- api_key8.txt (optional eighth Kite app)
- request_token.txt
- access_token.txt
- request_token2.txt
- access_token2.txt
- request_token3.txt
- access_token3.txt
- request_token4.txt
- access_token4.txt
- request_token5.txt
- access_token5.txt
- request_token6.txt
- access_token6.txt
- request_token7.txt
- access_token7.txt
- request_token8.txt
- access_token8.txt
- refresh_token.txt
- auth_v2_state.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import pytz
from kiteconnect import KiteConnect
from pyotp import TOTP
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    InvalidSessionIdException,
    JavascriptException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

ROOT = Path(__file__).resolve().parent
IST = pytz.timezone("Asia/Kolkata")
OTP_RE = re.compile(r"\b(\d{6})\b")

API_KEY_FILE = ROOT / "api_key.txt"
API_KEY2_FILE = ROOT / "api_key2.txt"
API_KEY3_FILE = ROOT / "api_key3.txt"
API_KEY4_FILE = ROOT / "api_key4.txt"
API_KEY5_FILE = ROOT / "api_key5.txt"
API_KEY6_FILE = ROOT / "api_key6.txt"
API_KEY7_FILE = ROOT / "api_key7.txt"
API_KEY8_FILE = ROOT / "api_key8.txt"
REQUEST_TOKEN_FILE = ROOT / "request_token.txt"
ACCESS_TOKEN_FILE = ROOT / "access_token.txt"
REQUEST_TOKEN2_FILE = ROOT / "request_token2.txt"
ACCESS_TOKEN2_FILE = ROOT / "access_token2.txt"
REQUEST_TOKEN3_FILE = ROOT / "request_token3.txt"
ACCESS_TOKEN3_FILE = ROOT / "access_token3.txt"
REQUEST_TOKEN4_FILE = ROOT / "request_token4.txt"
ACCESS_TOKEN4_FILE = ROOT / "access_token4.txt"
REQUEST_TOKEN5_FILE = ROOT / "request_token5.txt"
ACCESS_TOKEN5_FILE = ROOT / "access_token5.txt"
REQUEST_TOKEN6_FILE = ROOT / "request_token6.txt"
ACCESS_TOKEN6_FILE = ROOT / "access_token6.txt"
REQUEST_TOKEN7_FILE = ROOT / "request_token7.txt"
ACCESS_TOKEN7_FILE = ROOT / "access_token7.txt"
REQUEST_TOKEN8_FILE = ROOT / "request_token8.txt"
ACCESS_TOKEN8_FILE = ROOT / "access_token8.txt"
REFRESH_TOKEN_FILE = ROOT / "refresh_token.txt"
STATE_FILE = ROOT / "auth_v2_state.json"

SECONDARY_API_KEY_FILES = {
    2: API_KEY2_FILE,
    3: API_KEY3_FILE,
    4: API_KEY4_FILE,
    5: API_KEY5_FILE,
    6: API_KEY6_FILE,
    7: API_KEY7_FILE,
    8: API_KEY8_FILE,
}
SECONDARY_REQUEST_TOKEN_FILES = {
    2: REQUEST_TOKEN2_FILE,
    3: REQUEST_TOKEN3_FILE,
    4: REQUEST_TOKEN4_FILE,
    5: REQUEST_TOKEN5_FILE,
    6: REQUEST_TOKEN6_FILE,
    7: REQUEST_TOKEN7_FILE,
    8: REQUEST_TOKEN8_FILE,
}
SECONDARY_ACCESS_TOKEN_FILES = {
    2: ACCESS_TOKEN2_FILE,
    3: ACCESS_TOKEN3_FILE,
    4: ACCESS_TOKEN4_FILE,
    5: ACCESS_TOKEN5_FILE,
    6: ACCESS_TOKEN6_FILE,
    7: ACCESS_TOKEN7_FILE,
    8: ACCESS_TOKEN8_FILE,
}

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
SLOT_GRACE_SEC = 75

# A TOTP generated near the end of its 30-second window can expire while
# Selenium is still locating the field and typing the digits.  Kite then
# rejects the code, the page never redirects, and the login fails with
# "timed out before request_token was available".  When fewer than this many
# seconds remain, wait for the next window instead of using a doomed code.
TOTP_MIN_REMAINING_SEC = 10

# A secondary-app login that fails for a transient reason (expired TOTP
# window, a slow page, a dropped connection) almost always succeeds on a
# second attempt.  Without a retry a single failure silently costs one app
# for the whole trading day.
SECONDARY_LOGIN_ATTEMPTS = 2
SECONDARY_RETRY_DELAY_SEC = 5.0


def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


def _extract_otp(text: str) -> Optional[str]:
    if not text:
        return None
    m = OTP_RE.search(text)
    return m.group(1) if m else None


def _read_key_secret(path: Path = API_KEY_FILE) -> list[str]:
    parts = path.read_text(encoding="utf-8").split()
    if len(parts) < 5:
        raise RuntimeError(f"{path.name} must contain: api_key api_secret user_id password totp_secret")
    return parts


def _read_secondary_key_secret_with_fallback(primary_parts: list[str], app_idx: int) -> Optional[list[str]]:
    api_key_path = SECONDARY_API_KEY_FILES.get(app_idx)
    if api_key_path is None:
        raise RuntimeError(f"Unsupported secondary app index: {app_idx}")
    if not api_key_path.exists():
        return None

    raw = api_key_path.read_text(encoding="utf-8").split()
    if len(raw) >= 5:
        return raw[:5]
    if len(raw) >= 2:
        if len(primary_parts) < 5:
            raise RuntimeError(
                f"{api_key_path.name} has only api_key/api_secret, but primary credentials are incomplete."
            )
        # Allow compact api_keyN.txt containing only key+secret and reuse primary login creds.
        return [raw[0], raw[1], primary_parts[2], primary_parts[3], primary_parts[4]]

    raise RuntimeError(
        f"{api_key_path.name} must contain either: "
        "api_key api_secret OR api_key api_secret user_id password totp_secret"
    )


def _extract_request_token(url: str) -> Optional[str]:
    if not url:
        return None
    q = parse_qs(urlparse(url).query)
    tok = (q.get("request_token", [""])[0] or "").strip()
    return tok or None


def _resolve_totp(secret: str) -> Tuple[str, str]:
    cmd = os.environ.get("KITE_TOTP_COMMAND", "").strip()
    if cmd:
        try:
            out = subprocess.check_output(cmd, shell=True, text=True, timeout=12).strip()
            otp = _extract_otp(out)
            if otp:
                return otp, f"command:{cmd}"
        except Exception:
            pass

    fp = os.environ.get("KITE_TOTP_FILE", "").strip()
    if fp:
        p = Path(fp)
        deadline = time.time() + 10
        while time.time() < deadline:
            if p.exists():
                otp = _extract_otp(p.read_text(encoding="utf-8"))
                if otp:
                    return otp, f"file:{p}"
            time.sleep(0.5)

    try:
        totp = TOTP(secret)
        interval = int(getattr(totp, "interval", 30) or 30)
        remaining = interval - (int(time.time()) % interval)
        if remaining < TOTP_MIN_REMAINING_SEC:
            # Using this code would race the window rollover; wait it out so
            # the browser gets a code with a full interval ahead of it.
            print(
                f"[AUTH] TOTP window has {remaining}s left "
                f"(< {TOTP_MIN_REMAINING_SEC}s); waiting for the next code.",
                flush=True,
            )
            time.sleep(remaining + 0.5)
        otp = totp.now()
        otp = _extract_otp(otp)
        if otp:
            return otp, "pyotp_secret"
    except Exception:
        pass

    try:
        out = subprocess.check_output(
            'powershell -NoProfile -Command "Get-Clipboard"',
            shell=True,
            text=True,
            timeout=5,
        ).strip()
        otp = _extract_otp(out)
        if otp:
            return otp, "clipboard"
    except Exception:
        pass

    raise RuntimeError("Could not resolve a valid TOTP from command, file, pyotp_secret, or clipboard.")


def _find_first(
    wait: WebDriverWait,
    locators: Iterable[Tuple[str, str]],
    condition,
):
    predicates = [condition((by, val)) for by, val in locators]
    if not predicates:
        raise TimeoutException("No locator matched.")

    # WebDriverWait owns the single deadline.  Each poll checks every fallback
    # locator instead of starting a fresh full-length wait for each one.
    ignored_exceptions = tuple(getattr(wait, "_ignored_exceptions", ()))

    def first_match(driver):
        for predicate in predicates:
            try:
                value = predicate(driver)
            except Exception as exc:
                if isinstance(exc, ignored_exceptions):
                    continue
                raise
            if value:
                return value
        return False

    return wait.until(first_match)


def _type_with_retry(
    wait: WebDriverWait,
    locators: Iterable[Tuple[str, str]],
    text: str,
    retries: int = 5,
) -> None:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            element = _find_first(wait, locators, EC.visibility_of_element_located)
            element.clear()
            element.send_keys(text)
            return
        except (StaleElementReferenceException, TimeoutException, AttributeError) as exc:
            last_error = exc
            time.sleep(1)
            print(f"Retrying type action ({attempt}/{retries})", flush=True)
    raise last_error


def _safe_click(driver: webdriver.Chrome, element) -> None:
    if element is None:
        raise TimeoutException("Element is None")
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    except JavascriptException:
        pass
    try:
        element.click()
        return
    except Exception:
        pass
    driver.execute_script("arguments[0].click();", element)


def _click_with_retry(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    locators: Iterable[Tuple[str, str]],
    retries: int = 5,
) -> None:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            element = _find_first(wait, locators, EC.element_to_be_clickable)
            _safe_click(driver, element)
            return
        except (
            StaleElementReferenceException,
            ElementClickInterceptedException,
            TimeoutException,
            JavascriptException,
            AttributeError,
        ) as exc:
            last_error = exc
            time.sleep(1)
            print(f"Retrying click action ({attempt}/{retries})", flush=True)

    element = _find_first(wait, locators, EC.presence_of_element_located)
    if element is None:
        raise last_error or TimeoutException("Could not find element for click fallback.")
    driver.execute_script("arguments[0].click();", element)


def _fill_totp(wait: WebDriverWait, otp: str) -> None:
    single_locators = [
        (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[1]/input"),
        (By.ID, "totp"),
        (By.NAME, "totp"),
        (By.CSS_SELECTOR, "input[autocomplete='one-time-code']"),
        (By.XPATH, "//form//input[@type='text' or @type='tel' or @type='number']"),
    ]
    try:
        _type_with_retry(wait, single_locators, otp, retries=3)
        return
    except Exception:
        pass

    driver = wait._driver
    try:
        elems = driver.find_elements(By.XPATH, "//input[not(@disabled)]")
    except InvalidSessionIdException as exc:
        raise TimeoutException("Browser session closed before TOTP field could be located.") from exc

    boxes = []
    for e in elems:
        try:
            if not e.is_displayed():
                continue
            mx = (e.get_attribute("maxlength") or "").strip()
            typ = (e.get_attribute("type") or "").lower()
            if mx == "1" and typ in ("text", "tel", "number", ""):
                boxes.append(e)
        except Exception:
            continue

    if len(boxes) >= 6:
        for i, ch in enumerate(otp[:6]):
            boxes[i].clear()
            boxes[i].send_keys(ch)
        return

    raise TimeoutException("Could not locate TOTP input field(s).")


def _write_text(path: Path, value: str) -> None:
    path.write_text(value.strip() + "\n", encoding="utf-8")


def _read_first_line(path: Path) -> str:
    if not path.exists():
        return ""
    txt = path.read_text(encoding="utf-8", errors="replace").strip()
    if not txt:
        return ""
    return txt.splitlines()[0].strip()


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def _update_state(refresh_token: str, request_token: Optional[str] = None, slot: Optional[str] = None) -> None:
    st = _load_state()
    st["session_date_ist"] = str(today_ist())
    st["refresh_token"] = refresh_token
    st["updated_at_ist"] = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
    if request_token:
        st["request_token"] = request_token
    if slot:
        st["last_slot"] = slot
    _save_state(st)


def _access_token_is_valid(kite: KiteConnect) -> bool:
    try:
        kite.profile()
        return True
    except Exception:
        return False


def _redact_url_for_log(url: str) -> str:
    """Keep a useful URL location without leaking auth query values."""
    raw = str(url or "").strip()
    if not raw:
        return "unavailable"
    try:
        parsed = urlparse(raw)
        base = parsed._replace(query="", fragment="").geturl()
        return f"{base}?<redacted>" if parsed.query else base
    except Exception:
        return "unavailable"


def _wait_for_request_token_in_url(
    driver: webdriver.Chrome,
    timeout_seconds: float,
    poll_seconds: float = 0.5,
) -> Optional[str]:
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    while True:
        request_token = _extract_request_token(driver.current_url)
        if request_token:
            return request_token

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        time.sleep(min(max(0.0, float(poll_seconds)), remaining))


def _request_token_after_totp(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    submit_btn_locators: Iterable[Tuple[str, str]],
) -> str:
    # Kite's TOTP form can auto-submit.  Give that redirect a brief chance to
    # complete before looking for a submit button that may already be gone.
    request_token = _wait_for_request_token_in_url(
        driver,
        timeout_seconds=2.0,
        poll_seconds=0.1,
    )
    if request_token:
        return request_token

    try:
        _click_with_retry(driver, wait, submit_btn_locators, retries=3)
    except Exception:
        # Preserve the existing optional-click behavior: the redirect may
        # still complete even when no explicit submit control is available.
        pass

    request_token = _wait_for_request_token_in_url(
        driver,
        timeout_seconds=45.0,
        poll_seconds=0.5,
    )
    if not request_token:
        raise TimeoutException("request_token not found in URL")
    return request_token


def _do_browser_login_for_request_token(kite: KiteConnect, user_id: str, password: str, totp_secret: str) -> str:
    service = Service(ChromeDriverManager().install())
    options = Options()
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(kite.login_url())
        wait = WebDriverWait(driver, 15)

        username_locators = [
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input"),
            (By.ID, "userid"),
            (By.NAME, "user_id"),
            (By.CSS_SELECTOR, "input#userid"),
            (By.XPATH, "//form//input[@type='text']"),
        ]
        password_locators = [
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input"),
            (By.ID, "password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input#password"),
            (By.XPATH, "//form//input[@type='password']"),
        ]
        login_btn_locators = [
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.XPATH, "//button[@type='submit']"),
        ]
        submit_btn_locators = [
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[2]/button"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.XPATH, "//button[@type='submit']"),
        ]

        _type_with_retry(wait, username_locators, user_id)
        _type_with_retry(wait, password_locators, password)
        _click_with_retry(driver, wait, login_btn_locators)

        otp, source = _resolve_totp(totp_secret)
        print(f"[AUTH] TOTP source: {source}", flush=True)
        _fill_totp(wait, otp)

        return _request_token_after_totp(driver, wait, submit_btn_locators)
    except TimeoutException as exc:
        raise RuntimeError(
            "Login flow timed out before request_token was available. "
            f"Last URL: {_redact_url_for_log(driver.current_url)}"
        ) from exc
    finally:
        driver.quit()


def _seed_session_for_today(parts: list[str], force_login: bool) -> Tuple[KiteConnect, str]:
    api_key, api_secret, user_id, password, totp_secret = parts[:5]
    kite = KiteConnect(api_key=api_key)
    today = str(today_ist())
    state = _load_state()

    if not force_login and state.get("session_date_ist") == today:
        refresh_token = str(state.get("refresh_token", "")).strip()
        if refresh_token:
            try:
                print(f"[AUTH] Reusing stored refresh_token for {today}.", flush=True)
                refresh_token = _renew_access_token_fast(kite, api_secret, refresh_token, reason="startup")
                return kite, refresh_token
            except Exception as e:
                print(f"[WARN] Startup refresh failed ({e}). Falling back to browser login.", flush=True)

        existing_access = _read_first_line(ACCESS_TOKEN_FILE)
        if existing_access:
            kite.set_access_token(existing_access)
            if _access_token_is_valid(kite):
                print(
                    f"[AUTH] Reusing same-day access_token for {today} (refresh_token unavailable).",
                    flush=True,
                )
                return kite, ""
            print("[WARN] Same-day access_token is invalid. Browser login required.", flush=True)

    print(f"[AUTH] Fresh browser login required for {today}.", flush=True)
    request_token = _do_browser_login_for_request_token(kite, user_id, password, totp_secret)
    _write_text(REQUEST_TOKEN_FILE, request_token)
    print(f"[OK] request_token saved -> {REQUEST_TOKEN_FILE}", flush=True)

    sess = kite.generate_session(request_token, api_secret=api_secret)
    access_token = str(sess.get("access_token", "")).strip()
    refresh_token = str(sess.get("refresh_token", "")).strip()
    if not access_token:
        raise RuntimeError("generate_session() did not return access_token")

    kite.set_access_token(access_token)
    _write_text(ACCESS_TOKEN_FILE, access_token)
    print(f"[OK] access_token saved -> {ACCESS_TOKEN_FILE}", flush=True)

    if refresh_token:
        _write_text(REFRESH_TOKEN_FILE, refresh_token)
        _update_state(refresh_token=refresh_token, request_token=request_token, slot="startup-refresh")
        print(f"[OK] refresh_token saved -> {REFRESH_TOKEN_FILE}", flush=True)
    else:
        try:
            if REFRESH_TOKEN_FILE.exists():
                REFRESH_TOKEN_FILE.unlink()
        except Exception:
            pass
        _update_state(refresh_token="", request_token=request_token, slot="startup-no-refresh")
        print(
            "[WARN] generate_session() did not return refresh_token. "
            "Using access-only fallback mode.",
            flush=True,
        )

    return kite, refresh_token


def _seed_additional_session_for_today(primary_parts: list[str], force_login: bool, app_idx: int) -> None:
    req_file = SECONDARY_REQUEST_TOKEN_FILES.get(app_idx)
    acc_file = SECONDARY_ACCESS_TOKEN_FILES.get(app_idx)
    if req_file is None or acc_file is None:
        raise RuntimeError(f"Unsupported secondary app index: {app_idx}")

    tag = f"AUTH{app_idx}"
    try:
        parts2 = _read_secondary_key_secret_with_fallback(primary_parts, app_idx)
    except Exception as e:
        print(f"[WARN] [{tag}] Invalid api_key{app_idx} configuration: {e}. Skipping app{app_idx}.", flush=True)
        return
    if not parts2:
        print(f"[{tag}] api_key{app_idx}.txt not found; skipping app{app_idx} token generation.", flush=True)
        return

    api_key_n, api_secret_n, user_id_n, password_n, totp_secret_n = parts2[:5]
    kite_n = KiteConnect(api_key=api_key_n)
    today = str(today_ist())

    existing_access = _read_first_line(acc_file)
    if not force_login and existing_access:
        try:
            kite_n.set_access_token(existing_access)
            if _access_token_is_valid(kite_n):
                print(
                    f"[{tag}] Reusing same-day access_token{app_idx} for {today}.",
                    flush=True,
                )
                st = _load_state()
                st[f"session_date_ist_app{app_idx}"] = today
                st[f"updated_at_ist_app{app_idx}"] = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
                _save_state(st)
                return
        except Exception:
            pass

    print(f"[{tag}] Fresh browser login required for app{app_idx} ({today}).", flush=True)
    request_token_n = _do_browser_login_for_request_token(kite_n, user_id_n, password_n, totp_secret_n)
    _write_text(req_file, request_token_n)
    print(f"[OK] request_token{app_idx} saved -> {req_file}", flush=True)

    sess_n = kite_n.generate_session(request_token_n, api_secret=api_secret_n)
    access_token_n = str(sess_n.get("access_token", "")).strip()
    if not access_token_n:
        raise RuntimeError(f"generate_session() for app{app_idx} did not return access_token")

    _write_text(acc_file, access_token_n)
    print(f"[OK] access_token{app_idx} saved -> {acc_file}", flush=True)

    st = _load_state()
    st[f"session_date_ist_app{app_idx}"] = today
    st[f"request_token{app_idx}"] = request_token_n
    st[f"updated_at_ist_app{app_idx}"] = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
    _save_state(st)


def _renew_access_token_fast(kite: KiteConnect, api_secret: str, refresh_token: str, reason: str) -> str:
    if not refresh_token:
        raise RuntimeError("refresh_token is empty; fast renewal is unavailable")
    t0 = time.time()
    resp = kite.renew_access_token(refresh_token, api_secret=api_secret)
    access_token = str(resp.get("access_token", "")).strip()
    next_refresh = str(resp.get("refresh_token", "")).strip() or refresh_token
    if not access_token:
        raise RuntimeError("renew_access_token() did not return access_token")

    kite.set_access_token(access_token)
    _write_text(ACCESS_TOKEN_FILE, access_token)
    _write_text(REFRESH_TOKEN_FILE, next_refresh)
    _update_state(refresh_token=next_refresh, slot=reason)

    dt = time.time() - t0
    print(
        f"[RENEW] reason={reason} done in {dt:.2f}s at {now_ist().strftime('%Y-%m-%d %H:%M:%S%z')}",
        flush=True,
    )
    return next_refresh


def run_slot_scheduler(parts: list[str], force_login: bool, test_now: bool, max_refresh: int) -> None:
    # Backward-compatible wrapper name retained intentionally.
    # This script now runs only one auth pass and exits.
    if test_now:
        print("[INFO] --test-now is legacy; single-pass mode is already active.", flush=True)
    if max_refresh:
        print("[INFO] --max-refresh is ignored in single-pass mode.", flush=True)

    _seed_session_for_today(parts, force_login=force_login)
    print("[DONE] Single-pass auth completed for primary app.", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--force-login", action="store_true", help="Force browser login even if same-day state exists.")
    p.add_argument(
        "--test-now",
        action="store_true",
        help="Legacy flag (ignored): script already runs once and exits.",
    )
    p.add_argument(
        "--max-refresh",
        type=int,
        default=0,
        help="Legacy flag (ignored): intraday slot renewals are disabled.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    parts = _read_key_secret()

    # The primary app feeds the time-critical live jobs, so authenticate it
    # before spending time on any optional secondary app.  If it fails, retain
    # the failure, still attempt every secondary app for a complete auth pass,
    # and then re-raise so the scheduled task keeps its existing non-zero exit
    # semantics for a primary failure.
    primary_error: Optional[Exception] = None
    # The primary app feeds the time-critical live jobs, so it deserves at
    # least the same transient-failure tolerance as every secondary app.
    for attempt in range(1, SECONDARY_LOGIN_ATTEMPTS + 1):
        try:
            run_slot_scheduler(
                parts=parts,
                force_login=bool(args.force_login),
                test_now=bool(args.test_now),
                max_refresh=int(args.max_refresh),
            )
            primary_error = None
            break
        except Exception as e:
            primary_error = e
            if attempt < SECONDARY_LOGIN_ATTEMPTS:
                print(
                    f"[WARN] [AUTH1] Primary attempt {attempt}/"
                    f"{SECONDARY_LOGIN_ATTEMPTS} failed: {e}. "
                    f"Retrying in {SECONDARY_RETRY_DELAY_SEC:.0f}s.",
                    flush=True,
                )
                time.sleep(SECONDARY_RETRY_DELAY_SEC)
    if primary_error is not None:
        print(
            f"[ERROR] [AUTH1] Primary app token generation failed after "
            f"{SECONDARY_LOGIN_ATTEMPTS} attempts: {primary_error}. "
            "Continuing with secondary apps before exiting.",
            flush=True,
        )

    failed_apps: list[int] = []
    for app_idx in (2, 3, 4, 5, 6, 7, 8):
        last_error: Optional[Exception] = None
        for attempt in range(1, SECONDARY_LOGIN_ATTEMPTS + 1):
            try:
                _seed_additional_session_for_today(
                    primary_parts=parts,
                    force_login=bool(args.force_login),
                    app_idx=app_idx,
                )
                last_error = None
                break
            except Exception as e:
                last_error = e
                if attempt < SECONDARY_LOGIN_ATTEMPTS:
                    print(
                        f"[WARN] [AUTH{app_idx}] App{app_idx} attempt {attempt}/"
                        f"{SECONDARY_LOGIN_ATTEMPTS} failed: {e}. "
                        f"Retrying in {SECONDARY_RETRY_DELAY_SEC:.0f}s.",
                        flush=True,
                    )
                    time.sleep(SECONDARY_RETRY_DELAY_SEC)
        if last_error is not None:
            failed_apps.append(app_idx)
            print(
                f"[WARN] [AUTH{app_idx}] App{app_idx} token generation failed after "
                f"{SECONDARY_LOGIN_ATTEMPTS} attempts: {last_error}. "
                "Continuing with remaining apps.",
                flush=True,
            )

    # A single silent secondary failure costs one app for the whole trading
    # day, and the downstream shared papertrade session needs 7 of 8 healthy.
    # Always end with an explicit, greppable verdict.
    healthy = 8 - len(failed_apps) - (1 if primary_error is not None else 0)
    if failed_apps or primary_error is not None:
        detail = ", ".join(f"app{i}" for i in failed_apps) or "none"
        print(
            f"[ERROR] [AUTH-SUMMARY] healthy={healthy}/8 "
            f"primary_ok={primary_error is None} failed_secondary=[{detail}]",
            flush=True,
        )
    else:
        print("[AUTH-SUMMARY] healthy=8/8 all apps authenticated.", flush=True)

    if primary_error is not None:
        raise primary_error


if __name__ == "__main__":
    main()
