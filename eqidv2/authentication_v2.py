# -*- coding: utf-8 -*-
"""
authentication_v2.py

Design goals for test rollout:
1) One browser login (request_token flow) per IST day.
2) Use refresh_token to renew access_token at exact 15-minute slots:
   09:15, 09:30, ... , 15:30 IST.
3) Renewal path should be fast (single API call, no browser interaction).

Files used:
- api_key.txt
- request_token.txt
- access_token.txt
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
REQUEST_TOKEN_FILE = ROOT / "request_token.txt"
ACCESS_TOKEN_FILE = ROOT / "access_token.txt"
REFRESH_TOKEN_FILE = ROOT / "refresh_token.txt"
STATE_FILE = ROOT / "auth_v2_state.json"

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
SLOT_GRACE_SEC = 75


def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


def _extract_otp(text: str) -> Optional[str]:
    if not text:
        return None
    m = OTP_RE.search(text)
    return m.group(1) if m else None


def _read_key_secret() -> list[str]:
    parts = API_KEY_FILE.read_text(encoding="utf-8").split()
    if len(parts) < 5:
        raise RuntimeError("api_key.txt must contain: api_key api_secret user_id password totp_secret")
    return parts


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

    return TOTP(secret).now(), "pyotp_secret"


def _find_first(
    wait: WebDriverWait,
    locators: Iterable[Tuple[str, str]],
    condition,
):
    last = None
    for by, val in locators:
        try:
            el = wait.until(condition((by, val)))
            if el is not None:
                return el
        except TimeoutException as exc:
            last = exc
    if last:
        raise last
    raise TimeoutException("No locator matched.")


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

        try:
            _click_with_retry(driver, wait, submit_btn_locators, retries=3)
        except Exception:
            pass

        deadline = time.time() + 45
        request_token = None
        while time.time() < deadline:
            request_token = _extract_request_token(driver.current_url)
            if request_token:
                break
            time.sleep(0.5)

        if not request_token:
            raise TimeoutException("request_token not found in URL")

        return request_token
    except TimeoutException as exc:
        raise RuntimeError(
            f"Login flow timed out before request_token was available. Last URL: {driver.current_url}"
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


def _slots_for_day(d: date) -> list[datetime]:
    cur = IST.localize(datetime.combine(d, MARKET_OPEN))
    end = IST.localize(datetime.combine(d, MARKET_CLOSE))
    out = []
    while cur <= end:
        out.append(cur)
        cur += timedelta(minutes=15)
    return out


def _sleep_until(target: datetime) -> None:
    while True:
        now = now_ist()
        sec = (target - now).total_seconds()
        if sec <= 0:
            return
        time.sleep(min(sec, 1.0))


def run_slot_scheduler(parts: list[str], force_login: bool, test_now: bool, max_refresh: int) -> None:
    api_secret = parts[1]
    kite, refresh_token = _seed_session_for_today(parts, force_login=force_login)
    renew_supported = bool(refresh_token)

    if renew_supported:
        print("[MODE] refresh-token mode enabled (fast renewal at each slot).", flush=True)
    else:
        print(
            "[MODE] access-only fallback mode enabled (no refresh_token). "
            "Each slot will validate/recover access_token.",
            flush=True,
        )

    if test_now:
        if renew_supported:
            refresh_token = _renew_access_token_fast(kite, api_secret, refresh_token, reason="test-now")
        else:
            if _access_token_is_valid(kite):
                print("[TEST] access_token is valid; no renewal possible without refresh_token.", flush=True)
            else:
                print("[TEST] access_token invalid; running browser login fallback.", flush=True)
                kite, refresh_token = _seed_session_for_today(parts, force_login=True)
        print("[DONE] test-now completed.", flush=True)
        return

    today = today_ist()
    slots = _slots_for_day(today)
    print(
        f"[LIVE] auth_v2 scheduler started for {today} | slots={len(slots)} "
        f"({slots[0].strftime('%H:%M')}..{slots[-1].strftime('%H:%M')} IST)",
        flush=True,
    )

    runs = 0
    for slot in slots:
        now = now_ist()
        if now > (slot + timedelta(seconds=SLOT_GRACE_SEC)):
            continue
        if now < slot:
            print(f"[WAIT] next slot {slot.strftime('%H:%M:%S')}", flush=True)
            _sleep_until(slot)

        label = slot.strftime("%H:%M")
        try:
            if refresh_token:
                refresh_token = _renew_access_token_fast(kite, api_secret, refresh_token, reason=f"slot-{label}")
            else:
                if _access_token_is_valid(kite):
                    print(
                        f"[KEEP ] slot={label} access_token valid "
                        "(refresh_token unavailable).",
                        flush=True,
                    )
                else:
                    print(
                        f"[WARN] slot={label} access_token invalid; "
                        "running browser login fallback.",
                        flush=True,
                    )
                    kite, refresh_token = _seed_session_for_today(parts, force_login=True)
                    if refresh_token:
                        print(
                            f"[INFO] slot={label} refresh_token available after login; "
                            "switching to fast-renew mode.",
                            flush=True,
                        )
        except Exception as e:
            print(f"[WARN] slot={label} token step failed ({e}); attempting relogin", flush=True)
            kite, refresh_token = _seed_session_for_today(parts, force_login=True)

        runs += 1
        if max_refresh > 0 and runs >= max_refresh:
            print(f"[DONE] max_refresh={max_refresh} reached.", flush=True)
            return

    print("[DONE] Reached end of slot schedule (15:30 IST).", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--force-login", action="store_true", help="Force browser login even if same-day state exists.")
    p.add_argument("--test-now", action="store_true", help="Renew access token once immediately and exit.")
    p.add_argument("--max-refresh", type=int, default=0, help="Stop after N slot renewals (0 means full day).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    parts = _read_key_secret()
    run_slot_scheduler(
        parts=parts,
        force_login=bool(args.force_login),
        test_now=bool(args.test_now),
        max_refresh=int(args.max_refresh),
    )


if __name__ == "__main__":
    main()
