# -*- coding: utf-8 -*-
"""
Zerodha kiteconnect automated authentication (ET4 variant).

Designed to be run directly (Spyder runfile compatible).
It supports automatic external TOTP input:
- env KITE_TOTP_COMMAND (command returning 6-digit OTP)
- env KITE_TOTP_FILE (file containing OTP)
- fallback to pyotp secret from api_key.txt
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path
from typing import Iterable, Optional, Tuple

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
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
OTP_RE = re.compile(r"\b(\d{6})\b")


def _extract_otp(text: str) -> Optional[str]:
    if not text:
        return None
    m = OTP_RE.search(text)
    return m.group(1) if m else None


def _read_key_secret() -> list[str]:
    token_path = ROOT / "api_key.txt"
    parts = token_path.read_text(encoding="utf-8").split()
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

    # Clipboard fallback (useful with external TOTP apps that copy OTP).
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


def _safe_click(driver: webdriver.Chrome, wait: WebDriverWait, element) -> None:
    if element is None:
        raise TimeoutException("Element is None")
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    except JavascriptException:
        # ignore scroll failures; click fallback handles it
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
            _safe_click(driver, wait, element)
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

    # Final fallback: fresh presence lookup + js click.
    element = _find_first(wait, locators, EC.presence_of_element_located)
    if element is None:
        raise last_error or TimeoutException("Could not find element for click fallback.")
    driver.execute_script("arguments[0].click();", element)


def _fill_totp(wait: WebDriverWait, otp: str) -> None:
    single_locators = [
        # ET4-working explicit OTP field xpath
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

    # Fallback for 6 separate OTP boxes.
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


def autologin() -> None:
    key_secret = _read_key_secret()
    kite = KiteConnect(api_key=key_secret[0])

    service = Service(ChromeDriverManager().install())
    options = Options()
    # options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(kite.login_url())
        wait = WebDriverWait(driver, 15)

        username_locators = [
            # ET4-working explicit xpath
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input"),
            (By.ID, "userid"),
            (By.NAME, "user_id"),
            (By.CSS_SELECTOR, "input#userid"),
            (By.XPATH, "//form//input[@type='text']"),
        ]
        password_locators = [
            # ET4-working explicit xpath
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input"),
            (By.ID, "password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input#password"),
            (By.XPATH, "//form//input[@type='password']"),
        ]
        login_btn_locators = [
            # ET4-working explicit xpath
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.XPATH, "//button[@type='submit']"),
        ]
        submit_btn_locators = [
            # ET4-working explicit xpath
            (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[2]/button"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.XPATH, "//button[@type='submit']"),
        ]

        _type_with_retry(wait, username_locators, key_secret[2])
        _type_with_retry(wait, password_locators, key_secret[3])
        _click_with_retry(driver, wait, login_btn_locators)

        otp, source = _resolve_totp(key_secret[4])
        print(f"[AUTH] TOTP source: {source}", flush=True)
        _fill_totp(wait, otp)

        # Submit if available; if not available but redirect already happened, continue.
        try:
            _click_with_retry(driver, wait, submit_btn_locators, retries=3)
        except Exception:
            pass

        # Poll URL for request_token (handles both direct and localhost callback URLs).
        deadline = time.time() + 45
        request_token = None
        while time.time() < deadline:
            request_token = _extract_request_token(driver.current_url)
            if request_token:
                break
            time.sleep(0.5)

        if not request_token:
            raise TimeoutException("request_token not found in URL")
        (ROOT / "request_token.txt").write_text(request_token, encoding="utf-8")
        print(f"[OK] request_token saved -> {ROOT / 'request_token.txt'}", flush=True)
    except TimeoutException as exc:
        raise RuntimeError(
            f"Login flow timed out before request_token was available. Last URL: {driver.current_url}"
        ) from exc
    finally:
        driver.quit()


def main() -> None:
    autologin()


if __name__ == "__main__":
    main()
