# -*- coding: utf-8 -*-
"""
Zerodha kiteconnect automated authentication
"""

from kiteconnect import KiteConnect
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException,
    ElementClickInterceptedException,
    TimeoutException,
)
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
from pyotp import TOTP
import pandas as pd


def autologin():
    token_path = "api_key.txt"
    with open(token_path, 'r') as f:
        key_secret = f.read().split()
    kite = KiteConnect(api_key=key_secret[0])

    # Setup Chrome WebDriver
    service = Service(ChromeDriverManager().install())
    options = Options()
    # Uncomment this if you want to run headless
    # options.add_argument('--headless')

    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(kite.login_url())
        wait = WebDriverWait(driver, 12)

        def _find_first(locators, condition, timeout=12):
            last_err = None
            for by, value in locators:
                try:
                    return WebDriverWait(driver, timeout).until(condition((by, value)))
                except TimeoutException as err:
                    last_err = err
            if last_err:
                raise last_err
            raise TimeoutException("No matching locator found.")

        def retry_send_keys(locators, keys, retries=4):
            attempt = 0
            while attempt < retries:
                try:
                    elem = _find_first(locators, EC.presence_of_element_located)
                    elem.clear()
                    elem.send_keys(keys)
                    return
                except (StaleElementReferenceException, TimeoutException):
                    attempt += 1
                    time.sleep(1.5)
                    print(f"Retrying send_keys: {attempt}/{retries}")
            raise TimeoutException(f"Could not send keys after {retries} retries.")

        def retry_click(locators, retries=4):
            attempt = 0
            while attempt < retries:
                try:
                    elem = _find_first(locators, EC.element_to_be_clickable)
                    elem.click()
                    return
                except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException):
                    attempt += 1
                    time.sleep(1.5)
                    print(f"Retrying click: {attempt}/{retries}")
            raise TimeoutException(f"Could not click after {retries} retries.")

        def optional_click(locators):
            try:
                retry_click(locators, retries=2)
                return True
            except TimeoutException:
                return False

        username_locators = [
            (By.ID, "userid"),
            (By.NAME, "user_id"),
            (By.CSS_SELECTOR, "input#userid"),
            (By.XPATH, "//form//input[@type='text']"),
        ]
        password_locators = [
            (By.ID, "password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input#password"),
            (By.XPATH, "//form//input[@type='password']"),
        ]
        submit_button_locators = [
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.XPATH, "//button[@type='submit']"),
        ]
        totp_input_locators = [
            (By.ID, "totp"),
            (By.NAME, "totp"),
            (By.CSS_SELECTOR, "input[autocomplete='one-time-code']"),
            (By.XPATH, "//form//input[@type='text' or @type='tel' or @type='number']"),
        ]

        # Step 1: Username/password page
        retry_send_keys(username_locators, key_secret[2])
        retry_send_keys(password_locators, key_secret[3])
        retry_click(submit_button_locators)

        # Step 2: TOTP page
        totp = TOTP(key_secret[4])
        retry_send_keys(totp_input_locators, totp.now())
        optional_click(submit_button_locators)

        # Step 3: Wait for redirect with request_token
        request_token = None
        deadline = time.time() + 30
        while time.time() < deadline:
            current_url = driver.current_url
            if "request_token=" in current_url:
                request_token = current_url.split("request_token=")[1][:32]
                break
            time.sleep(1)

        if not request_token:
            raise RuntimeError(f"request_token not found in redirect URL: {driver.current_url}")

        # Save request token to file
        with open('request_token.txt', 'w') as the_file:
            the_file.write(request_token)
    finally:
        driver.quit()

autologin()

# Generate and store access token - valid till 6 AM the next day
with open("request_token.txt", 'r') as f:
    request_token = f.read()
with open("api_key.txt", 'r') as f:
    key_secret = f.read().split()
kite = KiteConnect(api_key=key_secret[0])
data = kite.generate_session(request_token, api_secret=key_secret[1])

with open('access_token.txt', 'w') as file:
    file.write(data["access_token"])


# Get dump of all NSE instruments
nse_instrument_dump = kite.instruments("NSE")
nse_instrument_df = pd.DataFrame(nse_instrument_dump)

# Get dump of all BSE instruments
bse_instrument_dump = kite.instruments("BSE")
bse_instrument_df = pd.DataFrame(bse_instrument_dump)

# Merge the NSE and BSE data
merged_instrument_df = pd.concat([nse_instrument_df, bse_instrument_df])

# Save the merged data to a single CSV
merged_instrument_df.to_csv("Merged_NSE_BSE_Instruments.csv", index=False)

