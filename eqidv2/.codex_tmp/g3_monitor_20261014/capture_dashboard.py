"""Capture authenticated local dashboard views without printing its token."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time
from urllib.parse import urlencode

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE.parent / "g3_selected_ten_20261013" / "helpers"))
import psutil

chrome = Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe")
assert chrome.is_file()
token = None
for process in psutil.process_iter(["name"]):
    if "python" not in (process.info["name"] or "").lower():
        continue
    try:
        if Path(process.cwd()).resolve() != ROOT.resolve():
            continue
        if "log_dashboard_server.py" in {Path(value).name for value in process.cmdline() if value.endswith(".py")}:
            token = process.environ().get("LOG_DASH_TOKEN")
            break
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        continue
assert token

dark = "--dark" in sys.argv
prefix = "dark_" if dark else ""
url = "http://127.0.0.1:8787/?" + urlencode({"token": token}) + "#g3-trade-monitor"
captures = []
options = webdriver.ChromeOptions()
options.binary_location = str(chrome)
for argument in ("--headless=new", "--disable-gpu", "--hide-scrollbars", "--incognito",
                 "--no-first-run", "--disable-background-networking"):
    options.add_argument(argument)
driver = None
try:
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(45)
    driver.set_window_size(1600, 1400)
    driver.get(url)
    if dark:
        driver.execute_script("localStorage.setItem('eqidv2_dashboard_theme', 'dark')")
        driver.refresh()
    wait = WebDriverWait(driver, 45)
    wait.until(lambda item: "Market-data feed:" in item.find_element(By.CSS_SELECTOR, "[data-zone='feed']").text)
    wait.until(lambda item: "Fixed-policy comparison" in item.find_element(By.CSS_SELECTOR, "[data-zone='research']").text)
    time.sleep(1)
    for name, width, height in ((prefix + "dashboard_desktop.png", 1600, 1400),
                                (prefix + "dashboard_mobile.png", 430, 1200)):
        destination = HERE / name
        driver.set_window_size(width, height)
        driver.execute_script("window.scrollTo(0, 0)")
        time.sleep(.5)
        assert driver.save_screenshot(str(destination))
        assert destination.is_file() and destination.stat().st_size > 10000
        captures.append({"file": name, "bytes": destination.stat().st_size,
                         "viewport": f"{width},{height}"})
    destination = HERE / (prefix + "dashboard_research.png")
    driver.set_window_size(1600, 1000)
    research = driver.find_element(By.CSS_SELECTOR, ".g3tm-research")
    driver.execute_script("arguments[0].scrollIntoView({block: 'start'})", research)
    time.sleep(.5)
    assert driver.save_screenshot(str(destination))
    assert destination.is_file() and destination.stat().st_size > 10000
    captures.append({"file": destination.name, "bytes": destination.stat().st_size,
                     "viewport": "1600,1000"})
except Exception as error:
    raise RuntimeError(f"Dashboard capture failed: {type(error).__name__}") from None
finally:
    if driver is not None:
        driver.quit()

receipt = {"status": "PASS", "captures": captures, "credential_printed": False}
(HERE / "dashboard_capture.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
print(json.dumps(receipt, indent=2))
