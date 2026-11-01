"""Headless blank-page handshake only; does not read credentials or log in."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import authentication_v2 as auth
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

prepared = auth._prepare_browser_driver()
options = Options()
options.add_argument('--headless=new')
options.binary_location = prepared['browser_path']
driver = webdriver.Chrome(service=Service(prepared['driver_path']), options=options)
try:
    driver.set_page_load_timeout(15)
    driver.get('about:blank')
    print('BLANK_PAGE_SMOKE_OK browser=' + driver.capabilities['browserVersion'])
finally:
    driver.quit()
