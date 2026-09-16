import json
import csv
import math
import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait

ROOT=Path(__file__).resolve().parent
HTML=Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/V13_V10_G_INTERACTIVE_BACKTEST.html')
DOWNLOAD=ROOT/('downloads_'+str(time.time_ns()))
DOWNLOAD.mkdir(exist_ok=True)
options=Options()
for arg in ['--headless=new','--disable-gpu','--window-size=1440,1000']:
    options.add_argument(arg)
options.set_capability('goog:loggingPrefs',{'browser':'ALL'})
options.add_experimental_option('prefs',{'download.default_directory':str(DOWNLOAD),'download.prompt_for_download':False,'profile.default_content_setting_values.automatic_downloads':1})
driver=webdriver.Chrome(options=options)
checks=[]
completed=False
def check(name,condition,details=None):
    checks.append(dict(name=name,passed=bool(condition),details=details))
    assert condition,(name,details)
def get(id):return driver.find_element(By.ID,id)
def select(id,value):Select(get(id)).select_by_value(value)
def capture(id,name):
    driver.execute_script("arguments[0].scrollIntoView({block:'start'})",get(id))
    time.sleep(.25)
    driver.save_screenshot(str(ROOT/name))
try:
    driver.get(HTML.as_uri()+'#options-projections')
    WebDriverWait(driver,20).until(lambda d:len(d.find_elements(By.CSS_SELECTOR,'.opt-section svg'))>=7)
    payload=driver.execute_script("return JSON.parse(document.getElementById('optModelData').textContent)")
    check('at_least_seven_options_graphs',len(driver.find_elements(By.CSS_SELECTOR,'.opt-section svg'))>=7)
    check('three_sizing_policies',{o.get_attribute('value') for o in Select(get('optSizing')).options}=={'fixed','monthly','stepup'})
    check('actual_trade_rows',len(driver.find_elements(By.CSS_SELECTOR,'#optTradeTable tbody tr'))==20)
    check('historical_session_rows',len(driver.find_elements(By.CSS_SELECTOR,'#optHistoryTable tbody tr'))==13)
    stock_stats=get('projectionStats').get_attribute('textContent')
    capture('options-projections','options_desktop.png')
    for a in payload['annual']:
        select('optSizing',a['sizing'])
        select('optScenario',a['scenario'])
        key=a['scenario']+'_'+a['sizing']
        text=get('optProjectionStats').text
        check('scenario_kpi_'+key,f"{a['mean_future_pnl']/1e5:.2f}L" in text,text)
        check('monthly_rows_'+key,len(driver.find_elements(By.CSS_SELECTOR,'#optMonthlyTable tbody tr'))==12)
        expected_months=[r for r in payload['monthly'] if r['scenario']==a['scenario'] and r['sizing']==a['sizing']]
        lot_points=driver.find_elements(By.CSS_SELECTOR,'#optLotChart circle')
        check('monthly_lot_curve_'+key,len(lot_points)==12 and all(math.isclose(float(point.get_attribute('data-opt-value')),row['p50_lots'],abs_tol=1e-8) for point,row in zip(lot_points,expected_months)))
        for measure in ['equity','future','cumulative','return']:
            select('optProjectionMeasure',measure)
            circles=driver.find_elements(By.CSS_SELECTOR,'#optProjectionChart circle')
            check('curve_'+key+'_'+measure,len(circles)==253)
            endpoint=circles[-1].get_attribute('data-opt-tip')
            expected={'equity':a['mean_ending_equity'],'future':a['mean_future_pnl'],'cumulative':a['mean_cumulative_pnl'],'return':a['mean_return_pct']}[measure]
            actual=float(circles[-1].get_attribute('data-opt-value'))
            check('endpoint_'+key+'_'+measure,math.isclose(actual,expected,rel_tol=1e-10,abs_tol=1e-7),endpoint)
    check('stock_stats_unchanged_by_options',get('projectionStats').get_attribute('textContent')==stock_stats)
    get('optShowBand').click()
    check('band_toggle_off',len(driver.find_elements(By.CSS_SELECTOR,'#optProjectionChart .opt-band, #optMonthlyProfitChart .opt-whisker, #optLotChart .opt-band'))==0)
    get('optShowBand').click()
    check('band_toggle_on',len(driver.find_elements(By.CSS_SELECTOR,'#optProjectionChart .opt-band'))==1)
    for value in ['pnl','equity','cumulative']:
        select('optHistoryMeasure',value)
        check('history_measure_'+value,len(driver.find_elements(By.CSS_SELECTOR,'#optHistoryChart svg'))==1)
    select('optScenario','retain_50')
    select('optSizing','stepup')
    select('optProjectionMeasure','equity')
    for id in ['optCurveDownload','optMonthlyDownload','optAnnualDownload']:
        before=set(DOWNLOAD.glob('*.csv'))
        driver.execute_script('arguments[0].click()',get(id))
        WebDriverWait(driver,10).until(lambda d:bool(set(DOWNLOAD.glob('*.csv'))-before))
        path=(set(DOWNLOAD.glob('*.csv'))-before).pop()
        check('download_'+id,path.stat().st_size>100)
        rows=list(csv.DictReader(path.read_text(encoding='utf-8-sig').splitlines()))
        check('download_sizing_'+id,all(r['sizing']=='stepup' for r in rows))
        check('download_count_'+id,len(rows)=={'optCurveDownload':253,'optMonthlyDownload':12,'optAnnualDownload':4}[id])
    before_svg=set(DOWNLOAD.glob('*.svg'))
    driver.execute_script("document.querySelector('[data-opt-export-chart=optProjectionChart]').click()")
    WebDriverWait(driver,10).until(lambda d:bool(set(DOWNLOAD.glob('*.svg'))-before_svg))
    svg_path=(set(DOWNLOAD.glob('*.svg'))-before_svg).pop()
    check('svg_export','<svg' in svg_path.read_text(encoding='utf8'))
    before=set(DOWNLOAD.glob('*.csv'))
    driver.execute_script('arguments[0].click()',get('optComparisonDownload'))
    WebDriverWait(driver,10).until(lambda d:bool(set(DOWNLOAD.glob('*.csv'))-before))
    comparison=(set(DOWNLOAD.glob('*.csv'))-before).pop()
    rows=list(csv.DictReader(comparison.read_text(encoding='utf-8-sig').splitlines()))
    check('full_comparison_export',len(rows)==12 and {r['sizing'] for r in rows}=={'fixed','monthly','stepup'})
    check('annual_comparison_table',len(driver.find_elements(By.CSS_SELECTOR,'#optAnnualTable tbody tr'))==12)
    capture('options-history','options_history_desktop.png')
    capture('optMonthlyProfitChart','options_monthly_desktop.png')
    capture('optLotChart','options_lots_desktop.png')
    driver.execute_script('arguments[0].click()',get('themeButton'))
    time.sleep(.3)
    capture('options-projections','options_dark.png')
    check('dark_theme',driver.execute_script('return document.documentElement.dataset.theme')=='dark')
    driver.execute_script('arguments[0].click()',get('themeButton'))
    for width in [390,360]:
        driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride',{'width':width,'height':900,'deviceScaleFactor':1,'mobile':True})
        time.sleep(.3)
        capture('options-projections',f'options_mobile_{width}.png')
        capture('optLotChart',f'options_lots_mobile_{width}.png')
        dimensions=driver.execute_script('return {width:innerWidth,scroll:document.documentElement.scrollWidth,sections:[...document.querySelectorAll(".opt-section")].map(e=>({width:e.clientWidth,scroll:e.scrollWidth}))}')
        check('mobile_viewport_'+str(width),dimensions['width']==width,dimensions)
        check('mobile_options_no_overflow_'+str(width),all(r['scroll']<=r['width']+1 for r in dimensions['sections']),dimensions)
    driver.execute_cdp_cmd('Emulation.clearDeviceMetricsOverride',{})
    driver.set_window_size(1440,1000)
    driver.execute_script("location.hash='projections'")
    check('stock_chart_present',len(driver.find_elements(By.CSS_SELECTOR,'#projectionChart svg'))==1)
    old=get('projectionStats').text
    choices=Select(get('scenario')).options
    Select(get('scenario')).select_by_index(0 if Select(get('scenario')).first_selected_option.get_attribute('value')!=choices[0].get_attribute('value') else 1)
    check('stock_scenario_controls_work',get('projectionStats').text!=old)
    errors=[r for r in driver.get_log('browser') if r['level']=='SEVERE']
    check('no_browser_javascript_errors',not errors,errors)
    completed=True
finally:
    driver.quit()
    result={'passed':completed and all(r['passed'] for r in checks),'checks_count':len(checks),'checks':checks}
    (ROOT/'browser_validation.json').write_text(json.dumps(result,indent=2),encoding='utf8')
    print(json.dumps(result,indent=2))
