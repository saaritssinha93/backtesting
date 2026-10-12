import json, re, base64
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.keys import Keys

launcher=Path('bat/run_log_dashboard_server.bat').read_text()
user=re.search(r'set "LOG_DASH_USER=([^"\r\n]+)"',launcher).group(1)
password=re.search(r'set "LOG_DASH_PASS=([^"\r\n]+)"',launcher).group(1)
options=Options()
options.add_argument('--headless=new')
options.add_argument('--window-size=1720,1200')
options.set_capability('goog:loggingPrefs',{'browser':'ALL'})
driver=webdriver.Chrome(options=options)
driver.set_script_timeout(100)
out=Path('outputs');out.mkdir(exist_ok=True)
report=[]
try:
    driver.execute_cdp_cmd('Network.enable',{})
    driver.execute_cdp_cmd('Network.setExtraHTTPHeaders',{'headers':{'Authorization':'Basic '+base64.b64encode((user+':'+password).encode()).decode()}})
    driver.get('http://127.0.0.1:8787/dashboard-flow#live')
    WebDriverWait(driver,100).until(lambda d: d.find_elements('css selector','.lf-rank-table tbody tr'))
    driver.find_element('css selector','.lf-auto-refresh').click()
    for kind in ['comparison','focus','heatmap','movement','scatter','cards']:
        Select(driver.find_element('css selector','[aria-label="Market analysis view"]')).select_by_value(kind)
        workspace=driver.find_element('css selector','.lf-workspace')
        driver.execute_script('arguments[0].scrollIntoView({block:"start"});',workspace)
        if kind=='focus':
            before=driver.execute_script('return scrollY')
            driver.find_element('css selector','.lf-lanes .lf-symbol').click()
            assert abs(driver.execute_script('return scrollY')-before)<5
            assert driver.find_element('css selector','.lf-detail').is_displayed()
        driver.save_screenshot(str(out/f'live-workspace-{kind}.png'))
        if kind=='focus':driver.find_element('css selector','.lf-close').click()
        report.append({'view':kind,'panels':len(driver.find_elements('css selector','.lf-lanes > section')),'pageOverflow':driver.execute_script('return document.documentElement.scrollWidth>innerWidth')})
    # Persist selected view, symbol and table scroll across a real data refresh.
    Select(driver.find_element('css selector','[aria-label="Market analysis view"]')).select_by_value('comparison')
    driver.find_element('css selector','.lf-lanes .lf-symbol').click()
    symbol=driver.find_element('css selector','.lf-detail h3').text
    driver.execute_script('document.querySelector(".lf-lane .lf-table-wrap").scrollTop=150')
    driver.execute_async_script('const done=arguments[arguments.length-1];FlowLive.refresh().then(()=>done(true),()=>done(false));')
    assert driver.find_element('css selector','.lf-detail h3').text==symbol
    assert driver.execute_script('return document.querySelector(".lf-lane .lf-table-wrap").scrollTop')==150
    driver.find_element('css selector','.lf-close').click()
    for width in [320,390,736,1024]:
        driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride',{'width':width,'height':1100,'deviceScaleFactor':1,'mobile':True})
        for kind in ['comparison','focus','heatmap','movement','scatter','cards']:
            Select(driver.find_element('css selector','[aria-label="Market analysis view"]')).select_by_value(kind)
            overflow=driver.execute_script('return document.documentElement.scrollWidth>innerWidth')
            report.append({'view':kind,'width':width,'pageOverflow':overflow})
            assert not overflow,(kind,width)
        if width==390:
            driver.find_element('css selector','.lf-direction[data-side="SHORT"]').click()
            assert len(driver.find_elements('css selector','.lf-lane'))==1
            driver.execute_script('document.querySelector(".lf-viewbar").scrollIntoView({block:"start"})')
            driver.save_screenshot(str(out/'live-workspace-mobile.png'))
    driver.execute_cdp_cmd('Emulation.clearDeviceMetricsOverride',{})
    driver.find_element('css selector','.lf-direction[data-side="BOTH"]').click()
    Select(driver.find_element('css selector','[aria-label="Market analysis view"]')).select_by_value('heatmap')
    cell=driver.find_elements('css selector','.lf-heat-cell')[1]
    chosen_time=cell.get_attribute('aria-label').split(' at ')[1].split(': ')[0]
    cell.click()
    assert driver.find_element('css selector','.lf-clock').text==chosen_time
    assert driver.find_element('css selector','.lf-detail').is_displayed()
    driver.find_element('css selector','.lf-detail').send_keys(Keys.ESCAPE)
    assert not driver.find_element('css selector','.lf-detail').is_displayed()
    report.append({'heatmapSelectionAndEscape':'passed'})
    driver.find_element('css selector','#flow-theme').click()
    driver.execute_script('document.querySelector(".lf-viewbar").scrollIntoView({block:"start"})')
    driver.save_screenshot(str(out/'live-workspace-dark.png'))
    Select(driver.find_element('css selector','[aria-label="Market session date"]')).select_by_value('2026-10-09')
    WebDriverWait(driver,100).until(lambda d: d.find_element('css selector','#live-view').get_attribute('aria-busy')=='false' and '09 Oct 2026' in d.find_element('css selector','.lf-clock-label').text)
    for kind in ['comparison','focus','heatmap','movement','scatter','cards']:
        Select(driver.find_element('css selector','[aria-label="Market analysis view"]')).select_by_value(kind)
        assert driver.find_element('css selector','.lf-lanes').text
    report.append({'historicalSession':'2026-10-09','viewsChecked':6})
    errors=[item for item in driver.get_log('browser') if item['level']=='SEVERE']
    report.append({'browserErrors':errors})
    assert not errors
    Path('outputs/live-workspace-qa.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
finally:
    driver.quit()
