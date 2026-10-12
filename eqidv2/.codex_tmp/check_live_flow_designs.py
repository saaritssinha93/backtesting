import json
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait

options=Options()
options.add_argument('--headless=new')
options.add_argument('--window-size=1120,1100')
options.add_argument('--disable-gpu')
driver=webdriver.Chrome(options=options)
out=Path('outputs')
try:
    driver.get('http://127.0.0.1:8916/live-flow-design-preview.html')
    WebDriverWait(driver,15).until(lambda d: d.find_elements('css selector','iframe'))
    driver.switch_to.frame(driver.find_element('css selector','iframe'))
    WebDriverWait(driver,15).until(lambda d: d.find_elements('css selector','.df-table'))
    report=[]
    for kind in ['comparison','inspector','heatmap','phone']:
        driver.execute_script("document.querySelectorAll('[data-variant]').forEach(s=>s.hidden=!s.querySelector('[data-design=\"'+arguments[0]+'\"]'));",kind)
        if kind=='comparison':
            Select(driver.find_element('css selector','[data-design="comparison"] select')).select_by_value('oi')
            assert 'TATASTEEL' in driver.find_element('css selector','[data-design="comparison"] tbody tr').text
        elif kind=='inspector':
            driver.find_element('css selector','[data-symbol="SAIL"]').click()
            assert driver.find_element('css selector','.df-inspector h3').text=='SAIL'
        elif kind=='heatmap':
            Select(driver.find_element('css selector','[data-design="heatmap"] select')).select_by_value('five')
            assert len(driver.find_elements('css selector','.df-gap'))==2
        else:
            driver.find_element('css selector','[data-side="SHORT"]').click()
            assert 'SAIL' in driver.find_element('css selector','.df-stockcard').text
            driver.find_element('css selector','.df-stockcard summary').click()
            assert driver.find_element('css selector','.df-stockcard details').get_attribute('open')
        el=driver.find_element('css selector',f'[data-design="{kind}"]')
        el.screenshot(str(out/f'live-flow-{kind}-desktop.png'))
        report.append(driver.execute_script('return {design:arguments[0].dataset.design,width:arguments[0].clientWidth,height:arguments[0].clientHeight,bodyOverflow:document.documentElement.scrollWidth>document.documentElement.clientWidth};',el))
    driver.switch_to.default_content()
    driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride',{'width':390,'height':1100,'deviceScaleFactor':1,'mobile':True})
    driver.switch_to.frame(driver.find_element('css selector','iframe'))
    driver.find_element('css selector','[data-design="phone"]').screenshot(str(out/'live-flow-phone-narrow.png'))
    report.append(driver.execute_script('return {mobileWidth:document.documentElement.clientWidth,bodyOverflow:document.documentElement.scrollWidth>document.documentElement.clientWidth};'))
    for width in [320,736,1024]:
        driver.switch_to.default_content()
        driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride',{'width':width,'height':1200,'deviceScaleFactor':1,'mobile':True})
        driver.switch_to.frame(driver.find_element('css selector','iframe'))
        for kind in ['comparison','inspector','heatmap','phone']:
            driver.execute_script("document.querySelectorAll('[data-variant]').forEach(s=>s.hidden=!s.querySelector('[data-design=\"'+arguments[0]+'\"]'));",kind)
            el=driver.find_element('css selector',f'[data-design="{kind}"]')
            report.append(driver.execute_script('return {design:arguments[0].dataset.design,width:document.documentElement.clientWidth,height:arguments[0].clientHeight,bodyOverflow:document.documentElement.scrollWidth>document.documentElement.clientWidth};',el))
    print(json.dumps(report,indent=2))
    print('Browser errors:',json.dumps(driver.get_log('browser')))
finally:
    driver.quit()
