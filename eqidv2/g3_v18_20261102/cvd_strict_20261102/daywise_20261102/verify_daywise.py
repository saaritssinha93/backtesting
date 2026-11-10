"""Check browser-calculated daily views and preserve every earlier artifact."""
import hashlib
import html
import json
import re
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parent
STRICT=ROOT.parent
WORKSPACE=STRICT.parent.parent


def sha(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b''):
            value.update(chunk)
    return value.hexdigest()


def main():
    summary=json.loads((STRICT/'results_20261102/summary.json').read_text(encoding='utf-8'))
    browser_checks=[]
    for name,variant,reference,cost,period,count,first in (
        ('rendered_default.html','strict_gate','cvd_ready_control',5,'all',62,'2026-07-29'),
        ('rendered_filtered.html','strict_rvol_gate','rvol_ready_control',15,'october',19,'2026-10-30')):
        body=(ROOT/name).read_text(encoding='utf-8-sig')
        daily=re.search(r'<tbody id="daily">(.*?)</tbody>',body,re.S).group(1)
        assert daily.count('<tr>')==count,(name,'row count')
        assert re.search(r'<td>(.*?)</td>',daily).group(1)==first,(name,'date order')
        expected={r['variant']:r for r in summary['overall_metrics'] if r['period']==period and r['cost_bps']==cost}
        for element,number in [('ownNet',expected[variant]['net_pnl_rs']),('refNet',expected[reference]['net_pnl_rs']),('delta',expected[variant]['net_pnl_rs']-expected[reference]['net_pnl_rs']),('drawdown',expected[variant]['daily_close_drawdown_rs'])]:
            text=html.unescape(re.search(r'<strong id="'+element+r'"[^>]*>(.*?)</strong>',body,re.S).group(1))
            actual=float(text.replace('₹','').replace(',','').replace('−','-'))
            assert abs(actual-number)<=.005,(name,element,actual,number)
        browser_checks.append({'file':name,'rows':count,'first_displayed_date':first,'variant':variant,'reference':reference,'cost_bps':cost,'period':period,'profit_difference_and_drawdown_match':True})
    if '--browser-only' in sys.argv:
        print(json.dumps({'passed':True,'browser_checks':browser_checks}))
        return
    prior=json.loads((STRICT/'preservation_before.json').read_text(encoding='utf-8'))
    for name,expected in prior.items():
        p=WORKSPACE/name
        assert sha(p)==expected['sha256'] and p.stat().st_mtime_ns==expected['mtime_ns'],name
    counts={}
    for directory,filename in [(STRICT/'results_20261102','manifest.json'),(STRICT,'publication_manifest.json')]:
        content=json.loads((directory/filename).read_text(encoding='utf-8'))
        for name,expected in content['files'].items():
            assert sha(directory/name)==expected,name
        counts[filename]=len(content['files'])
    links=[]
    content=(ROOT/'daywise_report.html').read_text(encoding='utf-8')
    for href in re.findall(r'href="([^"]+)"',content):
        assert (ROOT/html.unescape(href)).is_file(),href
        links.append(href)
    output={'passed':True,'old_files_hash_and_mtime_preserved':len(prior),'existing_result_artifacts_preserved':counts['manifest.json'],'existing_publication_artifacts_preserved':counts['publication_manifest.json'],'browser_checks':browser_checks,'links_checked':len(links),'backtest_rerun':False}
    (ROOT/'delivery_verification.json').write_text(json.dumps(output,indent=2)+'\n',encoding='utf-8')
    delivered=['daywise_report.html','daywise_comparison.csv','daywise_verification.json','delivery_verification.json','outputs/g3_strict_daywise/G3_strict_CVD_daywise.xlsx']
    (ROOT/'delivery_manifest.json').write_text(json.dumps({'files':{p:sha(ROOT/p) for p in delivered}},indent=2)+'\n',encoding='utf-8')
    print(json.dumps(output))


if __name__=='__main__':
    main()
