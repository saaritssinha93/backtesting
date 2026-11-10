"""Verify frozen results, chronological choices, browser report and links."""
import csv
import hashlib
import html
import json
import re
from pathlib import Path
from .policy import choose_policy

ROOT=Path(__file__).resolve().parent
RESULTS=ROOT/'results_20261102'


def sha(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b''):
            value.update(chunk)
    return value.hexdigest()


def main():
    manifest=json.loads((RESULTS/'manifest.json').read_text(encoding='utf-8'))
    for name,expected in manifest['files'].items():
        assert sha(RESULTS/name)==expected,name
    summary=json.loads((RESULTS/'summary.json').read_text(encoding='utf-8'))
    days=summary['days']
    assert len(days)==62 and days==sorted(set(days))
    with (RESULTS/'daily_results.csv').open(encoding='utf-8-sig',newline='') as stream:
        daily=list(csv.DictReader(stream))
    assert len(daily)==62*8*3
    for variant in summary['variants']:
        for cost in (5,9,15):
            rows=[r for r in daily if r['variant']==variant and int(r['cost_bps'])==cost]
            assert sorted(r['day'] for r in rows)==days,(variant,cost,'calendar')
    locks=summary['calibration_locks']
    assert sorted(locks)==['2026-09','2026-10']
    for month,lock in locks.items():
        assert lock['cutoff']<lock['before_loading_day']
        assert max(lock['training_dates'])<=lock['cutoff']
        assert lock['persisted_before_future_day'] and not lock['future_outcomes_used_for_policy']
        choice=choose_policy(lock['training_metrics'],lock['changed_selected_stock'])
        assert choice==lock['policy'],(month,'training policy proof')
    assessment=json.loads((ROOT/'research_assessment.json').read_text(encoding='utf-8'))
    best=assessment['best_relaxed_net_variant']
    browser=[]
    for name,variant,cost,period,count,first in [('rendered_default.html',best,5,'all',62,'2026-07-29'),('rendered_filtered.html',best,15,'october',19,'2026-10-30')]:
        body=(ROOT/name).read_text(encoding='utf-8-sig')
        tbody=re.search(r'<tbody id="dailyRows">(.*?)</tbody>',body,re.S).group(1)
        assert tbody.count('<tr>')==count
        assert re.search(r'<td>(.*?)</td>',tbody).group(1)==first
        displayed=html.unescape(re.search(r'<p id="dailySummary">(.*?)</p>',body,re.S).group(1))
        actual=float(re.search(r'(?:−)?₹[\d,.]+',displayed).group(0).replace('₹','').replace(',','').replace('−','-'))
        expected=next(r for r in summary['overall_metrics'] if r['variant']==variant and r['period']==period and r['cost_bps']==cost)
        assert abs(actual-expected['net_pnl_rs'])<=.005
        browser.append(dict(render=name,variant=variant,cost_bps=cost,period=period,rows=count,pnl_matches=True))
    content=(ROOT/'report.html').read_text(encoding='utf-8')
    assert not re.search(r'__[A-Z]+__',content)
    links=re.findall(r'href="([^"]+)"',content)
    for href in links:
        assert (ROOT/html.unescape(href)).is_file(),href
    with (ROOT/'daywise_comparison.csv').open(encoding='utf-8-sig',newline='') as stream:
        wide=list(csv.DictReader(stream))
    assert len(wide)==186 and len({(r['day'],r['cost_bps']) for r in wide})==186
    for row in wide:
        assert row['best_relaxed_variant']==best
        for variant in summary['variants']:
            source=next(r for r in daily if r['day']==row['day'] and r['cost_bps']==row['cost_bps'] and r['variant']==variant)
            assert float(row[variant+'_net_pnl_rs'])==float(source['net_pnl_rs'])
            assert int(row[variant+'_trades'])==int(source['trades'])
    preservation=json.loads((ROOT/'preservation_verified.json').read_text(encoding='utf-8'))
    assert preservation['passed'] and not preservation['changed']
    proof=dict(passed=True,result_artifacts_verified=len(manifest['files']),sessions=len(days),variants=len(summary['variants']),daily_rows=len(daily),chronological_locks_reproduced=2,browser_checks=browser,daywise_export_rows=len(wide),local_report_links_checked=len(links),prior_files_preserved=preservation['prior_files_checked'],execution_authority=False)
    (ROOT/'publication_verification.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf-8')
    artifacts=['START_HERE.html','report.html','research_assessment.json','daywise_comparison.csv','methodology.json','README.md','selection.py','run.py','policy.py','tests_selection.py','tests_runner.py','tests_policy.py','validation_tests.txt','preservation_before.json','preservation_verified.json','make_overview.py','verify_publication.py','publication_verification.json','results_20261102/manifest.json','report_preview.png']
    (ROOT/'publication_manifest.json').write_text(json.dumps({'files':{name:sha(ROOT/name) for name in artifacts}},indent=2)+'\n',encoding='utf-8')
    print(json.dumps(proof))


if __name__=='__main__':
    main()
