from pathlib import Path
import json
import subprocess
from bs4 import BeautifulSoup

folder = Path(__file__).resolve().parent
root = folder.parents[1]
report = root / 'docs/G3_V7_DETAILED_CONFIGURATION_2026-10-13.html'
snapshot = json.loads(report.with_suffix('.json').read_text(encoding='utf-8'))
soup = BeautifulSoup(report.read_text(encoding='utf-8'), 'html.parser')
table = soup.select('#setups tbody tr')
assert len(table) == 59
for dom, expected in zip(table, snapshot['setups']):
    values = [cell.get_text() for cell in dom.select('td')]
    assert len(values) == 16
    assert dom['data-setup'] == expected['setup_id']
    assert values[:3] == [expected['signal_end'], expected['confirmation_end'], expected['side']]
    for column, value in [
        (3, expected['price_change_pct']), (4, expected['core_price_change_pct']),
        (5, expected['oi_change_pct']), (6, expected['volume_ratio']),
        (7, expected['body_ratio'] * 100), (8, expected['max_wick_ratio'] * 100),
        (9, expected['confirmation_volume_ratio_min']), (10, expected['stop_pct']),
        (11, expected['target_pct']), (13, expected['max_entries']),
    ]:
        assert abs(float(values[column]) - value) < 1e-9, (expected['setup_id'], column)
    assert values[14:] == [expected['research_stage'], expected['group']]
for anchor in soup.select('a[href]'):
    href = anchor['href']
    if href.startswith('#'):
        assert soup.find(id=href[1:]) is not None
    elif not href.startswith(('http:', 'https:')):
        assert (report.parent / href).resolve().exists(), href

qa_js = r'''<script>
const qaResults=[];
function check(label,expected){const actual=document.querySelectorAll('#setups tbody tr:not([hidden])').length;if(actual!==expected)throw Error(label+': '+actual+' != '+expected);qaResults.push({label,count:actual});}
check('all',59);
side.value='LONG';side.dispatchEvent(new Event('input'));check('LONG',28);
group.value='Afternoon fifteen';group.dispatchEvent(new Event('input'));check('Afternoon LONG',7);
side.value='SHORT';side.dispatchEvent(new Event('input'));check('Afternoon SHORT',8);
document.getElementById('reset').click();check('reset',59);
search.value='09:50';search.dispatchEvent(new Event('input'));check('09:50',2);
search.value='1236_SHORT';search.dispatchEvent(new Event('input'));check('setup ID',1);
search.value='not a setup';search.dispatchEvent(new Event('input'));check('empty',0);
document.getElementById('reset').click();
const result=document.createElement('pre');result.id='qa-result';result.textContent=JSON.stringify({status:'PASS',checks:qaResults});document.body.append(result);
</script>'''
qa_page = folder / 'report_browser_qa.html'
qa_html = report.read_text(encoding='utf-8').replace('</body>', qa_js + '</body>')
# Show the matrix at the initial viewport for deterministic headless painting.
qa_html = qa_html.replace('</head>', '<style>main > :not(#matrix){display:none}</style></head>')
qa_page.write_text(qa_html, encoding='utf-8')
chrome = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
image = folder / 'report_matrix.png'
proc = subprocess.run([chrome, '--headless', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
                       '--run-all-compositor-stages-before-draw', '--disable-threaded-scrolling',
                       '--disable-extensions', '--hide-scrollbars', '--window-size=1700,1050',
                       '--virtual-time-budget=1200', '--dump-dom', '--screenshot=' + str(image), qa_page.as_uri()],
                      capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=45,
                      creationflags=subprocess.CREATE_NO_WINDOW)
assert proc.returncode == 0, proc.stderr[-1500:]
browser_soup = BeautifulSoup(proc.stdout, 'html.parser')
element = browser_soup.find(id='qa-result')
assert element is not None, 'Browser QA did not finish'
browser_qa = json.loads(element.get_text())
assert browser_qa['status'] == 'PASS'
assert image.exists()
receipt = {'html_rows': len(table), 'row_value_match': 'PASS', 'local_links': 'PASS',
           'browser_qa': browser_qa, 'screenshot': str(image)}
(folder / 'verification.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
print(json.dumps(receipt, indent=2))
