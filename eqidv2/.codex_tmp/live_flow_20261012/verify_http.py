import os,urllib.request,urllib.error,json,time,base64,gzip
from pathlib import Path
headers={'Authorization':'Basic '+base64.b64encode((os.environ['LOG_DASH_USER']+':'+os.environ['LOG_DASH_PASS']).encode()).decode(),'Accept-Encoding':'gzip'}
checks=[]
for port,day in [(8787,'2026-10-12'),(8787,'2026-10-12'),(8787,'2026-10-08'),(8790,'2026-10-12')]:
 start=time.monotonic()
 with urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{port}/api/dashboard-flow/live?date={day}',headers=headers),timeout=90) as response:
  raw=response.read(); p=json.loads(gzip.decompress(raw) if response.headers.get('Content-Encoding')=='gzip' else raw)
 assert len(p['slots'])==76 and p['read_only'] is True
 s=next(x for x in p['slots'] if x['time']=='09:30')
 item={'port':port,'date':day,'http':response.status,'seconds':round(time.monotonic()-start,2),'prices_0930':s['coverage']['prices'],'published_0930':s['evidence']['counts'].get('published'),'unpublished_0930':s['evidence']['counts'].get('base_not_published'),'market_errors':p['market']['error_count'],'history':p['history'].get('state') if p['history'] else None}
 print(json.dumps(item),flush=True);checks.append(item)
for port in [8787,8790]:
 try: urllib.request.urlopen(f'http://127.0.0.1:{port}/api/dashboard-flow/live',timeout=10)
 except urllib.error.HTTPError as error: assert error.code==401
 else: raise AssertionError('Unauthenticated data allowed')
print('Unauthenticated requests rejected on both dashboards',flush=True)
Path('.codex_tmp/live_flow_20261012/http_verification.json').write_text(json.dumps(checks,indent=2),encoding='utf-8')
