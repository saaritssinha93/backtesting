import os,urllib.request,json,time,base64,gzip
headers={'Authorization':'Basic '+base64.b64encode((os.environ['LOG_DASH_USER']+':'+os.environ['LOG_DASH_PASS']).encode()).decode(),'Accept-Encoding':'gzip'}
for port in (8787,8790):
 start=time.monotonic()
 with urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{port}/api/dashboard-flow/live?date=2026-10-12',headers=headers),timeout=90) as response:
  raw=response.read();p=json.loads(gzip.decompress(raw) if response.headers.get('Content-Encoding')=='gzip' else raw)
 selected=[s for s in p['slots'] if s['time'] in ('09:20','09:30','10:00')]
 values=[next(r for r in s['rows'] if r['symbol']=='360ONE') for s in selected]
 assert len({r['session_open'] for r in values})==1 and all(r['gap_open_pct'] is None for r in values)
 assert all(r['session_open']>0 for r in values)
 print(json.dumps({'port':port,'http':response.status,'seconds':round(time.monotonic()-start,2),'symbol':'360ONE','session_open':values[0]['session_open'],'verified_gap':values[0]['gap_open_pct'],'opening_source_time':values[0]['opening_source_time'],'day_constant':True}),flush=True)
