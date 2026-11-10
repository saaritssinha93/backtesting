from datetime import datetime
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
import importlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import Mock, patch

from muhurat_20261108 import dashboard


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_missing_and_invalid_reports_are_not_zero_trade_results(self):
        (self.root/'status.json').write_text('{partial', encoding='utf-8')
        value = dashboard.build_payload(self.root, now=datetime(2026,11,8,14,30,tzinfo=dashboard.IST))
        self.assertIsNone(value['paper_trades'])
        self.assertIsNone(value['status'])
        self.assertEqual(len(value['read_errors']),1)
        self.assertFalse(value['execution_authority'])
        self.assertFalse(value['live_enabled_by_this_setup'])

    def test_expiry_is_ist_date_bound(self):
        for stamp, expected in [(datetime(2026,11,8,23,59,tzinfo=dashboard.IST),'TODAY_ONLY'),
                                (datetime(2026,11,9,0,0,tzinfo=dashboard.IST),'EXPIRED'),
                                (datetime(2026,11,7,23,59,tzinfo=dashboard.IST),'NOT_TODAY')]:
            self.assertEqual(dashboard.build_payload(self.root,now=stamp)['date_status'],expected)

    def test_allowlist_rejects_traversal_and_private_access_file(self):
        for name in ['../status.json','status.json/../plan.json','dashboard_access.json','%2e%2e/status.json']:
            with self.assertRaises(ValueError):dashboard.read_artifact(self.root,name)

    def test_symlink_and_oversized_artifact_rejected(self):
        (self.root/'status.json').write_text('{}',encoding='utf-8')
        with patch.object(Path,'is_symlink',return_value=True):
            with self.assertRaises(ValueError):dashboard.read_artifact(self.root,'status.json')
        with patch.object(dashboard,'MAX_BYTES',1):
            with self.assertRaises(ValueError):dashboard.read_artifact(self.root,'status.json')

    def test_credentials_removed_from_nested_json_and_csv(self):
        (self.root/'status.json').write_text(json.dumps({'state':'WAITING','auth':{'healthy_count':2,
            'access_token':'secret'},'apps':[{'api_key':'key','healthy':True}]}),encoding='utf-8')
        (self.root/'paper_trades.csv').write_text('symbol,net_pnl_rs,password\nABC,25,secret\n',encoding='utf-8')
        value=dashboard.build_payload(self.root)
        self.assertEqual(value['status'],{'state':'WAITING','auth':{'healthy_count':2},'apps':[{'healthy':True}]})
        self.assertEqual(value['paper_trades'],[{'symbol':'ABC','net_pnl_rs':'25'}])

    def test_wrong_session_evidence_is_rejected(self):
        (self.root/'status.json').write_text('{"session_date":"2026-11-06"}',encoding='utf-8')
        (self.root/'paper_trades.csv').write_text('day,symbol\n2026-11-06,ABC\n',encoding='utf-8')
        value=dashboard.build_payload(self.root)
        self.assertIsNone(value['status'])
        self.assertIsNone(value['paper_trades'])
        self.assertEqual(len(value['read_errors']),2)

    def test_import_does_not_start_threads_processes_or_servers(self):
        with patch('subprocess.Popen',side_effect=AssertionError('process start')), \
             patch('threading.Thread.start',side_effect=AssertionError('thread start')), \
             patch('http.server.ThreadingHTTPServer.__init__',side_effect=AssertionError('server start')):
            importlib.reload(dashboard)

    def test_automatic_dashboard_will_not_start_on_next_day(self):
        with patch.object(dashboard,'local_now',return_value=datetime(2026,11,9,tzinfo=dashboard.IST)), \
             patch.object(dashboard,'ThreadingHTTPServer',side_effect=AssertionError('server start')):
            self.assertEqual(dashboard.main(['--no-browser']),0)

    def test_expiry_watcher_releases_server_without_waiting_for_user(self):
        server, stop = Mock(), Mock()
        stop.wait.return_value = False
        with patch.object(dashboard,'local_now',return_value=datetime(2026,11,9,tzinfo=dashboard.IST)):
            dashboard._expiry_watch(server, stop)
        stop.wait.assert_called_once_with(15)
        server.shutdown.assert_called_once_with()


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=TemporaryDirectory()
        cls.root=Path(cls.tmp.name)
        (cls.root/'status.json').write_text('{"state":"WAITING_FOR_SESSION"}',encoding='utf-8')
        (cls.root/'dashboard_access.json').write_text('{"url":"private"}',encoding='utf-8')
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),dashboard.MuhuratHandler)
        cls.server.api_token='test-token'
        cls.server.username=cls.server.password=''
        cls.server.muhurat_root=cls.root
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown();cls.server.server_close();cls.thread.join(timeout=2)
        cls.tmp.cleanup()

    def request(self,path,method='GET'):
        connection=HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        connection.request(method,path)
        response=connection.getresponse()
        result=response.status,dict(response.getheaders()),response.read()
        connection.close()
        return result

    def test_authenticated_api_and_unauthenticated_denial(self):
        self.assertEqual(self.request('/api/muhurat')[0],401)
        status,_,body=self.request('/api/muhurat?token=test-token')
        self.assertEqual(status,200)
        self.assertEqual(json.loads(body)['status']['state'],'WAITING_FOR_SESSION')

    def test_every_write_route_is_forbidden(self):
        for path in ['/api/restart','/api/muhurat','/']:
            for method in ['POST','PUT','PATCH','DELETE']:
                self.assertEqual(self.request(path+'?token=test-token',method)[0],403)

    def test_route_traversal_and_private_artifacts_are_unavailable(self):
        for name in ['../status.json','%2e%2e%2fstatus.json','dashboard_access.json','other.json']:
            self.assertEqual(self.request('/muhurat-artifact/'+name+'?token=test-token')[0],404)

    def test_root_redirect_and_muhurat_render(self):
        status,headers,_=self.request('/?token=test-token')
        self.assertEqual(status,302)
        self.assertEqual(headers['Location'],'/muhurat?token=test-token')
        status,headers,body=self.request('/muhurat?token=test-token')
        self.assertEqual(status,200)
        self.assertIn(b'Not enabled by this setup',body)
        self.assertEqual(headers['Referrer-Policy'],'no-referrer')


if __name__=='__main__':
    unittest.main()
