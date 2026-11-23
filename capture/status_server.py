"""最小 HTTP 查询服务：只读当前采集状态，供编辑台轮询。

与采集进程分开运行，读取同一个 SQLite 数据库（WAL 允许多读一写）。
只暴露只读视图，不提供任何改变状态的操作；导入编辑台撤下通知由采集进程负责。
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .ledger import Ledger
from .model import canonical
from .service import CaptureService


def build_state(store, talk, service=None):
    if service is None:
        service = CaptureService(store, client=None, talk=talk, auto_snapshot=False)
    return service.observe()


def build_exports(store, talk):
    ledger = Ledger(store, talk)
    items = []
    for row in store.exports(talk):
        item = dict(row)
        item['versions'] = json.loads(item.pop('versions_json') or '{}')
        item['payload_sha256'] = json.loads(item.pop('payload_sha_json') or '{}')
        item['evidence'] = json.loads(item.pop('receipts_json') or '{}')
        item['reasons'] = json.loads(item.pop('reasons_json') or '[]')
        item.pop('acked_reasons_json', None)
        items.append(item)
    return {
        'talk': talk,
        'exports': items,
        'pending': ledger.pending(),
        'external_withdrawals': store.external_withdrawals(),
        'withdrawals': store.withdrawals(),
    }


class StateHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    store = None
    talk = 'TALK-1'

    def log_message(self, *args):
        pass

    def _send(self, value, status=200):
        payload = canonical(value).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        # 每个请求线程各自持有 SQLite 连接；处理完即释放，避免线程堆积连接。
        try:
            self._route()
        finally:
            self.store.close()

    def _route(self):
        path = urlparse(self.path).path
        if path in ('/health', '/'):
            return self._send({'status': 'ok', 'talk': self.talk})
        if path == '/state':
            return self._send(build_state(self.store, self.talk))
        if path == '/exports':
            return self._send(build_exports(self.store, self.talk))
        if path == '/coverage':
            service = CaptureService(self.store, client=None, talk=self.talk,
                                     auto_snapshot=False)
            return self._send(service.coverage())
        return self._send({'error': 'not_found'}, 404)


def serve_state(store, talk='TALK-1', host='127.0.0.1', port=8766):
    handler = type('BoundStateHandler', (StateHandler,),
                   {'store': store, 'talk': talk})
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server
