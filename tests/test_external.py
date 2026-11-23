"""外部引用与许可冲突的账本测试。"""
import json
import os
import tempfile
import threading
import unittest

from capture.ledger import Ledger
from capture.sse import SSEEvent
from capture.store import Store


def cleanup(path):
    for suffix in ('', '-wal', '-shm'):
        try:
            os.unlink(path + suffix)
        except FileNotFoundError:
            pass


class ExternalExportTests(unittest.TestCase):
    def setUp(self):
        from capture.client import SourceClient
        from capture.service import CaptureService
        from source.app import App
        from source.common import serve

        self.app = App()
        self.server = serve(self.app, 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        base = 'http://127.0.0.1:%d' % self.server.server_port
        self.path = tempfile.mktemp(suffix='.db')
        self.store = Store(self.path)
        self.service = CaptureService(
            self.store, SourceClient(base, read_timeout=0.2), talk='TALK-1',
            backoff=(0.01,))
        self.service.run_once()
        self.ledger = Ledger(self.store, 'TALK-1')

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.store.close()
        cleanup(self.path)

    def test_external_export_is_registered_as_unverified_pending(self):
        self.service.client.post_editor('exports', {
            'reference': 'MEDIA-7',
            'segment': 'S-501',
            'text_version': 2,
            'speaker_version': 1,
            'permission_version': 1,
        })
        imported = self.service.import_external_withdrawals()
        self.assertEqual(imported['exports'], ['MEDIA-7'])
        item = self.store.export('MEDIA-7')
        self.assertEqual(item['origin'], 'editor')
        self.assertEqual(item['state'], 'affected')
        pending = self.ledger.pending()
        self.assertIn('MEDIA-7', [entry['reference'] for entry in pending])
        reasons = {entry['reference']: entry['reasons'] for entry in pending}
        self.assertIn('external_export_unverified', reasons['MEDIA-7'])

    def test_external_export_picks_up_later_correction(self):
        self.service.client.post_editor('exports', {
            'reference': 'MEDIA-8', 'segment': 'S-501',
            'text_version': 2, 'speaker_version': 1,
            'permission_version': 1,
        })
        self.service.import_external_withdrawals()
        self.app.stage = 1
        self.service.run_once()
        self.service.run_once()
        item = self.store.export('MEDIA-8')
        # 外部引用不能凭缺失依据被当成有效；撤许可后必须进入待处理。
        self.assertIn(item['state'], ('affected', 'withdrawn'))
        reasons = json.loads(item['reasons_json'])
        self.assertIn('permission_revoked', reasons)
        self.assertIn('MEDIA-8', [entry['reference']
                                  for entry in self.ledger.pending()])

    def test_import_is_idempotent(self):
        self.service.client.post_editor('exports', {
            'reference': 'MEDIA-9', 'segment': 'S-501',
            'text_version': 2, 'speaker_version': 1,
            'permission_version': 1,
        })
        first = self.service.import_external_withdrawals()
        second = self.service.import_external_withdrawals()
        self.assertEqual(first['exports'], ['MEDIA-9'])
        self.assertEqual(second['exports'], [])
        self.assertEqual(len(self.store.exports('TALK-1')), 1)


class PermissionConflictTests(unittest.TestCase):
    def test_permission_conflict_is_its_own_reason(self):
        from capture.service import CaptureService

        class Client:
            def stream(self, cursor, stop=None, talk=None):
                return iter(())

        path = tempfile.mktemp(suffix='.db')
        store = Store(path)
        service = CaptureService(store, Client(), talk='TALK-1')
        try:
            events = [
                SSEEvent('text', json.dumps({'segment': 'S', 'version': 1,
                                             'text': '词', 'final': True}),
                         '1', b'a'),
                SSEEvent('speaker', json.dumps({'segment': 'S', 'version': 1,
                                                'speaker': '甲',
                                                'confirmed': True}), '2', b'b'),
                SSEEvent('permission', json.dumps({'segment': 'S', 'version': 1,
                                                   'public': True}), '3', b'c'),
            ]
            for event in events:
                service._ingest_event(event, channel='stream', connection=1)
            export, _ = service.export_citation('S')
            # 同版本、不同许可内容：保留待核，引用被标为许可冲突而非“已撤销”。
            service._ingest_event(
                SSEEvent('permission', json.dumps({'segment': 'S', 'version': 1,
                                                   'public': False}), '4',
                         b'{"segment": "S", "version": 1, "public": false}'),
                channel='stream', connection=1)
            ledger = Ledger(store, 'TALK-1')
            ledger.refresh()
            pending = ledger.pending()
            self.assertEqual(pending[0]['reference'], export['reference'])
            self.assertIn('permission_conflict', pending[0]['reasons'])
            self.assertNotIn('permission_revoked', pending[0]['reasons'])
            self.assertEqual(pending[0]['state'], 'affected')
        finally:
            store.close()
            cleanup(path)


if __name__ == '__main__':
    unittest.main()
