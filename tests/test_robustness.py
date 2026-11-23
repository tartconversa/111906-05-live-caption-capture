"""来源异常与窗口边界的补充测试：日志、无效事件、重复投递。"""
import json
import os
import tempfile
import threading
import unittest

from source.app import App
from source.common import serve

from capture.client import SourceClient
from capture.service import CaptureService
from capture.store import Store


class InvalidEventTests(unittest.TestCase):
    def setUp(self):
        self.app = App()
        self.server = serve(self.app, 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base = 'http://127.0.0.1:%d' % self.server.server_port
        self.db_path = tempfile.mktemp(suffix='.db')
        self.store = Store(self.db_path)
        self.service = CaptureService(
            self.store, SourceClient(self.base, read_timeout=0.2),
            talk='TALK-1', backoff=(0.01,))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.store.close()
        for suffix in ('', '-wal', '-shm'):
            try:
                os.unlink(self.db_path + suffix)
            except FileNotFoundError:
                pass

    def test_unknown_type_is_stored_but_never_citable(self):
        from capture.sse import SSEEvent

        event = SSEEvent('translation', json.dumps({
            'segment': 'S-900', 'version': 1, 'text': '译文'}), '900', b'raw')
        self.service._ingest_event(
            event, channel='stream', connection=1)
        rows = self.store.receipts_for_segment('TALK-1', 'S-900')
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]['valid'])
        self.assertIn('unknown_event_type', rows[0]['errors_json'])
        observed = self.service.observe()
        self.assertEqual(observed['citable'], [])
        self.assertNotIn('S-900', [item['segment']
                                   for item in observed['pending_review']
                                   + observed['drafts']])

    def test_malformed_json_keeps_raw_bytes_and_is_not_counted(self):
        from capture.sse import SSEEvent

        raw = b'id: 901\r\nevent: text\r\ndata: {oops\r\n\r\n'
        event = SSEEvent('text', '{oops', '901', raw)
        self.service._ingest_event(event, channel='stream', connection=1)
        rows = self.store.receipts('TALK-1')
        self.assertEqual(rows[0]['raw'], raw)
        self.assertFalse(rows[0]['valid'])
        self.assertEqual(self.store.cursor('TALK-1'), 901)

    def test_transport_id_collision_with_different_bytes_is_kept(self):
        from capture.sse import SSEEvent

        first = SSEEvent('text', '{"segment": "S-1", "version": 1, '
                                 '"text": "a", "final": true}', '5', b'A')
        second = SSEEvent('text', '{"segment": "S-1", "version": 1, '
                                  '"text": "b", "final": true}', '5', b'B')
        self.service._ingest_event(first, channel='stream', connection=1)
        self.service._ingest_event(second, channel='stream', connection=1)
        rows = self.store.receipts('TALK-1')
        self.assertEqual(len(rows), 2)
        state = self.service.observe()
        self.assertEqual(state['pending_review'][0]['segment'], 'S-1')
        self.assertIn('text_version_conflict',
                      state['pending_review'][0]['reasons'])

    def test_repeated_snapshot_ingest_is_idempotent(self):
        self.service.run_once(max_events=4)
        self.app.stage = 1
        snapshot = self.service.fetch_snapshot()
        first = self.service.ingest_snapshot(snapshot)
        second = self.service.ingest_snapshot(snapshot)
        self.assertGreater(first['inserted'], 0)
        self.assertEqual(second['inserted'], 0)
        self.assertEqual(second['duplicate'], len(snapshot['events']))

    def test_unreachable_source_is_recorded_and_retried(self):
        service = CaptureService(
            self.store, SourceClient('http://127.0.0.1:9', timeout=0.5,
                                     read_timeout=0.2),
            talk='TALK-1', backoff=(0.0,))
        results = service.run(max_rounds=2)
        self.assertEqual([item['reason'] for item in results],
                         ['interrupted', 'interrupted'])
        kinds = [item['kind'] for item in self.store.incidents('TALK-1')]
        self.assertEqual(kinds.count('stream_interrupted'), 2)
        self.assertEqual(self.store.cursor('TALK-1'), 0)


class StubClient:
    """构造窗口失效但归档也覆盖不到的边界。"""

    def __init__(self, floor, covers=8, events=None):
        self.floor = floor
        self.covers = covers
        self.events = events or []

    def stream(self, cursor, stop=None, talk=None):
        from capture.client import ReplayExpired
        raise ReplayExpired(self.floor, '/snapshot/TALK-1', b'{}',
                            '0' * 64)

    def snapshot(self, talk=None):
        import hashlib
        body = json.dumps(
            {'talk': 'TALK-1', 'covers_through': self.covers,
             'events': self.events}, ensure_ascii=False, sort_keys=True
        ).encode('utf-8')
        return {'talk': 'TALK-1', 'covers_through': self.covers,
                'events': self.events, 'body': body,
                'body_sha256': hashlib.sha256(body).hexdigest()}


class ReplayWindowTests(unittest.TestCase):
    def setUp(self):
        self.paths = []

    def tearDown(self):
        for path in self.paths:
            for suffix in ('', '-wal', '-shm'):
                try:
                    os.unlink(path + suffix)
                except FileNotFoundError:
                    pass

    def store(self):
        path = tempfile.mktemp(suffix='.db')
        self.paths.append(path)
        return Store(path)

    def test_lost_window_is_explicit_gap_and_cursor_advances(self):
        store = self.store()
        service = CaptureService(store, StubClient(floor=20), talk='TALK-1',
                                 backoff=(0.0,))
        try:
            result = service.run_once()
            self.assertEqual(result['reason'], 'replay_expired')
            coverage = service.coverage()
            self.assertIn({'from': 9, 'to': 19, 'reason': 'replay_window_lost'},
                          coverage['missing'])
            self.assertIn('不能据此断定没有讲话', coverage['interpretation'])
            self.assertGreaterEqual(store.cursor('TALK-1'), 20)
            kinds = [item['kind'] for item in store.incidents('TALK-1')]
            self.assertIn('replay_window_lost', kinds)
        finally:
            store.close()

    def test_incomplete_snapshot_body_is_a_gap_not_silence(self):
        store = self.store()
        events = [{'id': 1, 'type': 'text', 'segment': 'S-1', 'version': 1,
                   'text': '开场', 'final': True}]
        service = CaptureService(store, StubClient(floor=8, covers=8,
                                                    events=events),
                                 talk='TALK-1', backoff=(0.0,))
        try:
            service.run_once()
            coverage = service.coverage()
            self.assertIn({'from': 2, 'to': 8,
                           'reason': 'snapshot_incomplete'},
                          coverage['missing'])
            self.assertIn('仍待补取', coverage['interpretation'])
            self.assertIn('snapshot_incomplete',
                          [item['kind'] for item in store.incidents('TALK-1')])
        finally:
            store.close()

    def test_snapshot_gap_records_keep_their_originating_reason(self):
        store = self.store()
        try:
            store.add_gap('TALK-1', 5, 6, 'replay_expired', {})
            # 后续部分补领只覆盖了 5，6 仍待补，原因不能被重写成补领来源。
            store.resolve_gaps('TALK-1', [5], reason='snapshot_recovery')
            self.assertEqual(store.gaps('TALK-1')[0]['reason'], 'replay_expired')
        finally:
            store.close()


if __name__ == '__main__':
    unittest.main()
