"""针对真实来源连接的采集集成测试。

全部通过实际 HTTP/SSE 连接、窗口失效与归档补领观察结果，不读取内部状态代替传输。
"""
import json
import os
import tempfile
import threading
import unittest

from source.app import App
from source.common import serve

from capture.client import SourceClient
from capture.ledger import Ledger
from capture.service import CaptureService
from capture.store import Store


class CaptureCase(unittest.TestCase):
    def setUp(self):
        self.app = App()
        self.server = serve(self.app, 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base = 'http://127.0.0.1:%d' % self.server.server_port
        self.db_path = tempfile.mktemp(suffix='.db')
        self.store = Store(self.db_path)
        self.service = CaptureService(
            self.store, SourceClient(self.base, read_timeout=0.2),
            talk='TALK-1', backoff=(0.01,), auto_snapshot=True)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.store.close()
        for suffix in ('', '-wal', '-shm'):
            try:
                os.unlink(self.db_path + suffix)
            except FileNotFoundError:
                pass

    def capture(self, **kwargs):
        return self.service.run_once(**kwargs)

    def observe(self):
        return self.service.observe()


class LiveStreamTests(CaptureCase):
    def test_stream_delivers_before_connection_closes(self):
        seen = []
        result = self.capture(max_events=3, on_event=lambda event: seen.append(event))
        self.assertEqual(result['events'], 3)
        self.assertEqual(len(seen), 3)
        # 连接尚未结束时已经拿到并持久化了部分内容。
        self.assertEqual(self.store.cursor('TALK-1'), 3)
        self.assertTrue(self.store.receipts('TALK-1'))

    def test_multiline_data_and_heartbeat_do_not_break_events(self):
        self.capture()
        receipts = self.store.receipts('TALK-1')
        self.assertEqual([row['transport_id'] for row in receipts], list(range(1, 9)))
        for row in receipts:
            self.assertNotIn(b'\xef\xbf\xbd', row['raw'] or b'')
            self.assertIn(b'\r\ndata: ', row['raw'])

    def test_citable_only_after_final_confirmed_public(self):
        self.capture()
        result = self.observe()
        segments = {item['segment']: item for item in result['citable']}
        self.assertIn('S-501', segments)
        self.assertEqual(segments['S-501']['text'], '出发前先看天气。')
        self.assertNotIn('S-502', segments)  # 说话人未确认
        self.assertNotIn('S-503', segments)  # 未定稿
        pending = {item['segment']: item for item in result['pending_review']}
        self.assertIn('S-502', pending)
        self.assertIn('speaker_unconfirmed', pending['S-502']['reasons'])

    def test_draft_is_separate_from_confirmed_text(self):
        self.capture(max_events=1)
        result = self.observe()
        self.assertEqual(result['drafts'][0]['text'], '出发先看天气')
        self.assertEqual(result['citable'], [])

    def test_stop_and_resume_keeps_cursor_and_byte_provenance(self):
        self.capture(max_events=2)
        first = self.store.receipts('TALK-1')
        self.service.stop()
        self.service.clear_stop()
        self.capture()
        receipts = self.store.receipts('TALK-1')
        # 第二条连接从持久游标继续，没有重复收取已采事件。
        self.assertEqual([row['transport_id'] for row in receipts],
                         list(range(1, 9)))
        self.assertEqual(len({row['raw_sha256'] for row in receipts}), 8)
        self.assertEqual(first[0]['raw_sha256'], receipts[0]['raw_sha256'])

    def test_correction_after_export_lists_affected_citation(self):
        self.capture()
        export, created = self.service.export_citation('S-501')
        self.assertTrue(created)
        self.assertEqual(export['speaker'], '甲嘉宾')
        self.app.stage = 1
        self.capture()  # 先收到 409，再按快照与后续事件补齐
        ledger = Ledger(self.store, 'TALK-1')
        # 连接尚未关闭时，撤销许可一到达就已把引用标为受影响。
        self.assertEqual([item['reference'] for item in ledger.pending()],
                         [export['reference']])
        self.capture()
        pending = self.observe()['pending_exports']
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]['reference'], export['reference'])
        self.assertIn('speaker_corrected', pending[0]['reasons'])
        self.assertEqual(pending[0]['state'], 'withdrawn')

    def test_export_invalidated_mid_connection_before_close(self):
        self.capture()
        export, _ = self.service.export_citation('S-501')
        seen = []

        def watch(event):
            # 每收到一条事件后立即查看引用效力，不等待连接关闭。
            rows = {item['reference']: item['state']
                    for item in Ledger(self.store, 'TALK-1').pending()}
            seen.append(rows)

        self.app.stage = 1
        self.capture(on_event=watch)
        self.assertTrue(seen)
        # 说话人更正（id 9）到达时引用就已受影响，无需等撤许可或连接关闭。
        self.assertEqual(seen[0].get(export['reference']), 'affected')
        self.assertIn('speaker_corrected', json.dumps(
            Ledger(self.store, 'TALK-1').pending(), ensure_ascii=False))
        # 撤许可（id 10）到达后，同一引用转为 withdrawn，全程未关闭连接。
        self.assertIn('withdrawn', [batch.get(export['reference'])
                                   for batch in seen])
        self.assertEqual(seen[-1].get(export['reference']), 'withdrawn')

    def test_revoked_permission_blocks_new_export_but_keeps_raw_text(self):
        self.capture()
        self.app.stage = 1
        self.capture()
        self.capture()
        with self.assertRaises(Exception) as context:
            self.service.export_citation('S-501')
        self.assertEqual(getattr(context.exception, 'reasons'), ['permission_revoked'])
        raw = self.store.receipts_for_segment('TALK-1', 'S-501')
        self.assertTrue(any(row['transport_id'] == 4 for row in raw))
        self.assertTrue(all(row['raw'] for row in raw))
        result = self.observe()
        restricted = {item['segment']: item for item in result['restricted']}
        self.assertTrue(restricted['S-501']['text_withheld'])
        exported = json.dumps(result, ensure_ascii=False)
        self.assertNotIn('出发前先看天气', exported)


class ReplayWindowTests(CaptureCase):
    def test_expired_replay_recovers_snapshot_and_describes_coverage(self):
        self.capture(max_events=4)
        self.assertEqual(self.store.cursor('TALK-1'), 4)
        self.app.stage = 1
        result = self.capture()
        self.assertEqual(result['reason'], 'replay_expired')
        recovered = result['recovered']
        self.assertEqual(recovered['covers_through'], 8)
        self.assertTrue(recovered['complete'])
        coverage = self.observe()['coverage']
        self.assertEqual(coverage['snapshot_covers_through'], 8)
        self.assertFalse(coverage['same_moment_as_stream'])
        self.assertEqual(coverage['ranges'], [
            {'from': 1, 'to': 4, 'source': 'stream'},
            {'from': 5, 'to': 8, 'source': 'snapshot'},
        ])
        self.assertEqual(coverage['missing'], [])
        self.assertTrue(any('不等于没有讲话' in note
                            for note in [coverage['interpretation']]))

    def test_snapshot_does_not_roll_back_speaker_correction(self):
        self.capture()
        self.app.stage = 1
        self.capture()   # 409 → 快照（水位 8，不含 9-11 的更正）
        self.capture()   # 9-11 实时到达，含 S-501 说话人更正
        state = {item['segment']: item for item in self.observe()['citable']}
        self.assertEqual(state['S-502']['speaker'], '乙嘉宾')
        # 再次取回早期快照也不得覆盖已收到的更正。
        self.service.fetch_snapshot()
        self.service.ingest_snapshot(self.service.fetch_snapshot())
        result = self.observe()
        restricted = {item['segment']: item for item in result['restricted']}
        self.assertIn('S-501', restricted)
        self.assertEqual(restricted['S-501']['versions']['speaker'], 2)

    def test_gap_is_not_reported_as_silence(self):
        self.capture(max_events=4)
        self.app.stage = 1
        result = self.capture()
        coverage = result['recovered']
        self.assertEqual(coverage['event_ids'], list(range(1, 9)))
        ranges = self.observe()['coverage']['ranges']
        self.assertNotIn(None, [item['source'] for item in ranges])
        # S-503 只存在于推送而非归档；它不是“没有讲话”。
        drafts = {item['segment'] for item in self.observe()['drafts']}
        self.assertIn('S-503', drafts)

    def test_second_stage_snapshot_extends_declared_coverage(self):
        self.capture(max_events=4)
        self.app.stage = 1
        self.capture()
        self.capture()
        self.app.stage = 2
        snapshot = self.service.fetch_snapshot()
        self.assertEqual(snapshot['covers_through'], 11)
        coverage = self.observe()['coverage']
        self.assertEqual(coverage['snapshot_covers_through'], 11)
        self.assertEqual(coverage['missing'], [])


class LedgerTests(CaptureCase):
    def test_exports_are_frozen_and_relisted_until_acknowledged(self):
        self.capture()
        export, _ = self.service.export_citation('S-501')
        self.assertEqual(export['versions_json'],
                         json.dumps({'permission': 1, 'speaker': 1, 'text': 2},
                                    sort_keys=True))
        ledger = Ledger(self.store, 'TALK-1')
        self.assertEqual(ledger.pending(), [])
        self.app.stage = 1
        self.capture()
        self.capture()
        self.assertEqual(len(ledger.pending()), 1)
        ledger.acknowledge(export['reference'], '已联系编辑撤下')
        self.assertEqual(ledger.pending(), [])
        # 已冻结的引用文字不随新状态改写。
        stored = self.store.export(export['reference'])
        self.assertEqual(stored['text'], '出发前先看天气。')
        self.assertEqual(stored['speaker'], '甲嘉宾')
        self.assertEqual(stored['state'], 'withdrawn')

    def test_duplicate_export_is_idempotent(self):
        self.capture()
        first, created = self.service.export_citation('S-501')
        second, again = self.service.export_citation('S-501')
        self.assertTrue(created)
        self.assertFalse(again)
        self.assertEqual(first['reference'], second['reference'])
        self.assertEqual(len(self.store.exports('TALK-1')), 1)

    def test_withdrawal_is_delivered_to_editor_receiver_and_kept_locally(self):
        self.capture()
        export, _ = self.service.export_citation('S-501')
        withdrawal = self.service.ledger.add_withdrawal(
            export['reference'], '编辑确认该引文不再使用', 'WDR-TEST-1')
        sent = self.service.flush_outbox()
        self.assertEqual(sent[0]['reference'], 'WDR-TEST-1')
        history = self.service.client.editor_history('withdrawals')
        self.assertIn('WDR-TEST-1', history)
        self.assertEqual(withdrawal['reason'], '编辑确认该引文不再使用')
        self.assertEqual(self.store.outbox('sent')[0]['status'], 'sent')

    def test_external_withdrawal_is_linked_and_refreshed(self):
        self.capture()
        export, _ = self.service.export_citation('S-501')
        SourceClient(self.base).post_editor('withdrawals', {
            'reference': 'WDR-EXT-9',
            'export_reference': export['reference'],
            'reason': '外部媒体已撤回',
        })
        imported = self.service.import_external_withdrawals()
        self.assertEqual(imported['withdrawals'], ['WDR-EXT-9'])
        self.assertEqual(imported['exports'], [])
        self.assertEqual(self.service.ledger.pending()[0]['reference'],
                         export['reference'])
        self.assertEqual(self.store.external_withdrawal('WDR-EXT-9')
                         ['export_reference'], export['reference'])

    def test_public_export_view_withholds_restricted_text(self):
        self.capture()
        self.app.stage = 1
        self.capture()
        self.capture()
        result = self.observe()
        public = json.dumps(
            {'citable': result['citable'], 'pending_review': result['pending_review'],
             'drafts': result['drafts'], 'restricted': result['restricted']},
            ensure_ascii=False)
        self.assertNotIn('高铁', public.replace('高铁也可抵达', ''))
        # 已获许可且说话人确认的 S-502 可以公开，S-501 撤许可后的原文不可见。
        self.assertIn('高铁也可抵达', public)
        self.assertNotIn('出发前先看天气', public)
        restricted = self.store.receipts_for_segment('TALK-1', 'S-501')
        self.assertTrue(any('出发前先看天气' in (row['data_text'] or '')
                            for row in restricted))


class ProvenanceTests(CaptureCase):
    def test_every_event_keeps_raw_bytes_and_hash(self):
        self.capture()
        receipts = self.store.receipts('TALK-1')
        for row in receipts:
            self.assertTrue(row['raw'])
            self.assertEqual(len(row['raw_sha256']), 64)
            self.assertIn(str(row['transport_id']).encode(), row['raw'])

    def test_snapshot_body_is_archived_without_mutating_source(self):
        self.capture(max_events=4)
        self.app.stage = 1
        self.capture()
        snapshot = self.store.latest_snapshot('TALK-1')
        self.assertEqual(snapshot['covers_through'], 8)
        self.assertEqual(len(snapshot['body_sha256']), 64)
        self.assertTrue(snapshot['event_count'] >= 8)
        body = self.store.snapshot_body(snapshot['id'])
        import hashlib
        self.assertEqual(hashlib.sha256(body['body']).hexdigest(),
                         snapshot['body_sha256'])

    def test_incidents_record_replay_expiry(self):
        self.capture(max_events=4)
        self.app.stage = 1
        self.capture()
        kinds = [item['kind'] for item in self.store.incidents('TALK-1')]
        self.assertIn('replay_expired', kinds)
        self.assertIn('snapshot_fetched',
                      [item['kind'] for item in self.store.audit_log(limit=100)])


class RestartTests(CaptureCase):
    def restart(self):
        """模拟进程停止后重新启动：关闭并重开同一状态库。"""
        self.store.close()
        self.store = Store(self.db_path)
        self.service = CaptureService(
            self.store, SourceClient(self.base, read_timeout=0.2),
            talk='TALK-1', backoff=(0.01,), auto_snapshot=True)
        return self.service

    def test_restart_continues_from_persisted_cursor(self):
        self.capture(max_events=5)
        self.assertEqual(self.store.cursor('TALK-1'), 5)
        self.restart()
        result = self.capture()
        self.assertIn(result['reason'], ('eof', 'replay_expired'))
        receipts = self.store.receipts('TALK-1')
        self.assertEqual([row['transport_id'] for row in receipts[:8]],
                         list(range(1, 9)))
        self.assertEqual(len(self.store.receipts('TALK-1')),
                         len({row['raw_sha256'] for row in self.store.receipts('TALK-1')}))

    def test_restart_after_expiry_uses_snapshot_and_keeps_gap_history(self):
        self.capture(max_events=4)
        self.app.stage = 1
        self.restart()
        result = self.capture()
        self.assertEqual(result['reason'], 'replay_expired')
        coverage = self.observe()['coverage']
        self.assertEqual(coverage['ranges'], [
            {'from': 1, 'to': 4, 'source': 'stream'},
            {'from': 5, 'to': 8, 'source': 'snapshot'},
        ])
        self.assertTrue(coverage['recoveries'])

    def test_export_and_affected_list_survive_restart(self):
        self.capture()
        export, _ = self.service.export_citation('S-501')
        self.app.stage = 1
        self.capture()
        self.capture()
        self.restart()
        ledger = Ledger(self.store, 'TALK-1')
        pending = ledger.pending()
        self.assertEqual([item['reference'] for item in pending],
                         [export['reference']])
        stored = self.store.export(export['reference'])
        self.assertEqual(stored['state'], 'withdrawn')
        self.assertEqual(stored['text'], '出发前先看天气。')

    def test_unacknowledged_withdrawal_outbox_survives_restart(self):
        self.capture()
        export, _ = self.service.export_citation('S-501')
        self.service.ledger.add_withdrawal(export['reference'], '撤回', 'WDR-R-1')
        self.restart()
        item = self.store.outbox_item('withdrawals', 'WDR-R-1')
        self.assertEqual(item['status'], 'pending')
        sent = self.service.flush_outbox()
        self.assertEqual(sent[0]['reference'], 'WDR-R-1')
        self.assertEqual(self.store.outbox_item('withdrawals', 'WDR-R-1')['status'],
                         'sent')


class StatusServerTests(CaptureCase):
    def test_state_is_readable_while_capture_is_writing(self):
        import urllib.request
        from capture.status_server import serve_state

        server = serve_state(self.store, port=0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            self.capture(max_events=4)
            with urllib.request.urlopen(
                    'http://127.0.0.1:%d/state' % server.server_port,
                    timeout=5) as response:
                value = json.loads(response.read())
            self.assertEqual(value['coverage']['cursor'], 4)
            self.assertEqual(value['talk'], 'TALK-1')
            with urllib.request.urlopen(
                    'http://127.0.0.1:%d/exports' % server.server_port,
                    timeout=5) as response:
                exports = json.loads(response.read())
            self.assertEqual(exports['exports'], [])
        finally:
            server.shutdown()
            server.server_close()



class EvidenceTests(CaptureCase):
    def test_evidence_links_receipts_used_by_an_export(self):
        import subprocess
        import sys

        self.capture()
        export, _ = self.service.export_citation('S-501')
        result = subprocess.run(
            [sys.executable, '-m', 'capture', 'evidence', '--db', self.db_path,
             '--talk', 'TALK-1', export['reference']],
            capture_output=True, text=True, timeout=30, cwd=os.getcwd())
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        used = [row['transport_id'] for row in value['receipts']
                if row.get('used_by_export')]
        self.assertEqual(sorted(used), [2, 3, 4])
        self.assertTrue(all('raw' in row for row in value['receipts']))
        self.assertTrue(value['receipts'][0]['restricted'])
        hashes_only = subprocess.run(
            [sys.executable, '-m', 'capture', 'evidence', '--db', self.db_path,
             '--talk', 'TALK-1', export['reference'], '--hashes-only'],
            capture_output=True, text=True, timeout=30, cwd=os.getcwd())
        value = json.loads(hashes_only.stdout)
        self.assertTrue(all('raw' not in row and 'data_text' not in row
                            for row in value['receipts']))


if __name__ == '__main__':
    unittest.main()
