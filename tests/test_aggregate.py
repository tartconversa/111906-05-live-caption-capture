"""业务归并规则：类型内版本比较、冲突保留待核、公开条件。"""
import unittest

from capture.aggregate import Aggregator
from capture.model import canonical, parse_business, sha256_hex


def receipt(event_type, segment, version, payload, event_id=1):
    return {
        'id': event_id,
        'event_type': event_type,
        'segment': segment,
        'version': version,
        'payload': payload,
        'payload_sha256': sha256_hex(canonical(payload)),
        'valid': True,
    }


class AggregateTests(unittest.TestCase):
    def build(self):
        aggregator = Aggregator()
        aggregator.add(receipt('text', 'S', 1, {'text': '草稿', 'final': False}))
        aggregator.add(receipt('text', 'S', 2, {'text': '定稿', 'final': True}))
        aggregator.add(receipt('speaker', 'S', 1, {'speaker': '甲', 'confirmed': True}))
        aggregator.add(receipt('permission', 'S', 1, {'public': True}))
        return aggregator

    def test_final_confirmed_public_is_citable(self):
        state = self.build().state('S')
        self.assertEqual(state['status'], 'citable')
        self.assertEqual(state['public_text'], '定稿')
        self.assertEqual(state['assembled']['speaker'], '甲')

    def test_draft_never_public(self):
        aggregator = self.build()
        aggregator.add(receipt('text', 'S', 3, {'text': '又改了', 'final': False}))
        state = aggregator.state('S')
        self.assertEqual(state['status'], 'draft')
        self.assertIsNone(state['public_text'])

    def test_unconfirmed_speaker_goes_to_review(self):
        aggregator = self.build()
        aggregator.add(receipt('speaker', 'S', 2, {'speaker': '乙', 'confirmed': False}))
        self.assertEqual(aggregator.state('S')['status'], 'pending_review')

    def test_versions_are_not_comparable_across_types(self):
        aggregator = self.build()
        # permission 版本号 1 远小于 speaker 版本号 2，但互不影响。
        aggregator.add(receipt('speaker', 'S', 3, {'speaker': '丙', 'confirmed': True}))
        state = aggregator.state('S')
        self.assertEqual(state['assembled']['speaker'], '丙')
        self.assertTrue(state['assembled']['public'])

    def test_older_version_cannot_overwrite_newer(self):
        aggregator = self.build()
        aggregator.add(receipt('speaker', 'S', 5, {'speaker': '新', 'confirmed': True}))
        result = aggregator.add(
            receipt('speaker', 'S', 3, {'speaker': '旧', 'confirmed': True}))
        self.assertIn(result, ('stale', 'stale_conflict'))
        self.assertEqual(aggregator.state('S')['assembled']['speaker'], '新')

    def test_same_version_conflict_stays_pending(self):
        aggregator = self.build()
        aggregator.add(receipt('speaker', 'S', 2, {'speaker': '乙', 'confirmed': True}))
        result = aggregator.add(
            receipt('speaker', 'S', 2, {'speaker': '丙', 'confirmed': True}))
        self.assertEqual(result, 'conflict')
        state = aggregator.state('S')
        self.assertTrue(state['conflicted']['speaker'])
        self.assertEqual(state['status'], 'pending_review')
        self.assertIsNone(state['public_text'])

    def test_revoking_permission_keeps_text_in_state(self):
        aggregator = self.build()
        aggregator.add(receipt('permission', 'S', 2, {'public': False}))
        state = aggregator.state('S')
        self.assertEqual(state['status'], 'publication_withdrawn')
        self.assertEqual(state['assembled']['text'], '定稿')
        self.assertIsNone(state['public_text'])

    def test_parse_requires_typed_fields(self):
        parsed = parse_business('text', '{"segment": "S", "version": 1, '
                                        '"text": "x", "final": "yes"}')
        self.assertFalse(parsed['ok'])
        self.assertIn('invalid_field:final', parsed['errors'])
        parsed = parse_business('speaker', '{"segment": "S", "version": 1}')
        self.assertFalse(parsed['ok'])
        self.assertIn('missing_field:speaker', parsed['errors'])

    def test_parse_transport_id_does_not_create_version(self):
        parsed = parse_business('text', '{"id": 99, "segment": "S", "version": 2, '
                                        '"text": "x", "final": true}')
        self.assertTrue(parsed['ok'])
        self.assertEqual(parsed['version'], 2)


if __name__ == '__main__':
    unittest.main()
