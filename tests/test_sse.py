"""SSE 增量解析的传输边界测试。"""
import unittest

from capture.sse import SSEDecoder, decode_all


def samples():
    yield b'event: text\r\ndata: {"a": 1}\r\n\r\n', 1
    yield b'id: 7\nevent: text\ndata: {"a": 1}\n\n', 1
    yield ': 心跳\r\n\r\nid: 8\r\ndata: {}\r\n\r\n'.encode('utf-8'), 1


class DecoderTests(unittest.TestCase):
    def test_split_at_every_byte(self):
        for payload, count in samples():
            for cut in range(1, len(payload)):
                decoder = SSEDecoder()
                events = decoder.feed(payload[:cut]) + decoder.feed(payload[cut:])
                self.assertEqual(len(events), count, (payload, cut))
                self.assertNotIn(b'\xef\xbf\xbd', events[-1].raw)

    def test_crlf_split_between_cr_and_lf(self):
        decoder = SSEDecoder()
        self.assertEqual(decoder.feed(b'id: 5\r'), [])
        events = decoder.feed(b'\ndata: {"x": 1}\r\n\r\n')
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].last_event_id, '5')

    def test_multibyte_character_split(self):
        payload = 'data: {"text": "嘉宾"}\n\n'.encode('utf-8')
        for cut in range(len(payload)):
            decoder = SSEDecoder()
            events = decoder.feed(payload[:cut]) + decoder.feed(payload[cut:])
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].data, '{"text": "嘉宾"}')

    def test_multiline_data_joined_with_newline(self):
        events = decode_all(b'data: {"a":\ndata: 1}\n\n')
        self.assertEqual(events[0].data, '{"a":\n1}')

    def test_heartbeat_without_data_is_not_event(self):
        decoder = SSEDecoder()
        self.assertEqual(decoder.feed(b': ping\r\n\r\n'), [])
        self.assertEqual(decoder.heartbeats, 1)

    def test_id_only_applies_to_its_block(self):
        events = decode_all(
            b'id: 3\nevent: text\ndata: {}\n\n: hb\n\ndata: {}\n\n')
        # 第二个块没有 id，沿用上一条的续传位置，而不是把心跳块当成新位置。
        self.assertEqual([event.last_event_id for event in events], ['3', '3'])
        self.assertEqual([event.event for event in events], ['text', 'message'])

    def test_empty_id_does_not_change_position(self):
        events = decode_all(b'id: 4\ndata: {}\n\nid\ndata: {}\n\n')
        self.assertEqual(events[-1].last_event_id, '4')

    def test_id_after_data_in_same_block_still_applies(self):
        events = decode_all(b'data: {"x": 1}\nid: 6\n\n')
        self.assertEqual(events[0].last_event_id, '6')


if __name__ == '__main__':
    unittest.main()
