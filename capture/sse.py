"""SSE 字节流解析：兼容任意字节切分、CRLF、多行 data 与注释心跳。

按字节切行，确保多字节 UTF-8 字符被拆开时不会误判，并原样保留每条事件的
传输字节（raw），作为来源凭据。传输事件 ID 只用于接续，不参与业务版本判断。
"""
import codecs

_CR = 0x0D
_LF = 0x0A


class SSEEvent:
    __slots__ = ('event', 'data', 'last_event_id', 'raw')

    def __init__(self, event, data, last_event_id, raw):
        self.event = event
        self.data = data
        self.last_event_id = last_event_id
        self.raw = raw

    def __repr__(self):
        return 'SSEEvent(event=%r, last_event_id=%r, data=%r)' % (
            self.event, self.last_event_id, self.data)


class SSEDecoder:
    """增量解码器。

    feed() 接受任意字节片段，返回本次已完整的 SSEEvent 列表。
    """

    def __init__(self):
        self._buffer = bytearray()
        self._block = bytearray()
        self._data = []
        self._event = ''
        self._last_event_id = ''
        # 规范要求：事件块的 id 字段只在块内生效；块结束时保留最近值，
        # 但块内没有 id 时，该事件仍沿用上一条的续传位置。
        self._block_id = None
        self._started = False
        self._text_errors = 0
        self._heartbeats = 0

    @property
    def text_errors(self):
        return self._text_errors

    @property
    def heartbeats(self):
        """只含注释的块数（心跳），不产生业务事件。"""
        return self._heartbeats

    def feed(self, chunk):
        if chunk:
            self._buffer.extend(chunk)
        events = []
        while True:
            line, separator = self._take_line()
            if separator is None:
                break
            self._block.extend(line)
            self._block.extend(separator)
            if not line:
                event = self._dispatch()
                if event is not None:
                    events.append(event)
            else:
                self._line(line)
        return events

    def _take_line(self):
        buffer = self._buffer
        if not self._started:
            self._started = True
            # 规范允许流以 UTF-8 BOM 开头。
            if buffer.startswith(b'\xef\xbb\xbf'):
                del buffer[:3]
        for index, byte in enumerate(buffer):
            if byte == _CR or byte == _LF:
                # 尾部单个 CR 可能是被切断的 CRLF，等后续字节再判定。
                if byte == _CR and index + 1 == len(buffer):
                    return None, None
                line = bytes(buffer[:index])
                if byte == _CR and index + 1 < len(buffer) and buffer[index + 1] == _LF:
                    separator = b'\r\n'
                elif byte == _CR and index + 1 == len(buffer):
                    separator = b'\r'
                else:
                    separator = bytes([byte])
                del buffer[:index + len(separator)]
                return line, separator
        return None, None

    def _dispatch(self):
        raw = bytes(self._block)
        self._block = bytearray()
        if not self._data:
            # 只有注释（心跳）的块不产生业务事件。
            self._heartbeats += 1
            self._block_id = None
            self._reset_fields()
            return None
        data = '\n'.join(self._data)
        if self._block_id is not None:
            self._last_event_id = self._block_id
        event = SSEEvent(self._event or 'message', data, self._last_event_id, raw)
        self._block_id = None
        self._reset_fields()
        return event

    def _reset_fields(self):
        self._data = []
        self._event = ''

    def _line(self, raw_line):
        if raw_line.startswith(b':'):
            return
        try:
            line = codecs.decode(raw_line, 'utf-8', 'strict')
        except UnicodeDecodeError:
            self._text_errors += 1
            line = raw_line.decode('utf-8', 'replace')
        name, colon, value = line.partition(':')
        if value.startswith(' '):
            value = value[1:]
        if name == 'data':
            self._data.append(value)
        elif name == 'event':
            self._event = value
        elif name == 'id':
            # NUL 在 id 中非法；空 id 不改变续传位置，但会清除块内待定 id。
            if '\x00' not in value:
                self._block_id = value if value else None
        # retry 与未知字段按规范忽略。


def decode_all(payload):
    """把完整响应体一次性解码，供测试与离线核验使用。"""
    return SSEDecoder().feed(payload)
