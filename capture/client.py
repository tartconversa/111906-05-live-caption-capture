"""来源 HTTP/SSE 客户端。

- stream(): 边读边产出事件，不等连接关闭；返回 (kind, payload)。
- 409 replay_expired 转为 ReplayExpired，携带来源正文与快照地址。
- snapshot(): 原样保留归档正文，供字节级留痕。
- 编辑台接收方接口（可选）只登记，不参与本产品的有效性判断。
"""
import http.client
import json
import socket
import time
from urllib.parse import urlsplit

from .model import sha256_hex
from .sse import SSEDecoder


class SourceError(RuntimeError):
    pass


class ReplayExpired(SourceError):
    def __init__(self, floor, snapshot, body, body_sha256):
        super().__init__('replay_expired floor=%s snapshot=%s' % (floor, snapshot))
        self.floor = floor
        self.snapshot = snapshot
        self.body = body
        self.body_sha256 = body_sha256


class StreamInterrupted(SourceError):
    def __init__(self, reason, last_event_id):
        super().__init__('stream interrupted: %s' % reason)
        self.reason = reason
        self.last_event_id = last_event_id


class SourceClient:
    def __init__(self, base_url, talk='TALK-1', timeout=10.0, read_timeout=0.5,
                 idle_timeout=60.0):
        parts = urlsplit(base_url)
        if parts.scheme not in ('http', 'https') or not parts.hostname:
            raise ValueError('base_url 必须是 http(s) 地址: %r' % (base_url,))
        self.scheme = parts.scheme
        self.host = parts.hostname
        self.port = parts.port or (443 if parts.scheme == 'https' else 80)
        self.prefix = parts.path.rstrip('/')
        self.talk = talk
        self.timeout = timeout
        self.read_timeout = read_timeout
        self.idle_timeout = idle_timeout

    # ---- 底层 ----

    def _connection(self, timeout):
        if self.scheme == 'https':
            return http.client.HTTPSConnection(self.host, self.port, timeout=timeout)
        return http.client.HTTPConnection(self.host, self.port, timeout=timeout)

    def _request(self, method, path, body=None):
        conn = self._connection(self.timeout)
        headers = {'Accept': 'application/json', 'Connection': 'close'}
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        try:
            conn.request(method, self.prefix + path, body=data, headers=headers)
            response = conn.getresponse()
            payload = response.read()
        finally:
            conn.close()
        return response.status, response, payload

    @staticmethod
    def _json(payload):
        try:
            return json.loads(payload.decode('utf-8'))
        except (UnicodeDecodeError, ValueError) as exc:
            raise SourceError('来源返回非 JSON: %s' % exc) from None

    # ---- 归档快照 ----

    def snapshot(self, talk=None):
        talk = talk or self.talk
        status, _, payload = self._request('GET', '/snapshot/%s' % talk)
        if status != 200:
            raise SourceError('快照获取失败 HTTP %s: %s' % (
                status, payload[:200].decode('utf-8', 'replace')))
        try:
            value = self._json(payload)
        except SourceError:
            raise
        if not isinstance(value, dict) or 'covers_through' not in value or 'events' not in value:
            raise SourceError('快照结构不符合契约')
        covers = value.get('covers_through')
        events = value.get('events')
        if not isinstance(covers, int) or isinstance(covers, bool) or covers < 0:
            raise SourceError('快照 covers_through 非法: %r' % (covers,))
        if not isinstance(events, list):
            raise SourceError('快照 events 不是数组')
        return {
            'talk': value.get('talk', talk),
            'covers_through': covers,
            'events': events,
            'body': payload,
            'body_sha256': sha256_hex(payload),
        }

    # ---- 实时流 ----

    def stream(self, last_event_id=0, stop=None, talk=None):
        """生成器：产出 ('event', SSEEvent) 或 ('heartbeat', 累计数)。

        连接关闭时抛 StreamInterrupted，由上层决定是否重连。
        """
        talk = talk or self.talk
        cursor = 0 if last_event_id in (None, '') else int(last_event_id)
        conn = self._connection(self.timeout)
        path = self.prefix + '/stream/%s' % talk
        headers = {
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'close',
        }
        if cursor:
            headers['Last-Event-ID'] = str(cursor)
        try:
            conn.request('GET', path, headers=headers)
            response = conn.getresponse()
        except (OSError, http.client.HTTPException) as exc:
            conn.close()
            raise StreamInterrupted('connect_failed:%s' % exc, cursor) from None
        try:
            if response.status == 409:
                payload = response.read()
                value = {}
                try:
                    value = json.loads(payload.decode('utf-8'))
                except (UnicodeDecodeError, ValueError):
                    pass
                floor = value.get('floor')
                snapshot = value.get('snapshot')
                raise ReplayExpired(
                    int(floor) if isinstance(floor, int) else None,
                    snapshot, payload, sha256_hex(payload))
            if response.status != 200:
                payload = response.read()
                raise StreamInterrupted(
                    'http_%s:%s' % (status_text(response.status),
                                    payload[:200].decode('utf-8', 'replace')), cursor)
            content_type = response.getheader('Content-Type', '')
            if 'text/event-stream' not in content_type:
                raise StreamInterrupted('unexpected_content_type:%s' % content_type, cursor)
            # 连接建立后缩短读超时，让 stop()/空闲检测能及时生效。
            try:
                conn.sock.settimeout(self.read_timeout)
            except (AttributeError, OSError):
                pass
            decoder = SSEDecoder()
            heartbeat_seen = 0
            idle_since = time.monotonic()
            while True:
                if stop is not None and stop.is_set():
                    return
                try:
                    chunk = response.read1(8192)
                except socket.timeout:
                    if stop is not None and stop.is_set():
                        return
                    if time.monotonic() - idle_since >= self.idle_timeout:
                        raise StreamInterrupted('idle_timeout', cursor) from None
                    continue
                except (OSError, http.client.HTTPException) as exc:
                    raise StreamInterrupted('read_failed:%s' % exc, cursor) from None
                if not chunk:
                    return
                idle_since = time.monotonic()
                events = decoder.feed(chunk)
                if decoder.heartbeats > heartbeat_seen:
                    heartbeat_seen = decoder.heartbeats
                    yield 'heartbeat', heartbeat_seen
                for event in events:
                    yield 'event', event
        finally:
            try:
                response.close()
            finally:
                conn.close()

    # ---- 编辑台接收方（可选） ----

    def post_editor(self, collection, value):
        if collection not in ('exports', 'withdrawals'):
            raise ValueError(collection)
        status, _, payload = self._request(
            'POST', '/editor/%s' % collection, value)
        if status != 200:
            raise SourceError('编辑台登记失败 HTTP %s: %s' % (
                status, payload[:200].decode('utf-8', 'replace')))
        return self._json(payload)

    def editor_history(self, collection):
        status, _, payload = self._request('GET', '/editor/%s' % collection)
        if status != 200:
            raise SourceError('编辑台查询失败 HTTP %s' % status)
        return self._json(payload)


def status_text(status):
    return str(status)
