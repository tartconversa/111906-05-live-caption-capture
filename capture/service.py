"""持续采集编排：接续、窗口失效、归档补领与导出效力刷新。

所有原始事件（含无法解释的、重复的与受限的）都先落库，再参与归并。
快照覆盖只表示归档中存在该传输区间，不表示实时流同一时刻也收到了事件。
"""
import json
import threading

from .client import ReplayExpired, SourceError, StreamInterrupted
from .ledger import Ledger
from .model import canonical, parse_business, sha256_hex
from .replay import reconstruct, segment_aggregator
from .sse import SSEEvent
from .store import utc_now


class CaptureService:
    """单一场次的采集服务。"""

    def __init__(self, store, client, talk='TALK-1', log=None,
                 backoff=(0.2, 0.5, 1.0, 2.0, 5.0), auto_snapshot=True):
        self.store = store
        self.client = client
        self.talk = talk
        self.log = log or (lambda message: None)
        self.backoff = tuple(backoff) or (1.0,)
        self.auto_snapshot = auto_snapshot
        self.ledger = Ledger(store, talk)
        self._stop = threading.Event()

    # ---- 生命周期 ----

    def stop(self):
        self._stop.set()

    def clear_stop(self):
        self._stop.clear()

    @property
    def stopped(self):
        return self._stop.is_set()

    def _next_connection(self):
        return self.store.begin_connection(self.talk)

    # ---- 归档快照 ----

    def fetch_snapshot(self):
        snapshot = self.client.snapshot(self.talk)
        self._remember_snapshot(snapshot, source='fetch')
        return snapshot

    def _remember_snapshot(self, snapshot, source):
        self.store.add_snapshot(
            self.talk, snapshot['covers_through'], len(snapshot['events']),
            snapshot['body'])
        self.store.meta_set(self.talk, 'snapshot_covers_at', {
            'covers_through': snapshot['covers_through'],
            'fetched_at': utc_now(),
            'body_sha256': snapshot['body_sha256'],
            'source': source,
        })
        self.store.audit('snapshot_fetched', self.talk, {
            'covers_through': snapshot['covers_through'],
            'event_count': len(snapshot['events']),
            'body_sha256': snapshot['body_sha256'],
            'source': source,
        })

    @staticmethod
    def _snapshot_ids(snapshot):
        ids = set()
        for event in snapshot['events']:
            if isinstance(event, dict):
                value = event.get('id')
                if isinstance(value, int) and not isinstance(value, bool) and value > 0:
                    ids.add(value)
        return ids

    def ingest_snapshot(self, snapshot, connection=None, source='snapshot'):
        """把归档快照中的事件作为独立通道原始事件留存。

        按 (segment, type, version) 归并，因此不会把旧说话人盖回已收到的更正。
        """
        if connection is None:
            connection = self._next_connection()
        inserted = duplicate = stale = 0
        ids = sorted(self._snapshot_ids(snapshot))
        for event in snapshot['events']:
            if not isinstance(event, dict):
                continue
            result = self._ingest_event(
                event, channel='snapshot', connection=connection)
            if result is None or not result['inserted']:
                duplicate += 1
                continue
            inserted += 1
            if result['applied'] in ('stale', 'stale_conflict', 'duplicate', 'conflict'):
                stale += 1
        covers = snapshot['covers_through']
        # 快照水位声明覆盖 [1, covers]；快照自称完整，但若原文不齐，
        # 仍只把确有原文的位置视为采齐，其余明确记为缺口。
        complete = bool(ids) and ids == list(range(1, covers + 1))
        if not complete:
            self.store.add_incident(self.talk, 'snapshot_incomplete', {
                'covers_through': covers,
                'event_count': len(ids),
                'body_sha256': snapshot['body_sha256'],
            })
        self.store.resolve_gaps(
            self.talk, ids, reason='snapshot_recovery',
            detail={'covers_through': covers,
                    'body_sha256': snapshot['body_sha256'],
                    'complete': complete})
        missing = sorted(set(range(1, covers + 1)) - set(ids))
        for run in _runs(missing):
            self.store.add_gap(
                self.talk, run[0], run[1], 'snapshot_incomplete',
                {'covers_through': covers,
                 'body_sha256': snapshot['body_sha256']})
        recovered = sorted(
            set(range(1, covers + 1)) - set(missing))
        if recovered:
            self.store.add_recovery(
                self.talk, recovered[0], recovered[-1], via='snapshot',
                reason=source,
                detail={'covers_through': covers,
                        'body_sha256': snapshot['body_sha256'],
                        'complete': complete,
                        'missing': missing})
        self.store.set_cursor(self.talk, covers, source='snapshot')
        self.store.meta_set(self.talk, 'snapshot_ingested_at', utc_now())
        self.ledger.refresh()
        return {'inserted': inserted, 'duplicate': duplicate, 'stale': stale,
                'covers_through': covers, 'complete': complete,
                'event_ids': ids, 'body_sha256': snapshot['body_sha256']}

    def recover_from_snapshot(self, reason='replay_expired', start_id=None):
        """窗口失效后的补领：取归档快照、归档入库、说明缺口。

        快照覆盖到的区间由原文补齐；覆盖不到的部分由调用方按来源 floor
        记为永久缺失。这里不预先把请求区间标成缺口，以免补领成功后
        残留误导性的缺口记录。
        """
        snapshot = self.fetch_snapshot()
        result = self.ingest_snapshot(snapshot, source=reason)
        result['reason'] = reason
        if start_id is not None:
            result['requested_from'] = start_id
        return result

    # ---- 编辑台（可选接收方） ----

    def import_external_withdrawals(self):
        """读取编辑台登记的撤下通知与外部导出引用。"""
        imported = []
        try:
            history = self.client.editor_history('withdrawals')
        except SourceError as exc:
            self.log('withdrawals_unavailable:%s' % exc)
            history = {}
        records = history.values() if isinstance(history, dict) else history or []
        for record in records:
            payload = record.get('payload', record) if isinstance(record, dict) else {}
            external = payload.get('reference')
            export_reference = payload.get('export_reference')
            reason = payload.get('reason', '')
            if not external or not export_reference:
                continue
            if self.store.export(export_reference) is None:
                continue
            if self.store.external_withdrawal(external) is not None:
                continue
            self.store.add_external_withdrawal(
                external, export_reference, reason, 'editor', payload)
            imported.append(external)
        registered = []
        try:
            history = self.client.editor_history('exports')
        except SourceError as exc:
            self.log('exports_unavailable:%s' % exc)
            history = {}
        records = history.values() if isinstance(history, dict) else history or []
        for record in records:
            payload = record.get('payload', record) if isinstance(record, dict) else {}
            if payload.get('reference') and self.store.export(payload['reference']) is None:
                if self.ledger.register_external_export(payload) is not None:
                    registered.append(payload['reference'])
        if imported or registered:
            self.ledger.refresh()
        return {'withdrawals': imported, 'exports': registered}

    def flush_outbox(self):
        """把本地登记的撤下通知送达编辑台（可选接口），保留回执。"""
        sent = []
        for item in self.store.outbox('pending'):
            payload = json.loads(item['payload_json'])
            try:
                receipt = self.client.post_editor(item['kind'], payload)
            except SourceError as exc:
                self.store.mark_outbox(item['id'], 'failed', error=str(exc))
                continue
            self.store.mark_outbox(item['id'], 'sent', receipt=receipt)
            sent.append({'reference': item['reference'], 'receipt': receipt})
        return sent

    # ---- 观察与导出 ----

    def observe(self):
        """返回连续覆盖、缺口、快照水位与可交付内容。"""
        aggregator, receipts, _ = reconstruct(self.store, self.talk)
        states = aggregator.all_states()
        citable = [state for state in states if state['citable']]
        drafts = [state for state in states if state['status'] == 'draft']
        # 未定稿、说话人未确认、版本冲突等都只进入待核资料，绝不进入公开导出。
        # 草稿单独成列，避免同一条段落同时出现在两个清单里。
        review = [state for state in states if state['status'] == 'pending_review']
        restricted = [state for state in states
                      if state['status'] in ('restricted', 'publication_withdrawn')]
        return {
            'talk': self.talk,
            'coverage': self.coverage(),
            'citable': [_public_view(state) for state in citable],
            'pending_review': [_review_view(state) for state in review],
            'drafts': [_draft_view(state) for state in drafts],
            'restricted': [_restricted_view(state) for state in restricted],
            'counts': {
                'segments': len(states), 'citable': len(citable),
                'pending_review': len(review), 'drafts': len(drafts),
                'restricted': len(restricted), 'receipts': len(receipts),
            },
            'pending_exports': self.ledger.pending(),
            'incidents': self.store.incidents(self.talk, 20),
            'generated_at': utc_now(),
        }

    def coverage(self):
        """分来源说明传输区间：连续采齐的部分与尚缺的部分。"""
        cursor = self.store.cursor(self.talk)
        stream_ids = self.store.stream_ids(self.talk)
        snapshot_ids = self.store.snapshot_ids(self.talk)
        snapshots = self.store.snapshots(self.talk)
        latest = snapshots[-1] if snapshots else None
        snapshot_covers = max((snap['covers_through'] for snap in snapshots),
                              default=0)
        # 只有“本产品明确知道存在”的位置才纳入描述；快照水位之后的空白
        # 与仅用于接续的 push cursor 都不代表已经存在的讲话。
        gaps = self.store.gaps(self.talk)
        known = set(stream_ids) | set(snapshot_ids)
        top = max(snapshot_covers, max(known, default=0),
                  max((gap['end_id'] for gap in gaps), default=0))
        labels = []
        for value in range(1, top + 1):
            if value in stream_ids:
                labels.append('stream')
            elif value in snapshot_ids:
                labels.append('snapshot')
            elif value <= snapshot_covers:
                labels.append('snapshot_declared')
            else:
                labels.append(None)
        merged = []
        index = 0
        while index < len(labels):
            source = labels[index]
            start = index + 1
            while index + 1 < len(labels) and labels[index + 1] == source:
                index += 1
            end = index + 1
            merged.append({'from': start, 'to': end, 'source': source})
            index += 1
        missing = []
        for item in merged:
            if item['source'] not in (None, 'snapshot_declared'):
                continue
            reasons = [gap['reason'] for gap in gaps
                       if gap['start_id'] <= item['to'] and gap['end_id'] >= item['from']]
            if reasons:
                reason = reasons[0]
            elif item['source'] == 'snapshot_declared':
                reason = 'snapshot_incomplete'
            else:
                reason = 'not_received'
            missing.append({'from': item['from'], 'to': item['to'],
                            'reason': reason})
        return {
            'cursor': cursor,
            'cursor_source': self.store.meta_get(self.talk, 'cursor_source'),
            'cursor_at': self.store.meta_get(self.talk, 'cursor_at'),
            'stream_tail': max(stream_ids) if stream_ids else 0,
            'snapshot': latest,
            'snapshot_covers_through': snapshot_covers,
            'same_moment_as_stream': False,
            'ranges': merged,
            'missing': missing,
            'recoveries': self.store.recoveries(self.talk),
            'interpretation': self._interpretation(snapshot_covers, missing, merged),
        }

    @staticmethod
    def _interpretation(snapshot_covers, missing, ranges):
        notes = []
        if snapshot_covers:
            notes.append(
                '归档快照覆盖至传输事件 %s；快照与实时流取自不同时刻，'
                '快照之后的范围内未见事件不等于没有讲话。' % snapshot_covers)
        declared = [item for item in ranges if item['source'] == 'snapshot_declared']
        if declared:
            notes.append('以下区间仅有快照水位声明、未随快照取回对应原文：%s；'
                         '仍待补取。' % '、'.join(
                             '%s–%s' % (item['from'], item['to'])
                             for item in declared))
        if missing:
            notes.append('以下区间既无实时推送原文、也无快照原文：%s；'
                         '不能据此断定没有讲话。' % '、'.join(
                             '%s–%s' % (item['from'], item['to'])
                             for item in missing))
        if snapshot_covers and not missing:
            notes.append('1–%s 已由实时推送或归档快照采齐。' % snapshot_covers)
        return ' '.join(notes)

    def citation_inputs(self, segment):
        aggregator, _, latest = reconstruct(self.store, self.talk)
        state = aggregator.state(segment)
        if state is None:
            raise KeyError('未知段落: %s' % segment)
        receipts = {name: latest.get((segment, name))
                    for name in ('text', 'speaker', 'permission')}
        return state, receipts

    def export_citation(self, segment):
        state, receipts = self.citation_inputs(segment)
        export, created = self.ledger.export_citation(state, receipts)
        if created:
            self.store.audit('citation_exported', export['reference'], {
                'segment': segment, 'evidence_sha256': export['evidence_sha256']})
        return export, created

    # ---- 运行循环 ----

    def run_once(self, max_events=None, on_event=None):
        """一次连接。返回结果字典，不抛出可重试错误；结束时刷新引用效力。"""
        try:
            return self._run_once(max_events=max_events, on_event=on_event)
        finally:
            self.ledger.refresh()

    def _run_once(self, max_events=None, on_event=None):
        cursor = self.store.cursor(self.talk)
        expected = cursor
        delivered = 0
        touched = set()
        connection = self._next_connection()
        try:
            for kind, payload in self.client.stream(
                    cursor, stop=self._stop, talk=self.talk):
                if kind != 'event':
                    continue
                transport = _transport_id(payload)
                if transport is not None and transport > expected + 1:
                    # 来源跳过了一段：明确登记，不能当作没有讲话。
                    self.store.add_gap(
                        self.talk, expected + 1, transport - 1, 'stream_skipped',
                        {'expected_from': expected + 1, 'received': transport})
                    self.store.add_incident(self.talk, 'stream_skipped', {
                        'from': expected + 1, 'to': transport - 1})
                self._ingest_event(payload, channel='stream', connection=connection)
                segment = _segment_of(payload)
                if segment:
                    touched.add(segment)
                    # 不等连接关闭：每条事件入库后立即重算受影响引用的效力。
                    self.ledger.refresh(segments=touched)
                if transport is not None and transport > expected:
                    expected = transport
                delivered += 1
                if on_event is not None:
                    on_event(payload)
                if self._stop.is_set():
                    return {'reason': 'stopped', 'events': delivered}
                if max_events is not None and delivered >= max_events:
                    return {'reason': 'max_events', 'events': delivered}
            return {'reason': 'eof', 'events': delivered}
        except ReplayExpired as expired:
            self.store.add_incident(self.talk, 'replay_expired', {
                'floor': expired.floor, 'requested_from': cursor,
                'snapshot': expired.snapshot,
                'body_sha256': expired.body_sha256})
            if expired.floor is not None:
                self.store.meta_set(self.talk, 'floor', expired.floor)
            self.log('replay_expired requested_from=%s floor=%s' % (cursor, expired.floor))
            recovered = None
            if self.auto_snapshot:
                recovered = self.recover_from_snapshot(
                    reason='replay_expired', start_id=cursor + 1)
                self.ledger.refresh()
            if expired.floor is not None:
                # 快照能覆盖到 watermark；watermark 之后、floor 之前这一段
                # 重放窗口与归档都没有，属于永久缺失，必须明确记录。
                snapshot_covers = (recovered or {}).get('covers_through', 0)
                lost_from = max(cursor + 1, snapshot_covers + 1)
                if expired.floor > lost_from:
                    reason = ('replay_window_lost' if recovered is not None
                              else 'replay_expired')
                    self.store.add_gap(
                        self.talk, lost_from, expired.floor - 1, reason,
                        {'floor': expired.floor,
                         'snapshot_covers_through': snapshot_covers,
                         'snapshot': expired.snapshot})
                    self.store.add_incident(self.talk, reason, {
                        'from': lost_from, 'to': expired.floor - 1})
                # 推进接续位置，避免对同一不可重放位置反复 409。
                if self.store.cursor(self.talk) < expired.floor:
                    self.store.set_cursor(
                        self.talk, expired.floor, source='replay_floor')
            if recovered is not None:
                return {'reason': 'replay_expired', 'events': delivered,
                        'recovered': recovered}
            return {'reason': 'replay_expired', 'events': delivered}
        except StreamInterrupted as exc:
            self.store.add_incident(self.talk, 'stream_interrupted', {
                'reason': exc.reason, 'last_event_id': exc.last_event_id,
                'attempted_from': cursor})
            self.log('stream_interrupted %s from=%s' % (exc.reason, cursor))
            return {'reason': 'interrupted', 'events': delivered,
                    'detail': exc.reason}
        except SourceError as exc:
            self.store.add_incident(self.talk, 'source_error', {
                'detail': str(exc), 'attempted_from': cursor})
            self.log('source_error %s' % exc)
            return {'reason': 'error', 'events': delivered, 'detail': str(exc)}

    def run(self, max_rounds=None, idle_sleep=None, on_event=None):
        """持续重连采集，直到 stop() 或达到轮次上限。"""
        self.clear_stop()
        rounds = 0
        results = []
        while not self._stop.is_set():
            result = self.run_once(on_event=on_event)
            results.append(result)
            rounds += 1
            self.ledger.refresh()
            if self._stop.is_set():
                break
            if max_rounds is not None and rounds >= max_rounds:
                break
            delay = self.backoff[min(rounds - 1, len(self.backoff) - 1)]
            if idle_sleep is not None:
                idle_sleep(delay)
            else:
                self._stop.wait(delay)
        return results

    # ---- 单条事件入库 ----

    def _ingest_event(self, event, channel, connection):
        if isinstance(event, dict):
            transport_raw = event.get('id')
            event_type = event.get('type')
            data_value = {key: value for key, value in event.items()
                          if key not in ('id', 'type')}
            data_text = canonical(data_value)
            raw = canonical(event).encode('utf-8')
        elif isinstance(event, SSEEvent):
            transport_raw = event.last_event_id
            event_type = event.event
            data_text = event.data
            raw = event.raw
        else:
            raise TypeError('无法识别的事件: %r' % (event,))
        raw_sha = sha256_hex(raw)
        transport_id = None
        if transport_raw not in (None, ''):
            try:
                value = int(transport_raw)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                transport_id = value
        if transport_id is not None:
            existing = self.store.find_delivery(
                self.talk, channel, transport_id, raw_sha)
        else:
            existing = self._find_by_raw(channel, raw_sha)
        if existing is not None:
            self.store.audit('duplicate_delivery', self.talk, {
                'channel': channel, 'transport_id': transport_id,
                'raw_sha256': raw_sha, 'existing_receipt': existing['id']})
            return None
        parsed = parse_business(event_type, data_text)
        applied = 'unvalidated'
        if parsed['ok']:
            current = segment_aggregator(
                self.store, self.talk, parsed['segment']
            ).segment_types(parsed['segment'])
            applied = current[event_type].apply(
                parsed['version'], parsed['payload'], parsed['payload_sha256'])
            current_version = self.store.max_version(
                self.talk, parsed['segment'], event_type)
            if parsed['version'] < current_version:
                # 旧版本：保留原文，但不参与归并，并记录与当前版本的差异。
                self.store.audit('stale_event', self.talk, {
                    'segment': parsed['segment'], 'event_type': event_type,
                    'version': parsed['version'],
                    'current_version': current_version,
                    'transport_id': transport_id})
            elif parsed['version'] == current_version and \
                    parsed['payload_sha256'] not in self.store.version_digests(
                        self.talk, parsed['segment'], event_type, parsed['version']):
                self.store.add_incident(self.talk, 'version_conflict', {
                    'segment': parsed['segment'], 'event_type': event_type,
                    'version': parsed['version'],
                    'transport_id': transport_id,
                    'payload_sha256': parsed['payload_sha256']})
        receipt = {
            'talk': self.talk, 'channel': channel, 'connection': connection,
            'transport_id': transport_id, 'event_type': event_type,
            'segment': parsed['segment'], 'version': parsed['version'],
            'payload_json': (canonical(parsed['payload'])
                             if parsed['payload'] is not None else None),
            'payload_sha256': parsed['payload_sha256'],
            'data_text': data_text, 'raw': raw, 'raw_sha256': raw_sha,
            'valid': parsed['ok'], 'errors': parsed['errors'],
            'applied': applied, 'received_at': utc_now(),
        }
        result = self.store.insert_receipt(receipt)
        if result['inserted']:
            self.store.audit('receipt_stored', self.talk, {
                'receipt_id': result['receipt']['id'], 'channel': channel,
                'transport_id': transport_id, 'segment': parsed['segment'],
                'event_type': event_type, 'applied': applied,
                'raw_sha256': raw_sha})
            if not parsed['ok']:
                self.store.add_incident(self.talk, 'invalid_event', {
                    'receipt_id': result['receipt']['id'],
                    'errors': parsed['errors']})
            result['applied'] = applied
        return result

    def _find_by_raw(self, channel, raw_sha):
        with self.store.connect() as conn:
            row = conn.execute(
                'SELECT * FROM receipts WHERE talk=? AND channel=? AND raw_sha256=? '
                'ORDER BY id LIMIT 1', (self.talk, channel, raw_sha)).fetchone()
        return dict(row) if row else None


def _runs(values):
    """把升序整数列表切成连续区间。"""
    runs = []
    for value in values:
        if runs and value == runs[-1][1] + 1:
            runs[-1][1] = value
        else:
            runs.append([value, value])
    return runs


def _segment_of(event):
    """从推送事件中取出段落后再解析正文，避免刷新时重扫整场。"""
    if not isinstance(event, SSEEvent):
        return None
    try:
        value = json.loads(event.data)
    except (TypeError, ValueError):
        return None
    segment = value.get('segment') if isinstance(value, dict) else None
    return segment if isinstance(segment, str) else None


def _transport_id(event):
    if not isinstance(event, SSEEvent):
        return None
    try:
        value = int(event.last_event_id)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


# ---- 展示视图：公开交付绝不含受限文本 ----

def _public_view(state):
    return {
        'segment': state['segment'],
        'text': state['assembled']['text'],
        'speaker': state['assembled']['speaker'],
        'versions': state['versions'],
        'status': state['status'],
    }


def _review_view(state):
    return {
        'segment': state['segment'],
        'text': state['assembled']['text'],
        'text_final': state['assembled']['final'],
        'speaker': state['assembled']['speaker'],
        'speaker_confirmed': state['assembled']['confirmed'],
        'status': state['status'],
        'reasons': state['reasons'],
        'versions': state['versions'],
    }


def _draft_view(state):
    return {
        'segment': state['segment'],
        'text': state['assembled']['text'],
        'versions': state['versions'],
    }


def _restricted_view(state):
    return {
        'segment': state['segment'],
        'status': state['status'],
        'reasons': state['reasons'],
        'versions': state['versions'],
        'text_withheld': True,
    }
