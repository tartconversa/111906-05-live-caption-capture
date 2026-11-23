"""SQLite 持久化：原始事件字节、传输接续、快照水位、导出依据与审计。

写入方只有采集进程；读取方（状态接口、编辑工具）各自打开连接。
使用 WAL，保证长时间 SSE 采集期间仍能查询已确认的段落。
"""
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from .model import canonical, sha256_hex

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    talk TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    PRIMARY KEY (talk, key)
);

CREATE TABLE IF NOT EXISTS receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    talk TEXT NOT NULL,
    channel TEXT NOT NULL,
    connection INTEGER,
    transport_id INTEGER,
    event_type TEXT,
    segment TEXT,
    version INTEGER,
    payload_json TEXT,
    payload_sha256 TEXT,
    data_text TEXT,
    raw BLOB,
    raw_sha256 TEXT,
    valid INTEGER NOT NULL DEFAULT 0,
    errors_json TEXT,
    applied TEXT,
    received_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS receipts_delivery
    ON receipts (talk, channel, transport_id, raw_sha256);
CREATE INDEX IF NOT EXISTS receipts_talk_id ON receipts (talk, id);
CREATE INDEX IF NOT EXISTS receipts_segment ON receipts (talk, segment);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    talk TEXT NOT NULL,
    covers_through INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    body BLOB,
    body_sha256 TEXT,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    talk TEXT NOT NULL,
    kind TEXT NOT NULL,
    detail_json TEXT,
    at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gaps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    talk TEXT NOT NULL,
    start_id INTEGER NOT NULL,
    end_id INTEGER NOT NULL,
    reason TEXT NOT NULL,
    detail_json TEXT,
    at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS gaps_talk ON gaps (talk, start_id, end_id);

CREATE TABLE IF NOT EXISTS recoveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    talk TEXT NOT NULL,
    start_id INTEGER NOT NULL,
    end_id INTEGER NOT NULL,
    via TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail_json TEXT,
    at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exports (
    reference TEXT PRIMARY KEY,
    talk TEXT NOT NULL,
    segment TEXT NOT NULL,
    text TEXT NOT NULL,
    speaker TEXT NOT NULL,
    versions_json TEXT NOT NULL,
    payload_sha_json TEXT NOT NULL,
    receipts_json TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    state TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    acked_at TEXT,
    ack_note TEXT,
    acked_reasons_json TEXT,
    origin TEXT NOT NULL DEFAULT 'capture'
);

CREATE TABLE IF NOT EXISTS external_withdrawals (
    reference TEXT PRIMARY KEY,
    export_reference TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    payload_json TEXT,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS withdrawals_export
    ON external_withdrawals (export_reference);

CREATE TABLE IF NOT EXISTS withdrawals (
    reference TEXT PRIMARY KEY,
    export_reference TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    reference TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    receipt_json TEXT,
    UNIQUE (kind, reference)
);

CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    kind TEXT NOT NULL,
    reference TEXT,
    detail_json TEXT
);
"""


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


class Store:
    def __init__(self, path):
        self.path = str(path)
        if self.path != ':memory:':
            parent = os.path.dirname(os.path.abspath(self.path))
            os.makedirs(parent, exist_ok=True)
        self._local = threading.local()
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO meta(talk, key, value) VALUES ('*', 'schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def _connection(self):
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=10000')
            conn.execute('PRAGMA foreign_keys=ON')
            self._local.conn = conn
        return conn

    @contextmanager
    def connect(self):
        conn = self._connection()
        try:
            yield conn
        except Exception:
            raise

    def transaction(self):
        return _Transaction(self._connection())

    def close(self):
        conn = getattr(self._local, 'conn', None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ---- meta ----

    def meta_get(self, talk, key, default=None):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT value FROM meta WHERE talk=? AND key=?', (talk, key)
            ).fetchone()
        return default if row is None else row['value']

    def meta_set(self, talk, key, value):
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        with self.connect() as conn:
            conn.execute(
                'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                (talk, key, value),
            )

    def cursor(self, talk):
        value = self.meta_get(talk, 'cursor')
        return int(value) if value not in (None, '') else 0

    def set_cursor(self, talk, value, source='stream', advance_only=True):
        value = int(value)
        current = self.cursor(talk)
        if advance_only and value <= current:
            return current
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                (talk, 'cursor', str(value)),
            )
            conn.execute(
                'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                (talk, 'cursor_source', source),
            )
            conn.execute(
                'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                (talk, 'cursor_at', utc_now()),
            )
        return value

    def begin_connection(self, talk):
        with self.transaction() as conn:
            row = conn.execute(
                'SELECT value FROM meta WHERE talk=? AND key=?',
                (talk, 'connection_count'),
            ).fetchone()
            number = int(row['value']) + 1 if row else 1
            conn.execute(
                'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                (talk, 'connection_count', str(number)),
            )
            return number

    # ---- receipts ----

    def find_delivery(self, talk, channel, transport_id, raw_sha256):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT * FROM receipts WHERE talk=? AND channel=? AND transport_id=? '
                'AND raw_sha256=?',
                (talk, channel, transport_id, raw_sha256),
            ).fetchone()
        return dict(row) if row else None

    def insert_receipt(self, receipt, advance_cursor=True):
        """写入一条已见原始事件；同一次投递重复出现时返回已有记录。"""
        with self.transaction() as conn:
            existing = conn.execute(
                'SELECT * FROM receipts WHERE talk=? AND channel=? AND transport_id=? '
                'AND raw_sha256=?',
                (receipt['talk'], receipt['channel'],
                 receipt.get('transport_id'), receipt['raw_sha256']),
            ).fetchone()
            if existing:
                return {'inserted': False, 'receipt': dict(existing)}
            conn.execute(
                '''INSERT INTO receipts(talk, channel, connection, transport_id,
                       event_type, segment, version, payload_json, payload_sha256,
                       data_text, raw, raw_sha256, valid, errors_json, applied,
                       received_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    receipt['talk'], receipt['channel'], receipt.get('connection'),
                    receipt.get('transport_id'), receipt.get('event_type'),
                    receipt.get('segment'), receipt.get('version'),
                    receipt.get('payload_json'), receipt.get('payload_sha256'),
                    receipt.get('data_text'), receipt.get('raw'),
                    receipt['raw_sha256'], 1 if receipt.get('valid') else 0,
                    json.dumps(receipt.get('errors') or [], ensure_ascii=False),
                    receipt.get('applied'), receipt['received_at'],
                ),
            )
            new_id = conn.execute('SELECT last_insert_rowid() AS id').fetchone()['id']
            if advance_cursor and receipt.get('transport_id') is not None:
                value = int(receipt['transport_id'])
                row = conn.execute(
                    'SELECT value FROM meta WHERE talk=? AND key=?',
                    (receipt['talk'], 'cursor'),
                ).fetchone()
                current = int(row['value']) if row and row['value'] else 0
                if value > current:
                    conn.execute(
                        'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                        'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                        (receipt['talk'], 'cursor', str(value)),
                    )
                    conn.execute(
                        'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                        'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                        (receipt['talk'], 'cursor_source', receipt['channel']),
                    )
                    conn.execute(
                        'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                        'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                        (receipt['talk'], 'cursor_at', receipt['received_at']),
                    )
                conn.execute(
                    'INSERT INTO meta(talk, key, value) VALUES (?, ?, ?) '
                    'ON CONFLICT(talk, key) DO UPDATE SET value=excluded.value',
                    (receipt['talk'], 'last_transport_id', str(value)),
                )
            row = conn.execute('SELECT * FROM receipts WHERE id=?', (new_id,)).fetchone()
            return {'inserted': True, 'receipt': dict(row)}

    def receipts(self, talk):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT * FROM receipts WHERE talk=? ORDER BY id', (talk,)
            ).fetchall()
        return [dict(row) for row in rows]

    def version_digests(self, talk, segment, event_type, version):
        """某段落某类型某版本已收到的全部内容摘要。"""
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT payload_sha256 FROM receipts WHERE talk=? AND segment=? '
                'AND event_type=? AND version=? AND valid=1',
                (talk, segment, event_type, version)).fetchall()
        return {row['payload_sha256'] for row in rows}

    def max_version(self, talk, segment, event_type):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT MAX(version) AS version FROM receipts WHERE talk=? AND segment=? '
                'AND event_type=? AND valid=1',
                (talk, segment, event_type)).fetchone()
        return int(row['version']) if row and row['version'] is not None else 0

    def stream_ids(self, talk):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT DISTINCT transport_id FROM receipts WHERE talk=? '
                'AND channel=? AND transport_id IS NOT NULL',
                (talk, 'stream')).fetchall()
        return {int(row['transport_id']) for row in rows}

    def snapshot_ids(self, talk):
        """归档快照中实际留存了原文的传输 ID。"""
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT DISTINCT transport_id FROM receipts WHERE talk=? '
                'AND channel=? AND transport_id IS NOT NULL',
                (talk, 'snapshot')).fetchall()
        return {int(row['transport_id']) for row in rows}

    def snapshots(self, talk):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT * FROM snapshots WHERE talk=? ORDER BY id', (talk,)
            ).fetchall()
        return [{key: row[key] for key in (
            'id', 'talk', 'covers_through', 'event_count', 'body_sha256',
            'fetched_at')} for row in rows]

    def receipts_for_segment(self, talk, segment):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT * FROM receipts WHERE talk=? AND segment=? ORDER BY id',
                (talk, segment),
            ).fetchall()
        return [dict(row) for row in rows]

    def receipt(self, receipt_id):
        with self.connect() as conn:
            row = conn.execute('SELECT * FROM receipts WHERE id=?', (receipt_id,)).fetchone()
        return dict(row) if row else None

    # ---- snapshots / incidents ----

    def add_snapshot(self, talk, covers_through, event_count, body, fetched_at=None):
        fetched_at = fetched_at or utc_now()
        body = body or b''
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO snapshots(talk, covers_through, event_count, body, '
                'body_sha256, fetched_at) VALUES (?,?,?,?,?,?)',
                (talk, int(covers_through), int(event_count), body,
                 sha256_hex(body), fetched_at),
            )
        return {'covers_through': int(covers_through), 'event_count': int(event_count),
                'body_sha256': sha256_hex(body), 'fetched_at': fetched_at}

    def latest_snapshot(self, talk):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT * FROM snapshots WHERE talk=? ORDER BY id DESC LIMIT 1', (talk,)
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item.pop('body', None)
        return item

    def snapshot_body(self, snapshot_id):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT body, body_sha256 FROM snapshots WHERE id=?', (snapshot_id,)
            ).fetchone()
        return dict(row) if row else None

    def add_incident(self, talk, kind, detail, at=None):
        at = at or utc_now()
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO incidents(talk, kind, detail_json, at) VALUES (?,?,?,?)',
                (talk, kind, json.dumps(detail, ensure_ascii=False, sort_keys=True), at),
            )
        return {'kind': kind, 'detail': detail, 'at': at}

    def incidents(self, talk, limit=50):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT * FROM incidents WHERE talk=? ORDER BY id DESC LIMIT ?',
                (talk, limit),
            ).fetchall()
        return [
            {'kind': row['kind'], 'detail': json.loads(row['detail_json'] or 'null'),
             'at': row['at']}
            for row in rows
        ]

    # ---- 覆盖区间 ----

    def add_gap(self, talk, start_id, end_id, reason, detail=None, at=None):
        """登记一段尚未用事件原文补齐的传输区间 [start_id, end_id]。"""
        start_id, end_id = int(start_id), int(end_id)
        if end_id < start_id:
            return None
        at = at or utc_now()
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO gaps(talk, start_id, end_id, reason, detail_json, at) '
                'VALUES (?,?,?,?,?,?)',
                (talk, start_id, end_id, reason,
                 json.dumps(detail or {}, ensure_ascii=False, sort_keys=True), at),
            )
        return {'start_id': start_id, 'end_id': end_id, 'reason': reason,
                'detail': detail or {}, 'at': at}

    def gaps(self, talk):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT * FROM gaps WHERE talk=? ORDER BY start_id, end_id', (talk,)
            ).fetchall()
        return [
            {'start_id': row['start_id'], 'end_id': row['end_id'],
             'reason': row['reason'],
             'detail': json.loads(row['detail_json'] or '{}'), 'at': row['at']}
            for row in rows
        ]

    def resolve_gaps(self, talk, covered_ids, reason='snapshot', detail=None):
        """用已有事件原文覆盖区间后，收缩或消除重叠的缺口。"""
        covered = sorted({int(value) for value in covered_ids if int(value) > 0})
        if not covered:
            return []
        remaining = []
        with self.transaction() as conn:
            rows = conn.execute(
                'SELECT * FROM gaps WHERE talk=? ORDER BY id', (talk,)
            ).fetchall()
            changes = []
            for row in rows:
                start, end = row['start_id'], row['end_id']
                missing = [value for value in range(start, end + 1) if value not in set(covered)]
                if not missing:
                    conn.execute('DELETE FROM gaps WHERE id=?', (row['id'],))
                    changes.append({'gap': [start, end], 'now': None})
                    continue
                # 收缩为最小连续缺失区间集合；通常仅一段。
                runs = []
                run_start = missing[0]
                previous = missing[0]
                for value in missing[1:]:
                    if value == previous + 1:
                        previous = value
                        continue
                    runs.append((run_start, previous))
                    run_start = previous = value
                runs.append((run_start, previous))
                conn.execute('DELETE FROM gaps WHERE id=?', (row['id'],))
                detail_json = json.dumps(detail or {}, ensure_ascii=False, sort_keys=True)
                for run_start, run_end in runs:
                    # 收缩后的缺口仍源自原来的原因，避免把待补说成已补来源。
                    conn.execute(
                        'INSERT INTO gaps(talk, start_id, end_id, reason, detail_json, at) '
                        'VALUES (?,?,?,?,?,?)',
                        (talk, run_start, run_end, row['reason'], detail_json,
                         utc_now()),
                    )
                    remaining.append({'start_id': run_start, 'end_id': run_end,
                                      'reason': row['reason']})
                changes.append({'gap': [start, end], 'now': [list(run) for run in runs]})
        return changes

    def covered_ids(self, talk):
        """返回已用原始事件覆盖的传输 ID（推送或快照原文均可）。"""
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT DISTINCT transport_id FROM receipts WHERE talk=? '
                'AND transport_id IS NOT NULL', (talk,)
            ).fetchall()
        return {int(row['transport_id']) for row in rows}

    # ---- 补领记录 ----
    def add_recovery(self, talk, start_id, end_id, via, reason, detail=None, at=None):
        """登记一段由归档快照补来的传输区间。

        与 gaps 相对：gaps 是尚未补齐的缺口，recoveries 是已明确补来的区间。
        两者都保留，便于解释“从哪里连续采齐、哪里尚缺”。
        """
        if end_id < start_id:
            return None
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO recoveries(talk, start_id, end_id, via, reason, '
                'detail_json, at) VALUES (?,?,?,?,?,?,?)',
                (talk, int(start_id), int(end_id), via, reason,
                 json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
                 at or utc_now()),
            )
        return {'start_id': int(start_id), 'end_id': int(end_id), 'via': via,
                'reason': reason, 'detail': detail or {}, 'at': at or utc_now()}

    def recoveries(self, talk):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT * FROM recoveries WHERE talk=? ORDER BY start_id, end_id',
                (talk,)).fetchall()
        return [
            {'start_id': row['start_id'], 'end_id': row['end_id'], 'via': row['via'],
             'reason': row['reason'],
             'detail': json.loads(row['detail_json'] or '{}'), 'at': row['at']}
            for row in rows
        ]

    # ---- exports / withdrawals / audit / outbox ----

    def export(self, reference):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT * FROM exports WHERE reference=?', (reference,)
            ).fetchone()
        return dict(row) if row else None

    def exports(self, talk):
        with self.connect() as conn:
            rows = conn.execute(
                'SELECT * FROM exports WHERE talk=? ORDER BY created_at, reference', (talk,)
            ).fetchall()
        return [dict(row) for row in rows]

    def put_export(self, item, update=False):
        columns = ('reference', 'talk', 'segment', 'text', 'speaker', 'versions_json',
                   'payload_sha_json', 'receipts_json', 'evidence_sha256', 'state',
                   'reasons_json', 'created_at', 'updated_at', 'acked_at', 'ack_note',
                   'acked_reasons_json', 'origin')
        values = [item.get(column) for column in columns]
        with self.transaction() as conn:
            if update:
                assignments = ', '.join('%s=?' % column for column in columns[1:])
                conn.execute(
                    'UPDATE exports SET %s WHERE reference=?' % assignments,
                    values[1:] + [item['reference']],
                )
            else:
                conn.execute(
                    'INSERT INTO exports(%s) VALUES (%s)'
                    % (', '.join(columns), ','.join('?' for _ in columns)),
                    values,
                )
        return self.export(item['reference'])

    def add_external_withdrawal(self, reference, export_reference, reason, source,
                                payload, received_at=None):
        received_at = received_at or utc_now()
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO external_withdrawals(reference, export_reference, reason, '
                'source, payload_json, received_at) VALUES (?,?,?,?,?,?) '
                'ON CONFLICT(reference) DO UPDATE SET reason=excluded.reason, '
                'source=excluded.source, payload_json=excluded.payload_json',
                (reference, export_reference, reason, source,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True), received_at),
            )
        self.audit('external_withdrawal', reference, {
            'export_reference': export_reference, 'reason': reason, 'source': source,
        })
        return self.external_withdrawal(reference)

    def external_withdrawal(self, reference):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT * FROM external_withdrawals WHERE reference=?', (reference,)
            ).fetchone()
        return dict(row) if row else None

    def external_withdrawals(self, export_reference=None):
        with self.connect() as conn:
            if export_reference is None:
                rows = conn.execute(
                    'SELECT * FROM external_withdrawals ORDER BY received_at, reference'
                ).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM external_withdrawals WHERE export_reference=? '
                    'ORDER BY received_at, reference', (export_reference,)
                ).fetchall()
        return [dict(row) for row in rows]

    def add_withdrawal(self, reference, export_reference, reason, source):
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO withdrawals(reference, export_reference, reason, source, '
                'created_at) VALUES (?,?,?,?,?) ON CONFLICT(reference) DO NOTHING',
                (reference, export_reference, reason, source, utc_now()),
            )
        return self.withdrawal(reference)

    def withdrawal(self, reference):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT * FROM withdrawals WHERE reference=?', (reference,)
            ).fetchone()
        return dict(row) if row else None

    def withdrawals(self, export_reference=None):
        with self.connect() as conn:
            if export_reference is None:
                rows = conn.execute(
                    'SELECT * FROM withdrawals ORDER BY created_at, reference'
                ).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM withdrawals WHERE export_reference=? ORDER BY created_at',
                    (export_reference,),
                ).fetchall()
        return [dict(row) for row in rows]

    def audit(self, kind, reference, detail):
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO audit(at, kind, reference, detail_json) VALUES (?,?,?,?)',
                (utc_now(), kind, reference,
                 json.dumps(detail, ensure_ascii=False, sort_keys=True)),
            )

    def audit_log(self, kind=None, reference=None, limit=200):
        query = 'SELECT * FROM audit'
        clauses, params = [], []
        if kind:
            clauses.append('kind=?')
            params.append(kind)
        if reference:
            clauses.append('reference=?')
            params.append(reference)
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY id DESC LIMIT ?'
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {'id': row['id'], 'at': row['at'], 'kind': row['kind'],
             'reference': row['reference'],
             'detail': json.loads(row['detail_json'] or 'null')}
            for row in rows
        ]

    def enqueue(self, kind, reference, payload):
        created = utc_now()
        with self.transaction() as conn:
            conn.execute(
                'INSERT INTO outbox(kind, reference, payload_json, status, created_at) '
                'VALUES (?,?,?,?,?) ON CONFLICT(kind, reference) DO UPDATE SET '
                'payload_json=excluded.payload_json, status=CASE WHEN outbox.status="sent" '
                'THEN "pending" ELSE outbox.status END',
                (kind, reference, json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 'pending', created),
            )
        return self.outbox_item(kind, reference)

    def outbox_item(self, kind, reference):
        with self.connect() as conn:
            row = conn.execute(
                'SELECT * FROM outbox WHERE kind=? AND reference=?', (kind, reference)
            ).fetchone()
        return dict(row) if row else None

    def outbox(self, status=None):
        with self.connect() as conn:
            if status:
                rows = conn.execute(
                    'SELECT * FROM outbox WHERE status=? ORDER BY id', (status,)
                ).fetchall()
            else:
                rows = conn.execute('SELECT * FROM outbox ORDER BY id').fetchall()
        return [dict(row) for row in rows]

    def mark_outbox(self, item_id, status, error=None, receipt=None):
        with self.transaction() as conn:
            conn.execute(
                'UPDATE outbox SET status=?, attempts=attempts+1, last_error=?, '
                'sent_at=?, receipt_json=? WHERE id=?',
                (status, error,
                 utc_now() if status == 'sent' else None,
                 json.dumps(receipt, ensure_ascii=False, sort_keys=True) if receipt else None,
                 item_id),
            )


class _Transaction:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.execute('BEGIN IMMEDIATE')
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.execute('COMMIT')
        else:
            self.conn.execute('ROLLBACK')
        return False
