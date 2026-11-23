"""从持久化收据重建业务状态。

单独成模块，避免采集编排与引用账本互相导入。
"""
import json

from .aggregate import Aggregator


def receipt_from_row(row):
    payload = None
    if row['payload_json']:
        try:
            payload = json.loads(row['payload_json'])
        except ValueError:
            payload = None
    return {
        'id': row['id'],
        'talk': row['talk'],
        'channel': row['channel'],
        'connection': row['connection'],
        'transport_id': row['transport_id'],
        'event_type': row['event_type'],
        'segment': row['segment'],
        'version': row['version'],
        'payload': payload,
        'payload_json': row['payload_json'],
        'payload_sha256': row['payload_sha256'],
        'data_text': row['data_text'],
        'raw_sha256': row['raw_sha256'],
        'valid': bool(row['valid']),
        'errors': json.loads(row['errors_json'] or '[]'),
        'applied': row['applied'],
        'received_at': row['received_at'],
    }


def reconstruct(store, talk):
    """回放全部原始收据，得到归并状态与当前版本对应的收据。

    返回 (aggregator, receipts_by_id, latest_by_type)：
    latest_by_type[(segment, type)] 是产生当前版本的那条有效收据。
    """
    aggregator = Aggregator()
    by_id = {}
    latest = {}
    for row in store.receipts(talk):
        receipt = receipt_from_row(row)
        by_id[receipt['id']] = receipt
        if not receipt['valid']:
            continue
        if receipt['segment'] is None or receipt['version'] is None:
            continue
        if receipt['event_type'] not in ('text', 'speaker', 'permission'):
            continue
        aggregator.add(receipt)
        key = (receipt['segment'], receipt['event_type'])
        best = latest.get(key)
        if best is None or receipt['version'] > best['version']:
            latest[key] = receipt
        elif receipt['version'] == best['version']:
            # 同版本不同内容：归并层会标记冲突；保留先到的收据作展示依据。
            pass
    return aggregator, by_id, latest


def segment_aggregator(store, talk, segment):
    """只回放一个段落的收据，避免每次入库都重扫整场。"""
    aggregator = Aggregator()
    for row in store.receipts_for_segment(talk, segment):
        receipt = receipt_from_row(row)
        if not receipt['valid']:
            continue
        if receipt['segment'] is None or receipt['version'] is None:
            continue
        if receipt['event_type'] not in ('text', 'speaker', 'permission'):
            continue
        aggregator.add(receipt)
    return aggregator

