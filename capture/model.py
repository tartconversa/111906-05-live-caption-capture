"""原始字幕事件的业务校验与规范化。

传输事件 ID 只作为接续与来源标识；业务新旧只比较 segment 与类型内部的 version。
"""
import hashlib
import json

EVENT_TYPES = ('text', 'speaker', 'permission')

REQUIRED = {
    'text': {'text': str, 'final': bool},
    'speaker': {'speaker': str, 'confirmed': bool},
    'permission': {'public': bool},
}

META_FIELDS = ('id', 'type', 'segment', 'version')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def sha256_hex(payload):
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def parse_business(event_type, data_text):
    """解析一条业务事件。

    返回 dict：ok、errors、segment、version、payload（不含 id/type/segment/version）、
    extras、payload_sha256。无法验证的内容保留 errors，供受限留存与待核展示。
    """
    errors = []
    if event_type not in EVENT_TYPES:
        errors.append('unknown_event_type:%s' % event_type)
    try:
        data = json.loads(data_text)
    except (TypeError, ValueError) as exc:
        return {
            'ok': False,
            'errors': ['invalid_json:%s' % exc],
            'segment': None,
            'version': None,
            'payload': None,
            'extras': {},
            'payload_sha256': None,
        }
    if not isinstance(data, dict):
        return {
            'ok': False,
            'errors': ['data_not_object'],
            'segment': None,
            'version': None,
            'payload': None,
            'extras': {},
            'payload_sha256': None,
        }
    segment = data.get('segment')
    version = data.get('version')
    if not isinstance(segment, str) or not segment.strip():
        errors.append('invalid_segment')
        segment = None
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        errors.append('invalid_version')
        version = None
    payload = {key: value for key, value in data.items() if key not in META_FIELDS}
    known = REQUIRED.get(event_type, {})
    extras = sorted(set(payload) - set(known))
    for field, expected in known.items():
        value = payload.get(field)
        if field not in payload:
            errors.append('missing_field:%s' % field)
        elif expected is bool and not isinstance(value, bool):
            errors.append('invalid_field:%s' % field)
        elif expected is str and not isinstance(value, str):
            errors.append('invalid_field:%s' % field)
    payload_sha256 = sha256_hex(canonical(payload))
    return {
        'ok': not errors,
        'errors': errors,
        'segment': segment,
        'version': version,
        'payload': payload,
        'extras': extras,
        'payload_sha256': payload_sha256,
    }
