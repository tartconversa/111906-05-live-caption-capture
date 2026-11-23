"""按 segment 与类型分别归并版本，得出定稿、说话人、许可与公开引用状态。"""
from .model import EVENT_TYPES


class TypeState:
    """单个段落某一业务类型的版本状态。"""

    __slots__ = ('version', 'payload', 'sha', 'history')

    def __init__(self):
        self.version = 0
        self.payload = None
        self.sha = None
        self.history = {}

    @property
    def conflicted(self):
        return self.version > 0 and len(self.history.get(self.version, ())) > 1

    def apply(self, version, payload, sha):
        seen = self.history.setdefault(version, set())
        seen.add(sha)
        if version < self.version:
            return 'stale_conflict' if len(seen) > 1 else 'stale'
        if version == self.version:
            if sha == self.sha:
                return 'duplicate'
            return 'conflict'
        # 更高版本覆盖较早版本；同版本历史冲突随新版本一起作废。
        self.version = version
        self.payload = payload
        self.sha = sha
        return 'applied'


class Aggregator:
    """仅依据业务事件重建每个段落的状态。"""

    def __init__(self):
        self.segments = {}
        self.receipts = []

    def segment_types(self, segment):
        return self.segments.setdefault(segment, {name: TypeState() for name in EVENT_TYPES})

    def apply(self, receipt):
        segment = receipt['segment']
        types = self.segment_types(segment)
        result = types[receipt['event_type']].apply(
            receipt['version'], receipt['payload'], receipt['payload_sha256'])
        return result

    def add(self, receipt):
        self.receipts.append(receipt)
        return self.apply(receipt)

    def state(self, segment):
        types = self.segments.get(segment)
        if types is None:
            return None
        text = types['text']
        speaker = types['speaker']
        permission = types['permission']
        assembled = {
            'text': text.payload.get('text') if text.payload else None,
            'final': bool(text.payload.get('final')) if text.payload else False,
            'speaker': speaker.payload.get('speaker') if speaker.payload else None,
            'confirmed': bool(speaker.payload.get('confirmed')) if speaker.payload else False,
            'public': bool(permission.payload.get('public')) if permission.payload else False,
        }
        reasons = []
        if text.payload is None:
            status = 'no_text'
            reasons.append('no_text_event')
        elif text.conflicted:
            status = 'pending_review'
            reasons.append('text_version_conflict')
        elif not assembled['final']:
            status = 'draft'
            reasons.append('text_not_final')
        else:
            if speaker.conflicted:
                reasons.append('speaker_version_conflict')
            if permission.conflicted:
                reasons.append('permission_version_conflict')
            if reasons:
                status = 'pending_review'
            elif speaker.payload is None:
                status = 'pending_review'
                reasons.append('speaker_missing')
            elif not assembled['confirmed']:
                status = 'pending_review'
                reasons.append('speaker_unconfirmed')
            elif permission.payload is None:
                status = 'restricted'
                reasons.append('permission_missing')
            elif not assembled['public']:
                status = 'publication_withdrawn'
                reasons.append('permission_revoked')
            else:
                status = 'citable'
        citable = status == 'citable'
        return {
            'segment': segment,
            'status': status,
            'reasons': reasons,
            'assembled': assembled,
            'versions': {name: types[name].version for name in EVENT_TYPES},
            'conflicted': {
                name: types[name].conflicted for name in EVENT_TYPES
            },
            'has_text_event': text.payload is not None,
            'public_text': assembled['text'] if citable else None,
            'citable': citable,
        }

    def all_states(self):
        return [self.state(segment) for segment in sorted(self.segments)]

    def citable(self):
        return [state for state in self.all_states() if state['citable']]
