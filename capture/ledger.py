"""编辑引用账本：冻结导出依据，并在归属更正或许可撤销后列出受影响引用。

导出的是一段引文的快照：文字、说话人、三类业务版本与命中的原始事件。
之后的状态变化不会改写已导出的事实，只会把引用标记为受影响并给出原因。
"""
import json

from .model import canonical, sha256_hex
from .store import utc_now


class ExportRejected(ValueError):
    def __init__(self, reasons, state):
        super().__init__('导出被拒绝: ' + ','.join(reasons))
        self.reasons = reasons
        self.state = state


class Ledger:
    def __init__(self, store, talk):
        self.store = store
        self.talk = talk

    def export_citation(self, state, receipts_by_type):
        """按当前可公开状态冻结一段引用。

        receipts_by_type: {'text': receipt, 'speaker': receipt, 'permission': receipt}
        每个 receipt 是产生当前版本的事件（含其传输 ID 与原文摘要）。
        """
        if not state['citable']:
            raise ExportRejected(state['reasons'] or [state['status']], state)
        assembled = state['assembled']
        evidence = {}
        payload_shas = {}
        receipt_refs = {}
        for name in ('text', 'speaker', 'permission'):
            receipt = receipts_by_type.get(name)
            if receipt is None:
                raise ExportRejected(['missing_evidence:%s' % name], state)
            evidence[name] = {
                'receipt_id': receipt['id'],
                'transport_id': receipt.get('transport_id'),
                'raw_sha256': receipt['raw_sha256'],
                'segment_version': receipt.get('version'),
            }
            payload_shas[name] = receipt['payload_sha256']
            receipt_refs[name] = receipt['id']
        basis = {
            'talk': self.talk,
            'segment': state['segment'],
            'text': assembled['text'],
            'speaker': assembled['speaker'],
            'versions': state['versions'],
            'payload_sha256': payload_shas,
            'receipts': receipt_refs,
        }
        evidence_sha = sha256_hex(canonical(basis))
        reference = 'EXP-%s-%s' % (
            state['segment'].replace(' ', '_'), evidence_sha[:12])
        now = utc_now()
        item = {
            'reference': reference,
            'talk': self.talk,
            'segment': state['segment'],
            'text': assembled['text'],
            'speaker': assembled['speaker'],
            'versions_json': canonical(state['versions']),
            'payload_sha_json': canonical(payload_shas),
            'receipts_json': canonical(evidence),
            'evidence_sha256': evidence_sha,
            'state': 'valid',
            'reasons_json': '[]',
            'created_at': now,
            'updated_at': now,
            'acked_at': None,
            'ack_note': None,
            'acked_reasons_json': None,
            'origin': 'capture',
        }
        existing = self.store.export(reference)
        if existing is not None:
            same = all(existing[column] == item[column] for column in (
                'text', 'speaker', 'versions_json', 'payload_sha_json', 'receipts_json'))
            if not same:
                raise ValueError('引用参考号冲突且依据不同: %s' % reference)
            return existing, False
        stored = self.store.put_export(item)
        self.store.audit('export_created', reference, {
            'segment': state['segment'],
            'versions': state['versions'],
            'evidence_sha256': evidence_sha,
        })
        return stored, True

    def refresh(self, segments=None):
        """依据当前归并结果与外部撤下通知，重新评估引用是否仍可公开。

        segments 非空时只重算这些段落，供采集连接未关闭时随事件增量刷新。
        只改状态与原因，不改冻结的引用内容。返回 (changed_refs, states)。
        """
        from .replay import segment_aggregator

        exports = self.store.exports(self.talk)
        if segments is not None:
            wanted = set(segments)
            exports = [item for item in exports if item['segment'] in wanted]
        aggregators = {}
        changed = []
        current = {}
        for export in exports:
            segment = export['segment']
            if segment not in aggregators:
                aggregators[segment] = segment_aggregator(
                    self.store, self.talk, segment)
            state = aggregators[segment].state(segment)
            reasons, expected_state = self._evaluate(export, state)
            for notice in self.store.external_withdrawals(export['reference']):
                if 'externally_withdrawn' not in reasons:
                    reasons.append('externally_withdrawn')
            if reasons:
                reasons = sorted(set(reasons))
            if 'externally_withdrawn' in reasons:
                expected_state = 'withdrawn'
            if export['state'] != expected_state or \
                    export['reasons_json'] != canonical(reasons):
                # 原因集合若与已确认时相同，保留确认；否则需要编辑重新处理。
                same_as_acked = export['acked_reasons_json'] == canonical(reasons)
                export['state'] = expected_state
                export['reasons_json'] = canonical(reasons)
                export['updated_at'] = utc_now()
                if not (same_as_acked and export['acked_at']):
                    export['acked_at'] = None
                    export['ack_note'] = None
                    export['acked_reasons_json'] = None
                self.store.put_export(export, update=True)
                self.store.audit('export_state_changed', export['reference'], {
                    'segment': export['segment'],
                    'state': expected_state,
                    'reasons': reasons,
                })
                changed.append(export['reference'])
            current[export['reference']] = {
                'state': export['state'], 'reasons': reasons,
            }
        return changed, current

    def _evaluate(self, export, state):
        """判断已导出引用在当前状态下的效力，返回 (reasons, state)。"""
        if state is None:
            return ['segment_missing'], 'withdrawn'
        reasons = []
        exported_versions = self._versions_from(export)
        external = export.get('origin', 'capture') != 'capture'
        if any(value is None for value in exported_versions.values()):
            if external:
                # 外部引用缺版本依据，无法判断内容是否被改写。
                return ['external_export_unverified'], 'affected'
        current_versions = state['versions']
        assembled = state['assembled']
        if external:
            # 本产品没有冻结这段外部引文的原文与命中收据，只能标待核。
            reasons.append('external_export_unverified')

        if any(state['conflicted'].values()):
            reasons.append('version_conflict')

        if state['conflicted']['speaker'] or (
                current_versions['speaker'] > exported_versions.get('speaker', 0)
                and assembled['speaker'] != export['speaker']):
            reasons.append('speaker_corrected')

        if state['conflicted']['permission']:
            reasons.append('permission_conflict')
        elif not assembled['public']:
            reasons.append('permission_revoked')

        if not state['has_text_event']:
            reasons.append('text_missing')
        elif state['conflicted']['text']:
            reasons.append('text_conflict')
        elif assembled['text'] != export['text']:
            reasons.append('text_changed')

        if not assembled['confirmed']:
            reasons.append('speaker_unconfirmed')

        reasons = sorted(set(reasons))
        if 'permission_revoked' in reasons or 'text_missing' in reasons:
            validity = 'withdrawn'
        elif reasons:
            validity = 'affected'
        else:
            validity = 'valid'
        return reasons, validity

    def register_external_export(self, payload, origin='editor'):
        """登记编辑台已存在的引用（可能并非本产品导出），使其也能被追溯。

        只按提交内容入账，并把该引用标记为待核，直到采集侧能对应到
        有效段落并重新评估。不替编辑台判断是否有效。
        """
        reference = payload.get('reference')
        segment = payload.get('segment')
        if not reference or not segment:
            return None
        if self.store.export(reference) is not None:
            return self.store.export(reference)
        versions = {}
        for name in ('text', 'speaker', 'permission'):
            value = payload.get('%s_version' % name)
            versions[name] = value if isinstance(value, int) else None
        now = utc_now()
        item = {
            'reference': reference,
            'talk': self.talk,
            'segment': segment,
            'text': payload.get('text') or '',
            'speaker': payload.get('speaker') or '',
            'versions_json': canonical(versions),
            'payload_sha_json': canonical({}),
            'receipts_json': canonical({}),
            'evidence_sha256': sha256_hex(canonical(payload)),
            'state': 'affected',
            'reasons_json': canonical(['external_export_unverified']),
            'created_at': payload.get('created_at') or now,
            'updated_at': now,
            'acked_at': None,
            'ack_note': None,
            'acked_reasons_json': None,
            'origin': origin,
        }
        self.store.put_export(item)
        self.store.audit('external_export_registered', reference, {
            'segment': segment, 'origin': origin})
        return item

    @staticmethod
    def _versions_from(export):
        try:
            value = json.loads(export['versions_json'])
        except (TypeError, ValueError):
            return {}
        return value if isinstance(value, dict) else {}

    def add_withdrawal(self, export_reference, reason, external_reference=None,
                       source='operator', deliver=True):
        export = self.store.export(export_reference)
        if export is None:
            raise KeyError('未知导出引用: %s' % export_reference)
        if external_reference is None:
            external_reference = 'WDR-%s-%s' % (
                export_reference, sha256_hex(canonical({
                    'reason': reason, 'at': utc_now()}))[:10])
        self.store.add_withdrawal(external_reference, export_reference, reason, source)
        self.store.add_external_withdrawal(external_reference, export_reference, reason,
                                           source, {'reference': external_reference,
                                                    'export_reference': export_reference,
                                                    'reason': reason})
        if deliver:
            self.store.enqueue('withdrawals', external_reference, {
                'reference': external_reference,
                'export_reference': export_reference,
                'reason': reason,
            })
        return self.store.withdrawal(external_reference)

    def pending(self):
        """编辑仍待处理的受影响引用清单。"""
        items = []
        for export in self.store.exports(self.talk):
            if export['state'] == 'valid':
                continue
            if export['acked_at'] and export['acked_reasons_json'] == export['reasons_json']:
                continue
            items.append({
                'reference': export['reference'],
                'segment': export['segment'],
                'text': export['text'],
                'speaker': export['speaker'],
                'state': export['state'],
                'reasons': json.loads(export['reasons_json'] or '[]'),
                'updated_at': export['updated_at'],
            })
        return items

    def acknowledge(self, export_reference, note=None):
        export = self.store.export(export_reference)
        if export is None:
            raise KeyError('未知导出引用: %s' % export_reference)
        export['acked_at'] = utc_now()
        export['ack_note'] = note
        export['acked_reasons_json'] = export['reasons_json']
        self.store.put_export(export, update=True)
        self.store.audit('export_acknowledged', export_reference, {
            'note': note,
            'reasons': json.loads(export['reasons_json'] or '[]'),
        })
        return self.store.export(export_reference)
