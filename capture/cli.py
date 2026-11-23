"""命令行入口：采集、观察、导出、撤下与恢复。"""
import argparse
import json
import signal
import sys
import threading

from .client import SourceClient, SourceError
from .ledger import ExportRejected, Ledger
from .model import canonical
from .service import CaptureService
from .status_server import serve_state
from .store import Store


def _emit(value):
    sys.stdout.write(canonical(value) + '\n')
    sys.stdout.flush()


def _service(args):
    store = Store(args.db)
    client = SourceClient(args.source, talk=args.talk,
                          timeout=args.timeout,
                          read_timeout=args.read_timeout,
                          idle_timeout=args.idle_timeout)
    return store, CaptureService(store, client, talk=args.talk,
                                 auto_snapshot=not args.no_snapshot)


def command_capture(args):
    store, service = _service(args)
    server = None
    if not args.no_status and args.status_port:
        server = serve_state(store, args.talk, host=args.status_host,
                             port=args.status_port)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print('状态查询: http://%s:%s/state' % (args.status_host,
                                             server.server_port), flush=True)

    def shutdown(signum, frame):
        print('收到停止信号，正在结束当前连接…', flush=True)
        service.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    results = []
    try:
        results = service.run(max_rounds=args.max_rounds)
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        _emit({'talk': args.talk, 'rounds': len(results), 'results': results})
        store.close()
    return 0


def command_observe(args):
    store = Store(args.db)
    try:
        service = CaptureService(store, client=None, talk=args.talk,
                                 auto_snapshot=False)
        _emit(service.observe())
    finally:
        store.close()
    return 0


def command_export(args):
    store = Store(args.db)
    try:
        service = CaptureService(store, client=None, talk=args.talk,
                                 auto_snapshot=False)
        try:
            export, created = service.export_citation(args.segment)
        except ExportRejected as exc:
            _emit({'error': 'not_citable', 'reasons': exc.reasons,
                   'state': exc.state})
            return 3
        _emit({'export': export, 'created': created})
    finally:
        store.close()
    return 0


def command_acknowledge(args):
    store = Store(args.db)
    try:
        ledger = Ledger(store, args.talk)
        item = ledger.acknowledge(args.reference, args.note)
        _emit({'export': item})
    finally:
        store.close()
    return 0


def command_withdraw(args):
    store = Store(args.db)
    try:
        ledger = Ledger(store, args.talk)
        item = ledger.add_withdrawal(args.export_reference, args.reason,
                                     args.reference, source='operator',
                                     deliver=not args.local_only)
        _emit({'withdrawal': item, 'outbox': store.outbox()})
    finally:
        store.close()
    return 0


def command_withdrawals(args):
    store = Store(args.db)
    try:
        _emit({'talk': args.talk, 'pending': Ledger(store, args.talk).pending(),
               'withdrawals': store.withdrawals(),
               'external': store.external_withdrawals()})
    finally:
        store.close()
    return 0


def command_snapshot(args):
    store = Store(args.db)
    client = SourceClient(args.source, talk=args.talk, timeout=args.timeout)
    try:
        service = CaptureService(store, client, talk=args.talk,
                                 auto_snapshot=False)
        snapshot = service.fetch_snapshot()
        result = service.ingest_snapshot(snapshot, source='manual')
        _emit({'talk': args.talk, 'snapshot': {
            'covers_through': snapshot['covers_through'],
            'event_count': len(snapshot['events']),
            'body_sha256': snapshot['body_sha256']},
            'ingest': result, 'coverage': service.coverage()})
    except SourceError as exc:
        _emit({'error': 'source', 'detail': str(exc)})
        return 2
    finally:
        store.close()
    return 0


def command_import(args):
    store, service = _service(args)
    try:
        imported = service.import_external_withdrawals()
        sent = service.flush_outbox() if args.flush else []
        _emit({'imported': imported, 'sent': sent,
               'pending': service.ledger.pending()})
    finally:
        store.close()
    return 0


def command_evidence(args):
    """授权人员取证：列出原始收据与字节来源，可带受限原文。"""
    store = Store(args.db)
    try:
        target = args.target
        reference = None
        if target.startswith('EXP-'):
            export = store.export(target)
            if export is None:
                _emit({'error': 'unknown_export', 'reference': target})
                return 4
            segment = export['segment']
            reference = json.loads(export.pop('receipts_json') or '{}')
        else:
            segment = target
        rows = store.receipts_for_segment(args.talk, segment)
        receipts = []
        for row in rows:
            item = {
                'receipt_id': row['id'],
                'channel': row['channel'],
                'connection': row['connection'],
                'transport_id': row['transport_id'],
                'event_type': row['event_type'],
                'version': row['version'],
                'valid': bool(row['valid']),
                'errors': json.loads(row['errors_json'] or '[]'),
                'applied': row['applied'],
                'payload_sha256': row['payload_sha256'],
                'raw_sha256': row['raw_sha256'],
                'received_at': row['received_at'],
                'restricted': True,
            }
            if not args.hashes_only:
                item['data_text'] = row['data_text']
                item['raw'] = (row['raw'] or b'').decode('utf-8', 'replace')
            if reference is not None:
                item['used_by_export'] = any(
                    evidence.get('receipt_id') == row['id']
                    for evidence in reference.values())
            receipts.append(item)
        _emit({
            'talk': args.talk,
            'segment': segment,
            'notice': '受限原始资料，仅供授权人员解释引用为何被撤下或更正；'
                      '不得作为公开导出。',
            'export': target if target.startswith('EXP-') else None,
            'receipts': receipts,
        })
    finally:
        store.close()
    return 0


def command_public(args):
    """只输出可公开引用的定稿段落，供发布流程直接使用。"""
    store = Store(args.db)
    try:
        service = CaptureService(store, client=None, talk=args.talk,
                                 auto_snapshot=False)
        state = service.observe()
        _emit({
            'talk': args.talk,
            'citable': state['citable'],
            'generated_at': state['generated_at'],
        })
    finally:
        store.close()
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog='python3 -m capture',
        description='旅游讲座字幕采集：接续、归档补领与引用撤下。')
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--db', default='capture.db', help='SQLite 状态库路径')
    common.add_argument('--talk', default='TALK-1', help='场次编号')
    common.add_argument('--source', default='http://127.0.0.1:8765',
                        help='字幕来源基地址')
    common.add_argument('--timeout', type=float, default=10.0)
    common.add_argument('--read-timeout', type=float, default=0.5)
    common.add_argument('--idle-timeout', type=float, default=60.0)
    common.add_argument('--no-snapshot', action='store_true',
                        help='窗口失效时不自动补领快照')
    sub = parser.add_subparsers(dest='command', required=True)

    capture = sub.add_parser('capture', parents=[common],
                             help='持续采集并自动重连')
    capture.add_argument('--status-port', type=int, default=None,
                         help='同时启动只读状态服务端口')
    capture.add_argument('--status-host', default='127.0.0.1')
    capture.add_argument('--no-status', action='store_true')
    capture.add_argument('--max-rounds', type=int, default=None,
                         help='最多重连轮次，用于演练；默认持续运行')
    capture.set_defaults(func=command_capture)

    observe = sub.add_parser('observe', parents=[common],
                             help='输出当前定稿、待核与覆盖情况')
    observe.set_defaults(func=command_observe)

    export = sub.add_parser('export', parents=[common],
                            help='导出一段可公开引用并冻结依据')
    export.add_argument('segment')
    export.set_defaults(func=command_export)

    acknowledge = sub.add_parser('acknowledge', parents=[common],
                                 help='标记某条受影响引用已由编辑处理')
    acknowledge.add_argument('reference')
    acknowledge.add_argument('--note')
    acknowledge.set_defaults(func=command_acknowledge)

    withdraw = sub.add_parser('withdraw', parents=[common],
                              help='登记某条已导出引用的撤下')
    withdraw.add_argument('export_reference')
    withdraw.add_argument('reason')
    withdraw.add_argument('--reference', help='外部撤下通知参考号')
    withdraw.add_argument('--local-only', action='store_true',
                          help='只记录本地，不投递编辑台接收方')
    withdraw.set_defaults(func=command_withdraw)

    withdrawals = sub.add_parser('withdrawals', parents=[common],
                                 help='列出待编辑处理的引用与撤下记录')
    withdrawals.set_defaults(func=command_withdrawals)

    snapshot = sub.add_parser('snapshot', parents=[common],
                              help='手工补领归档快照')
    snapshot.set_defaults(func=command_snapshot)

    importer = sub.add_parser('import-withdrawals', parents=[common],
                              help='读取编辑台撤下通知并投递本地待发通知')
    importer.add_argument('--flush', action='store_true',
                          help='同时把本地待发撤下通知投递到编辑台')
    importer.set_defaults(func=command_import)

    evidence = sub.add_parser('evidence', parents=[common],
                              help='授权取证：查看某段落或某引用的原始收据')
    evidence.add_argument('target', help='段落 ID 或导出引用号')
    evidence.add_argument('--hashes-only', action='store_true',
                          help='只输出摘要，不输出受限原文')
    evidence.set_defaults(func=command_evidence)

    public = sub.add_parser('public', parents=[common],
                            help='输出可公开引用的定稿段落（已撤许可者不含原文）')
    public.set_defaults(func=command_public)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
