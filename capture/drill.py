"""演练时序：启动本地来源，按阶段驱动真实流、窗口失效与归档补领。

用法：
    python3 -m capture.drill            # 使用临时状态库，自动清理
    python3 -m capture.drill --keep     # 保留状态库并打印路径

演练只通过 HTTP 与 SQLite 公开接口观察结果，不修改来源实现。
"""
import argparse
import json
import os
import tempfile
import threading

from source.app import App
from source.common import serve

from .client import SourceClient
from .ledger import Ledger
from .model import canonical
from .service import CaptureService
from .store import Store


def show(title, value):
    print('\n== %s ==' % title)
    print(canonical(value))


def run_drill(keep=False):
    app = App()
    server = serve(app, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = 'http://127.0.0.1:%d' % server.server_port
    path = tempfile.mktemp(prefix='capture-drill-', suffix='.db')
    store = Store(path)
    service = CaptureService(
        store, SourceClient(base, read_timeout=0.2), talk='TALK-1',
        backoff=(0.01,))
    ledger = Ledger(store, 'TALK-1')
    try:
        print('来源: %s  状态库: %s' % (base, path))
        show('阶段 0：先只收前 4 条，模拟编辑在讲座中途已导出引用',
             service.run_once(max_events=4))
        export, created = service.export_citation('S-501')
        show('已导出引用（冻结文字、说话人与许可依据）', {
            'reference': export['reference'],
            'text': export['text'],
            'speaker': export['speaker'],
            'versions': json.loads(export['versions_json']),
            'evidence_sha256': export['evidence_sha256'],
            'created': created,
        })
        show('阶段 0 观察：可公开引用与待核资料',
             _digest(service.observe()))

        print('\n把来源切到阶段 1：重放窗口 floor=8，旧位置 4 失效；'
              '8 之后有说话人更正与撤许可。')
        app.stage = 1
        result = service.run_once()
        show('阶段 1 第一次连接：窗口失效，已自动补领归档快照', result)
        coverage = service.coverage()
        show('连续覆盖说明（快照水位 8，与实时 tail 不同刻）', {
            'cursor': coverage['cursor'],
            'snapshot_covers_through': coverage['snapshot_covers_through'],
            'ranges': coverage['ranges'],
            'missing': coverage['missing'],
            'interpretation': coverage['interpretation'],
        })

        result = service.run_once()
        show('阶段 1 第二次连接：从快照水位续接，收到更正与撤许可', result)
        digest = _digest(service.observe())
        show('阶段 1 观察：S-501 已撤许可、S-502 归属已确认', digest)
        show('受影响的已导出引用（编辑仍需处理）', ledger.pending())

        show('尝试为已撤许可的 S-501 再次导出', _try_export(service, 'S-501'))
        show('受限原始事件仍留存（仅摘要，原文用 evidence 命令取）',
             [{'receipt_id': row['id'], 'transport_id': row['transport_id'],
               'event_type': row['event_type'], 'version': row['version'],
               'raw_sha256': row['raw_sha256'][:16] + '…'}
              for row in store.receipts_for_segment('TALK-1', 'S-501')])

        print('\n把来源切到阶段 2：快照补至 11。')
        app.stage = 2
        snapshot = service.fetch_snapshot()
        ingested = service.ingest_snapshot(snapshot, source='stage-2')
        show('阶段 2 补领：快照覆盖到 11', ingested)
        coverage = service.coverage()
        show('阶段 2 连续覆盖说明', {
            'snapshot_covers_through': coverage['snapshot_covers_through'],
            'ranges': coverage['ranges'],
            'missing': coverage['missing'],
            'interpretation': coverage['interpretation'],
        })
        show('重复入库被识别为已投递，不产生新版本', ingested['duplicate'])

        ledger.acknowledge(export['reference'], '已通知编辑撤下该引文')
        show('编辑确认处理后的待办清单', ledger.pending())

        show('审计：每次公开导出与状态变化的依据',
             [{'kind': item['kind'], 'reference': item['reference']}
              for item in store.audit_log(limit=12)])
        return path
    finally:
        server.shutdown()
        server.server_close()
        store.close()
        if not keep:
            for suffix in ('', '-wal', '-shm'):
                try:
                    os.unlink(path + suffix)
                except FileNotFoundError:
                    pass


def _digest(observation):
    return {
        'coverage': observation['coverage']['ranges'],
        'missing': observation['coverage']['missing'],
        'citable': observation['citable'],
        'pending_review': observation['pending_review'],
        'drafts': observation['drafts'],
        'restricted': observation['restricted'],
        'pending_exports': observation['pending_exports'],
    }


def _try_export(service, segment):
    try:
        export, created = service.export_citation(segment)
        return {'reference': export['reference'], 'created': created}
    except Exception as exc:
        return {'refused': type(exc).__name__, 'reasons': getattr(exc, 'reasons', None)}


def main(argv=None):
    parser = argparse.ArgumentParser(prog='python3 -m capture.drill')
    parser.add_argument('--keep', action='store_true', help='保留演练状态库')
    args = parser.parse_args(argv)
    path = run_drill(keep=args.keep)
    if args.keep:
        print('\n演练状态库保留在: %s' % path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
