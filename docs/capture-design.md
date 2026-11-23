# 采集产品设计说明

## 目标

现场字幕会把同一句话多次改写，也可能在嘉宾切换后仍显示上一位的识别内容。本产品让内容编辑
拿到两类东西：

1. **可引用的定稿段落**：文字已定稿、说话人已确认、许可明确允许公开，且交付时附带依据。
2. **仍在变化的草稿与待核资料**：永不进入公开导出，但可见，便于提前准备。

并要求：不等整场连接关闭才交付；连接会断、可恢复；已导出的引用在之后收到更正或撤许可时
能被列出并处理。

## 数据流

```
来源 /stream/TALK-1 ──SSE──▶ capture.client.SourceClient
                                   │ 原始字节
                                   ▼
                     capture.service.CaptureService
                        │             │
             receipts（原始留存）   Aggregate（按 segment+type 归并）
                        │             │
                        │      ┌──────┴───────┐
                        │      ▼              ▼
                        │   citable       pending_review / drafts / restricted
                        │      │
                        │      ▼
                        │   Ledger.export_citation（冻结依据）
                        │      │
                        ▼      ▼
                     SQLite（WAL）──▶ 只读状态服务 / CLI
来源 /snapshot/TALK-1 ──HTTP──▶ 快照通道（独立 receipts.channel='snapshot'）
```

## 版本与冲突

每种事件类型的业务版本只在自身类型内比较（`capture/aggregate.py`）：

- 更高版本覆盖更低版本；更低版本丢弃但保留原文，并记 `stale_event` 审计。
- 同一版本、不同内容：归并层标记 `conflicted`，该类型不参与可引用判断，段落进入
  `pending_review`；两条原文都保留。
- 同一次投递重复到达（相同通道、相同传输 ID、相同原始字节）只记一条，并留
  `duplicate_delivery` 审计。

因此后到的说话人更正能作用于先前已定稿段落，而早期快照里的旧说话人不会盖回已收到的更正。

## 可引用判定

`citable` 当且仅当：

- text 已定稿（`final=true`），
- speaker 存在且 `confirmed=true`，
- permission 存在且 `public=true`，
- 三类均无版本冲突。

其它情况按原因归类：`draft`、`pending_review`（含 `speaker_unconfirmed`、
`speaker_missing`、`*_version_conflict` 等）、`restricted`（缺许可）、
`publication_withdrawn`（许可被撤）。公开视图对这些段落**不返回文字**，
`observe`/`public` 的公开部分已如此过滤；受限原文只在 `evidence` 中提供。

## 接续与持久化

- 每条导出的原始事件保存：传输 ID、通道（stream / snapshot）、连接序号、原始字节、
  原始字节 SHA-256、解析后的正文与摘要、校验错误、归并结果、接收时间。
- 接续游标（`meta.cursor`）仅在游标前进时更新；启动时用它作 `Last-Event-ID`。
- 停止后重新运行同一条命令即从游标继续；重复投递不会产生新的业务版本。
- 每条事件落库后立即重算受影响段落的引用效力（`Ledger.refresh(segments=…)`），
  因此撤许可或更正到达时，受影响导出立刻进入待办，**无需等连接关闭**。

## 窗口失效与快照补领

来源返回 409 `replay_expired` 时：

1. 记录事件与 `floor`。
2. 取 `/snapshot/TALK-1`，原样留存快照正文（含 `covers_through` 与哈希）。
3. 快照事件按 `channel='snapshot'` 独立入库，按业务版本归并，不覆盖更新更正。
4. 用快照实际包含的传输 ID 收缩既有缺口；水位声明覆盖但快照未包含的区间仍记为
   `snapshot_incomplete` 缺口。
5. 接续游标推进到快照水位（或来源 `floor`），下一次连接继续向前。

`coverage.ranges` 把 1..N 的传输位置标为：

- `stream`：本次推送收到；
- `snapshot`：快照原文收到；
- `snapshot_declared`：仅有水位声明、未取回原文；
- `None`（列入 `missing`）：既无原文也无覆盖声明。

`interpretation` 明确提示：快照与实时流不同刻；快照之后的空白不等于没有讲话。
快照水位之后、来源 `floor` 之前无法补回的区间记 `replay_window_lost`。

## 引用效力

导出时冻结：文字、说话人、三类版本、各类型命中收据的 `receipt_id`、传输 ID、原始字节
摘要与正文摘要，并把整体依据做 SHA-256 作为 `evidence_sha256`。参考号由段落与依据摘要
导出，同一依据重复导出是幂等的。

`Ledger.refresh` 每次重算每份导出的效力：

| 原因 | 触发 |
| --- | --- |
| `speaker_corrected` | 说话人版本推进且显示名变化 |
| `speaker_unconfirmed` | 当前归属未确认 |
| `permission_revoked` | 许可被撤或缺失 |
| `permission_conflict` / `version_conflict` | 同版本冲突 |
| `text_changed` | 定稿文字与导出不同 |
| `externally_withdrawn` | 编辑台登记了针对该引用的撤下通知 |

`withdrawn` 会阻止新的无许可导出（`export` 返回拒绝与原因）。已冻结的引用内容不改写。
编辑确认处理（`acknowledge`）只对当时那组原因生效；原因再次变化会重新进入待办。

## 编辑台（可选）

来源提供 `POST /editor/exports` 与 `POST /editor/withdrawals`，仅登记收到的内容。
本产品：

- 导出后可在本地登记撤下（`withdraw`），并投递到编辑台（本地 outbox 保留回执）；
- 读取编辑台撤下通知（`import-withdrawals`），关联到本地已知引用并触发重算；
- 不依赖编辑台判断有效性，本地账本才是交付依据。

## 状态服务

`capture/status_server.py` 只读，独立进程/线程打开同一 SQLite（WAL 允许多读一写）：

- `/state`：完整观察视图；
- `/coverage`：连续覆盖、缺口、快照水位；
- `/exports`：导出历史、待办引用、撤下记录。

## 已知边界

- 单场次实例（`--talk` 可切换，但一次运行一场）。
- 状态服务不提供写操作；导出与确认走 CLI（或直接使用账本 API）。
- 授权取证（原文）只经 `evidence` 命令输出，不进入状态服务的公开视图。

## 测试覆盖

`tests/` 共 56 项，全部使用真实 HTTP/SSE 连接与窗口失效，不读内部状态替代传输：

- `test_sse.py`：逐字节切分、CRLF 断裂、多字节字符、多行 data、心跳、块级 id。
- `test_aggregate.py`：类型内版本、跨类型不可比、旧版本不覆盖、同版本冲突待核、撤许可保留原文。
- `test_capture.py`：连接未关闭即交付；停止/重启接续；窗口失效补领；快照不盖回更正；
  导出冻结与待办；外部撤下；状态服务并发读；取证命令。
- `test_external.py`：编辑台已有外部引用的登记与待核；许可冲突与撤销区分。
- `test_robustness.py`：未知事件、损坏 JSON、传输 ID 重号、重复快照、来源不可达、
  永久丢失窗口、快照原文不全。
