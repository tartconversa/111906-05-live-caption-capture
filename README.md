# 旅游讲座实时字幕的归档接续与引用撤下

本项目提供：

- 来源服务 `source/`：隔离的本地合成字幕来源（SSE 推送 + 归档快照 + 可选编辑台接收方），所有姓名、机构、费用均为演练数据。
- 采集产品 `capture/`：从来源持续采集原始事件，维护可引用的定稿段落、待核草稿、连续覆盖说明、引用依据与撤下清单。可停止后恢复。

采集产品不等整场连接关闭才交付：每条事件落库后立即重算内容，编辑随时可读取已确认的定稿、正在变化的草稿与引用效力。

## 环境与运行

需要 Python 3.11（`python3 --version`）。无第三方依赖：

```bash
python3 -m pip install -r requirements.txt   # 仅声明标准库
```

启动本地来源：

```bash
python3 -m source.app --port 8765
```

另开终端启动采集（会自动重连，并在窗口失效时补领归档快照）：

```bash
python3 -m capture capture --db talk.db --source http://127.0.0.1:8765 \
    --status-port 8766
```

状态查询只读，便于编辑台轮询：
`http://127.0.0.1:8766/state`、`/exports`、`/coverage`。数据库为 SQLite（WAL），
采集进程停止后重新执行同一条 `capture` 命令即从上次接续位置继续。

常用命令：

```bash
python3 -m capture observe   --db talk.db      # 定稿 / 待核 / 草稿 / 受限 / 覆盖说明
python3 -m capture public    --db talk.db      # 只输出可公开引用的段落
python3 -m capture export    --db talk.db S-501 # 导出一段引用并冻结依据
python3 -m capture withdrawals --db talk.db    # 受影响引用与撤下记录
python3 -m capture withdraw  --db talk.db EXP-… '撤下原因'
python3 -m capture acknowledge --db talk.db EXP-… --note '编辑已处理'
python3 -m capture evidence  --db talk.db S-501 # 授权取证（含受限原文，`--hashes-only` 只看摘要）
python3 -m capture snapshot  --db talk.db      # 手工补领归档快照
python3 -m capture import-withdrawals --db talk.db --flush  # 同步编辑台撤下通知
```

`import-withdrawals` 也会登记编辑台中已有、但非本产品导出的引用（`origin=editor`），
把它们列为 `external_export_unverified` 待核项；它们缺少冻结的原文与收据，产品不会
擅自判定其有效。

## 语义要点

- **传输事件 ID 只用于接续**，不代表段落版本。业务新旧只比较同一段落同一类型
  （text / speaker / permission）的 `version`。
- 段落只有 **定稿 + 说话人已确认 + 明确 `public=true`** 才能成为公开引用。
  未定稿、未确认或版本冲突只进入 `pending_review` / `drafts`，不具备引用效力。
- **撤销许可不删除原文**。受限原文仍以原始字节留存在 `receipts` 中，授权人员可用
  `evidence` 命令解释为何撤下；公开视图（`public`、`observe` 的 `citable` 等）不会包含它。
- 快照与实时流**并非同一时刻**。`coverage.ranges` 按 `stream` / `snapshot` /
  `snapshot_declared` / 缺口区分区间，`missing` 只列真正未知的位置；未知不等于没有讲话。
- 快照按业务版本归并，**早期快照中的旧说话人不会盖回**已经收到的更正。
- 每次导出冻结文字、说话人、三类版本、命中的原始收据 ID 与哈希；之后的状态变化只把
  该引用标为 `affected` / `withdrawn` 并给出原因，不改写已导出的事实。待办清单
  （`pending_exports` / `withdrawals`）列出尚未由编辑确认处理的受影响引用。
- 来源在窗口外返回 409 时，采集器取归档快照补领；快照也到不了的区间记为
  `replay_window_lost`，不会说成“没有讲话”。

## 检查

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q source capture tests
python3 -m capture.drill            # 三阶段演练：真实流、窗口失效、快照补领、引用撤下
```

测试全部通过真实 HTTP/SSE 连接、窗口失效与归档补领观察结果。来源协议细节见
[docs/source-contract.md](docs/source-contract.md)，产品设计说明见
[docs/capture-design.md](docs/capture-design.md)。
