# 字幕来源协议
场次 TALK-1。GET `/stream/TALK-1` 返回真实UTF-8 SSE连接，事件可跨任意传输字节，CRLF、多行data和注释心跳均有效；有限演练流主动关闭用于模拟断线，关闭不表示讲座结束。Last-Event-ID指定已持久采到的传输事件ID，本演练ID为十进制连续整数，实际保留窗口外返回409 replay_expired及snapshot地址。仅id用于传输覆盖，event为text/speaker/permission，data拼接后为JSON。
每种事件按segment与类型各自的version比较，不能跨类型比较版本。text的final决定字幕是否定稿；speaker的confirmed决定归属是否确认；permission的public决定是否允许公开引用。相同业务版本内容冲突要保留待核，较旧版本不能覆盖新版本。没有明确public=true不能公开；撤销许可不删除受限原件。一个段落只有定稿、归属确认且有公开许可时才可成为公开引用。
GET `/snapshot/TALK-1` 返回talk、covers_through和events，是覆盖从1到covers_through的归档事件原件；不承诺与实时源tail同刻，调用方需解释交界连续性。快照自身完整，但它之后的范围可能已在推送中变化，不能将缺口视作无讲话。原始事件可受限留存，普通公开导出须筛掉无许可文本。
编辑台是可选的本地接收方：POST `/editor/exports` JSON reference、segment、text_version、speaker_version、permission_version，GET同路径查历史；POST `/editor/withdrawals` JSON reference、export_reference、reason登记撤下通知。接收方只存提交内容，不替采集方判断有效性，received不是编辑已处理。产品也可用自身交付引用管理，需保留导出依据和影响清单，不强制使用这两个接收方接口。
检查人员X-Operator: local-operator POST `/__control` JSON stage：0源tail8但快照仅至4，1重放floor8、tail11、快照仅至8，2快照补至11。stage1旧接续位置将失效，8之后有说话人更正与撤许可。`/__inspect`仅检查人员可用，不代替流或快照获取。

## 采集示例（从零演练）

在仓库根目录，确认 `python3 -m source.app --port 8765` 已运行，然后：

```bash
python3 -m capture capture --db /tmp/talk.db --max-rounds 1   # 阶段 0：收到 1–8
python3 -m capture export  --db /tmp/talk.db S-501            # 冻结一条引用
curl -s -X POST http://127.0.0.1:8765/__control -H 'X-Operator: local-operator' \
     -H 'Content-Type: application/json' -d '{"stage":1}'     # 窗口 floor=8
python3 -m capture capture --db /tmp/talk.db --max-rounds 1   # 第一次：409 + 快照补领
python3 -m capture capture --db /tmp/talk.db --max-rounds 1   # 第二次：收到 9–11
python3 -m capture withdrawals --db /tmp/talk.db              # 旧引用已列为受影响
python3 -m capture snapshot    --db /tmp/talk.db              # 阶段 2：快照补至 11
```

`capture.drill` 会自动完成以上时序并打印观察结果，无需手工切换阶段：

```bash
python3 -m capture.drill
```
