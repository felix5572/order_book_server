# Fork 改动账本(felix5572/order_book_server)

本仓库 = 我们严肃维护的 order_book_server fork。git 关系:
  origin   = git@github.com:felix5572/order_book_server(本 fork)
  imperator = imperator-co/order_book_server(**实际上游**, 活跃维护, 我们跟它)
  official = hyperliquid-dex/order_book_server(官方样例, 基本停更)
上游同步:`git fetch imperator && git merge imperator/main`(我们有自有提交, 正常 merge, 逐处解冲突)。
回馈 PR:2026-10 起不再尝试 —— imperator 不受理外部 PR(可能出于安全考虑), 只单向合并上游。
主仓库 hyperliquid-trade 里的 order_book_server/ 已删除(本地留 gitignore 软链接方便工具)。

## 底座:imperator-co @ 47ce696(2026-06-30)— 2026-07-05 换入

原先基于官方样例自维护的旧 fork(下方"旧底座历史")已整体退役。imperator 是生态验证人
(官方 root peer 运营方), 2026-03~06 持续迭代 28 commits:
数据丢失记账体系(mark_desynced 按原因分类 + max_loss_height + 覆盖快照才清除)、
per-oid 双向 pending 配对(取代块级对齐)、parallel watchers + 有界 partial_line、
metrics.rs、心跳、OOM 修复、共享渲染帧、per-price-level 聚合、trades WS 带双方地址
(counterparty 实时流!)、bookDiffs 订阅、--no-resync 漂移容忍、自带 229 测试、含 yawc。

## 2026-10-09 合并 imperator 至 5710c9d(18 提交, 2026-07-15..08-17)

上游带来、与我们相关的:
- **粗精度 L2 先聚合整本再截断**(b083c18):此前粗 `nSigFigs` 变体从"截断到 MAX_LEVELS 的原始档"
  再分组, 深处的档丢失(实测 CASHCAT 3 位 20 档只到 ~1385bp, 按档宽应 ~1740bp)。现与官方 API 同语义,
  上限按聚合后的档数计。
- **重同步**:永久重同步循环修复(insertBefore 回退只计指标不再打失步, 5710c9d)、重同步抑制与
  期间保持响应、重放缓存按峰值事件率可调(新参数 `--replay-cache-events`, 默认 400 万 ≈ 2–4GB)。
- ALO 优先级 `insertBefore` 队列锚点、pong 与广播解耦(每连接写任务)、按订阅者决定是否构建广播、
  未触发条件单 `GET /untriggeredOrders`、L4 价格带 `GET /l4Book`、若干性能项。

冲突与语义交叉的处理(测试 244 → 309 全过):
- 待配对 New diff 缓存 = `(sz, diff px, insertBefore, 时间)`:保留官方 PR#9 移植(入簿价取 diff px)
  的同时带上锚点;HIP-2/援助基金合成单(PR#10 移植)同样按锚点入队并记回退指标。
- oracle 旁路:订阅不登记任何受控广播族(监听器有接收者就广播);推送改走上游的每连接 outbound。
- metrics 两边合并;我们"迟到回填不抬高丢失边界"测试与上游"重同步抑制"测试并存。
- **文件切换排空(审查 000212 P2)**:上游新增的 8MiB 分块读取与"读到零行即停"的切换排空循环不兼容
  (上游自身同样如此):一块落在长行内部时返回零行但位置已前进, 随即切换会清掉 partial_line、静默丢掉
  旧文件尾部且不标失步。改为"读取位置不再前进才停", 并补回归测试。
- 上游测试按我们的改动调整:粗聚合测试的卖单价放到所有买单之上(我们 MAX_LEVELS=400 会让买单越过
  原卖单成交);价格带测试的 diff 带上价格(PR#9 语义下入簿价取 diff px)。

同批我方改动:
- **`bbo` 线上格式改为官方 `WsBbo`**:`{coin, time, bbo: [bid|null, ask|null]}`(原为 `bid`/`ask`
  两个字段)。理由:hl_md_gateway 拿本服务与官方 API 赛跑并原样转发, 两种格式会让下游拿到两套
  wire。仓库内消费者(网关、qos_monitor)只读 coin/time, 不受影响。
- 块内中间态照旧逐事件推送(用户 2026-10-09 接受:HL 先挂后吃, 消费侧看往外的变化即可)。

官方仓库(hyperliquid-dex)同日核对:基线后只合并了 #12(insertBefore), imperator 已自行实现并随本次
合并带入;开放中的 #15(只算用到的 L2 形状)已被 imperator 的按订阅计算覆盖, #13/#14/#11 不需要。

## 2026-10-10 配对缓存按流进度淘汰(设计 000249 方案 A)

现象:节点卡顿 1.5–4 分钟后追赶期间, 网关内容对账 2 小时内 104 段持续差异, 全部是本服务比官方少单。
逐单追踪:节点把同一张单的 open 状态与 New diff 同一时刻写出, 本服务始终没让它入簿, 缺到它成交/撤单。

根因:imperator 的逐单双向配对(上文"取代块级对齐")靠两张缓存等另一半, 淘汰用墙钟 60s 和 5 万/1 万上限,
前提是"两条流相差毫秒"。节点侧确实如此(同块写入时差 ≤35ms), 但追赶时两个 watcher 在进程内拉开
几分钟, 缓存从 ~200 涨到 3–4 万, 在途的一半被扔掉 → 单永不入簿 → 失步 → 等检查点期间推缺单的盘口。

改动:
- 主网两个时间窗(各约 100 万张入簿单)核验:状态与 New diff 必在同一区块, 0 例跨块。据此缓存条目记区块号;
  另一条流应用到**更大**区块(一块有数百行, 严格大于)才判定配不上:状态 = 孤儿(计数, 不算丢失),
  New = 丢数据(失步)。等多久不再重要。上限 100 万只作防内存保险。`zeroed_awaiting_remove` 单流、只影响计数, 墙钟不变。
- l2/bbo 帧时间 = 两条流都已应用到的块时间(较小者):是进度标识, 不是按块一致切面(领先 diff 流的
  Update/Remove 已入簿)。L4 快照与 untriggered 仍用最远进度, 它们的 (time, height) 是 L4 diff 流的衔接点。
- 新指标 `orderbook_stream_height{stream}`、`orderbook_stream_skew_blocks`、`orderbook_pending_orphans_evicted_total`;
  孤儿清理日志降为 debug(原 INFO 每次清理一条, 冲掉 tmux 滚动缓冲)。
- 两个 watcher 为何会拉开仍未查明(当时日志已被滚动缓冲冲掉), 新指标会给出直接证据。

## 2026-10-10 已知丢数据期间停供盘口派生输出(设计 000249 方案 B)

原行为(imperator):判定丢数据后只安排重同步, 等检查点期间(实测 6–10 分钟)照常推缺单的盘口,
订阅方无从得知。网关的主备切换只看传输与进度, 识别不了(它的文件头写明了这条边界)。

改动:
- `book_trusted() = !(needs_resync && resync_data_loss)`;所有重同步都由丢数据触发, `--no-resync` 不打标所以永远可信。
  listener 持锁时在 `mark_desynced` / `finish_install` 末尾把变化发布到共享原子镜像 `BookTrust`(不用全局:并行测试会串)。
- 不可信时停:bbo 即时推送、l2 节流推送(dirty 继续累积, 覆盖安装后整本重建)、每连接心跳(会把旧帧盖上当前时间)、
  WS l4Book 订阅快照(返回错误并撤订阅)、`GET /l4Book` 与 `GET /untriggeredOrders`(503 `book resyncing after data loss`,
  不用空集合冒充)。照常:trades、bookDiffs、orderUpdates、l4Book 增量、oracle(原始事件, 不依赖盘口状态)。
- 两个 HTTP/l4Book 快照读:查缓存前读镜像, 锁内再复核一次(请求可能在等许可/锁时被打标);缓存 TTL 从锁内取快照时起算,
  序列化完已过期就不入缓存(审查 000249 r2 P3)。已过闸的在途请求/已入队的帧可能带原 time/height 完成, 不撤回。
- 下游:网关主路 2s 无帧即 Silent 切官方, 无需改网关;bm 上研究录制、QoS 监控在停供期间看到静默(预期, 可见)。
- 指标 `orderbook_book_trusted`(1/0)、`orderbook_untrusted_seconds_total`;转不可信打 error(原因 + 损失高度边界), 恢复打 info(时长)。
- 审查 000251 两处接线修正:① `BookTrust` 带"已开始的停供次数", 每个连接见到它变了就清掉自己的 l2/bbo 去重+心跳缓存并强制
  下一帧整本重评——否则恢复后心跳会把停供前的旧帧盖上当前时间重发, 去重也可能吞掉同价的恢复首帧(连接即使没赶上停供窗口也成立);
  ② 追赶放弃、丢弃残余缓存的安装把重标做进 `finish_install` 的最终判定, 镜像不再出现 假→真→假 的瞬间恢复信号。
  ③(000251 r2)`Snapshot` / `BboUpdate` 消息带生成时的停供次数, 连接只接受与当前次数一致的帧:停供前生成、停供中或恢复后
  才出队的旧帧直接丢弃, 不发送也不回填缓存(否则清空后又被它填回, 恢复后照样被心跳重发)。trades、L4 原始事件不受影响。

## 2026-10-10 日志去刷屏 + 落盘(设计 000252 第六节)

- "State progress" 原为每 1000 个批次一行(主网约每秒 18 行, 占全部输出约 90%), tmux 2000 行缓冲只留约 47 秒,
  事后无法复盘。改为每 10 秒至多一行(内容不变); 同处的指标与待配对清理仍按每 1000 批次执行。
- `start-server.sh`: `set -o pipefail`(保留服务退出状态), 经 `tee -a` 同时写
  `~/ob_logs/orderbook_server_<UTC 启动时刻>.log`, 启动时只删本工具 30 天前的旧日志; 慢盘/终端会反压管道, 与原先前台输出同类。

## 2026-10-10 孤儿计数排除"进场即了结"的单

**现象:** A 上线后, `orderbook_pending_orphans_evicted_total` 稳态每秒涨 1–3。

**主网抽样(175 块, 38 条未配对):**
- 全是"同一块内先 `open` 后 `filled`、没有任何 diff"的单, 如前端市价单、可立即成交的 Gtc;
- 早先另一批样本(181 块)另有 4 条 `triggered` 未配对, 其后续状态未逐条核;
- 0 例 New 落在别的块。

**原因:** `is_inserted_into_book`(官方初始提交)只看 `open` 且非 Ioc, 会把这类单放进待配对缓存。
它们从未挂上盘口, 被清掉是对的, 但混进孤儿计数, 淹没了真正的信号:
"状态到了、New diff 始终没来"只能由这个计数发现。

**改动:**
- 缓存中的状态, 同一块里又来该 oid 的非入簿状态 → 标记"本块已了结";
- 不提前删, 先挂上又在同块成交的单, New diff 照样能配上;
- 淘汰改为两条流都越过该块之后(审查 000255 r1:diff 流领先时, 同块后一行的 `filled` 可能还没读到),
  只把未标记的计为孤儿; 状态流本就到过该块, 只多等到它读完这一块;
- 测试用逐 state 镜像计数 `orphan_statuses`, 与 `missing_diff_targets` 同一做法。

**稳态应为 0, 上涨即 New diff 丢失。**
已知盲区: 先挂上、又在同块了结, 且 New diff 恰好丢失的单, 会被当成已了结而不计。

## 2026-10-10 节点重启重放时不刷屏、不重复计失步

**现象**: 盘口服务不重启、只重启节点时, 21:19:31–21:21:35 刷了 11,612 行 `Evicted N pending_new_diffs ... data loss`,
`orderbook_desyncs_total{reason="pending_cache_cleared"}` 也随之涨了 11,612。

**原因**:
- 节点从保存的状态往回重放, 把已经写过的块再写一遍(这次从约 1179092806 开始)。
- 盘口服务此前已处理到 1179108317。重放的 New 若在清理时还没配上状态, 按 A 的规则(状态流的最高块号已越过该块)立即判丢失;
  能与重放状态正常配对的不走这条路。
- 每次清理(每 1000 批)打一行, 并标一次失步。

**停推从哪来(审查 000257 r1 P3)**: 这次是节点重建文件时, watcher 先报 `watcher_data_loss` 并停推,
重放期间盘口一直不可信, 下游无影响。高水位下仍未配对的重放 New 在清理时继续判丢失, 本批不改这条规则,
只限制日志和重复标记。单调最高块号本身不是通用的倒带检测器, 不能据此保证"所有倒带都会停推"。

**改动**:
- 丢失日志限速: 每 10 秒至多一行, 写明本次条数和本盘口安装以来的累计数(`lost_new_diffs`)。
  限速函数 `progress_log_due` 改名为 `throttled_log_due`, 与进度日志共用。
- `mark_desynced`: 若盘口已处于"已知丢数据、等待覆盖性重同步", 且本次的丢失上界没有超出已记录的上界,
  这次标记不改变任何状态, 不再计数。新的失步, 或丢失上界往后推的, 照常计数并抬高上界。
- 测试:
  - 重放块低于状态流最高块号、仍未配对的 New 在清理时判丢失并累计;
  - 上界内重复标记不计数, 越过上界则计数并抬高上界(用独立的 reason 标签, 避开进程全局计数器的并发干扰)。

## 已移植(2026-07-05, 底座 47ce696 之上)

1. **官方 PR#9:新单进簿价用 diff 的 px**(status px 对 trigger/转化单可能不同)。
   两个到达顺序都覆盖:diff 先到 → `pending_new_diffs` 由 (Sz,Instant) 扩为 (Sz,Px,Instant);
   status 先到 → 配对时 `modify_px(diff px)`。px 解析失败 = schema 漂移 → Err fail-fast
   (不静默回退 status px)。`InnerOrder` trait 增 `modify_px`。
2. **官方 PR#10:HIP-2(0xFF..FF)/援助基金(0xFE..FE)合成单**。这类单永远没有 order
   status 事件;在 imperator 的 pending 模型下会挂满 60s 被当 data loss 驱逐并**触发
   resync**,且 spot book 长期缺系统做市商流动性。遇其 New diff 直接构造 Alo 限价单入簿。
   `NodeDataOrderDiff` 增 `side` 字段(实测 raw diff JSON 自带)。
   两者均带专属单测(state.rs "我方移植语义"节, 共 3 个);这两个修复适合回馈 PR 给 imperator。

3. **oracle 更新链(2026-07-05 移植完成)**——我方独有功能, 旧 fork 重写版:
   - 源:`hip3_oracle_updates_streaming`。**修正记录**:最初接 by_block(当时节点未开
     streaming, 它是唯一块级形式);节点切 `--stream-with-block-info` 后 *_by_block 全家
     停更、oracle 改写 streaming 目录 —— 已随之切换, **刻意不做 by_block fallback**
     (停更目录仍存在, fallback 会静默读旧数据)。信封 schema 两者相同, 解析不变。
     第四个并行 watcher, 不参与 backfill。
   - **旁路隔离(硬约束)**:oracle 是 side stream, 其 watcher 丢失/超大批/解析失败
     **绝不触发 orderbook resync**——独立计数 `obs_oracle_data_loss_total` +
     PARSE_ERRORS_TOTAL["oracle"], warn + skip(旧 fork 的解析 panic 已弃:panic 会把
     L2/BBO 一起带死)。不进 replay cache。
   - 分发:listener 内一次展平 `oracle_updates_by_coin`(spot/mark/oracle 三维按 coin
     合并)→ `InternalMessage::OracleUpdates` 广播;订阅 `{"type":"oracle","coins":[..]}`
     (校验=非空 + HIP-3 `dex:COIN` 形态;**刻意不绑 book universe**——deployer 推价与
     有无挂单无关, 如 flx:XMR 无簿也有 oracle;无更新的 coin 只是无帧), 推送
     channel="oracleUpdates", per-coin
     `SimplifiedOracleUpdate{coin,time(ms),height,markPx,oraclePx,spotPx}`(低频,
     不用共享帧机制)。**与旧 fork 的 wire 差异**:block_time 字符串 → time 毫秒 u64。
   - 测试 4 个:实机真实行解析(Mainnet 2026-07-04 样本, 含空 events 行)/三维展平
     合并/订阅校验(空列表拒绝+线上格式)/响应 channel 序列化。
   - review 修正:oracle 批**不推进 `last_seen_height`**(否则旁路流会抬高 book 的
     丢失恢复边界, resync 会等一个 book 流从未产出的高度)。

## 部署(nube 节点机)

`./start-server.sh`(direct 快照模式, 参数透传)。前提:节点开
`--stream-with-block-info`(*_streaming 目录)+ `--write-hip3-oracle-updates`。
探针:`tests/test_l4_websocket.py`(旧 wire 格式, 待按新订阅面翻新)。
py 运维入口(install-service/status)后议。

保留的我方文件:docs/(本账本 + HL_DATA_STRUCTURES.md)、analyze/hft_flow_monitor.py、
tests/test_l4_websocket.py、start-websocket.sh。

---

# 旧底座历史(已退役, 仅供参考)

# Fork 改动账本(vs 官方 hyperliquid-dex/order_book_server)

维护本 fork 的单一事实源:改了什么、为什么、和官方/社区 PR 的关系。
官方上游基线:2025-09(#4 yawc 合并)后基本停更;本 fork 底座取自 yawc 之前的版本。
review 记录:2026-07-05(全文件 diff 对照 + 上游全部 open/closed PR 评估)。

## 一、fork 相对官方的核心改动(2026-01 起的迭代)

### 1. 半行缓冲(修官方真 bug)— `listeners/order_book/mod.rs process_data`
官方逐行 parse,读到 writer 尚未写完的半行 → serde 失败 → seek 回退重试整批。
fork:每个 event source 各持 `pending_line_*`,无尾换行的末行暂存、下次读取时拼接;
拼接后首行仍 parse 失败 → 显式报"数据损坏"错误(不静默丢)。

### 2. 时序对齐重写(价值最大)— `pop_cache`
官方把 order_statuses / raw_book_diffs 两队列按块高三路比较,**高度不等直接丢弃较小侧**
→ IO 抖动即静默丢 batch → 状态漂移 →"凌晨 Orders do not match 崩溃"。
fork:高度不等时**等待**(不丢);gap>5 warn、gap>100 panic;相等时该高度两侧事件全量
合并弹出,**空块也返回**保证 height 连续推进不误报 gap。权衡:数据完整性 > 极致低延迟
(正常时零等待,只在 IO 抖动时等 50-200ms)。

### 3. 自愈替代崩溃(fail-open 行情服务语义)
- 快照校验失败:官方 return Err 服务崩;fork warn + 清 `order_book_state` → 10s 后下轮
  快照重建。
- `state.rs` 块 gap:官方 Err;fork warn 继续,且 `height = height`(官方 `+= 1` 在 gap 后
  会永久错位,这是跟进修正)。
- diff 找不到对应订单:官方 Err;fork warn + skip。
- **消费者须知**:这些 skip 意味着 book 可能带错误状态继续服务,靠 10s 快照校验兜底;
  两次校验之间读到的 book 可能是错的。研究/展示用途可接受;不要直接当做市定价输入
  (我们的 fair 层不依赖它)。

### 4. Oracle 更新全链(fork 新增功能)
新 EventSource 吃 `hip3_oracle_updates_by_block`,订阅类型 `Oracle{coins}`,推送
`SimplifiedOracleUpdate`(mark/oracle/spot px)。oracle 解析失败**刻意 panic**
(fail-fast;与 fills 的宽松不对称是有意的:oracle 稀疏且 schema 稳定,错了必须立刻知道)。

### 5. 周边
pub 可见性放开(供外部集成)、alloy 1→2、`docs/HL_DATA_STRUCTURES.md`(TWAP/fill 实测
schema)、analyze/tests 脚本、ticker 节奏 5s/10s→8s/5s。

## 二、2026-07-05 review 落地的修复

### 2a. 饥饿护栏(本 fork review 发现的新洞)— `pop_cache`
gap>100 panic 只覆盖"两侧都有数据但高度错位";若一侧文件流**彻底停写**(watch 丢失/
flag 关闭),另一侧无限堆积且永远走不到高度比较 → 内存无界增长。
修复:`MAX_CACHE_BATCHES = 5000`(~14 块/s ≈ 6 分钟单侧无数据)超限 panic。

### 2b. 吸收上游 PR#9(closed 未合并,但修复是真的)
- **新单进簿价用 book diff 的 px**,不用 order status 的 px(trigger/转化单两者可能不同,
  用 status px 会造成后续 diff 对不上 —— 正是 fork 里 skip 掉的那类 mismatch 的根因之一)。
  `InnerOrder` trait 增 `modify_px`。**比 PR#9 原版更严**:px 解析失败直接 `?` 崩
  (原版 if-let 静默回退 status px,schema 漂移时会重新引入错误进簿价)。
- 校验"空簿 ≡ 不存在" + **两侧非空分歧都计入 mismatch 触发自愈**:extra 侧只报非空 extra;
  missing 侧(本地有非空 book 而 expected 没有)原本只 warn 不触发自愈,同批修为对称计入。

### 2c. 吸收上游 PR#10(open)— **对我们的 @260 现货直接重要**
HIP-2(spot 系统做市商 `0xFF..FF`)与援助基金(`0xFE..FE`)的单**只出现在 raw_book_diffs,
永远没有 order status 事件**。官方遇到即崩;fork 此前降级为 skip —— 意味着**重建的 spot
book 一直缺 HIP-2 流动性**。修复:`NodeDataOrderDiff` 增 `side` 字段(实测 raw diff JSON
自带)+ `special_address()`,遇特殊地址的 New diff 按 diff 直接构造 Alo 限价单插簿。
- 附带:reqwest 显式开 `json` feature(此前靠依赖图偶然开启,单独构建会断)。

## 三、上游其余 PR 评估(不吸收的及理由)

| PR | 内容 | 结论 |
|---|---|---|
| #11(open) | serve-info fileSnapshot 兜底 + 端口修正 + 并发快照防护 + spot | 我们节点开着 periodic_abci_states,fallback 不需要;"防并发快照 + 超时"的思想好,等真跑出问题再取 |
| #5(closed) | 快照操作 watchdog 超时包络 + Slack 告警 | 卡死检测思想可取,但我们外层已有 node_status lag 报警兜底;Slack 不要(我们走 TG) |
| #6(closed) | inactivity_exit_secs 可配 | 小甜点,等真要调时再收 |
| #2(closed) | custom dir + 未完成的逐行处理 | fork 的 pending-line 更完整,略过 |
| #4(merged) | yawc WS(压缩) | **未跟**。将来把本 server 对外供流时值得合并评估 |

## 四、已知欠账

- clippy 全仓 ~56 条 lint(fork 历史欠账,未清)。
- 自愈语义(§3)未在 README 声明。
- 无单元测试覆盖 pending-line / pop_cache 对齐语义(重构时最该先补的两块)。
- 上游 yawc 未合并(见 §三 #4)。
