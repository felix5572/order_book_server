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

**补充: 重放状态不计孤儿**(同日, 用户定与上面一起部署)
- 同一次重放让 `orderbook_pending_orphans_evicted_total` 从 0 涨到 3,984,362。
  原因: 重放的 New 先到, 已被当丢失清掉; 状态后到, 配不上, 于是计成孤儿。
- 判定: 块号低于状态流最高块号的状态, 视为重放(一次运行内状态流从不倒退, 1.28 亿行核过)。
  这类状态标 `replayed`, 被清掉时不计孤儿, 仍可正常配对。
- 失步由 New 侧的丢失判定负责, 这条不变。
- 孤儿计数的目标回到"稳态为 0, 节点重启也不跳"。

## 2026-10-10 l2 推送改为"簿流静默即推"(设计 000260)

**现象**(bm 只读探针, ETH):
- 本地 l2 比节点自身延迟慢 25–40ms。
- 文件写完到收到 l2: p50 36ms、p90 54ms; 同路径的 bbo 约 0ms。
- 读文件、解析、更新盘口、生成 l2 都在 1ms 以内(l2 构建平均 0.63ms)。

**原因**: imperator 的 `L2_BROADCAST_THROTTLE_MS=50` + `L2_FLUSH_TICK_MS=10`, 实际节拍 60ms。
- 流模式下一个块是 1–20ms 的一串行。
- 前几行就触发推送并占掉节流窗口, 块写完后还要再等 50–60ms。
- 实测推送数是块数的 1.2 倍, 时间线呈固定 50/60ms 节拍。

**改动**(新模块 `listeners/order_book/l2_conflation.rs`, 待推币集合与推送时机归同一个 owner):
- **推送条件**: 有脏币, 且满足任一:
  - diffs/statuses 两条流静默 2ms(合并窗口, 不是"块已写完"的证明);
  - 脏币等待满 20ms。用户定 20ms 而非 50ms; 它只在行连续不断(节点追块)时生效。
- **主循环**: 10ms ticker 换成 `sleep_until(截止时间)`(无截止时间时挂起)。截止时间在每批事件后、每次唤醒后, 于锁内刷新。
- **不会空转**: 没有接收方或没有 l2 订阅时保留脏币, 按 20ms 复查(审查 000260 r1 P2)。
  复查时刻独占截止时间, 不让早已过去的静默截止时间一直到期。
- **不可信时**: 无截止时间; 安装时清空。
- **新指标**: `l2_flush_total{trigger=quiet|max_delay|recheck}`。

**取舍**:
- 约 5% 的块中间有超过 2ms 的停顿(最长 16.6ms)。这些块会先推一个中间状态, 静默后再推完整状态。
  网关按"同源同块的更晚一帧"转发修正帧。
- 没有采用 `node_fast_block_times` 那一行作块结束标记, 原因:
  - 尾部更慢(p99 晚 15–18ms);
  - 多一个按天切换的文件依赖;
  - 依赖未写入文档的写入顺序(529 次中有 1 次早 0.36ms)。
- 传输层不改:
  - 文件可见延迟 p50 0.2ms;
  - bm→trade RTT 2.3ms, 经 Caddy 只多约 0.1ms;
  - Unix socket/共享内存跨机器用不了, 本机只省微秒。

**预期与验收**:
- 本地 l2 − 节点延迟从约 40ms 降到约 10ms。
- 同一探针复测"文件写完 → 收到 l2", p50 应小于 5ms。
- 网关内容一致率不下降。
- 正常行情下 `max_delay` 触发占比接近 0。

**上线实测(10-10 22:46)**:
- 文件写完 → 收到 l2: p50 36.5 → 8.9ms, p90 54 → 15ms。
- 本地 l2 由比官方慢约 26ms 变为快约 4ms。
- 但 `max_delay` 触发约占 16%, 每块推 1.69 次。说明盘口服务内部看到的停顿和文件侧不同, 剩余延迟待拆。

**观测补充**: 新指标 `node_line_lag_seconds{stage=read|apply|publish}`, 从节点写行(`local_time`)起算, 到各阶段的累计滞后:
- read: 读文件线程读到, 每次读取取最新一行;
- apply: 这一批已应用到盘口, 每个簿批次一次;
- publish: l2 已构建并交给广播通道, 取最新已应用的簿行, 不含发到 socket。

三者取样集合不同, 只能并排对比各入口的累计滞后来辅助定位。分位数之差不是各段耗时(审查 000265)。

## 2026-10-10 价位内存整理改为删单时就地进行(去掉每 1000 批一次的全量扫描)

**定位**(000265 的分阶段延迟 + perf):
- 节点写行 → watcher 读到: p50 0.25ms。
- 节点写行 → 应用(`node_line_lag_seconds{stage="apply"}`, 累计滞后): p50 3.6–4.4ms, p90 8–9ms。
- 但解析加应用实测只有约 1.5ms/块(真实行解析 0.4–0.9µs, 应用 0.5–0.9µs)。
- perf 20s 采样: 约 30% 样本在 `apply_event_batch` 自身(内联代码), 约合一个核的 25%。
  热点是遍历价位 B 树的循环, 对应 `cleanup_stale_pending` 里每 1000 批(约 66ms 一次)调用的 `compact_all()`。
- `compact_all()` 遍历全部币种的全部价位, 只为检查是否需要整理。
  43.8 万单的本地测试盘口上一次耗时 1.4–8ms, 而线上现在约 53 万单、1217 个币。
  它在监听锁内运行, 期间事件全部排队。

**改动**:
- `PriceLevel::remove` 删单后调用 `LinkedList::compact()`:
  - 阈值检查是 O(1);
  - 超阈值才重建, O(该价位), 保持队列顺序。
  - 只有删单会让价位变"稀疏", 所以在这里整理就够了。
- 周期任务只保留清理空盘口: `MultiBook::evict_empty_books()`, O(币种数)。
- 删去 `compact_all`、`OrderBook::compact`、`PriceLevel::compact` 及 slab 统计; 统计函数只留测试用。
- "Compacted N price-level slabs" 日志随之消失。

**预期**: 监听器不再每 66ms 卡几毫秒, 应用滞后与 l2 发布滞后应明显下降(用 `node_line_lag_seconds` 验收)。

## 2026-10-12 新增 `userFills { user }` 订阅(私有成交两路竞争的 bm 一路)

**用途**: hyperliquid-trade 的行情网关(私有流代理)把它和官方 `userFills` 按成交身份 `(coin, time, tid)` 竞争,
先到先转(设计见主仓库 `.review/000330`, 网关实现 000332)。
10-12 实测: 节点 `node_fills_streaming` 里我们账户当天 612 笔成交与官方 `userFillsByTime` 的 tid 集合相同、字段逐字相同。

**改动**:
- 订阅 `{"type":"userFills","user":"0x…"}`, 地址校验与 `orderUpdates` 共用 `is_user_address`。
- 推送照官方增量帧: `{"channel":"userFills","data":{"user":<订阅原样>,"fills":[…]}}`, 从不发 `isSnapshot`。
  每个成交批(流式模式下一行一笔)里该用户的成交按节点顺序合成一帧; 没有就不发。
- `Fill` 补 `builderFee`; `builderFee` 与 `liquidation` 缺省或 null 时不输出。转发是按已建模字段的 typed 语义,
  不是逐字节原样: 缺省的 `cloid`/`twapId` 输出 null, 未建模的新字段丢弃(消费方按解析后语义比较, 缺省与 null 同为无)。
- 监听器: 有 `userFills` 订阅(新广播族 `UserFills`)时, `InternalMessage::Fills` 带上该批原始成交
  (流式一批一笔, 复制一笔); 无订阅时不复制。trades 配对照旧只在有 trades 订阅时做。
- 成交是原始事件流: 盘口不可信(已知丢数据停供)期间照推, 与 trades/orderUpdates 一致。
  ob 刚启动、首次装好快照前整个服务拒连, 订阅方按自己的退避重连。
- 节点重启重放的旧成交照原样推出; 去重和新鲜度由订阅方负责(网关: 成交身份 `(coin, time, tid)` 先到先转、
  会话边界 + 10s 新鲜度门)。
- 内部广播跟不上(Lagged)时, 持有 `userFills` 的连接直接关闭(被覆盖的成交补不回, 让订阅方看到缺口按断线处理);
  只订盘口的连接照旧等下一张完整快照(审查 000333 P2)。

## 2026-10-19 账户视图: 按币持仓与锚点清算价(`--accounts`, 设计 000419)

**用途**: 不经官方接口、不经节点 3001, 由本进程自己给出全市场每个币的持仓和预计清算价(主仓库 `.review/000419`
设计 r1–r4, 规则核实见主仓库 `analyze/research/hl_account_state_verify_20261019/README.md`)。默认关闭。

**改动**(`server/src/accounts/`, 三批: 000421 / 000422 / 第 3 批):
- **锚点**: 自己流式解析节点 checkpoint(`periodic_abci_states/<day>/<height>.rmp`, MessagePack), 只读需要的字段;
  每份算出全部仓位的清算价(普通账户按 dex 分池、统一账户按抵押币分池、逐仓独立; 档位按清算价处名义取;
  仓位缺 `M` = 单档 20 倍; mark 取 `pxs2[1]`; 账户类型取 `ust.a`)。只解析最新一份, 约 5–6 s、峰值 RSS 约 750 MB。
- **持仓投影**(`AccountBook`): 锚点 + 之后的成交(`startPosition ± sz`, 块内按 `(tid, side)` 去重, 旧块跳过,
  归零保留版本); universe 外的成交(现货、HIP-4 结果币、锚点之后才上的币、首个锚点之前的一切)暂存到下个锚点重放。
- **变化标记**: 按账户记锚点之后成交 / 账本 / 资金费各自的**第一个块**(从那起锚点清算价可能过期)。同一来源已有标记后,
  更晚的事件不再仅因标记触发推送; 更早的块晚到仍会把标记往前调并推送; 本币仓位变化照常推送。每块都在成交的做市账户
  因此不会把每个块都推一遍(10-19 实测 PUMP 推送 96% 来自"最新成交块"的变化, 改后约降到 1/35)。锚点之后的标记事件留日志, 装下个锚点时据此重建"它之后的第一块"; 日志有上限,
  溢出丢最旧一条并按其块号记该流丢失。
- **可信**: 丢失分已知块号(锚点高度 ≥ 它才覆盖)与未知块号(发现时间, 锚点块时间 ≥ 发现时间 + 1s 才覆盖;
  包括启动前的数据和读取线程定位到 EOF 时跳过的内容); checkpoint 解析失败、任一线程退出也失信; 失信时 HTTP 503。
- **读取层**: 原 `spawn_file_watcher` 抽成通用 `spawn_line_watcher`(单行上限按源, misc 主站资金费一行 42 MB 所以用
  256 MiB; 通道关闭即退出; 可选上报定位跳过)。订单簿的读取行为不变。换文件时旧文件剩余部分每读一块(8 MiB)就交给
  消费者, 送完再读下一块, 不再攒成一个整体(000429 r1 P2; 订单簿的行和顺序不变); 目录布局两种: `hourly/<日>/<时>`
  与 `replica_cmds` 的 `<会话>/<日>/<起始块高>`(会话、日按名字取最新, 文件按块高数值取最大)。
- **接口**: `GET /extCoinPositions?coin=`、订阅 `extCoinPositions`(snapshot / changes / withdrawn, 按币 version;
  只给有订阅的币生成帧, 走独立广播, 不占盘口通道; 积压或断号即撤订阅)。见 README "Account view"。
- **验收工具**: `accounts_checkpoint_probe`(单份 checkpoint 全部仓位)、`accounts_follow_probe`(独立跑视图并定时落盘,
  供主仓库 `follow_vs_node.py` 对照节点)。
- **罕见事件留证**(000425): 非 market 清算方式、没见过的成交方向 / 账本类型、未知 misc 事件各打一行
  `ACCOUNTS_EVIDENCE <json>`(WARN, 带原始行), 锚点与成交推出的数量不一致记 `silentPositionChange`(每次装锚点最多
  100 条); 计数 `orderbook_accounts_evidence_total{kind}`。`GET /extCoinPositions` 按 `Accept-Encoding` 返回 gzip。
- **组合保证金账户**(设计 000427 r2): 解析多读 `blp`(储备的指数、每账户借 / 存上限、LTV、oracle 价 `o`)和全部代币精度,
  组合保证金账户保留储备币的现货余额与借贷份额(现货余额只留持仓账户, 解析峰值内存反而降到约 440 MB)。`portfolio.rs`
  是抵押值、他币债务、可借余量(不足 1 USDC 记 0)和账户比率的唯一实现; 全仓池现金 = 现货 USDC + 存款 − 借款 + 各 USDC
  dex 全仓现金 + 可借余量, 沿用原有分档求解器, 得到节点 `liquidationPx` 同义的价(可用额归零, 比率约 1, 不是官方 0.95
  门槛); 每行带锚点时刻的账户 `portfolioMargin` 比率。非 USDC 结算 dex 上有状态、抵押超每账户存款上限、缺所需借款上限
  → 整个账户的全仓与比率 unsupported, 逐仓照常。
- **维持保证金按节点的整数规则**(10-19 研究"维持保证金的整数规则"):
  - 仓位在 mark 处的维持保证金 = floor(名义 ÷ (2 × 最大杠杆)) − 扣减额, 单位微 USDC; 名义 = 原始仓位 × 存储 mark, 两个整数相乘, 精确。
  - 求某仓位清算价时, 池里其他仓位用这个整数值, 该仓位自己连续随价格变; 组合保证金比率的维持保证金合计用同一个函数(`DexMeta::maintenance_micro`, 唯一实现)。
  - 依据: 节点 `crossMaintenanceMarginUsed` 在 271/271 个单档池子逐位相等, 浮点只有 40 个; 统一账户 `0xe4d448e3`(196 个尘埃仓位)的 80 行由此全部对上。
  - 档位表里有 6 个档的扣减额是连续性隐含值(1/3 的分数)取整后存的, 这里照文档用存储值; 与逐档精确求和最多差 1 微 USDC, 尚无跨档样本能区分节点用哪种。
  - 同一份 checkpoint(1190060000)上, 与 Python 参考实现逐行一致: 非组合保证金 297,638 行计算值 ≤ 1e-9、101,307 行都无正解; 组合保证金比率 7,874/7,874, 清算价差仍是原有的求根方法差(≤ 2.35e-8)。与改前相比 101,531 行变化, 393 行超过 1e-6, 最大 3.7e-4。
- **区块交易标记**(设计 000429 r2): 第三个流读 `replica_cmds`(每块一行, 一行最大实测 6.4 MB, 上限 64 MiB), 回执先按
  bundle hash 找、再按 bundle 内下标配对(找不到、重复、条数不等即契约违规); 只取成功的 `updateIsolatedMargin` /
  `updateLeverage`(→ `margin`)、`userSetAbstraction` / `agentSetAbstraction`(→ `accountMode`)、`withdraw3`
  (→ `withdrawal`), 通常取回执 `user`(带 `vaultAddress` 时即子账户 / 金库)。`userSetAbstraction` 则取动作内的
  `user`: H1198440715 的成功回执是主账户, 动作目标是子账户。漏掉此区别会让子账户在统一保证金关闭后仍合并现货现金;
  H1198440000→H1198450000 的 LIT 清算价差来自残留现货 0.006715 USDC。修复只将模式切换标记和
  `accountModeSwitch` 停止回放作用到正确目标, 不声称支持完整模式切换现金回放。每块都有一行, 块高跳号按丢到最后一个
  缺失块记丢失; 与成交 / 杂项流同一套丢失界与覆盖规则, 丢失未被锚点覆盖时视图 503。标记日志溢出时按被挤掉那条的
  来源记丢失。没做设计里的按字节预筛: 本地 400 个真实块(132 MB)全量解析 2.3 GB/s, 只读块高 2.4 GB/s, 预筛省不出
  东西。
  - bm 验收(10-19, `accounts_follow_probe` 跟 3 个锚点): 一整份 1 万块文件(3.54 GB)解析 1.53 s, 输出与 Python 逐条相同
    (5,145 条); 98 次落盘、157 万行的 margin / accountMode / withdrawal 标记(5,981 / 244 / 8 个)与 Python 按"锚点之后
    第一块"算的结果 0 处不同; 相邻 checkpoint 1189920000 → 1189930000(同一小时)之间, 持仓且该币无成交而保证金设置变了
    的 135 个全有 margin 操作, 持仓账户模式变了的 14 个全有 accountMode 操作; 现金无事件变化的持仓账户都是统一 / 组合
    保证金账户的池内划转(统一账户池子总额不变, 组合保证金扣掉利息后残差 ≤ 0.045 USDC), 另 13 个无持仓账户(援助基金等)
    不影响任何行。读取线程约 0.65% 个核; 查询时最新块距其区块时间中位 205 ms(p99 385 ms, 含节点写出延迟)。
- **按块入账**(设计 000433 r3 第一批, 两次锚点之间账户状态的前置): 三路输入先按块缓存(`accounts/blocks.rs`), 块完成后
  整块按高度依次进 `AccountBook::apply_block`, 每块只入账一次。块 H 完成 = 读到 H 和 H+1 的 `replica_cmds` 行, 且成交、
  杂项两个读取器各有一个"已读穿"证明的时刻晚于读到 H+1 那行的时刻。"已读穿"由读取器发(`FileReader::read_through`):
  先取时刻, 再确认没有更新的文件、读到 0 字节、没有半行、没有读错, 与数据同一通道送出; 只有账户视图的两路开, 盘口
  监听不变。依赖 bm 实测的写出顺序(块 H 的最后一条成交行、杂项行先于块 H+1 的 `replica_cmds` 行到达), 是运行前提
  不是协议保证: 已入账块又来行(节点重启重放或顺序被打破)按丢到该块处理, 节点重启可能多一段等锚点的不可用期。未
  完成块内按身份去重(成交 = 账户、币、tid、方向; 杂项行 = 从 `"events":` 起的文本哈希, 不含 `local_time`, 没有这段
  文本的行按解析错误); 待完成块数、输入数有上限, 溢出按丢失处理。"已读穿"只在确认读到末尾时成立: 取文件大小、seek、
  读任一失败都不算(审查 000434 r1 P2), 失败的句柄丢掉、下次重开再读。换文件时旧文件要确认读到末尾才算无损交接; 读
  失败或末尾留着半行就切换, 按丢失置 desync(审查 000434 r2 P2, 盘口监听同样受益)。新指标
  `orderbook_accounts_applied_height`(只随完整入账的块前进)、`_pending_blocks`、`_duplicate_inputs_total`。
- **锚点之间的现金回放**(设计 000433 第二批, 状态与自检, 还不对外输出): 锚点时持仓账户的保证金状态(`accounts/replay.rs`:
  普通 / disabled 账户每个 USDC dex 的全仓现金, 统一账户的 USDC 池 = 现货 + 各 USDC dex 全仓现金, 每个逐仓的现金, 以及
  每个币的逐仓设置)随按块入账的成交、资金费、账本事件和区块动作逐块变动, 金额是 10^-8 USDC 整数(`accounts/usdc.rs`)。
  规则全部来自研究 `hl_account_state_verify_20261019` 的实测; 没有实测规则的事件(逐仓成交、后备清算、账户类型切换、统一
  账户的 `borrowLend`、未读的账本类型、非 USDC 抵押、现货非 USDC 计价, 以及普通账户涉及现货又带 USDC 手续费的转账
  等)让该账户停止回放, 到下一个锚点恢复。原来只记"谁在哪块被标记"的日志改为完整的账户事件日志, 标记重建和现金回
  放共用。每个锚点装入时先拿上一锚点的状态 + 区间内事件
  逐户对比新锚点(逐位), 再从新锚点状态重放其后的事件; 已有可信回放时, 先到的锚点在 owner 里等到其块都已入账再装入
  (审查 000435 r1 P2)。同一块里切换过全仓 / 逐仓设置的币, 该块成交按不能确定为全仓处理, 停止回放(块内先动作后成交)。解析随之扩展: 账本事件按类型读出 USDC 金额、资金费带币和金额、
  区块动作带 `asset` / `ntli` / `isCross` / `amount`(只在保留的类型上读, 别的动作同名字段类型不同)、checkpoint 读币的
  `marginMode`、没有持仓的逐仓设置和现货交易对。新指标 `orderbook_accounts_replay_checked_total`、`_replay_stopped_total`、
  `_replay_checks_skipped_total`。
- **回放清算价**(设计 000437 r3, 第三批): 逐仓仓位和池里只有它一个的全仓仓位(普通账户同一 USDC dex, 统一账户所有 USDC dex)
  在账户回放中、且已入账高度追上锚点时, 用回放的现金 + 当前数量 + 这笔仓位自己的档位表按同一个 `solve` 算清算价, 行字段
  `liquidationPxSource: "replay"`, `liquidationPxHeight` = 已入账高度, `changedSinceLiquidationPx` 为空; 其他行仍是锚点行;
  停止回放的账户带 `replayStoppedReason` / `replayStoppedHeight`。档位表跟着当前这笔仓位: 锚点的 `M`, 锚点之后从 0 开的取
  币当前 `marginTableId`, 翻转保留(研究 4/4); 晚到锚点只覆盖它包含的那笔(`opened_after`)。按需在视图投影里算, 不另存
  回放行; 账户持仓索引判断是否单仓位池; 回放中账户的每个事件都触动其行(推送量随之上升, 实测见审查)。每个锚点装入时对
  它的仓位逐行自检(`orderbook_accounts_replay_rows_checked_total{result}`, `replayRowMismatch` 证据)。
- **多仓位池回放行与距离**(设计 000442 r2): 全仓池里有其它仓位时, 其它成员按同一高度的节点 mark(`NodeMarks::at`, 高度
  必须等于已入账高度)估值, 维持保证金按整数规则, 与锚点共用 `solve_held`(锚点用 checkpoint 存的 mark)。任一成员没有
  mark(mark 不可用、主站该币未复现、锚点后新上的币)或档位表未知, 就退回锚点行。行新增 `markPx` 与
  `liquidationDistanceBps`(同一高度的 mark、清算价与数量; 锚点行在构造时用锚点的数量和 mark 算好)。`poolPositions` 改为
  实际成员数。推送: mark 的普通变动不推; mark 由可用变不可用或恢复时, 对已订阅的币推一次 snapshot(账户视图本身仍可用)。
  每个锚点的逐行自检扩展到多仓位池(锚点的数量、记录的表、checkpoint 的 mark); 判等容差改为按清算价和 mark 两者较大者的
  1e-12(远离 mark 的极小清算价有浮点相消)。
- **清算候选推送 `extLiquidationCandidates`**(设计 000443 r3, 实现 000446 r2): 回放行中离清算价 1000bp 以内的仓位,
  带预计市价清算单(10 万美元以内整仓、以上五分之一, 部分清算后 30 秒内整仓; 冷却按完成块归并)和静态盘口扫单。
  同币同方向按 mark 先到的顺序共用深度(`together`, 前序量 `aheadSize`), 另给单独扫单(`alone`); 两者都带均价、最深价及其
  离盘口中间价(`midPx`)的 bp。扫单在独立任务里, 监听器锁内只复制档位(≤2,000 档); 结果按盘口可信代次、请求版本和数量校验
  后才发布。订阅按币和阈值投影, 首次快照, 之后进入 / 更新 / 移出。
- **自扩展接口统一 `ext` 前缀**(10-21): 订阅类型与推送 channel `extCoinPositions`、`extLiquidationCandidates`, HTTP
  `GET /extCoinPositions`; 名字集中在 `types/subscription.rs` 的常量。旧名直接拒绝(此前没有下游消费)。

**实测**(10-19, bm): 对齐的单仓位池清算价与节点 0.1bp 以内(另有 3 个节点只给 4–5 位有效数字的显示精度差);
锚点之间约 40 万个仓位按成交推出的数量与下一份 checkpoint 逐个相同。只核对了 USDC 抵押池。

**已知边界**: 停止回放的账户和组合保证金账户的清算价在两次锚点之间不随 mark 更新; 组合保证金只覆盖 USDC 结算 dex, 不给 0.95
情景价(借 USDH 的账户比率为 1 却 86 分钟未被清算, 真实触发条件待样本); 后备清算 / ADL 是否产生成交未核实(进入交易
判断前的阶段门); 区块交易标记只说明发生了可能使锚点失效的成功操作, 不在锚点之间重算现金、模式与清算价; 空变化
标记只说明账户自己没有这些事件, 不能证明清算价没变(同池其他仓位的 mark 在动, 组合保证金账户的借贷利息在累积)。

## 2026-10-20 被迫成交阶梯第 1 批: 条件单表(设计 000451 r3)

**用途**: 被迫成交阶梯(止损 + 清算, 第 2 批 `extForcedFlow`)要在同一进程里直接读监听器的未触发条件单, 不另建一份。
本批把这张表收成一个 owner(`listeners/order_book/triggers.rs` 的 `TriggerTable`), 并给出按触发价区间的读取。

- **存储**: 每币一份按 (触发价, oid) 排序的订单, 另按 oid 记触发价以便删除; 订单 `Arc` 共享, 锁内读取只加引用计数。
  撤单 / 触发等非 `open` 状态的删除路径不分配内存。触发价解析不了的单不进表(按缺口处理)。
- **完整性**: 快照转换随结果交出条件单段的状态 `TriggerSection`: 每个市场都带段且全部转换成功为 `Complete`(空段也算);
  有转换失败或有市场缺段为 `Partial`; 一个市场都没带为 `Absent`。表的可用性 `Complete` / `Gap(原因)`, 原因分
  `TrackingDisabled`(`--bbo-only`, 或现货过滤下的现货币)、`SnapshotWithoutTriggers`、`SnapshotParseFailed`、
  `LiveParseFailed`; 第一个缺口一直保留, 只有下一次 `Complete` 的安装能恢复。只有 `Absent` 时沿用旧表的单(原有行为);
  `Partial` 即使转换结果为空也整表替换。不跟踪的市场(现货过滤下的现货币)按币查询一律不完整(审查 000453 r1)。
  10-20 bm 实际快照: 1666 个市场全部带段, 132,597 张条件单。
- **版本**: 每次安装代次 +1; 代次内每币每次增删改版本 +1, 币的单删空后版本也不归零。
- **区间读取**(`OrderBookListener::untriggered_in_band`, 第 2 批使用): 一个币触发价在 `[lo, hi]` 内的单, 按触发价再按 oid,
  最多 `max` 张(带索引用的触发价), 超出标 `truncated`; 同时给可用性、代次、版本, 以及同一把锁下盘口的进度(time / height)、是否可信和
  `withheld_periods`(状态流丢数据可能丢了条件单的状态, 消费方要同时看表完整和盘口可信)。
- **接口**: `GET /untriggeredOrders` 多一个 `complete` 字段, 只在表 `Complete` 时为 true; 其余行为不变。

## 2026-10-22 被迫成交: 帧带出条件单表自己的高度

- **现象**(验收 run2 79 次、run6 3 次): 帧的 `listenerHeight` 是订单状态流和盘口差分流里较远的那个(`OrderBookState::latest`), 条件单表只随订单
  状态流前进; 差分领先时帧标着块 H, 表才到 H−1 或 H−2, 那两块里下的条件单看着像"表该有却没列"。单子不丢, 晚一两块出现; 消费方若拿
  `listenerHeight` 判断条件单部分的新鲜度会以为更新。
- **改法**: 区间读取(`untriggered_in_band`)在同一把锁下带出 `status_height`(订单状态流已应用的块), 帧 `heights` 新增 `triggerStatusHeight`;
  `listenerHeight` 含义不变(盘口部分仍用它)。一行一条状态, 所以该块本身可能只应用了一部分, 更早的块完整。

## 2026-10-21 账户重放: 逐仓成交不再停重放

- **现象**(被迫成交验收 run4, bm 10-21 14:35–15:07): 966 次市价清算里 106 次触发前的最后一帧没列出, 其中 65 次是逐仓仓位在检查点之后
  有过成交(含之前的分批清算)。重放遇到逐仓成交就停到下一个检查点, 只有重放出来的行才是清算候选, 这期间整个账户都列不出。
- **实测规则**(研究 `hl_account_state_verify_20261019` "逐仓成交的划转", checkpoint 1192420000 → 1192440000 两段, 区间里只有一笔成交的
  全部逐仓仓位, 微 USDC 逐位): 手续费先从逐仓现金扣; 部分减仓再把剩余现金按减仓比例划回全仓, 向零截断(488/488); 全平全部回全仓
  (768/768); 开仓、加仓从全仓划入 floor(数量 × 成交价 / 杠杆)(522/822); 反手 = 旧仓全平 + 余量按成交价开新仓, 手续费留在旧仓(30/58)。
  开仓/加仓/反手没对上的那部分, 节点划进逐仓的都比规则多, 所以这里的逐仓清算价比节点的更靠近 mark(p50 0.4 bp、p90 5.2 bp、最大 62 bp),
  方向是提前列出; 下一个检查点纠正。默认杠杆: 只能逐仓的币没有设置时用该币档位表第一档的最大杠杆(56 个这样开的仓位里 51 个, 另 5 个
  有 `updateLeverage`)。
- **改法**: checkpoint 读出逐仓杠杆 `l.I.l`(含只有设置、没有仓位的记录), `updateLeverage` 读出 `leverage`; 重放按 `startPosition` 判断
  开/加/减/平/反手并按上面的规则在全仓与逐仓之间划转; 同一块里该币改了逐仓设置或杠杆时成交仍停(块内先后没测过)。
- **估算要标出来**(审查 000466 r1): 逐仓少划的钱按守恒留在同一个全仓池, 所以同账户的全仓清算价反而可能比节点的远、晚列。走过开仓/加仓/
  反手的账户, 它的每一行(持仓接口、清算候选、被迫成交的清算条目)都带 `cashEstimated: true`, 到下一个锚点为止。锚点核对先比每个池
  "全仓 + 逐仓"的总额: 总额不同仍是 `mismatch`(漏记入金之类不会被掩盖); 总额相同、只是分配不同才计 `estimated`, 留至多 50 条
  `replayEstimated` 样本, 供以后核对残差方向和量级。
- **行价格核对同样区分**(10-22 部署后第一个锚点: 533 个估算账户, 1,522 行价格与锚点不同; 那时行核对不分估算与否, 这些行各属哪类要看本改动的 `differ` / `estimated`): 估算账户的行与锚点价格不同时计
  `estimated`、留 `replayRowEstimated` 样本, 不计 `differ`; 其余账户照旧。资格只看本次现金核对的判定(`MarginStates::split_estimated`, 与
  现金核对共用): 池总额相同、分配不同的账户才算; 用过模型但现金逐位吻合、或池总额不对的账户, 行价格不同仍计 `differ`(审查 000468 r1)。

## 2026-10-21 条件单表: 同块成交的母单, 子单出现即生效

- **现象**(验收 run4, bm 10-21 14:35–15:07, OB `6c7dd97`): 节点触发的 5,959 张条件单里 112 张在触发前的最后一帧里没有;
  全是 OB 启动后新下的单(快照载入的 2,338 张一张没漏, 启动后新下的漏 3.2%, OB 跑得越久漏得越多)。
- **节点行为**: 下单附带止盈止损、母单在本块就成交(市价单 47、IOC 28、穿价成交的 GTC 9), 同一块里的顺序是母单 `open`(子单挂在
  母单 oid 下)→ 母单 `filled` → 子单各自的 `open`。表在母单 `filled` 时放行, 子单那时还没出现; 子单 `open` 再按字段配对认到这张
  已成交的母单, 进了等待, 永远不放行。另 28 张是这样卡住的子单后来被改单, 新单继承了等待。
- **改法**(`listeners/order_book/triggers.rs`): 记下本块里 `filled` / `marginCanceled` 时还有子单没 `open` 的母单; 这些子单 `open` 时
  不再挂到母单下等待, 直接进触发价索引, 也不进"改单继任"的暂缓判断(同账户同币另有等待子单时, 下一行那张子单的取消会被当成改单,
  审查 000465 r1)。换块时清掉。回归测试用节点原始行(块 1192030080 市价单、1192032675 IOC 单带止盈止损、1192032705 对这两张子单的
  改单), 另测同账户同币有等待子单时 `filled` / `marginCanceled` 两种放行。

## 2026-10-21 条件单表: 改单后的子单继续等待母单

- **现象**(修复后的验收, bm 10-21 11:42 起): 两个程序账户在 PONS 上用 `batchModify` 每隔几分钟整组改一张限价母单和它的止盈止损;
  HL 改单给每张单新 oid, 新母单的 `open` 不列子单, 新子单被当成独立条件单进了被迫成交(一小时 36 条, 越过触发价 150–230 bp 不触发)。
- **改法**(`listeners/order_book/triggers.rs`): 改单在状态流里是 "新 oid `open`(带动作 hash)" 紧跟 "旧 oid `canceled`(无 hash)",
  同账户同币同块(11 时 129,342 对里 129,306 对是按 oid 或 cloid 的改单, 条件单 111/111)。旧 oid 是等待中的子单时, 新 oid 继承
  它的母单关系继续等待; 放行仍靠继任母单 `filled` 行列出的子单 oid。账户在该币持有等待子单时, 它新开的条件单先暂缓一行
  (`/untriggeredOrders` 照常列出, 区间读不读, 该币区间期间标 `successorPending` 不完整, 下一行后恢复; 已有的持久缺口不受影响),
  由下一行状态决定, 免得两行分在两批时被区间读插进来(审查 000464 r1)。

## 2026-10-21 条件单表按方向建索引: 已过触发价的单也进被迫成交

- **现象**(被迫成交验收): 节点触发、触发前表里却从没列过的单 36 张(20 分钟), 都是下单时 mark 已过触发价(例: BTC 止盈
  "Price above 82762" 下单时 mark 约 83,550, 18 个块后的价格动作即触发)。被迫成交收录距离 ≤ 档位的条目(负距离算在内), 但区间读
  只读 mark 两侧 ±档位的触发价, 过线较远的读不到。
- **改法**(`listeners/order_book/triggers.rs`): 每币的索引分 下跌触及(`Price below`、已激活的追踪卖单) / 上涨触及(`Price above`、
  已激活的追踪买单) / 无处放(未激活的追踪止损、未知条件写法)。区间读给出下跌触及中 ≥ 下界的全部、上涨触及中 ≤ 上界的全部, 以及
  全部无处放的单(`placed_nowhere`, 原 `unpriced`)。`Direction`/`TriggerRule`/`stop_direction` 从 `accounts/forced_flow.rs` 移到
  条件单 owner, 被迫成交从那里引用。

## 2026-10-21 条件单表: 母单还活着的 TP/SL 子单不进被迫成交

- **现象**(被迫成交验收, bm 10-21 09:30 起): 帧里节点 mark 已过触发价、节点却一直不触发的条件单 114 张, 逐账户查 3001:
  51 张是还没成交的限价母单下挂的止盈/止损。启动快照的 138,751 张条件单里 8,326 张是这类子单(BTC 1,918)。
- **节点行为**: 母单在盘口时子单不触发(8-9 时 49 张被触发的子单, 48 张在母单 `filled` 之后, 1 张在母单 `marginCanceled` 之后);
  快照里活着的订单的 `children` 带子单自己的 oid; 实时母单 `open` 行里子单带母单的 oid, 子单自己的 `open` 行在同一块随后到
  (9 时 1,745/1,745); 母单 `canceled` 后子单要么同块各自 `canceled`(226), 要么是改单, 转到新母单下(78; 新母单 `open` 不列子单,
  之后的状态行带子单自己的 oid), 没有一张之后被触发。
- **改法**(`listeners/order_book/triggers.rs`): 等母单的子单单独存放, 不进触发价索引(区间读、被迫成交都读不到), `/untriggeredOrders`
  照常列出; 按官方 TP/SL 文档(母单完全成交启用、部分成交仍等待、撤母单撤子单、部分成交后保证金不足撤单启用), 只有母单 `filled` 和
  `marginCanceled` 放行它的子单和该行 `children` 里列出的子单, 其他状态都让子单继续等待(审查 000463 r1)。快照关系由 `convert_cli_snapshot` 从活着的订单收集
  (`ParentLinks`); 实时按 用户/币/时间戳/方向/触发价与条件/单型/数量/只减仓 配对, 同块没等到的子单计数并记缺口 `childUnmatched`
  (被迫成交的条件单部分随之 incomplete); 缺段快照接替时连同等待中的子单一起沿用。

## 2026-10-21 本地 mark 第 1 批: 外所行情接入(`--local-marks`, 设计 000454 r2)

**用途**: 用外所现货(oracle)和永续(外部价)报价, 加上节点重放的精确状态, 在本进程算 HL 主站的本地 mark(第 2 批);
被迫成交阶梯的距离要节点和本地两个口径。本批只接行情、提供核对接口。

- **数据源**: `server/data/mark_sources.json`(`scripts/discover_mark_sources.py` 由各所公开交易对列表生成, 逐条与 HL 的
  oracle / mark 核对量级, 剔除同名不同资产和错误倍数), 编译时嵌入。10-21: 176 个币(HYPE、PURR 是 USDC 计价永续, 本批不做;
  HIP-3 不做), 每币至少一家现货和一家永续; 启动时校验所名、倍数、交易对唯一。
- **12 路行情**(`local_marks/`): 现货 币安/OKX/Bybit/Kraken/Kucoin(先取令牌)/Gate 走 WS, MEXC 现货 REST 每 250ms 轮询
  (其 WS 是 protobuf); 永续 币安/OKX/Bybit/Gate/MEXC 走 WS。WS 用 `yawc`, REST 用依赖树里已有的 hyper + tokio-rustls。
  订阅按实测上限分批(Bybit 每次 10 个, Kucoin 每次 100 个——一次 173 个回 509)。解析移植自 `hl_mark_prices` 服务, 用 bm 10-21
  的实帧做测试样本(`wire_fixtures.json`); 价格按原文精确解析(字符串、JSON 数字、指数写法), 倍数精确相乘。
- **最新值槽**(`slot.rs`): 行情任务覆盖写入、永不等待读方; 每笔报价带连接代次, 只在当前且已连接的连接上有效——断开重连发生在
  两次读之间, 旧报价也保持无效, 直到该交易对收到新报价; 新连接、订阅确认、pong 都不恢复旧报价。MEXC 现货报价 5 秒过期。
- **健康与恢复**: 各家订阅确认 10 秒内到齐、心跳 10 秒内回(Gate 现货 `spot.ping`、永续 `futures.ping`)、30 秒内总有帧; 断线
  撤回该路报价, 退避重连(0.5s 倍增到 10s, 连上 30 秒重置)。拒订、已知帧解析失败、交叉盘口 → 只停这一路(带原因), 不影响盘口和
  账户; 未知类型的帧原样 WARN、不采用(先按类型分流、再严格解析载荷: 已知行情缺载荷是契约错误, 审查 000455 r1)。任务由监督者持有:
  某一路 panic 或被取消(内部错误)→ 停掉全部行情、撤回全部报价直到重启(同账户 owner 退出的约定), 盘口照常。
- **接口**: `GET /extMarkInputs?coin=`: 各路连接状态, 逐币逐所的报价(原价 + 倍数 + 状态)和覆盖(用到 / 缺哪几家、权重)。
  `local_marks_probe` 可在不重启 OB 的情况下单独跑行情。指标 `orderbook_local_marks_feed_{frames,disconnects,unknown_frames}_total`、
  `orderbook_local_marks_feed_stopped`。

## 2026-10-21 被迫成交阶梯第 3 批(部分): 开销指标

- 读取任务在监听器锁下的等待和持有时间按读取种类(`band` 区间条件单、`walk` 一侧盘口)计时, 扫盘条数计数; 账户 owner 上每一步
  (`query`/`read`/`tick`/`local`)的耗时。锁计时抽成 `walk.rs` 的 `under_lock` + `LockTimers`, 清算候选扫盘沿用原指标名。
- 触发核对、数量核对改为 bm 上的离线验收(主仓 `analyze/research/`), 不在 OB 里常驻。

## 2026-10-21 被迫成交阶梯第 2 批: `extForcedFlow`(设计 000451 r1–r5)

- **只报事实**: 条件单(监听器条件单表的区间读, 第 1 批 `b4faa4b`)和清算(清算候选的底层清算价索引, 按双口径范围取, 同一个
  `ExpectedOrder`)逐条给出: 按节点规则对当前仓位的单独数量(整仓 TP/SL 取整仓、只减仓取 min(数量, 可减仓位)、非只减仓取订单数量、
  清算取下一张市价清算单), 以及按自身限价的单独扫盘(`walk.rs` 的 `alone_walks`, 结束方式多一种 `limitReached`)。不算 together、
  不做连锁情景。
- **两个口径的距离**: 节点 mark(账户 owner 的重放)和本地预测 mark(本地 mark 快照, 经查询通道接给账户 owner); 本地可为负。
  任一口径在订阅距离内即收录。
- **分档整帧**(`forced_flow_owner.rs`): 订阅按距离归入 10/100/1000 bp 三档, 有需求的(币, 档)分别每 100ms / 1s / 5s 跑一次两段流水线
  (读取任务锁内复制区间条件单 → owner 按仓位算数量 → 读取任务锁内复制对手盘、锁外逐条扫), 内容变了才发新 `seq` 的整帧(扫盘盘口的
  高度时间不算内容)。本地腿失效(含通道已关、owner 还没收到通知)时先立即发撤回帧: 去掉本地口径, 条目的本地距离置空, 只靠本地口径
  收录的条目退出; 有效性一变, 无论流水线在哪一步都立即重算, 旧读取作废; 扫盘期间盘口失信 → 扫盘作废、条件单部分 incomplete
  (审查 000460 r1/r2)。HTTP 和新订阅都要启动时间不早于请求的计算结果: 进行中的计算不作废, 先服务它启动前到的请求, 再接一轮服务后到的
  (审查 000460 r2); owner 不等待自己。条件单表的区间读顺带给出未激活(`triggerPx` 0)的追踪止损, 计入 `unplaced`; WS 订阅的未知字段
  拒订。
- **过滤与汇总**(`forced_flow_view.rs`): `sources`、`excludeCoins`、`minNotionalUsd`(默认 1 万 USD, 只过滤条目); 汇总按 (方向, 买卖)
  分 1–1000 bp 九档, 两个口径各一套。条件单表不完整(缺口、截断、盘口失信)时条件单部分标 `incomplete`, 清算部分照常。
- **接口**: WS `extForcedFlow`, `GET /extForcedFlow`; `extLiquidationCandidates` 不变。

## 2026-10-21 本地 mark 第 2 批: 节点状态导出 + 计算 + `extMarkPrices`(设计 000454 r2)

- **节点状态**: 账户 owner 在每个已应用块之后(以及种子重建状态时)经 `watch` 发布主站节点价格快照 `NativePriceSnapshot`
  (逐币 szDecimals、存储 oracle、存储 mark、msoe/lmpe、Mark 参考缓存中位数、最新成交, 以及种子高度、块高、块时间), 只含重放认领的币;
  账户视图或节点 mark 一不可用就立即发布原因。发布点是已有的 `track_marks`(输入、锚点、监督三条分支都调用), 与订阅者无关;
  状态(种子高度, 块高)和原因都没变时不重发。
- **盘口**: `OrderBookListener::best_prices`: 每拍一次监听器锁, 按币顺序只复制最优买卖价, 带盘口进度(两条盘口流都已应用到的块时间)、
  高度和是否可信。
- **计算**(`local_marks/compute.rs`, 用 `accounts::marks::rules` 即重放的同一份实现): oracle = 现在有报价的现货所中间价加权中位数
  (币安 3、OKX 2、Bybit 2、其余 1, 至少一家; 同 `hl_mark_prices` 的 `weighted_median`), HALF_EVEN 换成价格单位;
  A = oracle × (1 + basis), basis 为 msoe 按现在时刻、盘口中间价、存储 oracle 预览; L = median(bid, ask, 最新成交); C = 永续加权中位数;
  参考 mark = median(A, L, C); 预测 mark = median(参考, Mark 缓存, L)（主站不走 HIP-3 的 1% 限幅，见下方修正）, 是有条件的估计, 不写回。
- **快照与撤价**(`snapshot.rs`, `owner.rs`): 每 50ms 全部币算一遍。两条进度时钟对全部币一起判: 节点块时间落后本地时钟超过 5 秒 →
  两条腿 `stale`(`nodeProgress`); 盘口超过 5 秒 → 本地腿 `stale`(`bookProgress`)(设计定 3 秒; bm 10-21 验收录制里已应用块的
  落后每分钟 P99 在 2.4–2.9 秒, 3 秒时 14 分钟闪撤 10 次、每次不到 1 秒, 改为 5 秒); 安静币不会因自己没变化被撤。节点不可用(`node`)、
  盘口未就绪或失信(`hlBook`)、行情全停或账户 owner 退出(`stopped`, 直到重启)同样撤价; 逐币原因 `nodeCoin`、`markCache`、`hlBook`、
  `oracleCoverage`、`externalCoverage`、`calculation`。owner 任务结束(内部错误)关闭快照通道: WS 订阅带原因撤回, HTTP 503, 盘口照常。
- **接口**: WS `{"type":"extMarkPrices","coin":"BTC"}` 每 50ms 一帧(schema 2, 订阅时先给最新一帧); `GET /extMarkPrices[?coin=]`。
  每家报价仍在 `GET /extMarkInputs`。交易系统网关改接 OB(去掉 BTC/ETH/SOL/ZEC 白名单和 schema 1 检查)在第 3 批。

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

## 2026-10-21 主站 mark 移除误用的 HIP-3 限幅

- 根因：先前只追到 `kind == Mark` 的 1% 限幅，漏掉其外层 `MainOrHip3` 条件。6911b792 在
  `0x2965e0a..0x2965e1b` 对 Main 直接跳到写价；`0x2967ee1` 将 +0x378 对应到 `moh`，
  `0x2c3aa1f..0x2c3aa6e` 将判别值对应到 Main/Hip3。
- 主站重建与 `predictedMarkPx` 一起移除限幅，删除无效限幅计数和日志；HIP-3 仍直接读节点最终价格流。
- 金样本 `native_mark_liquidation_goldens.json`：ALT 四个块的清算 mark 来自节点成交，输入来自归档逐动作回放。
  块 1192114308 的正确中位数 0.011112 被旧逻辑截为 0.011085；块 1192112978 的正确中位数
  0.010917 被旧逻辑截为 0.010820。后续正常更新能覆盖这类中间错误，checkpoint 终点全等不等于逐步全等。


## 2026-10-22 逐仓成交按 mark 补足初始保证金

- 修正开仓、加仓和反手的全仓→逐仓划转：取成交名义/杠杆与 mark 初始保证金加即时浮亏的较大者，最后一次整数向下取整。反手只为余量开新仓，手续费归旧仓。
- 主站按既有逐动作执行切点提供成交 mark 范围；HIP-3 在缺少动作身份时保留块前及同块所有价格。范围两端算出相同划转额才作为精确值；否则保留已有估算和 `cashEstimated`，不换用块末 mark、不停止整个服务。
- 成交价证据随账户事件保存，checkpoint 重放复用同一事实；资金费继续使用块前 oracle。未知时只影响估算标记，实际解析错误仍沿原有丢流路径撤回价格。
- 精选节点金样本位于 `server/src/accounts/replay/isolated_goldens.json`；873 例归档可用 `ISOLATED_MARGIN_REPORT=/absolute/path/isolated_mark_requirement_report.json cargo test -p server --lib full_archived_isolated_margin_corpus -- --ignored --nocapture` 复核。金样本证明给定记录 mark 时的现金规则；不能代替块内执行位置或上线 checkpoint 验收。


## 2026-10-22 服务化:启动定义移交 hyperliquid-trade

- 用户定 orderbook_server 不再是核心进程, 编译/上线/修复性重启委托代办; bm 上改为 `orderbook-server.service`。
- 唯一的 unit 与命令行在 hyperliquid-trade `operations/orderbook_server/ob_ops.py`: 编译 origin/main 到
  `~/ob_releases/<提交>/`, 上线 = 移动 `~/ob_releases/current` 后重启 unit 并做健康检查, 回滚 = 指回上一版;
  `~/ob-snapshotter` 保持官方节点程序的换名副本(节点查到第二个 hl-node 进程会 panic), 按 sha 核对。
- 删掉本仓库的 `orderbook-server.service`(早已过时的示例, 参数与线上不符)和 `start-server.sh`(tmux 手动启动
  脚本, 参数会与 unit 分叉); 日志改由 journald 保存。

## 2026-10-27 HIP-3 美股本地 mark: Alpaca SIP + 文档三项中位(`--hip3-sip-marks`, 设计 000535 r1–r2)

- **用途**: 被迫成交扩到 HIP-3(用户 10-27)。止损、强平按 mark 触发, HIP-3 原来只有节点 mark。
- **机制**(trade.xyz mark price 文档, bm 10-27 用原始 setOracle 核对):
  - mark = median((1) oracle, (2) oracle+150s EMA, (3) median(买, 卖, 最新成交))。
  - 中继在 `markPxs` 里发 (1)(2), HL 出块时算 (3) 并取中位。
  - 节点 HIP-3 流的 `mark_px_inputs` 存的是中位之后的值。
- **计算**(`local_marks/hip3/`):
  - (1) 用 SIP NBBO 中间价, 按中继发布精度舍入, 限幅到存储 oracle ±50bp;
  - (2) 是 (1) 加我们自己算的 150 秒 EMA(HL 中间价 − 存储 oracle);
  - (3) 用 HL 实时盘口;
  - 输出走独立通道 `extHip3MarkPrices` 和 `GET /extHip3MarkPrices`。`extMarkPrices`(schema 2)主站帧逐字不变, 网关不用升级。
  - `extForcedFlow` 统一从 `LocalMarksSnapshot::local_predicted` 读本地距离。
- **节点状态**:
  - `Hip3Coin` 加 `external` 和 `last_trade`: 节点流的 external 与块内最后一笔吃单成交; 种子读 checkpoint 的 `pxs2` 槽位 2 和 HIP-3 盘口的 `last_trade_px`(盘口 asset = pdi×10000 + 序号, bm 1199490000 实测)。
  - 价格种类从布尔改成 `Hip3PriceKind`, 成交估值只认 Mark。
- **数据源**:
  - Alpaca SIP 一条连接: 认证和订阅一起发, 两次确认; 我们发 WS ping 维持连接。
  - 错误码按交易系统的分类: 406 有 60 秒预算, 只有订阅成功才清零; 407、500 重连; 其余停 feed。
  - 交叉的 NBBO 记为当前无报价, 不停 feed。
  - 对照表 `server/data/hip3_sip_sources.json` 由 `scripts/discover_hip3_sip_sources.py` 生成, 85 个币。
- **凭证**: 从环境变量读; `ob_ops.py` 经 `bash -ic` 加载 unit 用户的 bashrc(用户定)。可选参数按各版本 `--help` 给, 回滚安全。

## 2026-10-28 HIP-3 美股本地 mark v2: oracle 项加按币在线估计的偏离延续(设计 000537 r1–r2)

- **起因**: v1 上线后盘后实测(10-27 20:52–24:00Z, 85 只), 提前 1 秒的预测 P90 2.79bp, 比节点已知 mark 的 1.89bp 还差,
  误差几乎都来自 (1): 盘后点差宽, 中继的聚合价长时间停在点差里同一处(NET 常驻 +15bp, BX/UMC/KSTR 超过 50bp 撞限幅)。
  用户定"按币在线估计"。
- **机制**(`local_marks/hip3/carry.rs`):
  - 偏离 = 写入的 oracle − 区块前 `RELAYER_READ_LEAD_MS` 的 SIP 中间价(按币留几秒中间价历史, 换连接代次或无有效报价就清空)。
  - ρ̂ = 本美东时段(每天的盘前/正常盘/盘后各算)相邻写入的偏离对上一次偏离的加权回归斜率, 不带截距, 按区块时间衰减, 限 [0, 1]。
  - (1) = 中间价 + ρ̂ × 上次偏离, 再照 v1 舍入、限幅; (2) 跟着 (1) 走。
  - 预热(权重不够或偏离全为 0)、读数时已换时段: 不修正, 照出 v1; 帧里 `carryStatus` 标明。
- **节点状态**: `Hip3Coin` 加 `oracle_time_ns`(oracle 条目自己的 `last_update_time`): 一次更新会列出这次没写的币、带旧时间,
  只有它能识别中继这次写没写。checkpoint 种子为 `None`。
- **owner 状态**: 每个 HIP-3 币一份 `Hip3CoinState { ema, sip_mids, carry }`, 代替原来的 `Vec<Hip3Ema>`。
- **输出**: `extHip3MarkPrices` 升 `schemaVersion: 2`(oraclePx 含义变了), 加 `carry*`、`carriedPx`、节点腿 `storedOracleTimeNs`。
  主站帧不变; `local_predicted()` 接口不变但 HIP-3 的值变, `extForcedFlow` 的本地距离随之变。
- **离线证据**(hyperliquid-trade `analyze/research/xyz_oracle_sources_20261027/`, d7c76a42): 只看 (1), 提前 1 秒,
  P90 中位: 盘后 31 只 5.37 → 2.39bp, 盘前 2.25 → 1.65bp, 正常盘 1.52 → 1.53bp(自动学到 0–0.2), 都和本段事后最优斜率相当;
  换时段不清零开盘变差, 偏离按节点到达时刻算开盘也变差, 所以清零、留中间价历史。

## 2026-10-28 HIP-3 美股本地 mark 加夜盘: Alpaca BOATS(设计 000543 r1)

- **起因**: 用户问 Alpaca 是否有夜盘行情。trade.xyz 文档写明: 外部价覆盖周日 20:05 ET 到周五 20:00 ET;
  夜盘 20:05–04:00 ET 由 Blue Ocean ATS(BOATS)提供。v2 夜里一律撤回(`sipSession`)。
- **时段**(`hip3/session.rs`): `EquitySession { Pre, Regular, Post, Overnight }`, 身份带美东日期; 夜盘以开始那晚的
  日期为身份, 周日到周四晚开始; 20:00–20:05、周五晚到周日 20:05 不属于任何时段。
- **数据源**: 第二条 Alpaca 连接 `Feed::BOATS`(`wss://stream.data.alpaca.markets/v1beta1/boats`, 标签 `equity_alpaca_boats`),
  协议、确认、心跳、错误分类和 406 预算与 SIP 共用(各一份预算); 两条全天连着, 按 tick 的时段取报价。
  开关仍是 `--hip3-sip-marks`(改名会牵动 `ob_ops.py` 按版本给可选参数)。
- **偏离修正**: 夜盘自成时段; 中间价历史按(数据源, 连接代次)记, 换数据源清空, 查询指定数据源, 04:00 迟到的夜盘写入
  查不到 SIP 历史里的 BOATS 中间价。
- **输出**: `extHip3MarkPrices` 升 schema 3, 报价字段改成与数据源无关的 `quote*`, 加 `quoteFeed`; 撤回码 `quoteSession`、
  `quoteUnavailable`、`quoteQuiet`; `/extMarkInputs` 加 `boats` 段, 两段的币条目字段改名 `quote`。
- **离线证据**(hyperliquid-trade `analyze/research/xyz_oracle_sources_20261027/`, b20ac07d, 10-28 00:05–02:00Z):
  85 只都有 BOATS 双边报价; 读价提前 1–1.5 秒最准(同 SIP); oracle 项提前 1 秒 P90 中位: 只用中间价 6.27bp, 按币在线修正 2.68bp;
  8 只冷门股超过 10bp(最大 SHAZ 229bp), 本批不加门控, 上线后按币量化, 开 HIP-3 交易时按币核对。

## 2026-10-29 HIP-3 美股本地 mark: EMA 跨过 60 秒以内的空档(用户 10-29 定)

- **起因**: 10-28 全天实测(bm `hip3_live_v3`), 提前 1 秒有预测的比例只有夜盘 58%、盘前 42%、正常盘 33%, 开盘 13:30–13:45Z 为 0;
  多数撤回是 `emaWarmup`。旧规则两次采样相隔超过 5 秒(节点状态的新鲜度上限)EMA 就从头来, 要再采 750 秒。
  bm 节点繁忙时段约每 5 分钟卡一次(:x0、:x5 后几秒), NVDA 全天超过 5 秒的 53 次卡顿里 37 次 5–30 秒、2 次 30–60 秒;
  卡顿共 53 分钟, 之后重新预热共 442 分钟(全天 1,439 分钟)。
- **改动**(`local_marks/hip3/ema.rs`): `MAX_GAP` 从 5 秒改成 60 秒; 空档内照常按最后一个样本折入(每个 tick 本来就这样),
  超过 60 秒才从头来。漏掉的中继采样占 EMA 权重 1 − exp(−空档/150 秒): 15 秒 9.5%, 60 秒 33%, 误差是这部分权重乘 x 在空档里的变化。
- **有预测时的精度**(同一天, 提前 1 秒, 预测 / 节点已知 mark 的 P90/P99 bp): 正常盘 2.01/6.4 对 3.35/9.9, 盘前 2.28/10.9 对 2.53/10.6,
  夜盘 1.73/11.4 对 1.70/6.8。

## 2026-10-29 清算候选刷新: 一次刷新里的变化登记按(币, 用户)索引, 不再逐条扫描(用户 10-29 定)

- **起因**: hyperliquid-trade forced_flow 首晚实盘复盘(`analyze/research/forced_flow_live_20261028/`)里, 发晚的 IOC 用的都是止损成交前的帧;
  止损成交那一块之后, OB 0.8–1.4 秒不出任何币的被迫成交帧。账户线程 `accounts-owner` CPU 99%; 每 100ms 读一次 `/metrics` 共 16 秒,
  清算候选刷新占 85%, 每 1–3 秒一次独占 0.6–1.3 秒, 期间账户视图落后盘口 0→15–22 块。`perf`(15 秒, 99Hz): `note_update` 自身 23.9%,
  另有 13.9% 在 libc 的 ≤32 字节比较。
- **原因**: `note_update` 每记一条候选变化, 都线性扫一遍本次的 `enter`/`update` 找同一(币, 用户)。mark 更新时 `evaluate` 先列出上万条,
  `walk_book` 再把上万条的盘口估计改回 pending、逐条记, 是 n×m。
- **改动**(`accounts/candidates.rs`): `ChangeIndex` 在 `walk_book` 第一次记录时由两个列表建出(每个(币, 用户)取第一条, entries 在前,
  即扫描找到的那条), 之后的记录都经过它。输出的列表内容和顺序不变, `CandidateChanges` 结构不变。
- **验证**: 新测试 `indexed_notes_leave_the_changes_a_scan_leaves` 与原扫描逐条对照(含同一用户既在 entries 又在 updates、未列出的用户
  追加后再记); 计时测试(`#[ignore]`, release): 20,001 条、20,004 次记录, 扫描 316ms、索引 4.3ms(开发机)。
- **未改**: 多币持仓账户在每次 mark 更新时全部重算(`reindex` / `replayed_row` / 按币名查 `CoinBook`, perf 约 37%), 以及刷新与被迫成交帧
  共用一个线程; 部署后再量。

## 2026-10-29 被迫成交挪到独立线程，经账户视图的副本读数据（设计 hyperliquid-trade 000563 r1–r2，用户 10-29 定）

- **起因**：`5d192ec` 之后，账户线程仍有约 88% CPU，每 1–3 秒被强平候选刷新独占 0.35–0.8 秒；约每 12 分钟装一次检查点，也会占用约 1 秒。被迫成交帧在同一个 `select!` 循环里，这期间所有币一帧都出不来，而止损恰好在 mark 更新那一块触发，正是刷新最重的时候。用户定分两批：本批先隔离；“只重算离强平近的账户”另做批次 2。
- **结构**：
  - 新线程 `forced-flow`（`accounts/forced_flow_thread.rs`）：`ForcedFlow`、读盘口任务的读回、本地 mark、查询都在这里。`spawn_owner` 加了线程名参数，账户线程和它各用一个。
  - `AccountsHandle` 的 `forced_flow`、`attach_local_marks` 改走新线程。
- **账户副本**（`accounts/flow_accounts.rs`）：
  - 账户线程发事件：`Snapshot`（新代次）、`Block`（应用区块后、候选刷新前，带改动的持仓和变动的 mark）、`Candidates`（与广播共用 `Arc`，版本连续）、`CandidatesConfirmed`（每次刷新确认到的高度，内容没变也发）、`CandidatesWithdrawn`。候选变化统一从 `Publisher::publish_candidates` 这一个出口转发，刷新、walk、停止三条路径都经过它。
  - 账户线程只用 `try_send`：队列满了丢掉增量、下一个输入补发全量；被迫成交线程不在了，就由账户线程撤回被迫成交。
  - 有效性单独走 `watch`：队列满、账户视图或 mark 不可用、账户线程结束，都会立刻让副本停止服务；副本发现事件乱序，也停止服务，并请账户线程补发全量。副本不服务时帧全部为 `unavailable`，已发布的帧当场撤回。
  - mark 由账户线程用 `MarksAt::mark` 算好，只发变化的那些。这样没有第二份 mark 规则，设计稿里“共享 `NativePriceSnapshot`、另抽共用函数”的写法因此不需要。
- **代次与候选（审查 000566 r1）**：
  - 线程记住当前服务的代次，换了新代次就当场撤回旧帧、立刻按新代次重算，不依赖中间看到过“不可用”。
  - 每次计算开始时记下代次；扫盘、读回带回的代次对不上就作废，按新事实重读。
  - 撤回时，等待中查询已经收到的帧一并清掉，对应的币放回待算，由重算或“不可用”回答（审查 000566 r2）。
  - 强平候选可用并入 `serving()`：只撤回候选也会撤回所有帧、作废进行中的读取。`copy()` 只看持仓和 mark，用来展开“全部币”订阅，候选不可用时每个币仍发一帧“不可用”。
- **帧的语义**：
  - `accountsHeight` / `accountsTimeMs` 取持仓、mark 和候选确认高度中最旧的那个，是所有账户输入共同的下界；交易端的年龄门槛一行不改就能覆盖它们。
  - 强平条目在所有档位都只来自推送的候选（刷新时离节点 mark 1000 bp 以内，已越过强平价的也算），不再读完整的重放索引；预计强平单的冷却按刷新时的状态算。`Candidates::liquidation_inputs` 删除，两边共用 `liquidation_band` / `in_liquidation_band`。
- **指标**：
  - `orderbook_forced_flow_owner_seconds{step}` 名字不变，改为记在新线程上，并多一个 `accounts` 步；
  - 新增 `orderbook_forced_flow_accounts_resync_total{cause}`、`…_queue_depth`、`…_snapshot_seconds`。
- **验证**：
  - `cargo test -p server --lib` 全部 1,072 个测试通过。原有被迫成交测试都改成经副本计算，作为新旧对照。
  - 新增测试覆盖：块与版本按序应用、乱序时停止服务、监视信号控制服务、队列满时立刻撤回直到新快照、候选撤回后再恢复时补发全量、帧的账户高度取最旧那部分、副本停止服务时当场撤回且旧的读回不再采用、账户线程忙时查询照常回答、账户线程结束时撤回并退出；新代次先于监视信号生效时旧扫盘作废并按新事实重读、代次切换当场撤回并立刻重算、只撤回候选时扫盘不出可用帧、线程循环在换代次和只撤回候选时各撤回一次、多币查询已收的旧代次帧在换代后重算。

## 2026-10-29 账户在候选刷新之后变过的强平条目，等下一次刷新确认再列（设计 hyperliquid-trade 000569 r1–r2，用户 10-29 定）

- **起因**：`24e71a8` 上线后，止损触发到帧里不再列着它，OB 这一段中位 61 ms；强平却仍是中位 949 ms、P99 1,878 ms，和 `5d192ec` 一样（10-29 15:52–17:00 共 928 次，hyperliquid-trade `analyze/research/forced_flow_live_20261028/` 的 `liquidation_drop_latency.py`）。原因是强平条目用的是候选刷新时的仓位和清算价。副本每一块都已经知道仓位变了，但要等下一次刷新，这个人才从帧里消失。
- **规则**（`AccountsCopy` 一处判断）：
  - 副本记录候选确认高度之后、数量真正变过的仓位（`ChangedSinceConfirmed`：用户 → 币 → 变化高度）。块里被触及、但数量没变的不算。
  - 逐仓候选看它自己这个币；全仓候选看这个用户的任何币。全仓的清算价随池里任何仓位变化，刷新之后新开的全仓仓位又不在 `cross_pool_coins` 里，所以按用户全部算（用户确认）。
  - 变过的候选不列，直到 `CandidatesConfirmed` 推进到变化的那一块：那次刷新已经把 touched 用户全部重新评估过。全部强平、部分强平剩下的那截（用户定“先不列”）、持有人自己加减仓，都一样处理。
  - 快照的持仓比候选新时（账户线程在输入之后、刷新之前发快照），副本不知道这一段里谁变过，所以安装后所有强平条目都先不列，直到确认高度追上快照高度。
  - `liquidation_inputs` 按用户排序返回，同样的事实两次调用结果相等。
- **在途计算（审查 000569 r1 P2-1）**：带强平条目的计算要先发出盘口 walk，walk 回来时原本只核对代次。现在 `Walking` 记下这次计算用的强平输入和当时的 mark（`AccountsRead`）；回来时用同样的 mark 再取一次，不同就丢掉这次结果、整份重算，不在旧结果上删条目。
- **不变**：帧的 `accountsHeight`（仍取最旧的账户输入）、止损条目、强平条目的算法、`extForcedFlow` 字段、账户线程及其事件。只有现金在变的情况副本看不到，仍等刷新，和以前一样。
- **代价**：仓位变过的用户，强平条目会消失到下一次刷新，按上面的数字约 1 s。这段时间里他的下一截如果真被强平，帧里没有，交易端只会少发，不会多发。
- **验证**：`cargo test -p server --lib` 全部 1,078 个测试通过。新增测试覆盖：
  - 全仓候选：本币或别的币数量变过就不列；被触及但数量没变照列；确认只清掉不晚于它的变化；部分强平后按新数量重新列出；全部强平后不列。
  - 逐仓候选只看本币。
  - 快照持仓比候选新时全部不列，确认追上后恢复。
  - walk 在途时，仓位清零、或刷新先确认了新数量：都重算，不发旧结果；只有无关仓位变了：照常发布，不重算。
  - 变异检查：把在途核对改回只看代次，两个在途测试失败；把全仓改成只看本币，两个副本测试失败。

## 2026-10-29 清算候选批量读行：同一保证金池只估值一次（独立审查通过，未部署）

- **问题**：`Candidates::reindex` 调用 `AccountBook::replayed_rows`，后者对每个仓位再次遍历该账户全部持仓、解析市场和保证金表、估值其他同池仓位。多仓账户的这部分工作近似 O(n²)。BM 10-29 21:14 UTC 的 15.019 秒窗口里，整个候选刷新累计耗时 11.280 秒；这个指标包含索引等工作，不等同于本次优化部分的耗时。
- **实现**：`accounts/replay/rows.rs` 持有一次读取内的 `AccountRows`。按真实保证金池汇总 `size_raw * mark_raw - maintenance_micro`，每行用池总和减去自身；普通账户按 DEX，统一账户合并 USDC DEX，逐仓不参加全仓汇总。市场、表和持有价估值每成员处理一次，单仓直接求解。`CrossPool`、`RowTarget`、`HeldAtMark::net_micro` 和 `solve_net` 供批量及单仓路径共用，持仓输出顺序不变。
- **状态与精度**：池和、扣除自身都使用 i128 微美元，扣除后才转换 f64；目标仓位的维持保证金档位仍由原求解器按待求价格选择。缺 mark / 表单独计数：目标缺 mark 可求其自身清算价，其他同池成员缺 mark 则不可求；未知市场仍按原规则撤回该账户全部全仓行，逐仓可用性不受其影响。不跨读取、区块或 checkpoint 保留任何估值缓存；`AccountBook` 的 anchor/applied 高度门控及 mark 同高度条件不变。
- **范围**：本次只优化 live replay 的批量估值热路径。候选范围、mark 更新触发重算的范围、索引删除重建、checkpoint 的逐行核验和初始快照估值算法均未优化。没有服务级吞吐/延迟或线上验收结论。
- **正确性验证**：完整 `cargo test -p server --lib`：1088 通过、12 ignored；`cargo check --workspace --bins` 通过。新增测试中的 f018925 旧扫描作为冻结参照（只在测试内），核对可用行、顺序、池大小、estimated 和清算价 f64 位模式；覆盖 PerDex/Unified/逐仓/非 USDC，缺失 mark/表/未知币，停止/未跟踪账户，开平仓、现金/表/池变化、高度错配和换 checkpoint。ASTER 1196430000 的 40 仓金样本覆盖所有轮转、正反顺序和 ±1 USDC wei；另覆盖超过 2^53 的整数贡献相消。Release 下新增的 9 项正确性测试也通过。
- **隔离基准**：开发机 release，合成的单个 Unified 账户，保持约 20 万次仓位输出；每个规模交替新旧顺序跑 5 轮，取中位数。仅计 `MarginStates` 清算行计算和返回集合；不包含 `AccountBook` 外层投影、候选索引、网络或节点处理，不可外推为整体 OB 提速。

  | 仓位数 | 重算次数/轮 | 旧扫描中位数 ms | 批量中位数 ms | 旧/新 |
  | --- | --- | --- | --- | --- |
  | 1 | 200000 | 8.674 | 7.586 | 1.14 |
  | 4 | 50000 | 18.244 | 9.329 | 1.96 |
  | 16 | 12500 | 63.092 | 8.514 | 7.41 |
  | 64 | 3125 | 208.164 | 7.844 | 26.54 |
  | 196 | 1020 | 594.714 | 7.620 | 78.05 |

- **复现**：在独立 OB 仓库根目录运行以下命令；禁止 `cargo fmt`。

  ```sh
  cargo test -p server --lib
  cargo check --workspace --bins
  cargo test -p server --release --lib accounts::replay::rows::tests
  cargo test -p server --release --lib accounts::replay::rows::tests::benchmark_pool_batch_against_original_scan -- --exact --ignored --nocapture
  ```

- **独立审查**：用户转交的 Claude 审查通过，无阻塞问题；复跑 accounts 模块 589 项、release 新增 9 项正确性测试及二进制编译检查通过。变更基于 f018925，含新增的 `server/src/accounts/replay/rows.rs` 及 `rows/tests.rs`；优先复核池归属、缺失成员扣除、整数/浮点转换顺序、批量与单仓一致性。原始测试及基准日志保存于 `/home/yfb222333/research-data/hyperliquid-trade/hl_node_perf_20261029/pool_rows_optimization/`。提交前清理本批新增的 12 条 clippy 告警（const、文档反引号、测试整数转换和 ignored 原因）；未部署。

## 2026-10-29 被迫成交帧标出持仓和 mark 所在的块（`positionsHeight`）

- **起因**：24e71a8 起帧的 `accountsHeight` 是所有账户输入的下界，候选落后时取候选确认的块；帧里的持仓和 `nodeMarkPx` 却是副本当前块，可以更新。hyperliquid-trade 的常驻评估器按 `accountsHeight` 把帧当成 mark 和持仓所在的块，取"块之前的最后一帧"时取到了已经含触发块新 mark、或已拿掉被清算仓位的帧：10-29 15 时起"触发条件在上一次价格变动时已满足"每小时 400–5,000 条，19 时起清算几乎都判"前一帧没列"。实例：BTC 块 1201986266 触发，`accountsHeight` 1201986264 的帧已带该块的新 mark 79403；xyz:STRC 0x9bafb90f77bedd8e917036d314b25ef044b752ae 块 1201920927 被清算，帧一直列到 20:00:03.093，`accountsHeight` 1201920926 的帧已没有它。
- **改动**：`heights` 增 `positionsHeight` / `positionsTimeMs` = 副本当前块（`FlowAccounts::positions_at`，持仓和 mark 同块应用），不小于 `accountsHeight`。`accountsHeight` 含义不变，交易端的年龄门槛照旧读它；交易端的帧解析不拒绝未知字段。
- **测试**：`the_account_part_is_of_its_oldest_input` 加 positions 1,005 / 1,005,000（候选 1,000）；清算在 walk 途中消失的用例加 positions 1,001；JSON 视图测试加两个字段。

## 2026-10-30 后备清算接管按规则回放（`backstop`）

- **起因**：10-29 20:01 UTC 英特尔、亚马逊盘后财报，xyz 预言机每次更新最多动 50bp，mark 落后盘口 5–8%，HIP-3 一小时内 706 个账户被后备清算接管。现金回放遇到后备成交就让账户停到下一个检查点，被接管账户的其他仓位在最剧烈的这段时间里清算价未知。
- **规则（hyperliquid-trade `analyze/research/backstop_liquidation_20261029`）**：
  - 同一块里，杂项事件有一条 `liquidation` 账本记录（`leverageType`、`accountValue`、`liquidatedNtlPos`、`liquidatedPositions`）；成交里每个仓位一对，被清算方和清算金库 0x4000000000000000000000000000000000000001 各一笔，手续费 0，带 `markPx`。
  - 接管价 `px = mark × (1 − sign × accountValue / liquidatedNtlPos)`，截到价格精度。67 条全对：本服务证据日志 20 条，官方账本 47 条，其中全仓 38 条、两仓位 2 条；多头全向下截，偏差不超过 0.043bp。
  - 这个价格让被清算的池子只剩截断尾差。
- **改动**：
  - `accounts/backstop.rs` 负责规则：每个被清算账户在本块的证据（指向它的记录和它的后备成交）组成 `TakeoverEvidence`，后备成交分为 `Liquidated(证据)`、`Liquidator` 和 `Unchecked(原因)`。
  - 证据随成交事件写进事件日志。现金回放应用这笔成交时，才用当前锚点的 universe（币种精度）核对，所以检查点晚于接管块装入时结果也一样（审查 000577 r1 P2：原先在构造事件时就把判定定死，没有 universe 时判成"不在 universe 里"，检查点装入后也改不回来）。
  - `misc.rs` 把 `liquidation` 解析成 `LedgerDelta::Liquidation`。其他 `leverageType` 照旧算未读类型。
  - `book.rs` 在 `apply_block` 里用本块的杂项事件和成交收集证据，成交事件带着证据构造（`FillEvent::in_block`）。
  - `replay.rs` 把被清算一侧当普通成交回放，账户标 `estimated`，并记下接管块；账本记录必须跟在同块接管之后，本身不动现金。
  - 金库一侧、统一账户的全仓池、现货借贷清算、规则解释不了的接管，仍然停账户。停账户原因 `backstopLiquidation:<说明>` 现在带说明。
- **未测量**：截断尾差最后去哪、统一账户全仓池、金库自己的现金。下一次后备清算发生时，由检查点逐账户核对到 10^-8 USDC，对不上会留 `replayMismatch` 证据。本次测试里接管前的现金是按记录反推的合成值，不是节点余额（当时的检查点已过保留期）。
- **测试**：
  - `backstop::tests`：4 个真实样例的判定；另一个截断方向的价格接受；价格再差一个单位、数量不对、没有记录或两条记录、缺一个仓位的成交、名义对不上、币种不在 universe 里，都判为解释不了。
  - `replay::backstop_tests`：逐仓接管后交叉现金 −0.001748、全仓两仓位 −0.002952；统一账户全仓、池子和回放不一致、只有记录没有接管，都停账户。
  - `book::tests::a_backstop_takeover_block_replays_its_liquidated_side`：走完整块路径，锚点在块前、块后装入各跑一次，结果相同。
  - `misc::tests::reads_a_backstop_takeover_record`。
- **复现**：在独立 OB 仓库根目录运行以下命令；禁止 `cargo fmt`。

  ```sh
  cargo test -p server --lib -- accounts::
  cargo check --workspace --bins
  ```

## 2026-10-30 HIP-3 美股本地 mark：(2) 锚定中继自己公布的 EMA，去掉 750 秒预热（设计 000582 r2，用户 10-30 定）

- **起因**：(2) 的 EMA 原来完全自己攒。OB 每次启动要采满 750 秒，节点卡住超过 60 秒也要从头来。10-29 正常盘能出预测的只有 75%，其中 15% 是预热，来自两次 OB 重启。开盘 13:30–13:45 只有 0.9%：节点卡了 128 秒和 135 秒，又赶上一次重启。10-30 01:18、07:00 两次重启，各 12.5 分钟。
- **依据**：中继的 setOracle 里，`markPxs` = [[(1)], [(2)]]，(2)−(1) 就是它此刻的 EMA（10-27 逐块核对）。bm 10-30 08:36–08:39 共 73 次 xyz 写入，平均约 2.96 秒一次，每次带全部 127 个币；两组的币相同，第一组等于 `oraclePxs`。
- **改动**：
  - **`accounts/marks/actions.rs`**：`MarkAction::Hip3Oracle` 加 `submitted: Result<Vec<SubmittedMark>, String>`，按 xyz 模型读两组。其他形状只算这个模型的 `Err`，整条 action 照常成功，因为协议允许 0、1、2 组。
  - **`accounts/marks/mod.rs`、`seed.rs`**：每块按 action 顺序把 (2)−(1) 换算成币的价格单位，写到 `Hip3Coin.relayer_gap`。检查点恢复时是 `Err(SinceCheckpoint)`，读不出时是 `Err(Unreadable)`。读不出的原因在币转为 `Unreadable` 的那一刻记进 `MarksBlockReport.relayer_marks`，由 runtime 以 `Cause::Hip3RelayerMarks` 报诊断；不进 `errors`，不算流丢失，检查点重放归类相同。
  - **`local_marks/hip3/ema.rs`**：中继每次写入就锚定，值从写入那块用当前 x 外推到当前 tick；两次写入之间照旧用自己的采样往前推。采样空档超过 5 秒就清值，但保留已用过的写入时间，只认更新的写入。删掉 `WARMUP` 和 ef7a90c 的 60 秒规则。撤回原因 `emaWarmup` 改为 `relayerGapUnknown`。
  - **`extHip3MarkPrices` schema 4**：`emaSampledS` 换成 `emaAnchorTimeMs`。
- **测试**：
  - `actions` 解析：正常两组，以及 1 组、只在一组里、数字不是字符串；
  - `marks::a_set_oracle_sets_the_relayer_gap_and_an_unreadable_one_is_no_loss`：依次是有效 → 1 组 → 再 1 组（不重复报告）→ 有效 → 精度超出 → 有效，然后用检查点 1000 重放 7 块，`errors` 为空，节点价格一直可用；
  - `ema` 的 5 个测试；
  - snapshot 集成：4 秒空档继续可用，6 秒空档后等新写入，新写入后立即可用；`Unreadable` 撤回，同一次写入不能再用。
  - 变异检查：同一次写入也能重新锚定 → 7 个测试失败；空档上限改成 60 秒 → 集成测试失败；把读不出当成流丢失 → 验收测试失败。

## 2026-10-30 `extMarkPrices` 本地腿加外部买一、卖一的加权中位数和各家报价（设计 000588 r1，用户 10-30 定）

- **起因**：强平接货研究发现，挂单时接货价相对外部价的位置是区分赚亏的主要因素：接货价比外部还激进时，基本是 HL 落后于外部。用户要求列出外部各家的买一、卖一，以及与 `externalPx` 同口径的中位数。原来各家报价只在 `GET /extMarkInputs`，是另一次读取，和帧对不齐。
- **改动**（`local_marks/snapshot.rs`、`prices_view.rs`）：
  - 本 tick 有效的报价先各自乘 multiplier（`valid_quotes`），oracle 和 C 都从这里取各家中点的加权中位数（`mid_median`），结果和原来逐位相同。
  - `LocalLeg.external_quotes`：C 用到的那组永续报价，买一、卖一各自的加权中位数（同样的家、同样的权重），以及每家明细。
  - 本地腿在 `externalPx` 后加 `externalBidPx`、`externalAskPx`、`externalQuotes`（`{venue: {bidPx, askPx, receivedTsMs}}`）。价格是精确值，已乘 multiplier，不取整到价格网格；`externalPx` 照旧取整。本地腿撤回时这三项一起没有。
  - 只加字段，`schemaVersion` 保持 2：网关只认 2，录制原样整行。缺这三项的帧来自加字段之前的服务。
- **体量**：每帧约多 400 字节（+28%），`GET /extMarkPrices` 全部币约 260 KB → 330 KB（推算，上线后实测）。
- **测试**：
  - `snapshot::the_external_quotes_take_the_external_prices_venues_and_weights`：BTC 三家永续，Bybit 带 1,000 倍 multiplier；买一中位数取 Binance，卖一中位数取 Bybit 的 100,006.24（不在 0.1 网格上，不取整），C 是各家中点的 100,005，不是两侧中位数的中点；Binance 断线后三项和明细都剔除它。
  - `prices_view` 帧测试：三个新字段的格式；撤回的本地腿没有它们。
  - 变异检查：中位数换成取最高、不过滤有效性、买卖两侧写反、帧里买卖写反、明细不乘 multiplier、帧里单家买卖写反，各有测试失败。
- **复现**：在独立 OB 仓库根目录运行以下命令；禁止 `cargo fmt`。

  ```sh
  cargo test -p server --lib -- local_marks
  ```

## 2026-10-30 被迫成交帧：周期按 tick 预定时刻量（审查 000586）；近档改为来块就算（设计 000587 r2）

- **起因**（bm 10-30）：
  - 09 时 2,781 张止损，出块到帧里不再列它，中位 306 ms：节点段 187 ms，OB 段 102 ms（p90 175）。OB 段主要在等 100 ms tick。
  - 判断"满一个周期"时取的是处理时刻，两次处理的抖动让约一半 tick 被跳过：552 个近档 key 每秒只开算 6.4 次，BTC 帧间隔 57% 是 200 ms。
- **周期**（2ce4937）：tick 开算记 `Interval::tick()` 的预定时刻，周期在预定时刻之间量。
- **来块就算**：
  - 监听器：新增 book 修订号（`book_revision.rs`，`watch<u64>`）。
    - 输入：实时应用的 diff、status 批次（含触发单表变化），以及快照安装与重放。低于已安装高度被丢弃的批次、成交、oracle 不算。
    - 发布：与 L2 推送同一条规则（`burst_deadline`：静默 2 ms 或已等 20 ms），不看订阅和信任。
    - 一块约 1,000 行、跨度中位 13 ms，不能按首行的高度触发（000587 r1 P2-1）。
    - 主循环等 L2 与修订号两者中较早的截止时刻，快照拉取结束时也刷新一次。
  - 被迫成交线程：
    - tick 改为 50 ms；
    - 新增修订号分支；
    - 近档 key 空闲、修订号有变、距上次实际开算满 50 ms 即开算；
    - 周期兜底（满 100 ms，按预定时刻量）照旧；中档、远档只按周期。
  - Pipeline 分记 `started`（周期基准，也供查询比较）、`began`（实际开算时刻，下限用，000587 r1 P2-2）和 `revision`。
  - 线程各步的处理时刻取运行时时钟（`handled_now`），测试可暂停。
- **测试**：
  - 修订号：
    - 一阵多批只在静默后发一次；
    - 不停时 20 ms 发；
    - 发后同块后续批次再发；
    - 没有订阅也发。
  - 监听器：状态、diff 批次和安装触发；成交、低于安装高度的批次不触发；不可信时照发。
  - owner：
    - 修订立即开算；
    - 50 ms 内等到下一个 tick；
    - 在算不重开，结束后由 tick 接住；
    - 迟到 tick 的下限从实际开算时刻起算；
    - 无修订只按周期；
    - 中档不跟；
    - 查询不被它之前开始的计算满足。
  - 线程（暂停时钟）：
    - 迟到 3 ms 的 tick 不打乱周期；
    - 修订不等 tick 开算，50 ms 内不重开。
  - 变异检查 7 项，均有测试失败：
    - 下限改用 `started`；
    - 去掉修订比较；
    - 首个输入就发；
    - 修订号受信任牵连；
    - 安装不算输入；
    - tick 传处理时刻；
    - 修订分支不开算。
- **预期**（推算，上线实测）：
  - OB 段中位降到二三十毫秒；
  - 近档开算约每 key 14 次/秒（追块时修订路径最多 20 次/秒）；
  - 帧量和录制约为原来的 2 倍。

## 2026-10-30 被迫成交帧：止损触发按币快速重算（设计 000591 r1）

- **起因**（bm 10-30 13:00–13:10，"来块就算"上线后 514 张止损）：
  - OB 段（节点写出触发行 → 帧）中位 55 ms，p90 80 ms；删掉止损的帧，监听器中位已到触发块 +1。
  - 原因：块内 `triggered` 状态排在最后（位置中位 0.96），前面停顿中位 7 ms（p90 10 ms）。修订号的 2 ms 静默在停顿处就发布，触发行的修订又落在 50 ms 下限内。
- **改动**：
  - `Mailbox` 从 `accounts::walk` 原样移到 `crate::mailbox`，监听器也用它，不反向依赖 accounts。
  - `OrderBookState`：表里持有的触发单被 `triggered` 移除时（`on_status` 返回 true 且 status 为 `triggered`），记下币，`take_fired_trigger_coins` 取走。撤单、成交等其他移除照常走修订号。
  - 监听器：Orders 批次实时应用成功后，把这些币 `post` 到 `trigger_fires`。不等静默，不看订阅和信任。
  - `BookSource.fires` 交给被迫成交线程。
  - 被迫成交线程新增 `fires` 分支 → `on_fires`：该币需求中的近档 key 空闲则立即开算，不受 50 ms 下限约束；在算则记 `fired`，读回到空闲后补算一次。任何新开算都清除 `fired`。
- **边界**（审查 000591 r1 P3）：
  - 这是新增触发路径上每个 (coin, tier) 的合并与并发边界：信箱按币合并，在算只留一个待补算标记，通知不会堆积成任务。它不是 CPU 或锁占用的固定预算。
  - 同币 Near、NearWide 各自开算；周期、查询、重读照常；连环触发时仍可能连续重算。
  - 快速开算同样更新 `began`，块尾修订带来的盘口更新，由满 50 ms 且空闲后的下一次修订或 tick 接住，另加读取与计算时间。
- **测试**：
  - state：持有单被触发才记，撤单、表外单不记。
  - 监听器：触发批次应用即发；低于安装高度的批次不发。
  - owner：空闲 key 在下限内也立即开算；在算时多次触发只补算一次；只影响该币的近档；需求为"每个币"时也按币生效。
  - 线程（暂停时钟）：修订开算后 7 ms 来触发，立即开算。
  - 变异检查 6 项，均有测试失败：不区分 status、监听器不发、快速通道受下限、没有补算、开算不清 `fired`、线程忽略信箱。
- **预期**（推算，上线实测）：止损删除的 OB 段中位降到个位数至十几毫秒。

## 2026-10-30 `extMarkPrices` 本地腿加常态基差 `externalBasis`（设计 000594 r1，用户 10-30 定）

- **起因**：forced_flow_trader 的接货外部价过滤，把外部同一侧中位数先换到 HL 的水平，再比较。HL 主站中间价相对外部永续有常态偏差（一半的币超过 5 基点，HMSTR、USUAL、SAGA 这类达几十基点），不扣掉会把这些币的一个方向整个挡死。用户定：由 OB 算好推出来；最近 10 分钟、每秒一个样本取中位数，不用指数平均；至少 120 个样本才发；价格档位粗的币先不细化。
- **研究依据**（hyperliquid-trade `analyze/research/ob_external_basis_20261030/`，bm marks 录制 10-27..10-30，176 币）：
  - 常态基差变得慢：10 分钟中位数 1 小时内变 1.9 基点（各币中位）。
  - 跟踪误差：10 分钟窗 0.64 基点，30 分钟窗 1.01 基点。
  - INIT、PYTH 触发时 HL 分别偏离 92、15 基点，三种基线都只动了不到 1 基点。
  - 时间加权的指数平均，断档后的第一个样本占 39% 的权重。
- **改动**：
  - 新增 `local_marks/external_basis.rs`：
    - `sample`：HL 盘口中点 ÷ 外部两侧中位数的中点 − 1，f64 计算。
    - `ExternalBasisWindow`：按单调时钟，每 `SAMPLE_INTERVAL` 最多取一个样本，每个 tick 剔除 `WINDOW` 之前的样本，只在窗口变化时重算中位数（偶数个样本取上中位数，和 `weighted_median` 在等权时一致）；样本少于 `MIN_SAMPLES` 时撤回为 `externalBasisSamples`。
  - `snapshot.rs`：
    - 跨 tick 的状态合成 `OwnerState { hip3, external_basis }`，由 `CoinTable::owner_state()` 创建，`assemble` 拿 `&mut OwnerState`（原来是 `hip3_states`）。
    - 永续报价在 `coin_marks` 里每 tick 算一次（`valid_quotes(..).map(ExternalQuotes::of)`），同时给采样和 `local_leg` 用；本地腿的检查顺序和撤回码不变。
    - 采样只看自己的输入：盘口新鲜且可信、HL 两边都有价、至少一家永续报价、feeds 没停。不看节点状态，所以节点落后时照样采样。
    - `LocalLeg.external_basis`：`Result<ExternalBasis, Withheld>`。
  - `prices_view.rs`：本地腿加 `externalBasis`，就绪时是 `{status, ratio, samples}`，不可用时带撤回码和原因。本地腿撤回时没有这一项。
  - `price.rs`：新增 `VenuePrice::to_f64`。
  - `schemaVersion` 保持 2，只加字段。
- **已知限制**（README）：
  - 价格档位粗的币（28 个币一档 ≥ 2 基点），基差的精度只有半档。
  - 盘口比外部报价落后约 0.7 s。
  - 外部各家本身不一致时，基差是相对加权中位数的。
  - 用录制数据复算只能对照趋势，不能逐值相等（审查 000594 P3）。
- **测试**：
  - `external_basis`：120 个样本才发布；偶数个样本取上中位数；窗口采满时，短于半个窗口的偏离不挪动中位数，到半个窗口才挪（预热和断档后由偏离样本在窗口里的占比决定，审查 000595 P3）；样本到期离开窗口；50 ms 一个 tick 时每秒只取一个样本；样本比值。
  - `snapshot`：节点落后但盘口新鲜时照样采样，到 120 个样本时就绪的本地腿带上基差，比值等于 100,002 ÷ 100,004 − 1；盘口落后、不可信、一边没价、没有永续报价、feeds 停止时都不采样。
  - `prices_view`：就绪和不可用两种帧形状；撤回的本地腿没有这一项。
  - 变异检查 7 项，各有测试失败：中位数换成平均数、不剔除旧样本、每个 tick 都采样、比值写反、取下中位数、采样不看盘口新鲜度、采样受节点状态约束。
- **复现**：在独立 OB 仓库根目录运行以下命令；禁止 `cargo fmt`。

  ```sh
  cargo test -p server --lib -- local_marks
  ```

## 2026-10-30 清算候选重算: 未变的候选不再复制盘口估计

- **起因**（bm 10-30 15:42，f9d7a16）：账户线程 `accounts-owner` CPU 约 69%，强平候选刷新占用约 62% 时间（`orderbook_liquidation_candidates_refresh_seconds` 每秒 0.62 秒）。帧里的持仓和节点 mark 比盘口晚，中位 147 ms，p90 398 ms。
- **perf**（30 秒，499 Hz，10,264 个样本）自身耗时：
  - `reevaluate` 19.2%（含内联的 `evaluate`）；
  - `reindex` 12.7%；
  - 按地址的候选哈希表约 11%；
  - **`WalkShare::clone` 9.4%**；
  - `refresh_candidates` 5.8%；
  - 地址排序约 4%。
- **原因**：`evaluate` 对 mark 动了的币，把盘口附近的每个候选都重新建一遍，并复制已发布候选的盘口估计（`old.walk.clone()`，带最多 100 档明细）。随后多数候选比较下来没变，新建的连同复制的估计一起丢弃。
- **改动**（`accounts/candidates.rs`）：
  - 本轮候选先用进入时的估计（`NoBook` / `Untrusted` / `Pending`，不分配内存），收进按地址排序的列表，替代原来的哈希表加排序。
  - 未变的已发布候选原样保留；变了的，把已发布候选的估计移进新候选（`mem::replace`），不复制。
  - `differs` 不再比较估计：估计总是从已发布候选照搬，只经 `apply_walk` 和 `walk_book` 改变。
  - 输出的 enter / update / exit 内容和顺序不变。
- **测试**：新增 `a_reevaluated_candidate_keeps_its_published_walk`：
  - 估计完成后，重读不变 → `Unchanged`，估计不变；
  - 下一块 mark 上调 1% → update 带原估计，不回 pending，不重新请求。
  - 变异检查：估计不移交、`differs` 恢复比较估计、退出判定失效，三项都有测试失败。
- **预期**（推算，上线后 perf 复测）：去掉约 9% 的复制，以及部分哈希表操作。`reindex` 和多币账户重算（约 13%），以及每次刷新都重排全部扫单顺序（`sequences`），本批未动。

## 2026-10-31 影子盘口批次 1：监听线程盘口的块对齐副本（设计 hyperliquid-trade 000604 r1–r5，用户 10-31 定）

- **起因**：用户要消费 bm 内存池，比现在更早知道 HL 的 BBO、自己的成交和全市场成交。实测内存池里的动作，比节点写出它所在那一块早，中位 222 ms（hyperliquid-trade `analyze/research/mempool_pending_book_20261031/`）。影子盘口要在一个确定的块末盘口上叠加待入块的动作，本批先做这个底；内存池、叠加层和 `extPendingBbo` / `extPendingTrades` 频道放到批次 2。
- **参数**：`--pending-book`，默认关闭；不能和 `--no-resync`、`--bbo-only` 同开。`--pending-backlog-events`，默认 200,000。
- **监听线程一侧**（`listeners/order_book/replica.rs` 的 `ReplicaFeed`）：
  - pending 线程要副本、交付位空着、两条流相差不超过 2 块，监听线程才在下一个盘口批次应用前（持锁）调用 `OrderBookState::fork()`。复制的是盘口、两份配对缓存、两条流的进度，不复制条件单表；放进单独的一格交付位。
  - 之后每个成功应用的状态批次、增量批次，克隆后 `try_send`。
  - 队列满、盘口不可信，或撤回期数变了，就断开。断开之后要等 pending 线程再要，才会重新复制（修订 6、10）。
- **pending 线程一侧**（`BookReplica`，在新线程 `pending-book` 上）：
  - 一块要等两条流都出现更高的高度才算完整；完整后先应用这一块的状态，再应用增量。
  - 复制之后，要等两条流都越过复制时较快那条流的高度，副本才算站在块末（修订 1）。
  - 等慢流的积压按事件计预算，空批次算一个（修订 6、r3 P2-1）。
  - 应用出错（契约违规）、清理时清掉了可能有效的配对缓存，都断开并重新要副本。
- **不重复计数**：`OrderBookState` 新增 `reports` 标志，副本为 false。挂单锚点、找不到目标、孤儿清理这些进程级指标和日志，只由监听线程那份上报。
- **指标**：`orderbook_pending_book_fork_seconds`、`_forks_total`、`_breaks_total{cause}`、`_confirmed_height`、`_alignment_blocks`、`_backlog_events`。
- **成本**：复制 52.4 万张挂单、1,238 个币，release 版本地实测 0.20–0.22 s。这期间监听线程不应用任何批次；只在启动和断开之后发生。每个盘口事件多一次克隆和一次 `try_send`，只在喂副本时才有。
- **验证**：
  - `cargo test -p server --lib` 全部 1,144 个通过；另有一个按实盘规模计时的 ignore 测试。
  - 新增测试覆盖：同一块分两行写、增量流的下一块先到；复制时某块只应用了一半；复制时两条流相差两块；积压超出预算（含空批次）；应用出错；只在被要求、交付位空、两流相近时才复制；交付位被占时不复制；队列满后断开，要再要一次才复制；盘口不可信或撤回期数变化时断开；消费方不在时停止；复制出来的是一份独立的拷贝；监听线程端到端喂副本并在不同步时断开；过期的副本作废；监听线程断开时丢弃副本；副本超预算时断开并让监听线程换新代次。
  - 变异检查 6 处：改成任一条流越过就应用、去掉两流相差检查、队列满不断开、不等对齐、接收已作废的副本、忽略撤回期数变化。每处都有测试失败，检查完已还原。
- **审查 000606 r1 之后的修改**：
  - **代次由 pending 线程决定**：pending 线程说要第几代，不是这一代的副本或批次一律丢掉。批次比副本先到时，先从交付位取出副本装上，再应用这个批次（P2-2：两条通道之间不保证先后）。
  - **只往空队列里复制**：监听线程对每个被要的代次只复制一次，而且要等旧代次的积压全部取走、交付位空着才复制（P2-3）。
  - **盘口一不可信就结束**：盘口变得不可信的那一刻（`publish_book_trust`），或者盘口被替换时（`finish_install`），当场结束正在喂的代次，不等下一个批次（P2-4）。
  - **排队位置无法证明的订单**：因为插队锚点不在而排到队尾的订单，在 `OrderBookState` 里一直记着，直到离开盘口；复制时带上，副本上报其数量。价格和数量不受影响，批次 2 不在这些订单所在的价位归属成交方（P2-1）。bm 10-31：15.5 小时 21 次，对比 978 万次正常插队。
  - **新增测试**：审查给的同价队列例子；首个批次先于副本到达；旧积压未取完时不复制；盘口不可信时当场结束；不是这一代的副本和批次被丢掉；副本超出预算后换代次。
  - **变异检查 12 处**，每处都有测试失败。其中一处原先被交付位占用掩盖，修正测试后能抓到。
  - 1,146 个测试通过。
- **审查 000606 r2 之后的修改：按价位标记，不再按单张订单**：
  - 只标记插队失败的那张单不够：后来以它为锚点插进来的单会跟着排错，等它撤走，错误就留了下来。另一种情况是，一张新单在等状态时，同价位上又有后来的单挂了上去（排在队尾，或插在同一个锚点前），它落位时就被越过了，而这种情况全程没有插队失败可以标记。
  - 现在 `OrderBookState` 的 `UnprovenLevels` 按价位记录：插队锚点不在（fallback），或者一个等状态的新增记录在等待期间同价位有过新挂单（不管有没有锚点），这个价位就标为排队顺序不可证明，直到它清空。判断“等待期间有过新挂单”的办法是：记一个全局挂单计数，再记每个价位最后一次挂单时的计数；新增记录入缓存时记下当时的计数，落位时两者比较。没有等待中的新增记录还需要的计数，在清理时删掉。
  - 三处落位代码原来各写一遍，合并成了 `rest()`。
  - 只在打开 `--pending-book` 时跟踪：每次装快照建立盘口状态时打开，不打开的话这部分代码完全不起作用。复制时一并带过去，副本上报为 `orderbook_pending_book_unproven_levels`。
  - **新增测试**：审查给的两个例子（插队失败之后，又有单以它为锚点插进来；等状态的新单被 HIP-2 系统单越过）、同一锚点前被越过、没被越过时不标记、清理挂单计数的边界。
  - **变异检查 6 处**，都有测试失败：不检测越过、插队失败不标记、价位清空后不撤标记、不记挂单、过早清掉挂单计数、带锚点的不算越过（这一处暴露了原先规则的漏洞，已修正）。
  - 1,148 个测试通过。
- **审查 000606 r3 通过**；按 P3 顺手改了一处：清理时求“最早的等待计数”只在开启跟踪时做，关闭 `--pending-book` 时不多扫等待表。
- **bm 上线（10-31 08:30，用户定：周末行情淡，直接在现有实例上开）**：复制 1 次，用时 0.114 s，副本只落后 1 块，没有断开过；排队顺序不可证明的价位，启动后从 21 个降到 9 个。perf 显示监听线程处理增量批次的均值从约 1 µs 涨到约 24 µs，原因是每个增量批次结束时都把所有被标记的价位逐个查一遍：按币名在盘口里找要做字符串比较（`memcmp` 占这个函数的 27%），还要按价格找价位。改为：标记按币存放，只在有单撤掉或改量到 0 时检查这个币自己的标记；穿价成交导致价位清空的，在清理时统一处理。新增清理的测试；两条撤标记路径各做了一次变异检查，都有测试失败。审查 000607 r1 指出：价位被穿价成交吃空后，在清理到来之前同价又挂上新单，新单会继承旧标记。已改为新单落位前先看：价位带标记而盘口上已经空了，就先清掉旧标记，再按这张单自己的情况判断（P2，新增测试，做了变异检查）；测试里的 `drop(bool)` 改成断言（P3）。
- **上线方式**：审查原先建议先在隔离实例上量。用户 10-31 决定直接在现有的 bm 实例上打开（周末行情淡），等有明确证据表明它严重干扰正常运行、又难以修复，再另想办法。要盯的指标：复制频率、恢复耗时、原 BBO / L2 的发布延迟、监听线程处理批次的耗时。

## 2026-10-31 影子盘口批次 2a：读内存池（设计 hyperliquid-trade 000604 第 2 节、r2 修订 2–4、r3 修订 7，用户 10-31 “开工吧”）

- **做什么**：打开 `--pending-book` 时，在 pending 线程上跟读节点内存池 `<data-dir>/mempool_txs/hourly/<YYYYMMDD>/<H>`，解析动作，把资产编号换成币名。这一批只计数、出指标，不叠加、不发帧；叠加层是批次 2b，频道是 2c。
- **实测线格式**（bm 10-31 09:10–09:14，抽 200 MB）：
  - 每行 `[local_time, {tx_hash, signed_actions}]`；
  - 每秒约 26 行，每行约 140 个动作，中位 105 KB，最长 1.6 MB（设计稿写的 131 KB 偏小）；
  - `signed_actions[]` 的字段是 `action`、`nonce`、`signature`，可选 `vaultAddress`、`expiresAfter`、`isFrontend`；
  - `cancel` / `cancelByCloid` 多数带 `f: true`，即 fast 撤单标志。用户 10-31 确认它已经生效，作用是撤单优先；官方 API 文档仍写着“将来升级后才在内存池里优先”，是文档滞后。解析为 `fast`，供 2b 的排序和对比录制使用；
  - `batchModify` 的 `oid` 有 11% 是 cloid 字符串；
  - 优先费 `grouping: {"p": N}` 也出现在纯 ALO 上，不读。
- **一份线格式**：内存池的 signed action 和 `replica_cmds` 的是同一协议，所以：
  - 复用 `accounts::replica::Action`，加了动作级的 `a`（`always_place`）；
  - `WireOrder`、`WireModify`、`OrderRef`、`WireCancel`、`WireCancelByCloid` 从 `marks/actions.rs` 挪进 `replica.rs`，供两边共用；节点标记只改了引用路径；
  - `WireCancel` 和 `WireCancelByCloid` 补上资产编号（`a`、`asset`），这两个字段现在是必需的。实测 116,399 个撤单和 212,536 个按 cloid 撤单，全都带。
  - `WireOrderType` 补上 `trigger`：订单类型必须恰好是限价或触发之一，两个都有或都没有就算读不出。
- **跟读**：
  - 复用 `parallel.rs` 的逐小时跟读和换文件，新增 `LineShape::Array`，只接受 `[…]` 行；节点其他流仍是 `{…}`。读文件滞后（`NODE_LINE_LAG` 的 `read`）也能从数组行的前缀取到时间。
  - 从当前文件尾开始，启动时跳过的部分记为一次丢失（`positioning`）。
  - 单行上限 64 MB；队列 1,024 行。
- **背压，不丢行**：pending 线程跟不上时，跟读线程停在原处等它，不丢行。这与设计第 2 节的“通道满就丢行”不同。
  - 理由：行在通道里不会丢，只会晚。晚到的动作在 2b 的入口按当前 C 先过期（r5 修订 13），不会被当成新动作叠加。所以“丢行”这一类缺口不再出现，跟不上的程度由 `NODE_LINE_LAG` 的 `apply` 量出。
  - 缺口事件随数据走同一条通道；另有一个丢失计数，见下面“读文件层的丢失”（审查 000609 r1 P2-1）。
- **读不出的行**：一行里任何一个动作读不出，整行记为不可读，按它在文件里的位置发出（设计第 2 节）。不可读的情形包括：未知的 tif、类型既非限价也非触发、价格或数量不是十进制数、缺字段、`a` 不是布尔值。不认识的动作类型只计数。
- **读文件层的丢失**（截断、超长、不是 `[…]`、非 UTF-8、文件被替换或消失）：
  - 问题：这类丢失由读文件层置标志，丢失事件要在同一次读出的其余行之后才送出。所以排在丢失行后面的行（例如一笔撤单）会先到 pending 线程，那时它还以为输入是完整的（审查 000609 r1 P2-1）。
  - 修法：读文件层一发现丢失，当场把这条流的丢失计数加一（`Losses::flag`，内存池这条流才带计数，`LineStream::loss_count`）。这一步在同一次读出的任何行送出之前。丢失事件送出时，带上当时的计数 `through`。
  - pending 线程：计数大于最近一次丢失事件的 `through`，就说明有丢失还没报到（`MempoolIngest::loss_unreported`）。这期间取到的行记为 `read_after_unreported_loss`；2b 在这期间不把影子当作完整。
  - 跟读线程被背压卡住时，计数照样当场可见，不用等丢失事件排到。这就是设计 r3 修订 7 的“独立可见的缺口代次”。
  - 其他流不带计数，行为不变。
  - 解析失败的行同样当场计数（审查 000609 r2 P2-1）：跟读线程解析出错时，在把 `Unreadable` 送进可能已满的队列之前，就把“读不出的行”计数加一，`Unreadable` 带上自己的序号 `through`。两种丢失各用各的计数、各由自己的事件确认（`MempoolLosses`），所以收到一条坏行的报告，不会顺带把排在它前面、还没报到的文件层丢失也当成已报告。
- **文件通知有界**（审查 000609 r1、r2 的 P2-2）：
  - 问题：跟读线程阻塞在送行上时，inotify 回调仍往无界队列里塞通知，每次写入一条（含 Access）。
  - 修法（`NoticeGate`）：回调只放行跟读线程会处理的通知，即数据文件的新建和修改（判断和 `FileReader::notified_file` 共用 `stream_file`），以及内核队列溢出；Access、删除、文件夹、其他路径的通知直接丢掉。修改通知同时最多排一条：跟读线程取出它、开始读之前清掉标记，之后的写入会重新通知；读一次会把已写的内容都读走（每次最多 8 MB，没读完的由 1 ms 一次的轮询接着读），所以合并不丢数据。错误同时最多排一条，取出时日志里带上它排队期间被丢掉的条数。新建通知照原样每个文件一条。
  - 这一处改在共用的跟读线程里，节点各条流都受影响：平时队列很少积压，行为不变；积压时少了重复唤醒和无用通知。
- **内核通知溢出按丢失报**（审查 000609 r3）：notify 把 inotify 的 `Q_OVERFLOW` 报成 `Other` 加 `Rescan` 标记、不带路径，原来被直接忽略。溢出时可能丢了新建通知，定期检查按修改时间直接换到最新文件，会把中间的文件跳过。现在通知回调收到它（同时最多排一条）的那一刻就记进这条流的“已发现”丢失数（审查 000609 r6：跟读线程可能正卡在送行上，它之后写入的行会先到 pending 线程），跟读线程取出时才记进“已报告”数并报这次丢失（`Losses::flag_found`）。每份 `Lost` 只确认到它发出时“已报告”的数，不确认只是“已发现”、还在排队的溢出（`LossCounts { found, flagged }`，审查 000609 r7：一条坏行的报告曾顺带确认了排在它后面的溢出）。走各条流已有的丢失处理：盘口重新同步，账户等覆盖它的锚点，影子盘口按缺口处理。换文件和读完证明的逻辑不变。
- **换文件的逻辑不改**：r3–r5 一度把换文件改成按文件顺序逐个接、并让读完证明也看文件顺序，审查接连在新逻辑里找出回归（r4 P1：旧小时迟写的最后一行让读取器停在旧文件末尾、照样出证明，账户可能缺着新小时的成交放行一个块）和新问题（r5）。这些场景都是审查在本地构造的，线上没有观察到，原实现一直正常。按“没有证据的未来风险只记录触发条件，不预铺机制”，已全部退回原实现（用户 10-31 定）。原实现已知的边界，触发条件如下，线上出现再处理：
  - 节点先写新小时文件的第一行、再写旧小时文件的最后一行，且之后一段时间两个文件都不再写：定期检查按修改时间会把读取器送回旧文件、从头重读（重复的行）；在那之前读取器在新文件上不出读完证明。
  - 内核通知溢出：现在按丢失报，不会静默跳过。
- **资产编号 → 币名**：
  - 账户线程每装一个新锚点，经 `watch` 发一次 `ActionCoins`：各 dex 永续的动作编号，加上现货对（10000 + 对号），币名与节点各流一致（`PURR/USDC`、`@N`、`xyz:TSLA`）。
  - 表里没有的编号，或者还没有锚点时，计入 `unnamed_assets`。因此 `--pending-book` 需要 `--accounts`，启动时校验。
  - 资产表在装锚点时构造一次，由 `AccountBook` 持有；发布时只比较锚点高度，并复制那个 `Arc`。原先 `track` 每一步都先建整张表、再比较高度，没开 `--pending-book` 的账户线程也要付这笔开销（审查 000609 r1 P2-3）。
- **启动顺序**：副本的喂入仍在监听线程包进锁之前挂上；pending 线程改到账户视图启动之后才起。
- **交易身份**：`tx_hash` 在入口按 32 字节哈希解析，空串、长度不对、不是十六进制，都使整行不可读（审查 000609 r1 P2-4）。
- **标志位的 `null`**：`f`、`a` 缺省才算 false；写了 `null` 就按读不出处理（`replica::present` 保留显式 `null`，审查 000609 r2 P3）。
- **指标**：`orderbook_pending_book_mempool_lines_total{outcome}`（`read`、`read_after_unreported_loss`、`unreadable`）、`_losses_total{cause}`、`_actions_total{kind}`、`_orders_total{tif}`、`_unnamed_assets_total{reason}`；`orderbook_node_line_lag_seconds{stage="read"|"apply",source="mempool"}`。
- **成本**：bm 一段 60 MB 的真实切片（766 行、112,863 个动作）全部读出，没有不可读的行。本地 release 版解析约 457 MB/s，线上写入约 2.6 MB/s。
- **验证**：
  - `cargo test -p server --lib` 全部 1,160 个通过，13 个忽略。
  - 新增测试：一行里每类动作逐字段对照（取自 bm 的真实动作，含带与不带 `f` 的撤单）；八种读不出的动作，以及时间、tx_hash（缺失、空、太短、非十六进制）、行形状错误；修改通知一次只排一条；资产命名；跟读线程端到端（启动跳过、不可读行按位置发出、`{…}` 行被丢弃）；数组行的时间前缀；锚点的资产表（永续、HIP-3、现货对）；每个锚点只发一次。
  - 变异检查 12 处，每处都有测试失败：非限价非触发当触发、未知 tif 当 GTC、忽略 `always_place`、数组形状什么都收、没锚点也算命名了、启动跳过不算丢失、数组行按对象前缀取时间、丢掉 `vaultAddress`、改单目标取错、每步都发资产表、漏掉现货对、`track` 不发资产表。检查完都已还原。
  - 审查 000609 r1 之后又做 8 处，也都有测试失败：发现丢失不计数、丢失事件的 `through` 恒为 0、未报判断用 `>=`、收到丢失事件不记 `through`、每条修改通知都入队、取出后不清标记、发布时重建资产表、忽略 `f`。测试抓不到的一处：跟读循环取到修改通知后漏调 `modify_taken()`，此后的修改通知全被合并，只能靠 1 ms 的轮询读，数据不丢但唤醒慢；收到溢出通知后的报丢失同样只在跟读循环里，没有测试。
  - 审查 000609 r2 之后又做 7 处，也都有测试失败：坏行不计数、一条报告清掉两种计数、放行跟读线程不处理的通知、每条错误都入队、取出错误后不清标记、`f` 的 `null` 当缺省、`a` 的 `null` 当缺省。新增测试：坏行在送出前已计数且只由自己的报告确认；跟读线程端到端里坏行的报告不清掉排在前面的文件层丢失；通知闸门的单元测试（每种通知各一条、新建每个一条、溢出合并），以及真实 inotify 下 100 次打开、追加、关闭加一个新文件只排一条修改、一条新建。
  - clippy：新增的行上只有指标的 `expect`，与现有写法一致。没有运行 `cargo fmt`。
- **审查 000609 r1–r8**，r8 通过。本批没有部署，和 2b（叠加层）、2c（频道）一起上线。

## 2026-10-31 影子盘口批次 2b：待入块集合和叠加层，按区块哈希对账（设计 hyperliquid-trade 000604 第 3 节、r2 修订 4、r3 修订 8、r4 修订 12、r6 修订 14–17、r7 修订 18–20，用户 10-31 “继续吧”“好的”）

- **做什么**：打开 `--pending-book` 时，pending 线程把内存池每一行的盘口动作放进待入块集合，按币叠到副本上，算出预计的买一卖一和预计成交；一行进了副本已确认的块，就按它的哈希整行移出。这一批还不对外发：每一步的结果（预计成交、变动的币）交给 2c 的频道，现在只出指标。
- **为什么按哈希**（设计 r6）：设计 r1 写“不依赖 `replica_cmds`（节点从 10-25 起就没再写）”，这个前提是错的。之前的实现因此用订单状态去猜一笔动作是否已入块：按 cloid、按下单要素认领、同键多条记歧义、按订单时间戳认下单。审查 000611 r1、r2 的 P1/P2 大多出在这一层。研究（hyperliquid-trade a284e87a，节点升级后的 1 万块）：一行内存池就是一个 bundle，`tx_hash` 就是 bundle 哈希，18,939/18,939 对上；迟到的行 p99 晚 2.3 s、最大 2.34 s，晚 1 s 以上的 63 行带 17,738 个动作，按旧设计的 H = 1 s 都会被重复叠加；`replica_cmds` 的块行总在副本能确认这一块之前读到（中位早 60 ms）。
- **区块哈希流**（`pending/blocks.rs`，修订 14）：
  - pending 线程自己跟读 `replica_cmds`（账户视图整行解析，另读一份），从当前文件尾开始；每行只读块高、块时间和各 bundle 的哈希，主网约 270 KB 的一行 release 下 0.12 ms。
  - 跟读、丢失计数、背压与内存池共用一套（`pending/feed.rs`：2a 的 `MempoolEvent`/`MempoolLosses` 改成通用的 `FeedEvent<T>`/`FeedLosses`/`FeedReports`，内存池只保留自己的解析和指标）。
  - 块高记录（`pending/record.rs`）：哈希 → 块高，按块高连续；副本确认时间之前保留 R = H + E，领先副本最多 20,000 块（修订 20），到上限 pending 线程暂停取块，跟读线程在满队列上等。
- **副本**（`listeners/order_book/replica.rs`）：只报告确认高度和变动的币；对齐后维护按 cloid 找挂单的索引（找撤单、改单目标用；第一次对齐时从盘口建立，之后随状态增删，查的时候只认副本里还在的单；主网规模建一次 165 ms，在 pending 线程上）；叠加层读副本用的只读视图 `CoinBase`。为此给 `OrderBook` 加了 `levels_from_best`、`level`、`order`，`PriceLevel` 加了 `orders`、`order`，`LinkedList` 加了 `iter`、`node_value`，`OrderBookState` 加了 `unproven_in`、`books`，`Side` 加了 `opposite`。
- **待入块集合**（`pending/actions.rs`、`pending/shadow.rs`）：
  - 执行顺序：行与行按读到的先后；行内先跑新单全是 ALO 的动作，再跑撤单，最后跑其余，各类内部按排列先后。规则和节点标记共用一处（`accounts::replica::placing_category`）。fast 撤单在 bundle 内和普通撤单没有区别，不参与排序。触发单、新单是触发单的改单不进。
  - 到达：已在集合里的行丢弃；按确认块时间判过期；哈希所在的块已被副本确认，整行跳过（迟到）；会超上限，就在作用之前整体丢弃；其余放进集合，当场作用到叠加层，这一次的成交就是预计成交，只在可用时发出。哈希所在的块日志已出现、副本还没确认的，照常持有，等副本走到再移出。
  - 结算（修订 18）：副本确认高度推进、新代次对齐、哈希块到达，三种时刻走同一步：块高 ≤ 确认高度的哈希对应的行、过期的行，整行移出；有动作移出的币、盘口变了的币（新代次时全部币）从副本重放；之后才算可用。重放不发成交。
- **叠加层**（`pending/overlay.rs`，每币一份）：副本上的单被吃掉或撤掉多少、待入块的挂单；同一价位副本的单在前、待入块的在后。ALO 穿价预计被拒；GTC 先吃再挂；IOC 和 `FrontendMarket` 只吃。撤单和改单的目标按叠加后的状态找（按 oid 只查副本；按 cloid 先查待入块、再查副本，不止一张活着的就是歧义、不动）。改单按修订 8 的准入表，不带 `always_place` 的 GTC 改单排除目标后判断是否穿价。排队顺序不可证明的价位，吃到副本挂单的部分合成一条不带挂单方的成交，待入块挂单照常归属（它排在这一档所有副本挂单之后）。不模拟自成交、保证金、只减仓和价格带。
- **缺口**（修订 16、19）：
  - 内存池（丢失、未报告丢失之后的行、没有资产表、静默、超上限）：只是少了行，已有动作照常叠加；缺口期间不可用，由之后第一条新鲜行（不早于确认块时间减 S）结束，确认块时间过了它加 E 才恢复。
  - 副本（未对齐、换代）：不可用；新代次对齐后按哈希结算即可恢复，不预热。
  - 区块日志（启动、丢失、块高不连续）：缺口期间到达的、打开时已持有的动作一律未知（不叠）；由之后的第一块结束，W = max(W, `T_end` + H, 当时最新一行)，W + E 后恢复。日志落后于副本确认高度时也不可用。
  - pending 线程处理每个输入之前，先接收监听线程已结束的代次和两条跟读已发现、尚未报告的丢失（`PendingBook::sync`）。
- **参数**：`--pending-expiry-ms` 1000（E）、`--pending-max-late-ms` 5000（H，实测最大迟到 2.34 s）、`--pending-max-span-ms` 10000、`--pending-max-actions` 500000、`--pending-mempool-silence-ms` 2000（S），启动时都要求为正。
- **指标**：`orderbook_pending_book_actions_total{op,outcome}`、`orderbook_pending_book_overlay_total{op,outcome}`、`orderbook_pending_book_predicted_fills_total{maker}`、`orderbook_pending_book_held_actions{state}`、`orderbook_pending_book_gaps_total{cause}`、`orderbook_pending_book_span_seconds`、`orderbook_pending_book_moved_coins`、`orderbook_pending_book_unavailable_seconds_total{reason}`、`orderbook_pending_book_step_seconds{step}`、`orderbook_pending_book_cloid_index_orders`、`orderbook_pending_book_block_log_lines_total{outcome}`、`orderbook_pending_book_block_log_losses_total{cause}`、`orderbook_pending_book_block_log_lead_blocks`。新指标放在单独的 `lazy_static!` 块里（原块的宏展开到了递归上限）。
- **测试**：
  - 集合（设计 r6、r7 的验收）：吃单只发一次、入块后整行移出；晚 2.25 s 的行（块已确认）整行跳过；块在日志里、副本未到的行先持有再移出；日志晚于副本时不可用，哈希块到达即结算、首帧买档为 1；重投在集合中丢弃、确认后跳过；副本换代后按哈希移出、不预热、其他币重放回来；日志缺口的 W 覆盖最新一行（r7 的 15,900 → 16,900 时间线）；日志块高不连续即缺口；内存池缺口保留已知动作；旧行不结束静默；超上限那行不出成交；没入块的行到期移出；副本变了要重放；日志领先上限。
  - 叠加层、执行顺序、cloid 索引、两条跟读的解析与丢失、线程层的同步规则各有用例。`cargo test -p server --lib` 全部通过；`cargo clippy --all-targets` 对本批新代码无告警。没有运行 `cargo fmt`。
  - **变异 22 处**，都有测试失败：迟到和结算的块高边界各一、哈希块到达不结算、新代次不重放全部币、日志缺口不让动作变未知、W 不含最新一行、内存池缺口误清动作、记录只保留 E、日志不受领先上限、日志落后仍可用、旧行结束缺口、作用后才判上限、不判重投、块高不连续不算缺口、结算不过期、不同步日志的已发现丢失、日志丢失不算缺口、记录不重起，以及叠加层、执行顺序、cloid 索引各一处。还原用 `copyfile` 并 `touch`（cargo 按修改时间判断要不要重编）。
- **过程**：
  - 第一版按订单状态猜入块，审查 000611 r1（2 个 P1、4 个 P2、1 个 P3）、r2（1 个 P1、1 个 P2）都修过；r2 的两条是修 r1 时引入的（去重只记有动作留下的行，而“认领”本身改了状态；容量按原始动作数判）。用户 10-31 问“我们不是实现过解析区块信息吗”，查明是设计的错误前提，改按哈希：设计 000604 r6（需修改：哈希追平时要结算、日志缺口的 W 要覆盖最新一行、领先记录无界）、r7 通过。猜的那一层（订单状态的下单撤单事实、认领、歧义、时间戳规则、单独的去重表、按 oid 撤单目标已不在就提前移出）整体删掉。
  - r1 修过、新方案里仍保留的：先对账后淘汰的顺序问题不再存在（按块高结算）；输入前同步两类信号；只由新鲜行结束内存池缺口；先判容量再作用；cloid 索引在第一次对齐时建；换方向的被动 GTC 改单按排除目标判断；小写 `rejected` 已无用（不再读订单状态）。
- **审查 000611 r3**（需修改，1 个 P1、2 个 P2，全部核实成立；r2 的两个反例确认已关闭）：
  - P1：记录被淘汰清空后，“是否连续”拿不到最后读到的高度，日志跳块被当成连续，缺口里迟到的行会在新挂单上虚成交。改为单独记下最后读到的高度，淘汰不动它（`BlockRecord::last_height`）；“日志落后”也按它判断。
  - P2：现货开关原先看当前副本，副本缺席时放行了现货；改为启动配置（`ServerConfig::include_spot`，`--markets spot` 或 `all` 时为真）直接交给 pending owner。
  - P2：同一确认高度的批次也会触发结算、扫一遍集合；改为副本这一路只在确认高度变化（含第一次对齐）时结算，哈希块到达照旧单独结算。
  - 新增测试 4 个（全量 1,206 个通过）：淘汰清空后跳块仍是缺口（记录层和集合层各一）、无副本时不收现货、只入队的批次不结算；三处修复逐一回退，都有测试失败。
- **审查 000611 r4 通过**；按 P3 改了文档和两条注释里误写的 `--include-spot`（实际参数是 `--markets`）。
- 本批没有部署，和 2a、2c 一起上线。

## 2026-10-31 影子盘口批次 2c：`extPendingBbo` / `extPendingTrades` 频道（设计 hyperliquid-trade 000604 第 4 节、r3 修订 9，用户 10-31 “开始吧”）

- **做什么**：把 2b 算出的预计 BBO 和预计成交通过两个新订阅发出去。频道名、`ext` 前缀、未知参数拒绝订阅，都按设计。
- **pending 线程这边**（`pending/publish.rs`）：
  - 每处理完一个输入（副本批次、内存池行、日志块、定时 tick），发布器对这一步变动的币（新代次时全部币）算出帧：合并后的最优档 `bbo`、副本自己的最优档 `basisBbo`、基准高度和块时间、这个币持有的动作数和最早/最新见到时间。
  - 每个币最新的帧放进共享存储，订阅时首帧从这里取。广播只发 `bbo` 或 `basisBbo` 变了的帧，并且只在可用时发。
  - 顺序：存储先写帧、再写可用状态（读到“可用”的一定能读到帧）；广播先发可用状态变化、再发帧、最后发这一步的预计成交。
  - 每个帧带发布器的序号，连接用它跳过排在队里、比已发过的更旧的帧。
  - 存储里一个币的帧只在它变动时重算，所以取出时盖上最新的基准（之后没变过，最优档在新基准下仍成立）。没有帧的币表示两边都没有挂单、也没有待入块动作。
  - 每个持有的动作（含未知）都把它的币记为变动，帧里的动作计数才保持最新；只有最优档变了才广播。
  - pending 线程结束或崩溃时，守着它的任务把可用状态置为 `stopped`（序号取最大值）并广播，所有订阅都会收到。
- **连接这边**（`servers/websocket_server/pending_book.rs`，照 `extMarkPrices` 的会话写法）：
  - `extPendingBbo { coin }`：币须在盘口的币种范围内；订阅即发当前帧（不可用时发原因），之后发这个币变了的帧。
  - `extPendingTrades { coin?, user? }`：`user` 为挂单方，或吃单带的 `vaultAddress`；订阅即发当前状态。
  - 可用状态变化时，每个订阅告知一次：不可用发 `{"coin"?, "status": "unavailable", "reason"}`，恢复时 BBO 发恢复那一步提交的帧、成交频道发 `{"status": "ready"}`。
  - 连接只在有订阅时接收广播；跟不上广播（4,096 个事件）就撤销这些订阅并报错，客户端重订。
- **与设计稿的两处格式差异**：
  - 成交频道的 `data` 是对象 `{status, basisHeight, basisTimeMs, trades: [...]}`，不是设计稿示例里的数组，这样状态帧和成交帧同一种形状。
  - 吃单和待入块挂单的位置用 `action`（行内第几个签名动作）和 `item`（动作内第几张单），替代设计稿的 `index`；不可证明价位的成交 `maker` 为 `{"unattributed": true}`（设计稿之后才有这一类）。
- **指标**：`orderbook_pending_book_frames_total{kind}`（`bbo`、`trades`、`availability`）。
- **测试**：
  - 发布：线的到来先发帧（被吃的卖一、旁边的确认卖一）再发成交，不动最优档的行不发；不可用时不发帧、存储照写；新代次刷新全部币（新分叉里 BTC 卖单没了，没有任何块动过 BTC）；可用状态变化先写存储再广播。
  - 连接：首帧和之后更新的帧、跳过更旧的帧、别的币不发；不可用和恢复各告知一次、恢复时发当前帧且不重复；不可用时订阅收到原因；成交按币、按挂单方、按金库过滤，退订后不再接收广播；成交的线格式（三种挂单方）；跟不上广播撤销订阅、线程结束告知 `stopped`；未知参数拒绝订阅。
  - `cargo test -p server --lib` 1,217 个通过；`cargo clippy --all-targets` 对本批新代码无告警。没有运行 `cargo fmt`。
  - **变异 10 处**，都有测试失败：不可用也发帧、不去重、新代次不刷新全部币、存储帧不盖最新基准、连接发更旧的帧、可用状态告知两次、用户过滤不看挂单方、无订阅仍接收广播、跟不上广播不撤销、未知动作不记变动。
- 本批没有部署，和 2a、2b 一起上线。
- **审查 000614 r1**（需修改，3 个 P1、1 个 P2，全部核实成立）：
  - P1：超上限清空集合后，受影响的币没有重算帧，存储里的旧帧还被盖上新基准，恢复后首帧带着已删掉的挂单。日志缺口打开时清空叠加层也有同样漏洞。改为整体清空或重建叠加层时（超上限、日志缺口、副本缺席、新代次）挂“全部重算”标记，副本对齐后的下一次发布时用掉。
  - P1：帧和可用状态分两次写入存储，两次之间来的订阅会读到“新帧 + 旧的可用”。改为一步里的基准、帧、可用状态和序号在一把写锁内一次提交，提交后再广播。
  - P1：新订阅和恢复通知会把旧消息当成 `ready` 发出：队列里早于订阅的帧和成交被发出，之后同一次失信通知反被跳过；处理排队的恢复通知时不看存储的实际状态。改为每个订阅记下读到的快照序号和当时是否可用，帧和成交跳过序号不大于它的，不可用时不发数据；收到任何可用状态变化，都按存储当前的快照告知。
  - P2：一条连接上的两个成交订阅都命中同一笔成交时会收到两份；改为每批成交对全部有效过滤条件取并集，每条连接发一次。
  - 新增测试 6 个（全量 1,222 个通过）：超上限清空后恢复首帧等于实时视图；发布后存储序号达到这一步最后的事件；新订阅跳过快照里已有的事件（含排队的帧、成交和失信通知）；排队的恢复通知遇到新的失信，按存储告知；告知不可用的订阅不收数据；重叠过滤只发一次，退订全市场后 BTC 仍到、ETH 不再到。变异 15 处（原 10 处按新写法调整，加本轮 5 处）都有测试失败。“状态和帧分开写”这一处是写锁内外的时序窗口，确定性测试打不开，靠一次写入的结构保证。
- **审查 000614 r2**（需修改，2 个 P1，都在订阅端，核实成立）：
  - P1：已订阅的连接处理可用状态通知时，丢掉通知自带的状态，改读存储，并把序号跳到存储末尾。后果：
    - 排在后面的恢复后第一笔成交被静默跳过；
    - 连续的失信和恢复只告知最后一个。

    改为：新订阅从存储的快照开始；已有订阅按广播顺序推进，每个可用状态变化都按它当时的状态告知。为此：
    - 快照和可用状态通知合成同一个类型 `Snapshot { seq, served }`，`served` 为可用时那一步提交的全部帧，或者不可用的原因；
    - 恢复时 BBO 发的是恢复那一步提交的帧；
    - 存储里的帧表按写时复制共享，之后的提交不会改动已发出的通知。
  - P1：成交频道的状态帧和数据帧都不带订阅标识，但每个过滤条件各记各的状态。后果：新增过滤条件时会先告知 `unavailable`，随后旧过滤条件又放出排队的成交。

    改为一条连接只有一个成交频道（`TradesChannel`），统一维护一个状态和一个交付顺序：
    - 第一个过滤条件从存储开始；
    - 之后的过滤条件从频道当前的位置加入，并被告知频道此刻的状态；
    - 同一笔成交仍按过滤条件的并集只发一次。
  - 新增测试 4 个（全量 1,225 个通过）：
    - 恢复通知带着它那一步的盘口，存储之后变化也不受影响（发布端）；
    - 晚消费时“失信 → 恢复 → 再失信”逐条告知，恢复帧是恢复时的 100，不是后来的 99；
    - 晚消费时“恢复 → 帧 → 成交”都发出，成交不丢；
    - 新增过滤条件从频道当前位置加入：先告知 `ready`，再发排队的成交，再告知失信；频道已告知不可用时，新加入的收到原因。
  - 变异 8 处，都有测试失败：通知按存储告知（BBO、成交各一处）、恢复携带提交前的帧、新过滤条件按存储告知、新过滤条件把频道重置到存储、重复告知同一变化、最后一个过滤条件退订后频道不撤、频道告知不可用后仍收成交。
- **审查 000614 r3**（需修改，1 个 P2，核实成立）：
  - P2：新过滤条件从频道当前位置加入后，按全部过滤条件的并集放行成交。后果：订阅前就已在排队的成交，即便当时没有任何过滤条件命中，也会补发给新条件。比如连接原先只订 ETH，新增 BTC 之后，会收到加入前的那笔 BTC 成交。
  - 改为每个过滤条件记下加入时存储的序号。一笔成交只有被加入早于它的过滤条件命中才发出；共享频道的状态和顺序不变。
  - 新增测试 1 个（全量 1,226 个通过）：原先只订 ETH，排队一笔 BTC 成交和一次失信，此时新增 BTC。新条件先收到 `ready`，之后只收到失信，不补发那笔 BTC 成交。变异“忽略加入序号”被这个测试抓到。
  - 审查另指出：只要有地方持有快照，每次提交都会复制帧表。改为只在这一步有帧变动时才复制。

## 2026-10-31 影子盘口批次 2d：HL 按规则直接拒的单，影子也拒（hyperliquid-trade 研究 `mempool_pending_book_20261031` 的成交逐笔追查，用户 10-31 “按难易重要合理顺序修复”）

- **起因**：批次 3 的对比录制里，预计了但没发生的成交，有一部分是吃单或挂单在进盘口之前就被 HL 按规则拒掉，影子却照常叠加了上去（试录 18:00–18:10，18 个币）。
  - 金额不足：292 笔。
  - 价格或数量不合法、整单被丢：至少 18 个吃单。
  - 过了 `expiresAfter`：少量。
- **价格和数量格式**（`pending/actions.rs`、`pending/mempool.rs`）：
  - 解析内存池时，按原字符串精确记下价格的小数位、有效数字位数，以及数量的小数位（`DecimalShape`），不经过定点数。
  - 生成待入块动作时，对照该资产的格式规则判断：
    - 数量最多 `szDecimals` 位小数；
    - 价格最多 6 − `szDecimals` 位小数（现货为 8 − `szDecimals`），且最多 5 位有效数字，整数价不受有效数字限制；
    - 规则依据 HL API 文档的 Tick and lot size。
  - 不合法的单不进待入块集合，计入 `orderbook_pending_book_actions_total{outcome="off_grid"}`。
  - 依据：bm 10-31 18:01–18:03 带 cloid 的 445,318 张永续单里，价格不合法的 66 张有 65 张没有任何状态；剩下 1 张的 cloid 被别的单用过。
  - 资产表 `ActionCoins` 现在给每个资产带上 `szDecimals` 和价格小数上限（`ActionAsset`）：永续取自锚点，现货取自 `SpotPair`。
- **`expiresAfter`**：解析出来；早于这一行到达 bm 的时间，整个动作不进集合，计入 `outcome="expires_after"`。依据：同一时段到达时已过期的 22 张单里，18 张没有状态。
- **最小下单金额**（`pending/overlay.rs`）：
  - 判断方法：取订单最先成交的价格乘以数量，不足 10（报价币）就拒，计入 `orderbook_pending_book_overlay_total{outcome="min_notional"}`。
    - 最先成交的价格：会立即成交的单取合并盘口对面的最优价，否则取限价。
    - 买单相当于 min(限价, 卖一)，卖单相当于 max(限价, 买一)；ALO 只会挂在限价上，按限价算。
  - 依据：
    - 18:00–18:05 非 ALO 的 8,949 张单，按同一时刻的成交价判断，结果和节点全部一致；
    - 18:01–18:03 的 42.5 万张 ALO 按限价判断，同样全部一致；
    - 只减仓单也按此判断：节点让平掉整个仓位的那一种通过，影子不知道仓位，判不出来。
  - 改单的新单不足最小金额时，整笔改单拒掉、保留原单。这一点是推断，没有直接见到这种情况。
- **测试**（1,230 个通过）：
  - 价格和数量格式：
    - 6 位有效数字、小数位超限、数量小数位超限，三种都拒；
    - 6 位的整数价、数量带尾零，两种都放行；
    - 解析：普通小数、带尾零、科学计数法、负数、只有小数点后的写法。
  - 过期：恰好在到达那一毫秒到期的放行。
  - 最小金额：
    - 吃单按对面最优价、ALO 按限价；
    - 恰好 10 的放行；
    - 改单不足最小金额时保留原单。
  - 变异 5 处，都有测试失败：判格式恒为合法、不判过期、下单不查最小金额、只按限价判、改单不查最小金额。
- **审查 000617 r1**（需修改，2 个 P2，核实成立）：
  - P2：价格不合法时，HL 在预校验阶段拒掉整笔签名动作，同一动作里合法的单也不执行，原实现只丢不合法的那张。
    - 审查给的实例：区块 1204250452，签名动作 230，10 张 BTC ALO 里 9 张价格是 6 位有效数字、1 张是 77679。区块回执只有一条 `Price must be divisible by tick size. asset=0`，10 张都没有状态。我在 18:00–18:20 又找到 4 笔同样的混合动作，合法的单都没有状态。
    - HL 文档的“错误返回”一节也写明：预校验错误对整个请求只返回一个错误，例子里包括“some forms of tick size validation”。
    - 改为：签名动作里任何一张单（含触发单的价格）不合法，整笔动作都不进集合。自身不合法的计 `off_grid`，随整笔被拒的合法单计 `off_grid_batch`。同一行里其他独立的签名动作照常处理。
    - 数量不合法在数据里没见到混合的情况，按文档“由请求内容本身决定的错误”同样整笔拒绝。
    - 测试用了那笔真实动作，再加同一行里另一笔合法动作。
  - P2：ALO 的最小金额应按限价算。原实现对所有单都先看对面最优价，穿价的 ALO 改单会把原单的去留弄反。改为 ALO 一律按限价算。测试用了审查的两个反例：
    - 卖出 0.102 @ 98 的 ALO 改单，按限价 9.996 拒掉，原单保留；
    - 带 `always_place` 买入 0.095 @ 110 的 ALO 改单，按限价 10.45 通过：原单移除，新单穿价被拒。
  - P3：判断现货改用 `Coin::str_is_spot`，省去一次字符串分配。
  - 测试 1,232 个通过；变异 3 处都有测试失败：只按单张判格式、ALO 按对面价算、整笔被拒的原因不区分。

## 2026-10-31 影子盘口批次 2e：金额超过上限的单影子也拒（hyperliquid-trade 研究 `mempool_pending_book_20261031`，1 小时长录的成交逐笔追查）

- **起因**：长录 18:12–19:12 里，预计了但没发生的成交中有 1,731 笔，是因为吃单被 HL 以 `Order value too large. Max is $500000` / `Max is $2000000` 拒掉。这些吃单都是极少数几张超大的市价单，一张就能扫过很多档，生成几百笔预计成交。
- **规则**：依据 HL 文档 Contract specifications。
  - 市价单的金额上限按币的最大杠杆分档：≥ 25 倍 3,000 万 USD，20–25 倍 500 万，10–20 倍 200 万，其余 50 万。
  - 限价单的上限是 10 倍。
  - 最大杠杆取检查点里该币保证金表的第一档，存进 `PerpMarket.max_leverage`（原始值）；`ActionAsset.max_market_value` 记下市价单上限。现货没有文档，不设上限。
  - 原来的 `PerpMarket.default_leverage`（第一档最大杠杆截到 20）改为方法 `default_leverage()`，由原始值算出，账户回放的行为不变。这样每处构造 `PerpMarket` 都必须给出这个值，由编译器保证不漏（审查 000618 r1 P3：一开始另设了一张按币名索引的并行表，测试夹具会漏填）。
- **判断**（`pending/actions.rs`）：
  - 金额按限价 × 数量算；`FrontendMarket` 按市价单上限，其余按 10 倍；
  - 超过上限的单单独拒掉，计入 `orderbook_pending_book_actions_total{outcome="too_large"}`。这类错误回执里是逐张单给的，同一动作里的其他单照常进集合。
  - IOC 按限价单算，是推断：数据里被拒的全是 `FrontendMarket`。
- **依据**（bm 10-31 区块回执）：
  - LIT（5 倍）：市价单从 509,879 USD 起被拒，454,423 的被接受；
  - HYPE（10 倍）：市价单在 2,027,003 时被拒，报 `Max is $2000000`。
- **测试**（1,234 个通过）：
  - ETH 设为 50 万上限：市价单 51 万拒、45.4 万放行；IOC 51 万放行、500.2 万拒；同一动作里的其他单留下。
  - 分档边界：9/10、19/20、24/25 倍。
  - 变异 3 处都有测试失败：不判上限、所有单都按市价单上限、分档边界错一档。

## 2026-10-31 影子盘口批次 2f：ALO 优先费排队（设计 hyperliquid-trade 000620 r1–r2、代码 r3–r5，用户 10-31 提醒 HL 优先费规则）

- **起因**：1 小时长录 18:12–19:12，归为“排队顺序”的预计成交 239 笔。其中 94 笔，实际被吃的是一张带优先费的 ALO：它在我们预计的那张单之后 400 ms 内挂上，费更高，插到了前面。
- **规则**（HL 文档 Priority fees）：
  - 每一档最近 400 ms 内挂上的单，按优先费从高到低排。新单和“不满 400 ms 的连续队尾”比较。
  - 资格：动作的每张单都是非 outcome 资产（编号 < 100,000,000）上的非只减仓 ALO。不合资格却带 `p` 的动作，HL 照样接受，只是没有排队效果。
- **依据**（bm 10-31 20:15–20:20，节点盘口变动对照区块回执）：
  - 插队（`insertBefore`）共 17,187 次。被插的单能从回执确定优先费的 15,516 次，全部是挂上不到 400 ms、费更低；另 1,671 次查不到被插单的回执。
  - 优先费为 0 的单从不插队。
  - 副本里订单的 `timestamp` 等于它所在块的块时间（32,586 张，全部一致）。
- **改了什么**：
  - `accounts/replica.rs`：`Action.grouping`；`requested_priority()` 读 `{"p": N}`（字符串写法为 0），`alo_priority()` 按资格给出优先费，内存池与区块日志共用；`OUTCOME_ASSETS`。区块回执的逐单状态类型（`WireStatus` 等）从 `marks/actions.rs` 移到这里，两处共用。
  - `pending/mempool.rs`：`SignedAction.priority`。`pending/actions.rs`：`Placement.priority`，改单的新单为 0。
  - `pending/blocks.rs`：区块日志改用 `ReplicaLine` 读，`paired()` 按哈希配回执；对合资格、带正优先费的下单动作（含 `multiSig` 包着的），从回执取出挂上的 oid。动作级状态只认 `ok`（解析）与 `err`（跳过），逐单状态只认“单独的挂上或成交（带 oid）”和“单独的错误”，其他写法、与单数对不上，都整行读不出（不把读不出的费当成 0；审查 000620 r3–r4 P2）。`BlockHashes` 改名 `LoggedBlock`，新增 `priorities`。
  - 开销（审查 000620 r3 离线计时，bm 64 个连续真实块、优化构建）：单块解析 p50/p95 从只读哈希的 0.116/0.301 ms 变为 0.430/1.112 ms，约每秒 14 块，在 watcher 线程。`replica_cmds` 行没有 `local_time`，读取滞后指标没有样本；上线后看 `orderbook_pending_book_block_log_lead_blocks` 和频道的 `block_log_behind`。
  - `pending/record.rs`：优先费随所属区块保存、淘汰，记录重新开始时清空。
  - `pending/overlay.rs`：
    - 待入块挂单记下 `ahead_of`（排在哪张确认挂单前面）、挂上时间、优先费；挂单时按规则定位置：从合并队列的队尾往前，越过挂上不到 400 ms、费更低的单，遇到第一张不满足的就停；被叠加层吃完的确认挂单不挡。
    - 待入块单的挂上时间取 max(到达时间, 确认块时间)（审查 000620 r1 P2-1）。挂着的待入块单只记到达时间，每次比较时按当时的确认块时间取较大值（审查 000620 r3 P2：先到的零费单在空块推进后仍按旧时间比较）。
    - 吃单按合并队列走：每张确认挂单之前，先吃排在它前面的待入块单。
    - 排队无法证明的价位：待入块单排在队尾，这一档吃到的全部记为 `unattributed`，包括待入块单那部分（审查 000620 r2：原来具名报待入块单，但带费的单其实可能排在前面）。
  - `pending/shadow.rs`：
    - 确认块时间变化时，重放持有带优先费挂单的币（审查 000620 r2：只有时间变化，也能让它从插队变为不插队；r4：两单都已排好时，较早的零费单按新时间进入窗口，也要换位，所以不再按到达时间筛选）。
    - 区块日志落后于副本时应用过叠加层，就记标记；记录追上确认高度的那一步，先整体重放，再恢复可用（审查 000620 r1 P2-2）。
    - 启动校验 `--pending-max-late-ms` ≥ 400：记录保留 R = H + E 与空洞后的预热 H 都覆盖比较窗口，“记录里没有就是 0”的前提才成立。
  - `order_book`：`LinkedList::iter_rev`、`PriceLevel::orders_rev`。
- **测试**（1,250 个通过）：
  - 文档原例 B、A、C（A 为确认挂单、为待入块单各一次）；只比连续队尾；越过两张年轻单、停在老单前；被吃完的确认单不挡。
  - 时间口径：确认块时间作下界；恰好 400 ms 不插；同一毫秒按执行先后；费相同先来后到。
  - 排队无法证明的价位：整档不具名、不插队；“吃单、按 oid 撤单、再吃单”的序列区分排队先后。
  - 区块日志：真实回执取出 `(oid, 3)`；只减仓、混入 outcome 资产、`na` 不给优先费；`multiSig` 内层；回执与单数对不上则读不出。
  - shadow：“日志先到 / 副本先到”两种顺序下，恢复可用后的成交归属一致；只有确认块时间推进也会重排；H < 400 启动报错。
  - 零费单先到、空块推进后高费单到来，高费单排在前面；两单都先到、空块推进后换位。
  - 变异 17 处都有测试失败。

## 2026-11-01 影子盘口批次 2g：按签名人记住保证金被拒（设计 hyperliquid-trade 000621 r1–r3，用户 10-31 “用你推荐方案，接受这块潜在不一致”）

- **起因**：批次 2d 上线后 30 分钟（10-31 19:41–20:11），预计了没发生的 5,170 笔里，3,986 笔（77%）是吃单被 HL 以 `Insufficient margin to place order` 拒掉。被拒的账户会对同一个币反复打吃单：被拒之后，同账户、同币、同方向在后面块里的下一张吃单，5 秒内 97.2%、300 秒内 98.4% 又被拒（按回执账户的事后比例）。影子原先不知道一张单是谁签的。
- **规则**（设计 000621 r3）：
  - 身份：动作带 `vaultAddress` 时是这个 vault，否则是从签名恢复出的签名人。一个签名人只代表一个账户（10-31 一分钟 1,196 个签名人全部如此），一个账户用几个 API 钱包就算几个身份。
  - 区块回执里每张会吃单的单（IOC、`FrontendMarket`、GTC）给出一个结果：保证金错误为拒单；成交、挂上、IOC 没碰到挂单为清除；其他错误不更新；整批一条保证金错误，给其中每张会吃单的单。
  - 某身份在某币某方向、按确认高度的最后结果是块时间 t_r 的拒单时，它新到的会吃单的单，只要所在行到达时间满足 t_r ≤ seen ≤ t_r + M，就不进盘口（`margin_refused_recently`，在最小金额规则之后判断）。改单不按这条判断。M 默认 300 秒。
- **签名恢复**（`pending/signer.rs`）：与 HL Python SDK 的 `sign_l1_action` 相同。keccak(msgpack(动作) + nonce + vault 标记与地址 + expiresAfter) 作为 EIP-712 `Agent {source: "a", connectionId}` 的 `connectionId`，域为 `Exchange` / `1` / 1337 / 0x0。msgpack 由动作的 JSON 原文按原键序编码，整数与字符串取最短形式。`v` 为 27/28 或 0/1。动作含非 64 位整数（浮点、超过 64 位），或签名无法恢复时，身份未知、照常当能成交，计数。只对不带 vault、含会吃单的单的下单动作恢复，内存池与区块日志两个 watcher 共用一个函数；动作原文每行用一次 `PointerTree` + `get_many` 取出。
- **改了什么**：
  - `accounts/replica.rs`：`SignedAction` 改为共用，信封字段 `vaultAddress`、`expiresAfter`、`nonce`、`signature` 原样保留（账户视图与 marks 不读，解析行为不变），`vault()`、`expires_after_ms()` 由内存池与影子共用；`ReplicaLine::signed_action(bundle, position)`。
  - `pending/mempool.rs`：改用共用的 `SignedAction`；`SignedAction.signer`；`order_tif()` 由区块日志共用。`pending/actions.rs`：`Tif::may_take()`（非 ALO），`PendingAction.signer`、`asset`、`taker_key()`。
  - `pending/blocks.rs`：`LoggedBlock.taker_outcomes`，按区块执行顺序。动作级状态的判定（`ok` / `err` / 其他报错）与 2f 共用 `ran()`。会吃单的单的逐单状态只认单独的挂上或成交、单独的错误，其他写法与单数对不上都整行读不出。`multiSig` 包着的单不取（签名人不是账户本身，内存池也不持有它们）。
  - `pending/refusals.rs`：`TakerRefusals`。块的结果先按块高暂存，确认高度到了才按块内执行顺序应用，同键以最后一个结果为准；`clear()`；淘汰 t_r + M < 确认块时间 − E（严格小于；仍被持有的单都晚于这个时刻，淘汰无需重放）。
  - `pending/shadow.rs`：`on_block` 在区块日志没有缺口时（缺口由本块结束的也算）暂存本块结果；缺口期间与 `after_loss` 的块不暂存，开缺口时清空记忆与暂存。`settle` 每次都 `apply_through(确认高度)`，无论是副本推进还是日志追上触发；拒单变化的键所在、持有该键吃单的币重放，然后才发布。
  - `pending/overlay.rs`：2f 的 `Queueing` 扩为 `Basis`（优先费、拒单记忆、确认块时间），入池和重放共用；`place_new` 判断最近被拒。
  - `--pending-margin-refusal-ms`（默认 300000，须为正）。指标 `orderbook_pending_book_signers_total{source, outcome}`、`orderbook_pending_book_taker_outcomes_total{outcome}`。
- **核对**（bm 11-01 05:40 一分钟真实数据，840 个区块、1,560 行内存池）：
  - 区块的吃单结果 7,457 条（签名人 2,247 条，涉及 535 个签名人；vault 5,210 条），与研究脚本的独立 Python 实现逐行相同。
  - 内存池里需要恢复的 2,439 个签名全部恢复。
  - 开销（release）：单块解析 p50 / p95 从 0.31 / 0.78 ms 变为 0.82 / 2.27 ms；内存池单行 p50 0.28 ms、p95 1.4 ms。都在 watcher 线程。区块日志行比副本可确认时刻中位早约 60 ms。
- **测试**（1,267 个通过）：
  - msgpack 与 Python `msgpack.packb` 逐字节相同：整数边界、长度边界、键序、转义；浮点与超过 64 位的整数不编码。
  - bm 真实签名 4 例（API 钱包的 IOC、账户自签的 GTC、vault 带 `expiresAfter`、`v` 为 1）恢复出研究脚本给出的签名人。改动作、vault、有效期任何一处，结果都不同；缺 nonce 或签名时整行读不出。前两例同时走内存池与区块两个入口，身份一致。
  - 回执分类：拒单、清除、其他错误、整批保证金与整批其他错误、整笔 `err`；未知状态、单数对不上读不出；`multiSig` 不取。
  - 记忆：确认后才应用、同块最后一个结果为准、闭区间边界、淘汰相等时保留、清空。
  - 叠加层：IOC、`FrontendMarket`、不穿价的 GTC 被挡；ALO、改单、另一方向、另一资产、另一签名人、同地址的 vault、身份未知、窗口之外都照常；最小金额规则优先。
  - shadow：日志先到与副本先到，确认高度之前预测相同，之后都挡；没入块的 GTC 在盘口不变时随拒单、清除、再拒单进出，拒单晚于它的不挡；缺口前已生效的拒单、`after_loss` 块里的拒单，缺口后都不再挡；在窗口内到达、仍被持有的单，在 t_r + M 之后重放时仍被挡。
  - 变异 24 处都有测试失败。

## 2026-11-01 影子盘口批次 2h：预测 TWAP 子单（设计 hyperliquid-trade 000624 r1–r4，用户 11-01 “照常服务+标记”）

- **起因**：TWAP 子单由 HL 在块开始时直接执行，不经内存池，影子原先一笔也看不见；它们在整秒首块里先于所有用户动作吃掉对手盘，影子在这些块上的盘口和成交都会偏。
- **来源**：
  - 账户视图的锚点：checkpoint 里各 perp dex 的 `twap_tracker` 与 `exchange.spot_twap_tracker` 的 `user_to_id_to_state`（总量、已执行、时长、开始、下次到期 `n`、计数 `S`、随机化、父单限价 `sp`），以及等待触发的 id（`user_to_id_to_waiting`）。
  - 节点 TWAP 状态流 `node_twap_statuses_streaming`（激活、等待触发、完成、终止、子单被拒），成交流 `node_fills_streaming` 里带 `twapId` 的成交（按 tid 去重），区块日志里成功的 `twapCancel`。两个流各自一个 watcher，并证明读尽时刻（与账户视图同一机制）。
- **规则**：
  - 网格：m 分钟的 TWAP 有 2m + 1 个时隙，每 30 s 一个，起点为开始 + 1 s；等待触发后激活的，起点为激活时刻；checkpoint 里的 TWAP 按 `n` 所在网格。
  - 目标：总量 × (时隙 + 1) / N；checkpoint 里的 TWAP 用 `S` 减去 `n` 所在时隙（0 或 1）代替 + 1。已执行低于目标时发子单。
  - 执行：到期之后的第一个整秒首块，在块开始、所有用户动作之前。只预测到期不晚于下一整秒起点的时隙；到期更晚但早于该块时间的，也会在该块执行，但块时间事先不知道。
  - 量：缺口按 szDecimals 向下取整。地板量已估出时（两个时隙见到同一个超出缺口的量；随机化 TWAP 不估），取缺口与地板量中较大的。上限为 3 × max(总量 / N, 地板量) 与剩余量；没有地板量且缺口超过 3 × 总量 / N 时不预测。随机化 TWAP 的量随执行块的毫秒变化（常规子单的 ±20%），出块前定不下来，取期望值，即缺口。
  - 吃单：从合并盘口对手最优价起，最多吃到 3% 之外，且不越过父单限价。成交就是 TWAP 吃单的预测成交。
  - 完成证明：高度 H 的事件，要等读到 H + 1 的区块日志行（时刻 r）、且两个 TWAP 流都证明在 r 之后读尽，才按“成交、等待、激活、撤销与结束”的顺序应用。
  - 可信：已安装的锚点覆盖全部丢失，且事件完整到确认高度。丢失分两类：区块日志空洞或已完成高度的迟到行，要求锚点高度不低于该块；流或日志的丢失以及启动，要求锚点块时间比检测时刻晚至少 1 s。不可信时不叠加子单，`twapsKnown` 为 false；常态下每个块在读到下一块日志行之前都会短暂不可信。只重放子单集合有变化的币。
  - 成交只发一次：子单第一次在可服务时成交时发出，按 (twap, 时隙) 记住，直到时隙过去；不可信期间不清。
  - 新锚点：完成高度到达锚点后安装，替换全部 TWAP 状态，重放保留的 30 分钟事件中锚点之后的部分；同一父单保留已估出的地板量。
  - 父单限价两源冲突（审查 000624 r5 P2-1、r6 P2、r7 P2）：每个运行中 TWAP 见过的限价单独记录，与它能否被跟踪无关。
    - 来源：激活的 `stopPx`，在其高度完成时记录，装没装锚都记；没有激活记录时，取第一个列出它的检查点的 `sp`，只要币和价格精度可识别就记，时隙读不读得出都记。
    - 生命周期：撤销或结束时删除。装锚时先只保留锚点列出的 id，再随锚后事件重放激活、撤销、结束。
    - 新检查点给出不同限价时，这个父单不预测，每个锚点计数一次 `limit_conflict`。只在检查点里见到的父单照常预测。
  - 领先上界（r5 P2-2、r6 P3）：完成高度之上的区块日志读入、以及副本停滞时暂存的事件（流的状态与成交、区块日志的撤销），各超过 20,000 个高度就放弃，记为这些高度的丢失（`incomplete_too_long` / `staged_too_long`），等能覆盖的新锚点。
  - 已发现未上报的丢失（r5 P1）：watcher 已发现、通知还在队列里的 TWAP 流或区块日志丢失，在下一个输入（含定时器、锚点）处理前先生效，撤下的子单与本次输入的变更一起发布，订阅者在同一次提交里看到 `twapsKnown=false` 和恢复后的盘口。
- **wire**：
  - `extPendingBbo` 帧和 `extPendingTrades` 帧加 `twapsKnown`。
  - TWAP 吃单的成交：`taker` 为 `{"twap": {"twapId", "user", "slot"}}`，`seenMs` 为 `null`。
  - 交易频道的 ready 状态带 `twapsKnown`；可信状态变化时，即使没有帧变化，也单独通知（与可用性变化同一路径，先提交后广播）。
- **改了什么**：
  - 新增 `accounts/twap_tracker.rs`（checkpoint 的 TWAP 状态，asset 换成动作编号）、`pending/twap_streams.rs`（两个流的解析与 watcher）、`pending/twaps.rs`（`TwapBook`：完成证明、丢失与锚点、网格与量）。
  - `accounts/`：checkpoint 读 `twap_tracker`，`Anchor` 带上 TWAP；`PendingAnchor`（取代 `ActionCoins` 的单独发布）经 `AccountsHandle::pending_anchor()` 交给影子。
  - `pending/feed.rs`：`watch_proven`、`Proven`。`pending/blocks.rs`：`LoggedBlock.read_at`、`twap_cancels`。`pending/overlay.rs`：`take_slice`。
  - `pending/shadow.rs`：`PendingTrade.taker` 改为 `Taker::{Pending, Twap}`；每币先吃子单再重放动作；`settle_twaps`、`replay_coins`；区块日志丢失也是 TWAP 丢失。
  - `pending/publish.rs`、`servers/websocket_server/pending_book.rs`：`twapsKnown`、TWAP 吃单的 wire。`pending/mod.rs`：两个流与锚点的输入，流的丢失按未报告丢失处理；`sync` 返回它造成的变更（`Step::absorb` 合入本次输入），区块日志丢失也立即结算 TWAP。
  - 指标 `orderbook_pending_book_twap_events_total{event}`。
- **核对**（bm 真实数据回放，`pending::twaps::real_data`）：checkpoint 1204980000（439 个运行中的 TWAP）起 25,022 个块，前 8,000 块预热；之后 1,223 个整秒首块，2 个不可信。

  | 子单 | 量一致 | 成交更少 | 成交更多 | 没成交 | 被拒 |
  |---|---:|---:|---:|---:|---:|
  | 普通 | 2,779 | 1 | 38 | 326 | 163 |
  | 按地板量 | 988 | 27 | 40 | 0 | 0 |
  | 随机化 | 117 | 641 | 755 | 10 | 0 |

  - 成交了但没预测到的 317 个：到期在 [下一整秒起点, 块时间) 的时隙，以及地板量未知的追赶。
  - 普通“没成交”的 326 个：TWAP 始终不成交，也没有状态，留待后续研究。
- **测试**（1,300 个通过）：完成证明（读尽时刻须晚于下一块日志行）、整秒首块推进、追赶上限、地板量（两个时隙、随机化不估）、剩余量上限、整秒起点到期、锚点重放与地板保留、丢失覆盖、空洞、checkpoint 的 `n` 与 `S`、tid 去重、等待触发、撤销与结束、`twapCancel` 回执；shadow 的子单先吃并只发一次、先于已持有的吃单重放、不可信时撤下并重放、区块日志丢失；叠加层的滑点与父单限价；可信状态变化的通知；丢失在定时器与锚点入口先生效、撤下的子单同一次提交发布；限价冲突排除父单（首锚前的激活、锚点之后激活在当时未列出的币上、中间检查点读不出、首见检查点时隙读不出时都按证据排除）；证据随撤销与结束删除、装锚时裁剪；暂存超界（含撤销）记为丢失并等锚点。变异 53 处都有测试失败。

## 2026-11-01 影子盘口批次 2i：TWAP 子单一秒内保持稳定，成交临近执行才发（设计 hyperliquid-trade 000631 r1–r2，用户 11-01 定）

- **起因**：2h（`48896aa`）上线后录了 30 分钟（11-01 10:58–11:28）。
  - 子单在每块“读到下一块日志行之前”都不可信，被撤下再放回。`twapsKnown` 30 分钟变化 46,748 次；extPendingBbo 就绪帧是 2g 时的 5 倍，同一币 150 ms 内 A → B → A 来回跳占 7.1%。
  - 子单在这一秒第一个可信的基准上就发出成交，比执行早将近 1 秒。吃单方是 TWAP 的 2,706 笔预测没发生，主要是挂单在这段时间里撤了或动了。
- **规则**：
  - **一秒内保持**：确认高度的下一块还没证明齐（`Incomplete`）时，保留同一基准秒里已知的子单表。这一秒除首块外的块都不执行子单，激活、撤销、结束会晚一块体现。基准进入新的一秒仍未证明齐，或出现没有覆盖的丢失（`NoAnchor`）时，撤下子单。
  - **`twapsKnown`** 改为表示“有子单在叠加中”。约每秒一次为 false，直到新的一秒证明齐；丢失等锚点期间也为 false。同一秒内不再翻转，所以不再对每个 BBO 订阅重发完整帧。
  - **成交窗口**：子单的成交只在确认块时间距下一整秒不到 W（`--pending-twap-trade-window-ms`，默认 400）时发出。默认值的依据：副本确认比块时间晚，p50 227 ms，400 ms 时按中位约在执行前 150 ms 发出。
  - **入窗触发**：基准入窗时，持有尚未发出子单的币重放一次。每个确认高度至多一次，且只在可发布时计入，不可发布时不占用这次机会（审查 000631 r1 P2-2）。
  - **已知限制**：窗口里一个块都没有时，这个时隙的成交就不发。
- **改了什么**：
  - `pending/shadow.rs`：`settle_twaps` 按 `settle` 的结果和基准秒决定保留还是撤下，拆出 `due_anew`、`trade_window_coins`、`in_trade_window`。`replay_coins` 只在窗口内发 TWAP 成交。`settle` 和 `twap_step` 先算可发布性，再结算 TWAP。
  - `PendingParams.twap_trade_window_ms`，以及 `--pending-twap-trade-window-ms`（默认 400，须为正）。窗口判定为 W ≥ 1,000 − 基准毫秒 % 1,000：边界含在窗口内，W ≥ 1,000 时任何基准都在窗口内，不做加法，不会溢出（审查 000631 r3 P3）。
- **测试**（1,304 个通过）：
  - 同一秒内保持、不重放、标志为 true；进入新的一秒撤下并重放，证明齐后恢复；流丢失、区块日志丢失当步撤下。
  - 同一秒内发布器不出任何事件；跨秒只发状态、不发帧。
  - 窗口：3.1 s 起叠加，3.6 s（窗口边界）才发成交，且只发一次。
  - 同一高度两份读尽证明不再重放；下一高度挂单出现后发出一次。
  - 入窗时不可发布不占用高度。
  - 窗口边界：W 为 400 时，3,599 / 3,600 / 3,999 / 4,000 ms 依次为不在 / 在 / 在 / 不在；W 为 1,000 或 `u64::MAX` 时都在。
  - 原 2h 用例按新语义更新。变异 10 处都有测试失败。

## 2026-11-01 影子盘口批次 2j：TWAP 子单排在更早一块的待入块动作之后（设计 hyperliquid-trade 000634 r1–r2，用户 11-01 “你继续吧”）

- **起因**：2h 让子单先于所有待入块动作，只在已确认盘口上吃单；设计里标为近似。2i 让成交临近执行才发出之后，这个近似成了主要误差之一。
  - 2i 录制里，普通 TWAP、单独执行的子单中，175 张量相同、价格不同。
  - 子单吃到、但不在预测里的挂单，凡在读取的块里找到的，全部挂在子单执行块之前的块里，其中 88% 的内存池行早于录制端收到预测（主仓研究 `121eaf47`；这不说明 OB 当时已读到这些行，审查 000634 r3 P3）。
- **规则**：
  - 切点 = 确认块时间之后的下一整秒起点 − 20 ms（`SLICE_CUT_MS`）。
    - 依据（bm 11-01 12:10–12:20）：内存池行在整秒前 0–19 ms 到达的，29.9% 在下一秒首块之前执行；20–39 ms 的 60.0%；80 ms 及以上的 98.9–100%。
  - 重放顺序：切点前到达的已知动作 → 到期子单 → 其余已知动作。行到达时刻恰好等于切点的，归后侧。
  - **整行提交**（审查 000634 r1 P2-1）：一条内存池行先按币分组。币有到期子单、且这一行早于切点时，本行在该币的动作全部入列后，整币只重放一次；本行动作的效果从这次重放里取，子单成交也按这次重放判断窗口与去重。其他情况仍是增量应用。
  - **成交顺序**：本行动作的预计成交按原行执行次序发出，跨币也不变；之后才发子单成交（审查 000634 r2）。
- **改了什么**：
  - `pending/shadow.rs`：`CoinActions::replay(base, basis, slices, cut_ns, report_from)` 返回 `Replayed`；`Shadow::hold` 改为按行、按币处理；新增 `LineTrades`、`trades_of`、`slice_trades`、`slice_cut_ns`。
- **没有子单时不重排**（审查 000634 r3 P2-1）：币没有到期子单时，重放按到达顺序应用全部已知动作，与增量应用一致。行的时间戳不单调时，不会把已被 IOC 吃掉的卖单在下一次重放里挂回。
- **测试**（1,308 个通过）：
  - 没有子单时：先收到 3,981 ms 的卖单，再收到 3,979 ms 的 IOC 吃掉它，下一块重放后卖单仍是已被吃掉；
  - 切点前到达的 IOC 先吃（3,150 ms 与 3,979 ms），恰在切点（3,980 ms）的排在子单之后；
  - 同一行在切点前挂出 101、100 两张卖单：子单只按 100 成交一次，BBO 相符，后续重放不重复；
  - 同一行在 BTC、ETH、BTC 上依次成交，按此顺序发出；
  - 重放的一行只发自己的成交，子单成交在后；之后的行不重发历史动作；
  - 2h 中编码“子单先于所有待入块动作”的用例改为新规则。
  - 变异 9 处中 8 处有测试失败。漏网的一处是“切点后也重放”，它与增量应用的输出等价，只多开销。

## 2026-11-01 影子盘口批次 2k：限价低于参考价 5% 的单，影子也拒（用户 11-01 “你自行推进修复各种问题吧”）

- **起因**：2j 录制（13:43–14:13）里，LIT 上同一账户的两张 GTC 卖单，量 1,500 万、限价 0.0002（当时价格约 3.21）。
  - HL 以 “Order price cannot be more than 95% away from the reference price” 拒单。
  - 影子没有这条规则，预测它们吃穿买盘：共 6,084 笔预计成交没有发生，占这段录制全部“预计了没发生”的 71%。
- **边界**（bm 11-01 13:30–14:20，约 420 万张非触发单，以该币上一笔成交价为参考）：
  - 被拒：229 张买单，比值 0.00001–0.031；2 张卖单，比值 0.00006。
  - 被接受：买单 0.105 起，卖单 0.4999 起；高于参考价的，卖单到 2.62 倍仍被接受。
  - 所以规则只看低于参考价的一侧：限价 × 20 < 参考价 即拒。
- **参考价**：取已确认盘口的买一卖一中间价，只有一边时用那一边，两边都没有时不判断。被拒与被接受之间相距很远，用中间价代理不会改变判断。比较时不先做除法（审查 000637 r1 P2-2）：两边都有时判 40 × 限价 < 买一 + 卖一，只有一边时判 20 × 限价 < 该边。
- **整个签名动作一起拒**（审查 000637 r1 P2-1）：bm 11-01 13:20–14:30 共 479 个含多张单的签名动作带这条错误，每个都只回一条状态，没有逐单给状态的。所以动作里只要有一张非触发单低于价格带，整个动作在进影子之前就被拒，其中每张单记为 `price_band`；同一内存池行里的其他签名动作照常处理。触发单的价格不判（没有样本）。
- **没有副本时先记下，之后再判**（审查 000637 r2、r3）：
  - 入池时没有已对齐的副本，就没有参考价可判，不能把这当成“已判、没越带”。这样的签名动作先登记为“价格带未判”（`Shadow.band_unjudged`）。
  - 登记的依据是动作里全部要判的单（非触发单），包括之后会被逐单规则剔除的那些（`band_orders`，与正常入口共用），不从持有集合反推。价格带只看最低限价，所以每个币只留最低的一个（`unjudged_bands`）。
  - 之后每次结算在重放之前，用已对齐的副本补判：越带的签名动作整个移除，计为 `price_band`，涉及的币重放。
  - 这些（币，最低限价）槽位与持有的动作一起计入 `--pending-max-actions`（审查 000637 r4）：持有数 + 未判槽位 + 本行新增超过上限时，整个集合丢弃。只在通过这道检查、确实持有之后才登记；补判完成和超上限清空时，槽位计数一并归零。
- **改了什么**：
  - `pending/actions.rs`：`book_actions` 多一个参数 `below_band(coin, limit)`，在签名动作这一层判断。
  - `pending/overlay.rs`：`below_price_band(base, limit_px)` 和 `PRICE_BAND_DIVISOR`。
  - `pending/shadow.rs`：`on_tx` 用已确认的副本盘口构造这个判断；`judge_bands` 补判入池时没有副本的签名动作。
- **测试**（1,314 个通过）：
  - 超大加越带：动作里有一张单 4 × 1 亿（既超上限又越带）和一张 101 的 IOC。有副本时直接整批拒；没有副本时先持有，恢复后补判，同样整批移除，卖一 101 不变。
  - 容量：上限 3、副本缺失时连续送入 30 行，每行一个签名动作：BTC 50 张 4 × 1 亿、ETH 50 张 40 × 1 亿（都被逐单规则剔除）加一张 102 的 ALO。第一行被收下，持有 1、槽位 2（每个币只留最低价）；之后每一步持有数 + 槽位都不超过 3，槽位计数与实际清单一致。
  - 补判：盘口 99 / 101 时副本断开，期间入池一行：一个签名动作含 4 的卖单和 103 的 ALO，另一个是 102 的 ALO。副本恢复后，第一个动作整体被移除，102 的 ALO 留下，BBO 仍是 99 / 101；之后的 IOC 买单在 101 成交。
  - 动作级：一个签名动作里一张单低于价格带，两张单都记为 `price_band`，同一行的另一个动作照常；动作里的低价触发单不判，同一动作的 IOC 照常。
  - 价格带：中间价 100（99 / 101）时，4.99 被拒、5 不拒；买一 0.00002000、卖一 0.00002001 时，0.00000100 被拒；只有卖一 101 时，5.04 被拒、5.05 不拒；没有盘口时不判。
  - 影子端到端：只有卖一 101 时，含买 4 和买 101 两张 IOC 的动作没有成交，盘口不变；同一行的另一个 IOC 吃掉卖一。
  - 变异 16 处都有测试失败：逐单判、不判、触发单也判、边界含等号、先除后比、参考价只取买一、单边含等号、影子不传判断、没有副本时不记、之后不补判、补判时逐单移除、超上限不清、槽位不计入上限、不按币压缩、补判后槽位不归零、超上限后槽位不归零。

## 2026-11-01 影子盘口批次 2l 第一步：从内存池的 SetGlobalAction 预测新 mark，只出指标（用户 11-01 “分两步做”）

- **起因**：2i 录制里“真实成交没预计”有 36% 是触发单（2,270 笔），其中 2,263 笔在含 `SetGlobalAction`（下称 SGA）的块里。触发单在主站推 mark 时触发，所以第一步先确认能否在 SGA 行进内存池时算出它要写的 mark。第二步才在影子里执行触发单。
- **证据**（离线用例 `accounts/marks/sga_prediction.rs`，`--ignored`，真实数据 4 万块）：
  - 方法：取 SGA 所在块之前第 k 块的状态和盘口，在 SGA 的块时间刷新缓存，再施加该块的价格动作。
  - 精确比例：k=1 为 99.79%，k=3 为 98.70%（49,193/49,840）。
  - 有触发单成交的币：k=1 时 170 个中 169 个精确，k=3 时 167 个精确。
  - bm 11-01：内存池 47 条 SGA 行都只含这一个动作；236 个 SGA 块里只有 2 个是 3 s 桶的第一块。
- **规则只有一份**：
  - `native.rs` 抽出 `refreshed`，即缓存刷新：新桶的第一块取 10 s 内成员的中位数，没有成员就保留原中位数，桶号照样前进。`MemberCache::begin_block` 和快照都调用它。
  - `select_price` 改为接收中位数本身。
  - 新增 `CoinPriceState::set_global_mark`：只是 `set_global` 选 mark 的那一半。账户视图在 oracle 选价失败、lmpe 断档时不写 mark，这些情况由核对环节记为“未写入”。
  - `SetGlobalAction` 的 `pxs` 解析抽成 `marks::actions::set_global_prices`，marks 和内存池两处共用。
- **内存池解析**：`ActionKind::SetGlobal(Vec<SubmittedPrices>)`。它对盘口的处理与 `Other` 相同：不进叠加，不影响频道。
- **新 owner `pending/mark_forecast.rs` 的 `SgaForecasts`**：
  - 由 `PendingBook` 持有，与 `Shadow` 并列，只读影子的合并盘口和副本盘口，不改影子状态。
  - 价格状态来自账户视图每块发布的 `NodePrices`。pending 线程只多订阅一个 watch，账户线程不多做任何事。
  - watch 关闭（账户线程结束，盘口照常服务）时，撤回它最后给的快照，之后的 SGA 记 `no_prices`，不再读这个 watch（`PendingBook::take_prices`，审查 000639 r2 P2）。
  - **预测**：SGA 行到来、影子可服务时，对每个有报价的主站币分别用两种盘口预测：
    - `pending`：合并盘口的买一卖一；
    - `confirmed`：副本已确认的买一卖一。
    - 块时间取这一行的时间。它既影响是否跨桶，也影响刷新时成员 10 s 有效期的判断（审查 000639 r1 P3）。
  - **认块**：区块日志按哈希告诉预测它在哪一块。
  - **核对**：账户视图的快照到达该块时，按 `mark_update == SetGlobal` 且 mark 时间等于块时间认定为“这一块写的 mark”，再与预测比较。同块多条 SGA 不建模。
  - **容量**：在途最多 8 条，30 s 未核对即过期。
- **指标**：
  - `orderbook_pending_book_sga_lines_total{outcome}`
  - `orderbook_pending_book_sga_marks_total{book,outcome}`
  - `orderbook_pending_book_sga_mark_error_bps{book}`
  - `orderbook_pending_book_sga_bucket_total{outcome}`
  - `orderbook_pending_book_sga_prices_behind_blocks`
  - `orderbook_pending_book_step_seconds{step="sga_forecast"}`
  - 偏离的输入每 10 s 至多记一条日志。
  - 计数先汇总成 `ForecastReport` 再计入，测试读这份报告。
- **测试**（1,325 个通过）：
  - `set_global_mark` 与 `set_global` 写出的 mark 相等，覆盖以下几种情况：
    - 同桶；
    - 新桶有新鲜成员；
    - 新桶成员全部过期；
    - 成员恰好 10 s；
    - `Submitted` 模式。
  - 内存池解析真实形状的 SGA 行，覆盖 Priced、Unpriced、Partial 三种。
  - SGA 行进影子后不改盘口、不发成交。
  - 先收到有效快照，再关闭发送端，之后的 SGA 记 `no_prices`；盘口仍服务，IOC 照常预测成交。
  - 预测与核对：`exact` 与 `off`（含 bp）、`superseded`、`not_set`（含同块自动刷新）、`unclaimed`、两处 `late`、过期、在途上限、重复行、无价格表、无快照、不可服务。
  - 两种盘口一边为空，单位不能整除，没有中位数，提交值读不出，币不在快照里。
  - 桶号与实际块时间相同和不同两种情况。
  - 变异 20 处都有测试失败（第 20 处是 watch 关闭时不撤回快照）：
    - 同桶也刷新、成员寿命含等号、用未刷新的中位数、本地价不看最后成交、无成员时清空中位数；
    - 用确认盘口代替合并盘口、核对不看 mark 时间、不看更新来源；
    - 单位少一个 10、单位不整除也换算；
    - 不判两处 `late`、过期含等号、不限在途、重复行再预测、未到块就核对、桶号两边都用行时间、价格不可用时留旧快照、不解析 SGA。
    - 第一次跑时漏了“成员寿命含等号”，补了恰好 10 s 的用例后抓到。

## 2026-11-01 影子盘口批次 2l 第二步：在 SetGlobalAction 的位置执行被触发的触发单（用户 11-01 “分两步做”）

- **起因**：2k 录制里，“真实成交没被预计”有 23% 是触发单；第一步线上核对 SGA 的 mark，用合并盘口预测精确 99.21%（`0804308`，16:17–16:46，545 次）。
- **设计**：000642 r1–r3。
  - **输入**：forced flow 全币 50 bp 帧。pending 线程照 WS 订阅者的做法接入：持有 `(全币, NearWide)` 的需求，先全量查询一次，之后跟随广播。
  - **漏帧与停用**：广播 `Lagged` 后重查，查询回来前全部币未知；查询返回的 `seq` 与已采用的相同，也结束等待（r2）。forced flow 撤回或广播关闭后，直到重启都不执行（r1 P2-3）。
  - `Source::Trigger` 增加精确的 `trigger_px`。
- **判定**：
  - SGA 行到来时，用合并盘口预测的 mark 判断。合并盘口取“这一行执行时”的：行早于 TWAP 切点、该币有到期子单时，取子单之前的盘口（`Shadow::best_at_line`，与 `CoinActions::replay` 共用切点前那段，r1 P2-2）。第一步的预测指标也改用它。
  - 下穿型 mark ≤ 触发价、上穿型 mark ≥ 触发价即触发，已越过的一并触发；移动止损不建模。
  - 预测 mark 超出帧的 50 bp 带时，整个币未知，一张都不执行。
- **执行**：
  - 每币的被触发单组成一个批次（`BookOp::Triggers`），作为这条 SGA 行的动作持有。
  - 批次内按（有效量, 账户, oid）升序执行。
  - 每次执行（首次和每次重放）都从批次的入口仓位起算：只减仓单受可平仓量封顶，整仓单取可平仓量（r1 P2-1）。
  - 每笔成交同时更新吃单方，以及本批里账户已知的挂单方（副本挂单、挂回的触发单剩余，r3）。
  - 市价单下 IOC，限价单下 GTC；挂上的剩余带原 oid，按 oid 的撤单和改单能找到它。
  - 最小金额、价格带、保证金记忆都不判。
  - 容量按单数计；随 SGA 的块确认后离开。
- **线上协议**：
  - `extPendingTrades` 新增吃单方 `{"trigger": {"oid", "user"}}`；
  - `seenMs` 取 SGA 行的时间；
  - `user` 过滤匹配触发单的账户；挂单方按已知账户匹配，挂回的触发单剩余也算（`Maker::account`，审查 000642 r4 P2-1）。
- **不建模**：
  - HIP-3 触发单、SGA 内的强平单；
  - 内存池里新下或撤掉的触发单；
  - 账户不可知的内存池挂单被吃时的仓位。
- **指标**：
  - `orderbook_pending_book_trigger_frames_total{event}`
  - `orderbook_pending_book_triggers_total{outcome}`
  - `orderbook_pending_book_trigger_coins_total{outcome}`
  - `step_seconds{step="sga_triggers"}`
- **测试**（1,345 个通过）：
  - 判定：等号与已越过、带宽边界、排序、批次仓位。
  - 审查的三个仓位场景：
    - 重放时前序成交变化，后面的封顶随之重算；
    - 普通买单增加仓位；
    - A/B/A。
  - 执行：按 oid 撤掉挂回的剩余；不判最小金额；封顶为 0 的单不下。
  - 持有、结算与容量：随块确认、按单数计容量、批次本身超上限；`best_at_line` 在切点前后。
  - 订阅输入：去旧、`Lagged` 后同 `seq` 恢复、撤回或关闭后迟到的结果不恢复。
  - wire：trigger 吃单方；`user` 过滤认已知账户的挂单方（副本挂单、触发单剩余），不认账户未知的内存池挂单。
  - 端到端：SGA 行触发并成交；撤回后不再触发。
  - 变异 22 处都有测试失败（第 22 处：`user` 过滤只认副本挂单）：
    - 判定：等号改严格（两种方向）、带宽边界含等号、去掉带宽判断、排序不看有效量、移动止损也触发；
    - 批次仓位：挂单方不更新、只减仓不封顶、吃单方符号反了、不记成交；
    - 执行与持有：触发单也判最小金额、挂回的剩余不带 oid、切点前也读子单之后的盘口、持有和结算按动作数计、批次不判容量；
    - 线上协议：吃单方没有 `seenMs`；
    - 订阅输入：同 `seq` 时不结束等待、忽略撤回、停用后仍采用查询结果；
    - 接线：SGA 行不执行触发单。
    - 第一次漏了“结算按动作数计”和“批次不判容量”，补用例后抓到。

## 2026-11-01 影子盘口批次 2l 第三步：市价触发单按触发价 10% 滑点执行，不用它自己的限价

- **起因**：2l 第二步上线后录制 30 分钟（`after2l2_20261101T1755`）。
  - 预测的触发成交 1,651 笔，1,321 笔先于真实成交配上，中位领先 227 ms。
  - 漏掉的触发成交 839 笔，其中主站 818 笔；漏掉的触发单 758 张，754 张是 `Stop Market`。
  - 同期指标：被触发的单里，IOC 一笔都没吃到 1,367 次，吃到 1,347 次。
- **原因**：市价触发单挂着时，它的 `limitPx` 等于触发价。节点触发后执行时，限价是触发价约 10% 之外，例如 ENA 0.18347 → 0.16513、BTC 77993 → 70194。
  - 第二步按单上的限价下 IOC：卖单只肯在触发价以上成交，而那时盘口已在其下，于是一笔不成。
- **规则核实**（bm 11-01 17:00–19:00 的订单状态，触发后执行的 `Triggered` 行）：
  - 限价触发单 2,316 张，执行限价都等于它自己的限价。
  - 市价触发单 3,520 张：
    - 执行限价相对“触发价 ×0.9（卖）/ ×1.1（买）”在 −1 到 +10 bp 之间；
    - 自己的限价不是触发价的 835 张里，703 张照样换成 10% 那个价；
    - 保留自己限价的 132 张，自己的限价本来就在 ×0.9 / ×1.1（`own/trigger` 0.9000–0.9002、1.0999–1.1001）。
  - 精确的参考价和取整没有证实。差一个最小价位，对 10% 外的流动性几乎没有影响。
- **改了什么**：`pending/triggers.rs`：
  - 市价触发单的 IOC 限价取 `market_limit(side, trigger_px)`，即卖 ×0.9、买 ×1.1（`MARKET_SLIPPAGE_PERCENT` = 10）；
  - 限价触发单不变。
- **测试**（1,346 个通过）：
  - HYPE 卖出触发价 87.98 → 限价 79.182，SUI 买入 0.90198 → 0.992178；
  - 自己的限价是 1 的市价卖单，限价取 90，限价单保持自己的 1。
  - 变异两处都有测试失败：仍用自己的限价、买卖方向反了。
- **范围外（单独报告）**：forced flow 的 walk 对市价触发单也用它自己的限价（`Entry::limit_px`），扫盘估计会在触发价处停住。不在本批。

## 2026-11-01 forced flow：市价触发单的 `limitPx` 和单笔扫盘按节点的执行限价（用户 11-01 “如果小修复发现了就修复吧”）

- **起因**：2l 第三步发现，市价触发单在节点上按触发价 10% 滑点执行，不用它自己的限价，见上一节。
  - forced flow 的 `trigger_entry` 原来把 `Entry::limit_px` 设成单上自己的限价，而它通常就是触发价。
  - 于是 `extForcedFlow` 每条市价触发单的单笔扫盘（`walk`）在触发价处停住（`end: limitReached`），低估了它会扫到的深度。
  - 累计扫盘（`cumulativeWalk`）本来就不设价格限制，不受影响。`forced_flow_trader` 的布防判断读累计扫盘；单笔扫盘读出的盘口档位只给 IOC 用，IOC 10-31 起已关闭。
- **改了什么**：
  - `accounts/forced_flow.rs` 新增 `trigger_limit(kind, side, own_limit, trigger_px)`：
    - 限价触发单取它自己的限价；
    - 市价触发单：卖为触发价 ×0.9，买为 ×1.1（`MARKET_TRIGGER_SLIPPAGE_PERCENT` = 10）。
    - `Entry::limit_px` 由它给出，单笔扫盘随之按这个价走。
    - 买单的 ×1.1 超出监听价格单位时返回错误，经 `trigger_entry` 走已有分支，把该币标为不可用，不会 panic（审查 000650 r1 P3）。
  - 影子盘口（`pending/triggers.rs`）改为直接用条目的 `limit_px`，这条规则只在 forced flow 一处，原来的 `market_limit` 删除。
- **线上协议**：`extForcedFlow` 条目的 `limitPx`，对市价触发单从“单上自己的限价”变为“节点执行的限价”。字段与格式不变，README 写明含义。主仓的 `forced_flow_trader` 和 `forced_flow_eval` 都不读这个字段。
- **测试**（1,347 个通过）：
  - `trigger_limit`：HYPE 卖出 87.98 → 79.182，SUI 买入 0.90198 → 0.992178，限价单保持自己的；
  - 自己的限价等于触发价时，市价条目取 ×0.9，限价条目取自己的；
  - 影子盘口的批次单取条目的限价。
  - 变异两处都有测试失败：买卖方向反了、条目仍用自己的限价。

## 2026-11-01 影子盘口批次 2m-1：止损止盈单按普通挂单的方式建模（用户 11-01 “动手吧……按照正确的来”）

- **起因**：2l 第二步取了捷径，被触发单的清单直接用 forced flow 的全币 50 bp 帧，带来四个问题（设计 000652 r1）：
  - 滞后：帧要等区块写出、再等 forced flow 一轮计算；
  - 范围：只含 node mark 50 bp 内的单，预测 mark 越出就整币未知；
  - 口径：帧的高度不是副本的确认高度；
  - 内存池里的触发单挂单、撤单、改单都不进清单。
  - 2l 第三步录制（`after2l3_20261101T1857`）里，漏掉的触发单 416 张中 234 张挂单不到 1 秒就被触发；误报的 233 张中 229 张在 SGA 前已被撤。
- **设计**：000652 r1–r5。
- **副本里的未触发单表**：
  - fork 时随盘口一起复制监听线程的 `TriggerTable`（`replica_copy`），含等待母单的子单、modify 继承和逐币缺口；用同一批状态行维护。
  - 副本的表不对外报告：计数、日志、`fired_trigger_coins`（通知 forced flow 重算）都只在监听线程执行（`Observer::{Listener, Replica}`、`OrderBookState::reports`）。
  - 副本把“触发单表有变化的币”并入变动币，该币即使盘口没变也重放（r3 P2-2）；改单后继单挂起（`SuccessorPending`）与解除也算变化（r6 P2-4）。
  - cloid 索引含未触发的触发单：fork 时的表，和之后 `open` 的触发单。
- **内存池的触发单动作**：
  - 解析 `t.trigger`（触发价、是否市价、tp/sl）和动作的 `grouping`。方向由 tp/sl 与买卖决定：止损卖、止盈买为下穿，其余为上穿（bm 11-01 19 时 122,804 张 `open` 全部符合）。
  - `positionTpsl` 为整仓单；改单的新单数量为 0 也是整仓单；`normalTpsl` 的子单（等母单成交）不建模。
  - 新动作 `BookOp::PlaceTrigger`、`BookOp::ModifyToTrigger`；叠加层按币维护 `triggers_gone`（副本表里被撤、改、触发掉的 oid）和 `triggers_added`（待入块、未触发的单）。
- **撤改目标统一解析**（普通单与触发单共用；撤单统一为 `BookOp::Cancel { target, fast }`）：
  - 动作账户已知时（vault），已知属于其他账户的单不是目标，按 oid、按 cloid 都如此（代码审查 000652 r6 P2-2）；
  - 按 oid：副本盘口 → 副本触发单表（未移出）→ 已触发单挂回的余量；
  - 按 cloid：待入块（挂着的单、新挂的触发单）优先，其次副本盘口和副本触发单表；多于一个为歧义。
  - 快速撤单（`f`）不撤未触发的触发单，计 `fast_trigger_refused`（HL 文档 “Cancel order(s)”，r6 P2-1）；已触发后挂着的余量照常可撤。
  - 改成触发单的改单，沿用原准入：`always_place` 时目标在不在都挂；否则目标缺失或歧义即拒。
- **SGA**：
  - 在每个有预测 mark 的主站币上持有一个动作 `BookOp::SetGlobal`：mark 按“这一行执行时”的合并盘口预测，行到达时冻结；同时冻结同一位置（TWAP 切点前后与 mark 相同，r6 P2-3）被越过单的已知账户的仓位。
  - 仓位来自影子线程自己的账户副本：账户发布器原来只喂 forced flow 一份，改为两份，各自队列、代次、重同步和有效性 watch（`CopyOwner`）；影子那份结束只关闭它自己，不撤回 forced flow。
  - 每次应用（首次与每次重放）：候选 = 副本表里被越过的单（`in_band(coin, mark, mark, 10_000)`，减去已移出的）加上待入块里被越过的单；各触发一次，移出未触发集合（待入块单一次过滤，r6 P3）；按（有效量，已知账户在前，副本单按 oid 在待入块单前）排序，作为一批执行，规则同 2l：冻结仓位起算、每笔成交更新吃单方和已知挂单方、`trigger_limit` 定限价、余量带账户和 cloid（副本单还带 oid）挂回。
  - 整币不执行的条件：表不完整、越过超过 10,000 张；需要仓位的单账户未知，或账户未知的单与需要仓位的单同批；需要仓位的单其账户未冻结（`positions_unfrozen`；账户副本不可用时为 `positions_unavailable`），或在本次重放里已被成交动过（`position_moved`）。
  - 被成交动过的账户（`moved`）：重放中每笔成交的吃单方和已知挂单方都记，普通待入块挂单也带原动作的 vault（r5 P2-1）。每次应用把 `moved ∩ 冻结账户` 写回该动作的 `stale`，直到这一行结算（r4、r5 P2-2）。
  - 实现时补的一条：判为整币不执行时，被越过的单仍视为已触发——移出未触发集合，它们的已知账户记入 `moved`。否则同一次重放里后面的 SGA 会再次触发它们。
- **线上协议**：`extPendingTrades` 的触发吃单方：
  - 副本单 `{"trigger": {"oid", "user"}}`，与原来相同；
  - 待入块单 `{"trigger": {"placed": {"txHash", "action", "item", "cloid"}, "user"}}`，`user` 不知道时为 `null`，不虚构 oid。
- **删除**：`pending/trigger_frames.rs`、`FlowFeed`、对 forced flow 帧的订阅与需求，`orderbook_pending_book_trigger_frames_total`。forced flow 只留 `trigger_limit` 一条共用规则（`OrderKind::of` 改为 `pub(crate)`）。
- **未建模**：只有代理钱包签名、不带 vault 的动作，账户未知（2m-2 学签名人 → 账户）；HIP-3 触发单；SGA 内的强平；`normalTpsl` 子单。
- **指标**：
  - `orderbook_pending_book_triggers_total{outcome}`：`confirmed`、`pending`、`zero_size`；
  - `orderbook_pending_book_trigger_coins_total{outcome}`：`executed`、`none_fired`，及上面各个不执行原因、行到达时的 `no_forecast`、`mark_overflow`；
  - `orderbook_pending_book_accounts_resync_total{cause}`、`_accounts_queue_depth`、`_accounts_snapshot_seconds`；
  - `overlay_total` 新增 `trigger_placed`、`set_global`。
- **代码审查**：000652 r6 需修改（6 项 P2、1 项 P3），r7 OB 部分通过、研究脚本 3 项 P2 和 1 项 P3，r8 研究脚本 1 项 P2，均已修复。r6 各项：快速撤单不撤触发单、撤改按账户过滤且余量保留 cloid、冻结位置与 mark 一致、后继单挂起通知重放、研究工具一对一归因与旧测试迁移、批量移出一次过滤。
- **测试**（OB lib+bins 全量通过，数字见送审）：
  - 叠加层：每次重放按冻结仓位重算；普通买单加仓；批内挂单方减仓；按 oid 在触发前后都能找到；最小金额不判、零量；只触发一次、等号触发；待入块单触发、同账户按 cloid 撤掉、别的账户撤不到；余量按 cloid 撤；改成触发单及目标缺失时拒；账户未知的两种情形；未冻结与副本不可用；vault 挂单被吃后整仓单未知（r5 验收）；`stale` 已写入时不再用旧仓位；表缺失或不完整；盘口与触发单表同 cloid 为歧义；判未知时仍触发；快速撤单（按 oid、按 cloid）不撤触发单、可撤余量和盘口单；他人账户按 oid 撤不到（触发单、盘口单）；已确认触发单的余量按原 cloid 撤。
  - 影子集合：随行持有、随块离开；两条 SGA 只触发一次、第一条确认后第二条重放不再触发；待入块限价单被拒后不留余量；确认撤单单独唤起重放；IOC 先确认后 `stale` 仍生效（r5 验收）；待入块按 cloid 撤副本触发单；切点前的 SGA 按切点前的状态冻结账户（r6 P2-3 反例）。
  - 副本：fork 后与监听线程同一批状态行下表一致，且不记 `fired_trigger_coins`（真实的母单、子单、成交释放、触发行）；cloid 索引含 fork 时与之后的触发单；后继单挂起与结算（仍等母单）都报该币变动。
  - 端到端：SGA 行触发副本止损；冻结被越过单账户的仓位（账户副本 1 BTC，整仓单卖 1）；账户副本不可用时整仓单不执行。
  - wire：两种触发吃单方；`user` 过滤认已知账户（两种来源），不认未知。
  - 仓位换算：原始单位 × 10^(8−szDecimals)，币不在宇宙或无副本时为未知。
  - 变异 34 处都有测试失败：第一轮 27 处（见送审 r6），r6 修复后 7 处（快速撤单拒、按 oid 盘口单与触发单的账户过滤、确认余量的 cloid、冻结位置、后继单挂起与结算的变动通知）。“后继单结算仍等母单”第一次漏了，改用例后抓到。
- **研究脚本**（主仓 `analyze/research/mempool_pending_book_20261031/scripts`）：`channel_compare` 认两种触发吃单方并按来源分桶；`trade_miss_causes` 的 `PlacedTriggers`：回执给 oid 用回执，回执 `waiting` 时同块同账户同触发价同量、与预测同币、cloid 完全相同（无对无）、且不是其他回执已给出的 oid 的 `open` 行一对一关联（一条行能对应多张单、或一张单对应多条行，都不归因；回执不带币名，同组其他单的币按未知处理），吃单方和挂单方余量共用；无法归因的单列 `unattributable` 类，不进漏报误报，汇总按类别计数；挂单余量的结局里 `triggered` 不算结束（000652 r7）；窗口内所有触发单都记为触发单，经 `open` 还原的挂单方其对手也能认出（r8）。54 个测试通过，变异 13 处都有测试失败。

## 2026-11-01 影子盘口批次 2m-2：签名人 → 账户（用户 11-01 “按照你的顺序吧”）

- **起因**：2m-1 上线后录制 30 分钟（`after2m1_20261101T2115`）。逐单追查 27 张漏掉的主站止损止盈单，其中 7 张（41 笔）出在同一条 NEAR 的 SGA 上。
  - 原因：有人用代理钱包签名、不带 vault，反复挂 NEAR 止盈触发单，账户未知。这些单与已确认的整仓单一起被越过时，按 2m-1 的规则整币不执行。
  - 设计 000652 r1 第 3 节、r2 P2-3 已定：区块回执学“签名人 → 账户”，只从不带 vault 的回执学；同一签名人对应过两个账户即不可用；有上限、有过期。
- **改了什么**：
  - `pending/blocks.rs`：
    - `signers` 一处恢复区块里要读签名人的动作：不带 vault、已执行，且是可能吃单的 order（2g 原有），或挂触发单的 order、modify、batchModify（2m-2 新增）。`multiSig` 不读。
    - `taker_outcomes` 改用 `signers` 的结果，2g 行为不变。
    - `signer_accounts` 给出（签名人，回执的 `user`）。已执行的动作回执没有 `user` 时，这一行读不出。
  - `pending/signer_accounts.rs`（新）：`SignerAccounts`。
    - 按块时间记录每个签名人最后一次出现，淘汰索引按最后一次出现排序（`BTreeSet`，代码审查 000656 r1 P2-1）；过期和上限见文件里的常量，超过上限时最久未出现的先移出。
    - 每块先清过期，再学这块的回执，再按上限清：过期的签名人在新块里重新学，不算冲突。
    - 同一签名人对应过两个账户，判为冲突，此后不给账户，直到它过期移出。
    - 区块日志有缺口时照样学：块里执行过的事实不因缺口失效。
  - 签名人第一次变成冲突时，或被淘汰（过期或超出上限）时，仍持有、没有 vault、账户由它学来的动作，账户撤回为未知，所在币重放；之前已发出的预测不改（r1 P2-2、r2 P2）。被淘汰后即使重新学到同一账户也不恢复：持有的动作只活一两秒，实际只有容量淘汰会碰到，取保守。
  - `pending/mempool.rs`：签名人恢复的范围扩到不带 vault、挂触发单的动作（`places_trigger`）。
  - `pending/actions.rs`：`PendingAction.account` 在动作到达时定下：vault，否则查学到的签名人账户（`account_of`）。挂触发单的动作按来源计数。
  - `pending/overlay.rs`：凡是用到动作账户的地方都改用 `account`，包括冻结仓位与排序的触发单账户、`moved`、挂单余量的账户、撤改目标的账户过滤。2g 的保证金拒单身份（`TakerKey`）仍按 vault 或签名人，不变。
- **线上协议**：没有新字段。
  - 待入块触发吃单方的 `user` 现在可以是学到的账户。
  - 挂单方的 `user` 过滤会匹配学到的账户（`Maker::account`）。
  - 普通待入块吃单方的 `vault` 字段仍只是 vaultAddress。
- **指标**：`orderbook_pending_book_trigger_accounts_total{source}`（`vault`、`learned`、`signer_unseen`、`signer_conflicting`、`no_signer`），`orderbook_pending_book_signer_accounts{state}`（`known`、`conflicting`）。
- **测试**（OB lib+bins 全量通过）：
  - 区块学映射：可能吃单的 IOC 与挂触发单的动作都学到；只挂 ALO 的动作和带 vault 的动作不学；同一个触发单动作，区块日志与内存池恢复出的签名人相同；已执行却没有 `user` 的回执读不出。
  - 动作账户：vault 优先、学到的签名人账户、冲突、未见过、没有签名人。
  - 影子集合：区块日志缺口中照样学到，随后该签名人的待入块触发单带上账户；相隔 25 小时的块重新学，不判冲突；签名人变冲突后，持有的只减仓单失去账户，随后的 SGA 整币不执行（原来会按旧账户卖出 1）。
  - `SignerAccounts`：冲突（只在变成冲突那一次报告）、交错续期后各按自己的最后一次出现过期、过期后重新学、超过上限时移出最久未出现的（中途续期的保留）。
  - 被淘汰的签名人（重新学到同一账户或另一账户）撤回已持有动作的账户。
  - 变异 17 处都有测试失败（r2 后加：淘汰不报告、冲突不撤回、撤回不重放）：区块不读触发单签名人、带 vault 也学、内存池不恢复触发单签名人、不用学到的账户、vault 不优先、叠加层仍用 vault、影子不学、不判冲突、过期从首次出现起算、不设上限；r1 后：续期不重排、学之前不清过期、冲突不撤回、撤回不重放。“学之前不清过期”第一次漏了，补影子层用例后抓到。

## 2026-11-02 移动止损批次 2n-1：节点 mark 重放维护主站移动止损（设计 hyperliquid-trade 000659 r1–r4，用户 11-01 “接着做移动止损吧”）

- **起因**：影子盘口不执行移动止损。节点在 best 移动时不推状态行，所以副本表里移动止损的 `triggerPx` 是过时的。设计 000659 r2 定下：best 由账户视图的 mark 重放这一处维护。理由是它同时有两样：节点检查点里的当前状态，以及按块内执行顺序的每一次 mark 写入。
  - 研究依据（主仓 `analyze/research/mempool_pending_book_20261031`，`c4ae01e3`）：
    - 节点快照存的是当前 best：22:48 的快照，203 张里 111 张与最后一条状态行不同，全部偏向跟踪方向。
    - 激活：卖单在 mark ≥ 激活价时激活，买单在 ≤ 时激活，首个 best 取激活那条 mark（用户 11-02 实测）。
    - 1 小时录制逐笔重放：mark 覆盖完整的 6 张，都在第一次越过的那一块触发。
    - 主站挂着 299 张移动止损，挂单时长中位数 57 小时。
  - 本批只维护和自检，不改影子；影子使用放在 2n-2。r2 设计把“发给账户副本”列在 2n-1，因为它在本批没有读者，移到 2n-2 与读者一起做。
- **改了什么**：
  - `accounts/marks/trailing.rs`（新）：`TrailingStops`，由 `MarkState` 持有，随 mark 状态一起装载、快照、按日志重放、换锚点核对。
    - 每块按执行顺序处理：
      - `open` 行按行里写的加入；
      - `SetGlobalAction` 写入的每个 mark 推进该币的单：等待中的在 mark 到达或越过激活价时激活，best 取这条 mark；已激活的，卖单 best 取最高、买单取最低；
      - 同块里在写入之后才下的单不推进，先后取 `trailingStop` 回执给出的 oid 所在动作的执行位置；
      - 单子离开未触发集合的那一行（`triggered`、`canceled` 等）核对规则、best 和触发价，然后删除；之后同 oid 的行（`filled`，条件为 `Triggered`）不再读（审查 r3 P2-1）。
    - 触发价：卖单 best 减回撤、买单加，向上取半到价格单位（270 条状态行）。
    - 写入覆盖每个被定价的币，不论是否已持有单：一个币第一张单可能在同块 SGA 之前挂出（r1 P2-2）。
    - 以下情况记为未知，直到检查点重新装载：
      - 自动刷新写过 mark 的币（价格 owner 交出实际写过的币，r1 P2-3）：会不会推进 best 没有证据，10-31 至 11-02 也从未发生（r3 阶段门）；
      - 本块价格动作没有应用：已有的单和本块新挂的单；
      - 与本币写入同块、却没有下单执行位置的新单；
      - mark 不被重放认领的币：价格动作可能撤掉某币的 mark 而不写它（如 SGA 撤掉报价覆盖）。每个价格动作之后，这些币在该执行位置记一条“无 mark 的写入”，同块之后的离开行不按旧 best 核对（r2 P2）；块后和装载时再清一遍；
      - 读不出的 `open` 行；
      - 日志重放不到的锚点上：核对不一致的单、多出的单，缺单的币的全部单，缺的那张保留为 `anchor_missing`，让它的离开行能对上（r2 P2）。旧锚点不让任何单变成已知，也不删单，因为其间可能已有真实的离开行（r1 P2-4）。
  - `accounts/marks/seed.rs`、`accounts/checkpoint.rs`：主站盘口改用 `WireNativeBook`，读 `user_states[].U` 中的移动止损（`t`），普通止损（`s`）跳过。
    - 移动止损先按 MessagePack 自身的类型整体读成值（`WireValue`，二进制等类型也照读），再在条目边界读字段（代码审查 000661 r1、r2 P1）。
      - 字段约束与 serde 读法一致：已知字段重复、`r` 不是恰好一个 `Pct` 或 `Px` 的，该单读不出；`a`、`b` 缺省或为 nil 都读作没有（r3 P2）。以下几种只影响移动止损：字段读不出（既无 best 又无激活价、字段是另一种形式），或 `U` 中出现其他种类。这些条目按（资产，oid）保留为未知，计 `unread` 并留证据；检查点照常读完，mark 不扣住。这与 XTEN 一事后“缺键的币只影响它自己”的处理一致。
    - 现货盘口不变。
  - `accounts/marks/actions.rs`：`trailingStop` 是第 3 类动作，`MarkAction::PlaceTrailing { oid }`，oid 取自回执。回执读不出只影响移动止损，不影响 mark。
  - `accounts/marks/native.rs`：`refresh_stale` 返回这次自动刷新实际写过的币。
  - `accounts/marks/mod.rs`：
    - `apply_prices` 返回本块 SGA 的写入，按执行位置排列；
    - 换锚点时 `MarksCheck::Checked` 带上 `trailing`。它不计入 `differs`，不会扣住 mark。日志重放不到的种子，把不一致的单记为未知。
  - `listeners/order_book/triggers.rs`：`TrailingTerms` 解析 `triggerCondition`，账户视图和 2n-2 的副本表共用。
- **线上协议**：不变，本批没有对外字段。
- **指标**：
  - `orderbook_node_marks_trailing_lines_total{line, result}`；
  - `orderbook_node_marks_trailing_stops{state}`；
  - `orderbook_node_marks_checked_total{market="trailingStops"}`；
  - 证据 `nodeMarkTrailingLine`、`nodeMarkTrailingCheck`。
  - 指标名用节点 mark 的前缀，r2 设计里写的是 `orderbook_accounts_trailing_*`。
- **测试**：
  - 激活：等号算激活、一步跨过取当时的 mark；
  - 触发价向上取半（BTC 591778253588 的 0.34% 为 77875.7）；
  - 百分比的千分位读法；
  - 多块推进保留极值（r1 P2-1 的 100 → 110 → 107 → 104）；
  - `triggered` → `filled` 只核对一次（bm 1205810990）；
  - 与写入同块的新单按执行位置推进，没有执行位置的记为未知；
  - 写入改变了单子时，同块离开行记为 `unordered`；
  - 不一致、自动刷新、读不出的回执；
  - 换锚点核对；
  - 检查点装载三种形态；
  - `trailingStop` 回执；
  - `MarkState` 整体：一条 SGA 推进已装载的单，在它之后下的单不推进，换锚点发现不一致后以检查点重建。
  - 代码审查 r1 后补：
    - 某币首张单挂在同块 SGA 之前（`MarkState` 整体）；
    - 价格动作未应用的块里新挂的单；
    - 自动刷新只影响写过的币；
    - mark 不被认领的币；
    - 检查点读不出的条目保留为未知，字段为另一种形式或 MessagePack 二进制值时检查点照常读完；
    - 同块里先撤掉币的 mark、后撤单，离开行记为 `unordered`（`MarkState` 整体）；
    - 空集合上缺单时，缺的那张保留为未知，离开行计 `unknown`；
    - 日志重放不到时撤回不一致、多出和缺单的币。
  - 变异 18 处，都有测试失败：
    - 首轮 6 处：激活等号、下单先后、同块离开行延后核对、向上取半、写入、自动刷新；
    - r1 后 6 处：只写已持有的币、未应用块的新单、刷新不分币、不清理不被认领的币、读不出的条目不保留、缺单不撤回整币；
    - r2 后 3 处：撤掉 mark 不记写入、缺单不保留、二进制值（一处第一次因测试过滤词没选中用例而显示未抓到，换过滤词后抓到）；
    - r3 后 3 处：字段重复、`r` 多一个键、nil 读作没有。

## 2026-11-02 移动止损批次 2n-2：影子盘口执行移动止损（设计 hyperliquid-trade 000659 r2–r4，用户 11-01 “接着做移动止损吧”）

- **起因**：2n-1 上线后（17bea52，11-02 04:57Z），节点 mark 已准确维护主站移动止损：上线 1 小时，5 个检查点 1,353 次比对全部一致，27 条离开行全部核对一致。本批让影子盘口在 SGA 处执行移动止损；r2 第 2 节的“发给账户副本”随本批一起做。
- **改了什么**：
  - 副本发布（`accounts/flow_accounts.rs`、`marks/mod.rs`、`marks/native.rs`、`marks/refresh.rs`）：
    - `MarksAt::trailing` 给出各单的状态（按 oid，带币名）和自动刷新最早可能发生的块时间（`RefreshState::due_ms`）。
    - 只有影子盘口的副本接收：快照带全部；之后每次变化发 `FlowAccountsEvent::Trailing`，高度为副本当前高度（在 `Block` 之后，或标记重建后的同一高度）。副本收到其他高度的变化即断开，等下一个快照。
  - 块日志（`pending/blocks.rs`、`record.rs`）：
    - `LoggedBlock.set_global` 记下块里是否有 `SetGlobalAction`；
    - `BlockRecord::marks_unwritten(A, R, due)` 证明 (A, R] 内没有 mark 写入：记录覆盖整段、没有 SGA、块时间都早于刷新截止时刻；A = R 不需证明，A > R 不可用（r3 P2-2）。
  - 副本表（`listeners/order_book/triggers.rs`、`replica.rs`）：每个币维护移动止损的 oid 索引，`TriggerTable::trailing(coin)`。
  - 影子（`pending/trailing.rs` 新增、`shadow.rs`、`mod.rs`）：
    - `TrailingBasis` 是副本在高度 A 的状态；
    - `TrailingAt::coin` 读出某币每张单的状态：副本有的用副本的，副本没有的（A 之后挂的）按 `open` 行；覆盖不成立时每张单都是未知，并带原因；
    - 副本事件、定时 tick、副本盘口结算时重新读取，状态有变的持有币重放。
  - 叠加层（`pending/overlay.rs`、`triggers.rs`）：
    - SGA 时，已激活的移动止损按 best 给出的触发价判定是否越过，与其他触发单同批执行（`TriggerOrder::trailing`，限价用共用的 `trigger_limit`）；批次之后，未触发的单用该 SGA 的 mark 推进（`TrailingStop::advanced`，与 2n-1 同一规则）。
    - 有未知的单可能被越过时，计 `trailing_unknown`：确定越过的单照常离开，相关账户全部记入 `moved`，该币从这条 SGA 起不可判定。之后的动作仍会应用，但不报成交（r3 P2-3）。
    - SGA 到达时，若该币在它的位置已不可判定，则以 `SetGlobalCoin::undecided` 占位持有，不冻结 mark 和仓位，直到它离开（r4）。
  - 发布（`pending/publish.rs`、`servers/websocket_server/pending_book.rs`）：
    - `BboFrame.undecided`：该币的帧发 `{"coin", "status": "unavailable", "reason"}`；
    - 去重键包含这一项，所以最优价不变时，转为不可用或恢复也会发出，新订阅从存储读到的也一致。
  - 内存池：`trailingStop` 动作单独计数（`kind="trailing_stop"`），入块前不建模。
- **线上协议**：`extPendingBbo` 的不可用帧原先只在整本盘口不可用时出现，现在也可以按币出现，原因为 `trailing_unknown`。没有新字段。现有消费方只有主仓研究脚本（`channel_compare.py` 本就按币处理不可用帧）。
- **指标**：
  - `orderbook_pending_book_trigger_coins_total{outcome}` 新增 `trailing_unknown`；
  - `orderbook_pending_book_mempool_actions_total{kind}` 新增 `trailing_stop`；
  - r2 设计列的 `trailing_fired_total` 未单列：被触发的移动止损与其他副本触发单一起计入 `triggers_total{outcome="confirmed"}`，逐笔核对时从录制里区分。
- **测试**：
  - `pending::overlay`：
    - 两条待入块 SGA，110 推高 best、104 触发；
    - 等待中的单激活后触发；
    - 有未知单时整个 run 不可判定：B 离开且不重复触发、第三个账户的 IOC 不报成交，已知时正常；
    - 不可判定的输入。
  - `pending::shadow`：
    - 副本里 best 变了而盘口没有新事件时，币重放；
    - 一个币直到最后一个依赖它的动作离开，才恢复可判定；
    - 副本之上的块写了 mark 时，该币的单记为未知，副本追平后恢复。
  - `pending::tests`：
    - 帧先转为不可用、后恢复都会发出（恢复时最优价没变）；
    - 第二条 SGA 以占位持有；
    - 新订阅读到的与已有订阅一致。
  - `pending::record`：覆盖证明的各种情形。
  - `pending::trailing`：按副本或状态行读取各单状态、未知的情形。
  - `accounts::flow_accounts`：副本的快照、同高度变化、块后变化，以及断开。
  - WS：不可判定币的帧。
  - 代码审查 000662 r1 后（7 条 P2）：
    - 一个块的止损变化随 `Block` 一起发，`Trailing` 只用于标记重建后的同一高度（P2-1）；
    - 主循环监听副本的有效性 watch，每次输入前比较副本的（代，高度），不同就先重读（P2-2）；
    - 副本里 mark 不被认领的币，不按 `open` 行补出已知状态（P2-3）；
    - 行的价格预测读带移动止损的盘口（P2-4）；
    - SGA 按资产表确定涉及的币，不论预测是否成功，都给不可判定的币保留占位（P2-5）；
    - 不可判定之后的 TWAP 子单照常推进盘口、不报成交（P2-6）；
    - 触发表不完整时另行传递移动止损未知（P2-7）。
    - 每条一个用例，变异 7 处都有测试失败。
  - 代码审查 000662 r2 后（4 条 P2）：
    - 影子盘口的副本只在节点 mark 与账户视图同高度时才发布；不在同一高度就撤回，对齐后用完整快照恢复（P2-1）。
    - 已认领 mark 的币集合随移动止损一起发送，同一高度只撤回认领也会更新（P2-2）。
    - 步骤提交前再核对一次副本的（代，高度），变了就重读，丢弃受影响币的待发成交（P2-3）。
    - `sync` 先应用本次输入已知的数据缺口，再重读移动止损（P2-4）。
    - 每条一个用例；P2-4 的用例第一次因 TWAP 未进入已知状态而没走到目标路径，变异未抓到；按审查场景调整顺序后抓到。变异 4 处都有测试失败。
  - 代码审查 000662 r3 后（2 条 P2）：
    - 提交前重读后，不再丢弃受影响币的全部成交，改为按吃单方（动作、TWAP 子单 slot、被触发的单）判断：新版本重放对同一吃单方给出完全相同的成交才发，否则这一吃单方的成交都不发（`Shadow::retain_yielded`）。第一条不可判定 SGA 之前的动作因此照常发出；之后的动作在新版本里不报成交，不发；新版本可判定但成交变了的，也不按旧结果发。只判断本步骤的成交，不补发、不重发历史成交（P2-1）。
    - 因此被撤回的 TWAP 子单从 `emitted` 中移除，之后有效的重放仍会发出一次；同一步内多次重放照旧去重（P2-2）。
    - 原先的撤回用例改为真正依赖未知止损的成交（SGA 之后卖进挂单买盘），并补正向对照：没有移动止损的币的 IOC 照发；切点使 IOC 排在不可判定 SGA 之前时照发。另加：副本换成 SGA 会触发止损的新版本时，旧结果不发；子单撤回时 0 次、恢复时 1 次、再次重放不重复。
    - 重放拆出 `CoinActions::run`，同时供 `replay` 与核对使用；成交的生成与计数分开（`effect_trades` / `count_effect`），核对时不重复计数。
    - 变异 5 处都有测试失败（整币丢弃、全部保留、不撤回去重记录、只认吃单方不比成交、恒保留）。
  - 代码审查 000662 r4 后（3 条 P2）：
    - 输入本身就是丢数（TWAP 流、内存池、块日志的 `Loss`）时，它与 watcher 尚未投递的丢数一起在 `sync` 开头生效，再重读移动止损；原先的 `Loss` 分支不再重复处理（P2-1）。
    - 子单 slot 的去重交给 `SuborderSlots`：步骤内算过的先占位（`computed`），同一步不再算；提交时只有成交帧确实发出，其中的 slot 才记为已发（`told`），其余放回，之后的重放可以再算。发布器返回本步骤的成交是否发出；没有对齐的副本时不发（P2-2）。
    - 被触发单的吃单方加上触发它的 SGA 的 `ActionId`（`Taker::Trigger.set_global`），两条 SGA 行时间相同时不会互相顶替。仅内部比较用，WS 字段不变（P2-3）。
    - 用例：三种丢数各自不报子单成交，无丢数的对照报一次；副本在同一步失效时不报，下一代副本上报一次，之后不重复；两条同时间 SGA，副本换版后由前一条触发，后一条的旧成交不发。
    - 变异 6 处都有测试失败（三种丢数各一处、不管是否发出都记为已发、触发单不带 SGA、提交时不清占位）。
  - 代码审查 000662 r5 后（1 条 P2）：
    - 移动止损重读（`Shadow::on_trailing`，在 `sync`、提交前复核、副本事件、tick 里）只重放，不再当场发子单成交，并记下重放过的币。
    - 提交时（`PendingBook::publish` → `Shadow::tell_held_back`），这些币按输入应用后的最终状态重跑一次：
      - 只在盘口可服务、TWAP 已知、处于成交窗口时，发出既未发出、也未在本步骤算过的 slot；
      - 先追加到本步骤成交末尾，再随整帧排序（见 r6）。
    - 因此同一步里切点前的 IOC 先吃掉卖一时，子单按最终盘口成交，不会超量；输入处理中才发现的缺口（块日志跳号、TWAP 迟到行）使子单不再发出。
    - 用例：恢复与切点前买 1 同一步只报 IOC；买 0.5 报 IOC 再报子单；切点后买 0.5 先报子单；块日志跳号、TWAP 迟到行都不报子单。
    - 变异 4 处都有测试失败（重读当场发子单、提交时不看可服务、子单不按位置插入、提交时不发被扣下的子单）。
  - 代码审查 000662 r6 后（1 条 P2）：
    - 提交时对整帧成交做一次稳定排序（`Shadow::order_trades`）：切点前的动作（保持原序）→ 到期子单（按到期时间、TWAP id）→ 切点及之后的动作（保持原序）。每张单自己的多笔成交保持原序。
    - 子单吃单方 `Taker::Twap` 带上到期时间 `due_ms`，排序用；WS 字段不变。
    - 被扣下的子单先追加，不再逐币找插入位置。
    - 用例：单元测试覆盖三个区段，以及同时到期的 TWAP 7、8（8 有两笔成交）；主循环层覆盖切点时刻的 ETH IOC、切点后 ETH 加 BTC 两张 IOC、切点前对照。
    - 变异 4 处都有测试失败（不排序、子单只按 id、切点时刻算作切点前、不稳定排序）。
  - 变异 10 处，都有测试失败：
    - 未知单被忽略、批次后不推进、不可判定后仍报成交、忽略不可判定的输入；
    - 结算时不重读、副本变化看不到、可用性变化不发帧；
    - 不认 SGA 块、离开的单不发送；
    - 冻结时不留占位（第一次未抓到，补了主循环层用例后抓到）。

## 2026-11-02 移动止损批次 2n-3：账户副本领先盘口副本时也可证明（用户 11-02 “你自己推进吧”）

- **背景**（2n-2 上线验收，研究 hyperliquid-trade 169841c1）：
  - 14 个主站录制币同步出现 134 段不可判定，合计 8.5 s（0.47% 的时间），每段从一条 SGA 行到达、而副本里的移动止损不可证明时开始。
  - 这些时段压掉了 169 笔“及时看到”的真实成交（全部真实成交的 0.46%），以及 HYPE 一条 SGA 触发的 2 张止损。
  - 账户视图已应用高度减副本确认高度，60 次采样中领先 1 块 11 次。设计 000659 r3 P2-2 把领先一律判为不可用（`account_copy_ahead`）。按 SGA 约 40 块一次估算，领先是主因（推断，当时没有按原因的计数）。
- **改了什么**：
  - `BlockRecord::marks_unwritten(copy, replica, due)` 两个方向都按“较低高度之上、到较高高度为止”检查：记录覆盖、没有 SGA、早于刷新截止时刻。副本领先时检查 (R, A]。
  - `TrailingAt` 记下副本是否领先。领先时，副本持有的单按副本的状态读（两者之间没有写过 mark，R 处的状态就是 A 处的状态）；表里有、副本没有的单，是 R 之后才离开的，记为未知（`left_after_replica`），不再按 `open` 行补。副本落后时不变。
  - 新指标 `orderbook_pending_book_trailing_reads_total{result}`：影子每次重读移动止损（副本事件、副本盘口结算）计一次，`proven` 或不可证明的原因。用于下次验收直接看到原因分布。
- **用例**：
  - `pending::record`：副本领先时可证明；领先区间里有 SGA（含 A 那一块）、刷新截止、记录不够都不可证明。
  - `pending::trailing`：领先一块时，副本持有的单读出副本状态，副本没有的记 `left_after_replica`；A 那一块有 SGA 时全部未知。
  - `pending::tests`：副本在 34、盘口确认到 33，SGA 在 100 不触及止损 70，BTC 可判定（旧代码判为 `trailing_unknown`）。
  - 变异 4 处都有测试失败：领先一律不可用、领先时缺单按 `open` 行补、区间漏掉较高那一块、领先标志取反。
  - 设计 + 代码审查 000666 r1 后（2 条 P2）：
    - 副本领先时，“副本持有的单在 R 处的状态等于 A 处的状态”还要求它在 R 之后没被重新设置：同 oid 的第二条 `open`、标记重建，或检查点重装后的新快照，都会把它改成 A 处才有的状态（P2-1、P2-2）。
    - 影子线程的副本改为 `CopiedTrailing`：每张单带上最近一次设置的高度（快照为快照高度，块或同高度的标记重建为该高度），变化的应用逻辑从 `TrailingCopy` 移到这里；`TrailingCopy` 仍是 owner 发出的快照和比对类型。
    - 领先时，设置高度晚于 R 的单记为未知（`set_after_replica`）。检查点重装由新快照或同高度的变化带来，R 追到那个高度前，受影响的单都是未知。
    - 副本落后时（A < R），同 oid 在 (A, R] 里再次 `open` 也会让副本的旧状态失效。bm 11-02 06–09 点开出的 241,656 张触发单（含 110 张移动止损）没有一张同 oid 开第二次，按“无证据不预铺机制”只记录，不处理；这只支持本批暂缓，不是协议保证：若在 (A, R] 实际看到同 oid 被重设，旧副本就不能再作为该单在 R 的依据。
    - `trailing_reads_total` 的 `proven` 只表示区块区间通过检查，币级的未知另看 `trigger_coins_total{outcome="trailing_unknown"}`。
    - 用例：领先一块时块 13 重新设置 70 号单、副本快照就在 A，都记 `set_after_replica`；主循环里块 34 把止损 70 的 best 改成 108（旧代码会在 mark 100 误报一笔触发成交），BTC 不可判定、不报成交；副本按快照加逐块事件构造。
    - 变异 5 处都有测试失败（设置在 R 之后照读、快照设置高度记 0、变化不更新高度、领先一律不可用、领先时缺单按 `open` 行补）。
  - 设计 + 代码审查 000666 r2 后（1 条 P2）：
    - 账户 owner 可以把几块合在一起发布一次 `Block`：止损变化只比较首尾，中间块里设置过、首尾又相同的单看不出来。审查方复现：best 依次为 108、100、108，从 1000 直接发布到 1002，读 1001 拿到 108，在 mark 100 处报出一笔错误的触发成交。
    - 副本收到跨多个高度的 `Block` 时（`trailing: None` 也算），把所有止损的设置高度推进到末块高度（`CopiedTrailing::set_all_at`）。逐块发送时不变。
    - 用例：`flow_accounts` 里经实际 owner 发布两块合一，7、9 号单的设置高度都变为 1003；主循环里副本从 30 一次跳到 34、止损首尾不变，BTC 不可判定、不报成交。
    - 变异 2 处都有测试失败（合并时不推进、逐块也全部推进）。

## 2026-11-02 影子盘口批次 2o：TWAP 子单（用户 11-02 “你按照你的顺序处理吧”）

- **依据**（hyperliquid-trade 研究 `mempool_pending_book_20261031`，`scripts/twap_misses.py`、`twap_tracker_dump.py`；`pending::twaps::real_data` 回放检查点 1206270000，预热 9,000 块后 30 分钟）：
  - 被拒的子单：08–11 点 1,752 张 TWAP 有子单被拒（1,742 张保证金不足），被拒后没有一张再成交，也没有第二条状态；检查点 10:43:51 前一分钟以上被拒的 846 张，没有一张还在它的 twap_tracker 里。HL 撤掉 TWAP 不发结束状态，影子却按还在运行预测：回放里“预测了、实际没执行”745 张，494 张在同一 TWAP 被拒之后。
  - 随机化 TWAP 的下限：1,427 张随机化子单中 391 张的量超过“缺口 + 普通子单的 20%”（随机项最多 +19%），只能来自下限；这样的情况出现两次以上的 23 张 TWAP，19 张每次同一个量（如 HYPE 的 0.14）。
- **改了什么**（`pending/twaps.rs`）：
  - 子单被拒（`TwapEvent::Refused`）等同结束，TWAP 移出（指标照计 `suborder_refused`）。
  - 下限估计也看随机化 TWAP：量超过“目标 + 普通子单的 20% − 已执行”才算证据（普通 TWAP 仍是超过取整后的缺口），同一个量两次即估出；之后子单量取 max(缺口, 下限)，与普通 TWAP 同一规则。
- **回放效果**（同一窗口，改前 → 改后）：普通 TWAP“预测了没执行”745 → 251；随机化量对上 31 → 382，实际 > 预测 683 → 404，实际 < 预测 613 不变（随机项）；实际执行没预测 346 → 274；随机化“预测了没执行”9 → 19（预测了下限，随机项使 HL 该格没发）。
  - 代码审查 000668 r1 后：随机化的证据上界改为一个分数比较（量 + 已执行 > 总量 × (格 + 偏移 + 1/5) / N）；原先目标与 20% 项各自向下取整，上界偏低，纯随机成交也能估出下限（审查方复现：总量 160、17 格、单位 10，前三格各一手，估出下限 10，第 4 格多报）。用例 `a_randomized_reach_is_one_fraction`。
- **未做**：
  - 到期刚过整秒边界的子单（30 分钟 122 张）：HL 在首块时间 ≥ 到期时就本秒执行，首块落在第几毫秒约 0–75 ms 均匀（p50 37），只能按概率预测，取舍待用户定。
  - 追赶上限（23 张）：13 张为 3 × 普通量向上取整，其余不一致，规则未明。
  - 普通 TWAP 下限未及估出（101 张，执行稀疏的 TWAP 要很久才见到两次）；无拒单的“预测了没执行”251 张待查。
- **用例**：`a_refused_suborder_ends_its_twap`；`a_floor_needs_two_slots`（原 `a_floor_needs_two_slots_and_no_randomizing`，随机化也估出）；`a_randomized_floor_is_past_the_randomizing`（随机范围内的量不算证据）。变异 3 处都有测试失败。

## 2026-11-02 影子盘口批次 2p：只减仓被拒、自成交（用户 11-02 “你按照你的顺序处理吧”）

- **依据**（hyperliquid-trade 研究 `mempool_pending_book_20261031`；录制 `after2n3_20261102T1008`）：
  - 预测了没发生的里，“吃单只减仓被拒”319 笔，17 个吃单账户，其中一个账户在 PUMP 上 173 笔。块日志 08–11 点（`scripts/margin_repeat.py --refusal reduce_only`）：只减仓被拒 6,701 次，6,409 次紧接上一次；被拒后同一账户、币、方向的下一张只减仓吃单，之后的块里 5 s 内 93.7%、300 s 内 98.2% 再被拒（同块 98.6%），与 2g 保证金的 98.4% 相当。
  - “吃单方与挂单方同一账户”129 笔，113 张挂单的最终状态全部是 `selfTradeCanceled`，吃单照常成交：HL 撤掉同账户挂单，吃单继续。
- **改了什么**：
  - `pending/refusals.rs`：被拒原因 `Refusal::{Margin, ReduceOnly}`；`TakerOutcome` 带订单的只减仓标记。只减仓被拒只对该身份同币同向的只减仓单生效（`reduce_only_refused_recently`），窗口同 M；任何下单清保证金推断，只减仓单下单（成交、挂上、IOC 落空）才清只减仓推断。
  - `pending/blocks.rs`：`Reduce only order would increase position` 读作只减仓被拒（单张订单自己的状态；整批一个错误时不读，同保证金以外的错误）；`orderbook_pending_book_taker_outcomes_total{outcome="refused_reduce_only"}`。
  - `pending/actions.rs`：`Placement.reduce_only`。触发单执行时按其定量方式（非普通量即只减仓/整仓）给出，只用于标记，不经被拒判断。
  - `pending/overlay.rs`：吃单走到同一账户的挂单（回放的按用户、待入块的按已知账户），撤掉它、不成交、继续往下吃；吃单账户未知时不变。TWAP 子单、触发单同样适用。
- **测试夹具**：回放止损单（`btc_stop_status`）的用户改为 `0x..02`，影子测试 `ACCOUNT` 随之改；overlay 用例里 TWAP 子单和移动止损的主人改为 CAROL。原先它们与挂单同为 `0x..01` / ALICE，新规则下会被当成自成交。signer 用例按真实块订单的 `r` 断言只减仓标记。
- **用例**：`a_reduce_only_refusal_holds_for_reduce_only_orders`、`a_line_reads_its_reduce_only_refusals`、`a_reduce_only_taker_lately_refused_is_refused_again`、`a_taker_cancels_its_own_resting_orders_and_goes_on`、`a_placement_keeps_its_orders_reduce_only_flag`。变异 6 处都有测试失败。
  - 代码审查 000670 r1 后（2 条 P2）：
    - 价位队列顺序未证实时，已知账户的吃单在含自己挂单的价位上，是否撞到自己的单取决于排序（自己排前：撤掉、吃别人的；别人排前：吃满就停，自己的单还在），剩下的盘口不同。现在只要吃单余量 ≤ 别人的总量，该币从这一步起不可判定（`self_trade_unproven`，沿用 2n-2 的按币 `undecided`）；这一笔的成交量与排序无关，照报。余量超过别人的总量时，排序无关，照常处理。TWAP 子单改为在吃之前看是否已不可判定，不让它自己引起的不可判定吞掉它自己的成交。
    - 98.2% 是研究脚本按回执账户、自己的批量错误口径算的，与线上规则（可吃单项、逐项回执、vault 或签名人身份）不同。新增 `pending::refusals::real_data::real_refusals_repeat`（忽略的真实数据用例）：用 `parse_block` 读真实块日志，按确认滞后 2 块的推断逐张判断，再与该块的实际结果比对。按订单所在块的时间判断（线上按行到达时间），只计逐项回执能分类的吃单，“放行”含 IOC 落空。bm 11-02 09:57–11:09（6 个块日志文件，连续）：判为只减仓被拒 3,742 张，实际只减仓被拒 3,513（93.9%）、放行 229；判为保证金被拒 30,960 张，实际 30,205（97.6%）；只减仓被拒但未判出 810 张（多为一轮的第一次）。先前未查连续性的 09:45–10:57 一次为 3,246 / 3,303（98.3%），两段有 5 个文件重叠；最早的文件已被节点清理，不能补查。
  - 代码审查 000670 r2 后（3 条 P2）：
    - 匿名成交后的归属：顺序未证实的价位被吃单吃了一部分（吃单在此用完），剩下的单属于谁不知道，记为归属不明（`blurred`）；之后已知账户、在该价位有自己挂单的吃单到来，直接判不可判定，不再用 `left` 的某一种排列算自己与别人的剩余。没有自己挂单的吃单照常。
    - 同一次 SGA 的批次：以单张触发单为报成交的边界，执行前该币已不可判定（由同批前面一张引起）的触发单不报成交、结果记为该原因，持仓照常按实际成交推进，仍离开未触发表。
    - 离线核验：块高不连续（断档、重复）时按线上块日志缺口清空推断并计数；读不出高度的行报文件与行号后停止。数字的含义写明：固定滞后 2 块、按块时间判断、“放行”含 IOC 落空，放行数不等于漏掉的成交，也不归因于反方向开仓。
    - 用例：`a_self_trade_after_a_level_taken_part_of_leaves_the_run_undecided`（DAVE 先吃一半，ALICE 再买，两种排列；CAROL 无挂单照常）、`a_trigger_order_after_one_turning_the_run_undecided_reports_no_fill`（一次 SGA 触发 ALICE 1、DAVE 2，两种排列）。变异 3 处都有测试失败。
    - 测试夹具里新旧两条账户说明合为一条（P3）。
    - 不同地址之间的自成交撤单（OB PROVE 样本，区块 1192106175）不在本批，单独处理。
  - 代码审查 000670 r3 后（1 条 P2）：
    - r2 的 `blurred` 只拦住“之后的吃单在该价位有自己的单”。但价位被吃掉一部分后，剩下哪几张单本身就取决于排序，之后任何吃单在该价位的成交量和价格、按 oid 撤单找不找得到、BBO 的订单数都会随排列变。
    - 改为：顺序未证实的价位上有两张以上回放挂单，被一张吃单吃掉（或撤掉）一部分后还有剩余时，该币从这张吃单之后不可判定（`unproven_level_part_taken`）。这张吃单本身的成交（一笔，不报挂单方）与排序无关，照报。待入块挂单在这种价位一律排在回放挂单之后，顺序已知，不计入。`blurred` 删除。
    - 线上频率：2o 录制 30 分钟 127,046 笔预测成交里，落在顺序未证实价位的只有 15 笔，按币不可用的代价很小。
    - 用例：
      - `a_level_of_unproven_order_taken_part_of_leaves_the_run_undecided` 替换 r2 的用例：两种排列下，DAVE 吃一半之后，ALICE 限价 101、ALICE 限价 102、CAROL 都不报成交；
      - `a_level_taken_whole_or_of_one_order_or_proven_leaves_the_run_decided`：整档吃完、只有一张回放挂单、顺序已证实，三种对照；
      - 2f 用例 `no_priority_an_order_taken_out_and_an_unproven_level` 改为：先吃 2（整档回放单）再吃 1，两笔都报；先吃 1 之后的吃单不报；吃 1 后按 oid 撤单再吃，也不报。
    - 变异 4 处都有测试失败。
  - 代码审查 000670 r4 后（1 条 P2）：
    - 顺序未证实的价位上，待入块 ALO 一律排在回放挂单之后，这只是 2f 的估计。带优先费的 ALO 可能越过 400 ms 内费更低的回放挂单，实际排在前面。只有一张回放挂单加一张这样的待入块单时，吃掉一部分后剩下哪张同样不确定。
    - 改为按合并队列判断：吃单前合并价位上多于一张单、吃完仍有剩余，就不可判定。待入块单都计入，不区分是否带费，只多拦这种本已罕见的情况。
    - 用例 `a_level_of_unproven_order_taken_part_of_with_a_pending_order_leaves_the_run_undecided`：ALICE 回放卖 2，BOB 待入块 ALO 卖 1，费 6。DAVE 买 1 报一笔匿名成交，之后不可判定，ALICE 的 IOC 不报；DAVE 买 3 吃完整档，仍可判定。
    - 2f 用例改为：整档吃 3 报一笔匿名；先吃 1 之后不报。原来“先吃 2 再吃 1”的那张 ALO 带费 5，同样可能排前，现在第二笔不报。
    - 变异 5 处都有测试失败，其中一处是“只数回放挂单”。

## 2026-11-02 影子盘口批次 2q：到期刚过整秒的 TWAP 子单按较大可能预测（用户 11-02 “按照概率吧”）

- **依据**：HL 在“块时间不早于到期”的整秒首块执行子单，可首块的毫秒数出块前不知道。bm 11-02 的块日志共 11,863 秒，每秒首块落在整秒后的毫秒数 p10/p25/p50/p75/p90 为 7/18/35/53/67。首块不早于整秒后 35 ms 的占 51.0%，不早于 40 ms 的占 43.5%。
- **改了什么**：`pending/twaps.rs` 新增常量 `LIKELY_FIRST_BLOCK_MS = 35`。子单到期不晚于下一个整秒后 35 ms，就预测它在那一秒的首块执行；原来要求不晚于整秒本身。首块若早于到期，子单在后一秒执行，影子盘口应用那个首块之后会在后一秒重新预测它，这条路径不变。
- **回放**（`pending::twaps::real_data`，检查点 1206420000，预热 9,000 块后 30 分钟，1,793 个首块）：实际执行但没预测 462 → 213，预测了没成交 30 → 124，量也对上 6,015 → 6,219。
- **用例**：`a_slot_due_at_or_just_past_the_second_is_predicted_in_its_first_block` 替换原来的 `a_slot_due_at_the_second_runs_in_its_first_block`，到期分别为整秒、整秒后 1 ms、35 ms、36 ms；首块恰在整秒时，后三张都在后一秒预测。变异 2 处都有测试失败：去掉放宽，或把“不晚于”改成“早于”。
- 代码审查 000675 r1 后（P1、P2 各 1 条）：
  - P1：同一格子单从一秒顺延到下一秒时，子单本身不变，原来不会重放；可切点跟着秒变了，待入块单与子单的先后可能翻转。现在进入新的一秒时，持有子单的币全部按新切点重放（`Shadow::due_anew`）。用例 `a_suborder_going_on_to_the_next_second_is_replayed_at_its_cut`：子单到期 4,035 ms，ALO 卖单 3,990 ms 到达。首块 4,000 ms 早于到期，子单顺延；两路证明先于副本到齐，卖一从 1 @ 100 重放成 0.9 @ 100。
  - P2：首块时间正好等于到期、子单只成交一部分时，下一秒原来还会按同一格预测剩余量。现在 `Twap.ran_slot` 记下成交所在的格，首块正好在该格到期时间时跳到下一格。用例 `a_slot_run_in_part_at_its_due_time_is_not_predicted_again`：成交 4 / 10 之后下一秒不再预测，slot 1 预测 20 − 4 = 16。
  - 回放重跑：只有“预测了没成交 / 随机化”从 45 变为 44，其余相同。变异：去掉新秒重放、不跳过已执行格、不记录已执行格，各有用例失败。
- 代码审查 000675 r2 后（P2、P3 各 1 条）：
  - P2：安装检查点后，已执行的格会重新出现在影子盘口里。原因是 `ran_slot` 只来自实时成交，检查点种子为 None，而锚点只重放高度大于它的事件。改为 `Twap.next_slot`，即最早还没执行的格：实时成交格 k 把它推进到至少 k + 1；检查点按其 `n` 所在的格初始化；选格时取首块时间对应的格与它中的较大者。用例 `a_slot_run_in_part_at_its_due_time_is_not_predicted_again` 对比四条路径：连续运行、高度 20 的检查点替换、高度 25 的检查点替换、首次从高度 20 的检查点跟踪。同一高度下一秒都不再预测 slot 0，slot 1 都预测 16。变异 3 处（种子不初始化、成交不推进、选格不取较大者）都有用例失败。
  - P3：README 改为本轮口径——预测了没成交 30 → 123；进入新的一秒时，持有子单的币全部重放。

## 2026-11-02 影子盘口批次 2r：IOC 的金额上限按市价单那一档

- **依据**：
  - 录制 `after2o_20261102T1223` 里，HYPE 一张只减仓 IOC 卖 38,387.2 @ 76.655（约 $2.94M），HL 以 "Order value too large. Max is $2000000." 拒绝。2e 把 IOC 按限价单放宽到 10 倍上限，没有判它被拒，影子盘口预测出 591 笔不存在的成交。
  - hyperliquid-trade 研究脚本 `value_cap.py` 扫 bm 11-02 的块日志（3.3 小时），主站每个资产按最大杠杆档位算上限，与被拒消息里的上限全部一致。超过上限的单：IOC 1 张，被拒；市价单 5 张，全部被拒；ALO 106 张挂上；GTC 1 张成交（都在 10 倍以内）。没有一张超过上限的 IOC 被放行。
- **改了什么**：`pending/actions.rs` `too_large` 中，IOC 与 `FrontendMarket` 一样用市价单上限；GTC、ALO 仍为 10 倍。
- **用例**：`an_order_worth_too_much_is_refused_alone` 增加 IOC 255（$510k，现在被拒）、IOC 227（放行）和 GTC 255（放行）、GTC 2501（被拒）。
- **局限**：IOC 只有 1 个样本，而且是只减仓单。市价单不论是否只减仓都被拒，所以只减仓不豁免。
- 代码审查 000677 r1 后（1 条 P2）：
  - 问题：IOC 的 1 倍上限原先也作用于改单（`modify` / `batchModify`）的替换 IOC。`always_place` 的改单若被提前判拒，会连带跳过撤旧单。而 HL 拒绝这种改单时旧单是否还在，没有样本。
  - 改法：只有新下的 IOC（`BookOp::Place`）用市价单上限；改单的 IOC 保持 2e 的 10 倍。
  - 用例 `a_modify_to_an_ioc_keeps_the_limit_order_cap`：两种 `always_place` 下，改成 $510k 的 IOC 都放行，改成 $5.002M 的拒绝。变异“改单也用 1 倍”时该用例失败。

## 2026-11-02 账户视图：影子盘口副本的移动止损只在 mark 状态有新修订号时才重读（线上故障）

- **现象**：bm 11-02 起，账户视图出现 `pending_overflow`：12:44（01167ab）、13:37（6472eb9）；14:30 之后每 12–15 分钟一次（14:30、14:31、14:43、14:55、15:09、15:24）。每次都撤回强平流，要到下一个检查点重新播种才恢复。溢出前 `block_actions` 落后约 250–300 块，此时其他输入流积压超过 100 万条上限（`MAX_PENDING_INPUTS`）。
- **根因**：`accounts-owner` 线程在 15:30–15:33 连续三个 10 秒窗口都是 100–102% CPU。perf 采样 15 秒，约 40% 花在移动止损副本上：
  - `TrailingCopy::changes` 8.8%；
  - `MarksAt::trailing` 7.6%；
  - 以币名字符串为键的表重建、扩容、比较和析构，合计约 22%；
  - 另约 27% 是强平候选的 `reevaluate` / `reindex`，开盘后随成交量上升。

  `FlowAccountsFeed::on_applied` 在每条输入后都会调用（每块约 7 条），高度未变时走 `send_trailing`，把全部移动止损（约 300 张）和全部 claimed 币名（约 230 个）重建一遍再整体比较，绝大多数时候没有变化。这是批次 2n-2 / 2n-3 带进来的成本，美股开盘后强平候选的计算量上来，主线程就跑满了。
- **改法**：
  - `MarksAt::revision`：`NodeMarks` 在每应用一块（`apply_block`）、每装一次种子（`install`）之后，给状态一个新的修订号（`stamp_revision`）。
  - 影子盘口副本的 `Sent.trailing_revision` 记下上次读移动止损时的修订号。高度未变时，修订号没变就不读、不比较；新块照旧，每块读一次。
- **用例**：
  - `the_state_takes_a_revision_at_each_block_and_seed`：应用一块和装种子都会换修订号，种子回放到同一高度时也换。
  - `the_pending_books_copy_keeps_the_trailing_stops`：同一高度变更止损但修订号未变时，不读不发；换了修订号后才发 `Trailing`。
  - 变异 3 处都有用例失败：装种子不换号、应用块不换号、不看修订号。

## 2026-11-02 账户视图：落后时强平候选合并刷新（用户 11-02 “先搞你这个推荐先做的吧”）

- **依据**：修好移动止损副本（928b588）后，15:55 的 perf 中，`accounts-owner` 的 15 秒采样里：
  - `Candidates::reevaluate` 31.7%，`reindex` 11.3%，`refresh_candidates` 7.1%，排序约 4%，合计过半；
  - 线程三个 10 秒窗口 CPU 为 61% / 64% / 84%。
  原来每条输入之后都刷新一次候选（新块到来时全量重算）。主线程一落后，积压的每一块各算一次，结果在下一块就过时，于是越落后越慢，直到 `pending_overflow`。
- **改了什么**：
  - `runtime.rs` 新增 `DeferredRefresh`：每应用一条输入，就把它改动的行并入暂存；只有输入队列为空时才刷新候选，一次用上全部暂存的行。
  - 视图或 mark 不可信时（`candidates::trusted` 为假）立即刷新，即立即撤回候选。
  - 检查点安装分支和每秒一次的巡检分支，都带着暂存的行刷新。所以持续积压时，刷新最晚也在下一个巡检时发生，`select!` 在就绪分支间随机选取。
  - 不落后时行为不变，仍是每块刷新一次。
- **用例**：`the_candidates_refresh_once_the_queued_inputs_are_applied`。变异 3 处都有失败：不看是否可信、从不延后、暂存不清空。
- 代码审查 000680 r1 后（2 条 P2）：
  - **P2-1 盘口撤回**：扫盘所用的盘口变得不可信、或撤回代数变化时（`Candidates::book_moved`，与 `walk_book` 判断依据相同），立即刷新，已发布的扫盘估价马上标为 `untrusted`；不可信的账户视图或 mark 照旧立即刷新。
  - **P2-2 延后上限**：`DeferredRefresh.since` 记下第一条被延后的输入，满 `MAX_REFRESH_DEFER`（1 s，即 README 写的 1 秒）就刷新，不再依赖 `select!` 的随机调度。
  - **说明**：合并刷新时，两次刷新之间进入又退出的候选不会被发出，README 已写明。
  - **用例**：`the_book_moving_is_seen_until_a_refresh_takes_it`（撤回、撤回后恢复且代数变化、未接盘口）；`the_candidates_refresh_once_the_queued_inputs_are_applied` 加了可控时钟，第 1,000 ms 刷新、第 999 ms 不刷。变异 4 处都有失败：`book_moved` 恒为假、去掉时间上限、忽略立即刷新、刷新后不清起点。
- 代码审查 000680 r2 后（1 条 P2）：延后已满 `MAX_REFRESH_DEFER` 时，主循环若先选中查询或扫盘结果，原来会先用上一次确认的候选去服务。现在这两个分支处理之前先检查 `DeferredRefresh::overdue`，到期就先刷新。输入分支照旧在应用输入后检查。用例加了 `overdue` 的边界：无暂存时为假，满 1,000 ms 为真、999 ms 为假。

## 2026-11-02 影子盘口线程性能：待入块挂单按 cloid / 触发单 oid 建索引，未证实价位按币合并检查

- **依据**：
  - 用户的 forced flow 会话看到，账户数据在 16:00:20、16:10:25 过期约 4 s。同一时段影子盘口副本因 `queue_full` 成串重建：15:58 有 26 次，16:00 有 11 次，16:10 有 17 次。每次重建，监听器都在持锁时 `fork`，58 次平均 0.22 s，最长约 0.5 s。
  - 16:15 一串重建期间，pending-book 线程 CPU 为 90–118%。perf 显示：逐单遍历待入块挂单（`resting_where`）约 22%，`has_level` 6.2%。
- **改了什么**：
  - `pending/overlay.rs` 新增 `RestingIndex`，含 `by_cloid` 和 `by_trigger_oid`。在挂上、被成交或自撤出队、撤单或改单移除这三处同步维护。`cloid_target` 和 `oid_target` 的兜底改为查索引，`resting_where` 删除。
  - `listeners/order_book/state.rs` 的 `apply_order_diffs_hft`：Remove 和改量为零只登记币，并且只登记有未证实价位的币（`UnprovenLevels::holds`）。同币下一条 New 之前先结算（审查 000682 r1 P2：避免合批与拆批时挂单方归属不同），批次末尾再处理剩下的币。
- **用例**：
  - `the_resting_orders_index_follows_them`：每步都与重建出的索引比对，并检查同一 cloid 两边挂单时为 `Ambiguous`、自撤后为 `Missing`。
  - `a_removal_then_a_crossing_new_leave_the_marks_alike_however_batched`：一批与拆两批的结果对照。
  - 变异 6 处都有失败，另有一处 `holds` 恒为真只影响开销。审查方用 3,000 个动作重放，做了 90,000 次 cloid 查找，与逐单扫描全部一致。

## 2026-11-02 账户副本跨检查点不换代：什么都没改变的检查点沿用原代

- **依据**：
  - 每次装检查点（约 12 分钟一次），账户视图都会给强平流和影子盘口的副本重发整份快照并换一代。强平流线程一看到换代，就把所有币标为不可用（heights 全为 0，原因写“the account view was sent anew”），交易器随即撤掉接货。用户的 forced flow 会话因此看到每约 10 分钟撤一次。
  - bm 11-02 全天装了 93 次检查点。做过重放比对的 75 次里，63 次所有行完全一致、没有悄悄变化的持仓，12 次只有 1–29 行属于估算；其余 18 次是重启后的第一次，或者丢块之后，没有比对。
- **改了什么**：
  - `book.rs`：`InstallReport` 新增 `touched`，记下这次安装改动的持仓，即全部悄悄变化，以及被重放到已上市币的挂起成交；新增 `universe_changed`，有币退出、有新币加入，或精度改变时为真。
  - `runtime.rs`：`install_ready` 返回 `Installed { continues }`。只有币种没变、没有改动任何持仓、并且有旧检查点时，`continues` 才是 `(旧, 新)`。两个装检查点的入口（输入分支和锚点分支）都调用 `continue_installed`。
  - `flow_accounts.rs`：`FlowAccountsFeed::continue_anchor` 在副本正站在旧检查点上时，改为站在新检查点上，有一个例外：副本高度等于视图高度、且副本持有的某个 mark 已不同于视图（随检查点装上的种子修正了它）。这时同高度没有块事件来带新 mark，所以照旧换代。视图高度高于副本时，接下来的块事件会带上所有变动的 mark，包括种子修正的那些。
  - 只要有任何修正（持仓、币种），就照旧整份快照、换一代。
- **审查 000687 r1**：第一版对有修正的检查点也续接，用同高度的持仓修正事件补发。审查复现了三个问题：同高度漏了种子修正的 mark；持仓修正后，候选要等更高的块才重新确认；修正前发出的扫盘晚到时，仍按旧持仓发布。改为“只在什么都没改变时续接”后，同高度增量协议整个删除。
- **用例**：
  - `an_install_continues_the_copies_unless_it_changed_a_coin_or_a_position`：没有变化时续接；悄悄变化、出现新持仓、新币加入、币退出、精度变化时都不续接。
  - `a_copy_stands_on_through_a_checkpoint_that_changes_nothing`：同高度续接时不发任何事件，仍是第 1 代；视图高度在前时，块事件带上变动的 mark；同高度 mark 被修正时换成第 2 代。
  - 改写 `an_anchor_waits_for_its_blocks_and_a_newer_one_replaces_it`：续上的是 (1000, 1003)。
  - 变异 4 处都有失败。
- **审查 000687 r2**：影子盘口的副本还带着每张移动止损的 `set_at`。检查点可能先修正较早一块的止损，再重放到当前块，结果和原值相同；这时副本收不到更新，旧的 `set_at` 会让影子盘口把后一块的值当成早先已证实。所以只让强平流的副本续接，影子盘口副本照旧换代（它不面向交易器，没有撤接货的问题）。用例补上了影子盘口副本换代的对照；变异“影子盘口也续接”有用例失败。
- **不在本批**：装检查点时强平候选仍整体重建，约 0.5–1 s。

## 2026-11-03 强平流帧账户数据过旧按段记日志

- **依据**：交易器拒收 `accountsTimeMs` 落后本机墙钟超过 4 s 的帧。11-03 03:01:07–10 拒了 4 次（KAS、kBONK），节点当时最多落后 1.14 s，其余约 3 s 出在 OB。当时没有记录，分不清卡在哪里。
- **改了什么**：
  - 新增 `accounts/flow_staleness.rs` 的 `FlowStaleness`，按发布时刻的墙钟检查强平流每一帧的账户数据有多旧。
    - 第一帧超过 1 s 时开一段。
    - 段内最后一帧过旧之后至少 1 s，再出现回到 1 s 以内的帧，这一段才结束，结束时记一行 INFO。
    - 不同时刻取数的帧会在同一步内、也会在相邻步之间交错（审查 000693 r1：同一时刻 200 张新旧交替的帧结束了 100 段），有了这 1 s 静默，每段只记一行，最多每秒一行。
    - heights 全为 0 的帧（副本不服务、还没有盘口）不开段，也不结束段。
  - 日志写：
    - 段的起点和最后一帧过旧的时刻（UTC，到毫秒）、时长、帧数；
    - 段内账户数据最旧的那一帧：币种，以及它输入的三个年龄，各带块高：accounts（持仓与候选确认块中较旧者）、positions、book（`listenerTime`）；
    - 这一帧发布时副本自身的 accounts、positions 年龄；
    - 段内 book 的最大年龄。
  - 年龄口径（审查 000693 r1 P3-1）：输入是计算开始时读取的，所以帧的三个年龄都包含强平流自己的计算和扫盘等待，两两之差不能直接算作某个线程的落后。发布时副本自身的年龄可以作对照：副本比帧的输入新，说明差距在强平流这一侧。
  - `forced_flow_thread.rs`：每一步的帧经 `FlowSink` 发布，发布前先交给 `FlowStaleness`；只有本步有帧时才读副本的块。`runtime::now_ms` 改为 crate 内可见，复用同一个墙钟函数。
  - 日志不在强平流线程上写（审查 000693 r1–r2 P2-1：`env_logger` 在调用线程同步写 stderr，写入受阻时会拖住帧的发布）。结束的段 `try_send` 进容量 16 的队列，由专门的系统线程 `flow-spell-log` 用 `blocking_recv` 取出后写日志；不放在主运行时的任务里，以免写入受阻时占住承载网络和盘口读取的工作线程（审查 000693 r3）。队列满或任务已不在时丢弃这一行，计入 `orderbook_forced_flow_stale_spells_dropped_total{cause="full"|"closed"}`。
  - 1 s 的线取自 bm 11-03 08:45Z 实测：120 s 内 64,240 帧，p50 395 ms，p99 780 ms。
- **用例**：
  - `a_spell_runs_from_the_first_stale_frame_to_one_back_within_a_second_after_the_last`（含日志原文）；
  - `frames_of_inputs_old_and_new_interleaved_keep_one_spell`；
  - `a_frame_without_account_data_neither_opens_nor_ends_a_spell`；
  - `the_clock_reads_utc_to_the_ms`；
  - `a_full_spell_log_drops_the_line_and_the_frames_go_out`：队列满时丢一行、计数加一，4 张帧照样发出。
  - `a_log_write_waiting_holds_neither_the_frames_nor_the_runtime`：单工作线程的运行时上，写日志卡住时，别的任务照样能跑，后续帧照样发出。
  - 变异 11 处都有失败：把日志写入改回主运行时任务后，上一条用例 0.5 s 内失败。

## 2026-11-04 基差短中长三档 + 样本存 Redis 撑过重启（hyperliquid-trade 设计 000741 r2，用户 11-04 定）

- 用户：“OB 服务借助 redis 处理重启之类的，然后同时提供短中长基差。”
- `external_basis.rs`：
  - 新增 `LongBasisWindow`：每 `LONG_SAMPLE_INTERVAL`（10 s）取一个样本，用的是和 10 分钟窗口同一拍的原始样本，保留 240 分钟；60 分钟和 240 分钟各算一个中位数，各自满 `LONG_MIN_SAMPLES`（60 个）才发布。
  - 中位数和“发布 / 撤回”规则提成共用函数，所有窗口用同一套。
  - 每个窗口新增 `take_entered`（交出这一拍取的样本，只交一次）和 `restore`（把上一次运行的样本放回）。
- HIP-3 币安腿：
  - `Hip3BinanceLeg { leg, longer }`、`BinanceBases { near, longer }`；
  - `extHip3ExternalQuotes` 的 `external` 在原有字段之后追加 `externalBasis60m`、`externalBasis240m`，格式和 `externalBasis` 一样；
  - 不升 schema：交易器只拒收不认识的版本号，不拒收新字段；
  - 报价节拍的帧指纹带上这两个长基差，长基差变了也会重新发帧。
- `basis_store.rs`（`--basis-store-redis`）：
  - 序列名：`main:<币>`、`venue:<币>:<交易所>`、`hip3:<币>:binance`、`hip3:<币>:<sip|boats>`；
  - 存法：一个时间段一个 hash，每个序列一个字段，值是 `"<样本墙钟毫秒> <比值>"`；`HSET` 和 `EXPIRE` 放在一个 `MULTI` 里，过期时间是窗口长度加 60 s；
  - 不同币在同一秒里不同拍取样，各自一个字段，互不覆盖（r1 P2）；
  - 写入：后台任务处理，有界队列容量 30，满了就丢弃并计数；
  - 恢复：在 owner 任务里、第一次发布之前进行，5 s 内完成，只放回仍在各自窗口时间内的样本；Redis 不可用就从空窗口开始，并打 WARN。
- 测试：
  - 长窗口：取样节奏、两个跨度、样本老化；
  - 存储：两个序列错开取样后恢复，样本数和中位数都和内存一致；格式、过期时间、读取范围；队列满丢弃；Redis 不可达；
  - 用真实币种表跑 300 s、恢复后继续跑，结果一致；
  - 真实 Redis 往返测试默认跳过，设 `OB_BASIS_STORE_TEST_REDIS` 才跑。

## 2026-11-05 基差录制：每 10 s 每个币一行，长期落盘（hyperliquid-trade 设计 000751 r1–r3，用户 11-05 定）

- 用户：“我们也扩展下 orderbook 这块吧，也录一下这些：bn 和 hl 口径的 oracle、mark、指数、标记，以及我们自己口径算的中点的 10 min / 1 h / 4 h 基差”；“主站币也录吧，完整哈”；“HYPE 这个币也录一下吧”；“还有当时 usdc usdt 汇率”“当时 pyth 的 usdt/usd、usdc/usd”；“新增 zstd 依赖吧”。
- 新模块 `local_marks/basis_record/`，只录不发布，开关 `--basis-record-dir`、`--basis-record-days`、`--basis-record-pyth-token-env`（三个一起给）：
  - `mod.rs`：启动校验（目录可写、天数 ≥ 1、令牌非空，令牌不进日志和 `Debug`）；每 10 s 墙钟对齐的取样任务：短暂借用快照、复制 `Arc` 后释放，读各交易所行情读数、HYPE 盘口，组好一拍的行交给写入线程。
  - `row.rs`：一行一个币（主站全部、HIP-3 外部报价频道全部、HYPE）。各交易所价格一律原生尺度加 `px_multiplier`（r1 P2，直接取行情读数，不从快照的 HL 尺度倒推）；没有的值记 `{"code","detail"}`，不用 0 或旧值；每个值带自己的时间。
  - `binance.rs`：录制自己的连接：`/market/stream` 的 `@markPrice`（266 个合约，每连接 100 路）、HYPEUSDT 永续和 USDCUSDT 现货 `bookTicker`。断线按 local marks 的退避重连、保留上一份；契约错误停这一路、此后记 `stopped`。不进 local marks 的行情监督。
  - `pyth.rs`：每 10 s 请求一次 Hermes `latest`（USDT/USD、USDC/USD），Bearer 只放请求头；瞬时失败计数、每分钟最多一条 WARN；其他状态和契约错误停掉（r3 P3）。
  - `writer.rs`：单独线程，队列 3 拍，满了丢弃并计数；按 `t_ms` 的 UTC 日期写 `.jsonl`；启动和换日时把更早的天压成 `.jsonl.zst.tmp`（zstd 9）、解压逐字节比对一致后改名 `.jsonl.zst`、再删原文件，失败保留原件下次再收尾（r1 P2）；保留期外的 `.zst` 和遗留 `.jsonl` 删掉。
- 其他改动：
  - 快照带上这一拍 owner 读到的 `BestPricesAt`（`with_book_prices`，基差样本就从它取），新增 `hip3_node_state`；
  - `RestClient::call_authorized`（Bearer 头，错误信息不含令牌）；
  - `connection.rs` 的 `connect_error`、`read_error`、`close_error` 和 `feeds.rs` 的退避常量改为 `pub(crate)` 供录制复用；
  - `VenuePrice::ONE`；
  - 指标 `orderbook_basis_record_{ticks_total,dropped_total,errors_total,stream_disconnects_total,stream_stopped,pyth_errors_total}`；
  - 新依赖 `zstd = "0.13"`（交易系统同版本）。
- 测试：标记价 / bookTicker / Hermes 帧解析与契约错误；倍率还原（kNEIRO 1000）；主站、HIP-3、HYPE 三类行的字段与缺失记法；分天写、换日收尾、跨午夜停机后启动收尾、压缩失败后再收尾、保留期、崩溃遗留 `.tmp`；队列满丢弃不阻塞；启动校验与令牌不外泄。

## 2026-11-05 读节点输出改为纯轮询，去掉 inotify（hyperliquid-trade 设计 000767 r1，用户 11-05 定）

- 起因：11-05 美股开盘时，bm 的 hl-node 落后 35–60 分钟。perf 测节点主线程：CPU 86%，其中 34% 在内核里，主要是写文件触发的 `fsnotify` / `inotify` 通知和唤醒。节点开着 `--disable-output-file-buffering`，每秒约 10.5 万次逐行 `write`；OB 的 15 个跟读线程共 57 个 inotify 监听，每写一行都要唤醒它们一次，这笔开销算在节点的写线程头上。用户：“纯轮询吧”。
- `spawn_line_watcher`（所有节点流共用）：
  - 不再用 inotify：删掉 `recommended_watcher`、`NoticeGate`、`notice_sender`、`notified_file`、`stream_file`，以及通知溢出记丢失那条路（`Losses::flag_found` 并入 `flag`）；去掉 `notify` 依赖。
  - 读取：一次读到数据就立刻再读（积压按 `READ_CHUNK_BYTES` 分段读完），读到末尾或没读成就睡 `POLL_INTERVAL = 1 ms`。
  - 找文件：每 `FILE_CHECK_INTERVAL = 10 ms` 一次（`FileReader::check_files`，替代原来的“新建”通知和约 10 s 一次的 `periodic_check`）。
    - 启动时：跳到流里按布局顺序**最后一个**文件的末尾开始跟（Adopt，`start_skip_is_loss` 照旧在这时报）。较新的日期或会话目录是空的，不挡住更早目录里的文件（审查 000769 r1 P2-2：原来只看最新目录，会把有文件的流当成空流，从头重读启动前的文件）。原来小时文件取最新日期里修改时间最新的那个，现在和按顺序切换统一按顺序取：如果跟一个较早的文件，排在它后面、启动前就已存在的文件会被从头读。
    - 列目录确认整个流没有文件：之后出现的第一个文件从头读。列目录或查条目类型失败（不存在之外的错误，审查 000769 r2 P3：`is_file`/`is_dir` 会把查不了当成“不是”）既不算有文件也不算空流，下次检查再列；失败按一段记一次日志并计数（`orderbook_node_file_io_errors_total{operation="list"}`）。
    - 跟踪中：先看同一路径是否重新创建（inode 变了），是就先切过去，读完旧句柄再从头读重建的文件；然后才按布局顺序取当前文件之后的下一个文件（小时文件按日期、小时数；replica 按会话、日期、起始高度）（审查 000769 r1 P2-1：原来先取后继，重建的文件会被跳过、也不记丢失）。一次一个，读完旧文件再切（`on_create` 的读完和记丢失规则不变），不再像原来的兜底检查那样直接跳到最新文件、跳过中间的文件。
    - 下一个文件之前的目录读不了，这次就不切换，不跳过它的文件。
  - 语义变化：原来在第一次收到修改通知时才定位到文件末尾，会连触发通知的那一行一起跳过；现在启动时就定位，之后写入的行都会读到。
  - read-through 证明（000433）：“跟踪的是最新文件”改为“跟踪的是按顺序最后一个文件”，列目录失败不证明。读取和按行切分、行格式检查、残行上限、丢失与重新同步、通道背压都不变。
- 代价（估算）：读到数据平均晚约 0.5 ms，空闲时最多晚 1 ms；新小时文件的第一批行最多晚 10 ms；OB 空闲时每个线程每秒读 1000 次、每 10 ms 扫一次目录。
- 测试：去掉通知闸门和真实 inotify 的测试；第一次检查跳到末尾、较新目录为空时仍跳到最后一个文件末尾（小时和 replica）、列目录失败不算空流、没有文件时第一个文件从头读、多个新文件按顺序逐个读完（含跨天）、同一路径重建和后继同时出现时依次读旧尾、重建文件、后继、replica 按会话切换、临时目录不抢游标、跟踪的文件被删时由切换记丢失；跟读线程端到端的测试改为启动后写入的行会读到。`accounts/runtime.rs`、`pending/mempool.rs` 两个依赖旧定位方式的测试同步去掉了用来触发定位的那一行。
