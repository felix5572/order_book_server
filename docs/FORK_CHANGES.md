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
