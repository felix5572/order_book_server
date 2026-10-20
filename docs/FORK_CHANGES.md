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
- **接口**: `GET /coinPositions?coin=`、订阅 `coinPositions`(snapshot / changes / withdrawn, 按币 version;
  只给有订阅的币生成帧, 走独立广播, 不占盘口通道; 积压或断号即撤订阅)。见 README "Account view"。
- **验收工具**: `accounts_checkpoint_probe`(单份 checkpoint 全部仓位)、`accounts_follow_probe`(独立跑视图并定时落盘,
  供主仓库 `follow_vs_node.py` 对照节点)。
- **罕见事件留证**(000425): 非 market 清算方式、没见过的成交方向 / 账本类型、未知 misc 事件各打一行
  `ACCOUNTS_EVIDENCE <json>`(WARN, 带原始行), 锚点与成交推出的数量不一致记 `silentPositionChange`(每次装锚点最多
  100 条); 计数 `orderbook_accounts_evidence_total{kind}`。`GET /coinPositions` 按 `Accept-Encoding` 返回 gzip。
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
  (→ `withdrawal`), 账户取回执 `user`(带 `vaultAddress` 时即子账户 / 金库)。每块都有一行, 块高跳号按丢到最后一个
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
  账户的 `borrowLend`、未读的账本类型、非 USDC 抵押、现货非 USDC 计价等)让该账户停止回放, 到下一个锚点恢复。原来只记
  "谁在哪块被标记"的日志改为完整的账户事件日志, 标记重建和现金回放共用。每个锚点装入时先拿上一锚点的状态 + 区间内事件
  逐户对比新锚点(逐位), 再从新锚点状态重放其后的事件; 已有可信回放时, 先到的锚点在 owner 里等到其块都已入账再装入
  (审查 000435 r1 P2)。同一块里切换过全仓 / 逐仓设置的币, 该块成交按不能确定为全仓处理, 停止回放(块内先动作后成交)。解析随之扩展: 账本事件按类型读出 USDC 金额、资金费带币和金额、
  区块动作带 `asset` / `ntli` / `isCross` / `amount`(只在保留的类型上读, 别的动作同名字段类型不同)、checkpoint 读币的
  `marginMode`、没有持仓的逐仓设置和现货交易对。新指标 `orderbook_accounts_replay_checked_total`、`_replay_stopped_total`、
  `_replay_checks_skipped_total`。

**实测**(10-19, bm): 对齐的单仓位池清算价与节点 0.1bp 以内(另有 3 个节点只给 4–5 位有效数字的显示精度差);
锚点之间约 40 万个仓位按成交推出的数量与下一份 checkpoint 逐个相同。只核对了 USDC 抵押池。

**已知边界**: 两次锚点之间清算价不随 mark 更新(实时 mark 规则未复现); 组合保证金只覆盖 USDC 结算 dex, 不给 0.95
情景价(借 USDH 的账户比率为 1 却 86 分钟未被清算, 真实触发条件待样本); 后备清算 / ADL 是否产生成交未核实(进入交易
判断前的阶段门); 区块交易标记只说明发生了可能使锚点失效的成功操作, 不在锚点之间重算现金、模式与清算价; 空变化
标记只说明账户自己没有这些事件, 不能证明清算价没变(同池其他仓位的 mark 在动, 组合保证金账户的借贷利息在累积)。

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
