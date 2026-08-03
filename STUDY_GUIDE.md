# 撮合引擎学习指南（阅读顺序与代码导航）

> 本文档帮助你按"从宏观到微观、从入口到核心"的顺序理解 `market-match` 撮合引擎项目。
> 配套文档：`README.md`（概念白话）、`architecture_diagram.md`（架构/流程图）。
> 所有 Go 源码均已添加中文注释，可直接在 IDE 中打开对应文件阅读。

---

## 一、先建立全局认知（不读代码）

1. 读 `README.md` —— 撮合引擎是什么、订单/订单簿/撮合/L2行情/快照的概念。
2. 读 `architecture_diagram.md` —— 模块关系图 + 订单撮合时序图 + 系统启动流程图。
3. 记住核心数据流：
   ```
   客户端/API → RabbitMQ(消息队列) → Puller → Validate → Match(撮合核心)
                                                         ↓
                       OrderBook(红黑树) / Market(行情) / L2Quote / Snapshotter / Persistence(MySQL)
   行情结果同时写 Redis 并推送到 RabbitMQ 供下游消费。
   ```

---

## 二、推荐的代码阅读顺序

按以下顺序读，每读一个包，先读文件顶部的**包级注释**，再看导出函数注释。

### 第 1 步：程序入口（系统地图）
- **`market-match.go`**（根目录主入口，最关键）
  - `main()`：14 步启动流程（加载配置 → HTTP 探活 → 日志 → 统计 → 快照 → Redis → RabbitMQ → 撮合引擎 → 市场初始化 → 启动交易所 → 校验 → 写启动标记 → 主协程阻塞）。
  - `startExchange()`：每个交易对怎么装配（perch / ch / mrCh 三个通道的作用、L2行情创建、快照恢复 vs 新建订单簿、puller 从 `lastId+1` 拉单保证不重不漏）。
  - `startMatcher()`：一个交易对的撮合事件循环（快照定时、订单撮合扇出、深度上报、10 秒状态打印等 6 个 `case`）。

### 第 2 步：撮合核心（项目灵魂，重点精读）
- **`match/` 包**
  - `order.go`：订单模型 `Order`、买卖枚举 `OrderBuyOrSell`、13 种 `OrderState`、`OrderType`、自成交策略 `SelfTradeWMType`、红黑树排序规则 `Comparator`（买单价格降序 / 卖单价格升序 / 同价 SeqId 小者优先）、`fillAmount` 成交扣减、`CheckOrderScale` 精度校验砍单。
  - `order_book.go`：订单簿结构（双红黑树 `BuySet`/`SellSet` + `cache` 索引；`BuySet` 队首=买一、`SellSet` 队首=卖一）；`Enqueue/Dequeue/Peek`、`Clone`（深拷贝供快照）、`CompareOrderBook`（快照恢复校验）、`TreeSet` 的 gob 序列化。
  - `order_book_map.go`：全局 `symbol → OrderBook` 注册表。
  - `matcher.go`：撮合算法主体。重点函数：
    - `matchMarket` 市价单逐档吃单（+ 熔断保护 `CircuitRate`、精度撤销 `PrecisionCanceled`）
    - `matchLimit` 限价单（成交不了就挂单）
    - `matchIoc` / `matchFok` / `matchLimitMaker` 各类订单类型分支
    - `matchCancel` / `matchBatchCancel` / `matchSystemCancel` 撤单与状态映射
    - `matchAble` 价格可成交判定（买价 ≥ 卖价）
    - `matchAmountBasedOrder` / `matchCashAmountBasedOrder` 按数量/金额撮合（成交价以 maker 价为准）
    - 自成交 STP 四策略：`CB`/`CO`/`CN`/`DC`
    - `finalizeTaker` 系列：各订单类型终态结果字符串映射
    - `PublishResultChan` / `BatchMatchResult`：RabbitMQ 批量发布

### 第 3 步：数据接入与校验
- **`puller/puller.go`**：从 RabbitMQ/DB 拉取订单，自增 id 游标保证有序；订单类型整型编码常量（0 撤单 / 1-2 市价 / 3-4 限价 / 5-6 IOC / 7-8 FOK / 9-10 LimitMaker / 13 批量撤单）；退避策略（无新单时降为长轮询）。
- **`validate/validate.go`**：订单簿回放校验原理（从 `baseBook` 快照出发回放 `MatchResult`，与 `lastBook` 快照比对，确保撮合确定性）。

### 第 4 步：行情与市场数据
- **`market/` 包**：`market.go`（`MarketThreadInit`，每交易对一个 goroutine）、`thread.go`（批量打包发 MQ）、`depth.go`（深度聚合：多步长并行、百分比深度、累计深度、买单向下取整/卖单向上取整）、`depth_incr.go`（增量深度）。
- **`l2quote/` 包**：`l2quote.go`（L2 行情主循环 `Run`）、`kline.go`（K线 OHLCV 聚合与旧结果向前重绘 `klineForwardLimit`）、`market.go`（24h 滚动汇总，1440 个 1 分钟桶滑动窗口）、`snapshot.go`（行情快照设计）、`trade.go`（成交流水）、`ticker.go`（24h 汇总 + 盘口 + 涨跌幅）、`rabbitmq_ops.go`（MQ 批量发送节流）、`redis_ops.go`（Redis 缓存键设计）。

### 第 5 步：持久化、快照与调度
- **`persistence/persistence.go`**：两级 channel 流水线（单条缓冲 → 攒批 → 多协程批量 INSERT）；按 `f_id` 排序减少 InnoDB 页分裂；maker/taker/cancel 角色推断。
- **`snapshotter/snapshotter.go`**：撮合引擎容灾关键机制；临时文件+重命名保证原子写入；S3 备份；`Load` 用 gob 解码重建缓存索引；多实例错开快照时间。
- **`scheduler/scheduler.go`**：对齐到固定时间点触发的定时器（与标准库 `time.Ticker` 的区别）。

### 第 6 步：基础设施与公共能力
- **`rabbitmq/rabbitmq.go`**：连接池、随机选 URI 故障转移、断线重连、zlib 压缩、按 symbol 负载均衡、交换机声明。
- **`redis/redis.go`**：Redis 客户端（K线行情缓存），`Ping` 验证连通性。
- **`common/` 包**：日志（`zap.go` 初始化 + 轮转）、配置加载（`config.go` 的 `LoadConfigViper` + 精度/符号信息）、时间工具（`time.go` 的 K线周期常量与窗口计算）、错误码（`errno.go`）、环境变量（`os.go`）、通道映射（`chan.go`）。
- **`config/` 包**：`config.go`（8 个 `Get*` 配置读取）、`cache.go`（Apollo `agcache` 接口实现）、`listen.go`（热更新监听）。
- **`statistics/statistics.go`**：两类统计（每秒吞吐计数 + 每千笔采样的全链路耗时）。
- **`dogstatsd/dogstatsd.go`**：DataDog 监控上报封装（注意：当前初始化被注释停用，`Gauge` 调用有 nil panic 风险）。
- **`assign/assign.go`**：离线校验+重放工具（从快照重放订单与历史结果比对）。

---

## 三、核心概念速查

| 概念 | 含义 | 主要在哪 |
|---|---|---|
| 订单簿 OrderBook | 所有未成交订单的集合，双红黑树（买降序/卖升序） | `match/order_book.go` |
| Taker / Maker | Taker 主动吃单者，Maker 挂单被动成交者 | `match/matcher.go` |
| 价格优先 / 时间优先 | 同方向订单按价格排队，同价按 SeqId（时间）排队 | `match/order.go` Comparator |
| 限价单 / 市价单 / 取消单 | 指定价成交 / 立即按市价成交 / 撤单 | `match/matcher.go` |
| IOC / FOK / LimitMaker | 立即成交否则撤销 / 全部成交否则整撤 / 只做 Maker(Post-Only) | `match/matcher.go` |
| 自成交 STP | CB 双边撤 / CO 撤老单 / CN 撤新单 / DC 减量 | `match/order.go` SelfTradeWMType |
| L2 行情 | Level 2 实时行情：最新价、成交量、买卖盘深度 | `l2quote/` |
| K线 | OHLCV 聚合；按周期（1m/5m/...）聚合与旧结果重绘 | `l2quote/kline.go` |
| 深度 Depth | 各价位挂单量（买一卖一、百分比深度、累计深度） | `market/depth.go` |
| 快照 Snapshot | 订单簿某一时刻完整状态，用于重启恢复/容灾 | `snapshotter/` |
| 回放校验 | 从快照重放 MatchResult 与真实结果比对，验证确定性 | `validate/validate.go` |

---

## 四、动手学习建议

1. **跟一个订单走完全程**：在 `market-match.go` 的 `startMatcher` 事件循环里，下断点看一个订单如何被 `puller` 拉取 → `match` 撮合 → 扇出到 `perch`(持久化) / `publishChan`(RabbitMQ) / `mrChan`(L2行情)。
2. **跑测试理解边界**：`match/matcher_test.go`（限价/市价/熔断/Post-Only）、`market/depth_test.go`（深度聚合）、`snapshotter/snapshotter_test.go`（快照）都有详细注释，是理解算法边界的好材料。
3. **本地启动**：用 Windows Docker Desktop（开启 WSL2 集成）在 WSL2 终端执行 `docker-compose up -d`，起 mysql/redis/rabbitmq/app 四个服务，看 `conf/dev.yaml` 配置的交易对与组件地址。

---

## 五、已知的原代码小问题（非注释引入，供参考）

- `l2quote/redis_ops.go:49`：`%s` 格式化动词与 int 参数不匹配（vet 警告，原代码已有）。
- `common/time.go`：常量 `Type8Hour` 疑似笔误（注释里已标注）。
- `dogstatsd/dogstatsd.go`：`client` 初始化被注释停用，调用 `Gauge` 时若 client 为 nil 会 panic。
