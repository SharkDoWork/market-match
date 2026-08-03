// market-match 是一个数字货币现货撮合引擎服务，也是整个程序的入口包。
//
// 整体职责：
//  1. 加载配置（viper），初始化日志、统计、快照、Redis、RabbitMQ、撮合引擎等基础设施；
//  2. 为配置中的每个交易对（symbol）初始化 L2 行情推送、订单簿（OrderBook）、
//     订单拉取器（puller，从 MySQL 订单序列表中拉取订单）以及撮合协程（startMatcher）；
//  3. 撮合结果通过 RabbitMQ 发布，同时写入持久化通道，并定期对订单簿做快照以便故障恢复。
//
// 数据流概览：
//
//	MySQL 订单序列表 --puller--> 订单 channel --startMatcher--> 撮合引擎(OrderBook.GenMatchResult)
//	      |                                                        |
//	      |                                                        +--> 撮合结果(MatchResult) --> RabbitMQ 发布 / 持久化 / L2行情
//	      +--> 快照定时器定期克隆订单簿 --> snapshotter 落盘，用于重启后恢复订单簿状态
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"market-match/common"
	"market-match/config"
	"market-match/dogstatsd"
	"market-match/l2quote"
	"market-match/market"
	"market-match/match"
	"market-match/persistence"
	"market-match/puller"
	"market-match/rabbitmq"
	"market-match/redis"
	"market-match/scheduler"
	"market-match/snapshotter"
	"market-match/statistics"
	"market-match/validate"
	"net/http"
	"os"
	"runtime/debug"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"

	"strconv"
	"time"
)

// 编译期注入的版本信息，可通过 go build -ldflags "-X main.GitTag=xxx" 覆盖
var (
	GitTag    = "2021.7.31.init-release" // 发布版本标签
	BuildTime = "2021-7-31T00:00:00+0800" // 构建时间
)

// init 在 main 之前执行：设置 decimal 全局除法精度，并打印启动日志。
// 注意：此处日志系统尚未初始化（ZapInit 在 main 中调用），Trace 使用的是默认配置。
func init() {
	// load config
	//market-matchadConfig()

	// decimal 库的全局除法精度设为 37 位，保证撮合计算中价格/数量精度足够
	decimal.DivisionPrecision = 37
	// init log with default setup
	//common.ZapInit()
	//if !ok {
	//	fmt.Println("log init failed, plz check whether there is dir \"./log\" for file:", common.LogFile)
	//
	//}

	// starting log
	common.Trace("=============== starting server ===============")

}

// httpRespose 是健康检查/探活接口的 handler，简单返回一段文本表明服务存活。
func httpRespose(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, "I am exchange !!!")
}

// preStart 在独立协程中启动一个 HTTP 服务，用于容器/负载均衡的健康探活。
// 端口由配置 http.port 指定，默认 9000。
func preStart(port int) {
	go func() {
		http.HandleFunc("/", httpRespose)
		err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
		if err != nil {
			common.Fatal("http listen error ！！")
		}
	}()
}

// catch 是全局 panic 兜底：recover 后上报 DataDog 事件、记录堆栈日志并以系统错误码退出。
// 通过 defer catch() 挂在 main 及各撮合协程上。
func catch() {
	if e := recover(); e != nil {
		dogstatsd.Event("service crashed", fmt.Sprintf("service crashed Panicing %s", e), statsd.Error)
		common.Error("server exit :", e, string(debug.Stack()))
		os.Exit(common.ErrnoSystemError)
	}
}

// main 是程序主入口，启动流程如下：
//
//  1. LoadConfigViper：加载配置文件（viper），失败直接 panic；
//  2. preStart：启动 HTTP 探活服务（默认 :9000）；
//  3. ZapInit：初始化 zap 日志系统；
//  4. defer catch()：挂载全局 panic 兜底，异常时上报监控并退出；
//  5. statistics.Init：启动撮合性能统计协程（撮合笔数/持久化笔数/全链路耗时）；
//  6. snapshotter.Init：初始化快照模块（订单簿定期落盘，用于重启恢复）；
//  7. redis.InitClient：初始化 Redis 客户端（K 线数据等）；
//  8. rabbitmq.Init：初始化 RabbitMQ 连接（撮合结果发布、行情推送）；
//  9. match.Init：初始化撮合引擎核心（订单簿并发容器等）；
// 10. marketInit：为每个交易对启动 L2 行情推送协程；
// 11. startExchange：为每个交易对初始化 L2行情 + 订单簿 + 订单拉取器 + 撮合协程（核心）；
// 12. ValidateOrderbook：启动订单簿校验逻辑；
// 13. WriteStartOk：写启动完成标记文件（供部署脚本探测）；
// 14. 主协程进入永久 sleep，所有实际工作由上述各协程承担。
func main() {
	// init proc -> for trace log-level
	// finish init -> load log-leve
	//common.ZapInit()

	if err := common.LoadConfigViper(); err != nil {
		panic("config load error")
	}

	preStart(config.GetInt("http.port", 9000))
	common.ZapInit()
	//	dogstatsd.Start()
	//	dogstatsd.Event("service starting", "service starting", statsd.Info)
	defer catch()
	statistics.Init()
	snapshotter.Init()
	redis.InitClient()
	//persistence.Init()
	rabbitmq.Init()
	match.Init()
	marketInit()

	startExchange()
	validate.ValidateOrderbook()
	common.WriteStartOk()
	common.Info("exchange started")

	// 主协程永久阻塞，实际工作全部在各子协程中进行
	for {
		time.Sleep(100 * time.Second)
	}
}

// marketInit 为配置 symbols 列表中的每个交易对启动 L2 行情（market depth）推送协程。
// exchangeName 形如 "<app.name>.l2quote"，是行情推送使用的 RabbitMQ topic exchange。
func marketInit() {
	//todo wait for config center
	//ResultExchangeName := config.GetString("app.profile", "market") + "." + config.GetString("rabbitmq.exchange.quotation", "l2quote")

	//rabbitmq.DeclareExchange(ResultExchangeName, "topic", false)
	for id, symbol := range config.GetStringSlice("symbols", []string{}) {
		common.Trace("initializing symbol[", id, "]:", symbol)
		// init market
		exchangeName := fmt.Sprintf("%s.%s", config.GetString("app.name", "market"),
			config.GetString("rabbitmq.exchange.quotation", "l2quote"))
		market.MarketThreadInit(exchangeName, symbol)
	}
}

// startExchange 为配置中的每个交易对（symbol）完成撮合链路的初始化，是整个系统的装配核心。
// 每个 symbol 的处理步骤：
//  1. 创建持久化通道 perch 并初始化 persistence（撮合结果异步落盘）；
//  2. 创建订单通道 ch（puller -> matcher）与撮合结果通道 mrCh（matcher -> l2quote）；
//  3. 创建并启动 L2 行情推送器 l2quote（消费 mrCh，生成 K 线/深度行情发往 RabbitMQ）；
//  4. 从 snapshotter 读取最近一次快照：若存在快照则加载恢复订单簿，否则新建空订单簿；
//  5. 启动 puller，从 MySQL 订单序列表的 lastId+1 位置开始拉取订单写入 ch；
//  6. 将订单簿注册到全局 match.OrderBookMap；
//  7. 启动撮合协程 startMatcher，进入事件循环。
//
// TODO: 排查下为什么报错了
func startExchange() {
	for _, symbol := range config.GetStringSlice("symbols", []string{}) {
		// perch: 撮合结果 -> 持久化模块的通道
		perch := make(chan []byte, 10000)
		persistence.Init(symbol, perch)

		// ch: puller 拉到的订单 -> 撮合协程；mrCh: 撮合结果 -> L2 行情模块
		ch := make(chan *match.Order, 5000)
		mrCh := make(chan []byte, 5000)
		exchangeName := config.GetString("app.profile", "market") + "." + config.GetString("rabbitmq.exchange.quotation", "l2quote")

		// log.Println("l2quote.snapshot.path:", config.GetString("l2quote.snapshot.path", "./sp/"))
		// 创建 L2 行情推送器：负责 K 线生成、深度行情快照与增量推送
		l2 := l2quote.NewL2quote(symbol, redis.KlineClient, mrCh, config.GetString("l2quote.snapshot.path", "./sp/"),
			exchangeName, config.GetInt64("l2quote.mq-send-interval-ms", 500),
			config.GetInt64("l2quote.kline-forward-limit", 1440),
			config.GetInt("batch_result", 90),
			config.GetInt64("l2quote.snapshot.n-history", 10),
			config.GetInt("l2quote.make-new-kline-at-sec", 2))
		l2.Init()
		go l2.Run()
		common.Info(fmt.Sprintf("init %s l2quote finish", symbol))
		// 查询该 symbol 最近一次订单簿快照的 id 及压缩类型
		lastId, ctype := snapshotter.GetLastSnapshotId(symbol)

		var book *match.OrderBook
		if lastId > 0 {
			// 存在快照：从快照文件恢复订单簿，重启后无需从头重放全部订单
			var err error
			book, err = snapshotter.Load(symbol, ctype, lastId)
			if err != nil {
				common.Fatal("load snapshotter failed lastId:", lastId, " symbol:", symbol)
			}
		} else {
			// 无快照：创建全新空订单簿
			book = match.InitOrderBook(0, symbol)
		}

		// 启动订单拉取协程，从快照位点的下一条订单开始拉取，保证不重不漏
		puller.Init(ch, symbol, lastId+1)

		match.OrderBookMap[symbol] = book // add to map

		// 初始化depth深度
		// 启动该 symbol 的撮合主协程
		go startMatcher(book, ch, mrCh, perch)
	}

}

// startMatcher 启动单个交易对的撮合主协程，是整个撮合引擎的事件循环核心。
// 参数：
//   - book: 该 symbol 的订单簿（可能从快照恢复）；
//   - orderSeqChan: puller 推入的订单流（按 SeqId 严格有序）；
//   - mrChan: 撮合结果输出通道，下游是 L2 行情模块（l2quote）；
//   - perch: 撮合结果输出通道，下游是持久化模块（persistence）。
//
// 协程内部通过 select 多路复用处理以下事件：
//  1. snapshotTicker：定期克隆订单簿并发送给 snapshotter 落盘，同时通知 l2quote 做行情快照；
//  2. orderBookReportTicker：定期上报订单簿内部状态（统计/监控）；
//  3. orderSeqChan：收到新订单 -> 调用 book.GenMatchResult 执行撮合 -> 撮合结果分别写入
//     持久化通道(perch)、RabbitMQ 发布通道(publishChan)、行情通道(mrChan)，并做性能统计；
//  4. reportTicker：每 10 秒打印一次撮合协程状态（待撮合订单积压数、当前撮合位点）；
//  5. minDepthTicker：最小深度行情推送——若自上次撮合后有订单变动(workMark)则立即推送一次深度，
//     否则超过默认间隔(default-update-interval-ms)也兜底推送一次，保证行情心跳；
//  6. stackedTicker：定期推送 10 档百分比聚合深度（BuildAndReportDepthPercent10）。
func startMatcher(book *match.OrderBook, orderSeqChan chan *match.Order, mrChan chan []byte, perch chan []byte) {
	publishChan := match.PublishResultChan(book.Symbol)
	snapshotChan := snapshotter.DumpSnapshotChan(book.Symbol)
	go func() {
		defer catch()
		// 快照定时器：控制订单簿快照落盘频率
		snapshotTicker := scheduler.NewTickerSnapshot()
		// 订单簿状态上报定时器
		orderBookReportTicker := scheduler.NewTickerOrderbookReport()
		// 最小深度行情推送定时器（默认 100ms）
		minDepthTicker := time.NewTicker(cast.ToDuration(config.GetInt64("market.min-depth-update-interval-ms", 100)) * time.Millisecond)
		// 聚合深度（10档百分比）推送定时器（默认 1000ms）
		stackedTicker := time.NewTicker(cast.ToDuration(config.GetInt64("market.min-stacked-depth-update-interval-ms", 1000)) * time.Millisecond)

		// workMark 标记自上次深度推送后是否有订单被撮合，有则尽快推送新深度
		workMark := false
		// 下一次兜底深度推送的时间点（即使无成交也保证行情心跳）
		defaultDepthTimeMs := common.TimestampNowMs() + config.GetInt64("market.default-update-interval-ms", 1000)

		reportTicker := time.NewTicker(time.Second * 10)

		for {
			select {
			case <-snapshotTicker.C:
				// 快照定时触发：克隆当前订单簿发给 snapshotter 异步落盘，
				// 同时通知 l2quote 模块同步做一次行情快照，保证两者位点一致
				//lastSnapshotterId, _ := snapshotter.GetLastSnapshotId(book.Symbol)
				cloneBook := book.Clone()
				// 保证快照同步
				snapshotChan <- cloneBook
				mrChan <- []byte("snapshot!")
				//if book.FromId > lastSnapshotterId+snapshotter.MinGap()-1 {
				//
				//}
			case <-orderBookReportTicker.C:
				// 定期上报订单簿内部统计信息
				book.Report()
			case order := <-orderSeqChan:
				// 收到新订单：执行撮合，并将结果扇出到持久化、MQ 发布、L2 行情三个下游
				common.Debug("获取订单", order.OrderId, "|symbol", book.Symbol, "|", order.CreateAt)
				mrAB := book.GenMatchResult(order)
				bytesJson, err := json.Marshal(mrAB)
				if err != nil {
					common.Fatal("match encode to json err", err, mrAB)
				}
				// 记录该订单完成撮合的时间点，用于全链路耗时统计
				statistics.SetMatchTag(order.SeqId)
				mrBytesJson, err := json.Marshal(mrAB.Mr)
				if err != nil {
					common.Fatal("match encode to json err", err, mrAB)
				}
				//persistence.PersistMR(mrBytesJson)
				perch <- mrBytesJson       // -> 持久化模块
				publishChan <- mrBytesJson // -> RabbitMQ 撮合结果发布
				mrChan <- bytesJson        // -> L2 行情模块（含订单簿变动信息）

				common.Debug(string(bytesJson))
				statistics.IncrMatchNum()
				workMark = true
			case <-reportTicker.C:
				// 每 10 秒输出一次撮合协程运行状态：订单通道积压长度与当前撮合到的订单 id
				//dogstatsd.GaugeBySymbol("puller.channel.length", cast.ToFloat64(len(orderSeqChan)), book.Symbol)
				//dogstatsd.GaugeBySymbol("match.current.id", cast.ToFloat64(book.FromId), book.Symbol)
				common.Info(fmt.Sprintf("%s matcher status --- puller.channler.length[%d] currentId[%d]",
					book.Symbol, len(orderSeqChan), book.FromId))
			case <-minDepthTicker.C:
				// 最小深度推送：有撮合发生时立即推送一次最新深度；
				// 无撮合时超过默认间隔也兜底推送，维持行情心跳
				nowMs := common.TimestampNowMs()
				if workMark == true {
					market.BuildAndReportDepth(book)
					defaultDepthTimeMs = nowMs + config.GetInt64("market.default-update-interval-ms", 1000)
					workMark = false
				}

				if defaultDepthTimeMs < nowMs {
					market.BuildAndReportDepth(book)
					defaultDepthTimeMs = nowMs + config.GetInt64("market.default-update-interval-ms", 1000)
				}
			case <-stackedTicker.C:
				// 定期推送按百分比聚合的 10 档深度行情
				market.BuildAndReportDepthPercent10(book)
			}
		}
	}()
	return
}
