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

var (
	GitTag    = "2021.7.31.init-release"
	BuildTime = "2021-7-31T00:00:00+0800"
)

func init() {
	// load config
	//market-matchadConfig()

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

func httpRespose(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, "I am exchange !!!")
}

func preStart(port int) {
	go func() {
		http.HandleFunc("/", httpRespose)
		err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
		if err != nil {
			common.Fatal("http listen error ！！")
		}
	}()
}

func catch() {
	if e := recover(); e != nil {
		dogstatsd.Event("service crashed", fmt.Sprintf("service crashed Panicing %s", e), statsd.Error)
		common.Error("server exit :", e, string(debug.Stack()))
		os.Exit(common.ErrnoSystemError)
	}
}

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

	for {
		time.Sleep(100 * time.Second)
	}
}

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

// TODO: 排查下为什么报错了
func startExchange() {
	for _, symbol := range config.GetStringSlice("symbols", []string{}) {
		perch := make(chan []byte, 10000)
		persistence.Init(symbol, perch)

		ch := make(chan *match.Order, 5000)
		mrCh := make(chan []byte, 5000)
		exchangeName := config.GetString("app.profile", "market") + "." + config.GetString("rabbitmq.exchange.quotation", "l2quote")

		// log.Println("l2quote.snapshot.path:", config.GetString("l2quote.snapshot.path", "./sp/"))
		l2 := l2quote.NewL2quote(symbol, redis.KlineClient, mrCh, config.GetString("l2quote.snapshot.path", "./sp/"),
			exchangeName, config.GetInt64("l2quote.mq-send-interval-ms", 500),
			config.GetInt64("l2quote.kline-forward-limit", 1440),
			config.GetInt("batch_result", 90),
			config.GetInt64("l2quote.snapshot.n-history", 10),
			config.GetInt("l2quote.make-new-kline-at-sec", 2))
		l2.Init()
		go l2.Run()
		common.Info(fmt.Sprintf("init %s l2quote finish", symbol))
		lastId, ctype := snapshotter.GetLastSnapshotId(symbol)

		var book *match.OrderBook
		if lastId > 0 {
			var err error
			book, err = snapshotter.Load(symbol, ctype, lastId)
			if err != nil {
				common.Fatal("load snapshotter failed lastId:", lastId, " symbol:", symbol)
			}
		} else {
			book = match.InitOrderBook(0, symbol)
		}

		puller.Init(ch, symbol, lastId+1)

		match.OrderBookMap[symbol] = book // add to map

		// 初始化depth深度
		go startMatcher(book, ch, mrCh, perch)
	}

}

func startMatcher(book *match.OrderBook, orderSeqChan chan *match.Order, mrChan chan []byte, perch chan []byte) {
	publishChan := match.PublishResultChan(book.Symbol)
	snapshotChan := snapshotter.DumpSnapshotChan(book.Symbol)
	go func() {
		defer catch()
		snapshotTicker := scheduler.NewTickerSnapshot()
		orderBookReportTicker := scheduler.NewTickerOrderbookReport()
		minDepthTicker := time.NewTicker(cast.ToDuration(config.GetInt64("market.min-depth-update-interval-ms", 100)) * time.Millisecond)
		stackedTicker := time.NewTicker(cast.ToDuration(config.GetInt64("market.min-stacked-depth-update-interval-ms", 1000)) * time.Millisecond)

		workMark := false
		defaultDepthTimeMs := common.TimestampNowMs() + config.GetInt64("market.default-update-interval-ms", 1000)

		reportTicker := time.NewTicker(time.Second * 10)

		for {
			select {
			case <-snapshotTicker.C:
				//lastSnapshotterId, _ := snapshotter.GetLastSnapshotId(book.Symbol)
				cloneBook := book.Clone()
				// 保证快照同步
				snapshotChan <- cloneBook
				mrChan <- []byte("snapshot!")
				//if book.FromId > lastSnapshotterId+snapshotter.MinGap()-1 {
				//
				//}
			case <-orderBookReportTicker.C:
				book.Report()
			case order := <-orderSeqChan:
				common.Debug("获取订单", order.OrderId, "|symbol", book.Symbol, "|", order.CreateAt)
				mrAB := book.GenMatchResult(order)
				bytesJson, err := json.Marshal(mrAB)
				if err != nil {
					common.Fatal("match encode to json err", err, mrAB)
				}
				statistics.SetMatchTag(order.SeqId)
				mrBytesJson, err := json.Marshal(mrAB.Mr)
				if err != nil {
					common.Fatal("match encode to json err", err, mrAB)
				}
				//persistence.PersistMR(mrBytesJson)
				perch <- mrBytesJson
				publishChan <- mrBytesJson
				mrChan <- bytesJson

				common.Debug(string(bytesJson))
				statistics.IncrMatchNum()
				workMark = true
			case <-reportTicker.C:
				//dogstatsd.GaugeBySymbol("puller.channel.length", cast.ToFloat64(len(orderSeqChan)), book.Symbol)
				//dogstatsd.GaugeBySymbol("match.current.id", cast.ToFloat64(book.FromId), book.Symbol)
				common.Info(fmt.Sprintf("%s matcher status --- puller.channler.length[%d] currentId[%d]",
					book.Symbol, len(orderSeqChan), book.FromId))
			case <-minDepthTicker.C:
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
				market.BuildAndReportDepthPercent10(book)
			}
		}
	}()
	return
}
