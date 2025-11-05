package l2quote

import (
	"context"
	"fmt"
	"market-match/common"
	"market-match/dogstatsd"
	"market-match/match"
	"market-match/scheduler"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
)

// 引入滴滴的高效json库
var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	KLINETYPES = []string{"1min",
		"5min",
		"15min",
		"30min",
		"60min",
		"4hour",
		"1day",
		"1week",
		"1mon"}

	KLINE_NAME_TYPE_MAP = map[string]int{"1min": Type1min,
		"5min":  Type5min,
		"15min": Type15min,
		"30min": Type30min,
		"60min": Type60min,
		"4hour": Type4hour,
		"1day":  Type1day,
		"1week": Type1week,
		"1mon":  Type1mon}

	KLINE_TYPE_TO_NAME_MAP = map[int]string{Type1min: "1min",
		Type5min:  "5min",
		Type15min: "15min",
		Type30min: "30min",
		Type60min: "60min",
		Type4hour: "4hour",
		Type1day:  "1day",
		Type1week: "1week",
		Type1mon:  "1mon"}
)

var (
	Type1min  = 0
	Type5min  = 1
	Type15min = 2
	Type30min = 3
	Type60min = 4
	Type4hour = 5
	Type1day  = 6
	Type1week = 7
	Type1mon  = 8
)

const (
	CONTUSD_DEFAULT int64 = 100
)

/*
所有内存状态
统一成一个结构，便于打快照
内存会是一个不断增长的list
存储时只取最新match result所在kline、与上一个kline两个kline
*/
type Quotation struct {
	// 最新match result的时间戳
	MaxMRTS int64
	// 最新match result的id
	MaxMRId int64
	// klines，使用treemap为了方便处理夏令时问题
	Klines map[string]*treemap.Map
	// market，由24小时的1minute数据组成
	Market []kline
	// ticker
	//TickerDetail []kline
	lastSnapshotTime int
}

type L2quote struct {
	symbol                string        // 交易对名称, 初始化时传入
	redisClient           *redis.Client // redisClient连接实例，初始化时传入
	mrChan                chan []byte   // 传入match result的channel， 初始化时传入
	snapshotPath          string        // 镜像路径， 初始化时传入
	mqExchangeName        string        // rabbitMQ的exchange名称， 初始化时传入
	mqSendIntervalMS      int64         // 向MQ中发送kline与market.detail的时间间隔， 初始化时传入
	klineForwardLimit     int64         // kline向前重绘补全的限制值， 初始化时传入
	mqBatchSize           int           // mq发送打包大小， 初始化时传入
	snapshotMaxHistoryNum int64         // 快照保留的数量, 初始化时传入
	makeNewKlineAtSec     int           // 每分钟第几秒钟检测是否需要新建空kline, 初始化时传入

	redisCache       []map[int64]int // redis update的缓存
	quotation        *Quotation      // 内存行情结构体
	marketDetail     *kline          // 24小时汇总
	ticker           *Ticker         //ticker message
	mqSendChan       chan *MqMessage // mq汇总发送队列
	sendMRID         int64           // 行情发送点标记
	sendMarketMRID   int64           // market detail发送点标记
	lastSnapshotTime int             // 最后一次快照时间
	ctx              context.Context
}

func NewL2quote(symbol string, client *redis.Client, matchResultChan chan []byte, snapshotPath string,
	mqExchangeName string, mqSendIntervalMS int64, klineForwardLimit int64, mqBatchSize int,
	snapshotMaxHistoryNum int64, makeNewKlineAtSec int) *L2quote {

	return &L2quote{symbol: symbol, redisClient: client, mrChan: matchResultChan, snapshotPath: snapshotPath,
		mqExchangeName: mqExchangeName, mqSendIntervalMS: mqSendIntervalMS,
		klineForwardLimit: klineForwardLimit, mqBatchSize: mqBatchSize,
		snapshotMaxHistoryNum: snapshotMaxHistoryNum, makeNewKlineAtSec: makeNewKlineAtSec, ctx: context.Background()}
}

func (L *L2quote) Init() int64 {
	L.checkRedisKline()
	L.initKlineFromSnapshots()
	L.redisCache = make([]map[int64]int, len(KLINETYPES), len(KLINETYPES))

	for _, t := range KLINETYPES {
		L.redisCache[KLINE_NAME_TYPE_MAP[t]] = make(map[int64]int)
	}

	L.mqSendChan = make(chan *MqMessage, 1024)
	return L.quotation.MaxMRId
}

/*
1. snapshot
2. save kline to redis
3. send match results -> l2quote
*/
func (L *L2quote) Run() {
	// ticker 初始化
	redisSaveTicker := time.NewTicker(time.Second * 1)
	reportTicker := time.NewTicker(time.Second * 10)
	sendKlineTicker := time.NewTicker(time.Millisecond * cast.ToDuration(L.mqSendIntervalMS))
	sendKlineFlushTicker := time.NewTicker(time.Second * 1)
	checkKlineTicker := time.NewTicker(time.Second * 1)
	oMinuteTicker := scheduler.OMinuteBySecond(5)        // 每分钟第5秒触发
	klineClearingTicker := time.NewTicker(time.Hour * 1) // 每小时做一次内存kline释放

	var count int64        // 消费counter
	var consumedPeriod int // 10秒内消费counter

	// 初始化market 24小时汇总数据
	L.buildMarket(time.Now().Unix())

	// 为各个计算单元初始化管道
	channels := make([]chan *match.MatchResult, 11, 11)
	for i := 0; i < len(channels); i++ {
		channels[i] = make(chan *match.MatchResult, 1)
	}

	// wait group for each calculator goroutine
	wg := sync.WaitGroup{}
	// 启动各个计算单元
	// 8种kline
	//go L.klineUpdater(channels[1], "3min", &wg)
	//go L.klineUpdater(channels[6], "2hour", &wg)
	//go L.klineUpdater(channels[8], "6hour", &wg)
	//go L.klineUpdater(channels[9], "8hour", &wg)
	//go L.klineUpdater(channels[10], "12hour", &wg)
	go L.klineUpdater(channels[0], "1min", &wg)
	go L.klineUpdater(channels[1], "5min", &wg)
	go L.klineUpdater(channels[2], "15min", &wg)
	go L.klineUpdater(channels[3], "30min", &wg)
	go L.klineUpdater(channels[4], "60min", &wg)
	go L.klineUpdater(channels[5], "4hour", &wg)
	go L.klineUpdater(channels[6], "1day", &wg)
	go L.klineUpdater(channels[7], "1week", &wg)
	go L.klineUpdater(channels[8], "1mon", &wg)
	// 24小时汇总行情
	go L.market(channels[9], &wg)
	// 交易记录
	go L.trade(channels[10], &wg)
	// ticker
	// update or flush Ticker message with market
	//go L.Ticker(channels[11], &wg)

	// mq发送协程
	go L.sendToMQ(L.mqSendChan)

	for {
		select {
		// 整分钟ticker，用于汇报kline状态
		case curTS := <-oMinuteTicker.C:
			// 触发是每分钟的5秒， -10秒汇报上一个分钟时间段的kline，这时kline应该已经close，不会再改变
			// 目前发现的一个边界情况，mq出现问题等待重连，channel满后会堵住,时间超过一分钟恢复的瞬间跳到这里会拿不到kline，所以补一个getKline
			L.getAllCurrentKline(curTS.Unix())
			L.reportKlines(curTS.Unix() - 10)
		case <-reportTicker.C:
			common.Info(fmt.Sprintf("%s l2quote status --- mrChanLen[%d] curMRID[%d] count[%d] consumed[%d/10sec]", L.symbol, len(L.mrChan), L.quotation.MaxMRId, count, consumedPeriod))
			// 汇报数据到datadog
			dogstatsd.GaugeBySymbol("mrChanLen", cast.ToFloat64(len(L.mrChan)), L.symbol)
			dogstatsd.GaugeBySymbol("curMRID", cast.ToFloat64(L.quotation.MaxMRId), L.symbol)
			dogstatsd.GaugeBySymbol("mrConsumed", cast.ToFloat64(consumedPeriod), L.symbol)
			// 清空10秒消费计数
			consumedPeriod = 0
		case curTS := <-sendKlineTicker.C:
			// 没有新的match result 不触发根据match result发送kline逻辑
			// 只发送60s以内的match result触发的kline
			if L.quotation.MaxMRId > L.sendMRID &&
				(curTS.Unix()-L.quotation.MaxMRTS) < 60 {
				L.getAllCurrentKline(L.quotation.MaxMRTS)
				L.sendAllKlines(L.quotation.MaxMRTS)
				L.sendMRID = L.quotation.MaxMRId
			}
		case curTS := <-sendKlineFlushTicker.C:
			// 最低每秒发送一次kline到下游，不管有没有更新
			// 每分钟前N秒不触发新kline
			if curTS.Second() < L.makeNewKlineAtSec {
				continue
			}
			L.getAllCurrentKline(curTS.Unix())
			L.sendAllKlines(curTS.Unix())
		case curTS := <-checkKlineTicker.C:
			// 检查是否需要建立空kline
			L.getAllCurrentKline(curTS.Unix())
		case <-redisSaveTicker.C:
			// 保存kline到redis
			L.saveKlinesToRedis()
		case <-klineClearingTicker.C:
			for i := range KLINETYPES {
				klineType := KLINETYPES[i]
				windowTime := int64(common.CurWindowTime(L.quotation.lastSnapshotTime, common.HmName2Type[klineType], 0))
				klines := L.quotation.Klines[KLINETYPES[i]]
				// 释放内存中陈旧kline
				for klines.Size() > 1440 {
					// klines是按照时间戳排过序的
					key, kl := klines.Min()
					if kl.(*kline).TS >= windowTime {
						// 当里正在撮合更早期数据时，
						// getAllCurrentKline(time.Now().Unix()) 会生成超过1440个数据
						// 此时可能会使早期数据在这里被删除，
						// 当打快照时找不到MRTS的快照信息，会crash掉
						// 所以先留存数据，等待早期数据撮合完成
						break
					}
					common.Debug("l2quote drop expired kline :", L.symbol, KLINETYPES[i], key)
					klines.Remove(key)
				}
			}
		case mrRaw := <-L.mrChan:
			// 根据上游信号打快照
			if string(mrRaw) == "snapshot!" {
				common.Info("start to tack", L.symbol, "l2quote snapshot")
				startTS := time.Now().UnixNano() / int64(time.Microsecond)

				L.getAllCurrentKline(time.Now().Unix())

				// 打快照前保存kline到redis，保证redis数据与快照数据同步
				err := L.saveKlinesToRedis()
				if err != nil {
					common.Warn("redis ops error , skip snapshot : ", err)
					// 这里redis暂时写失败会停止打快照，保证快照比redis中数据只少不多
					continue
				}
				// 打快照
				L.takeSnapShot()
				endTS := time.Now().UnixNano() / int64(time.Microsecond)
				common.Info(L.symbol, " l2quote snapshot time cost (micro-second): ", endTS-startTS)
				continue
			}

			//startTS := time.Now().Nanosecond() / int(time.Microsecond)

			count++
			consumedPeriod++
			// mr结构中有大量的指针，为了防止引用问题，mr生成后都是序列化再发送到管道，这里做反序列化
			mrAB := &match.MatchResultWithAskBid{}
			err := json.Unmarshal(mrRaw, mrAB)
			if err != nil {
				common.Fatal(L.symbol, "l2quote mr unmarshal error : ", err)
			}
			// 去重
			if mrAB.Mr.Id <= L.quotation.MaxMRId {
				common.Trace(fmt.Sprintf("%s get old msg, match result id : %d, l2quote max id : %d", L.symbol, mrAB.Mr.Id, L.quotation.MaxMRId))
				continue
			}

			L.ticker.AskVol = mrAB.AskVol
			L.ticker.AskPrice = mrAB.AskPrice
			L.ticker.BidVol = mrAB.BidVol
			L.ticker.BidPrice = mrAB.BidPrice
			wg.Add(11)
			// 分发到所有处理队列
			for i := 0; i < 11; i++ {
				channels[i] <- &mrAB.Mr
			}
			wg.Wait()

			L.quotation.MaxMRId = mrAB.Mr.Id
			L.quotation.MaxMRTS = mrAB.Mr.Ts / 1000

			//endTS := time.Now().Nanosecond() / int(time.Microsecond)
			//common.Warn(L.symbol, "l2quote match result time cost (micro-second) : ", endTS-startTS)
		}
	}
}

/*
判断是否是挂单
*/
func isPendingOrder(mr *match.MatchResult) bool {
	if len(mr.Items) == 1 || mr.Price.Equal(decimal.Zero) {
		return true
	}

	return false
}
