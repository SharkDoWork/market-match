// Package l2quote L2 行情模块主文件。
// 本模块负责接收撮合引擎（match）产生的撮合结果（MatchResult），
// 基于成交数据实时生成并维护各类行情数据：
//   - 多周期 K 线（1min/5min/15min/30min/60min/4hour/1day/1week/1mon）
//   - 24 小时滚动汇总行情（market detail）
//   - 实时 ticker（最新价、买一卖一、涨跌幅等）
//   - 成交流水（trade detail）
//
// 生成的行情数据一方面缓存到 Redis 供查询服务读取，
// 另一方面通过 RabbitMQ 推送给下游订阅者（如行情推送网关）。
// 同时模块支持定时打快照（snapshot）到本地磁盘并上传 S3，
// 以便重启后能快速恢复内存中的行情状态，避免全量重放撮合结果。
//
// 本文件定义了模块的核心数据结构（Quotation、L2quote）、
// K 线周期常量，以及主运行循环 Run()。
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
	// KLINETYPES 本模块支持的所有 K 线周期名称列表。
	// 每种周期都会有一个独立的 klineUpdater 协程负责维护。
	KLINETYPES = []string{"1min",
		"5min",
		"15min",
		"30min",
		"60min",
		"4hour",
		"1day",
		"1week",
		"1mon"}

	// KLINE_NAME_TYPE_MAP K 线周期名称到内部整型编号的映射，
	// 用于 redisCache 等以 int 为下标的场景。
	KLINE_NAME_TYPE_MAP = map[string]int{"1min": Type1min,
		"5min":  Type5min,
		"15min": Type15min,
		"30min": Type30min,
		"60min": Type60min,
		"4hour": Type4hour,
		"1day":  Type1day,
		"1week": Type1week,
		"1mon":  Type1mon}

	// KLINE_TYPE_TO_NAME_MAP 内部整型编号到 K 线周期名称的反向映射。
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

// 各 K 线周期的内部整型编号，与 KLINETYPES 的顺序一一对应。
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
	// CONTUSD_DEFAULT 合约面值默认值（当前代码中未实际使用，保留作默认配置）。
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

// L2quote 单个交易对的 L2 行情计算实例。
// 每个交易对（symbol）会创建一个 L2quote 实例，
// 内部持有该交易对全部行情状态（Quotation）、
// 与外部交互的句柄（redis、mq、撮合结果 channel），
// 以及各类配置参数。
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

// NewL2quote 创建一个交易对的 L2 行情实例。
// 参数说明：
//   - symbol: 交易对名称，如 "BTC_USDT"
//   - client: redis 客户端，用于读写 K 线缓存
//   - matchResultChan: 上游撮合引擎推送撮合结果的 channel（序列化后的字节流）
//   - snapshotPath: 本地快照文件存储目录
//   - mqExchangeName: RabbitMQ exchange 名称
//   - mqSendIntervalMS: 向 MQ 批量发送行情的时间间隔（毫秒），用于节流
//   - klineForwardLimit: 旧撮合结果向前重绘 K 线时的最大迭代窗口数
//   - mqBatchSize: MQ 批量发送时每包最多打包的消息条数
//   - snapshotMaxHistoryNum: 本地保留的快照文件最大数量
//   - makeNewKlineAtSec: 每分钟第几秒之后才允许创建新 K 线（避开整分钟边界抖动）
func NewL2quote(symbol string, client *redis.Client, matchResultChan chan []byte, snapshotPath string,
	mqExchangeName string, mqSendIntervalMS int64, klineForwardLimit int64, mqBatchSize int,
	snapshotMaxHistoryNum int64, makeNewKlineAtSec int) *L2quote {

	return &L2quote{symbol: symbol, redisClient: client, mrChan: matchResultChan, snapshotPath: snapshotPath,
		mqExchangeName: mqExchangeName, mqSendIntervalMS: mqSendIntervalMS,
		klineForwardLimit: klineForwardLimit, mqBatchSize: mqBatchSize,
		snapshotMaxHistoryNum: snapshotMaxHistoryNum, makeNewKlineAtSec: makeNewKlineAtSec, ctx: context.Background()}
}

// Init 初始化行情实例：
//  1. 校验 redis 中历史 K 线数据的完整性（checkRedisKline）
//  2. 从本地快照恢复内存行情状态，无快照则从零初始化（initKlineFromSnapshots）
//  3. 初始化 redis 写缓存（redisCache，按 K 线周期分桶）
//  4. 创建 MQ 发送队列
//
// 返回值：恢复后的最大撮合结果 ID（MaxMRId），供上游决定从哪条撮合结果开始重放。
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

// Run 启动行情计算主循环，是整个模块的核心调度入口。
// 工作流程：
//  1. 启动 9 个 klineUpdater 协程（每种 K 线周期一个）、1 个 market 协程（24h 汇总）、
//     1 个 trade 协程（成交流水）、1 个 sendToMQ 协程（批量发送 MQ）。
//  2. 主循环通过 select 监听多路事件：
//     - mrChan: 接收上游撮合结果，反序列化后分发给各计算协程，并用 WaitGroup 等待全部处理完，
//       保证同一时刻内存状态一致，便于打快照。
//     - "snapshot!" 控制消息: 先把 K 线落 Redis（保证快照与 Redis 数据一致），再执行 takeSnapShot。
//     - 各类定时 ticker: 定时发送 K 线/行情到 MQ、定时落 Redis、定时建新 K 线、
//       定时清理内存中过期的 K 线、定时上报监控指标。
//
// 注意：分发采用"扇出 + WaitGroup"模式，每条撮合结果会被 11 个计算单元并行处理，
// 全部完成后才更新 MaxMRId/MaxMRTS，因此快照时刻的内存状态是某一撮合结果边界上的一致性视图。
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

// isPendingOrder 判断一条撮合结果是否为"纯挂单"（即没有产生成交）。
// 判定逻辑：撮合结果的 Items 只有 1 条（仅 taker 自身，未匹配到任何 maker），
// 或成交价为 0，都视为未成交的挂单。
// 挂单不产生 K 线/行情更新，各计算协程收到后会直接忽略。
func isPendingOrder(mr *match.MatchResult) bool {
	if len(mr.Items) == 1 || mr.Price.Equal(decimal.Zero) {
		return true
	}

	return false
}
