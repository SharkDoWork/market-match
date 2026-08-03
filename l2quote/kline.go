// Package l2quote 本文件实现多周期 K 线（OHLCV）的实时聚合逻辑。
// 核心概念：
//   - K 线（蜡烛图）：一个时间窗口内的开盘价(Open)、最高价(High)、最低价(Low)、
//     收盘价(Close)、成交量(Vol)、成交额(TurnOver)、成交笔数(Count)。
//   - 每种周期（1min/5min/.../1mon）由独立的 klineUpdater 协程维护，
//     存储在按时间戳排序的 treemap 中，方便按窗口查找与补齐。
//   - 当收到"旧"撮合结果（重放/补数据场景）时，会从该结果所在窗口开始，
//     用其成交价向前（向最新方向）重绘后续窗口的 K 线，直到当前窗口，
//     重绘深度受 klineForwardLimit 限制，防止长时间重放拖垮系统。
package l2quote

import (
	"fmt"
	"market-match/common"
	"market-match/match"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// QuoteKline 发送到下游（MQ）的 K 线消息封装格式。
// Type 固定为 "market.candles"，Interval 为周期名（如 "1min"），
// Candle 为 K 线本体，TS/SeqId 用于下游排序与去重。
type QuoteKline struct {
	Type     string `json:"type"`
	PairCode string `json:"pairCode"`
	Interval string `json:"interval"`
	Candle   *kline `json:"data"`
	TS       int64  `json:"id"`
	SeqId    int64  `json:"SeqId"`
}

// kline 单根 K 线（蜡烛）数据结构，即 OHLCV 聚合结果。
// TS 为该 K 线所在时间窗口的起始时间戳（秒），SeqId 为最后更新它的撮合结果 ID。
// 该结构同时用于：内存存储、24h 汇总（market detail 的每个分钟桶）、
// 以及序列化后作为 MQ 消息与 Redis 缓存的内容。
type kline struct {
	TS         int64           `json:"id"`
	SeqId      int64           `json:"seqId"`
	OpenPrice  decimal.Decimal `json:"open"`
	ClosePrice decimal.Decimal `json:"close"`
	HighPrice  decimal.Decimal `json:"high"`
	LowPrice   decimal.Decimal `json:"low"`
	TurnOver   decimal.Decimal `json:"turnOver"`
	Vol        decimal.Decimal `json:"vol"`
	Count      int64           `json:"count"`
	UnitAmount int64           `json:"unitAmount,omitempty"`
}

/*
kline绘制协程
1.检查是否需要新建空kline
2.接收match result并绘制相应kline
3.定时发送kline到mq
*/
func (L *L2quote) klineUpdater(mrCh chan *match.MatchResult, klineType string, wg *sync.WaitGroup) {
	for {
		select {
		case mr := <-mrCh:
			//bendel
			//t1 := time.Now().UnixNano() / 1e6
			// 更新kline前检查当前分钟kline是否存在
			if L.symbol != mr.Symbol {
				common.Fatal("mr klu =", mr)
			}

			ts := common.CurWindowTime(int(time.Now().Unix()), common.HmName2Type[klineType], 0)
			L.getCurrentKline(klineType, int64(ts))
			// 更新kline
			L.updateKline(mr, int64(ts), klineType)
			wg.Done()
			//common.Warn("l2  kline cost time :", L.symbol, klineType, " | ", time.Now().UnixNano() / 1e6 - t1)
		}
	}
}

// stateIsFilled 判断撮合条目的状态是否表示"已成交"（完全成交或部分成交）。
// 只有已成交的条目才参与 K 线/行情的价格与成交量聚合。
func stateIsFilled(state string) bool {
	if state == "filled" || state == "partial-filled" {
		return true
	}

	return false
}

// getStartPrice 从撮合结果中取出第一笔有效成交价，
// 用作新 K 线的开盘/最高/最低初始价。
// 若所有条目都未成交则返回 nil（调用方会跳过本次更新）。
func getStartPrice(mr *match.MatchResult) *decimal.Decimal {
	for i := range mr.Items {
		if mr.Items[i].Price != nil && stateIsFilled(mr.Items[i].State) {
			return mr.Items[i].Price
		}
	}
	return nil
}

/*
根据match result更新kline

1.处于当前时间段的match result，只更新当前窗口的kline

2.处于当前时间段之前的match result，更新所在时间段kline，并迭代用closePrice更新到当前最新窗口kline

更新kline后保存key到用于redis update的map中，由ticker触发，统一更新redis

klineForwardLimit，2状态的mr，常见于出现问题重绘或者重启，如果重绘的match result离当前时间太久，迭代更新的损耗会非常大
这里设置一个可配置的参数来控制这个限制 ---
1）需要快速追上数据时，可以设置较小数值
2）中间有较长时间没有成交单，又需要严格重绘kline，可设置较大值，例如有交易对已经中断1天没有交易对，可设置为1440
*/
func (L *L2quote) updateKline(mr *match.MatchResult, ts int64, klineType string) {
	// 挂单不触发值更新
	if isPendingOrder(mr) {
		return
	}

	// 这个地方处理收到上一个时间段mr的边界情况
	mrWindowTime := common.CurWindowTime(int(mr.Ts/1000), common.HmName2Type[klineType], 0)
	updateTimeWindow := mrWindowTime
	// 如果是旧时间段的数据，要依次更新到最新的时间段
	// 这个在重新大量数据的时候代价太高，增加一个迭代限制
	// 超过1个小时的数据，最多更新60个后面的数据
	limit := L.klineForwardLimit
	for int64(updateTimeWindow) <= ts && limit > 0 {
		//for int64(updateTimeWindow) <= ts {
		kInterface, ok := L.quotation.Klines[klineType].Get(int64(updateTimeWindow))
		if !ok {
			newKline := &kline{}
			newKline.TS = int64(updateTimeWindow)
			kInterface = newKline
			L.quotation.Klines[klineType].Put(int64(updateTimeWindow), newKline)
		}

		k := kInterface.(*kline)

		if int64(updateTimeWindow) == int64(mrWindowTime) { // 更新mr所在时间段
			olhPrice := getStartPrice(mr)
			if olhPrice == nil {
				continue
			}

			if k.Count == 0 {
				k.OpenPrice = *olhPrice
				k.LowPrice = *olhPrice
				k.HighPrice = *olhPrice
			}
			// 挂单在上层已经过滤掉，所以mr.Items的长度最少为2， len(mr.Items)-2是最后一个maker
			k.ClosePrice = mr.Price
			// 保存一下match result id，（定序id）
			k.SeqId = mr.Id

			for i := range mr.Items {
				if mr.Items[i].Role == "maker" && mr.Items[i].Price != nil && stateIsFilled(mr.Items[i].State) {
					// high price
					if mr.Items[i].Price.GreaterThan(k.HighPrice) {
						k.HighPrice = *mr.Items[i].Price
					}

					// low price
					if mr.Items[i].Price.LessThan(k.LowPrice) {
						k.LowPrice = *mr.Items[i].Price
					}

					k.Vol = k.Vol.Add(*mr.Items[i].FilledAmount)
					k.TurnOver = k.TurnOver.Add(mr.Items[i].FilledAmount.Mul(*mr.Items[i].Price))

					k.Count++
				}
			}
		} else { // 迭代更新mr后面的时间段,把所有价格设置为最后一个maker价格
			k.SeqId = mr.Id
			// 挂单不会进到这里，len(mr.Items)-2是最后一个maker
			k.OpenPrice = mr.Price
			k.ClosePrice = mr.Price
			k.HighPrice = mr.Price
			k.LowPrice = mr.Price

			// 不更新
		}

		// add kline to redis write cache
		L.redisCache[KLINE_NAME_TYPE_MAP[klineType]][k.TS] = 1

		//updateTimeWindow += common.HmType2Step[common.HmName2Type[klineType]]
		updateTimeWindow = int(common.NextKlineTimeWindow(int64(updateTimeWindow), klineType))
		limit--
	}
}

/*
检查是否有当前时间段kline，
如果没有则根据上一个kline新建

从快照启动、并第一次执行时，会从快照kline时间窗口直接补全kline到当前时间窗口
*/
func (L *L2quote) getCurrentKline(klineType string, ts int64) {
	windowTime := int64(common.CurWindowTime(int(ts), common.HmName2Type[klineType], 0))
	// 没有快照的情况下恢复，直接新建一个当前kline
	if L.quotation.Klines[klineType].Size() == 0 {
		newKline := kline{}
		newKline.TS = windowTime
		L.quotation.Klines[klineType].Put(windowTime, &newKline)
		L.redisCache[KLINE_NAME_TYPE_MAP[klineType]][newKline.TS] = 1
	} else if _, ok := L.quotation.Klines[klineType].Get(windowTime); !ok {
		// 拿到当前最近的kline
		_, lastKline := L.quotation.Klines[klineType].Max()
		//lastWindowTime := lastKline.(*kline).TS + int64(common.HmType2Step[common.HmName2Type[klineType]])
		lastWindowTime := common.NextKlineTimeWindow(lastKline.(*kline).TS, klineType)
		lastPrice := lastKline.(*kline).ClosePrice
		// 迭代补齐kline到传入的时间
		for lastWindowTime <= windowTime {

			newKline := kline{}
			newKline.TS = lastWindowTime
			newKline.OpenPrice = lastPrice
			newKline.ClosePrice = lastPrice
			newKline.HighPrice = lastPrice
			newKline.LowPrice = lastPrice
			newKline.SeqId = lastKline.(*kline).SeqId
			L.quotation.Klines[klineType].Put(lastWindowTime, &newKline)

			//lastWindowTime += int64(common.HmType2Step[common.HmName2Type[klineType]])
			lastWindowTime = common.NextKlineTimeWindow(lastWindowTime, klineType)

			// add kline to redis write cache
			L.redisCache[KLINE_NAME_TYPE_MAP[klineType]][newKline.TS] = 1
		}
	}
}

/*
检查所有类型kline是否需要新建空kline
*/
func (L *L2quote) getAllCurrentKline(ts int64) {
	for i := range KLINETYPES {
		klineType := KLINETYPES[i]
		L.getCurrentKline(klineType, ts)
	}
}

/*
发送kline到mq发送管道
*/
func (L *L2quote) sendAllKlines(ts int64) {
	for i := range KLINETYPES {
		klineType := KLINETYPES[i]
		curTS := int64(common.CurWindowTime(int(ts), common.HmName2Type[klineType], 0))
		k, ok := L.quotation.Klines[klineType].Get(curTS)
		if !ok {
			common.Warn(L.symbol, "send kline get kline error", curTS, klineType, L.quotation.Klines[klineType])
			return
		}

		// 空价格不吐kline
		if k.(*kline).ClosePrice.Equal(decimal.Zero) {
			return
		}

		qk := QuoteKline{Type: "market.candles", PairCode: L.symbol, Interval: klineType, Candle: k.(*kline),
			SeqId: k.(*kline).SeqId, TS: k.(*kline).TS}
		body, err := json.Marshal(qk)
		if err != nil {
			common.Fatal(L.symbol, "kline marshal error :", err)
			return
		}

		var routingKey string

		routingKey = fmt.Sprintf("market.%s.kline.%s", L.symbol, klineType)
		mqMsg := MqMessage{RoutingKey: routingKey, Body: body}
		// 发送消息到队列
		common.Trace("[sendAllKlines] mqmsg=", mqMsg, L.mqSendChan)
		L.mqSendChan <- &mqMsg
	}
}

/*
用相对紧凑的格式打印kline状态
*/
func (L *L2quote) reportKlines(ts int64) {
	strBuilder := strings.Builder{}

	strBuilder.WriteString(L.symbol)
	strBuilder.WriteString(" klines status maxMRID[")
	strBuilder.WriteString(strconv.FormatInt(L.quotation.MaxMRId, 10))
	strBuilder.WriteString("] ")

	for i := range KLINETYPES {
		windowTime := common.CurWindowTime(int(ts), common.HmName2Type[KLINETYPES[i]], 0)
		k, ok := L.quotation.Klines[KLINETYPES[i]].Get(int64(windowTime))
		// 这个地方理论上不会走到，但是因为只是一个日志中汇报的操作。如果出现问题则只报警，然后继续
		if !ok {
			common.Warn(fmt.Sprintf("%s %s %d report kline get current kline error : %+v", L.symbol, KLINETYPES[i], windowTime, L.quotation.Klines[KLINETYPES[i]]))
			return
		}
		_, _, klineStr := L.klineToRedisFormat(k.(*kline), KLINETYPES[i])
		strBuilder.WriteString(KLINETYPES[i])
		strBuilder.WriteString("[")
		strBuilder.WriteString(klineStr)
		strBuilder.WriteString("] ")
	}
	common.Info(strBuilder.String())
}

/*
check redis kline between 2022-2027(5year).
fatal if any error
*/
func (L *L2quote) checkRedisKline() {
	// check 1min,3min,5min,15min,30min,60min,2hour,4hour,6hour,8hour,12hour,1day
	// partitioned by year
	//for _, klineType := range []string{"1min", "3min", "5min", "15min", "30min", "60min", "2hour", "4hour", "6hour", "8hour", "12hour", "1day"} {

	for _, klineType := range []string{"1min", "5min", "15min", "30min", "60min", "4hour", "1day"} {

		for _, year := range []int{2022} {
			startTime, _, length := common.GetListTsOffLen(common.HmName2Type[klineType], int(time.Date(year, 1,
				1, 0, 0, 0, 0, common.Location).Unix()), 0)
			key := fmt.Sprintf("market.%s.kline.%s.%d", L.symbol, klineType, startTime)
			res := L.redisClient.LLen(L.ctx, key)

			// redis connect error？
			if res.Err() != nil {
				common.Fatalf(L.symbol, "l2quote check redis kline error: ", res.Err())
			}

			if int(res.Val()) != length {
				// log.Println(fmt.Sprintf("%s l2quote check %s redis kline error, expected %d, get %d",
				// 	L.symbol, key, length, res.Val()))
				common.Warn(fmt.Sprintf("%s l2quote check %s redis kline error, expected %d, get %d",
					L.symbol, key, length, res.Val()))
			}
		}
	}
	// check 1week,1mon
	// no partition
	for _, klineType := range []string{"1mon"} {
		startTime, _, length := common.GetListTsOffLen(common.HmName2Type[klineType], int(time.Date(2022, 1,
			1, 0, 0, 0, 0, common.Location).Unix()), 0)
		key := fmt.Sprintf("market.%s.kline.%s.%d", L.symbol, klineType, startTime)
		res := L.redisClient.LLen(L.ctx, key)
		// redis connect error？
		if res.Err() != nil {
			common.Fatal(L.symbol, "l2quote check redis kline error: ", res.Err())
		}

		if int(res.Val()) != length {
			common.Warn(fmt.Sprintf("%s l2quote check %s redis kline error, expected %d, get %d",
				L.symbol, key, length, res.Val()))
		}
	}
}
