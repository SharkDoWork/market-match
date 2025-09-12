package l2quote

import (
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"market-match/common"
	"market-match/match"
	"sync"
	"time"
)

const (
	ONEDAYSECONDS = 60 * 60 * 24
)

/*
market detail绘制协程
负责检查是否需要新建market detail、根据match result更新market detail
*/
func (L *L2quote) market(mrCh chan *match.MatchResult, wg *sync.WaitGroup) {
	sendTicker := time.NewTicker(time.Millisecond * cast.ToDuration(L.mqSendIntervalMS))
	sendFlushTicker := time.NewTicker(time.Second * 1)
	for {
		select {
		// 检查是否需要新建market，并发送当前market信息
		case <-sendTicker.C:
			if L.quotation.MaxMRId > L.sendMarketMRID {
				curTS := time.Now().Unix()
				// 检查是否需要重建market，需要则重建
				L.buildMarket(curTS)
				// 发送market detail，只是发送到mq消息处理队里，不是实时发送
				L.sendMarketDetail(curTS)
				L.sendMarketMRID = L.quotation.MaxMRId
			}
		case <-sendFlushTicker.C:
			curTS := time.Now().Unix()
			// 检查是否需要重建market，需要则重建
			L.buildMarket(curTS)
			// 发送market detail，只是发送到mq消息处理队里，不是实时发送
			L.sendMarketDetail(curTS)
		case mr := <-mrCh:
			// 更新前先检查是否需要重建
			curTS := time.Now().Unix()
			L.buildMarket(curTS)
			// 更新market
			L.updateMarketDetail(mr, curTS)
			wg.Done()
		}
	}
}

func (L *L2quote) syncTickerMessage() {

	L.ticker.TS = L.marketDetail.TS
	L.ticker.SeqId = L.marketDetail.SeqId
	L.ticker.HighPrice = L.marketDetail.HighPrice
	L.ticker.LowPrice = L.marketDetail.LowPrice
	L.ticker.OpenPrice = L.marketDetail.OpenPrice
	L.ticker.ClosePrice = L.marketDetail.ClosePrice
	L.ticker.Vol = L.marketDetail.Vol
	L.ticker.TurnOver = L.marketDetail.TurnOver
}

func (L *L2quote) buildMarket(ts int64) {
	L.buildMarket24Hour(ts)

	//build ticker
	L.syncTickerMessage()
}

/*
1.当天版本跟1day kline 信息相同
2. L.quotation.Market 只存储1个的数据, 所以只取0
*/
func (L *L2quote) buildMarketCurDay(ts int64) {

	curTS := int64(common.CurWindowTime(int(ts), common.Type1Day, 0))

	// 当前时间段已经存在，且不是初始化状态则不更新
	if curTS == L.quotation.Market[0].TS && L.marketDetail != nil {
		return
	}

	cur := 0
	marketDetail := &kline{}

	if curTS-L.quotation.Market[cur].TS >= ONEDAYSECONDS {
		//跨天,即新的一天,在没 mr 更新的时候, 价格都取前一天的 closePrice, 量都为0
		lastPrice := L.quotation.Market[cur].ClosePrice
		marketDetail.OpenPrice = lastPrice
		marketDetail.HighPrice = lastPrice
		marketDetail.LowPrice = lastPrice
		marketDetail.ClosePrice = lastPrice

		marketDetail.TurnOver = decimal.Zero
		marketDetail.Vol = decimal.Zero
		marketDetail.Count = 0
	} else {
		// 没有跨天,直接按从快照恢复的缓存复制 价格 和 量
		marketDetail.OpenPrice = L.quotation.Market[cur].OpenPrice
		marketDetail.HighPrice = L.quotation.Market[cur].HighPrice
		marketDetail.LowPrice = L.quotation.Market[cur].LowPrice
		marketDetail.ClosePrice = L.quotation.Market[cur].ClosePrice

		marketDetail.TurnOver = L.quotation.Market[cur].TurnOver
		marketDetail.Vol = L.quotation.Market[cur].Vol
		marketDetail.Count = L.quotation.Market[cur].Count
	}

	marketDetail.SeqId = L.quotation.Market[cur].SeqId
	marketDetail.TS = L.quotation.Market[cur].TS

	common.Trace("mrde build :", marketDetail)

	L.marketDetail = marketDetail
	L.quotation.Market[cur] = *marketDetail
}

/*
更新market 24小时汇总信息

由1min颗粒度kline建立最近24小时的汇总信息。
*/
func (L *L2quote) buildMarket24Hour(ts int64) {

	curTS := int64(common.CurWindowTime(int(ts), common.Type1Minute, 0))

	// 当前时间段已经存在，且不是初始化状态则不更新
	if curTS == L.quotation.Market[0].TS && L.marketDetail != nil {
		return
	}

	trancateOffset := (curTS - L.quotation.Market[0].TS) / 60
	// 快照中market已经是1天前的数据
	if trancateOffset > 24*60 {
		trancateOffset = 24 * 60
	}
	newMarket := make([]kline, trancateOffset, 24*60)
	lastPrice := L.quotation.Market[0].ClosePrice
	for i := 0; i < int(trancateOffset); i++ {
		newMarket[i] = kline{TS: curTS - int64(60*i), OpenPrice: lastPrice, ClosePrice: lastPrice, HighPrice: lastPrice, LowPrice: lastPrice}
	}
	L.quotation.Market = append(newMarket, L.quotation.Market[:len(L.quotation.Market)-int(trancateOffset)]...)

	marketDetail := &kline{}
	// 生成24hour汇总数据
	for i := 24*60 - 1; i >= 0; i-- {
		if marketDetail.OpenPrice.Equal(decimal.Zero) {
			marketDetail.OpenPrice = L.quotation.Market[i].OpenPrice
		}

		if marketDetail.HighPrice.LessThan(L.quotation.Market[i].HighPrice) {
			marketDetail.HighPrice = L.quotation.Market[i].HighPrice
		}

		if marketDetail.LowPrice.Equal(decimal.Zero) || marketDetail.LowPrice.GreaterThan(L.quotation.Market[i].LowPrice) {
			marketDetail.LowPrice = L.quotation.Market[i].LowPrice
		}

		marketDetail.TurnOver = marketDetail.TurnOver.Add(L.quotation.Market[i].TurnOver)
		marketDetail.Vol = marketDetail.Vol.Add(L.quotation.Market[i].Vol)
		marketDetail.Count += L.quotation.Market[i].Count
	}
	marketDetail.ClosePrice = L.quotation.Market[0].ClosePrice
	marketDetail.SeqId = L.quotation.MaxMRId
	marketDetail.TS = curTS
	L.marketDetail = marketDetail
}

func (L *L2quote) buildMarketPriceCurDayVol24Hour(ts int64) {

	curTS := int64(common.CurWindowTime(int(ts), common.Type1Minute, 0))

	curDayStartTS := int64(common.CurWindowTime(int(ts), common.Type1Day, 0))

	// 当前时间段已经存在，且不是初始化状态则不更新
	if curTS == L.quotation.Market[0].TS && L.marketDetail != nil {
		return
	}

	trancateOffset := (curTS - L.quotation.Market[0].TS) / 60
	// 快照中market已经是1天前的数据
	if trancateOffset > 24*60 {
		trancateOffset = 24 * 60
	}
	newMarket := make([]kline, trancateOffset, 24*60)
	lastPrice := L.quotation.Market[0].ClosePrice
	for i := 0; i < int(trancateOffset); i++ {
		newMarket[i] = kline{TS: curTS - int64(60*i), OpenPrice: lastPrice, ClosePrice: lastPrice, HighPrice: lastPrice, LowPrice: lastPrice}
	}
	L.quotation.Market = append(newMarket, L.quotation.Market[:len(L.quotation.Market)-int(trancateOffset)]...)

	marketDetail := &kline{}
	curUpdateFlag := true
	// 生成24hour汇总数据
	for i := 24*60 - 1; i >= 0; i-- {
		common.Trace("mrde line", L.symbol, "|", i, "|", L.quotation.Market[i], "|", curDayStartTS, "|", curUpdateFlag)
		if L.quotation.Market[i].TS >= curDayStartTS {
			if marketDetail.OpenPrice.Equal(decimal.Zero) || curUpdateFlag {
				if !L.quotation.Market[i].Vol.Equal(decimal.Zero) {
					marketDetail.OpenPrice = L.quotation.Market[i].OpenPrice
					marketDetail.HighPrice = L.quotation.Market[i].HighPrice
					marketDetail.LowPrice = L.quotation.Market[i].LowPrice
					curUpdateFlag = false
				}
			}

			if marketDetail.HighPrice.LessThan(L.quotation.Market[i].HighPrice) {
				marketDetail.HighPrice = L.quotation.Market[i].HighPrice
			}

			if marketDetail.LowPrice.Equal(decimal.Zero) || marketDetail.LowPrice.GreaterThan(L.quotation.Market[i].LowPrice) {
				marketDetail.LowPrice = L.quotation.Market[i].LowPrice
			}

		} else {
			marketDetail.OpenPrice = L.quotation.Market[i].ClosePrice
			marketDetail.HighPrice = L.quotation.Market[i].ClosePrice
			marketDetail.LowPrice = L.quotation.Market[i].ClosePrice
		}

		marketDetail.TurnOver = marketDetail.TurnOver.Add(L.quotation.Market[i].TurnOver)
		marketDetail.Vol = marketDetail.Vol.Add(L.quotation.Market[i].Vol)

		marketDetail.Count += L.quotation.Market[i].Count
	}
	marketDetail.ClosePrice = L.quotation.Market[0].ClosePrice
	marketDetail.SeqId = L.quotation.MaxMRId
	marketDetail.TS = curTS

	common.Trace("mrde build:", marketDetail, L.symbol)

	L.marketDetail = marketDetail
}

/*
根据match result更新24小时汇总信息
*/
func (L *L2quote) updateMarketDetail(mr *match.MatchResult, ts int64) {
	L.updateMarketDetail24Hour(mr, ts)

	L.syncTickerMessage()
}

func (L *L2quote) updateMarketDetail24Hour(mr *match.MatchResult, ts int64) {

	// 挂单不处理
	if isPendingOrder(mr) {
		return
	}

	mrTimeWindow := int64(common.CurWindowTime(int(mr.Ts/1000), common.Type1Minute, 0))
	offset := (ts - mrTimeWindow) / 60

	onlyUpdatePrice := false
	// 24小时以前的只更新价格
	if offset > 24*60-1 {
		onlyUpdatePrice = true
		offset = 24*60 - 1
	}

	// [0]为最新数据段，从mr所在offset迭代更新到最近分钟
	for i := offset; i >= 0; i-- {
		marketK := &L.quotation.Market[i]
		newPrice := getStartPrice(mr)
		if newPrice == nil {
			continue
		}
		// 更新mr所在时间段
		if i == offset && !onlyUpdatePrice {
			if marketK.Count == 0 {
				marketK.OpenPrice = *newPrice
				marketK.HighPrice = *newPrice
				marketK.LowPrice = *newPrice
			}

			marketK.ClosePrice = mr.Price
			marketK.SeqId = mr.Id

			for _, item := range mr.Items {
				if item.Role == "maker" && stateIsFilled(item.State) {
					if item.Price == nil {
						continue
					}
					// high price
					if item.Price.GreaterThan(marketK.HighPrice) {
						marketK.HighPrice = *item.Price
					}

					// low price
					if item.Price.LessThan(marketK.LowPrice) || marketK.LowPrice.Equal(decimal.Zero) {
						marketK.LowPrice = *item.Price
					}

					marketK.Vol = marketK.Vol.Add(*item.FilledAmount)
					marketK.TurnOver = marketK.TurnOver.Add(item.FilledAmount.Mul(*item.Price))

					marketK.Count++
				}
			}
		} else { // 迭代更新mr所在时间窗到当前时间窗
			marketK.OpenPrice = mr.Price
			marketK.ClosePrice = mr.Price
			marketK.HighPrice = mr.Price
			marketK.LowPrice = mr.Price

			marketK.SeqId = mr.Id
		}
	}

	// 更新汇总market detail
	for _, item := range mr.Items {
		if item.Role == "maker" && !onlyUpdatePrice && stateIsFilled(item.State) {
			if item.Price == nil {
				continue
			}

			L.marketDetail.ClosePrice = *item.Price

			if L.marketDetail.OpenPrice.Equal(decimal.Zero) {
				L.marketDetail.OpenPrice = *item.Price
			}

			// high price
			if item.Price.GreaterThan(L.marketDetail.HighPrice) {
				L.marketDetail.HighPrice = *item.Price
			}

			// low price
			if item.Price.LessThan(L.marketDetail.LowPrice) || L.marketDetail.LowPrice.Equal(decimal.Zero) {
				L.marketDetail.LowPrice = *item.Price
			}

			L.marketDetail.Vol = L.marketDetail.Vol.Add(*item.FilledAmount)
			L.marketDetail.TurnOver = L.marketDetail.TurnOver.Add(item.FilledAmount.Mul(*item.Price))

			L.marketDetail.Count++
		}
	}

	L.marketDetail.TS = mrTimeWindow
	L.marketDetail.SeqId = mr.Id
}

func (L *L2quote) updateMarketDetailPriceCurDayVol24Hour(mr *match.MatchResult, ts int64) {

	// 挂单不处理
	if isPendingOrder(mr) {
		return
	}

	mrTimeWindow := int64(common.CurWindowTime(int(mr.Ts/1000), common.Type1Minute, 0))
	//mr 可以为某一时刻启动后**旧的MR**,所以需要计算距离当前时间的 时间差/60 = offset
	offset := (ts - mrTimeWindow) / 60

	onlyUpdatePrice := false
	// 24小时以前的只更新价格
	if offset > 24*60-1 {
		onlyUpdatePrice = true
		offset = 24*60 - 1
	}

	// [0]为最新数据段，从mr所在offset迭代更新到最近分钟
	for i := offset; i >= 0; i-- {
		marketK := &L.quotation.Market[i]
		newPrice := getStartPrice(mr)
		if newPrice == nil {
			continue
		}
		// 更新mr所在时间段
		if i == offset && !onlyUpdatePrice {
			if marketK.Count == 0 {
				marketK.OpenPrice = *newPrice
				marketK.HighPrice = *newPrice
				marketK.LowPrice = *newPrice
			}

			marketK.ClosePrice = mr.Price
			marketK.SeqId = mr.Id

			for _, item := range mr.Items {
				if item.Role == "maker" && item.Price != nil {
					// high price
					if item.Price.GreaterThan(marketK.HighPrice) {
						marketK.HighPrice = *item.Price
					}

					// low price
					if item.Price.LessThan(marketK.LowPrice) || marketK.LowPrice.Equal(decimal.Zero) {
						marketK.LowPrice = *item.Price
					}
					marketK.Vol = marketK.Vol.Add(*item.FilledAmount)
					marketK.TurnOver = marketK.TurnOver.Add(item.FilledAmount.Mul(*item.Price))

					marketK.Count++
				}
			}
		} else { // 迭代更新mr所在时间窗到当前时间窗
			marketK.OpenPrice = mr.Price
			marketK.ClosePrice = mr.Price
			marketK.HighPrice = mr.Price
			marketK.LowPrice = mr.Price
			marketK.SeqId = mr.Id
		}
	}

	// 更新汇总market detail
	for _, item := range mr.Items {
		if item.Role == "maker" && !onlyUpdatePrice &&
			item.Price != nil {
			L.marketDetail.ClosePrice = *item.Price

			if L.marketDetail.OpenPrice.Equal(decimal.Zero) {
				L.marketDetail.OpenPrice = *item.Price
			}

			// high price
			if item.Price.GreaterThan(L.marketDetail.HighPrice) {
				L.marketDetail.HighPrice = *item.Price
			}

			// low price
			if item.Price.LessThan(L.marketDetail.LowPrice) || L.marketDetail.LowPrice.Equal(decimal.Zero) {
				L.marketDetail.LowPrice = *item.Price
			}

			L.marketDetail.Vol = L.marketDetail.Vol.Add(*item.FilledAmount)
			L.marketDetail.TurnOver = L.marketDetail.TurnOver.Add(item.FilledAmount.Mul(*item.Price))

			L.marketDetail.Count++
		}
	}

	common.Trace("mrde up:", L.marketDetail, "|", mr.Symbol, "|", L.symbol)

	L.marketDetail.TS = mrTimeWindow
	L.marketDetail.SeqId = mr.Id
}

/*
发送market detail消息到MQ管道
*/
func (L *L2quote) sendMarketDetail(ts int64) {

	/*
		body, err := json.Marshal(L.marketDetail)
		if err != nil {
			common.Fatal(L.symbol, "market detail marshal error :", err)
			return
		}

		var routingKey string

		routingKey = fmt.Sprintf("market.%s.detail", L.symbol)

		mqMsg := MqMessage{RoutingKey: routingKey, Body: body}

		common.Trace("[sendMarketDetail] data=", mqMsg)

		L.mqSendChan <- &mqMsg

	*/

	//sync Tickser message
	if L.ticker.AskVol.Equal(decimal.Zero) ||
		L.ticker.BidVol.Equal(decimal.Zero) {
		common.Debug("paircode:", L.symbol, " orderBooker warn!!!")
	}
	L.ticker.Change = L.ticker.ClosePrice.Sub(L.ticker.OpenPrice)
	if L.ticker.OpenPrice.Equal(decimal.Zero) {
		L.ticker.ChangePercent = decimal.Zero
	} else {
		L.ticker.ChangePercent = L.ticker.Change.Div(L.ticker.OpenPrice)
	}

	tk := QuoteTicker{Type: "market.ticker", PairCode: L.symbol, Ticker: L.ticker, SeqId: L.ticker.SeqId, TS: L.marketDetail.TS}

	tkBody, err := json.Marshal(tk)
	if err != nil {
		common.Fatal(L.symbol, "ticker message marshal error:", err)
		return
	}

	tkRouterKet := fmt.Sprintf("market.%s.ticker", L.symbol)

	tkMqMsg := MqMessage{RoutingKey: tkRouterKet, Body: tkBody}
	L.mqSendChan <- &tkMqMsg

}

//vol
func (L *L2quote) updateMarketDetailCurDay(mr *match.MatchResult, ts int64) {
	// 挂单不处理
	if isPendingOrder(mr) {
		return
	}

	marketK := L.marketDetail
	mrTimeWindow := int64(common.CurWindowTime(int(mr.Ts/1000), common.Type1Day, 0))

	for _, item := range mr.Items {
		if item.Role == "maker" && item.Price != nil {
			if mrTimeWindow-L.marketDetail.TS >= ONEDAYSECONDS ||
				L.marketDetail.OpenPrice.Equal(decimal.Zero) {
				// open price
				marketK.OpenPrice = *item.Price

				marketK.HighPrice = *item.Price
				marketK.LowPrice = *item.Price

			}

			// high price
			if item.Price.GreaterThan(marketK.HighPrice) {
				marketK.HighPrice = *item.Price
			}

			// low price
			if item.Price.LessThan(marketK.LowPrice) || marketK.LowPrice.Equal(decimal.Zero) {
				marketK.LowPrice = *item.Price
			}

			//close price
			marketK.ClosePrice = *item.Price

			marketK.TurnOver = marketK.TurnOver.Add(item.FilledAmount.Mul(*item.Price))
			marketK.Vol = marketK.Vol.Add(*item.FilledAmount)

			marketK.Count++
		}
	}

	marketK.TS = mrTimeWindow
	marketK.SeqId = mr.Id

	common.Trace("mrde up :", marketK)
	L.marketDetail = marketK
	L.quotation.Market[0] = *marketK
}
