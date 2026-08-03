package market

import (
	"market-match/common"
	"market-match/config"
	"market-match/match"
	"time"

	"github.com/shopspring/decimal"
)

var (
	MarketDefaultUpdateIntervalMs int64 = 1000 // 深度数据默认更新间隔（毫秒）
)

// BuildAndReportDepthUncombinded 生成不聚合的原始深度数据（Uncombined Depth）并上报。
// 与 BuildAndReportDepth 不同，本函数不对价格做步长聚合，每个订单的价格独立成档，
// 适用于需要精确到每一笔挂单价格的场景。
func BuildAndReportDepthUncombinded(book *match.OrderBook) {

	ts00 := time.Now().UnixNano()

	depths := buildDepthUncombinded(book)

	ts01 := time.Now().UnixNano()

	for _, depth := range depths {
		depthChan[book.Symbol] <- depth
	}

	ts02 := time.Now().UnixNano()

	tsAll := (ts02 - ts00) / int64(time.Millisecond)
	tsBuild := (ts01 - ts00) / int64(time.Millisecond)
	tsQue := (ts02 - ts01) / int64(time.Millisecond)

	//dogstatsd.TimeInMilliseconds("BuildAndReportDepthUncombinded", float64(tsAll))

	if tsAll > 50 {
		common.Info("DEPTH|BuildAndReportDepthUncombinded|timeout|tsAll:", tsAll, ",tsBuild:", tsBuild, ", tsQue:", tsQue, ", symbol:", book.Symbol)
	}
}

// buildDepthUncombinded 从订单簿中取出全部买卖订单，按配置的多个步长分别生成不聚合的深度数据。
// 将生成的过程拆出来，方便做单测
func buildDepthUncombinded(book *match.OrderBook) []*QuoteDepths {

	// buyOrders, sellOrders := takeOrdersFromBookLimit(book, viper.GetInt64("exchange.l2quote.size"))
	buyOrders, sellOrders := takeOrdersFromBookForUncombinded(book)

	steps := common.GetSymbolInfo(book.Symbol).UncombinedDepthSteps
	var stepCount = int32(len(steps))
	depthCount := stepCount

	depthList := make([]*QuoteDepths, depthCount)

	var tsBids int64 = 0
	var tsAsks int64 = 0

	var taskDone int32 = 0

	for index, depthStep := range steps {
		// go buildDepthStep(index, depthStep, int(stepCount), buyOrders, sellOrders, book, depthList, &taskDone, &tsBids, &tsAsks)
		buildDepthStepForUncombinded(index, depthStep, int(stepCount), buyOrders, sellOrders, book, depthList, &taskDone, &tsBids, &tsAsks)
	}

	for taskDone != stepCount {
		time.Sleep(3 * time.Millisecond)
	}

	if (tsBids + tsAsks) > 30 {
		common.Info("DEPTH-LOCAL|buildDepthUncombinded|timeout|tsBids:", tsBids, ", tsAsks:", tsAsks, "book.sell.size:", book.SellSet.Size(), "book.buy.size:", book.BuySet.Size(), ", symbol:", book.Symbol)
	}

	return depthList
}

// buildDepthStepForUncombinded 按指定步长生成不聚合的深度数据。
// 注意：虽然传入 step 参数，但实际取整函数不做任何舍入，每个订单价格独立成档。
func buildDepthStepForUncombinded(index int,
	depthStep common.DepthStep,
	stepCount int,
	buyOrders []*match.Order,
	sellOrders []*match.Order,
	book *match.OrderBook,
	depthList []*QuoteDepths,
	taskDone *int32,
	tsBids *int64,
	tsAsks *int64,
) {
	bids := orderDepthForUncombinded(buyOrders,
		depthStep.Accuracy,
		depthStep.Capacity,
		false)

	asks := orderDepthForUncombinded(sellOrders,
		depthStep.Accuracy,
		depthStep.Capacity,
		true)

	//这一步先不要直接发到MQ，塞到chan里面做一次异步，因为该方法与撮合同步调用，不能让mq的性能成为撮合瓶颈
	//注意step后面没有'.'，与线上保持一致
	var routingKey string
	// var routingKeyCode string
	routingKey = "market." + book.Symbol + ".depth.size_" + depthStep.Name
	//common.Trace("[buildDepth] symbol=", routingKey)

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	quoteDepths := &QuoteDepths{
		Bids:    bids,
		Asks:    asks,
		Ts:      ts,
		Version: ts / config.GetInt64("market.default-update-interval-ms", 1000),
		// ID:      ts / viper.GetInt64("market.default-update-interval-ms"),
		ID:    book.FromId,
		ch:    routingKey,
		SeqId: book.FromId,
	}
	depthList[index] = quoteDepths

}

// takeOrdersFromBookForUncombinded 从订单簿中取出买卖两侧的全部订单。
// 与 takeOrdersFromBookLimit 不同，本函数不限制条数，取出整个盘口。
func takeOrdersFromBookForUncombinded(book *match.OrderBook) (buyOrders []*match.Order, sellOrders []*match.Order) {
	buyOrders = book.Take(match.Buy, int64(book.BuySet.Size()))
	sellOrders = book.Take(match.Sell, int64(book.SellSet.Size()))
	return
}

// orderDepthForUncombinded 将订单列表聚合成不聚合的深度档位。
// 与 orderDepth 的区别在于：取整函数不做任何舍入，每个订单价格独立成档，
// 因此相同价格的订单才会合并到同一档位。
func orderDepthForUncombinded(orders []*match.Order, step float64, capacity int64, isSell bool) (depths [][2]decimal.Decimal) {

	var tsRound int64 = 0
	var tsAppend int64 = 0

	var stepPrice decimal.Decimal

	for _, order := range orders {
		ts00 := time.Now().UnixNano()
		if isSell {
			stepPrice = roundUpForUncombinded(order.Price, step)
		} else {
			stepPrice = roundDownForUncombinded(order.Price, step)
		}
		ts01 := time.Now().UnixNano()
		if len(depths) == 0 || !depths[len(depths)-1][0].Equal(stepPrice) {
			if len(depths) == int(capacity) {
				return
			}
			depths = append(depths, [2]decimal.Decimal{stepPrice, order.UnfilledAmount})
		} else {
			depths[len(depths)-1][1] = depths[len(depths)-1][1].Add(order.UnfilledAmount)
		}
		ts02 := time.Now().UnixNano()
		_tsRound := (ts01 - ts00) / int64(time.Millisecond)
		_tsAppend := (ts02 - ts01) / int64(time.Millisecond)
		tsRound += _tsRound
		tsAppend += _tsAppend
	}

	tsAll := tsRound + tsAppend

	if tsAll > 6 {
		common.Info("DEPTH-LOCAL|orderDepthForUncombinded|timeout|tsAll:", tsAll, ", tsRound:", tsRound, ", tsAppend:", tsAppend, "size:", len(orders))
	}

	return
}

// roundUpForUncombinded 不聚合场景下的"向上取整"，实际直接返回原价格。
func roundUpForUncombinded(d decimal.Decimal, step float64) decimal.Decimal {
	return d
}

// roundDownForUncombinded 不聚合场景下的"向下取整"，实际直接返回原价格。
func roundDownForUncombinded(d decimal.Decimal, step float64) decimal.Decimal {
	return d
}
