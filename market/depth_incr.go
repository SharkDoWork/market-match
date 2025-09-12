package market

import (
	"market-match/common"
	"market-match/config"
	"market-match/match"
	"time"

	"github.com/shopspring/decimal"
)

var (
	MarketDefaultUpdateIntervalMs int64 = 1000
)

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

//将生成的过程拆出来，方便做单测
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

func takeOrdersFromBookForUncombinded(book *match.OrderBook) (buyOrders []*match.Order, sellOrders []*match.Order) {
	buyOrders = book.Take(match.Buy, int64(book.BuySet.Size()))
	sellOrders = book.Take(match.Sell, int64(book.SellSet.Size()))
	return
}

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

func roundUpForUncombinded(d decimal.Decimal, step float64) decimal.Decimal {
	return d
}

func roundDownForUncombinded(d decimal.Decimal, step float64) decimal.Decimal {
	return d
}
