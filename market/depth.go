package market

import (
	"market-match/common"
	"market-match/config"
	"market-match/match"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

var ()

// Depth 表示一个价格档位上的深度数据。
// PriceAmount[0] 为价格，PriceAmount[1] 为该价格对应的挂单总量。
type Depth struct {
	PriceAmount [2]decimal.Decimal //[0]:Price [1]:Amount
}

// QuoteDepths 是上报给行情服务的深度数据结构。
// Bids 为买方盘口各价位挂单量（价格从高到低），Asks 为卖方盘口各价位挂单量（价格从低到高）。
// 每个价位用 [2]decimal.Decimal 表示：[0] 价格，[1] 该价位的挂单总量。
type QuoteDepths struct {
	SeqId    int64                `json:"seqId"`    // 订单簿当前处理到的撮合序号，用于客户端去重/排序
	ID       int64                `json:"id"`       // 深度数据 ID，通常与 Version 相同
	Bids     [][2]decimal.Decimal `json:"bids,omitempty"` // 买方深度，[价格, 挂单量]
	Asks     [][2]decimal.Decimal `json:"asks,omitempty"` // 卖方深度，[价格, 挂单量]
	Ts       int64                `json:"ts"`       // 深度数据生成时间戳（毫秒）
	Version  int64                `json:"version"`  // 版本号，按固定间隔递增
	Type     string               `json:"type"`     // 数据类型，如 "market.orderBook" 或 "market.percent10"
	PairCode string               `json:"pairCode"` // 交易对代码
	Interval string               `json:"interval,omitempty"` // 聚合步长名称，如 "step0"、"step1"
	ch       string               // 内部使用的路由键（routing key），不序列化
}

// BuildAndReportDepth 根据订单簿生成全量深度数据，并写入 channel 异步上报。
// 生成深度同步版本
func BuildAndReportDepth(book *match.OrderBook) {

	//ts00 := time.Now().UnixNano()

	depths := buildDepth(book)

	//ts01 := time.Now().UnixNano()

	for _, depth := range depths {
		depthChan[book.Symbol] <- depth
	}

	/*
		ts02 := time.Now().UnixNano()

		tsAll := (ts02 - ts00) / int64(time.Millisecond)
		tsBuild := (ts01 - ts00) / int64(time.Millisecond)
		tsQue := (ts02 - ts01) / int64(time.Millisecond)

		if tsAll > 200 {
			common.Info("DEPTH|BuildAndReportDepth|timeout|tsAll:", tsAll, ",tsBuild:", tsBuild, ", tsQue:", tsQue, ", symbol:", book.Symbol)
		}

	*/
}

// buildDepth 从订单簿中取出买卖订单，按配置的多个步长（DepthSteps）分别聚合生成深度数据。
// 将生成的过程拆出来，方便做单测
func buildDepth(book *match.OrderBook) []*QuoteDepths {

	// 从订单簿中取出买卖两侧的订单，数量受配置限制
	buyOrders, sellOrders := takeOrdersFromBookLimit(book, config.GetInt64("exchange.l2quote.size", 1800))

	depthList := make([]*QuoteDepths, len(common.GetSymbolInfo(book.Symbol).DepthSteps))
	steps := common.GetSymbolInfo(book.Symbol).DepthSteps

	var tsBids int64 = 0
	var tsAsks int64 = 0

	var taskDone int32 = 0

	// 每个步长启动一个 goroutine 并行聚合，提高深度生成速度
	for index, depthStep := range steps {
		go buildDepthStep(index, depthStep, buyOrders, sellOrders, book, depthList, &taskDone, &tsBids, &tsAsks)
	}

	var stepCount = int32(len(steps))
	for taskDone != stepCount {
		time.Sleep(10 * time.Millisecond)
	}

	return depthList
}

// buildDepthStep 按指定步长（DepthStep）聚合买卖订单，生成一个步长对应的深度数据。
// 聚合规则：买单价格向下取整到步长倍数，卖单价格向上取整到步长倍数，相同价位的订单量累加。
func buildDepthStep(index int,
	depthStep common.DepthStep,
	buyOrders []*match.Order,
	sellOrders []*match.Order,
	book *match.OrderBook,
	depthList []*QuoteDepths,
	taskDone *int32,
	tsBids *int64,
	tsAsks *int64,
) {
	ts00 := time.Now().UnixNano()

	bids := orderDepth(buyOrders,
		depthStep.Accuracy,
		depthStep.Capacity,
		false)

	ts01 := time.Now().UnixNano()

	asks := orderDepth(sellOrders,
		depthStep.Accuracy,
		depthStep.Capacity,
		true)

	//这一步先不要直接发到MQ，塞到chan里面做一次异步，因为该方法与撮合同步调用，不能让mq的性能成为撮合瓶颈
	//注意step后面没有'.'，与线上保持一致
	var routingKey string
	routingKey = "market." + book.Symbol + ".depth.step" + depthStep.Name
	//common.Trace("[buildDepth] symbol=", routingKey)

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	quoteDepths := &QuoteDepths{
		Bids:     bids,
		Asks:     asks,
		Ts:       ts,
		Version:  ts / config.GetInt64("market.default-update-interval-ms", 1000),
		ID:       ts / config.GetInt64("market.default-update-interval-ms", 1000),
		ch:       routingKey,
		SeqId:    book.FromId,
		Type:     "market.orderBook",
		PairCode: book.Symbol,
		Interval: depthStep.Name,
	}
	depthList[index] = quoteDepths

	ts02 := time.Now().UnixNano()

	_tsBids := ((ts01 - ts00) / int64(time.Millisecond))
	_tsAsks := ((ts02 - ts01) / int64(time.Millisecond))
	//_tsAll := _tsBids + _tsAsks

	atomic.AddInt64(tsBids, _tsBids)
	atomic.AddInt64(tsAsks, _tsAsks)
	atomic.AddInt32(taskDone, 1)

}

// BuildAndReportDepthPercent10 生成百分比深度（DepthPercent10）并上报。
// 百分比深度以盘口中间价为基准，按固定百分比间隔（如 0.1%、0.2% ...）统计各价位的累计挂单量，
// 常用于交易页面的"深度图"展示。
func BuildAndReportDepthPercent10(book *match.OrderBook) {
	quoteDepths := buildDepthPercent10(book)
	depthChan[book.Symbol] <- quoteDepths
}

// buildDepthPercent10 构建百分比深度数据。
// 中间价取买卖盘口最优价的平均值；步长 = 中间价 / (capacity * 10)，
// 即每个档位代表 0.1% 的价格偏移，共 capacity 个档位（默认 10 档，覆盖 1% 范围）。
func buildDepthPercent10(book *match.OrderBook) *QuoteDepths {
	//calculate midPrice
	var midPrice, step decimal.Decimal
	var bids, asks [][2]decimal.Decimal
	empty := false

	buyOrders, sellOrders := takeOrdersFromBookLimit(book, config.GetInt64("exchange.l2quote.size", 1800))
	buyOrderLen := len(buyOrders)
	sellOrderLen := len(sellOrders)

	if buyOrderLen == 0 && sellOrderLen == 0 {
		//双向都没有挂单
		//发送一个默认的空包
		empty = true
	} else if buyOrderLen == 0 {
		//only买单为空
		midPrice = sellOrders[0].Price
	} else if sellOrderLen == 0 {
		//only卖单为空
		midPrice = buyOrders[0].Price
	} else {
		//双向都有挂单
		//取两侧挂单队首的中值
		midPrice = decimal.Avg(buyOrders[0].Price, sellOrders[0].Price)
	}

	capacity := common.GetSymbolInfo(book.Symbol).Depth10PercentCapacity
	priceScale := common.GetSymbolInfo(book.Symbol).PriceScale

	if !empty {
		step = midPrice.Div(decimal.New(capacity*10, 0)).Truncate(priceScale)
		bids = orderAccumulatedDepth(buyOrders, midPrice, step, capacity, false)
		asks = orderAccumulatedDepth(sellOrders, midPrice, step, capacity, true)
	}

	var routingKey string
	routingKey = "market." + book.Symbol + ".depth.percent10"
	common.Trace("buildDepthPercent10 book symbol=", routingKey)

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	quoteDepths := &QuoteDepths{
		Bids:     bids,
		Asks:     asks,
		Ts:       ts,
		Version:  ts / 1000,
		ID:       ts / 1000,
		ch:       routingKey,
		SeqId:    book.FromId,
		Type:     "market.percent10",
		PairCode: book.Symbol,
	}
	return quoteDepths
}

// takeOrdersFromBookLimit 从订单簿中取出买卖两侧的前 limit 条订单。
// 订单已按价格排序：买单从高到低，卖单从低到高。
func takeOrdersFromBookLimit(book *match.OrderBook, limit int64) (buyOrders []*match.Order, sellOrders []*match.Order) {
	buyOrders = book.Take(match.Buy, limit)
	sellOrders = book.Take(match.Sell, limit)
	return
}

// roundUp 将价格向上取整到 step 的整数倍。
// 用于卖单（asks）聚合，保证同一档位的价格一致。
func roundUp(d decimal.Decimal, step float64) decimal.Decimal {
	dStep := decimal.NewFromFloat(step)
	m := d.Mod(dStep)
	if m.GreaterThan(decimal.Zero) {
		return d.Sub(m).Add(dStep)
	}
	return d
}

// roundDown 将价格向下取整到 step 的整数倍。
// 用于买单（bids）聚合，保证同一档位的价格一致。
func roundDown(d decimal.Decimal, step float64) decimal.Decimal {
	dStep := decimal.NewFromFloat(step)
	return d.Div(dStep).Truncate(0).Mul(dStep)
}

// orderDepth 将订单列表按步长聚合成深度档位。
// 买单向下取整，卖单向上取整，相同档位的订单量累加，最多返回 capacity 个档位。
func orderDepth(orders []*match.Order, step float64, capacity int64, isSell bool) (depths [][2]decimal.Decimal) {

	var stepPrice decimal.Decimal
	for _, order := range orders {
		if isSell {
			stepPrice = roundUp(order.Price, step)
		} else {
			stepPrice = roundDown(order.Price, step)
		}
		// 应对币种降价 聚合精度变化 比如某币种聚合精度为1  盘口价格降到0.8
		//if stepPrice.Equal(decimal.Zero) {
		//	stepPrice = decimal.NewFromFloat(step)
		//}
		if len(depths) == 0 || !depths[len(depths)-1][0].Equal(stepPrice) {
			if len(depths) == int(capacity) {
				return
			}
			depths = append(depths, [2]decimal.Decimal{stepPrice, order.UnfilledAmount})
		} else {
			depths[len(depths)-1][1] = depths[len(depths)-1][1].Add(order.UnfilledAmount)
		}

	}

	return
}

// orderAccumulatedDepth 生成累计深度档位，用于交易页下方的"深度图"。
// 与 orderDepth 不同，本函数强制生成所有 capacity 个档位（即使某档位挂单量为 0），
// 且每个档位的 amount 是截止当前价格的所有订单 amount 总和（累计值而非单档值）。
// 积累深度档, 用于出交易页下方“深度图”
// amount为截止当前价格的所有订单amount总和
// 强制生成所有梯度的数据，哪怕为0
func orderAccumulatedDepth(orders []*match.Order, midPrice, step decimal.Decimal, capacity int64,
	isSell bool) (depths [][2]decimal.Decimal) {

	//build up price array
	var price decimal.Decimal
	for i := int64(1); i <= capacity; i++ {
		if isSell {
			price = midPrice.Add(step.Mul(decimal.New(i, 0)))
		} else {
			price = midPrice.Sub(step.Mul(decimal.New(i, 0)))
		}

		var depth = Depth{
			PriceAmount: [2]decimal.Decimal{price, decimal.New(0, 0)},
		}
		depths = append(depths, depth.PriceAmount)
	}

	//calculate amount in each price window
	for _, order := range orders {
		var id int
		if isSell {
			id = int(order.Price.Sub(midPrice).Div(step).IntPart())
		} else {
			id = int(midPrice.Sub(order.Price).Div(step).IntPart())
		}
		if id < 0 || id >= int(capacity) {
			break
		}
		depths[id][1] = depths[id][1].Add(order.UnfilledAmount) //[1]Amount
	}

	return depths
}
