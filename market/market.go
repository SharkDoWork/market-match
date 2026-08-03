// Package market 负责根据撮合引擎的订单簿（OrderBook）生成市场深度数据，
// 并将深度数据通过消息队列上报给行情服务。深度数据包括买卖盘口各价位的挂单量，
// 支持按固定步长聚合（step）、按百分比聚合（percent10）以及不聚合的原始深度。
package market

import (
	"github.com/shopspring/decimal"
	"market-match/common"
	"market-match/config"
)

var (
	registration map[string]*MarketThread     // 不同的交易对一个Thread, Thread是刚开始的设计，目前看后面可以精简掉
	depthChan    map[string]chan *QuoteDepths // 不同的交易所与不同的交易对各有一个chan，用于异步传递深度数据
)

func init() {
	decimal.MarshalJSONWithoutQuotes = true // 让 decimal 序列化为 JSON 数字而非字符串
	registration = make(map[string]*MarketThread)
	depthChan = make(map[string]chan *QuoteDepths)
}

// MarketThreadInit 初始化一个交易对的行情上报线程。
// 每个交易对（symbol）对应一个独立的 goroutine，负责将深度数据发送到 MQ。
func MarketThreadInit(exchange, symbol string) {

	//后边加上配置可控buffer大小
	ch := make(chan *QuoteDepths, config.GetInt64("exchange.depth.size", 1000))
	m := &MarketThread{
		symbol:    symbol,
		exchange:  exchange,
		depthChan: ch,
	}

	registration[symbol] = m

	common.Trace("MarketThreadInit symbol", symbol)
	depthChan[symbol] = ch

	go m.thread()
}
