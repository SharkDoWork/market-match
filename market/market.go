package market

import (
	"github.com/shopspring/decimal"
	"market-match/common"
	"market-match/config"
)

var (
	registration map[string]*MarketThread     //不同的交易对一个Thread, Thread是刚开始的设计，目前看后面可以精简掉
	depthChan    map[string]chan *QuoteDepths //不同的交易所与不同的交易对各有一个chan
)

func init() {
	decimal.MarshalJSONWithoutQuotes = true
	registration = make(map[string]*MarketThread)
	depthChan = make(map[string]chan *QuoteDepths)
}

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
