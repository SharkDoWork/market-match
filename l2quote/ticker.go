// Package l2quote 本文件定义实时 ticker 消息的数据结构。
// ticker 是面向交易前端的精简行情：在 24h 汇总（OHLCV）基础上，
// 叠加盘口买一/卖一价量（AskPrice/AskVol/BidPrice/BidVol）
// 以及涨跌额(Change)与涨跌幅(ChangePercent)。
// ticker 的行情部分由 marketDetail 同步而来（见 market.go syncTickerMessage），
// 买卖一价量由每条撮合结果直接更新（见 l2quote.go Run 主循环）。
package l2quote

import (
	"github.com/shopspring/decimal"
)

// QuoteTicker 发送到下游（MQ）的 ticker 消息封装格式，Type 固定为 "market.ticker"。
type QuoteTicker struct {
	Type     string  `json:"type"`
	PairCode string  `json:"pairCode"`
	Interval string  `json:"interval"`
	Ticker   *Ticker `json:"data"`
	TS       int64   `json:"id"`
	SeqId    int64   `json:"seqId"`
}

// Ticker 实时行情快照结构。
// Open/Close/High/Low/Vol/TurnOver 来自 24h 滚动汇总；
// AskPrice/AskVol/BidPrice/BidVol 为盘口最优卖一/买一价量；
// Change = Close - Open，ChangePercent = Change / Open。
type Ticker struct {
	TS            int64           `json:"id"`
	SeqId         int64           `json:"seqId"`
	OpenPrice     decimal.Decimal `json:"open"`
	ClosePrice    decimal.Decimal `json:"close"`
	HighPrice     decimal.Decimal `json:"high"`
	LowPrice      decimal.Decimal `json:"low"`
	Vol           decimal.Decimal `json:"vol"`
	TurnOver      decimal.Decimal `json:"turnOver"`
	AskPrice      decimal.Decimal `json:"askPrice"`
	AskVol        decimal.Decimal `json:"askVol"`
	BidPrice      decimal.Decimal `json:"bidPrice"`
	BidVol        decimal.Decimal `json:"bidVol"`
	Change        decimal.Decimal `json:"change"`
	ChangePercent decimal.Decimal `json:"changePercent"`
}

/*

func (L L2quote) Ticker(mrCh chan *match.MatchResult,  wg *sync.WaitGroup)  {
	sendTicker := time.NewTicker(time.Millisecond * cast.ToDuration(L.mqSendIntervalMS))
	sendFlushTicker := time.NewTicker(time.Second * 1


	for {
		select {
		case <-sendTicker.C:
			if L.quotation.MaxMRId > L.sendMarketMRID {

				L.quotation.MaxMRId = L.sendMarketMRID
			}

		case <-sendFlushTicker.C:



		case mr := <-mrCh:




			L.quotation.Ticker.AskPrice = mr.AskPrice
			L.quotation.Ticker.AskVol = mr.AskVol
			L.quotation.Ticker.BidPrice = mr.BidPrice
			L.quotation.Ticker.BidVol = mr.BidVol

			wg.Done()
		}
	}
}
*/
