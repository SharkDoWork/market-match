package l2quote

import (
	"github.com/shopspring/decimal"
)

type QuoteTicker struct {
	Type     string  `json:"type"`
	PairCode string  `json:"pairCode"`
	Interval string  `json:"interval"`
	Ticker   *Ticker `json:"data"`
	TS       int64   `json:"id"`
	SeqId    int64   `json:"seqId"`
}

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
