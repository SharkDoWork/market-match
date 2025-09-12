package l2quote

import (
	"fmt"
	"market-match/common"
	"market-match/match"
	"sync"

	"github.com/shopspring/decimal"
)

type TradeDetailData struct {
	//Amount    decimal.Decimal `json:"amount"`
	Vol       decimal.Decimal `json:"vol"`
	Ts        int64           `json:"ts"`
	Id        int64           `json:"id"`
	Price     decimal.Decimal `json:"price"`
	Direction string          `json:"direction"`
}

type TradeDetail struct {
	Id       int64             `json:"id"` //seq-id of match result
	Ts       int64             `json:"ts"`
	Type     string            `json:"type"`
	PairCode string            `json:"pairCode"`
	Data     []TradeDetailData `json:"data"`
}

/*
trade detail绘制协程
负责将match result转换成下游需要的trade detail信息
*/
func (L *L2quote) trade(mrCh chan *match.MatchResult, wg *sync.WaitGroup) {
	for {
		select {
		case mr := <-mrCh:
			if L.symbol != mr.Symbol {
				common.Fatal("trade mr", mr)
			}
			// 把每一条mr打成下游需要的消息包，送到mq消息处理队列
			L.sendTradeDetail(mr)
			wg.Done()
		}
	}
}

/*
把match result格式化成下游需要的消息格式，并发送到mq发送队列
*/
func (L *L2quote) sendTradeDetail(mr *match.MatchResult) {
	// 挂单不处理
	if isPendingOrder(mr) {
		return
	}

	var direction string
	if "sell-limit" == mr.OrderTypeStr ||
		"sell-market" == mr.OrderTypeStr ||
		"sell-ioc" == mr.OrderTypeStr ||
		"sell-fok" == mr.OrderTypeStr {
		direction = "sell"
	} else if "buy-limit" == mr.OrderTypeStr ||
		"buy-market" == mr.OrderTypeStr ||
		"buy-ioc" == mr.OrderTypeStr ||
		"buy-fok" == mr.OrderTypeStr {
		direction = "buy"
	}

	var data []TradeDetailData
	for i := range mr.Items {
		if mr.Items[i].Role == "maker" &&
			(mr.Items[i].State == "filled" ||
				mr.Items[i].State == "partial-filled") {
			id := mr.Id*10000 + int64(i)
			d := TradeDetailData{
				Vol:       *mr.Items[i].FilledAmount,
				Ts:        mr.Ts,
				Id:        id,
				Price:     *mr.Items[i].Price,
				Direction: direction}
			data = append(data, d)
		}
	}

	pkg := &TradeDetail{Id: mr.Id,
		PairCode: mr.Symbol,
		Ts:       mr.Ts,
		Type:     "market.fills",
		Data:     data}

	body, err := json.Marshal(pkg)
	if err != nil {
		common.Fatal(L.symbol, "l2quote match result json marshal error", err)
	}

	routingKey := fmt.Sprintf("market.%s.trade.detail", mr.Symbol)
	mqMsg := MqMessage{RoutingKey: routingKey,
		Body: body}

	common.Trace("[sendTradeDetail] TradeDetail:", mqMsg)
	L.mqSendChan <- &mqMsg
}
