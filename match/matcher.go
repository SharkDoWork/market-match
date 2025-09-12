package match

import (
	"market-match/common"
	"market-match/config"
	"market-match/rabbitmq"
	"reflect"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type OrderResult struct {
	OrderId        int64            `json:"order-id"`
	UserId         int64            `json:"user-id"`
	Role           string           `json:"role"`
	Price          *decimal.Decimal `json:"price,omitempty"`           // 为空 不会出现在json里面
	UnfilledAmount *decimal.Decimal `json:"unfilled-amount,omitempty"` // taker  只有 unfilledAmount
	FilledAmount   *decimal.Decimal `json:"filled-amount,omitempty"`   // maker 只有 filled-amount
	State          string           `json:"state,omitempty"`
	Taker          string           `json:"taker"`
	Maker          string           `json:"maker"`
}

// MarshalJSON todo stp?
func (result *OrderResult) MarshalJSON() ([]byte, error) {
	if result.Role == "maker" {
		return json.Marshal(struct {
			OrderId      int64            `json:"order-id"`
			UserId       int64            `json:"user-id"`
			Role         string           `json:"role"`
			Price        *decimal.Decimal `json:"price,omitempty"` // 为空 不会出现在json里面
			FilledAmount *decimal.Decimal `json:"filled-amount"`   // maker 只有 filled-amount
			State        string           `json:"state,omitempty"`
			Taker        string           `json:"taker"`
			Maker        string           `json:"maker"`
		}{
			OrderId:      result.OrderId,
			UserId:       result.UserId,
			Role:         result.Role,
			Price:        result.Price,
			FilledAmount: result.FilledAmount,
			State:        result.State,
			Taker:        result.Taker,
			Maker:        result.Maker,
		})

	} else if result.Role == "taker" {
		if result.State == "failed" { // 撤单 失败。
			return json.Marshal(struct {
				OrderId int64  `json:"order-id"`
				Role    string `json:"role"`
				State   string `json:"state,omitempty"`
				UserId  int64  `json:"user-id"`
				Taker   string `json:"taker"`
				Maker   string `json:"maker"`
			}{
				OrderId: result.OrderId,
				UserId:  result.UserId,
				Role:    result.Role,
				State:   result.State,
				Taker:   result.Taker,
				Maker:   result.Maker,
			})

		} else {
			return json.Marshal(struct {
				UserId         int64            `json:"user-id"`
				OrderId        int64            `json:"order-id"`
				Role           string           `json:"role"`
				Price          *decimal.Decimal `json:"price,omitempty"` // 为空 不会出现在json里面
				UnfilledAmount *decimal.Decimal `json:"unfilled-amount"` // taker  只有 unfilledAmount
				State          string           `json:"state,omitempty"`
				Taker          string           `json:"taker"`
				Maker          string           `json:"maker"`
			}{
				OrderId:        result.OrderId,
				UserId:         result.UserId,
				Role:           result.Role,
				Price:          result.Price,
				UnfilledAmount: result.UnfilledAmount,
				State:          result.State,
				Taker:          result.Taker,
				Maker:          result.Maker,
			})
		}
	}
	return nil, errors.New("error role:" + result.Role)
}

// 数据结构中包含指针, 只能接受同步操作,不能传输到其他协程
type MatchResult struct {
	Id           int64             `json:"id"`
	UserId       int64             `json:"user-id"`
	Symbol       string            `json:"symbol"`
	Ts           int64             `json:"ts"`
	OrderTypeStr string            `json:"order-type"`
	Items        []*OrderResult    `json:"items,omitempty"`
	PublishTs    int64             `json:"publish-ts"`
	Price        decimal.Decimal   `json:"price"`
	ExtParams    map[string]string `json:"ext-params,omitempty"`
	Stp          SelfTradeWMType   `json:"stp"`
	PullTime     int64             `json:"pull-time"`
	OrderId      int64             `json:"order-id"`
	Taker        string            `json:"taker"`
	Maker        string            `json:"maker"`
	TakerRate    string            `json:"taker-rate"`
}

type MatchResultWithAskBid struct {
	Mr       MatchResult
	AskPrice decimal.Decimal `json:"askPrice"`
	AskVol   decimal.Decimal `json:"askVol"`
	BidPrice decimal.Decimal `json:"bidPrice"`
	BidVol   decimal.Decimal `json:"bidVol"`
}

// 状态字符串
const (
	submittedState              = "submitted"
	partialCancelState          = "partial-canceled"
	partialFilledState          = "partial-filled"
	selfTradePartialCancelState = "self-trade-partial-canceled"
	selfTradeCancelState        = "self-trade-canceled"
	selfTradeDecreaseState      = "self-trade-decreased"
	filledState                 = "filled"
	canceledState               = "canceled"
	failedState                 = "failed"
	circuitCancelState          = "circuit-canceled"
	precisionCancelState        = "precision-canceled"
)

//var ResultExchangeName string

func Init() {
	//ResultExchangeName = config.GetString("app.profile", "market") + ".exchange.matchresults"
	//rabbitmq.DeclareExchange(ResultExchangeName, "fanout", true)
	InitOrderBookMap()
}

func PublishResultChan(symbol string) chan []byte {
	ch := make(chan []byte, config.GetInt64("exchange.trade.size", 5000))
	rabbitmqCh := rabbitmq.GetMatchResultRabbitMq(symbol)
	exchangeName := config.GetString("rabbitmq.exchange.trade-detail", "exchange.market-match.match-result") + "." + symbol
	go func() {
		batchNum := config.GetInt("batch_result", 90) - 1
		for {
			select {
			case msg := <-ch:
				size := len(ch)
				if size > batchNum {
					size = batchNum
				}
				results := BatchMatchResult(msg, ch, size)
				rabbitmq.PublishWithChan(rabbitmqCh, exchangeName, "fanout", results, common.TimestampNowMs())
			}
		}
	}()
	return ch
}

func BatchMatchResult(result []byte, ch chan []byte, num int) []byte {
	resultsStr := string(result)
	for num > 0 {
		msg := <-ch
		resultsStr += ","
		resultsStr += string(msg)
		num--
	}
	return []byte("[" + resultsStr + "]")
}

// 是否 匹配成功
func matchAble(order *Order, oppoOrder *Order) bool {
	if order.BuyOrSell == Buy {
		return order.Price.GreaterThanOrEqual(oppoOrder.Price)
	} else {
		return order.Price.LessThanOrEqual(oppoOrder.Price)
	}
}

// 生成结果
func (book *OrderBook) GenMatchResult(order *Order) *MatchResultWithAskBid {
	book.FromId = order.SeqId
	price, results := book.Match(order)

	askOrder := book.Peek(Sell)
	if askOrder == nil {
		askOrder = &Order{
			Price:          decimal.Zero,
			UnfilledAmount: decimal.Zero,
		}
	}
	bidOrder := book.Peek(Buy)
	if bidOrder == nil {
		bidOrder = &Order{
			Price:          decimal.Zero,
			UnfilledAmount: decimal.Zero,
		}
	}
	mrAB := &MatchResultWithAskBid{
		Mr: MatchResult{
			Id:           order.SeqId,
			UserId:       order.UserId,
			Symbol:       book.Symbol,
			Ts:           order.CreateAt,
			OrderTypeStr: order.OrderCombineTypeStr(),
			Items:        results,
			Price:        price,
			PublishTs:    common.TimestampNowMs(),
			Stp:          order.Stp,
			PullTime:     order.PullTime,
			OrderId:      order.OrderId,
			Taker:        order.Taker,
			Maker:        order.Maker,
			TakerRate:    getTakerStateStr(order.State),
		},
		AskPrice: askOrder.Price,
		AskVol:   askOrder.UnfilledAmount,
		BidPrice: bidOrder.Price,
		BidVol:   bidOrder.UnfilledAmount,
	}

	return mrAB
}

// 撮合
func (book *OrderBook) Match(order *Order) (price decimal.Decimal, results []*OrderResult) {

	switch order.Type {
	case Market:
		return book.matchMarket(order)
	case Limit:
		return book.matchLimit(order)
	case Cancel:
		return book.matchCancel(order)
	case SystemCancel:
		return book.matchSystemCancel(order)
	case Fok:
		common.Debug("fok 订单处理", order.Type, order.OrderId)
		return book.matchFok(order)
	/*
		case Ioc:
			return book.matchIoc(order)


	*/
	case BatchCancel:
		return book.matchBatchCancel(order)

	default:
		common.Fatal("error type:", order.Type, ", order.SeqID:", order.SeqId)
	}
	return decimal.Zero, nil
}

// MatchMarket
// 1. If not fuse(in circuit rate range), keep finding oppoOrder
// 2. If in fuse(our of circuit rate range) -> CircuitCanceled
// 3. Not Self-Trade
func (book *OrderBook) matchMarket(order *Order) (price decimal.Decimal, results []*OrderResult) {
	for {
		if order.isFilled() || order.isSelfTradePartialCanceled() || order.isSelfTradeCanceled() {
			results = append(results, finalizeTaker(order))
			break
		} else {
			oppoOrder := book.peekOppoOrder(order)
			if oppoOrder == nil {
				// no oppoOrder
				results = append(results, finalizeTaker(order))
				break
			} else {
				// if -> circuit
				// else -> match
				if len(results) > 0 &&
					!order.CircuitRate.Equals(decimal.Zero) &&
					((decimal.New(1, 0).Sub(order.CircuitRate).
						Mul(*results[0].Price)).GreaterThanOrEqual(oppoOrder.Price) ||
						((decimal.New(1, 0).Add(order.CircuitRate).
							Mul(*results[0].Price)).LessThanOrEqual(oppoOrder.Price))) { // 超出范围结束
					results = append(results, finalizeTaker(order.SetState(CircuitCanceled)))
					break
				} else {
					tmpOrderResult := matchOrder(book, order, oppoOrder)
					if tmpOrderResult != nil {
						results = append(results, tmpOrderResult)
					} else {
						if order.State == PrecisionCanceled {
							results = append(results, finalizeTaker(order.SetState(PrecisionCanceled)))
							break
						}
						continue
					}

					if oppoOrder.isFilled() || oppoOrder.isSelfTradeCanceled() ||
						oppoOrder.isSelfTradePartialCanceled() {
						book.Dequeue(oppoOrder.OrderId)
						if oppoOrder.isFilled() {
							price = oppoOrder.Price
						}
						continue
					} else {
						if oppoOrder.isPartialFilled() {
							price = oppoOrder.Price
						}

						if order.State == SelfTradePartialCanceled {
							results = append(results, finalizeTaker(order.SetState(SelfTradePartialCanceled)))

						} else if order.State == SelfTradeCanceled {
							results = append(results, finalizeTaker(order.SetState(SelfTradeCanceled)))

						} else {
							if order.State == Filled {
								results = append(results, finalizeTaker(order.SetState(Filled)))
							} else {
								continue
							}
						}
						break
					}
				}
			}
		}
	}
	return price, results
}

// 成交不了就挂单
func (book *OrderBook) matchLimit(order *Order) (price decimal.Decimal, results []*OrderResult) {
	for {
		if order.isFilled() || order.isSelfTradeCanceled() || order.isSelfTradePartialCanceled() {
			results = append(results, finalizeTaker(order))
			break
		} else {
			oppoOrder := book.peekOppoOrder(order)
			if oppoOrder == nil || !matchAble(order, oppoOrder) { // 成交不了 挂单
				book.Enqueue(order)
				results = append(results, finalizeTaker(order))
				break
			} else {
				// 撮合
				tmpOrderResult := matchOrder(book, order, oppoOrder)
				if tmpOrderResult != nil {
					results = append(results, tmpOrderResult)
				} else {
					continue
				}

				if oppoOrder.isFilled() || oppoOrder.isSelfTradeCanceled() ||
					oppoOrder.isSelfTradePartialCanceled() {
					book.Dequeue(oppoOrder.OrderId)
				}

				if oppoOrder.isFilled() || oppoOrder.isPartialFilled() {
					price = oppoOrder.Price
				}
			}
		}
	}
	return price, results
}

// 不成交 就撤单
func (book *OrderBook) matchIoc(order *Order) (results []*OrderResult) {
	for {
		if order.isFilled() {
			results = append(results, finalizeTaker(order))
			break
		} else {
			// 取出的订单合适才撮合
			oppoOrder := book.peekOppoOrder(order)
			if oppoOrder == nil || !matchAble(order, oppoOrder) { // 成交不了撤单
				results = append(results, finalizeTaker(order))
				break
			} else {
				// 撮合
				results = append(results, matchOrder(book, order, oppoOrder))
				if oppoOrder.isFilled() {
					book.Dequeue(oppoOrder.OrderId)
				}
			}
		}
	}
	return results
}
func (book *OrderBook) canFilledFokOrder(fokOrder *Order) (bool, OrderState) {
	common.Debug("fok 订单预判断", fokOrder.Type, fokOrder.OrderId)
	if fokOrder.BuyOrSell == Sell {
		common.Debug("fok 卖单", fokOrder.BuyOrSell)
		var orderSet = book.orderSet(Buy)
		it := orderSet.Iterator()
		amount := decimal.New(0, 0)
		for it.Next() {
			order := it.Value().(*Order)
			// CircuitRate 保护判断
			if !order.CircuitRate.Equals(decimal.Zero) &&
				((decimal.New(1, 0).Sub(fokOrder.CircuitRate).
					Mul(fokOrder.Price)).
					GreaterThanOrEqual(order.Price)) { // 超出范围结束
				common.Debug("fok 卖单超过限价范围", fokOrder.CircuitRate)
				return false, CircuitCanceled
			}

			if order.Price.GreaterThanOrEqual(fokOrder.Price) {
				amount = amount.Add(order.UnfilledAmount)
				if amount.GreaterThanOrEqual(fokOrder.UnfilledAmount) {
					return true, Filled
				}
			} else {
				return false, Canceled
			}
		}
	} else if fokOrder.BuyOrSell == Buy {
		var orderSet = book.orderSet(Sell)
		common.Debug("fok 买单", fokOrder.BuyOrSell)
		it := orderSet.Iterator()
		amount := decimal.New(0, 0)
		for it.Next() {

			order := it.Value().(*Order)
			// CircuitRate 保护判断
			if !order.CircuitRate.Equals(decimal.Zero) &&
				((decimal.New(1, 0).Add(order.CircuitRate).
					Mul(fokOrder.Price)).LessThanOrEqual(order.Price)) { // 超出范围结束
				return false, CircuitCanceled
			}

			if order.Price.LessThanOrEqual(fokOrder.Price) {
				amount = amount.Add(order.UnfilledAmount)
				if amount.GreaterThanOrEqual(fokOrder.UnfilledAmount) {
					return true, Filled
				}
			} else {
				return false, Canceled
			}
		}
	}
	return false, Canceled
}

func (book *OrderBook) matchFok(order *Order) (price decimal.Decimal, results []*OrderResult) {
	if ok, state := book.canFilledFokOrder(order); !ok {
		results = append(results, finalizeTaker(order))
		common.Debug("fok 订单预撮合失败", order.OrderId, order.State, state)
		return
	}

	for {
		if order.isFilled() || order.isSelfTradePartialCanceled() || order.isSelfTradeCanceled() {
			results = append(results, finalizeTaker(order))
			common.Debug("fok 订单成交或自成交撤单", order.State)
			break
		} else {
			oppoOrder := book.peekOppoOrder(order)
			if oppoOrder == nil {
				// no oppoOrder
				common.Debug("fok 无对手单", order.State)
				results = append(results, finalizeTaker(order.SetState(Canceled)))
				break
			} else {
				// if -> circuit
				// else -> match
				if len(results) > 0 &&
					!order.CircuitRate.Equals(decimal.Zero) &&
					((decimal.New(1, 0).Sub(order.CircuitRate).
						Mul(*results[0].Price)).GreaterThanOrEqual(oppoOrder.Price) ||
						((decimal.New(1, 0).Add(order.CircuitRate).
							Mul(*results[0].Price)).LessThanOrEqual(oppoOrder.Price))) { // 超出范围结束
					common.Debug("fok 超过限制", order.State)
					results = append(results, finalizeTaker(order.SetState(CircuitCanceled)))
					break
				} else {
					tmpOrderResult := matchOrder(book, order, oppoOrder)
					if tmpOrderResult != nil {
						common.Debug("fok 成交单下发", oppoOrder.State)
						results = append(results, tmpOrderResult)
					} else {
						if order.State == PrecisionCanceled {
							common.Debug("fok PrecisionCanceled", order.State)
							results = append(results, finalizeTaker(order.SetState(PrecisionCanceled)))
							break
						}
						continue
					}

					if oppoOrder.isFilled() || oppoOrder.isSelfTradeCanceled() ||
						oppoOrder.isSelfTradePartialCanceled() {
						book.Dequeue(oppoOrder.OrderId)
						if oppoOrder.isFilled() {
							price = oppoOrder.Price
						}
						continue
					} else {
						if oppoOrder.isPartialFilled() {
							price = oppoOrder.Price
						}

						if order.State == SelfTradePartialCanceled {
							common.Debug("fok SelfTradePartialCanceled", order.State)
							results = append(results, finalizeTaker(order.SetState(SelfTradePartialCanceled)))

						} else if order.State == SelfTradeCanceled {
							common.Debug("fok SelfTradeCanceled", order.State)
							results = append(results, finalizeTaker(order.SetState(SelfTradeCanceled)))

						} else {
							if order.State == Filled {
								common.Debug("fok Filled", order.State)
								results = append(results, finalizeTaker(order.SetState(Filled)))
							} else {
								continue
							}
						}
						break
					}
				}
			}
		}
	}
	return price, results
}

func (book *OrderBook) matchLimitMaker(order *Order) (results []*OrderResult) {
	oppoOrder := book.peekOppoOrder(order)
	if oppoOrder == nil || !matchAble(order, oppoOrder) { // 成交不了 挂单
		book.Enqueue(order)
		results = append(results, finalizeTaker(order))
	} else {
		order.State = Canceled
		results = append(results, finalizeTaker(order))
	}
	return results
}

// 撤单
func (book *OrderBook) matchCancel(order *Order) (price decimal.Decimal, results []*OrderResult) {
	var targetOrder = book.Find(order.OrderId)
	if targetOrder != nil {
		book.Dequeue(order.OrderId)
		var state string
		switch targetOrder.State {
		case Submitted: // 挂单变成撤销
			state = canceledState
		case PartialFilled: // 成交了一部分 变成部分撤单
			state = partialCancelState
		case Filled:
			state = filledState
		case SelfTradeDecreased:
			state = partialCancelState
		}
		results = append(results,
			&OrderResult{
				OrderId:        targetOrder.OrderId,
				Role:           "taker",
				UnfilledAmount: &targetOrder.UnfilledAmount,
				State:          state,
				UserId:         order.UserId,
				Taker:          order.Taker,
				Maker:          order.Maker,
			})
	} else {
		results = append(results,
			&OrderResult{
				OrderId: order.OrderId,
				Role:    "taker",
				State:   failedState,
				UserId:  order.UserId,
				Taker:   order.Taker,
				Maker:   order.Maker,
			})
	}
	return decimal.Decimal{}, results
}

// 批量撤单
func (book *OrderBook) matchBatchCancel(order *Order) (price decimal.Decimal, results []*OrderResult) {
	var batchOrderList []BatchCancelOrder
	if err := json.Unmarshal([]byte(order.Extra), &batchOrderList); err != nil || len(batchOrderList) < 1 {
		return decimal.Decimal{}, results
	}
	for _, info := range batchOrderList {
		var targetOrder = book.Find(info.OrderId)
		if targetOrder != nil {
			book.Dequeue(info.OrderId)
			var state string
			switch targetOrder.State {
			case Submitted: // 挂单变成撤销
				state = canceledState
			case PartialFilled: // 成交了一部分 变成部分撤单
				state = partialCancelState
			case Filled:
				state = filledState
			case SelfTradeDecreased:
				state = partialCancelState
			}
			results = append(results,
				&OrderResult{
					OrderId:        targetOrder.OrderId,
					Role:           "taker",
					UnfilledAmount: &targetOrder.UnfilledAmount,
					State:          state,
					UserId:         order.UserId,
					Taker:          order.Taker,
					Maker:          order.Maker,
				})
		} else {
			results = append(results,
				&OrderResult{
					OrderId: info.OrderId,
					Role:    "taker",
					State:   failedState,
					UserId:  order.UserId,
					Taker:   order.Taker,
					Maker:   order.Maker,
				})
		}

	}

	return decimal.Decimal{}, results
}

func (book *OrderBook) matchSystemCancel(order *Order) (price decimal.Decimal, results []*OrderResult) {
	return decimal.Decimal{}, append(results,
		&OrderResult{
			OrderId:        order.OrderId,
			Role:           "taker",
			UnfilledAmount: &order.UnfilledAmount,
			State:          canceledState,
			Taker:          order.Taker,
			Maker:          order.Maker,
		})
}

// taker 生成 生成订单结果
func finalizeTaker(order *Order) *OrderResult {
	result := &OrderResult{
		OrderId:        order.OrderId,
		Role:           "taker",
		UnfilledAmount: &order.UnfilledAmount,
		UserId:         order.UserId,
		Taker:          order.Taker,
		Maker:          order.Maker,
	}

	switch order.Type {
	case Market:
		finalizeTakerMarket(order, result)
	case Limit:
		finalizeTakerLimit(order, result)
	case Ioc:
		finalizeTakerIoc(order, result)
	case Fok:
		finalizeTakerFok(order, result)
	case LimitMaker:
		finalizeTakerLimitMaker(order, result)
	}
	return result
}

// 不填价格
func finalizeTakerMarket(order *Order, result *OrderResult) {
	switch order.State {
	case Submitted:
		result.State = canceledState
	case PartialFilled:
		result.State = partialCancelState
	case CircuitCanceled:
		result.State = circuitCancelState
	case SelfTradeCanceled:
		result.State = selfTradeCancelState
	case Filled:
		result.State = filledState
	case SelfTradeDecreased:
		result.State = partialCancelState
		//result.State = selfTradeDecreaseState
	case SelfTradePartialCanceled:
		result.State = selfTradePartialCancelState
	case PrecisionCanceled:
		result.State = precisionCancelState

	}
}

func finalizeTakerLimit(order *Order, result *OrderResult) {
	result.Price = &order.Price
	switch order.State {
	case Submitted:
		result.State = submittedState
	case PartialFilled:
		result.State = partialFilledState
	case Filled:
		result.State = filledState
	case SelfTradeCanceled:
		result.State = selfTradeCancelState
	case SelfTradeDecreased:
		result.State = selfTradeDecreaseState
	case SelfTradePartialCanceled:
		result.State = selfTradePartialCancelState
	}
}

//不成交就撤单
func finalizeTakerIoc(order *Order, result *OrderResult) {
	result.Price = &order.Price
	switch order.State {
	case Submitted:
		result.State = canceledState
	case PartialFilled:
		result.State = partialCancelState
	case Filled:
		result.State = filledState
	}
}

//不完全成交就撤单
func finalizeTakerFok(order *Order, result *OrderResult) *OrderResult {
	result.Price = &order.Price
	switch order.State {
	case Submitted:
		result.State = canceledState
	case PartialFilled:
		result.State = partialFilledState
	case Filled:
		result.State = filledState
	case SelfTradeCanceled:
		result.State = selfTradeCancelState
	case SelfTradeDecreased:
		result.State = selfTradeDecreaseState
	case SelfTradePartialCanceled:
		result.State = selfTradePartialCancelState
	}
	return result
}

func finalizeTakerLimitMaker(order *Order, result *OrderResult) {
	result.Price = &order.Price
	switch order.State {
	case Submitted:
		result.State = submittedState
	case Canceled:
		result.State = canceledState
	}
}

// 处理能匹配上的订单
func matchOrder(orderBook *OrderBook, order *Order, oppoOrder *Order) *OrderResult {
	if order.SeqId <= oppoOrder.SeqId {
		common.Fatal("order seqid  error", order.SeqId, oppoOrder.SeqId)
	}
	switch order.Type {
	case Market:
		return matchOrderMarket(orderBook, order, oppoOrder)
	case Limit:
		return matchOrderLimit(order, oppoOrder)
	case Ioc:
		return matchOrderIoc(order, oppoOrder)
	case Fok:
		return matchOrderFok(order, oppoOrder)
	default:
		common.Fatal("error type", order.Type)
		return nil
	}
}

// 只有根据 市价单 买的时候才会根据钱数去处理
func matchOrderMarket(orderBook *OrderBook, order *Order, oppoOrder *Order) *OrderResult {
	switch order.BuyOrSell {
	case Sell:
		return matchAmountBasedOrder(order, oppoOrder)
	case Buy:
		return matchCashAmountBasedOrder(orderBook, order, oppoOrder)
	default:
		common.Fatal("order buy or sell:", order.BuyOrSell)
		return nil
	}
}

//
func matchOrderLimit(order *Order, oppoOrder *Order) *OrderResult {
	return matchAmountBasedOrder(order, oppoOrder)
}

func matchOrderIoc(order *Order, oppoOrder *Order) *OrderResult {
	return matchAmountBasedOrder(order, oppoOrder)
}

func matchOrderFok(order *Order, oppoOrder *Order) *OrderResult {
	return matchAmountBasedOrder(order, oppoOrder)
}

func matchAmountBasedOrderSelfTrade(order *Order, oppoOrder *Order) *OrderResult {

	var state string

	// self-trade stc
	if order.Stp == CB {
		//cancel both
		stOrderState(order)
		state = stOppoOrderState(oppoOrder)

		return &OrderResult{
			OrderId:      oppoOrder.OrderId,
			Role:         "maker",
			FilledAmount: &oppoOrder.UnfilledAmount,
			Price:        &oppoOrder.Price,
			State:        state,
			UserId:       oppoOrder.UserId,
			Taker:        oppoOrder.Taker,
			Maker:        oppoOrder.Maker,
		}

	} else if order.Stp == CO {
		//cancel old
		state = stOppoOrderState(oppoOrder)

		return &OrderResult{
			OrderId:      oppoOrder.OrderId,
			Role:         "maker",
			Price:        &oppoOrder.Price,
			FilledAmount: &oppoOrder.UnfilledAmount,
			State:        state,
			UserId:       oppoOrder.UserId,
			Taker:        oppoOrder.Taker,
			Maker:        oppoOrder.Maker,
		}
	} else if order.Stp == CN {
		//cancel new(this time)
		stOrderState(order)

	} else {
		matchAmount := decimal.Min(order.UnfilledAmount, oppoOrder.UnfilledAmount)
		//cad or dac -> cancel and decrease
		if order.UnfilledAmount.Equal(oppoOrder.UnfilledAmount) {
			//both cancel
			stOrderState(order)
			state = stOppoOrderState(oppoOrder)

			return &OrderResult{
				OrderId:      oppoOrder.OrderId,
				Role:         "maker",
				Price:        &oppoOrder.Price,
				FilledAmount: &matchAmount,
				State:        state,
				UserId:       oppoOrder.UserId,
				Taker:        oppoOrder.Taker,
				Maker:        oppoOrder.Maker,
			}
		} else if order.UnfilledAmount.GreaterThan(oppoOrder.UnfilledAmount) {
			//decrease order, cancel oppoOrder
			order.UnfilledAmount = order.UnfilledAmount.Sub(matchAmount)
			order.State = SelfTradeDecreased

			state = stOppoOrderState(oppoOrder)

			return &OrderResult{
				OrderId:      oppoOrder.OrderId,
				Role:         "maker",
				FilledAmount: &matchAmount,
				Price:        &oppoOrder.Price,
				State:        state,
				UserId:       oppoOrder.UserId,
				Taker:        oppoOrder.Taker,
				Maker:        oppoOrder.Maker,
			}

		} else {
			//cancel order, decrease oppoOrder
			stOrderState(order)

			oppoOrder.UnfilledAmount = oppoOrder.UnfilledAmount.Sub(matchAmount)
			state = selfTradeDecreaseState
			oppoOrder.State = SelfTradeDecreased

			return &OrderResult{
				OrderId:      oppoOrder.OrderId,
				Role:         "maker",
				FilledAmount: &matchAmount,
				Price:        &oppoOrder.Price,
				State:        state,
				UserId:       oppoOrder.UserId,
				Taker:        oppoOrder.Taker,
				Maker:        oppoOrder.Maker,
			}
		}
	}

	return nil
}

// 基于数量去配单
func matchAmountBasedOrder(order *Order, oppoOrder *Order) *OrderResult {

	matchAmount := decimal.Min(order.UnfilledAmount, oppoOrder.UnfilledAmount)
	if order.UserId == oppoOrder.UserId && order.Stp > 0 {
		return matchAmountBasedOrderSelfTrade(order, oppoOrder)
	} else {
		order.fillAmount(matchAmount)
		oppoOrder.fillAmount(matchAmount)

		return &OrderResult{
			OrderId:      oppoOrder.OrderId,
			Price:        &oppoOrder.Price,
			Role:         "maker",
			FilledAmount: &matchAmount,
			State:        getOrderStateStr(oppoOrder.State),
			UserId:       oppoOrder.UserId,
			Taker:        oppoOrder.Taker,
			Maker:        oppoOrder.Maker,
		}
	}
}

func matchCashAmountBasedOrderSelfTrade(order *Order, oppoOrder *Order, unitPrice decimal.Decimal) *OrderResult {
	var state string

	// self-trade stc
	if order.Stp == CB {
		//cancel both
		stOrderState(order)
		state = stOppoOrderState(oppoOrder)

		return &OrderResult{
			OrderId:      oppoOrder.OrderId,
			Role:         "maker",
			FilledAmount: &oppoOrder.UnfilledAmount,
			State:        state,
			Price:        &oppoOrder.Price,
			UserId:       oppoOrder.UserId,
			Taker:        oppoOrder.Taker,
			Maker:        oppoOrder.Maker,
		}

	} else if order.Stp == CO {
		//cancel old
		state = stOppoOrderState(oppoOrder)

		return &OrderResult{
			OrderId:      oppoOrder.OrderId,
			Role:         "maker",
			FilledAmount: &oppoOrder.UnfilledAmount,
			State:        state,
			Price:        &oppoOrder.Price,
			UserId:       oppoOrder.UserId,
			Taker:        oppoOrder.Taker,
			Maker:        oppoOrder.Maker,
		}
	} else if order.Stp == CN {
		//cancel new(this time)
		stOrderState(order)

	} else {
		orderAmount := order.UnfilledAmount.Div(unitPrice).Truncate(0).Mul(common.LOWPRECISION)

		matchAmount := decimal.Min(orderAmount, oppoOrder.UnfilledAmount)
		matchCashAmount := matchAmount.Mul(oppoOrder.Price)
		/*matchCashAmount := matchPrice.Mul(matchAmount)
		order.fillAmount(matchCashAmount.Truncate(common.AmountScale(book.Symbol)))*/

		if matchAmount.GreaterThan(oppoOrder.UnfilledAmount) ||
			matchCashAmount.GreaterThan(order.UnfilledAmount) {
			common.Fatal("filling amount more than need",
				unitPrice, orderAmount, order.UnfilledAmount, matchCashAmount,
				oppoOrder.UnfilledAmount, oppoOrder.Price, matchAmount)
		}

		//cad or dac -> cancel and decrease
		if orderAmount.Equal(oppoOrder.UnfilledAmount) {

			//decrease order, cancel oppoOrder
			if order.UnfilledAmount.GreaterThan(matchCashAmount) {
				order.UnfilledAmount = order.UnfilledAmount.Sub(matchCashAmount)
				//when market order and role is taker , SelfTradeDecreased is a useful state?
				order.State = SelfTradeDecreased

				state = stOppoOrderState(oppoOrder)

				return &OrderResult{
					OrderId:      oppoOrder.OrderId,
					Role:         "maker",
					FilledAmount: &matchAmount,
					State:        state,
					Price:        &oppoOrder.Price,
					UserId:       oppoOrder.UserId,
					Taker:        oppoOrder.Taker,
					Maker:        oppoOrder.Maker,
				}
			}

			//both cancel
			stOrderState(order)
			state = stOppoOrderState(oppoOrder)

			return &OrderResult{
				OrderId:      oppoOrder.OrderId,
				Role:         "maker",
				FilledAmount: &matchAmount,
				Price:        &oppoOrder.Price,
				State:        state,
				UserId:       oppoOrder.UserId,
				Taker:        oppoOrder.Taker,
				Maker:        oppoOrder.Maker,
			}
		} else if orderAmount.GreaterThan(oppoOrder.UnfilledAmount) {
			//decrease order, cancel oppoOrder
			order.UnfilledAmount = order.UnfilledAmount.Sub(matchCashAmount)
			//when market order and role is taker , SelfTradeDecreased is a useful state?
			order.State = SelfTradeDecreased

			state = stOppoOrderState(oppoOrder)

			return &OrderResult{
				OrderId:      oppoOrder.OrderId,
				Role:         "maker",
				FilledAmount: &matchAmount,
				State:        state,
				Price:        &oppoOrder.Price,
				UserId:       oppoOrder.UserId,
				Taker:        oppoOrder.Taker,
				Maker:        oppoOrder.Maker,
			}

		} else {
			//cancel order, decrease oppoOrder
			stOrderState(order)

			oppoOrder.UnfilledAmount = oppoOrder.UnfilledAmount.Sub(matchAmount)
			state = selfTradeDecreaseState
			oppoOrder.State = SelfTradeDecreased

			return &OrderResult{
				OrderId:      oppoOrder.OrderId,
				Role:         "maker",
				FilledAmount: &matchAmount,
				Price:        &oppoOrder.Price,
				State:        state,
				UserId:       oppoOrder.UserId,
				Taker:        oppoOrder.Taker,
				Maker:        oppoOrder.Maker,
			}
		}
	}

	return nil
}

func stOrderState(order *Order) {
	switch order.State {
	case SelfTradeDecreased:
		order.State = SelfTradePartialCanceled
	case PartialFilled:
		order.State = SelfTradePartialCanceled
	case Submitted:
		order.State = SelfTradeCanceled
	}
}

func stOppoOrderState(oppoOrder *Order) (state string) {
	switch oppoOrder.State {
	case PartialFilled:
		state = selfTradePartialCancelState
		oppoOrder.State = SelfTradePartialCanceled
	case Submitted:
		state = selfTradeCancelState
		oppoOrder.State = SelfTradeCanceled
	case SelfTradeDecreased:
		state = selfTradePartialCancelState
		oppoOrder.State = SelfTradePartialCanceled
	case Filled:
		state = filledState
		oppoOrder.State = Filled
	}

	return state
}

// 基于钱去配单 ,amount 是基于钱
func matchCashAmountBasedOrder(book *OrderBook, order *Order, oppoOrder *Order) *OrderResult {
	unitPrice := common.LOWPRECISION.Mul(oppoOrder.Price)

	if unitPrice.GreaterThan(order.UnfilledAmount) {
		//common.Warn("market buy warn:", order)
		order.State = PrecisionCanceled
		return nil
	}

	if order.UserId == oppoOrder.UserId && order.Stp > 0 {
		return matchCashAmountBasedOrderSelfTrade(order, oppoOrder, unitPrice)
	} else {

		orderAmount := order.UnfilledAmount.Div(unitPrice).Truncate(0).Mul(common.LOWPRECISION)

		matchAmount := decimal.Min(orderAmount, oppoOrder.UnfilledAmount)
		matchCashAmount := matchAmount.Mul(oppoOrder.Price)
		/*matchCashAmount := matchPrice.Mul(matchAmount)
		order.fillAmount(matchCashAmount.Truncate(common.AmountScale(book.Symbol)))*/

		if matchAmount.GreaterThan(oppoOrder.UnfilledAmount) ||
			matchCashAmount.GreaterThan(order.UnfilledAmount) {
			common.Fatal("filling amount more than need",
				unitPrice, orderAmount, order.UnfilledAmount, matchCashAmount,
				oppoOrder.UnfilledAmount, matchAmount)
		}

		order.fillAmount(matchCashAmount)
		oppoOrder.fillAmount(matchAmount)

		return &OrderResult{
			OrderId:      oppoOrder.OrderId,
			Price:        &oppoOrder.Price,
			Role:         "maker",
			FilledAmount: &matchAmount,
			State:        getOrderStateStr(oppoOrder.State),
			UserId:       oppoOrder.UserId,
			Taker:        oppoOrder.Taker,
			Maker:        oppoOrder.Maker,
		}
	}
}

func ResultEqual(s1, s2 string) (bool, error) {
	var result1 MatchResult
	var result2 MatchResult
	var err error
	err = json.Unmarshal([]byte(s1), &result1)
	if err != nil {
		common.Error("result decode to json err:", err, string(s1))
	}
	result1.PublishTs = 0
	result1.PullTime = 0

	err = json.Unmarshal([]byte(s2), &result2)
	if err != nil {
		common.Error("result decode to json err:", err, string(s1))
	}
	result2.PublishTs = 0
	result2.PullTime = 0
	return reflect.DeepEqual(result1, result2), nil
}
