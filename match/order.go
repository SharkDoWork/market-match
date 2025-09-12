package match

import (
	"fmt"
	"github.com/spf13/cast"
	"market-match/common"

	"github.com/shopspring/decimal"
)

type OrderBuyOrSell int

const (
	Buy OrderBuyOrSell = iota
	Sell
	Submit // 提交
)

func getBuyOrSellStr(buyOrSell OrderBuyOrSell) string {
	switch buyOrSell {
	case Buy:
		return "buy"
	case Sell:
		return "sell"
	case Submit:
		return "submit"
	default:
		common.Fatal("Error buyOrSell", buyOrSell)
	}
	return ""
}

// 订单状态
type OrderState int

const (
	Submitted OrderState = iota
	PartialFilled
	PartialCanceled
	Canceled
	Filled
	Failed
	Error
	CircuitCanceled
	SystemCanceled
	SelfTradeCanceled
	SelfTradePartialCanceled
	SelfTradeDecreased
	PrecisionCanceled
)

// 订单类型
type OrderType int

//0撤单申请， 1市价买入，2市价卖出，3限价买入，4限价卖出 ，5立即成交否则取消未成交的剩余部分（IOC）买入，6立即成交否则取消未成交的剩余部分（IOC）卖出，7全部成交，否则全部取消（FOK）买入，8全部成交，否则全部取消（FOK）卖出，9只做Maker买入，10只做Maker卖出，11取消前有效（GTC）买入，12取消前有效（GTC）卖出 13 批量撤单
const (
	Market OrderType = iota
	Limit
	Ioc
	Cancel
	SystemCancel
	Fok
	LimitMaker
	_
	_
	_
	_
	_
	_
	BatchCancel
)

// self-trade when market
type SelfTradeWMType int8

const (
	AST = iota
	DC
	CO
	CN
	CB
)

func getOrderTypeStr(orderType OrderType) string {
	var typeStr string
	switch orderType {
	case Market:
		typeStr = "market"
	case Limit:
		typeStr = "limit"
	case Ioc:
		typeStr = "ioc"
	case Cancel:
		typeStr = "cancel"
	case SystemCancel:
		typeStr = "system-cancel"
	case Fok:
		typeStr = "fok"
	case BatchCancel:
		typeStr = "batch-cancel"
	default:
		common.Fatal("orderType ", orderType)
	}
	return typeStr
}

// 转换成字符串
func getOrderStateStr(state OrderState) string {
	var stateStr string
	switch state {
	case Submitted:
		stateStr = "submitted"
	case PartialFilled:
		stateStr = "partial-filled"
	case Canceled:
		stateStr = "canceled"
	case PartialCanceled:
		stateStr = "partial-canceled"
	case Filled:
		stateStr = "filled"
	case Failed:
		stateStr = "failed"
	case Error:
		stateStr = "error"
	case CircuitCanceled:
		stateStr = "circuit-canceled"
	case SelfTradeCanceled:
		stateStr = "self-trade-canceled"
	case SelfTradePartialCanceled:
		stateStr = "self-trade-partial-canceled"
	case SelfTradeDecreased:
		stateStr = "self-trade-decreased"

	default:
		common.Fatal("error state type " + cast.ToString(state))
	}
	return stateStr
}

// 转换成字符串
func getTakerStateStr(state OrderState) string {
	var stateStr string
	switch state {
	case Submitted:
		stateStr = "submitted"
	case PartialFilled:
		stateStr = "partial-filled"
	case Canceled:
		stateStr = "canceled"
	case PartialCanceled:
		stateStr = "partial-canceled"
	case Filled:
		stateStr = "filled"
	case Failed:
		stateStr = "failed"
	case Error:
		stateStr = "error"
	case CircuitCanceled:
		stateStr = "circuit-canceled"
	case SelfTradeCanceled:
		stateStr = "self-trade-canceled"
	case SelfTradePartialCanceled:
		stateStr = "self-trade-partial-canceled"
	case SelfTradeDecreased:
		stateStr = "self-trade-decreased"
	case SystemCanceled:
		stateStr = "system-canceled"
	case PrecisionCanceled:
		stateStr = "precision-canceled"
	default:
		stateStr = fmt.Sprintf("undefined %v", state)
	}
	return stateStr
}

type Order struct {
	SeqId          int64
	UserId         int64
	OrderId        int64
	BuyOrSell      OrderBuyOrSell
	Type           OrderType
	State          OrderState //  初始化的时候都是Submitted
	Price          decimal.Decimal
	UnfilledAmount decimal.Decimal
	CircuitRate    decimal.Decimal // 保护比例 ，市价单的会触发
	CreateAt       int64
	Stp            SelfTradeWMType
	PullTime       int64
	Extra          string // 批量撤单的订单ID集合
	Taker          string
	Maker          string
}

type BatchCancelOrder struct {
	OrderId     int64 `json:"order_id"`
	CancelState bool  `json:"cancel_state"`
}

func (order *Order) String() string {
	s := fmt.Sprintf("(seqId:%v orderId:%v buyorsell:%v UnfilledAmount:%v price:%v, circuitRate:%v, state:%v, type:%v)",
		order.SeqId,
		order.OrderId,
		order.BuyOrSell,
		order.UnfilledAmount,
		order.Price,
		order.CircuitRate,
		order.State,
		order.Type,
	)
	return s
}

func (order *Order) fillAmount(amount decimal.Decimal) {
	if amount.GreaterThan(order.UnfilledAmount) {
		common.Fatal("filling amount more than need", amount, order.UnfilledAmount)
	}
	order.UnfilledAmount = order.UnfilledAmount.Sub(amount)
	if order.UnfilledAmount.Equal(decimal.Zero) {
		order.State = Filled
	} else {
		order.State = PartialFilled
	}
}

func (order *Order) SetState(state OrderState) *Order {
	order.State = state
	return order
}

// 是否已经埋完单
func (order *Order) isFilled() bool {
	return order.State == Filled
}

func (order *Order) isPartialFilled() bool {
	return order.State == PartialFilled
}

func (order *Order) isSelfTradeCanceled() bool {
	return order.State == SelfTradeCanceled
}

func (order *Order) isSelfTradePartialCanceled() bool {
	return order.State == SelfTradePartialCanceled
}

// 获取相反类型
func (order *Order) oppoBuyOrSell() OrderBuyOrSell {
	if order.BuyOrSell == Buy {
		return Sell
	}
	return Buy
}

// 组合类型
func (order *Order) OrderCombineTypeStr() string {
	return getBuyOrSellStr(order.BuyOrSell) + "-" + getOrderTypeStr(order.Type)
}

// 订单比较
func Comparator(a, b interface{}) int {
	orderA := a.(*Order)
	orderB := b.(*Order)

	if orderA.BuyOrSell == Buy {
		switch {
		case orderA.Price.GreaterThan(orderB.Price) ||
			(orderA.Price.Equal(orderB.Price) && orderA.SeqId < orderB.SeqId):
			return -1
		case orderA.SeqId == orderB.SeqId:
			return 0
		default:
			return 1
		}
	} else {
		switch {
		case orderA.Price.LessThan(orderB.Price) ||
			(orderA.Price.Equal(orderB.Price) && orderA.SeqId < orderB.SeqId):
			return -1
		case orderA.SeqId == orderB.SeqId:
			return 0
		default:
			return 1
		}
	}
}

func CompareOrder(order1 *Order, order2 *Order) bool {
	if !order1.Price.Equal(order2.Price) ||
		!order1.UnfilledAmount.Equal(order2.UnfilledAmount) ||
		order1.SeqId != order2.SeqId ||
		order1.OrderId != order2.OrderId ||
		order1.Type != order2.Type ||
		order1.State != order2.State {
		return false
	}
	return true
}

func amountScale(symbol string, order *Order) int32 {
	if order.Type == Market && order.BuyOrSell == Buy {
		return common.PriceScale(symbol)
	} else {
		return common.AmountScale(symbol)
	}
}

// 如果精度有问题 就系统砍单
func CheckOrderScale(symbol string, order *Order) bool {
	amountScaled := order.UnfilledAmount.Truncate(amountScale(symbol, order))
	priceScaled := order.Price.Truncate(common.PriceScale(symbol))
	rateScaled := order.CircuitRate.Truncate(common.PriceScale(symbol))

	equaled := amountScaled.Equal(order.UnfilledAmount) &&
		priceScaled.Equal(order.Price) &&
		rateScaled.Equal(order.CircuitRate)
	// 为了和clojure 结果一致
	order.UnfilledAmount = amountScaled
	order.Price = priceScaled
	order.CircuitRate = rateScaled
	return equaled
}
