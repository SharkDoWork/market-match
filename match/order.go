// order.go 定义撮合引擎的核心数据模型——订单（Order）及其相关枚举。
// 包括：买卖方向、订单状态、订单类型（限价/市价/IOC/FOK/撤单等）、
// 自成交防护（STP）策略，以及订单的比较器（Comparator，决定订单簿中的排序规则：
// 买单价格降序、卖单价格升序，同价时按进入引擎的序号 SeqId 先到先得）。
package match

import (
	"fmt"
	"github.com/spf13/cast"
	"market-match/common"

	"github.com/shopspring/decimal"
)

// OrderBuyOrSell 订单的买卖方向。
type OrderBuyOrSell int

const (
	Buy  OrderBuyOrSell = iota // 买单
	Sell                       // 卖单
	Submit                     // 提交
)

// getBuyOrSellStr 将买卖方向枚举转换为对外输出用的字符串（buy/sell/submit）。
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

// 订单在撮合引擎内部的生命周期状态。
// 注意：这些是引擎内部使用的枚举，对外输出时会经 getOrderStateStr/getTakerStateStr
// 转换为对应的字符串形式。
const (
	Submitted               OrderState = iota // 已提交（已挂入订单簿，尚未有任何成交）
	PartialFilled                             // 部分成交
	PartialCanceled                           // 部分撤销（成交一部分后剩余被撤销）
	Canceled                                  // 已撤销
	Filled                                    // 全部成交
	Failed                                    // 失败（如撤销一个不存在的订单）
	Error                                     // 错误
	CircuitCanceled                           // 熔断撤销（市价单成交价超出保护比例范围时触发）
	SystemCanceled                            // 系统撤销
	SelfTradeCanceled                         // 自成交撤销（STP 策略撤销新单/整单）
	SelfTradePartialCanceled                  // 自成交部分撤销
	SelfTradeDecreased                        // 自成交减量（STP 策略下仅减少未成交量，不撤整单）
	PrecisionCanceled                         // 精度撤销（市价买单剩余金额不足以按最小精度买任何数量时触发）
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

// 自成交防护（Self-Trade Prevention, STP）策略。
// 当新订单（taker）与订单簿中同一用户的挂单（maker）即将成交时，
// 按该策略决定如何处理，避免同一用户自己与自己成交。
const (
	AST = iota // 允许自成交（allow self-trade），不做任何防护
	DC         // 减量（decrease & cancel）：两边中数量小的一方撤销，大的一方减去相应数量
	CO         // 撤销老单（cancel old）：撤销订单簿中已存在的挂单（maker），新单继续撮合
	CN         // 撤销新单（cancel new）：撤销本次进入的新订单（taker），老挂单保留
	CB         // 双边撤销（cancel both）：新单与老挂单同时撤销
)

// getOrderTypeStr 将订单类型枚举转换为对外输出用的字符串（market/limit/ioc 等）。
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
// getOrderStateStr 将订单状态枚举转换为字符串，用于 maker（挂单）侧的成交结果输出。
// 注意：该函数未覆盖 SystemCanceled/PrecisionCanceled 等状态，遇到未覆盖状态会 Fatal，
// 因为 maker 侧正常不会出现这些状态。
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
// getTakerStateStr 将订单状态枚举转换为字符串，用于 taker（吃单）侧的结果输出。
// 与 getOrderStateStr 相比覆盖了全部状态（含 SystemCanceled、PrecisionCanceled），
// 且对未知状态返回 "undefined" 描述而不是 Fatal，因为 taker 侧可能出现任意终态。
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

// Order 撮合引擎中的订单实体。
// 同一个结构既表示进入引擎的新订单（taker），也表示订单簿中的挂单（maker），
// 通过 State 和剩余未成交量 UnfilledAmount 跟踪撮合进度。
type Order struct {
	SeqId          int64           // 进入撮合引擎的全局递增序号，决定同价订单的时间优先顺序
	UserId         int64           // 用户 ID，用于自成交（STP）判定
	OrderId        int64           // 订单 ID（业务侧分配）
	BuyOrSell      OrderBuyOrSell  // 买卖方向
	Type           OrderType       // 订单类型（限价/市价/IOC/FOK/撤单等）
	State          OrderState      //  初始化的时候都是Submitted
	Price          decimal.Decimal // 限价单价格；市价单该字段不参与定价（市价买单中 UnfilledAmount 表示金额）
	UnfilledAmount decimal.Decimal // 未成交量：普通订单表示数量；市价买单表示剩余可用金额（quote 货币）
	CircuitRate    decimal.Decimal // 保护比例 ，市价单的会触发
	CreateAt       int64           // 订单创建时间戳（毫秒）
	Stp            SelfTradeWMType // 自成交防护策略
	PullTime       int64           // 订单从队列拉取的时间戳，用于延迟统计
	Extra          string          // 批量撤单的订单ID集合
	Taker          string          // taker 费率相关标识（透传字段）
	Maker          string          // maker 费率相关标识（透传字段）
}

// BatchCancelOrder 批量撤单请求中的单个撤单条目，由 Order.Extra 中的 JSON 反序列化得到。
type BatchCancelOrder struct {
	OrderId     int64 `json:"order_id"`
	CancelState bool  `json:"cancel_state"`
}

// String 返回订单的紧凑字符串表示，主要用于日志输出和调试。
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

// fillAmount 从订单的未成交量中扣除本次成交数量，并据此更新订单状态：
// 扣减到 0 置为 Filled（全部成交），否则置为 PartialFilled（部分成交）。
// 若成交数量超过未成交量说明撮合逻辑出错，直接 Fatal。
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

// SetState 设置订单状态并返回订单自身，支持链式调用（如 finalizeTaker(order.SetState(Filled))）。
func (order *Order) SetState(state OrderState) *Order {
	order.State = state
	return order
}

// 是否已经埋完单
func (order *Order) isFilled() bool {
	return order.State == Filled
}

// isPartialFilled 判断订单是否处于部分成交状态。
func (order *Order) isPartialFilled() bool {
	return order.State == PartialFilled
}

// isSelfTradeCanceled 判断订单是否因自成交防护被整单撤销。
func (order *Order) isSelfTradeCanceled() bool {
	return order.State == SelfTradeCanceled
}

// isSelfTradePartialCanceled 判断订单是否因自成交防护被部分撤销。
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
// OrderCombineTypeStr 返回"方向-类型"的组合字符串（如 buy-limit、sell-market），
// 用于撮合结果中的 order-type 字段输出。
func (order *Order) OrderCombineTypeStr() string {
	return getBuyOrSellStr(order.BuyOrSell) + "-" + getOrderTypeStr(order.Type)
}

// 订单比较
// Comparator 是订单簿红黑树（TreeSet）的排序比较器，实现交易所核心的撮合优先级规则：
//   - 买单（Buy）：价格优先，价格越高排越前（优先成交）；同价时 SeqId 小（先进入引擎）的排前，即时间优先。
//   - 卖单（Sell）：价格优先，价格越低排越前（优先成交）；同价时同样 SeqId 小者优先。
// 返回值约定：-1 表示 a 排在 b 前，0 表示同一订单（SeqId 相等），1 表示 a 排在 b 后。
// 注意：比较时假定 a、b 属于同一方向（同一棵树内只存同方向订单），方向由 a 决定。
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

// CompareOrder 逐字段比较两个订单是否完全一致（价格、未成交量、序号、ID、类型、状态），
// 主要用于快照恢复校验和单元测试。
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

// amountScale 返回该订单"未成交量"字段应使用的精度（小数位数）。
// 特殊点：市价买单的 UnfilledAmount 表示的是金额（quote 货币，如 USDT），
// 因此使用价格精度；其余订单的 UnfilledAmount 表示数量，使用数量精度。
func amountScale(symbol string, order *Order) int32 {
	if order.Type == Market && order.BuyOrSell == Buy {
		return common.PriceScale(symbol)
	} else {
		return common.AmountScale(symbol)
	}
}

// 如果精度有问题 就系统砍单
// CheckOrderScale 校验订单的数量、价格、熔断比例是否符合该交易对的精度要求，
// 并将三个字段截断到合法精度（原地修改订单）。
// 返回 true 表示订单原本就符合精度；返回 false 表示存在精度越界（调用方通常会据此砍单）。
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
