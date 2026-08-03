// matcher.go 撮合引擎的核心：实现各类订单的撮合逻辑与撮合结果生成。
//
// 核心概念：
//   - Taker（吃单方）：新进入引擎、主动与订单簿中挂单成交的订单；
//     Maker（挂单方）：已存在于订单簿中、被动被成交的订单。
//   - 撮合循环：taker 订单不断与对手方队首订单（最优价）比较，
//     价格满足则成交（成交价以 maker 挂单价格为准），直到 taker 全部成交、
//     无对手单、价格不再满足或触发保护机制（熔断/自成交防护/精度不足）。
//   - 剩余未成交的限价单会挂入订单簿成为 maker；市价单剩余部分直接撤销。
//
// 支持的订单类型：市价单（Market）、限价单（Limit）、IOC、FOK、只做 Maker（LimitMaker）、
// 撤单（Cancel）、系统撤单（SystemCancel）、批量撤单（BatchCancel）。
// 撮合结果经 RabbitMQ 批量发布给下游（行情、清算等）。
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

// json 全局 JSON 编解码器，兼容标准库行为但性能更好。
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// OrderResult 一笔订单的撮合结果项。
// 同一笔 taker 订单可能产生多个 OrderResult：每个被成交的 maker 一个（Role="maker"），
// 最后再加一个 taker 自身的终态结果（Role="taker"）。
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
// MarshalJSON 自定义 OrderResult 的 JSON 序列化：
// maker 结果输出 filled-amount（本次成交量），taker 结果输出 unfilled-amount（剩余未成交量），
// 撤单失败的 taker 结果则不输出任何数量字段。通过匿名结构体控制字段有无，
// 避免下游解析到无意义的空字段。
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

// MatchResultWithAskBid 在撮合结果之外附带撮合完成后的盘口一档行情：
// 卖一价/量（ask）与买一价/量（bid），供下游直接更新行情而无需再查询订单簿。
type MatchResultWithAskBid struct {
	Mr       MatchResult
	AskPrice decimal.Decimal `json:"askPrice"` // 卖一价（最低卖价）
	AskVol   decimal.Decimal `json:"askVol"`   // 卖一量
	BidPrice decimal.Decimal `json:"bidPrice"` // 买一价（最高买价）
	BidVol   decimal.Decimal `json:"bidVol"`   // 买一量
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

// Init 初始化 match 包：创建全局订单簿注册表。服务启动时调用。
func Init() {
	//ResultExchangeName = config.GetString("app.profile", "market") + ".exchange.matchresults"
	//rabbitmq.DeclareExchange(ResultExchangeName, "fanout", true)
	InitOrderBookMap()
}

// PublishResultChan 为指定交易对创建一个撮合结果发布通道。
// 调用方将序列化后的单条撮合结果写入返回的 channel；
// 内部 goroutine 会将通道中积压的多条结果批量合并成一个 JSON 数组，
// 通过 RabbitMQ 以 fanout 方式发布，降低消息发送频率、提高吞吐。
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

// BatchMatchResult 将首条结果与通道中积压的最多 num 条结果拼接成一个 JSON 数组，
// 实现撮合结果的批量发布。注意结果是字符串级拼接，要求每条结果本身是合法 JSON 对象。
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
// matchAble 判断 taker 订单与对手方队首订单（maker）的价格是否可成交：
//   - 买单：出价 >= 对手卖价即可成交（愿意出更高的钱，当然能买）；
//   - 卖单：要价 <= 对手买价即可成交（愿意卖得更便宜，当然能卖）。
// 这是限价类订单是否继续撮合循环的核心判定。
func matchAble(order *Order, oppoOrder *Order) bool {
	if order.BuyOrSell == Buy {
		return order.Price.GreaterThanOrEqual(oppoOrder.Price)
	} else {
		return order.Price.LessThanOrEqual(oppoOrder.Price)
	}
}

// 生成结果
// GenMatchResult 是订单进入订单簿的统一入口：执行撮合并生成完整的撮合结果。
// 流程：更新已处理序号 -> 调用 Match 执行撮合 -> 读取撮合后的买一/卖一，
// 组装为带盘口行情的 MatchResultWithAskBid 返回（盘口为空时以 0 填充）。
// 返回的 price 为最后一笔成交的价格（无成交时为 0）。
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
// Match 按订单类型分发到对应的撮合函数，是撮合逻辑的总路由：
// 市价单、限价单、撤单、系统撤单、FOK、批量撤单各有独立实现。
// 返回最后一笔成交价与该订单产生的全部结果项。
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
//
// matchMarket 市价单撮合：不限定价格，按对手方最优价逐档吃单，直到全部成交或对手簿被吃空。
// 与限价单的关键区别：剩余未成交部分绝不挂单，直接以撤销类状态结束。
// 保护机制：
//   - 熔断保护（CircuitRate）：以第一笔成交价为基准，若下一档对手价偏离超过保护比例
//     （买单：高于基准价*(1+rate)；卖单：低于基准价*(1-rate)），停止撮合并将剩余部分
//     标记为 CircuitCanceled，防止市价单在深度不足时打出极端价格；
//   - 精度保护：市价买单剩余金额不足以按最小精度买到任何数量时，标记 PrecisionCanceled；
//   - 自成交防护（STP）：遇到同用户挂单时按订单的 Stp 策略处理。
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
// matchLimit 限价单撮合：反复与对手方队首订单比较价格，
// 价格可成交（matchAble）则按 maker 价格成交并继续吃下一档；
// 一旦价格不再满足或对手簿为空，剩余未成交部分挂入订单簿成为 maker（GTC 语义）。
// 被完全成交或因自成交防护被撤销的 maker 会从订单簿中摘除。
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
// matchIoc IOC（Immediate-Or-Cancel）订单撮合：立即成交，能成交多少算多少，
// 剩余未成交部分立即撤销，绝不挂单。与限价单的区别仅在于不进入订单簿。
// 注意：当前 Match 路由中 Ioc 分支被注释掉，该函数暂未被启用。
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
// canFilledFokOrder FOK 订单的预检查：在不改变订单簿的前提下，沿对手方价格档位
// 累计可成交数量，判断该 FOK 订单能否被一次性全部成交。
// 返回 (true, Filled) 表示可全部成交，matchFok 才会真正执行撮合；
// 返回 (false, Canceled/CircuitCanceled) 表示数量不足或触发熔断保护，整单撤销。
// 遍历中一旦遇到价格超出 FOK 限价的档位即停止（后续档位价格更差，不可能再成交）。
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

// matchFok FOK（Fill-Or-Kill）订单撮合：要么立即全部成交，要么整单撤销，不允许部分成交。
// 先通过 canFilledFokOrder 预检查对手簿深度是否足够，不足则直接撤销（不产生任何成交）；
// 足够才进入与市价单类似的逐档撮合循环（同样带熔断与自成交防护）。
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

// matchLimitMaker 只做 Maker（Post-Only）订单撮合：
// 若当前价格无法与对手方队首成交，则正常挂入订单簿；
// 若价格可立即成交（会成为 taker），则直接撤销，保证该订单只以 maker 身份存在于簿中。
// 注意：当前 Match 路由未分发到该函数（LimitMaker 类型未在 switch 中处理）。
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
// matchCancel 处理单笔撤单请求：按 OrderId 在订单簿中查找目标挂单并摘除。
// 结果状态取决于目标单被撤时的状态：
//   - 未成交过（Submitted）-> canceled；
//   - 已部分成交（PartialFilled/SelfTradeDecreased）-> partial-canceled；
//   - 已完全成交（Filled，理论上已不在簿中）-> filled；
// 目标单不存在（可能已成交或已撤）时返回 failed。
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
// matchBatchCancel 处理批量撤单请求：从 order.Extra 中解析出订单 ID 列表（JSON），
// 逐个执行与 matchCancel 相同的撤单逻辑，每个 ID 产生一条结果（成功按目标单状态给出
// canceled/partial-canceled/filled，未找到则为 failed）。
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

// matchSystemCancel 系统撤单：由系统侧发起的强制撤单（如交易对下线、风控触发），
// 直接生成 canceled 结果。注意该函数只生成结果，不从订单簿摘除订单，
// 调用方需保证订单已被妥善处理。
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
// finalizeTaker 为 taker 订单生成终态结果项（每笔进入引擎的订单最后都会有一条），
// 按订单类型分发到对应的 finalize 函数，将内部状态枚举映射为对外输出的状态字符串。
// 结果中 UnfilledAmount 指向订单剩余的未成交量（市价买单为剩余金额）。
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
// finalizeTakerMarket 生成市价单的 taker 终态结果。市价单没有限价，结果不含价格字段。
// 状态映射要点：市价单剩余部分必然被撤销——从未成交记 canceled，
// 成交过一部分记 partial-canceled；熔断/自成交/精度原因则映射为对应的撤销类状态。
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

// finalizeTakerLimit 生成限价单的 taker 终态结果，结果中包含限价价格。
// 状态映射要点：限价单未成交完会挂入订单簿，因此 Submitted 映射为 submitted（已挂单）、
// PartialFilled 映射为 partial-filled（部分成交且剩余已挂单），而非撤销类状态。
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
// finalizeTakerIoc 生成 IOC 订单的 taker 终态结果。
// IOC 剩余部分立即撤销：未成交记 canceled，部分成交记 partial-canceled。
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
// finalizeTakerFok 生成 FOK 订单的 taker 终态结果。
// FOK 语义为"全部成交或整单撤销"，正常路径只会出现 filled 或 canceled；
// 其余分支（自成交等）为防御性映射。
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

// finalizeTakerLimitMaker 生成只做 Maker（Post-Only）订单的 taker 终态结果：
// 成功挂簿为 submitted；因会立即成交而被拒绝则为 canceled。
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
// matchOrder 执行 taker 与单个 maker 之间的一笔成交，按 taker 订单类型分发。
// 返回 maker 侧的成交结果（taker 的终态结果由 finalizeTaker 单独生成）。
// 前置校验：taker 的 SeqId 必须大于 maker（maker 先于 taker 进入引擎），
// 否则说明消息乱序，属于严重错误直接 Fatal。
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
// matchOrderMarket 市价单的单笔成交分发：
//   - 市价卖单：UnfilledAmount 表示数量，按数量撮合（matchAmountBasedOrder）；
//   - 市价买单：UnfilledAmount 表示金额（要花多少钱），需按金额换算成数量撮合
//     （matchCashAmountBasedOrder）。
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

// matchOrderLimit 限价单的单笔成交：直接按数量撮合。
func matchOrderLimit(order *Order, oppoOrder *Order) *OrderResult {
	return matchAmountBasedOrder(order, oppoOrder)
}

// matchOrderIoc IOC 订单的单笔成交：与限价单相同，按数量撮合（是否撤剩余由上层循环控制）。
func matchOrderIoc(order *Order, oppoOrder *Order) *OrderResult {
	return matchAmountBasedOrder(order, oppoOrder)
}

// matchOrderFok FOK 订单的单笔成交：按数量撮合（能否全部成交已在上层预检查）。
func matchOrderFok(order *Order, oppoOrder *Order) *OrderResult {
	return matchAmountBasedOrder(order, oppoOrder)
}

// matchAmountBasedOrderSelfTrade 按数量撮合场景下的自成交（taker 与 maker 属于同一用户）处理。
// 根据订单携带的 STP 策略执行：
//   - CB（cancel both）：taker 与 maker 都撤销；
//   - CO（cancel old）：撤销订单簿中的老挂单（maker），taker 继续后续撮合；
//   - CN（cancel new）：撤销新订单（taker），返回 nil 表示本档无成交结果；
//   - DC（默认分支，decrease & cancel）：数量小的一方撤销，大的一方减去相同数量
//     （等量则双方都撤），不产生真实成交。
// 返回的结果项描述的是 maker（老挂单）侧的变化。
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
// matchAmountBasedOrder 按数量撮合一笔成交：成交量取双方未成交量的较小值，
// 成交价以 maker（oppoOrder）的挂单价格为准（价格优先原则下 maker 价格不劣于 taker 心理价）。
// 若双方属于同一用户且设置了 STP 策略，则转入自成交处理分支，不产生真实成交。
// 成交后双方的未成交量与状态（Filled/PartialFilled）由 fillAmount 更新，
// 返回 maker 侧的成交结果项。
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

// matchCashAmountBasedOrderSelfTrade 市价买单（按金额撮合）场景下的自成交处理，
// STP 策略语义与 matchAmountBasedOrderSelfTrade 相同。
// 区别仅在于数量换算：taker 的 UnfilledAmount 是金额，需先除以单价（unitPrice）
// 并截断到最小精度换算成可买数量后，再比较双方大小决定撤谁、减谁。
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

// stOrderState 自成交处理中更新 taker（新订单）侧的状态：
// 已有过成交/减量的记为"自成交部分撤销"，未成交过的记为"自成交整单撤销"。
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

// stOppoOrderState 自成交处理中更新 maker（老挂单）侧的状态，
// 并返回对应的对外输出状态字符串：已有过成交/减量的记为自成交部分撤销，
// 未成交过的记为自成交整单撤销，已完全成交的记为 filled。
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
// matchCashAmountBasedOrder 市价买单的单笔成交：taker 的 UnfilledAmount 是金额（如 USDT），
// 需按 maker 价格换算成可买数量参与撮合。
// 换算规则：以"最小精度单价"（LOWPRECISION * price）为最小可买单元，
// 可买数量 = floor(剩余金额 / 最小精度单价) * LOWPRECISION，保证数量落在合法精度上。
// 若剩余金额连最小单元都买不起，则将订单标记为 PrecisionCanceled（精度撤销）并返回 nil。
// 成交时 taker 扣减金额、maker 扣减数量，成交价以 maker 价格为准。
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

// ResultEqual 比较两个 JSON 字符串表示的撮合结果是否等价。
// 比较前会将 PublishTs、PullTime 两个与撮合逻辑无关的时间戳字段清零，
// 用于测试中忽略时序差异做结果断言。
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
