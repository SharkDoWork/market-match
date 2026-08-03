// order_book.go 实现单个交易对的订单簿（OrderBook）。
// 订单簿是撮合引擎的核心数据结构：买单、卖单分别存放在两棵红黑树（TreeSet）中，
// 按 Comparator 定义的规则排序（买单价格降序、卖单价格升序、同价时间优先），
// 使队首（Peek）永远是当前最优价格的挂单；另用一个 map 缓存所有在簿订单，
// 支持按 OrderId O(1) 查找（撤单场景）。
// 本文件还实现了订单簿的 gob 序列化/反序列化，用于快照（snapshot）持久化与恢复。
package match

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"market-match/common"
	"market-match/dogstatsd"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

// TreeSet 对 gods 库红黑树集合的封装，额外实现了 gob 二进制序列化接口，
// 使订单簿可以被整体编码存入快照（如 Redis/文件），重启后恢复。
type TreeSet struct {
	*treeset.Set
}

// MarshalBinary 将树中所有订单按当前排序编码为 gob 字节流，用于快照持久化。
func (set *TreeSet) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(set.Values()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary 从 gob 字节流恢复订单并重新逐个插入红黑树，
// 插入时由 Comparator 自动重建排序结构，用于快照恢复。
func (set *TreeSet) UnmarshalBinary(data []byte) error {
	var elements []interface{}
	reader := bytes.NewBuffer(data)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&elements); err != nil {
		return err
	}
	set.Clear()
	//common.Debug("decode len :", len(elements))
	for i := range elements {
		order := elements[i].(Order)
		set.Add(&order)
	}
	return nil
}

// newTreeSet 创建一棵以 Comparator 为排序规则的红黑树订单集合。
func newTreeSet() *TreeSet {
	return &TreeSet{treeset.NewWith(Comparator)}
}

// OrderBook 单个交易对的订单簿。
// 买单、卖单分别存于 BuySet/SellSet 两棵红黑树中：
//   - BuySet 队首是当前最高买价（买一）；SellSet 队首是当前最低卖价（卖一）。
//   - cache 保存所有在簿订单（OrderId -> Order），用于撤单时按 ID 快速定位。
type OrderBook struct {
	Symbol  string           // 交易对，如 btcusdt
	FromId  int64            // 已处理到的最新订单 SeqId，用于快照恢复后确定续传起点
	cache   map[int64]*Order // 在簿订单索引：OrderId -> Order
	BuySet  *TreeSet         // 买单簿（价格降序，同价 SeqId 升序）
	SellSet *TreeSet         // 卖单簿（价格升序，同价 SeqId 升序）
}

// InitOrderBook 创建指定交易对的订单簿，fromId 为快照恢复时已处理到的 SeqId。
func InitOrderBook(fromId int64, symbol string) *OrderBook {
	return &OrderBook{
		Symbol:  symbol,
		FromId:  fromId,
		cache:   make(map[int64]*Order),
		BuySet:  newTreeSet(),
		SellSet: newTreeSet(),
	}
}

// NewOrderBook 创建一个空订单簿（不指定 symbol 与 fromId），主要用于 Clone 和测试。
func NewOrderBook() *OrderBook {
	return &OrderBook{
		cache:   make(map[int64]*Order),
		BuySet:  newTreeSet(),
		SellSet: newTreeSet(),
	}
}

// String 返回订单簿的摘要信息（交易对、已处理 SeqId、在簿订单数），用于日志。
func (book *OrderBook) String() string {
	return "OrderBook symbol:" + book.Symbol + " FromId:" + cast.ToString(book.FromId) +
		" size:" + cast.ToString(len(book.cache))
}

// SetCache 仅将订单写入 cache 索引（不加入买卖树）。
// 用于快照恢复等场景：先重建索引，再由调用方保证树结构一致。
func (book *OrderBook) SetCache(order *Order) {
	book.cache[order.OrderId] = order
}

// 查找
func (book *OrderBook) Find(orderId int64) *Order {
	return book.cache[orderId]
}

// Cache 返回在簿订单索引 map（OrderId -> Order），供遍历或快照使用。
func (book *OrderBook) Cache() map[int64]*Order {
	return book.cache
}

// 获得对手的头单
func (book *OrderBook) peekOppoOrder(order *Order) *Order {
	return book.Peek(order.oppoBuyOrSell())
}

// orderSet 按方向返回对应的订单树：买单返回 BuySet，卖单返回 SellSet。
func (book *OrderBook) orderSet(buyOrSell OrderBuyOrSell) (orderSet *TreeSet) {
	if buyOrSell == Buy {
		orderSet = book.BuySet
	} else {
		orderSet = book.SellSet
	}
	return
}

// 获取 挂单的头单
func (book *OrderBook) Peek(buyOrSell OrderBuyOrSell) *Order {
	var orderSet = book.orderSet(buyOrSell)
	it := orderSet.Iterator()
	if it.First() {
		return it.Value().(*Order)
	} else {
		return nil
	}
}

// 获得前size个订单
func (book *OrderBook) Take(buyOrSell OrderBuyOrSell, size int64) (values []*Order) {
	var orderSet = book.orderSet(buyOrSell)
	it := orderSet.Iterator()
	var i int64
	for i = 0; it.Next() && i < size; i++ {
		values = append(values, it.Value().(*Order))
	}
	return
}

// asksLessPrice 从买单簿队首开始，收集所有价格低于给定值的买单（按撮合优先级顺序）。
// 注意：函数名含 asks 但实际遍历的是 Buy 树（从买单视角找"低于某价"的挂单）。
func (book *OrderBook) asksLessPrice(price decimal.Decimal) (values []*Order) {
	var orderSet = book.orderSet(Buy)
	it := orderSet.Iterator()
	for it.Next() {
		order := it.Value().(*Order)
		if order.Price.GreaterThanOrEqual(price) {
			break
		}
		values = append(values, it.Value().(*Order))
	}
	return
}

// bidsGreater 从卖单簿队首开始，收集所有价格高于给定值的卖单（按撮合优先级顺序）。
// 注意：函数名含 bids 但实际遍历的是 Sell 树。
func (book *OrderBook) bidsGreater(price decimal.Decimal) (values []*Order) {
	var orderSet = book.orderSet(Sell)
	it := orderSet.Iterator()
	for it.Next() {
		order := it.Value().(*Order)
		if order.Price.LessThanOrEqual(price) {
			break
		}
		values = append(values, it.Value().(*Order))
	}
	return
}

// removeFromOrderSet 按订单方向将其从对应的红黑树中移除。
// 若树中不存在该订单，说明订单簿内部数据不一致（cache 与树不同步），直接 Fatal。
func (book *OrderBook) removeFromOrderSet(order *Order) bool {
	if order.BuyOrSell == Buy {
		if book.BuySet.Contains(order) {
			book.BuySet.Remove(order)
			return true
		} else {
			common.Fatal("book buyset not contain order:", order.OrderId)
			return false
		}
	} else {
		if book.SellSet.Contains(order) {
			book.SellSet.Remove(order)
			return true
		} else {
			common.Fatal("book sellset not contain order:", order.OrderId)
			return false
		}
	}
}

// 挂单
func (book *OrderBook) Enqueue(order *Order) {
	if _, ok := book.cache[order.OrderId]; ok {
		// 重复id
		common.Fatal("dup enqueue cached order id:", order.SeqId)
	}
	book.cache[order.OrderId] = order
	if order.BuyOrSell == Buy {
		book.BuySet.Add(order)
	} else {
		book.SellSet.Add(order)
	}
}

// Dequeue 将订单从订单簿中摘除：先从 cache 索引删除，再从对应的红黑树中移除。
// 订单全部成交、被撤销或触发自成交防护时调用。
// 若订单不在 cache 中，说明重复摘除或数据不一致，直接 Fatal。
func (book *OrderBook) Dequeue(orderId int64) {
	if deleteOrder, ok := book.cache[orderId]; ok {
		delete(book.cache, orderId)
		if !book.removeFromOrderSet(deleteOrder) {
			common.Fatal("order not in set:", orderId)
		}
	} else {
		common.Fatal("cached key is not in cache")
	}
}

// Clone 深拷贝整个订单簿：逐订单复制值（而非共享指针），
// 生成一个与原簿完全独立的副本。用于快照保存时避免与正在撮合的簿共享数据。
func (book *OrderBook) Clone() (newBook *OrderBook) {
	newBook = NewOrderBook()
	newBook.FromId = book.FromId
	newBook.Symbol = book.Symbol
	for id := range book.cache {
		order := book.cache[id]
		newOrder := *order
		newBook.Enqueue(&newOrder)
	}
	return
}

// Report 上报订单簿的运行指标（已处理 SeqId、买卖盘深度）到 dogstatsd，并输出日志。
func (book *OrderBook) Report() {
	dogstatsd.GaugeBySymbol("orderbook.currentId", cast.ToFloat64(book.FromId), book.Symbol)
	dogstatsd.GaugeBySymbol("orderbook.bids.size", cast.ToFloat64(book.BuySet.Size()), book.Symbol)
	dogstatsd.GaugeBySymbol("orderbook.asks.size", cast.ToFloat64(book.SellSet.Size()), book.Symbol)
	common.Info(fmt.Sprintf("%s order book status --- currentId[%d] bids.size[%d] asks.size[%d]",
		book.Symbol, book.FromId, book.BuySet.Size(), book.SellSet.Size()))
}

// CompareOrderBook 比较两个订单簿是否完全一致（symbol、FromId、cache 中每个订单、
// 以及卖单树的排序结果）。用于快照恢复正确性校验和测试。
func CompareOrderBook(book1 *OrderBook, book2 *OrderBook) bool {
	book1Orders := book1.SellSet.Values()
	book2Orders := book2.SellSet.Values()
	length1 := len(book1Orders)
	if length1 != len(book2Orders) {
		return false
	}

	if book1.Symbol != book2.Symbol ||
		book1.FromId != book2.FromId {
		return false
	}

	for key := range book1.cache {
		if !CompareOrder(book1.cache[key], book2.cache[key]) {
			common.Fatal("compare error1:", book1.cache[key], book2.cache[key], book2.FromId)
			return false
		}
	}

	for i := 0; i < length1; i++ {
		if !CompareOrder(book1Orders[i].(*Order), book2Orders[i].(*Order)) {
			return false
		}
	}
	return true
}

// CheckOrderBookAndClear 检查订单簿是否已清空（cache、买单树、卖单树均应为空），
// 未清空则记录错误日志。用于交易对下线或重置前的完整性检查。
func (book *OrderBook) CheckOrderBookAndClear() {
	if len(book.cache) > 0 {
		/*
			for orderID := range book.cache {
				delete(book.cache, orderID)
			}
		*/
		common.Error(book.Symbol, "cache not clear all")
	}

	if book.BuySet.Size() > 0 {
		//book.BuySet.Clear()
		common.Error(book.Symbol, "BuySet not clear all")
	}

	if book.SellSet.Size() > 0 {
		//book.SellSet.Clear()
		common.Error(book.Symbol, "SellSet not clear all")
	}
}
