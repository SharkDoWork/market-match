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

type TreeSet struct {
	*treeset.Set
}

func (set *TreeSet) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(set.Values()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

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

func newTreeSet() *TreeSet {
	return &TreeSet{treeset.NewWith(Comparator)}
}

type OrderBook struct {
	Symbol  string
	FromId  int64
	cache   map[int64]*Order
	BuySet  *TreeSet
	SellSet *TreeSet
}

func InitOrderBook(fromId int64, symbol string) *OrderBook {
	return &OrderBook{
		Symbol:  symbol,
		FromId:  fromId,
		cache:   make(map[int64]*Order),
		BuySet:  newTreeSet(),
		SellSet: newTreeSet(),
	}
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		cache:   make(map[int64]*Order),
		BuySet:  newTreeSet(),
		SellSet: newTreeSet(),
	}
}

func (book *OrderBook) String() string {
	return "OrderBook symbol:" + book.Symbol + " FromId:" + cast.ToString(book.FromId) +
		" size:" + cast.ToString(len(book.cache))
}

func (book *OrderBook) SetCache(order *Order) {
	book.cache[order.OrderId] = order
}

// 查找
func (book *OrderBook) Find(orderId int64) *Order {
	return book.cache[orderId]
}

func (book *OrderBook) Cache() map[int64]*Order {
	return book.cache
}

// 获得对手的头单
func (book *OrderBook) peekOppoOrder(order *Order) *Order {
	return book.Peek(order.oppoBuyOrSell())
}

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

func (book *OrderBook) Report() {
	dogstatsd.GaugeBySymbol("orderbook.currentId", cast.ToFloat64(book.FromId), book.Symbol)
	dogstatsd.GaugeBySymbol("orderbook.bids.size", cast.ToFloat64(book.BuySet.Size()), book.Symbol)
	dogstatsd.GaugeBySymbol("orderbook.asks.size", cast.ToFloat64(book.SellSet.Size()), book.Symbol)
	common.Info(fmt.Sprintf("%s order book status --- currentId[%d] bids.size[%d] asks.size[%d]",
		book.Symbol, book.FromId, book.BuySet.Size(), book.SellSet.Size()))
}

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
