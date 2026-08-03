// order_book_test.go 订单簿（order_book.go）的单元测试：
// 覆盖挂单（Enqueue）、摘单（Dequeue）、按 ID 查找（Find）、队首查询（Peek）、
// 批量取单（Take）、缓存索引、订单簿比较（CompareOrderBook）与深拷贝（Clone）的正确性，
// 并包含一个万级订单规模下 Clone 耗时的性能测试。
package match

import (
	"log"
	"testing"
	"time"
)

// orderId 测试用的全局递增订单 ID，保证同一测试进程内 ID 不重复。
var orderId int64

// RandomOrder 测试辅助函数：构造一个仅含序号/ID/类型/方向的简单订单。
func RandomOrder(orderType OrderType, buyOrSell OrderBuyOrSell) *Order {
	orderId++
	return &Order{
		SeqId:     orderId,
		OrderId:   orderId,
		Type:      orderType,
		BuyOrSell: buyOrSell,
	}
}

func TestOrderBook_Enqueue(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	orderBook.Enqueue(RandomOrder(Limit, Buy))
	if orderBook.BuySet.Size() != 1 {
		t.Error()
	}
	orderBook.Enqueue(RandomOrder(Limit, Sell))
	if orderBook.SellSet.Size() != 1 {
		t.Error()
	}
	orderBook.Enqueue(RandomOrder(Limit, Buy))
	if orderBook.BuySet.Size() != 2 {
		t.Error(orderBook.BuySet.Size())
	}
}

func TestOrderBook_Dequeue(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	orderId := randomOrder.OrderId
	orderBook.Enqueue(randomOrder)
	orderBook.Dequeue(orderId)
	if orderBook.BuySet.Size() != 0 {
		t.Error(orderBook.BuySet.Size())
	}
}

func TestOrderBook_Find(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	orderId := randomOrder.OrderId
	orderBook.Enqueue(randomOrder)
	testOrder := orderBook.Find(orderId)
	if testOrder == nil {
		t.Error(orderId)
	}
	randomOrder = RandomOrder(Limit, Sell)
	orderId = randomOrder.OrderId
	orderBook.Enqueue(randomOrder)
	testOrder = orderBook.Find(orderId)
	if testOrder == nil {
		t.Error(orderId)
	}
}

func TestOrderBook_Peek(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	orderBook.Enqueue(randomOrder)
	peekOrder := orderBook.Peek(Buy)
	if peekOrder != randomOrder {
		t.Error(peekOrder)
	}
	randomOrder = RandomOrder(Limit, Sell)
	orderBook.Enqueue(randomOrder)
	peekOrder = orderBook.Peek(Sell)
	if peekOrder != randomOrder {
		t.Error(peekOrder)
	}
}

func TestOrderBook_Take(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	orderBook.Enqueue(randomOrder)

	randomOrder1 := RandomOrder(Limit, Buy)
	orderBook.Enqueue(randomOrder1)

	randomOrder2 := RandomOrder(Limit, Buy)
	orderBook.Enqueue(randomOrder2)
	orderSlice := orderBook.Take(Buy, 3)
	if orderSlice[0] != randomOrder {
		t.Error(orderSlice)
	}
	if orderSlice[1] != randomOrder1 {
		t.Error(orderSlice)
	}
	if orderSlice[2] != randomOrder2 {
		t.Error(orderSlice)
	}
}

func TestOrderBook_Cache(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	orderBook.Enqueue(randomOrder)
	cacheMap := orderBook.Cache()
	if len(cacheMap) != 1 {
		t.Error(cacheMap)
	}
}

func TestOrderBook_SetCache(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	orderBook.SetCache(randomOrder)
	cacheMap := orderBook.Cache()
	if randomOrder != cacheMap[randomOrder.SeqId] {
		t.Error(cacheMap)
	}
}

func TestCompareOrderBook(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	randomOrder2 := RandomOrder(Limit, Sell)
	orderBook.Enqueue(randomOrder)
	orderBook.Enqueue(randomOrder2)
	orderBook2 := InitOrderBook(1, "test")
	orderBook2.Enqueue(randomOrder)
	orderBook2.Enqueue(randomOrder2)
	result := CompareOrderBook(orderBook, orderBook2)
	if result != true {
		t.Error(result)
	}
}

func TestOrderBook_Clone(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	randomOrder := RandomOrder(Limit, Buy)
	randomOrder2 := RandomOrder(Limit, Sell)
	orderBook.Enqueue(randomOrder)
	orderBook.Enqueue(randomOrder2)
	orderBook1 := orderBook.Clone()
	result := CompareOrderBook(orderBook, orderBook1)
	if result != true {
		t.Error(result)
	}
	if orderBook1 == orderBook {
		t.Error("point equal")
	}

	for id := range orderBook.cache {
		if orderBook.cache[id] == orderBook1.cache[id] {
			t.Error("order point equal")
		}
	}
}

// TestOrderBook_Clone2 性能测试：2 万笔在簿订单规模下测量 Clone 的耗时（毫秒级输出）。
func TestOrderBook_Clone2(t *testing.T) {
	orderBook := InitOrderBook(1, "test")
	for i := 0; i < 10000; i++ {
		orderBook.Enqueue(RandomOrder(Limit, Buy))
		orderBook.Enqueue(RandomOrder(Limit, Sell))
	}

	time1 := time.Now().UnixNano()
	orderBook.Clone()
	time2 := time.Now().UnixNano()
	log.Println("use Ms :", (time2-time1)/1000000)
}
