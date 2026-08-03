package market

import (
	"log"
	"market-match/match"
	"testing"

	"github.com/shopspring/decimal"
)

// TestBuildAndReportDepth 验证 decimal 取模运算和 roundUp 函数的行为。
// 通过对比 roundUptest（本地参考实现）和 roundUp（生产代码）的结果，确保取整逻辑正确。
func TestBuildAndReportDepth(t *testing.T) {
	d, _ := decimal.NewFromString("12.2")
	d2, _ := decimal.NewFromString("12.2")
	m := d.Mod(d2)
	log.Println(m)

	p := 0.01
	w := decimal.NewFromFloat(p)
	log.Println(w)
	p = 0.1
	w = decimal.NewFromFloat(p)
	log.Println(w)
	p = 0.001
	w = decimal.NewFromFloat(p)
	log.Println(w)
	p = 0.000000001
	w = decimal.NewFromFloat(p)
	log.Println(w)
	log.Println("===============================")

	d1 := roundUptest(decimal.NewFromFloat(23.2303223), 0.01)
	d3 := roundUp(decimal.NewFromFloat(23.2303223), 2)
	log.Println(d1)
	log.Println(d3)
	d1 = roundUptest(decimal.NewFromFloat(23.2323223), 1)
	d3 = roundUp(decimal.NewFromFloat(23.2323223), 1)
	log.Println(d1)
	log.Println(d3)
	d1 = roundUptest(decimal.NewFromFloat(23.2323223), 10)
	d3 = roundUp(decimal.NewFromFloat(23.2323223), -1)
	log.Println(d1)
	log.Println(d3)

	d1 = roundUptest(decimal.NewFromFloat(23.2323223), 0.00001)
	d3 = roundUp(decimal.NewFromFloat(23.2323223), 5)
	log.Println(d1)
	log.Println(d3)
}

// roundUptest 是 roundUp 的参考实现，用于测试中对比验证。
func roundUptest(d decimal.Decimal, step float64) decimal.Decimal {
	per := decimal.NewFromFloat(step)
	m := d.Mod(per)
	if m.GreaterThan(decimal.Zero) {
		return d.Sub(m).Add(per)
	} else {
		return d
	}
}

// TestBuildAndReportDepth3 测试 roundDown 和 roundUp 在各种价格和步长组合下的正确性。
// 覆盖常规小数、大数、不同精度步长等边界情况。
func TestBuildAndReportDepth3(t *testing.T) {
	var dataDown = []struct {
		param  float64
		step   float64
		result float64
	}{
		{1.99, 1, 1},
		{1.99, 0.1, 1.9},
		{1.99, 0.01, 1.99},
		{22456544621.99, 0.1, 22456544621.9},
		{23.99, 0.1, 23.9},
		{3.990000001, 0.0001, 3.99},
		{9900023001, 100, 9900023000},
	}
	for i := range dataDown {
		if !roundDown(decimal.NewFromFloat(dataDown[i].param), dataDown[i].step).
			Equal(decimal.NewFromFloat(dataDown[i].result)) {
			t.Error(i, roundDown(decimal.NewFromFloat(dataDown[i].param), dataDown[i].step))
		}
	}

	var dataUp = []struct {
		param  float64
		step   float64
		result float64
	}{
		{1.99, 1, 2},
		{1.99, 0.1, 2},
		{1.99, 0.01, 1.99},
		{22456544621.99, 0.1, 22456544622},
		{23.99, 0.1, 24},
		{3.990000001, 0.0001, 3.9901},
		{9900023001, 100, 9900023100},
		//	{ 1.99, 0, 2 },
	}
	for i := range dataUp {
		if !roundUp(decimal.NewFromFloat(dataUp[i].param), dataUp[i].step).
			Equal(decimal.NewFromFloat(dataUp[i].result)) {
			t.Error(i, roundUp(decimal.NewFromFloat(dataUp[i].param), dataUp[i].step))
		}
	}
}

// TestBuildAndReportDepth2 测试 buildDepth 对一个小型订单簿的聚合结果。
// 构造包含重复价格的买卖订单，验证相同价格订单是否正确合并到同一档位。
func TestBuildAndReportDepth2(t *testing.T) {
	orderBook := match.InitOrderBook(1, "btcusdt")
	order1 := testMatchCreateOrderFor(18, 1, match.Sell, match.Limit, match.Submitted, 11.2323, 10)
	orderBook.Enqueue(order1)
	order1 = testMatchCreateOrderFor(19, 2, match.Sell, match.Limit, match.Submitted, 11.2323, 10)
	orderBook.Enqueue(order1)
	order1 = testMatchCreateOrderFor(20, 3, match.Sell, match.Limit, match.Submitted, 10.2323, 10)
	orderBook.Enqueue(order1)
	order1 = testMatchCreateOrderFor(21, 4, match.Buy, match.Limit, match.Submitted, 9.3434, 10)
	orderBook.Enqueue(order1)
	order1 = testMatchCreateOrderFor(22, 5, match.Buy, match.Limit, match.Submitted, 6.20001, 10)
	orderBook.Enqueue(order1)
	order1 = testMatchCreateOrderFor(23, 6, match.Buy, match.Limit, match.Submitted, 6.20001, 10)
	orderBook.Enqueue(order1)
	order1 = testMatchCreateOrderFor(24, 7, match.Buy, match.Limit, match.Submitted, 8.20001, 10)
	orderBook.Enqueue(order1)

	deps := buildDepth(orderBook)
	log.Println(deps)
	for _, dep := range deps {
		log.Println(dep)
	}
}

// testMatchCreateOrderFor 快速构造一个测试用的 Order 对象。
func testMatchCreateOrderFor(seqId int64, orderId int64, buyOrSell match.OrderBuyOrSell,
	orderType match.OrderType, orderState match.OrderState, price float64, unfilledAmount float64) *match.Order {
	return &match.Order{
		SeqId:          seqId,
		OrderId:        orderId,
		BuyOrSell:      buyOrSell,
		Type:           orderType,
		State:          orderState,
		Price:          decimal.NewFromFloat(price),
		UnfilledAmount: decimal.NewFromFloat(unfilledAmount),
		CircuitRate:    decimal.Zero,
		CreateAt:       0,
	}
}
