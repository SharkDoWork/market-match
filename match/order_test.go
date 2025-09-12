package match

import (
	"github.com/shopspring/decimal"
	"log"
	"testing"
	"time"
)

func init() {
}

func TestInitOrder(t *testing.T) {
	//f := big.NewFloat(0.43434343002342342342354676766)
	/*
			ToNearestEven RoundingMode = iota // == IEEE 754-2008 roundTiesToEven
		ToNearestAway                     // == IEEE 754-2008 roundTiesToAway
		ToZero                            // == IEEE 754-2008 roundTowardZero
		AwayFromZero                      // no IEEE 754-2008 equivalent
		ToNegativeInf                     // == IEEE 754-2008 roundTowardNegative
		ToPositiveInf
	*/
}

func TestOrder_String(t *testing.T) {
	order := Order{}
	str := order.String()
	if str != "(seqId:0 orderId:0 buyorsell:0 UnfilledAmount:0 price:0, circuitRate:0, state:0, type:0)" {
		t.Error(str)
	}
}

func TestOrder_SetState(t *testing.T) {
	var test = []struct {
		setState OrderState
		state    OrderState
	}{
		{
			Submitted,
			Submitted,
		},
		{
			PartialFilled, // 部分埋单
			PartialFilled,
		},
		{
			PartialCanceled,
			PartialCanceled,
		},
		{
			Canceled,
			Canceled,
		},
		{
			Filled,
			Filled,
		},
		{
			Failed,
			Failed,
		},
		{
			Error,
			Error,
		},
		{
			CircuitCanceled,
			CircuitCanceled,
		},
	}
	order := &Order{}
	for _, tt := range test {
		state := order.SetState(tt.setState).State
		if state != order.State {
			t.Error(tt)
		}
	}
}
func TestOrder_OrderCombineTypeStr(t *testing.T) {
	var test = []struct {
		buyOrSell OrderBuyOrSell
		orderType OrderType
		resultStr string
	}{
		{Buy, Market, "buy-market"},
		{Sell, Market, "sell-market"},
		{Buy, Limit, "buy-limit"},
		{Sell, Limit, "sell-limit"},
	}

	for _, tt := range test {
		order := &Order{
			Type:      tt.orderType,
			BuyOrSell: tt.buyOrSell,
		}
		str := order.OrderCombineTypeStr()
		if str != tt.resultStr {
			t.Error(str)
		}
	}
}

func TestComparator(t *testing.T) {
	var test = []struct {
		order1 *Order
		order2 *Order
		result int
	}{
		{
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			0,
		},
		{
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			testCreateOrder(2, 2, Buy, Limit, 0.1, 0.2),
			-1,
		},
		{
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			testCreateOrder(3, 3, Buy, Limit, 1.1, 0.2),
			1,
		},
		{
			testCreateOrder(1, 2, Buy, Limit, 10000000022222.1, 0.2),
			testCreateOrder(1, 2, Buy, Market, 0.1000, 0.2),
			-1,
		},
		{
			testCreateOrder(12, 22, Buy, Limit, 1.111111111111111, 0.299),
			testCreateOrder(1, 2, Buy, Limit, 1.111111111111111, 0.2),
			1,
		},
		{
			testCreateOrder(1, 2, Sell, Limit, 0.1, 0.2),
			testCreateOrder(1, 2, Sell, Limit, 0.1, 1.2),
			0,
		},
		{
			testCreateOrder(1, 2, Sell, Limit, 0.1, 0.2),
			testCreateOrder(2, 2, Sell, Limit, 0.1, 0.2),
			-1,
		},
		{
			testCreateOrder(1, 2, Sell, Limit, 0.1, 0.2),
			testCreateOrder(2, 2, Sell, Limit, 1123, 0.2),
			-1,
		},
		{
			testCreateOrder(1, 2, Sell, Limit, 222223232.1, 0.2),
			testCreateOrder(2, 2, Sell, Limit, 11, 0.2),
			1,
		},
	}

	for _, tt := range test {
		if Comparator(tt.order1, tt.order2) != tt.result {
			t.Error(tt.order1, tt.order2)
		}
	}
}

func TestCompareOrder(t *testing.T) {
	var test = []struct {
		order1 *Order
		order2 *Order
		result bool
	}{
		{
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			true,
		},
		{
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			testCreateOrder(2, 2, Buy, Limit, 0.1, 0.2),
			false,
		},
		{
			testCreateOrder(1, 2, Buy, Limit, 0.1, 0.2),
			testCreateOrder(3, 3, Buy, Limit, 1.1, 0.2),
			false,
		},
		{
			testCreateOrder(1, 2, Buy, Limit, 10000000022222.1, 0.2),
			testCreateOrder(1, 2, Buy, Market, 0.1000, 0.2),
			false,
		},
	}
	for i, tt := range test {
		if CompareOrder(tt.order1, tt.order2) != tt.result {
			t.Error(i, tt.order1, tt.order2)
		}
	}

}

func testCreateOrder(seqId int64, orderId int64, buyOrSell OrderBuyOrSell,
	orderType OrderType, price float64, amount float64) *Order {
	return &Order{
		SeqId:          seqId,
		OrderId:        orderId,
		BuyOrSell:      buyOrSell,
		Type:           orderType,
		State:          Submitted,
		Price:          decimal.NewFromFloat(price),
		UnfilledAmount: decimal.NewFromFloat(amount),
		CircuitRate:    decimal.NewFromFloat(0.1),
		CreateAt:       time.Now().UnixNano() / 1000000,
	}
}

func TestGetBuyOrSell(t *testing.T) {
	decimal.DivisionPrecision = 30
	d4 := decimal.NewFromFloat(2).Div(decimal.NewFromFloat(3))
	d4.String() // output: "0.667"
	log.Println(d4)
}
