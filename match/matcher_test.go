package match

import (
	"fmt"
	"github.com/shopspring/decimal"
	"log"
	"testing"
	"unsafe"
)

func testMatchCreateOrderFor(seqId int64, orderId int64, buyOrSell OrderBuyOrSell,
	orderType OrderType, orderState OrderState, price float64, unfilledAmount float64) *Order {
	return &Order{
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

func testCreateOrderWithCircuitRate(seqId int64, orderId int64, buyOrSell OrderBuyOrSell,
	orderType OrderType, orderState OrderState, price float64, unfilledAmount float64, circuitRate float64) *Order {
	return &Order{
		SeqId:          seqId,
		OrderId:        orderId,
		BuyOrSell:      buyOrSell,
		Type:           orderType,
		State:          orderState,
		Price:          decimal.NewFromFloat(price),
		UnfilledAmount: decimal.NewFromFloat(unfilledAmount),
		CircuitRate:    decimal.NewFromFloat(circuitRate),
		CreateAt:       0,
	}
}

func NewDecimal(f float64) *decimal.Decimal {
	d := decimal.NewFromFloat(f)
	return &d
}

func TestMatchOrder(t *testing.T) {
	var testMatchAble = []struct {
		order1 *Order
		order2 *Order
		result bool
	}{
		// 限价单
		{
			testMatchCreateOrderFor(1, 2, Buy, Limit, PartialFilled, 10, 50),
			testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 10, 50),
			true,
		},
		{
			testMatchCreateOrderFor(1, 2, Sell, Limit, PartialFilled, 10, 50),
			testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 10, 50),
			true,
		},
		{
			testMatchCreateOrderFor(1, 2, Buy, Limit, PartialFilled, 20, 5),
			testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 10, 3),
			true,
		},
		{
			testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 5, 1.33),
			testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 10, 3),
			true,
		},
		{
			testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 10.000000001, 1000000),
			testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 10, 1),
			false,
		},
		{
			testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 9.999999999, 223),
			testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 10, 223),
			false,
		},
	}
	for _, tt := range testMatchAble {
		match := matchAble(tt.order1, tt.order2)
		if match != tt.result {
			t.Error(tt.order1, tt.order2, tt.result)
		}
	}

	var testMatchOrder = []struct {
		order1 *Order
		order2 *Order
		result *OrderResult
	}{
		{
			testMatchCreateOrderFor(20, 3, Sell, Limit, Submitted, 10, 10),
			testMatchCreateOrderFor(10, 2, Buy, Limit, Submitted, 10, 10),
			&OrderResult{OrderId: 2, Price: NewDecimal(10), Role: "maker", FilledAmount: NewDecimal(10), State: filledState},
		},
		{
			testMatchCreateOrderFor(20, 2, Buy, Limit, PartialFilled, 10, 5),
			testMatchCreateOrderFor(10, 3, Sell, Limit, PartialFilled, 10, 10),
			&OrderResult{OrderId: 3, Price: NewDecimal(10), Role: "maker", FilledAmount: NewDecimal(5), State: partialFilledState},
		},
		{
			testMatchCreateOrderFor(200, 1, Sell, Limit, Submitted, 10, 5),
			testMatchCreateOrderFor(20, 2, Buy, Limit, Submitted, 20, 10),
			&OrderResult{OrderId: 2, Price: NewDecimal(20), Role: "maker", FilledAmount: NewDecimal(5), State: partialFilledState},
		},
		{
			testMatchCreateOrderFor(323, 2, Buy, Limit, PartialFilled, 20, 5),
			testMatchCreateOrderFor(1, 3, Sell, Limit, Submitted, 10, 3),
			&OrderResult{OrderId: 3, Price: NewDecimal(10), Role: "maker", FilledAmount: NewDecimal(3), State: filledState},
		},
	}

	for _, tt := range testMatchOrder {
		result := matchOrder(nil, tt.order1, tt.order2)
		jsonResult, err := json.Marshal(result)
		if err != nil {
			t.Error("encode err :", err)
		}
		jsonResult2, err := json.Marshal(tt.result)
		if err != nil {
			t.Error("encode err :", err)
		}
		if string(jsonResult) != string(jsonResult2) {
			t.Error(string(jsonResult), string(jsonResult2))
		}

	}

}

func TestOrderBook_Match(t *testing.T) {
	//config.LoadConfig()
	//book := InitOrderBook(0, "btcetc")
	//var i int64
	//for i = 0; i < 10; i++  {
	//   order := testGenOrder(i)
	//   matchResult := book.GenMatchResult(order)
	//   b, _:=json.Marshal(matchResult)
	//   log.Println("results: ", string(b))
	//}

	//testMatchFilled()
	//testMatchPartFilled()
	//testMatchFilled1()
	//testMatchPartFilled1()
	//testMatchMarket()
	//testMatchMarketSellFloat()
	//  testMatchMarketBuyFloat()
	decimal.DivisionPrecision = 37
	d, _ := decimal.NewFromString("0.059")
	d1, _ := decimal.NewFromString("0.01988")
	w := d.Div(d1)
	s := w.Truncate(18)
	log.Println(s)
	log.Println(s.Exponent())
}

func TestOrderResult_MarshalJSON(t *testing.T) {
	decimal.MarshalJSONWithoutQuotes = true
	price := decimal.NewFromFloat(1.23)
	unfilledAmount := decimal.NewFromFloat(223.23)
	filledAmount := decimal.NewFromFloat(1223.23)
	orderResult := &OrderResult{
		OrderId:        1,
		Role:           "taker",
		Price:          &price,
		UnfilledAmount: &unfilledAmount,
		FilledAmount:   &filledAmount,
		State:          "filled",
	}

	bytes, err := orderResult.MarshalJSON()
	if err != nil {
		t.Error(err)
	}
	if string(bytes) != "{\"user-id\":0,\"order-id\":1,\"role\":\"taker\",\"price\":1.23,\"unfilled-amount\":223.23,\"state\":\"filled\"}" {
		t.Error(string(bytes))
	}
}

func testMatchFilled() {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 24, 200)
	order2 := testMatchCreateOrderFor(2, 3, Sell, Limit, Submitted, 24, 200)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "btcetc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))
}

func TestOrderBook_GenMatchResult(t *testing.T) {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 24, 200)
	order1.CreateAt = 1523328694262
	order2 := testMatchCreateOrderFor(2, 3, Sell, Limit, Submitted, 24, 200)
	order2.CreateAt = 1523328360649
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "btcetc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1.Mr)
	resultb := "{\"id\":1,\"user-id\":0,\"symbol\":\"btcetc\",\"price\":0,\"ts\":1523328694262,\"order-type\":\"buy-limit\",\"items\":" +
		"[{\"order-id\":2,\"role\":\"taker\",\"user-id\":0,\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
		"\"publish-ts\":0}"
	if result, err := ResultEqual(string(b), resultb); result != true {
		t.Error(result, err, string(b))
	}

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2.Mr)
	resultb1 := "{\"id\":2,\"user-id\":0,\"symbol\":\"btcetc\",\"price\":24,\"ts\":1523328360649,\"order-type\":\"sell-limit\",\"items\":" +
		"[{\"order-id\":2,\"role\":\"maker\",\"price\":\"24\",\"user-id\":0,\"filled-amount\":\"200\",\"state\":\"filled\"}," +
		"{\"order-id\":3,\"role\":\"taker\",\"price\":\"24\",\"user-id\":0,\"unfilled-amount\":\"0\",\"state\":\"filled\"}]" +
		",\"publish-ts\":0}"
	if result, err := ResultEqual(string(b1), resultb1); result != true {
		t.Error(result, err, string(b1))
	}
}

func TestResultEqual(t *testing.T) {
	var checkData = []struct {
		str1   string
		str2   string
		result bool
	}{
		{
			"{\"id\":1,\"symbol\":\"btcetc\",\"ts\":1523328694262,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			"{\"id\":1,\"symbol\":\"btcetc\",\"ts\":1523328694262,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			true,
		},
		{
			"{\"id\":1,\"symbol\":\"btcusdt\",\"ts\":1523328694262,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			"{\"id\":1,\"symbol\":\"btcetc\",\"ts\":1523328694262,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			false,
		},
		{
			"{\"id\":1,\"symbol\":\"btcusdt\",\"ts\":1523328694262,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			"{\"id\":1,\"symbol\":\"btcetc\",\"ts\":1523328694269,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			false,
		},
		{
			"{\"id\":1,\"symbol\":\"btcusdt\",\"ts\":1523328694262,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":1523328694262}",
			"{\"id\":1,\"symbol\":\"btcetc\",\"ts\":1523328694269,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			false,
		},
		{
			"{\"id\":1,\"symbol\":\"btcetc\",\"ts\":1523328694269,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":1523328694262}",
			"{\"id\":1,\"symbol\":\"btcetc\",\"ts\":1523328694269,\"order-type\":\"buy-limit\",\"items\":" +
				"[{\"order-id\":2,\"role\":\"taker\",\"price\":\"24\",\"unfilled-amount\":\"200\",\"state\":\"submitted\"}]," +
				"\"publish-ts\":0}",
			true,
		},
	}

	for i := range checkData {
		if result, err := ResultEqual(checkData[i].str1, checkData[i].str2); result != checkData[i].result {
			t.Error(i, err, checkData[i])
		}
	}
}

func ExampleMatchFilled1() {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 24, 200)
	order2 := testMatchCreateOrderFor(2, 3, Sell, Limit, Submitted, 23, 200)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "btcetc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))
}
func ExampleMatchPartFilled0() {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 24, 200)
	order2 := testMatchCreateOrderFor(2, 3, Sell, Limit, Submitted, 23, 180)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "btcetc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))
}

func ExampleMatchPartFilled1() {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 24.334, 200)
	order2 := testMatchCreateOrderFor(2, 3, Sell, Limit, Submitted, 23.23, 180)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "btcetc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))
}

func ExampleMatchMarket() {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 24.334, 200)
	order2 := testMatchCreateOrderFor(2, 3, Buy, Limit, Submitted, 23.334, 540)
	order3 := testMatchCreateOrderFor(3, 4, Buy, Limit, Submitted, 20.334, 72)
	order4 := testMatchCreateOrderFor(5, 9, Sell, Market, Submitted, 23.23, 1000)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "btcetc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))

	matchResult3 := book.GenMatchResult(order3)
	b3, _ := json.Marshal(matchResult3)
	log.Println("results:", string(b3))

	matchResult4 := book.GenMatchResult(order4)
	b4, _ := json.Marshal(matchResult4)
	log.Println("results:", string(b4))
}

func TestOrderBook_GenMatchResult2(t *testing.T) {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 24.334, 200)
	order2 := testMatchCreateOrderFor(2, 3, Buy, Limit, Submitted, 23.334, 540)
	order3 := testMatchCreateOrderFor(3, 4, Buy, Limit, Submitted, 20.334, 72)

	order4 := testMatchCreateOrderFor(4, 5, Sell, Limit, Submitted, 34.334, 200)
	order5 := testMatchCreateOrderFor(5, 6, Sell, Limit, Submitted, 33.334, 540)
	order6 := testMatchCreateOrderFor(6, 7, Sell, Limit, Submitted, 30.334, 72)

	order7 := testCreateOrderWithCircuitRate(7, 9, Sell, Market, Submitted, 23.23, 1000, 0.1)
	book := InitOrderBook(0, "btcetc")
	book.GenMatchResult(order1)
	book.GenMatchResult(order2)
	book.GenMatchResult(order3)
	book.GenMatchResult(order4)
	book.GenMatchResult(order5)
	book.GenMatchResult(order6)
	Result := book.GenMatchResult(order7)
	b, _ := json.Marshal(Result.Mr)
	if string(b) == "{\"id\":7,\"symbol\":\"btcetc\",\"ts\":0,\"order-type\":\"sell-market\",\"items\":[{\"order-id\":2,\"role\":\"maker\",\"price\":\"24.334\",\"filled-amount\":\"200\",\"state\":\"filled\"},{\"order-id\":3,\"role\":\"maker\",\"price\":\"23.334\",\"filled-amount\":\"540\",\"state\":\"filled\"},{\"order-id\":9,\"role\":\"taker\",\"unfilled-amount\":\"260\",\"state\":\"partial-canceled\"}],\"publish-ts\":1529999952656}" {
		t.Errorf("result equal")
	}
}

func ExampleMatchMarketSellFloat() {
	order1 := testMatchCreateOrderFor(1, 2, Buy, Limit, Submitted, 21.334, 200.2)
	order2 := testMatchCreateOrderFor(2, 3, Buy, Limit, Submitted, 22.234, 540.3)
	order3 := testMatchCreateOrderFor(3, 4, Buy, Limit, Submitted, 22.334, 72.9)
	order4 := testMatchCreateOrderFor(5, 9, Sell, Market, Submitted, 24.23, 1000.1)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "btcetc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))

	matchResult3 := book.GenMatchResult(order3)
	b3, _ := json.Marshal(matchResult3)
	log.Println("results:", string(b3))

	matchResult4 := book.GenMatchResult(order4)
	b4, _ := json.Marshal(matchResult4)
	log.Println("results:", string(b4))
}
func ExampleMatchMarketBuyFloat() {
	order1 := testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 21.334, 200.2)
	order2 := testMatchCreateOrderFor(2, 3, Sell, Limit, Submitted, 22.234, 540.3)
	order3 := testMatchCreateOrderFor(3, 4, Sell, Limit, Submitted, 22.334, 72.9)
	order4 := testMatchCreateOrderFor(5, 9, Buy, Market, Submitted, 24.23, 1000.1)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "ethbtc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))

	matchResult3 := book.GenMatchResult(order3)
	b3, _ := json.Marshal(matchResult3)
	log.Println("results:", string(b3))

	matchResult4 := book.GenMatchResult(order4)
	b4, _ := json.Marshal(matchResult4)
	log.Println("results:", string(b4))
}

func TestOrderBook_Match2(t *testing.T) {
	order1 := testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 90.334, 200.2)
	order2 := testMatchCreateOrderFor(2, 3, Sell, Fok, Submitted, 90.234, 540.3)
	order3 := testMatchCreateOrderFor(3, 4, Buy, Ioc, Submitted, 22.334, 72.9)
	order4 := testMatchCreateOrderFor(5, 9, Buy, Fok, Submitted, 24.23, 1000.1)
	// order1 := testMatchCreateOrderFor(1, 2, Buy,  Limit, 24, 200 )
	book := InitOrderBook(0, "ethbtc")
	matchResult1 := book.GenMatchResult(order1)
	b, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b))

	matchResult2 := book.GenMatchResult(order2)
	b1, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b1))

	matchResult3 := book.GenMatchResult(order3)
	b2, _ := json.Marshal(matchResult3)
	log.Println("results:", string(b2))

	matchResult4 := book.GenMatchResult(order4)
	b3, _ := json.Marshal(matchResult4)
	log.Println("results:", string(b3))

	order5 := testMatchCreateOrderFor(6, 10, Sell, Limit, Submitted, 9.1, 232)
	matchResult5 := book.GenMatchResult(order5)
	b4, _ := json.Marshal(matchResult5)
	log.Println("results:", string(b4))

	order6 := testMatchCreateOrderFor(7, 11, Buy, Limit, Submitted, 2.1, 231)
	matchResult6 := book.GenMatchResult(order6)
	b5, _ := json.Marshal(matchResult6)
	log.Println("results:", string(b5))

	order7 := testMatchCreateOrderFor(8, 12, Buy, Ioc, Submitted, 4.1, 1.5)
	matchResult7 := book.GenMatchResult(order7)
	b6, _ := json.Marshal(matchResult7)
	log.Println("results:", string(b6))

	order8 := testMatchCreateOrderFor(9, 13, Sell, Ioc, Submitted, 3.2, 2.3)
	matchResult8 := book.GenMatchResult(order8)
	b7, _ := json.Marshal(matchResult8)
	log.Println("results:", string(b7))

	order9 := testMatchCreateOrderFor(10, 14, Sell, Fok, Submitted, 3.2, 2.3)
	matchResult9 := book.GenMatchResult(order9)
	b8, _ := json.Marshal(matchResult9)
	log.Println("results:", string(b8))

	order10 := testMatchCreateOrderFor(11, 15, Buy, Fok, Submitted, 4.2, 2.3)
	matchResult10 := book.GenMatchResult(order10)
	b9, _ := json.Marshal(matchResult10)
	log.Println("results:", string(b9))
}

func TestOrderBook_Match_LimitMaker(t *testing.T) {
	book := InitOrderBook(0, "ethbtc")
	order1 := testMatchCreateOrderFor(1, 1, Sell, Limit, Submitted, 9.1, 232)
	matchResult1 := book.GenMatchResult(order1)
	b1, _ := json.Marshal(matchResult1)
	log.Println("results:", string(b1))

	order2 := testMatchCreateOrderFor(2, 2, Buy, Limit, Submitted, 2.1, 231)
	matchResult2 := book.GenMatchResult(order2)
	b2, _ := json.Marshal(matchResult2)
	log.Println("results:", string(b2))

	order3 := testMatchCreateOrderFor(3, 3, Buy, LimitMaker, Submitted, 10.1, 2)
	matchResult3 := book.GenMatchResult(order3)
	b3, _ := json.Marshal(matchResult3)
	log.Println("results:", string(b3))

	order4 := testMatchCreateOrderFor(4, 4, Buy, LimitMaker, Submitted, 3.1, 2)
	matchResult4 := book.GenMatchResult(order4)
	b4, _ := json.Marshal(matchResult4)
	log.Println("results:", string(b4))

	order5 := testMatchCreateOrderFor(5, 5, Sell, LimitMaker, Submitted, 1.1, 2)
	matchResult5 := book.GenMatchResult(order5)
	b5, _ := json.Marshal(matchResult5)
	log.Println("results:", string(b5))

	order6 := testMatchCreateOrderFor(6, 6, Sell, LimitMaker, Submitted, 8.1, 2)
	matchResult6 := book.GenMatchResult(order6)
	b6, _ := json.Marshal(matchResult6)
	log.Println("results:", string(b6))

}

func TestOrderBook_GenMatchResult7(t *testing.T) {
	order1 := testMatchCreateOrderFor(1, 2, Sell, Limit, Submitted, 25.29, 1)
	order2 := testMatchCreateOrderFor(2, 3, Buy, Market, Submitted, 423.33, 0.4615)

	book := InitOrderBook(0, "btchk")
	book.GenMatchResult(order1)
	Result := book.GenMatchResult(order2)
	b, _ := json.Marshal(Result)
	fmt.Println(Result.Mr)
	if string(b) == "{\"id\":7,\"symbol\":\"btcetc\",\"ts\":0,\"order-type\":\"sell-market\",\"items\":[{\"order-id\":2,\"role\":\"maker\",\"price\":\"24.334\",\"filled-amount\":\"200\",\"state\":\"filled\"},{\"order-id\":3,\"role\":\"maker\",\"price\":\"23.334\",\"filled-amount\":\"540\",\"state\":\"filled\"},{\"order-id\":9,\"role\":\"taker\",\"unfilled-amount\":\"260\",\"state\":\"partial-canceled\"}],\"publish-ts\":1529999952656}" {
		t.Errorf("result equal")
	}
}

func TestPublishResult(t *testing.T) {
	s, _ := decimal.NewFromString("0.0001")
	log.Println(-s.Exponent())
	order := &Order{}
	log.Println("=============:", unsafe.Sizeof(*order))
}

func TestBatchMatchResult(t *testing.T) {
	ex := make(map[string]string)
	ex["instrument-id"] = "btc232"
	mr := &MatchResult{
		Id:        1,
		ExtParams: ex,
	}
	b, _ := json.Marshal(mr)
	log.Println("byte :", string(b))

	mr2 := &MatchResult{
		Id: 2,
	}
	b2, _ := json.Marshal(mr2)
	log.Println("byte :", string(b2))

}
