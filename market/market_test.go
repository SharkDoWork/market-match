package market

import (
	"fmt"
	"github.com/shopspring/decimal"
	"market-match/common"
	"market-match/config"
	"market-match/match"

	"math/rand"
	"os"
	"testing"
)

//里面一些逻辑依赖配置与common库
func init() {
	os.Chdir("../")
	decimal.DivisionPrecision = 37 // 大于默认的精度。
	// init log with default setup
	//ok := common.LogInit(common.DefaultLogLevel)
	//if !ok {
	//	fmt.Println("log init failed, plz check whether there is dir \"./log\" for file:", common.LogFile)
	//	os.Exit(common.ErrnoLogInitFailed)
	//}
	//
	//// starting log
	//common.Trace("=============== starting server ===============")
	//
	//// load config
	//common.LoadConfigViper()

}

// initOrderBook 生成测试需要用到的 OrderBook 对象。
// 在盘口中间价 100.0 两侧分别构造买卖订单：卖单在中间价上方按步长递增，买单在中间价下方按步长递减。
// 每个价格档位内的订单数量等于档位序号（第 1 档 1 笔，第 2 档 2 笔...），便于验证聚合结果。
// type传入进来，简单的复用一下
func initOrderBook(orderType match.OrderType, depthStep common.DepthStep) *match.OrderBook {
	//OrderBook的操作没有完全开放，所以现在外面初始化所有的订单
	//订单数量 单边数量，总订单数*2
	//递增ID，测试就从0开始吧
	var seqID int64 = 0
	//订单ID，跟seqID区分一下从10000开始
	var orderID int64 = 10000

	var midPrice decimal.Decimal = decimal.NewFromFloat(100.0)

	var curPrice decimal.Decimal = midPrice

	orderBook := match.NewOrderBook()
	orderBook.Symbol = "bchbtc"
	exchangeName := fmt.Sprintf("%s.%s", config.GetString("app.name", "market"), config.GetString("rabbitmq.exchange.quotation", "l2quote"))
	MarketThreadInit(exchangeName, "bchbtc")
	//init maker order list

	// 构造卖单：从中间价上方一个步长开始，逐档向上，每档订单数递增
	curPrice = midPrice.Add(decimal.NewFromFloat(depthStep.Accuracy))
	for i := 1; i <= int(depthStep.Capacity); i++ {
		for j := 1; j <= i; j++ {
			var deltaPrice decimal.Decimal
			if j == 1 {
				deltaPrice = decimal.Zero
			} else {
				rand := decimal.NewFromFloat(float64(rand.Intn(1000-1) + 1)).Div(decimal.NewFromFloat(1000))
				deltaPrice = decimal.NewFromFloat(depthStep.Accuracy).Mul(rand)
			}
			o := &match.Order{
				SeqId:          seqID,
				BuyOrSell:      match.Sell,
				OrderId:        orderID,
				Type:           orderType,
				State:          match.Submitted, //这块状态无所谓
				Price:          curPrice.Add(deltaPrice),
				UnfilledAmount: decimal.NewFromFloat(1.0),
			}
			orderBook.Enqueue(o)
			seqID++
			orderID++
		}
		curPrice = curPrice.Add(decimal.NewFromFloat(depthStep.Accuracy))
	}

	// 构造买单：从中间价下方一个步长开始，逐档向下，每档订单数固定为 Capacity
	curPrice = midPrice.Sub(decimal.NewFromFloat(depthStep.Accuracy))
	for i := 1; i <= 10; i++ {
		for j := 1; j <= int(depthStep.Capacity); j++ {
			var deltaPrice decimal.Decimal
			if j == 1 {
				deltaPrice = decimal.Zero
			} else {
				rand := decimal.NewFromFloat(float64(rand.Intn(1000-1) + 1)).Div(decimal.NewFromFloat(1000))
				deltaPrice = decimal.NewFromFloat(depthStep.Accuracy).Mul(rand)
			}
			o := &match.Order{
				SeqId:          seqID,
				BuyOrSell:      match.Buy,
				OrderId:        orderID,
				Type:           orderType,
				State:          match.Submitted, //这块状态无所谓
				Price:          curPrice.Sub(deltaPrice),
				UnfilledAmount: decimal.NewFromFloat(1.0),
			}
			orderBook.Enqueue(o)
			seqID++
			orderID++
		}
		curPrice = curPrice.Sub(decimal.NewFromFloat(depthStep.Accuracy))
	}

	//init taker order list
	return orderBook

}

// TestBuildDepth 测试不同步长下的深度聚合结果。
// 验证卖盘各档位的挂单量是否与构造的订单数量一致（第 i 档应有 i 笔订单，每笔 1.0）。
func TestBuildDepth(t *testing.T) {
	od := initOrderBook(match.Market, common.DepthStep{Accuracy: 0.1, Capacity: 20})

	for _, depth := range buildDepth(od) {
		if depth.ch == "market.bchbtc.depth.step5" {
			for i := 0; i < 10; i++ {
				if depth.Asks[i][1].Equal(decimal.NewFromFloat(float64(i))) {
					t.Failed()
				}
			}
		}
	}

	od = initOrderBook(match.Market, common.DepthStep{Accuracy: 0.01, Capacity: 20})

	for _, depth := range buildDepth(od) {
		if depth.ch == "market.bchbtc.depth.step4" {
			for i := 0; i < 10; i++ {
				if depth.Asks[i][1].Equal(decimal.NewFromFloat(float64(i))) {
					t.Failed()
				}
			}
		}
	}

	od = initOrderBook(match.Market, common.DepthStep{Accuracy: 0.001, Capacity: 20})

	for _, depth := range buildDepth(od) {
		if depth.ch == "market.bchbtc.depth.step3" {
			for i := 0; i < 10; i++ {
				if depth.Asks[i][1].Equal(decimal.NewFromFloat(float64(i))) {
					t.Failed()
				}
			}
		}
	}

	od = initOrderBook(match.Market, common.DepthStep{Accuracy: 0.0001, Capacity: 20})

	for _, depth := range buildDepth(od) {
		if depth.ch == "market.bchbtc.depth.step2" {
			for i := 0; i < 10; i++ {
				if depth.Asks[i][1].Equal(decimal.NewFromFloat(float64(i))) {
					t.Failed()
				}
			}
		}
	}

	od = initOrderBook(match.Market, common.DepthStep{Accuracy: 0.00001, Capacity: 20})

	for _, depth := range buildDepth(od) {
		if depth.ch == "market.bchbtc.depth.step1" {
			for i := 0; i < 10; i++ {
				if depth.Asks[i][1].Equal(decimal.NewFromFloat(float64(i))) {
					t.Failed()
				}
			}
		}
	}

	od = initOrderBook(match.Market, common.DepthStep{Accuracy: 1, Capacity: 20})

	for _, depth := range buildDepth(od) {
		if depth.ch == "market.bchbtc.depth.step0" {
			for i := 0; i < 10; i++ {
				if depth.Asks[i][1].Equal(decimal.NewFromFloat(float64(i))) {
					t.Failed()
				}
			}
		}
	}

	// TODO 10percent的深度
	//od = initOrderBook(match.Market, common.DepthStep{ Accuracy:0.01, Capacity:10})
	//
	//depths := buildDepthPercent10(od)
	//
	//fmt.Printf("10percent : %+v\n", depths)
}
