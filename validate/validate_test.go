package validate

import (
	"github.com/shopspring/decimal"
	"log"
	"market-match/match"
	"testing"
)

func TestValidateOrderbook(t *testing.T) {
	//  testValidateMatchResult()
}

func TestValidateOrderbook2(t *testing.T) {
	matchResult := &match.MatchResult{
		Id:           1,
		Symbol:       "ethbtc",
		Ts:           1235566,
		PublishTs:    23323,
		OrderTypeStr: "cancel",
	}
	matchResult2 := &match.MatchResult{
		Id:           1,
		Symbol:       "ethbtc",
		Ts:           1235566,
		PublishTs:    23323,
		OrderTypeStr: "cancel",
	}

	price := decimal.NewFromFloat(0.33)
	unfilledAmount := decimal.NewFromFloat(0.66)
	filledAmount := decimal.NewFromFloat(0.99)
	matchResult.Items = append(matchResult.Items,
		&match.OrderResult{
			OrderId:        111,
			Role:           "maker",
			Price:          &price,
			UnfilledAmount: &unfilledAmount,
			FilledAmount:   &filledAmount,
		})

	price1 := decimal.NewFromFloat(0.33)
	unfilledAmount1 := decimal.NewFromFloat(0.66)
	filledAmount1 := decimal.NewFromFloat(0.99)
	matchResult2.Items = append(matchResult.Items,
		&match.OrderResult{
			OrderId:        111,
			Role:           "maker",
			Price:          &price1,
			UnfilledAmount: &unfilledAmount1,
			FilledAmount:   &filledAmount1,
		})

	bytes1, _ := json.Marshal(matchResult)
	str1 := string(bytes1)

	bytes2, _ := json.Marshal(matchResult2)
	str2 := string(bytes2)

	e, err := match.ResultEqual(str1, str2)
	if err != nil {
		log.Println(err)
	}

	log.Println("equal =================:", e)

}

func TestValidateOrderbook3(t *testing.T) {
	price, err := decimal.NewFromString("23.233333333333333333333999999999")
	if err != nil {
		log.Println("err: ", err)
	}
	var sliceResult []*match.OrderResult
	orderResult := &match.OrderResult{
		OrderId: 10000101,
		Price:   &price,
		Role:    "taker",
	}
	sliceResult = append(sliceResult, orderResult)

	result1 := &match.MatchResult{
		Id:           1,
		Symbol:       "tbcusdt",
		Ts:           12234,
		OrderTypeStr: "filled",
		PublishTs:    22222,
		Items:        sliceResult,
	}
	s1, err := json.Marshal(result1)
	log.Println("s1:", string(s1))
	if err != nil {
		log.Println(" err :", err)
	}

	price1, err := decimal.NewFromString("23.233333333333333333333999999999")
	if err != nil {
		log.Println("err: ", err)
	}
	var sliceResult1 []*match.OrderResult
	orderResult1 := &match.OrderResult{
		OrderId: 10000101,
		Price:   &price1,
		Role:    "taker",
	}
	sliceResult1 = append(sliceResult1, orderResult1)

	result2 := &match.MatchResult{
		Id:           1,
		Symbol:       "tbcusdt",
		Ts:           12234,
		OrderTypeStr: "filled",
		Items:        sliceResult1,
		PublishTs:    22222,
	}

	s2, err := json.Marshal(result2)
	log.Println("s1:", string(s2))
	if err != nil {
		log.Println(" err :", err)
	}
	equal, err := match.ResultEqual(string(s1), string(s2))

	log.Println("equal:", equal, err)

}

//func testValidateMatchResult(){
//    common.LogInit("../log/exchange.log", common.LogLevel)
//    common.Conf, _= common.LoadConfig("../conf/preconf")
//    redis.InitClient()
//
//    result := &match.MatchResult{
//        Id:10001,
//        Symbol: "bchbtc",
//        Ts:common.TimestampNowMs(),
//        OrderTypeStr:"buy-limit",
//        PublishTs:common.TimestampNowMs(),
//    }
//    orderResult := &match.OrderResult{
//        OrderId:23434,
//        Role:"maker",
//        Price: 23.3434,
//        UnfilledAmount:22.33,
//        FilledAmount:223.3,
//        State:"filled",
//    }
//    result.Items = append(result.Items, orderResult)
//
//    bytes, err := json.Marshal(result)
//    if err != nil{
//        log.Fatal(err)
//    }
//
//    redis.Set("o" + strconv.FormatInt(result.Id, 10), string(bytes))
//    checkResult(result)
//}
