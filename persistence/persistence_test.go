package persistence

import (
	"encoding/json"
	"github.com/shopspring/decimal"
	"log"
	"market-match/common"
	"market-match/match"
	"market-match/statistics"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	os.Setenv(common.ENV_CONFFILE, "../conf/market-match.conf.yaml")
	var err error
	if err != nil {
		log.Println(err)
	}
}

func TestPersistMR(t *testing.T) {
	BanchPersistence()
}

func BanchPersistence() {
	//initPersistenceInfo()
	statistics.Init()
	var results [][]byte
	for i := 0; i < 1000000; i++ {
		results = append(results, createMatchResult(int64(i)))
	}
	go func() {
		for i := range results {
			PersistMR(results[i])
		}
	}()

	for {
		time.Sleep(time.Second)
		log.Println("persistence num:", statistics.PersistenceNum)
	}
}

func TestPersistMR2(t *testing.T) {
	//	var mrSlice []*match.MatchResult
	//	for i:= 0; i < 20;i ++{
	//		mr := createMR( rand.Int63n(300))
	////		mrSlice = append(mrSlice, mr)
	//		for n := 0; mr <=
	//	}

	log.Print(math.Log2(60))
	var tt []int
	//for i:= 0; i< 20;i ++{
	//	tt = append(tt, rand.Intn(80))
	//}
	for i := 0; i < 10; i++ {
		tt = Insert(rand.Intn(300), tt)
	}
	log.Print(tt)
}

func TestPersistMR3(t *testing.T) {
	//var mrSlice []*match.MatchResult
	var sortDataSlice []*sortData
	for i := 0; i < 12; i++ {
		mr := createMR(rand.Int63n(300))
		data := &sortData{}
		data.mr = mr
		data.index = i
		sortDataSlice = insertSortMr(data, sortDataSlice)
	}

	for i := range sortDataSlice {
		log.Print(sortDataSlice[i].index, sortDataSlice[i].mr.Id)
	}

	var dataSlice [][]byte
	for i := 0; i < 12; i++ {
		mr := createMR(rand.Int63n(300))
		data, err := json.Marshal(mr)
		if err != nil {
			log.Println(err)
		}
		dataSlice = append(dataSlice, data)
	}
	sql := createSql(dataSlice)
	log.Println(*sql)
}

func Insert(x int, tt []int) []int {
	for i := 0; i <= len(tt); i++ {
		if i == len(tt) {
			tt = append(tt, x)
			break
		}
		if x < tt[i] {
			rear := append([]int{}, tt[i:]...)
			tt = append(append(tt[0:i], x), rear...)
			break
		}
	}
	return tt
}
func createMR(id int64) *match.MatchResult {
	var orderResults []*match.OrderResult
	n := rand.Intn(4)
	//	log.Println("======",n)
	for i := 0; i < n; i++ {
		orderResults = append(orderResults, createOrderResult())
	}

	result := &match.MatchResult{
		Id:           id,
		Symbol:       "btcusdt",
		Ts:           common.TimestampNowMs(),
		OrderTypeStr: "sell-limit",
		Items:        orderResults,
		PublishTs:    common.TimestampNowMs(),
	}
	return result
}

func createMatchResult(id int64) []byte {
	var orderResults []*match.OrderResult
	n := rand.Intn(4)
	//	log.Println("======",n)
	for i := 0; i < n; i++ {
		orderResults = append(orderResults, createOrderResult())
	}

	result := &match.MatchResult{
		Id:           id,
		Symbol:       "btcusdt",
		Ts:           common.TimestampNowMs(),
		OrderTypeStr: "sell-limit",
		Items:        orderResults,
		PublishTs:    common.TimestampNowMs(),
	}
	bytes, err := json.Marshal(result)
	if err != nil {
		log.Print("err:", err)
	}
	return bytes
}

func createOrderResult() *match.OrderResult {
	price := decimal.NewFromFloat(12.32)
	filledAmount := decimal.NewFromFloat(223.222)
	unFilledAmount := decimal.NewFromFloat(2.1123)

	return &match.OrderResult{
		OrderId:        123232,
		Role:           "maker",
		Price:          &price,
		FilledAmount:   &filledAmount,
		UnfilledAmount: &unFilledAmount,
	}
}
