// Package persistence 测试文件：包含批量持久化性能基准、插入排序、SQL 拼接等功能的测试与辅助函数。
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

// init 设置测试用的配置文件路径，使测试能读取到正确的 MySQL 等配置。
func init() {
	os.Setenv(common.ENV_CONFFILE, "../conf/market-match.conf.yaml")
	var err error
	if err != nil {
		log.Println(err)
	}
}

// TestPersistMR 触发批量持久化基准测试。
func TestPersistMR(t *testing.T) {
	BanchPersistence()
}

// BanchPersistence 构造 100 万条模拟成交结果并持续写入持久化通道，
// 每秒打印一次已持久化条数，用于压测 MySQL 批量写入吞吐。
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

// TestPersistMR2 演示插入排序（Insert 函数）的行为，
// 随机生成 10 个整数插入有序切片并打印结果。
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

// TestPersistMR3 测试 insertSortMr 排序和 createSql 拼接：
// 先随机生成 12 条成交结果验证排序正确性，再序列化后拼接 SQL 并打印。
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

// Insert 把一个整数按升序插入到已排序的 int 切片中，返回新切片。
// 与 insertSortMr 逻辑相同，用于单独验证插入排序算法。
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

// createMR 构造一条指定 f_id 的模拟 MatchResult（含随机 0~3 条成交明细）。
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

// createMatchResult 构造一条模拟 MatchResult 并序列化为 JSON 字节，用于压测写入。
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

// createOrderResult 构造一条固定的模拟 OrderResult（maker 角色）。
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
