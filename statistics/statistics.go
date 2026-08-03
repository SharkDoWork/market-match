// Package statistics 提供撮合引擎的运行时性能统计能力。
//
// 包含两类统计：
//  1. 计数统计：每秒打印一次累计撮合笔数（MatchNum）与持久化笔数（PersistenceNum），
//     用于观察系统吞吐；
//  2. 全链路耗时统计：对每第 1000 笔订单，在 puller 拉取时（SetPullTag）与撮合完成时
//     （SetMatchTag）各记录一次时间戳，配对后计算"订单从拉取到撮合完成"的耗时，
//     并通过 dogstatsd 上报为 since_order_in_puller 指标（毫秒）。
//
// 所有统计都通过 channel 传递给独立协程处理，避免阻塞撮合主链路。
package statistics

import (
	"github.com/spf13/cast"
	"market-match/common"
	"market-match/dogstatsd"
	"time"
)

var statMatchChan chan int          // 撮合笔数计数通道
var statPersistenceChan chan int    // 持久化笔数计数通道
var PersistenceNum = 0              // 累计持久化笔数
var tagChan chan *statTag           // puller 拉取时间点通道（内部）
var StatTagChan chan *statTag       // 撮合完成时间点通道

// statTag 记录某笔订单在链路某个环节的时间戳
type statTag struct {
	Id   int64 // 订单序列号（SeqId）
	Time int64 //microsecond 微秒级时间戳
}

// Init 初始化各统计通道并启动统计协程：
//   - 每秒打印一次累计撮合笔数与持久化笔数；
//   - 启动 StatTag 协程做全链路耗时配对统计。
func Init() {
	statMatchChan = make(chan int, 10)
	statPersistenceChan = make(chan int, 10)
	tagChan = make(chan *statTag, 100)
	StatTagChan = make(chan *statTag, 100)
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		var matchNum = 0
		for {
			select {
			case <-ticker.C:
				common.Info("MatchNum:", matchNum, "persistenceNum:", PersistenceNum)
			case <-statMatchChan:
				matchNum++
			case num := <-statPersistenceChan:
				PersistenceNum += num
			}
		}
	}()
	StatTag()
}

// IncrMatchNum 将撮合完成笔数加一（异步，不阻塞调用方）。
func IncrMatchNum() {
	statMatchChan <- 1
}

// IncrPersistenceNum 累加持久化完成的笔数（异步）。
func IncrPersistenceNum(num int) {
	statPersistenceChan <- num
}

// SetPullTag 在 puller 拉取到订单时记录时间戳。
// 为控制开销只采样每第 1000 笔订单（SeqId 能被 1000 整除）。
func SetPullTag(id int64) {
	if id%1000 != 0 {
		return
	}
	tag := &statTag{
		Id:   id,
		Time: time.Now().UnixNano() / 1000,
	}
	tagChan <- tag
}

// SetMatchTag 在订单完成撮合时记录时间戳，与 SetPullTag 配对使用。
// 同样只采样每第 1000 笔订单。
func SetMatchTag(id int64) {
	if id%1000 != 0 {
		return
	}
	tag := &statTag{
		Id:   id,
		Time: time.Now().UnixNano() / 1000,
	}
	StatTagChan <- tag
}

// StatTag 启动耗时配对协程：缓存 puller 侧时间戳，等到同一 SeqId 的撮合完成
// 时间戳到达后，计算两者差值（即订单从被拉取到撮合完成的耗时，单位毫秒），
// 通过 dogstatsd 上报 since_order_in_puller 指标，随后从缓存中删除该记录。
func StatTag() {
	statTagMap := make(map[int64]int64, 0)
	go func() {
		for {
			select {
			case tag := <-tagChan:
				statTagMap[tag.Id] = tag.Time
			case tag := <-StatTagChan:
				if repTime, ok := statTagMap[tag.Id]; ok {
					dogstatsd.Gauge("since_order_in_puller", cast.ToFloat64(tag.Time-repTime)/1000)
					delete(statTagMap, tag.Id)
				}
			}
		}
	}()
}
