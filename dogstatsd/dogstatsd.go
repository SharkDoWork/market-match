// Package dogstatsd 是对 DataDog statsd 客户端的封装，用于上报撮合引擎的
// 运行指标（Gauge）、事件（Event）与耗时（TimeInMilliseconds）。
//
// 注意：当前版本中 DataDog 的实际连接与大部分上报逻辑被整段注释停用
// （见 Start 中的注释块），仅保留了全局 tag 组装与 Go 运行时内存指标
// 的日志输出。client 为 nil 时调用 Gauge 会panic，调用方需注意。
package dogstatsd

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"market-match/common"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

var client *statsd.Client // DataDog statsd 客户端（当前初始化代码被注释，实际为 nil）
var globalTags []string   // 附加到所有指标上的全局标签：app/profile/seq

// var timeReportChan chan *TimeReportObject

// type TimeReportObject struct {
// 	name  string
// 	value float64
// 	tags  []string
// }

// Start 初始化 dogstatsd 模块：组装全局标签（app 名称、环境 profile、实例序号 seq）。
// 实际的 statsd 客户端连接代码当前被注释停用，恢复时需取消下方注释块
// 并配置 datadog.statsd.address。
func Start() {
	// 注释datadog beg
	/**
	var err error
	client, err = statsd.New(viper.GetString("datadog.statsd.address"))
	if err != nil {
		common.Error("dogstatd connect failed error:", err)
		return
	}
	client.Namespace = "dawn_exchange_go" // TODO add config
	*/
	//注释datadog end
	globalTags = append(globalTags,
		"app:"+viper.GetString("app.name"),
		"profile:"+viper.GetString("app.profile"),
		"seq:"+strconv.Itoa(viper.GetInt("app.seq")),
	)

	// timeReportChan = make(chan *TimeReportObject, 50000)

	// timeReportGo()
	//metricsReport()
}

// GaugeBySymbol 上报带 symbol 标签的 Gauge 指标（如某交易对的通道长度、撮合位点）。
func GaugeBySymbol(name string, value float64, symbol string) {
	Gauge(name, value, "symbol:"+symbol)
}

// Gauge 上报一个 Gauge 类型指标，自动附加全局标签，指标名前加 "." 前缀。
// 注意：client 未初始化（Start 中连接代码被注释）时调用会 panic。
func Gauge(name string, value float64, tags ...string) error {
	tags = append(tags, globalTags...)
	err := client.Gauge("."+name, value, tags, 1)
	return err
}

// Event 上报一个 DataDog 事件（如服务崩溃告警）。当前实现被整体注释，为空操作。
func Event(title, text string, alertType statsd.EventAlertType) {
	//e := statsd.NewEvent(title, text)
	//e.AlertType = alertType
	//e.Tags = globalTags
	//client.Event(e)
	//common.Info("event title:", title, "text:", text)
}

// TimeInMilliseconds 上报耗时类指标（毫秒）。当前实现被整体注释，为空操作；
// 原设计是通过带缓冲的 channel 异步上报，通道满时丢弃 30% 旧数据以防阻塞主链路。
func TimeInMilliseconds(name string, value float64, tags ...string) {
	// capTR := cap(timeReportChan)
	// lenTR := len(timeReportChan)
	// if lenTR >= capTR {
	// 	sizeToDrop := int(float64(capTR) * 0.3)
	// 	for i := 0; i < sizeToDrop && len(timeReportChan) > 0; i++ {
	// 		<-timeReportChan
	// 	}
	// }
	//	tags = append(tags, globalTags...) //注释datadog
	// timeReportChan <- &TimeReportObject{name: name, value: value, tags: tags}
	//	client.TimeInMilliseconds(".timecost."+name, value, tags, 1) //注释datadog
}

// timeReportGo 是耗时指标的异步上报协程（当前整体被注释停用）：
// 从 timeReportChan 消费耗时记录并调用 statsd 客户端上报。
func timeReportGo() {
	// go func() {
	// 	for {
	// 		obj := <-timeReportChan
	// 		client.TimeInMilliseconds(".timecost."+obj.name, obj.value, obj.tags, 1)
	// 		//time.Sleep(100 * time.Millisecond)
	// 	}
	// }()
}

// metricsReport 启动一个每 10 秒触发一次的协程，原计划用于定期采集
// Go 运行时内存指标（readMemstats 调用当前被注释）。
func metricsReport() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				//readMemstats()
			}
		}
	}()
}

// readMemstats 读取 Go 运行时内存统计（堆/栈/GC/协程数），
// 既通过 Gauge 上报到 DataDog，也以日志形式打印一份 STATUS 摘要。
func readMemstats() {
	memStat := &runtime.MemStats{}
	runtime.ReadMemStats(memStat)
	heapInuse := memStat.HeapInuse
	heapMax := memStat.HeapSys
	stackInuse := memStat.StackInuse
	stackMax := memStat.StackSys
	pauseTotalNs := memStat.PauseTotalNs
	heapObjects := memStat.HeapObjects
	numGc := memStat.NumGC //NumGC is the number of completed GC cycles
	heapIdle := memStat.HeapIdle
	heapReleased := memStat.HeapReleased
	goroutineNum := runtime.NumGoroutine()

	Gauge("heap.used", cast.ToFloat64(heapInuse))
	Gauge("heap.max", cast.ToFloat64(heapMax))
	Gauge("heap.idle", cast.ToFloat64(heapIdle))
	Gauge("heap.heapReleased", cast.ToFloat64(heapReleased))
	Gauge("stack.used", cast.ToFloat64(stackInuse))
	Gauge("stack.max", cast.ToFloat64(stackMax))
	Gauge("pause.totalns", cast.ToFloat64(pauseTotalNs))
	Gauge("heapObjects.num", cast.ToFloat64(heapObjects))
	Gauge("numgc", cast.ToFloat64(numGc))

	common.Info(fmt.Sprintf("STATUS : heap.used[%d] heap.max[%d] heap.idle[%d] heap.heapReleased[%d] "+
		"stack.used[%d] stack.max[%d] pause.totalns[%d] heapObjects.num[%d] numgc[%d] numRoutine[%d]",
		heapInuse, heapMax, heapIdle, heapReleased, stackInuse, stackMax,
		pauseTotalNs, heapObjects, numGc, goroutineNum))

	// trick reduce print datadog error
	err := Gauge("goroutine.num", cast.ToFloat64(goroutineNum))
	if err != nil {
		common.Error("datadog cauge error:", err)
	}
}
