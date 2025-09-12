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

var client *statsd.Client
var globalTags []string

// var timeReportChan chan *TimeReportObject

// type TimeReportObject struct {
// 	name  string
// 	value float64
// 	tags  []string
// }

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

func GaugeBySymbol(name string, value float64, symbol string) {
	Gauge(name, value, "symbol:"+symbol)
}

func Gauge(name string, value float64, tags ...string) error {
	tags = append(tags, globalTags...)
	err := client.Gauge("."+name, value, tags, 1)
	return err
}

func Event(title, text string, alertType statsd.EventAlertType) {
	//e := statsd.NewEvent(title, text)
	//e.AlertType = alertType
	//e.Tags = globalTags
	//client.Event(e)
	//common.Info("event title:", title, "text:", text)
}

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

func timeReportGo() {
	// go func() {
	// 	for {
	// 		obj := <-timeReportChan
	// 		client.TimeInMilliseconds(".timecost."+obj.name, obj.value, obj.tags, 1)
	// 		//time.Sleep(100 * time.Millisecond)
	// 	}
	// }()
}

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
