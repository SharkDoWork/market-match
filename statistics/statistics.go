package statistics

import (
	"github.com/spf13/cast"
	"market-match/common"
	"market-match/dogstatsd"
	"time"
)

var statMatchChan chan int
var statPersistenceChan chan int
var PersistenceNum = 0
var tagChan chan *statTag
var StatTagChan chan *statTag

type statTag struct {
	Id   int64
	Time int64 //microsecond
}

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

func IncrMatchNum() {
	statMatchChan <- 1
}

func IncrPersistenceNum(num int) {
	statPersistenceChan <- num
}

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
