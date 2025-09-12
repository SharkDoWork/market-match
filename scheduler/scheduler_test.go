package scheduler

import (
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"log"
	"market-match/common"
	"testing"
	"time"
)

func TestNewTickerAfter(t *testing.T) {
	//    ticker := newTickerAfter(time.Duration(1e9), time.Duration(2e9))
	//    log.Printf("test ticker start")
	//    i := 1
	//    for {
	//        select {
	//        case <-ticker.C:
	//            log.Printf("test ticker")
	//            i++
	//            if i > 3 {
	////                ticker.Stop()
	//                return
	//            }
	//        }
	//    }
}

func TestTickerStop(t *testing.T) {

}

func testNewTickerSnapshot() {
	//common.LogInit(common.LogLevel)
	common.LoadConfigViper()
	baseMs := cast.ToIntSlice(viper.Get("scheduler.snapshot"))[0]
	intervalMs := cast.ToIntSlice(viper.Get("scheduler.snapshot"))[1]
	ticker := newTickerBase(time.Duration(baseMs)*time.Millisecond,
		time.Duration(intervalMs)*time.Millisecond)
	for {
		select {
		case <-ticker.C:
			log.Println(common.TimeNowMs())
		}
	}
}

func TestNewTickerSnapshot(t *testing.T) {
	//   testNewTickerSnapshot()
}
