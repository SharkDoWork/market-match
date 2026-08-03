// Package scheduler 测试文件：包含自定义 Ticker 的触发、停止、快照定时器等功能的测试用例。
package scheduler

import (
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"log"
	"market-match/common"
	"testing"
	"time"
)

// TestNewTickerAfter 测试延迟触发 Ticker 的行为（当前为注释掉的示例代码，占位）。
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

// TestTickerStop 测试 Ticker 停止功能（当前为空实现，占位）。
func TestTickerStop(t *testing.T) {

}

// testNewTickerSnapshot 加载真实配置，创建快照定时器并持续打印触发时间，
// 用于手动验证快照定时器是否按预期时间点触发。
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

// TestNewTickerSnapshot 测试快照定时器创建（当前为注释掉的手动验证入口，占位）。
func TestNewTickerSnapshot(t *testing.T) {
	//   testNewTickerSnapshot()
}
