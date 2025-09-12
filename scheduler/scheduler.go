package scheduler

import (
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"time"
)

type Ticker struct {
	C    chan time.Time
	stop chan bool
}

func (ticker *Ticker) Stop() {
	select {
	case ticker.stop <- true:
	}
}

// 固定时间点的 ticker 看逻辑
func newTickerBase(base time.Duration, d time.Duration) (ticker *Ticker) {
	nowNano := time.Now().UnixNano()
	after := nowNano/d.Nanoseconds()*d.Nanoseconds() + base.Nanoseconds() - nowNano
	ticker = newTickerAfter(time.Duration(after), d)
	return
}

// 延后after 开始间隔为d的ticker
func newTickerAfter(after time.Duration, d time.Duration) (ticker *Ticker) {
	ticker = &Ticker{
		C:    make(chan time.Time, 1),
		stop: make(chan bool, 1),
	}
	go func() {
		var realTicker *time.Ticker
		if after > 0 {
			time.Sleep(after)
			ticker.C <- time.Now()
		}
		realTicker = time.NewTicker(d)
		defer realTicker.Stop()

		for {
			select {
			case tick := <-realTicker.C:

				ticker.C <- tick
			case <-ticker.stop:
				return
			}
		}
	}()
	return
}

func NewTickerSnapshot() *Ticker {

	baseMs := cast.ToIntSlice(viper.Get("scheduler.snapshot"))[0]
	intervalMs := cast.ToIntSlice(viper.Get("scheduler.snapshot"))[1]
	seqOffset := time.Second * 60 * time.Duration(viper.GetInt("app.seq"))

	return newTickerBase(time.Duration(baseMs)*time.Millisecond+seqOffset,
		time.Duration(intervalMs)*time.Millisecond)
}

func NewTickerOrderbookReport() *Ticker {
	baseMs := cast.ToIntSlice(viper.Get("scheduler.orderbook-report"))[0]
	intervalMs := cast.ToIntSlice(viper.Get("scheduler.orderbook-report"))[1]

	return newTickerBase(time.Duration(baseMs)*time.Millisecond,
		time.Duration(intervalMs)*time.Millisecond)
}

// 整分钟ticker
func OMinuteTicker() *Ticker {
	after := 60 - time.Now().Second()
	interval := time.Minute
	return newTickerAfter(cast.ToDuration(after)*time.Second, interval)
}

func OMinuteBySecond(sec int) *Ticker {
	after := 60 + (60+sec-time.Now().Second())%60
	interval := time.Minute
	return newTickerAfter(cast.ToDuration(after)*time.Second, interval)
}
