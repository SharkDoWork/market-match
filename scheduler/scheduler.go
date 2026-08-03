// Package scheduler 封装撮合引擎使用的各类定时器（Ticker）。
// 与标准库 time.Ticker 的区别在于：本包的 Ticker 支持"对齐到固定时间点"触发，
// 例如每小时整点、每分钟的第 N 秒等，而不是简单的从启动时刻开始等间隔触发。
// 主要用途：定时生成订单簿快照（snapshot）、定时上报订单簿状态（orderbook-report）等。
package scheduler

import (
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"time"
)

// Ticker 是自定义定时器，语义与 time.Ticker 类似，
// 但支持首次触发时间对齐到固定时刻，并可通过 Stop 主动停止。
type Ticker struct {
	C    chan time.Time // 每次触发时向该 channel 发送当前时间
	stop chan bool      // 停止信号 channel
}

// Stop 停止定时器，后台 goroutine 收到信号后退出。
// 使用 select 非阻塞发送，避免重复调用 Stop 时死锁。
func (ticker *Ticker) Stop() {
	select {
	case ticker.stop <- true:
	}
}

// newTickerBase 创建一个对齐到固定时间点的 Ticker。
// base 表示每个周期内的触发偏移（如每小时的第 base 毫秒），
// d 表示触发间隔。首次触发时间会计算到下一个对齐点，之后每 d 触发一次。
// 例如 base=5min, d=1h，则每小时的第 5 分钟触发。
func newTickerBase(base time.Duration, d time.Duration) (ticker *Ticker) {
	nowNano := time.Now().UnixNano()
	// 计算距离下一个对齐点的等待时长：
	// 当前时间向下取整到 d 的整数倍，再加上 base 偏移，减去当前时间
	after := nowNano/d.Nanoseconds()*d.Nanoseconds() + base.Nanoseconds() - nowNano
	ticker = newTickerAfter(time.Duration(after), d)
	return
}

// newTickerAfter 创建一个延迟 after 后开始、之后每隔 d 触发一次的 Ticker。
// after <= 0 时立即开始等间隔触发；after > 0 时先睡眠等待，触发一次后再进入等间隔循环。
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

// NewTickerSnapshot 创建订单簿快照定时器。
// 从配置 scheduler.snapshot 读取 [基准偏移ms, 间隔ms]，
// 并叠加 app.seq * 60s 的实例偏移，使多实例部署时各实例的快照时间错开，
// 避免同时写快照造成存储压力。
func NewTickerSnapshot() *Ticker {

	baseMs := cast.ToIntSlice(viper.Get("scheduler.snapshot"))[0]
	intervalMs := cast.ToIntSlice(viper.Get("scheduler.snapshot"))[1]
	seqOffset := time.Second * 60 * time.Duration(viper.GetInt("app.seq"))

	return newTickerBase(time.Duration(baseMs)*time.Millisecond+seqOffset,
		time.Duration(intervalMs)*time.Millisecond)
}

// NewTickerOrderbookReport 创建订单簿状态上报定时器。
// 从配置 scheduler.orderbook-report 读取 [基准偏移ms, 间隔ms]，
// 按固定时间点周期触发，用于定期向监控系统上报订单簿深度等指标。
func NewTickerOrderbookReport() *Ticker {
	baseMs := cast.ToIntSlice(viper.Get("scheduler.orderbook-report"))[0]
	intervalMs := cast.ToIntSlice(viper.Get("scheduler.orderbook-report"))[1]

	return newTickerBase(time.Duration(baseMs)*time.Millisecond,
		time.Duration(intervalMs)*time.Millisecond)
}

// OMinuteTicker 创建整分钟对齐的 Ticker，每到下一分钟的第 0 秒触发，之后每分钟触发一次。
func OMinuteTicker() *Ticker {
	after := 60 - time.Now().Second()
	interval := time.Minute
	return newTickerAfter(cast.ToDuration(after)*time.Second, interval)
}

// OMinuteBySecond 创建每分钟第 sec 秒触发的 Ticker，
// 首次触发对齐到下一分钟的第 sec 秒，之后每分钟触发一次。
func OMinuteBySecond(sec int) *Ticker {
	after := 60 + (60+sec-time.Now().Second())%60
	interval := time.Minute
	return newTickerAfter(cast.ToDuration(after)*time.Second, interval)
}
