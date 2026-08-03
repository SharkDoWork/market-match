package market

import (
	"market-match/common"
	"market-match/config"
	"market-match/rabbitmq"
	"math"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// MarketThread 是每个交易对独立的行情上报协程结构体。
// 它从 depthChan 中读取深度数据，批量打包后发送到 RabbitMQ。
type MarketThread struct {
	// 交易对标识
	symbol string
	// 交易所标识
	exchange string
	// 深度消息队列，撮合引擎生成深度后写入此 channel，由本协程消费
	depthChan chan *QuoteDepths
}

// MsgBundle 是发送到 MQ 的消息包装结构。
// CMD 字段存放路由键（routing key），Data 字段存放具体的深度数据。
type MsgBundle struct {
	CMD  string `json:"cmd"`
	Data *QuoteDepths
}

// thread 是 MarketThread 的主循环，持续从 depthChan 消费深度数据并上报到 MQ。
// 目前Thread只保留了一个向mq发送消息的作用，鉴于扩展考虑，先保留这个结构
func (m *MarketThread) thread() {
	for {
		ts00 := time.Now().UnixNano()
		depth := <-m.depthChan
		// 从 channel 中批量取出多条深度数据，减少 MQ 发送次数，提高吞吐
		batchSize := config.GetInt64("batch_result", 90) - 1
		chanLen := len(m.depthChan)
		sizeToBatch := int(math.Min(float64(batchSize), float64(chanLen)))
		sizeToBatch = int(math.Max(float64(sizeToBatch), 1))
		depthArr := make([]*QuoteDepths, sizeToBatch)
		depthArr[0] = depth
		for i := 1; i < sizeToBatch; i++ {
			_depth := <-m.depthChan
			depthArr[i] = _depth
		}
		// 将每条深度数据包装为 MsgBundle，CMD 为路由键
		bundleArr := make([]MsgBundle, sizeToBatch)
		for i, dpt := range depthArr {
			bundle := MsgBundle{}
			bundle.CMD = dpt.ch
			bundle.Data = dpt
			bundleArr[i] = bundle
		}
		content, err := json.Marshal(bundleArr)
		ts01 := time.Now().UnixNano()
		// bundle := MsgBundle{}
		// bundle.CMD = depth.Ch
		// bundle.Data = depth
		// content, err := json.Marshal([]MsgBundle{bundle})
		if err != nil {
			common.Fatal("content encode json error:", err)
		}
		rabbitmqCh := rabbitmq.GetMatchResultRabbitMq(m.symbol)
		//exchangeName := config.GetString("app.profile", "market") + "." + config.GetString("rabbitmq.exchange.quotation", "l2quote") + "." + m.symbol
		exchangeName := config.GetString("app.profile", "market") + "." + config.GetString("rabbitmq.exchange.quotation", "l2quote")
		ts02 := time.Now().UnixNano() / int64(time.Millisecond)
		rabbitmq.PublishWithChan(rabbitmqCh, exchangeName, depth.ch, content, depth.Ts)
		ts03 := time.Now().UnixNano() / int64(time.Millisecond)

		// 统计各阶段耗时，用于监控行情上报的延迟
		tsDepthChan := (ts01 - ts00) / int64(time.Millisecond)
		tsGetMq := (ts02 - ts01) / int64(time.Millisecond)
		tsPub := (ts03 - ts02) / int64(time.Millisecond)
		tsAll := (ts03 - ts00) / int64(time.Millisecond)
		tsFromBuild := (ts03 - depth.Ts) / int64(time.Millisecond)

		if tsAll > 20 || (tsFromBuild > 50 && tsFromBuild < 10000) {
			common.Info("DEPTH|MarketThread.thread|timeout|tsAll:", tsAll,
				", tsDepthChan:", tsDepthChan,
				", tsGetMq:", tsGetMq,
				", tsPub:", tsPub,
				", tsFromBuild:", tsFromBuild,
				", key:", depth.ch,
				"depthChan_len:", len(m.depthChan),
				"rabbitmqCh_len:", len(rabbitmqCh),
			)
		}
	}
}
