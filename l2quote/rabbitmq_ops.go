// Package l2quote 本文件实现行情消息到 RabbitMQ 的批量发送。
// 设计要点（节流与打包）：
//   - 各计算协程（K 线、ticker、trade）不直接写 MQ，而是把消息投入内存队列 mqSendChan；
//   - sendToMQ 协程从队列取消息时，会一次性把当前积压在队列中的消息
//     （最多 mqBatchSize 条）打包成一个批次（[]MsgBundle）一次性发布；
//   - 打包后每条消息的原始 routing key 被放入 MsgBundle.CMD 字段，
//     由下游解包后按 CMD 分发，因此 MQ 层面的 routing key 写一个固定值即可；
//   - 配合 mqSendIntervalMS 定时触发，实现"尽可能实时、尽可能打包"的发送策略，
//     降低 MQ 发布频率，避免高频小消息打满网络与 broker。
package l2quote

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"market-match/common"
	"market-match/config"
	"market-match/rabbitmq"
	"time"
)

// MsgBundle 批量发送时的单条消息封装。
// CMD 存放原始 routing key（如 "market.BTC_USDT.kline.1min"），
// Data 为原始消息体（JSON），下游按 CMD 解包分发。
type MsgBundle struct {
	CMD  string              `json:"cmd"`
	Data jsoniter.RawMessage `json:"data"`
}

// MqMessage 内部 MQ 发送队列中流转的消息结构。
// RoutingKey 为该消息原本应使用的 MQ routing key，Body 为已序列化的消息体。
type MqMessage struct {
	Ts         time.Time
	Type       string
	Interval   string
	PairCode   string
	RoutingKey string
	Body       []byte
}

/*
伪MQ批量发送协程
逻辑：尽可能实时，尽可能打包
*/
func (L *L2quote) sendToMQ(mqCh chan *MqMessage) {
	var count int64
	var publishedPeriod int
	reportTicker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-reportTicker.C:
			common.Info(fmt.Sprintf("%s l2quote MQ status --- count[%d] published[%d/10sec]", L.symbol, count, publishedPeriod))
			publishedPeriod = 0
		case msg := <-mqCh:
			//case <-mqCh:
			// 批量发送的逻辑
			size := len(mqCh)
			if size > L.mqBatchSize {
				size = L.mqBatchSize
			}
			beans := BatchMqPub(msg, mqCh, size)
			count = count + int64(size) + 1
			publishedPeriod = publishedPeriod + size + 1

			data, err := json.Marshal(beans)
			if err != nil {
				common.Fatal(L.symbol, "quotation msg beans jsonlize error :", err)
			}
			rabbitmqCh := rabbitmq.GetMatchResultRabbitMq(L.symbol)
			// 打包后，发送到mq的routing key已经没有实际效果，写死一个
			routeKey := fmt.Sprintf("market.%s.kline.1min", L.symbol)
			exchangeName := config.GetString("app.profile", "market") + "." + config.GetString("rabbitmq.exchange.quotation", "l2quote")

			rabbitmq.PublishWithChan(rabbitmqCh, exchangeName, routeKey, data, msg.Ts.UnixNano()/int64(time.Millisecond))
		}
	}
}

// BatchMqPub 把首条消息 msg 与队列 ch 中后续 size 条消息打包成一个批次返回。
// 调用方负责保证 size 不超过队列当前积压量，避免阻塞在读空队列上。
func BatchMqPub(msg *MqMessage, ch chan *MqMessage, size int) (beans []*MsgBundle) {
	bean := &MsgBundle{}
	bean.CMD = msg.RoutingKey
	bean.Data = msg.Body
	beans = append(beans, bean)
	count := size
	for count > 0 {
		m := <-ch
		bean = &MsgBundle{}
		bean.CMD = m.RoutingKey
		bean.Data = m.Body
		beans = append(beans, bean)
		count--
	}
	return beans
}
