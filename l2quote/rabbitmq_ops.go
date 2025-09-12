package l2quote

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"market-match/common"
	"market-match/config"
	"market-match/rabbitmq"
	"time"
)

type MsgBundle struct {
	CMD  string              `json:"cmd"`
	Data jsoniter.RawMessage `json:"data"`
}

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
