package rabbitmq

import (
	"bytes"
	"compress/zlib"
	rand "crypto/rand"
	"fmt"
	"market-match/common"
	"market-match/config"
	"market-match/dogstatsd"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/streadway/amqp"
)

type MqConnection struct {
	Connection *amqp.Connection
	Uri        string
}

type PublishContent struct {
	Exchange   string
	Routingkey string
	Content    []byte
	Ts         int64
}

type RabbitMq struct {
	Id          int
	Uris        []string
	Connection  *amqp.Connection
	amqpChannel *amqp.Channel
	publishChan chan *PublishContent
}

type TimeIntervalObject struct {
	name         string
	lastTs       int64
	lastReportTs int64
	value        int64
	count        int64
}

var (
	rabbitMqPool []*RabbitMq

	reportPublishCh chan int

	mqTs     map[string]*TimeIntervalObject
	mqTsLock *sync.RWMutex
)

func Init() {
	initRabbitmq(config.GetString("rabbitmq.protocol", "amqp"),
		config.GetString("rabbitmq.username", ""),
		config.GetString("rabbitmq.password", ""),
		config.GetStringSlice("rabbitmq.address", []string{}),
		config.GetString("rabbitmq.virtual-host", ""),
		config.GetInt("rabbitmq.conn-num", 10))
	StartReportPublishCount()
	mqTs = make(map[string]*TimeIntervalObject)
	mqTsLock = &sync.RWMutex{}
}

func initRabbitmq(protocol string, user string, pwd string, addresses []string, vHost string, count int) {
	uris := make([]string, 0)
	for _, address := range addresses {
		uris = append(uris, fmt.Sprintf("%s://%s:%s@%s/%s", protocol, user, pwd, address, vHost))
	}
	common.Trace("amqp:", uris)

	var i int
	for i = 0; i < count; i++ {
		rabbitMq := &RabbitMq{}
		rabbitMq.Init(i, uris)
		rabbitMqPool = append(rabbitMqPool, rabbitMq)
	}
}

func (rabbitMq *RabbitMq) Init(id int, uris []string) {
	rabbitMq.Id = id
	rabbitMq.Uris = uris
	err := rabbitMq.connect()
	if err != nil {
		common.Fatal("Failed to connect uri:", uris, "err:", err)
	}
	PublishChannelGo(rabbitMq)
}

func (rabbitMq *RabbitMq) ChooseUri() string {
	r, err := rand.Int(rand.Reader, big.NewInt(int64(len(rabbitMq.Uris))))
	if err != nil {
		common.Fatal("rabbitmq ChooseUri fatal!")
	}
	return rabbitMq.Uris[r.Int64()%int64(len(rabbitMq.Uris))]
}

func (rabbitMq *RabbitMq) connect() error {
	if rabbitMq.Connection != nil {
		rabbitMq.Connection.Close()
		rabbitMq.Connection = nil
	}
	uri := rabbitMq.ChooseUri()
	conn, err := amqp.Dial(uri)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	rabbitMq.amqpChannel = ch
	rabbitMq.Connection = conn
	return nil
}

func (rabbitMq *RabbitMq) reconnect() {
	common.Warn("start to reconnect")
	for {
		err := rabbitMq.connect()
		if err != nil {
			common.Warn("connect failed:", err)
			time.Sleep(time.Millisecond * 10)
			continue
		}
		break
	}
	common.Warn("reconnect success")
}

func (rabbitMq *RabbitMq) Publish(publishContent *PublishContent) {

	contentType := "text/json"
	content := publishContent.Content

	if config.GetBool("rabbitmq.compressed", false) {
		contentType = "text/plain"
		content = ZlibCompress(publishContent.Content)
	}

	ts00 := time.Now().UnixNano()

	for {
		err := rabbitMq.amqpChannel.Publish(publishContent.Exchange, publishContent.Routingkey,
			false, false,
			amqp.Publishing{
				ContentType: contentType,
				Body:        content,
			})
		if err != nil {
			switch Err := err.(type) {
			case *amqp.Error:
				if Err.Code == amqp.ChannelError {
					common.Error("ERR|RabbitMq.Publish|err:", err,
						"|", publishContent.Exchange, "|", publishContent.Routingkey)
					rabbitMq.reconnect()
					continue
				}
			}
			common.Error("Failed to publish a message", err,
				"|", publishContent.Exchange, "|", publishContent.Routingkey)
			time.Sleep(time.Millisecond * 10)
		} else {
			break
		}
	}

	ts01 := time.Now().UnixNano()
	tsAll := (ts01 - ts00) / int64(time.Millisecond)
	tsFromBuild := (ts01 - publishContent.Ts) / int64(time.Millisecond)

	if tsAll > 20 || (tsFromBuild > 50 && tsFromBuild < 10000) {
		common.Info("DEPTH|RabbitMq.Publish|timeout|tsAll:", tsAll, ", tsFromBuild:", tsFromBuild, ", key:", publishContent.Routingkey, "publishChan_len:", len(rabbitMq.publishChan))
	}

	if strings.Contains(publishContent.Routingkey, "depth.step") {

		mqTsLock.RLock()
		lastTs, ok := mqTs[publishContent.Routingkey]
		mqTsLock.RUnlock()

		if !ok {
			mqTsLock.Lock()
			mqTs[publishContent.Routingkey] = &TimeIntervalObject{name: publishContent.Routingkey, lastTs: publishContent.Ts, lastReportTs: publishContent.Ts, value: 0, count: 0}
			mqTsLock.Unlock()
		} else {

			tsSinceLast := publishContent.Ts - lastTs.lastTs
			tsSinceLastReport := publishContent.Ts - lastTs.lastReportTs

			if tsSinceLast > 150 {
				common.Info("DEPTH|RabbitMq.Publish|timeout|tsSinceLast:", tsSinceLast, ", tsAll:", tsAll, ", tsFromBuild:", tsFromBuild, ", key:", publishContent.Routingkey, "publishChan_len:", len(rabbitMq.publishChan))
			}

			if tsSinceLast >= 0 && tsSinceLast < 10000 {
				lastTs.value += tsSinceLast
				lastTs.count++
			}

			if tsSinceLastReport >= 10000 && lastTs.count > 0 {
				//avgTs := float64(lastTs.value / lastTs.count)
				//dogstatsd.TimeInMilliseconds(lastTs.name, avgTs)
				lastTs.lastReportTs = publishContent.Ts
				lastTs.value = 0
				lastTs.count = 0
			}

			lastTs.lastTs = publishContent.Ts

			mqTsLock.Lock()
			mqTs[publishContent.Routingkey] = lastTs
			mqTsLock.Unlock()
		}
	}

	addPublishCount()
}

func ZlibCompress(src []byte) []byte {
	var in bytes.Buffer
	w := zlib.NewWriter(&in)
	w.Write(src)
	w.Close()
	return in.Bytes()
}

func PublishWithChan(ch chan *PublishContent, exchangeName string, routingKey string, content []byte, ts int64) {
	publishContent := &PublishContent{
		Exchange:   exchangeName,
		Routingkey: routingKey,
		Content:    content,
		Ts:         ts,
	}
	ch <- publishContent
}

func PublishChannelGo(mq *RabbitMq) {
	publishChan := make(chan *PublishContent, 10000)
	mq.publishChan = publishChan
	gaugeName := "publish.channel.length." + strconv.Itoa(mq.Id)
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-ticker.C:
				dogstatsd.Gauge(gaugeName, cast.ToFloat64(len(publishChan)))
				common.Info("mq id:", strconv.Itoa(mq.Id), " chan length:", len(publishChan))
			case publishContent := <-publishChan:
				mq.Publish(publishContent)
			}
		}
	}()
}

func GetMatchResultRabbitMq(symbol string) chan *PublishContent {
	for i := range config.GetStringSlice("symbols", []string{}) {
		if symbol == config.GetStringSlice("symbols", []string{})[i] {
			return rabbitMqPool[i%len(rabbitMqPool)].publishChan
		}
	}

	return nil
}

func DeclareExchange(exchangeName string, exchangeType string, durable bool) {
	err := rabbitMqPool[0].amqpChannel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		durable,
		false,
		false,
		false,
		nil)
	if err != nil {
		common.Fatal("Failed to declare exchange:", exchangeName, exchangeType, durable, err)
	}
	return
}

func addPublishCount() {
	reportPublishCh <- 1
}

func StartReportPublishCount() {
	reportPublishCh = make(chan int, 10000)
	go func() {
		ticker := time.NewTicker(time.Second * 60)
		count := 0
		reportCount := 0
		for {
			select {
			case <-reportPublishCh:
				count++
				reportCount++
			case <-ticker.C:
				dogstatsd.Gauge("publish.msg.minute.count", cast.ToFloat64(reportCount))
				reportCount = 0
			}
		}
	}()
}
