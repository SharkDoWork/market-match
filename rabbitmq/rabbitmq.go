// Package rabbitmq 封装 RabbitMQ 消息队列的客户端功能。
// 主要职责包括：交换机（Exchange）声明、订单消息的消费通道管理、
// 成交结果（MatchResult）等消息的异步发布，以及连接池与断线重连机制。
// 系统中各交易对的撮合结果通过本包发送到指定的 Exchange/RoutingKey，
// 下游的行情推送、深度订阅等服务再从 MQ 消费这些消息。
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

// MqConnection 封装一个 AMQP 连接及其对应的连接 URI。
type MqConnection struct {
	Connection *amqp.Connection
	Uri        string
}

// PublishContent 表示一条待发布的消息，
// 包含目标交换机、路由键、消息体以及消息构建时的时间戳（用于延迟统计）。
type PublishContent struct {
	Exchange   string // 目标交换机名称
	Routingkey string // 路由键，决定消息投递到哪些队列
	Content    []byte // 消息体（JSON 或压缩后的字节流）
	Ts         int64  // 消息构建时间戳（纳秒），用于计算发布延迟
}

// RabbitMq 表示一个 RabbitMQ 连接实例，
// 内部维护一条 AMQP 信道和一个带缓冲的发布 channel，
// 业务方把消息写入 publishChan，由后台 goroutine 异步发送。
type RabbitMq struct {
	Id          int                  // 连接实例编号（用于区分连接池中的多个连接）
	Uris        []string             // 候选连接 URI 列表（对应多个 MQ 节点，实现故障转移）
	Connection  *amqp.Connection     // 底层 AMQP 连接
	amqpChannel *amqp.Channel        // AMQP 信道，实际执行 Publish/Declare 操作
	publishChan chan *PublishContent // 异步发布缓冲 channel，业务方写入，后台 goroutine 消费
}

// TimeIntervalObject 用于统计某个 routing key 下相邻两条 depth 消息的时间间隔，
// 帮助监控行情推送的实时性。
type TimeIntervalObject struct {
	name         string // routing key 名称
	lastTs       int64  // 上一条消息的时间戳
	lastReportTs int64  // 上次上报统计的时间戳
	value        int64  // 统计窗口内间隔时间总和
	count        int64  // 统计窗口内样本数量
}

var (
	// rabbitMqPool 是 RabbitMq 连接池，按配置创建多个连接实例，
	// 不同交易对的消息会轮询分配到池中的连接上，实现负载均衡。
	rabbitMqPool []*RabbitMq

	// reportPublishCh 用于统计已发布消息数量的内部 channel。
	reportPublishCh chan int

	// mqTs 记录每个 depth.step 相关 routing key 的时间间隔统计信息。
	mqTs map[string]*TimeIntervalObject
	// mqTsLock 保护 mqTs map 的并发读写。
	mqTsLock *sync.RWMutex
)

// Init 初始化 RabbitMQ 模块：
// 从配置读取连接参数，创建连接池，并启动发布计数上报 goroutine。
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

// initRabbitmq 根据配置参数拼接连接 URI，并创建指定数量的连接实例放入连接池。
// 每个地址生成一条 amqp://user:pwd@address/vhost 形式的 URI，
// 多个地址意味着 MQ 集群部署，连接时随机选取实现简单的故障转移。
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

// Init 初始化单个 RabbitMq 连接实例：建立连接并启动异步发布 goroutine。
func (rabbitMq *RabbitMq) Init(id int, uris []string) {
	rabbitMq.Id = id
	rabbitMq.Uris = uris
	err := rabbitMq.connect()
	if err != nil {
		common.Fatal("Failed to connect uri:", uris, "err:", err)
	}
	PublishChannelGo(rabbitMq)
}

// ChooseUri 从候选 URI 列表中随机选择一个，
// 使用 crypto/rand 保证随机性，避免多个连接总是打到同一个 MQ 节点。
func (rabbitMq *RabbitMq) ChooseUri() string {
	r, err := rand.Int(rand.Reader, big.NewInt(int64(len(rabbitMq.Uris))))
	if err != nil {
		common.Fatal("rabbitmq ChooseUri fatal!")
	}
	return rabbitMq.Uris[r.Int64()%int64(len(rabbitMq.Uris))]
}

// connect 建立 AMQP 连接并创建信道。
// 如果已有旧连接会先关闭，保证重连时资源被正确释放。
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

// reconnect 不断尝试重新连接，直到成功为止。
// 每次失败间隔 10ms，避免在 MQ 不可用时疯狂重试耗尽资源。
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

// Publish 将一条消息发布到指定的 Exchange 和 RoutingKey。
// 流程：
//  1. 根据配置决定是否对消息体做 zlib 压缩（减小网络传输量）；
//  2. 循环尝试发送，遇到 channel 错误时自动重连并重试；
//  3. 统计发布耗时与消息从构建到发出的总延迟，超阈值时打日志告警；
//  4. 对 depth.step 类消息额外统计相邻消息时间间隔，监控行情推送实时性。
func (rabbitMq *RabbitMq) Publish(publishContent *PublishContent) {

	contentType := "text/json"
	content := publishContent.Content

	// 开启压缩后，消息体使用 zlib 压缩，ContentType 标记为 text/plain 以区别于普通 JSON
	if config.GetBool("rabbitmq.compressed", false) {
		contentType = "text/plain"
		content = ZlibCompress(publishContent.Content)
	}

	ts00 := time.Now().UnixNano()

	// 发送失败时循环重试：channel 错误触发重连，其他错误短暂等待后重试
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

	// 发布耗时超过 20ms，或消息从构建到发出超过 50ms（且小于 10s 排除异常值）时记录告警日志
	if tsAll > 20 || (tsFromBuild > 50 && tsFromBuild < 10000) {
		common.Info("DEPTH|RabbitMq.Publish|timeout|tsAll:", tsAll, ", tsFromBuild:", tsFromBuild, ", key:", publishContent.Routingkey, "publishChan_len:", len(rabbitMq.publishChan))
	}

	// 针对 depth.step 开头的 routing key，统计相邻两条消息的时间间隔，
	// 用于监控行情深度数据的推送频率是否正常
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

			// 相邻两条 depth 消息间隔超过 150ms 视为异常，记录日志
			if tsSinceLast > 150 {
				common.Info("DEPTH|RabbitMq.Publish|timeout|tsSinceLast:", tsSinceLast, ", tsAll:", tsAll, ", tsFromBuild:", tsFromBuild, ", key:", publishContent.Routingkey, "publishChan_len:", len(rabbitMq.publishChan))
			}

			// 累计有效样本（过滤掉负数或超过 10s 的异常间隔）
			if tsSinceLast >= 0 && tsSinceLast < 10000 {
				lastTs.value += tsSinceLast
				lastTs.count++
			}

			// 每 10s 重置一次统计窗口（原本用于上报平均间隔到 dogstatsd，已注释）
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

// ZlibCompress 使用 zlib 算法压缩字节流，用于减小大消息（如深度行情）的网络传输体积。
func ZlibCompress(src []byte) []byte {
	var in bytes.Buffer
	w := zlib.NewWriter(&in)
	w.Write(src)
	w.Close()
	return in.Bytes()
}

// PublishWithChan 把消息封装为 PublishContent 并写入指定的发布 channel，
// 由该 channel 对应的后台 goroutine 异步完成实际发送，避免阻塞业务协程。
func PublishWithChan(ch chan *PublishContent, exchangeName string, routingKey string, content []byte, ts int64) {
	publishContent := &PublishContent{
		Exchange:   exchangeName,
		Routingkey: routingKey,
		Content:    content,
		Ts:         ts,
	}
	ch <- publishContent
}

// PublishChannelGo 为一个 RabbitMq 连接实例创建带缓冲（容量 10000）的发布 channel，
// 并启动后台 goroutine 循环消费 channel 中的消息执行 Publish。
// 同时每 10 秒上报一次 channel 积压长度到 dogstatsd，用于监控消息堆积情况。
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

// GetMatchResultRabbitMq 根据交易对 symbol 返回对应的发布 channel。
// 按 symbol 在配置列表中的下标对连接池大小取模，把不同交易对的消息
// 均匀分配到连接池中的各个连接上，实现简单的负载均衡。
// 若 symbol 不在配置中则返回 nil。
func GetMatchResultRabbitMq(symbol string) chan *PublishContent {
	for i := range config.GetStringSlice("symbols", []string{}) {
		if symbol == config.GetStringSlice("symbols", []string{})[i] {
			return rabbitMqPool[i%len(rabbitMqPool)].publishChan
		}
	}

	return nil
}

// DeclareExchange 在连接池第一个连接的信道上声明交换机。
// exchangeType 常见取值：fanout（广播）、topic（按 routing key 模式匹配）、direct（精确匹配）。
// durable 为 true 时交换机在 MQ 重启后仍然存在。
// 声明失败会直接 Fatal 退出，因为交换机是消息投递的前提条件。
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

// addPublishCount 向统计 channel 发送一个计数信号，表示完成了一次消息发布。
func addPublishCount() {
	reportPublishCh <- 1
}

// StartReportPublishCount 启动发布计数上报 goroutine：
// 每收到一个计数信号累加一次，每 60 秒把这一分钟内的发布总数上报到 dogstatsd，
// 用于监控系统的消息吞吐量。
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
