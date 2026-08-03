// Package puller 负责从 MySQL 订单序列表中持续拉取订单，并按序推送给撮合引擎。
//
// 交易所的订单（下单/撤单等）先由上游服务按全局递增 id 写入订单序列表
// （每个交易对一张表，表名形如 aibit_spot_sequence_<symbol>），
// puller 以"自增 id 游标"的方式轮询该表，保证订单按 SeqId 严格有序地进入撮合引擎，
// 这是撮合结果确定性的前提。重启时从快照位点 lastId+1 继续拉取，做到不重不漏。
package puller

import (
	"database/sql"
	"fmt"
	"github.com/spf13/viper"
	"market-match/common"
	"market-match/config"
	"market-match/match"
	"market-match/statistics"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	DataSourceName string
	//Prepare        string
	DB       *sql.DB
	DBSymbol map[string]*sql.DB
	//dbStmt         *sql.Stmt
	dbSymbolsStmt  map[string]*sql.Stmt
	pullerInterval time.Duration // 无新订单时的基础轮询间隔
)

// 订单序列表中 type 字段的整型编码，对应撮合引擎内部的订单方向与类型组合
const (
	submitCancel      int32 = 0  // 撤销单（单个撤单）
	buyMarket         int32 = 1  // 买-市价单
	sellMarket        int32 = 2  // 卖-市价单
	buyLimit          int32 = 3  // 买-限价单
	sellLimit         int32 = 4  // 卖-限价单
	buyIoc            int32 = 5  // 买-IOC（立即成交剩余撤销）
	sellIoc           int32 = 6  // 卖-IOC
	buyFok            int32 = 7  // 买-FOK（全部成交否则撤销）
	sellFok           int32 = 8  // 卖-FOK
	buyLimitMaker     int32 = 9  // 买-只做 Maker 限价单
	sellLimitMaker    int32 = 10 // 卖-只做 Maker 限价单
	batchCancel       int32 = 13 // 批量撤单
	continuousAuction int32 = 22 // 连续竞价
	earlySettlement   int32 = 23 // 提前交割/结算
	settlement        int32 = 24 // 交割结算
	delivery          int32 = 25 // 交割
)

// DbInfoList 按 symbol 索引各交易对的数据库拉取实例
var DbInfoList = make(map[string]*DbInfo)

// DbInfo 封装单个交易对的 MySQL 连接、预编译查询语句及拉取参数
type DbInfo struct {
	DataSourceName string        // MySQL DSN 连接串
	symbol         string        // 交易对
	Prepare        string        // 拉取订单的 SQL 语句
	DB             *sql.DB       // 数据库连接池
	dbStmt         *sql.Stmt     // 预编译的拉单语句
	pullerInterval time.Duration // 无新订单时的轮询间隔
	ch             chan *match.Order // 订单输出通道（下游为撮合协程）
}

// Init 初始化指定交易对的订单拉取器：建立 MySQL 连接、预编译拉单 SQL，
// 启动后台拉单协程（从 fromId 开始），并加载该交易对的精度配置。
// fromId 通常为最近快照位点 lastId+1，保证重启后不重不漏。
func Init(ch chan *match.Order, symbol string, fromId int64) {
	db := &DbInfo{
		symbol: symbol,
		ch:     ch,
	}
	db.initDbInfo(symbol)
	db.pullerInterval = viper.GetDuration("mysql.pull-gap") * time.Millisecond
	db.GoPuller(ch, fromId)
	DbInfoList[symbol] = db
	db.InitSymbolConf()
}

// getPrepare 构造按 id 游标批量拉取订单的 SQL：从该 symbol 的订单序列表中
// 取 id >= ? 的最多 2000 条记录，按 id 升序返回。
//
//todo  可以根据配置文件生成语句
func getPrepare(symbol string) string {
	return "SELECT id        as id, " +
		"type      as `type`," +
		" order_id     as `order-id`," +
		" amount       as amount," +
		" price        as price," +
		" circuit_rate as `circuit-rate`," +
		" created_at   as `created-at`," +
		" user_id as `user-id`," +
		" stp as `stp`," +
		"taker as `taker`, " +
		"maker as `maker` " +
		" FROM " +
		fmt.Sprintf("aibit_spot_sequence_%s", symbol) + " " +
		"WHERE id >= ?   ORDER BY id ASC limit 2000 "

}

// initDbInfo 根据配置（mysql.user/password/endpoint/db）建立 MySQL 连接池，
// 并预编译该 symbol 的拉单 SQL。连接数上限由 mysql.conn-num 控制（默认 10）。
func (d *DbInfo) initDbInfo(symbol string) {
	d.DataSourceName = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8",
		config.GetString("mysql.user", ""),
		config.GetString("mysql.password", ""),
		config.GetString("mysql.endpoint", ""),
		config.GetString("mysql.db", ""),
	)
	var err error
	d.DB, err = sql.Open("mysql", d.DataSourceName)
	if err != nil {
		common.Fatal("open db error", err)
	}
	d.DB.SetMaxOpenConns(config.GetInt("mysql.conn-num", 10))
	d.DB.SetMaxIdleConns(config.GetInt("mysql.conn-num", 10))
	d.DB.SetConnMaxLifetime(8 * time.Hour) // set use forever

	d.Prepare = getPrepare(symbol)
	d.dbStmt, err = d.DB.Prepare(d.Prepare)
	if err != nil {
		common.Fatal("prepare sql error:", err)
	}
}

// GetMinIdFromDb 查询订单序列表中的最小订单 id（用于校验起始拉取位点的合法性）。
func GetMinIdFromDb() int64 {
	var Id int64
	DB.QueryRow("select min(f_id) from" +
		" t_order_sequence").Scan(&Id)
	return Id
}

// ExistSymbolInDb 判断指定交易对在订单序列表中是否存在记录。
func ExistSymbolInDb(symbol string) bool {

	query := "select 1 from " + fmt.Sprintf("t_order_sequence_%s", symbol) + "  where f_symbol = '" + symbol + "' limit 1;"
	var t int64
	err := DB.QueryRow(query).Scan(&t)
	if err == sql.ErrNoRows {
		return false
	} else if err != nil {
		common.Fatal("error:", err)
	}
	return true
}

// GoPuller 启动后台协程，以 fromId 为游标持续轮询数据库拉取订单并写入 ch。
//
// 拉取策略：
//   - 每轮拉到订单时，游标前进到最后一条订单 SeqId+1，立即继续下一轮；
//   - 没有新订单时先按 pullerInterval 短间隔轮询；
//   - 连续约 50 轮无新订单且游标未推进（已追上最新），退化为 1 秒长轮询，降低数据库压力。
//
// 异步去取订单
func (d *DbInfo) GoPuller(ch chan *match.Order, fromId int64) {
	longSleep := 0

	go func() {
		var nextFromId int64
		nextFromId = fromId
		for {

			orders := d.pullOrder(fromId)
			orderNum := len(orders)
			for i := range orders {
				// 记录拉取时间点，用于全链路耗时统计
				statistics.SetPullTag(orders[i].SeqId)
				fmt.Println("order , ", orders[i])
				ch <- orders[i]
			}

			if orderNum > 0 {
				longSleep = 0
				nextFromId = orders[orderNum-1].SeqId + 1
			} else {
				if longSleep >= 50 && nextFromId == fromId {
					time.Sleep(time.Second)
					longSleep = 0
				} else {
					time.Sleep(pullerInterval)
					longSleep++
				}
			}

			fromId = nextFromId

		}
	}()

	return
}

// pullOrder 拉取 fromId 之后的一批订单（当前实现直接转发给 getOrdersFromDb）。
func (d *DbInfo) pullOrder(fromId int64) (orders []*match.Order) {
	orders = d.getOrdersFromDb(fromId)
	return
}

// getOrdersFromDb 执行预编译 SQL，把数据库行扫描为 match.Order 对象，
// 并将整型订单类型编码转换为撮合引擎内部的（方向, 类型）枚举。
// 同一批订单共用相同的 PullTime（毫秒时间戳），用于耗时统计。
//
// sql 一次取1000
func (d *DbInfo) getOrdersFromDb(fromId int64) (orders []*match.Order) {
	results, err := d.dbStmt.Query(fromId)
	if err != nil {
		common.Error("exe sql error:", err)
		return
	}
	ts := time.Now().UnixMilli()
	for results.Next() {
		var orderIntType int32
		order := &match.Order{
			State:    match.Submitted,
			PullTime: ts,
		}
		/*
			return "SELECT id        as id, " +
				"type      as `type`," +
				" order_id     as `order-id`," +
				" amount       as amount," +
				" price        as price," +
				" circuit_rate as `circuit-rate`," +
				" created_at   as `created-at`," +
				" user_id as `user-id`," +
				" stp as `stp`," +
				"taker as `taker`, " +
				"maker as `maker` " +
				" FROM " +
				fmt.Sprintf("aibit_spot_sequence_%s", symbol) + " " +
				"WHERE id >= ?   ORDER BY id ASC limit 2000 "
		*/
		results.Scan(&order.SeqId, &orderIntType, &order.OrderId, &order.UnfilledAmount,
			&order.Price, &order.CircuitRate, &order.CreateAt, &order.UserId, &order.Stp, &order.Taker, &order.Maker)
		setOrderType(d.symbol, order, orderIntType)
		orders = append(orders, order)
	}
	return
}

// setOrderType 将数据库中的整型订单类型编码映射为撮合引擎内部的
// (BuyOrSell 方向, Type 类型) 枚举组合；随后校验订单的数量/价格精度，
// 精度不合法的订单被标记为 SystemCancel（系统撤销），不再参与正常撮合。
func setOrderType(symbol string, order *match.Order, orderIntType int32) {
	switch orderIntType {
	case submitCancel:
		order.BuyOrSell = match.Submit
		order.Type = match.Cancel
	case buyMarket:
		order.BuyOrSell = match.Buy
		order.Type = match.Market
	case sellMarket:
		order.BuyOrSell = match.Sell
		order.Type = match.Market
	case buyLimit:
		order.BuyOrSell = match.Buy
		order.Type = match.Limit
	case sellLimit:
		order.BuyOrSell = match.Sell
		order.Type = match.Limit
	case buyIoc:
		order.BuyOrSell = match.Buy
		order.Type = match.Ioc
	case sellIoc:
		order.BuyOrSell = match.Sell
		order.Type = match.Ioc
	case buyFok:
		order.BuyOrSell = match.Buy
		order.Type = match.Fok
	case sellFok:
		order.BuyOrSell = match.Sell
		order.Type = match.Fok
	case batchCancel:
		order.BuyOrSell = match.Submit
		order.Type = match.BatchCancel

	}

	ok := match.CheckOrderScale(symbol, order)
	if !ok {
		order.Type = match.SystemCancel
		common.Warn("system cancel :", order)
	}
	return
}

// InitSymbolConf 从交易对配置表（aibit_coin_pair_config）读取该 symbol 的价格精度
// （price_scale），并注册到全局配置中供撮合/行情模块使用；精度非法时直接 panic。
//
// 初始化币对深度
func (d *DbInfo) InitSymbolConf() {
	var scale int
	d.DB.QueryRow(fmt.Sprintf("SELECT  price_scale FROM aibit_coin_pair_config WHERE symbol = '%s' ", d.symbol)).Scan(&scale)
	if scale <= 0 {
		panic("error price scale " + d.symbol)
	}
	common.SetSymbolDepth(d.symbol, scale)

}
