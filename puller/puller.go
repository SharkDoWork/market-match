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
	pullerInterval time.Duration
)

const (
	submitCancel      int32 = 0
	buyMarket         int32 = 1
	sellMarket        int32 = 2
	buyLimit          int32 = 3
	sellLimit         int32 = 4
	buyIoc            int32 = 5
	sellIoc           int32 = 6
	buyFok            int32 = 7
	sellFok           int32 = 8
	buyLimitMaker     int32 = 9
	sellLimitMaker    int32 = 10
	batchCancel       int32 = 13
	continuousAuction int32 = 22
	earlySettlement   int32 = 23
	settlement        int32 = 24
	delivery          int32 = 25
)

var DbInfoList = make(map[string]*DbInfo)

type DbInfo struct {
	DataSourceName string
	symbol         string
	Prepare        string
	DB             *sql.DB
	dbStmt         *sql.Stmt
	pullerInterval time.Duration
	ch             chan *match.Order
}

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

func GetMinIdFromDb() int64 {
	var Id int64
	DB.QueryRow("select min(f_id) from" +
		" t_order_sequence").Scan(&Id)
	return Id
}

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

func (d *DbInfo) pullOrder(fromId int64) (orders []*match.Order) {
	orders = d.getOrdersFromDb(fromId)
	return
}

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

// 初始化币对深度
func (d *DbInfo) InitSymbolConf() {
	var scale int
	d.DB.QueryRow(fmt.Sprintf("SELECT  price_scale FROM aibit_coin_pair_config WHERE symbol = '%s' ", d.symbol)).Scan(&scale)
	if scale <= 0 {
		panic("error price scale " + d.symbol)
	}
	common.SetSymbolDepth(d.symbol, scale)

}
