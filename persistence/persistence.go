package persistence

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"market-match/common"
	"market-match/config"
	"market-match/match"
	"market-match/statistics"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cast"
)

var (
	selectSymbolStmt map[string]*sql.Stmt
)

var mrChan chan []byte

type sortData struct {
	mr    *match.MatchResult
	index int
}

type Persisten struct {
	DB            *sql.DB
	selectPrepare string
	selectStmt    *sql.Stmt
	symbol        string
	mrChan        chan []byte
	mrBatchChan   chan [][]byte
}

var DbPersistenList = make(map[string]*Persisten)

func Init(symbol string, ch chan []byte) {
	p := &Persisten{
		symbol:      symbol,
		mrChan:      ch,
		mrBatchChan: make(chan [][]byte, 1000),
	}
	p.initPersistenceInfo()
	DbPersistenList[symbol] = p
}

func (p *Persisten) initPersistenceInfo() {
	DataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8",
		config.GetString("persistence.user", ""),
		config.GetString("persistence.password", ""),
		config.GetString("persistence.endpoint", ""),
		config.GetString("persistence.db", ""),
	)

	var err error
	p.DB, err = sql.Open("mysql", DataSourceName)
	if err != nil {
		common.Fatal("open db error", err)
	}
	p.DB.SetMaxOpenConns(config.GetInt("persistence.conn-num", 10))
	p.DB.SetMaxIdleConns(config.GetInt("persistence.conn-num", 10))
	p.DB.SetConnMaxLifetime(8 * time.Hour) // set use forever

	p.selectPrepare = fmt.Sprintf("SELECT f_id, mr FROM t_exchange_match_result_%s WHERE "+
		"f_id >=? AND f_id <=? ", p.symbol)
	p.selectStmt, err = p.DB.Prepare(p.selectPrepare)
	if err != nil {
		common.Fatal("select prepare error:", err)
	}

	for i := 0; i < config.GetInt("persistence.conn-num", 10); i++ {
		p.goPersistence()
	}
	p.goBatch()
	p.GaugePersistChan()
	//p.GetSymbolPrecision()
}

func PersistMR(bytes []byte) {
	mrChan <- bytes
}

func (p *Persisten) goBatch() {
	go func() {
		for {
			bytes := <-p.mrChan
			size := len(p.mrChan)
			if size > viper.GetInt("persistence.batch-size")-1 {
				size = viper.GetInt("persistence.batch-size") - 1
			}
			batchBatch := make([][]byte, size+1)
			batchBatch[0] = bytes
			for i := 0; i < size; i++ {
				batchBatch[i+1] = <-p.mrChan
			}
			p.mrBatchChan <- batchBatch
		}
	}()
}

func (p *Persisten) goPersistence() {
	go func() {
		for {
			p.insertData(<-p.mrBatchChan)
		}
	}()
}

func (p *Persisten) insertData(datas [][]byte) {
	sqlStr := p.createSql(datas)
	result, err := p.DB.Exec(*sqlStr)
	if err != nil {
		common.Error("exe sql err:", err, result)
	}
	statistics.IncrPersistenceNum(len(datas))
}
func (p *Persisten) createSql(datas [][]byte) *string {
	sqlStr := fmt.Sprintf(Head, p.symbol)
	var sortDataSlice []*sortData
	for i := range datas {
		mr := &match.MatchResult{}
		sd := &sortData{}
		sd.mr = mr
		sd.index = i
		err := json.Unmarshal(datas[i], mr)
		if err != nil {
			common.Error("Unmarshal json error")
			continue
		}
		sortDataSlice = insertSortMr(sd, sortDataSlice)
	}

	for i := range sortDataSlice {
		sd := sortDataSlice[i]
		var role = GetRole(sd.mr)
		var extra = "{}"
		if role == "batch-cancel" {
			extra = GetExtra(sd.mr.Items)
		}

		data := "(" + cast.ToString(sd.mr.Id) + ",'" +
			sd.mr.Symbol + "'," +
			cast.ToString(sd.mr.Ts) + ",'" +
			getOrderDirection(sd.mr) + "','" +
			role + "','" +
			string(datas[sd.index]) + "','" +
			extra + "'),"

		data = strings.Replace(data, "\\", "\\\\", -1)
		sqlStr += data
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	return &sqlStr
}

func (p *Persisten) GaugePersistChan() {
	go func() {
		ticker := time.NewTicker(time.Second * 600)
		for {
			select {
			case <-ticker.C:
				common.Info("persistence.channel.length", len(p.mrChan))
				common.Info("persistence.batch.channel.length", len(p.mrBatchChan))
			}
		}
	}()
}

var Head = "insert ignore t_exchange_match_result_%s (f_id, symbol, ts, order_type, role, mr, extra) values "

func createSql(datas [][]byte) *string {
	sqlStr := Head
	var sortDataSlice []*sortData

	for i := range datas {
		mr := &match.MatchResult{}
		sd := &sortData{}
		sd.mr = mr
		sd.index = i
		err := json.Unmarshal(datas[i], mr)
		if err != nil {
			common.Error("Unmarshal json error")
			continue
		}
		sortDataSlice = insertSortMr(sd, sortDataSlice)
	}

	for i := range sortDataSlice {
		sd := sortDataSlice[i]
		var role = GetRole(sd.mr)
		var extra = "{}"
		if role == "batch-cancel" {
			extra = GetExtra(sd.mr.Items)
		}
		data := "(" + cast.ToString(sd.mr.Id) + ",'" +
			sd.mr.Symbol + "'," +
			cast.ToString(sd.mr.Ts) + ",'" +
			getOrderDirection(sd.mr) + "','" +
			role + "','" +
			string(datas[sd.index]) + "','" +
			extra + "'),"
		sqlStr += data
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	return &sqlStr
}

func insertSortMr(data *sortData, dataSlice []*sortData) []*sortData {
	for i := 0; i <= len(dataSlice); i++ {
		if i == len(dataSlice) {
			dataSlice = append(dataSlice, data)
			break
		}
		if data.mr.Id < dataSlice[i].mr.Id {
			rear := append([]*sortData{}, dataSlice[i:]...)
			dataSlice = append(append(dataSlice[0:i], data), rear...)
			break
		}
	}
	return dataSlice
}

func GetRole(mr *match.MatchResult) string {
	if mr.OrderTypeStr == "submit-cancel" {
		return "cancel"
	}
	if mr.OrderTypeStr == "submit-batch-cancel" {
		return "batch-cancel"
	}
	for _, item := range mr.Items {
		if item.Role == "maker" {
			return "maker"
		}
	}
	return "taker"
}

func getOrderDirection(mr *match.MatchResult) string {
	var direction string
	if "sell-limit" == mr.OrderTypeStr ||
		"sell-market" == mr.OrderTypeStr ||
		"sell-ioc" == mr.OrderTypeStr ||
		"sell-fok" == mr.OrderTypeStr ||
		"sell-limit-maker" == mr.OrderTypeStr {
		direction = "sell"
	} else if "buy-limit" == mr.OrderTypeStr ||
		"buy-market" == mr.OrderTypeStr ||
		"buy-ioc" == mr.OrderTypeStr ||
		"buy-fok" == mr.OrderTypeStr ||
		"buy-limit-maker" == mr.OrderTypeStr {
		direction = "buy"
	} else if "submit-cancel" == mr.OrderTypeStr || mr.OrderTypeStr == "submit-batch-cancel" {
		direction = "cancel"
	} else {
		common.Warn("unknown order type:", mr.Id, mr.OrderTypeStr)
		return ""
	}
	return direction
}

func (p *Persisten) GetMatchResult(fromId, toId int64) map[int64]string {
	resultMap := make(map[int64]string, 0)
	results, err := p.selectStmt.Query(fromId, toId)
	if err != nil {
		common.Error("query mr error:", err)
		return nil
	}
	for results.Next() {
		var key int64
		var value string
		err := results.Scan(&key, &value)
		if err != nil {
			common.Error("scan error:", err)
		}
		resultMap[key] = value
	}
	return resultMap
}

func GetExtra(items []*match.OrderResult) string {
	var (
		extraList []match.BatchCancelOrder
		extra     string
	)
	for _, res := range items {
		var cancelState bool
		if res.State == "canceled" || res.State == "partial-canceled" || res.State == "partial-canceled" {
			cancelState = true
		}
		extraList = append(extraList, match.BatchCancelOrder{
			OrderId:     res.OrderId,
			CancelState: cancelState,
		})
	}
	extraBytes, err := json.Marshal(extraList)
	if err != nil {
		common.Error("get match result failed list:%s; err: %s", extraList, err)
		return extra
	}
	extra = string(extraBytes)
	return extra
}
