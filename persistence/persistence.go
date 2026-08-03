// Package persistence 负责把撮合引擎产生的成交记录（MatchResult）批量持久化到 MySQL。
// 核心设计：每个交易对对应一个 Persisten 实例，内部通过两级 channel 流水线工作——
// 第一级 mrChan 接收单条成交结果，goBatch 协程把多条结果攒成批次写入 mrBatchChan，
// 第二级由多个 goPersistence 协程消费批次并执行批量 INSERT，从而大幅提升写入吞吐。
// 此外本包还提供按 ID 区间查询历史成交结果的能力，供 validate 包做订单簿回放校验。
package persistence

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"market-match/common"
	"market-match/config"
	"market-match/match"
	"market-match/statistics"
	"strings"
	"time"

	"github.com/spf13/viper"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cast"
)

var (
	// selectSymbolStmt 按交易对缓存预编译查询语句（当前未使用，保留扩展）。
	selectSymbolStmt map[string]*sql.Stmt
)

// mrChan 全局单条成交结果 channel（当前未使用，各交易对使用自己的 p.mrChan）。
var mrChan chan []byte

// sortData 包装一条 MatchResult 及其在原始批次中的下标，
// 用于在生成 SQL 时按 f_id 排序后仍能取回原始 JSON 字节。
type sortData struct {
	mr    *match.MatchResult // 反序列化后的成交结果
	index int                // 该结果在原始 datas 切片中的下标
}

// Persisten 表示一个交易对的持久化 worker，
// 持有独立的数据库连接、预编译语句和两级缓冲 channel。
type Persisten struct {
	DB            *sql.DB      // MySQL 连接池
	selectPrepare string       // 按 ID 区间查询成交结果的 SQL 模板
	selectStmt    *sql.Stmt    // 预编译后的查询语句
	symbol        string       // 交易对符号，如 btcusdt
	mrChan        chan []byte  // 接收单条成交结果 JSON 字节的 channel（第一级缓冲）
	mrBatchChan   chan [][]byte // 攒批后的成交结果切片 channel（第二级缓冲）
}

// DbPersistenList 按交易对 symbol 索引所有 Persisten 实例，
// 供 validate 等外部包查询历史成交结果。
var DbPersistenList = make(map[string]*Persisten)

// Init 为指定交易对创建并初始化一个 Persisten 实例，
// ch 为该交易对的成交结果输入 channel（通常由 match 包写入）。
func Init(symbol string, ch chan []byte) {
	p := &Persisten{
		symbol:      symbol,
		mrChan:      ch,
		mrBatchChan: make(chan [][]byte, 1000),
	}
	p.initPersistenceInfo()
	DbPersistenList[symbol] = p
}

// initPersistenceInfo 建立 MySQL 连接、预编译查询语句，
// 并启动批量写入所需的全部后台协程：
//   - conn-num 个 goPersistence 协程负责执行批量 INSERT；
//   - 1 个 goBatch 协程负责把单条结果攒成批次；
//   - 1 个 GaugePersistChan 协程定期上报 channel 积压长度。
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

	// 预编译按 f_id 区间查询的语句，供 validate 包回放校验使用
	p.selectPrepare = fmt.Sprintf("SELECT f_id, mr FROM t_exchange_match_result_%s WHERE "+
		"f_id >=? AND f_id <=? ", p.symbol)
	p.selectStmt, err = p.DB.Prepare(p.selectPrepare)
	if err != nil {
		log.Println("select prepare error", err, DataSourceName)

		common.Fatal("select prepare error:", err)
	}

	for i := 0; i < config.GetInt("persistence.conn-num", 10); i++ {
		p.goPersistence()
	}
	p.goBatch()
	p.GaugePersistChan()
	//p.GetSymbolPrecision()
}

// PersistMR 把一条成交结果 JSON 字节写入全局 mrChan（当前未使用，各交易对走自己的 channel）。
func PersistMR(bytes []byte) {
	mrChan <- bytes
}

// goBatch 启动攒批协程：
// 从 mrChan 读一条结果后，立刻把当前 channel 中积压的结果一并取出，
// 凑成一个批次（最大 batch-size 条）写入 mrBatchChan。
// 这样可以在流量高峰时自动攒大批次，低峰时及时落库，兼顾吞吐与时延。
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

// goPersistence 启动一个批量写入协程，
// 不断从 mrBatchChan 消费批次并执行 INSERT，协程数量由 persistence.conn-num 控制。
func (p *Persisten) goPersistence() {
	go func() {
		for {
			p.insertData(<-p.mrBatchChan)
		}
	}()
}

// insertData 把一个批次的成交结果生成 SQL 并执行写入，
// 写入成功后更新统计计数；失败只记日志不中断，保证后续批次继续写入。
func (p *Persisten) insertData(datas [][]byte) {
	sqlStr := p.createSql(datas)
	result, err := p.DB.Exec(*sqlStr)
	if err != nil {
		common.Error("exe sql err:", err, result)
	}
	statistics.IncrPersistenceNum(len(datas))
}

// createSql 把一个批次的成交结果 JSON 字节拼接成一条批量 INSERT SQL。
// 流程：先反序列化每条结果，按 f_id 升序插入排序（保证主键有序，减少 InnoDB 页分裂），
// 再逐条拼接 VALUES，最后去掉末尾多余逗号。
// 使用 insert ignore 避免主键冲突时整批失败。
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

		// 转义反斜杠，防止 JSON 中的特殊字符破坏 SQL 语法
		data = strings.Replace(data, "\\", "\\\\", -1)
		sqlStr += data
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	return &sqlStr
}

// GaugePersistChan 启动一个协程，每 10 分钟打印一次两级 channel 的积压长度，
// 用于监控持久化流水线是否存在堆积。
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

// Head 是批量 INSERT 语句的前缀模板，%s 处填入交易对符号。
// insert ignore：主键冲突时跳过该条而不是整批报错。
var Head = "insert ignore t_exchange_match_result_%s (f_id, symbol, ts, order_type, role, mr, extra) values "

// createSql 是包级版本的 SQL 拼接函数（未按交易对替换表名，当前主要供测试使用）。
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
			cast.ToString(sd.mr.Ts) + "','" +
			getOrderDirection(sd.mr) + "','" +
			role + "','" +
			string(datas[sd.index]) + "','" +
			extra + "'),"
		sqlStr += data
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	return &sqlStr
}

// insertSortMr 把一条 sortData 按 f_id 升序插入到已排序切片中的正确位置。
// 使用插入排序是因为批次内数据量不大（batch-size 条），且大部分数据本身近似有序。
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

// GetRole 根据成交结果推断订单角色：
//   - submit-cancel / submit-batch-cancel 分别对应 cancel / batch-cancel；
//   - Items 中包含 maker 角色说明该单在撮合中作为挂单被成交，返回 maker；
//   - 否则为 taker（主动吃单）。
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

// getOrderDirection 根据订单类型字符串推断买卖方向，
// 撤单类型返回 cancel，无法识别时打告警日志并返回空字符串。
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

// GetMatchResult 按 f_id 区间 [fromId, toId] 从 MySQL 查询历史成交结果，
// 返回 map[f_id]mr_json，供 validate 包回放订单簿时逐条比对。
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

// GetExtra 为批量撤单（batch-cancel）类型的成交结果生成 extra 字段内容：
// 把每个被撤订单的 ID 和是否成功撤掉的状态序列化为 JSON 数组，
// 方便下游查询哪些订单被实际取消。
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
