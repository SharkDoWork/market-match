// Package validate 负责订单簿（OrderBook）正确性校验。
// 核心思路：加载最近两次订单簿快照，从较早的快照（baseBook）出发，
// 按顺序回放 MySQL 中持久化的成交结果（MatchResult），逐条重新撮合，
// 最终得到的订单簿应与较晚的快照（lastBook）完全一致。
// 同时每条回放的撮合结果还会与 MySQL 中存储的原始结果逐字段比对，
// 任何不一致都会 Fatal 退出，确保撮合引擎的确定性。
package validate

import (
	jsoniter "github.com/json-iterator/go"

	"market-match/common"
	"market-match/config"
	"market-match/l2quote"
	"market-match/match"
	"market-match/persistence"
	"market-match/puller"
	"market-match/snapshotter"
)

// json 使用 jsoniter 的兼容模式，兼顾性能与标准库行为一致。
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// ValidateOrderbook 对配置中的所有交易对逐一执行订单簿回放校验。
// 校验流程（每个交易对）：
//  1. 加载最新快照 lastBook 和次新快照 baseBook（若无次新快照则从空簿开始）；
//  2. 通过 puller 从消息源拉取 baseBook.FromId+1 之后的订单序列；
//  3. 从 MySQL 查询同一区间的持久化成交结果；
//  4. 启动 CheckMatcher 协程逐条回放并比对；
//  5. 所有交易对校验完成后返回 true。
func ValidateOrderbook() bool {
	mapSymbols := make(map[string]bool)
	checkCh := make(chan string, len(config.GetStringSlice("symbols", []string{})))

	for _, symbol := range config.GetStringSlice("symbols", []string{}) {
		if snapshotter.HaveSnapshot(symbol) == false {
			common.Fatal("x, if first time touch 0."+snapshotter.EndWith(symbol), " first")
		}
		ids, cType := snapshotter.GetSnapshotIds(symbol)
		if len(ids) <= 0 {
			continue //first order -> end
		}
		common.Info("ValidateOrderbook ids:", ids, "cType:", cType)

		// 检查订单簿快照与 l2quote 行情快照的 ID 是否衔接，
		// 若订单簿快照 ID 大于行情快照最大 ID，说明行情数据可能有缺失，需人工介入
		l2quoteMaxMRID := l2quote.GetLargestMRID(config.GetString("l2quote.snapshot.path", "./sp/"), symbol)

		if ids[0] > l2quoteMaxMRID {
			common.Warnf(symbol, "l2quote snapshot match result id ", ids[0], " smaller than exchange match result id ", l2quoteMaxMRID, "need handle it by manual")
		}

		mapSymbols[symbol] = false
		var baseBook *match.OrderBook
		var lastBook *match.OrderBook
		var err error
		lastBook, err = snapshotter.Load(symbol, cType[0], ids[0])
		if err != nil {
			common.Fatal("load last book error:", err, symbol, ids[0])
		}

		ch := make(chan *match.Order, 5000)
		if len(ids) >= 2 {
			baseBook, err = snapshotter.Load(symbol, cType[1], ids[1])
			if err != nil {
				common.Fatal("load error ", err)
			}
		} else {
			// 只有一次快照时，从空订单簿开始回放全部历史
			baseBook = match.InitOrderBook(0, symbol)
		}
		common.Info("start to check symbol:", symbol)
		pull, ok := puller.DbInfoList[symbol]
		if !ok {
			common.Fatal("validate book error pull not init:", symbol)
		}
		pull.GoPuller(ch, baseBook.FromId+1)
		//puller.Init(ch, symbol, baseBook.FromId+1)
		//resultMap := persistence.GetMatchResult(baseBook.FromId+1, lastBook.FromId, baseBook.Symbol)
		per, ok := persistence.DbPersistenList[symbol]
		if !ok {
			common.Fatal("validate book error persistence not init:", symbol)
		}
		resultMap := per.GetMatchResult(baseBook.FromId+1, lastBook.FromId)
		CheckMatcher(lastBook, baseBook, ch, checkCh, resultMap)
	}

	// 等待所有交易对校验完成，checkCh 每收到一个 symbol 说明该交易对通过
	for {
		if len(mapSymbols) == 0 {
			common.Info("all symbols check finished !")
			return true
		}
		select {
		case symbol := <-checkCh:
			common.Info("check finished symbol:", symbol)
			delete(mapSymbols, symbol)
		}
	}
}

// CheckMatcher 启动一个协程，从 orderSeqChan 逐条读取订单，
// 在 baseBook 上重新执行撮合，并把每次撮合结果与 MySQL 中的原始记录比对。
// 当 baseBook 的 FromId 追上 lastBook 时，比较两个订单簿是否完全一致：
// 一致则向 checkCh 发送 symbol 表示通过，不一致则 Fatal 退出。
func CheckMatcher(lastBook *match.OrderBook, baseBook *match.OrderBook, orderSeqChan chan *match.Order,
	checkCh chan string, resultMap map[int64]string) {
	go func() {
		for {
			order := <-orderSeqChan
			matchResult := &(baseBook.GenMatchResult(order).Mr)
			CheckResult(matchResult, resultMap)

			if baseBook.FromId == lastBook.FromId {
				if match.CompareOrderBook(lastBook, baseBook) {
					checkCh <- baseBook.Symbol
				} else {
					common.Fatal("checked orderbook error symbol:",
						baseBook.Symbol, len(lastBook.Cache()), len(baseBook.Cache()),
						lastBook.SellSet.Size(), baseBook.SellSet.Size(),
						lastBook.BuySet.Size(), baseBook.BuySet.Size(), baseBook.FromId)
				}
				break
			}
		}
	}()
}

// CheckResult 把一条回放产生的撮合结果与 MySQL 中存储的原始结果做逐字段比对。
// 可通过配置 mrredis.check-result=false 跳过比对（默认开启）。
// 比对失败（找不到记录、序列化失败、内容不一致）都会 Fatal 退出。
func CheckResult(matchResult *match.MatchResult, resultMap map[int64]string) {

	if config.GetBool("mrredis.check-result", true) != true {
		return
	}
	str, ok := resultMap[matchResult.Id]
	if !ok {
		common.Fatal("persistence get result error:", str, matchResult.Id)

	}

	matchBytes, err := json.Marshal(matchResult)
	if err != nil {
		common.Fatal("check matchresult marshal error:", err, matchResult.Id)
	}

	ok, err = match.ResultEqual(str, string(matchBytes))
	if err != nil {
		common.Fatal("compare error", err)
	}

	if !ok {
		common.Fatal("compare not equal mysql:", str, "match result:", string(matchBytes))
	}
}
