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

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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

// CheckMatcher match and cpmpare orderbook
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
