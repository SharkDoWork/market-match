package assign

import (
	"github.com/json-iterator/go"
	"market-match/common"
	"market-match/match"
	"market-match/persistence"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

//func Start(symbol string, fromId int64, endId int64) {
//	if !puller.ExistSymbolInDb(symbol) {
//		common.Fatal("symbol:", symbol, " not in db !")
//	}
//	minId := puller.GetMinIdFromDb()
//	if fromId < minId {
//		common.Fatal("check failed start id smaller than db id:", fromId, minId)
//	}
//
//	baseBook, checkBook := snapshotter.GetBaseOrderBookFromS3(symbol, fromId)
//	ch := make(chan *match.Order, 5000)
//	puller.GoPuller(ch, baseBook.Symbol, baseBook.FromId+1)
//	resultMap := persistence.GetMatchResult(fromId, endId, symbol)
//	var matchResult *match.MatchResult
//	ticker := time.NewTicker(time.Second)
//	common.Info("start check matchresult from basebook start from:", baseBook)
//	for {
//		select {
//		case order := <-ch:
//			matchResult = &(baseBook.GenMatchResult(order).Mr)
//			if matchResult.Id >= fromId {
//				common.Info("end to check")
//				goto forEnd
//			}
//			validate.CheckResult(matchResult, resultMap)
//			if checkBook != nil && checkBook.FromId == baseBook.FromId {
//				if !match.CompareOrderBook(baseBook, checkBook) {
//					common.Fatal("check orderbook error fromId", baseBook.FromId)
//				}
//			}
//		case <-ticker.C:
//			common.Info("current check id :", matchResult.Id)
//		}
//	}
//forEnd:
//	statistics.IncrMatchNum()
//	publishChan := match.PublishResultChan(baseBook.Symbol)
//	publish(matchResult, publishChan)
//
//	ticker = time.NewTicker(time.Second)
//	common.Info("start to exchange")
//	for {
//		select {
//		case order := <-ch:
//			matchResult = &(baseBook.GenMatchResult(order).Mr)
//			statistics.IncrMatchNum()
//			if matchResult.Id > endId {
//				common.Info("match finished, ", endId)
//				os.Exit(0)
//			}
//			publish(matchResult, publishChan)
//		case <-ticker.C:
//			common.Info("current match id:", matchResult.Id)
//		}
//	}
//}

func publish(matchResult *match.MatchResult, ch chan []byte) {
	bytesJson, err := json.Marshal(matchResult)
	if err != nil {
		common.Fatal("match encode to json err", err, matchResult)
	}
	persistence.PersistMR(bytesJson)
	ch <- bytesJson
}
