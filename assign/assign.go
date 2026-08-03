// Package assign 提供订单簿"校验+重放"的离线工具逻辑。
//
// 主要用途（见被注释的 Start 函数）：从某个快照订单簿出发，重新拉取并撮合
// [fromId, endId] 区间的订单，将重放得到的撮合结果与线上持久化的历史结果
// 逐一比对（validate.CheckResult），并在位点一致时校验订单簿内容是否吻合，
// 用于排查撮合正确性问题或做数据修复。校验通过后继续撮合至 endId 并发布结果。
//
// 当前 Start 被整体注释停用，包内仅 publish 一个可用函数。
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

// publish 将撮合结果序列化为 JSON，先写入持久化模块（persistence.PersistMR），
// 再发送到指定的发布通道（下游为 RabbitMQ 撮合结果发布）。
func publish(matchResult *match.MatchResult, ch chan []byte) {
	bytesJson, err := json.Marshal(matchResult)
	if err != nil {
		common.Fatal("match encode to json err", err, matchResult)
	}
	persistence.PersistMR(bytesJson)
	ch <- bytesJson
}
