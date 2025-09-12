package l2quote

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"market-match/common"
)

/*
根据redis cache map保存kline到redis中

有redis操作错误不清空cache，下次ticker触发redis操作时再次尝试写入
*/
func (L *L2quote) saveKlinesToRedis() error {
	//t1 := time.Now().UnixNano()/1e6
	// 获取redis中保存的maxMRID
	maxMRIDRedis, err := L.getMaxMRIDFromRedis()
	if err != nil {
		common.Warn(L.symbol, "get maxMRID from redis failed, err :", err)
		return err
	}

	// 当redis中maxMRID比内存大的时候跳过redis update
	if maxMRIDRedis > L.quotation.MaxMRId {
		for i := range KLINETYPES {
			L.redisCache[KLINE_NAME_TYPE_MAP[KLINETYPES[i]]] = make(map[int64]int)
		}
		return nil
	}

	pipe := L.redisClient.Pipeline()
	for klineType, tsMap := range L.redisCache {
		for ts := range tsMap {
			k, ok := L.quotation.Klines[KLINE_TYPE_TO_NAME_MAP[klineType]].Get(ts)
			if !ok {
				common.Fatal(fmt.Sprintf("redis update %s get %s kline failed", L.symbol, klineType))
			}
			key, offset, value := L.klineToRedisFormat(k.(*kline), KLINE_TYPE_TO_NAME_MAP[klineType])
			common.Trace(fmt.Sprintf("%s redis update key %s, offset %d, value %s\n", L.symbol, key, offset, value))
			pipe.LSet(L.ctx, key, offset, value)
		}

	}
	pipe.Set(L.ctx, genRedisMaxMRIDKey(L.symbol), L.quotation.MaxMRId, 0)
	cmds, err := pipe.Exec(L.ctx)
	if err != nil {
		// redis暂时写失败不会中断程序，保留cache下次继续尝试写入
		common.Warn(L.symbol, "redis ops error : ", cmds)
		//dogstatsd.Event("l2quote redis update failed", "l2quote redis update failed "+L.symbol+" : "+fmt.Sprintln(cmds), statsd.Error)
		return err
	}

	/*res := L.redisClient.Set(L.ctx, genRedisMaxMRIDKey(L.symbol), L.quotation.MaxMRId, 0)
	if res.Err() != nil {
		common.Warn(L.symbol, "set maxMRID to redis failed :", res.Err())
		return res.Err()
	}
	*/

	for i := range KLINETYPES {
		L.redisCache[KLINE_NAME_TYPE_MAP[KLINETYPES[i]]] = make(map[int64]int)
	}
	//common.Warn("save all candles cost :", time.Now().UnixNano()/1e6 - t1)
	return nil
}

/*
把kline格式化成redis需要的格式，并返回所在list的offset
*/
func (L *L2quote) klineToRedisFormat(k *kline, klineType string) (string, int64, string) {
	msg := fmt.Sprintf("%d,%s,%s,%s,%s,%s,%s,%d",
		k.TS,
		k.OpenPrice.String(),
		k.ClosePrice.String(),
		k.LowPrice.String(),
		k.HighPrice.String(),
		k.Vol.String(),
		k.TurnOver.String(),
		k.Count)

	tableTS, offset, _ := common.GetListTsOffLen(common.HmName2Type[klineType], int(k.TS), 0)

	key := fmt.Sprintf("market.%s.kline.%s.%d", L.symbol, klineType, tableTS)

	return key, int64(offset), msg
}

func (L *L2quote) getMaxMRIDFromRedis() (int64, error) {
	maxMRID, err := L.redisClient.Get(L.ctx, genRedisMaxMRIDKey(L.symbol)).Int64()

	if err != nil {
		// 没有设置该值时返回0
		if err == redis.Nil {
			return 0, nil
		}
		// 其他情况返回报错
		return 0, err
	}

	return maxMRID, nil
}

func genRedisMaxMRIDKey(symbol string) string {
	return fmt.Sprintf("market.%s.maxMRID", symbol)
}
