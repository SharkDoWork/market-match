package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"market-match/common"
	"market-match/config"
)

var KlineClient *redis.Client

func InitClient() {
	InitKlineClient()
}

func InitKlineClient() {
	ctx := context.Background()
	if KlineClient == nil {
		opt := &redis.Options{
			Addr: config.GetStringSlice("redis.address", []string{})[0],
			//Password: config.GetString("redis.password", ""),
			PoolSize: config.GetInt("redis.poolsize", 10),
		}
		KlineClient = redis.NewClient(opt)
	}
	err := KlineClient.Ping(ctx).Err()
	if err != nil {
		common.Fatal("kline redis init error:", err)
	}
}
