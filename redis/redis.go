// Package redis 封装 Redis 客户端的初始化与管理。
// 目前主要用于 K 线（kline）行情数据的缓存读写，
// 通过全局单例 KlineClient 向其他模块提供 Redis 访问能力。
package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"market-match/common"
	"market-match/config"
)

// KlineClient 是 K 线数据专用的 Redis 客户端全局单例，
// 由 InitKlineClient 初始化，其他包通过它读写 kline 缓存。
var KlineClient *redis.Client

// InitClient 初始化本包管理的所有 Redis 客户端（目前只有 KlineClient）。
func InitClient() {
	InitKlineClient()
}

// InitKlineClient 初始化 K 线 Redis 客户端：
// 从配置读取地址和连接池大小创建客户端，并通过 Ping 验证连通性，
// 连接失败时直接 Fatal 退出，因为 kline 缓存是行情服务的关键依赖。
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
