// Package redis 测试文件：包含 Redis 客户端连接与写入性能的示例/测试代码。
package redis

import (
	"github.com/go-redis/redis"
	"log"
	"testing"
	"time"
)

// TestNewClient 测试 Redis 客户端创建（当前为空实现，占位）。
func TestNewClient(t *testing.T) {
}

// ExampleClient 演示如何创建 Redis 客户端并连续写入 10000 次，
// 输出总耗时（毫秒），用于粗略评估 Redis 写入性能。
func ExampleClient() {
	opt := &redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	}
	client := redis.NewClient(opt)
	ms := time.Now().UnixNano()
	key := "sdfsdfas"
	value := "{\"key\": \"value\"}"
	for i := 0; i < 10000; i++ {
		client.Set(key, value, 0)
	}
	ms2 := time.Now().UnixNano()

	t := ms2 - ms
	log.Println(t / 1e6)
}

// ExampleRedisConnect 演示 Redis 连接方式（当前为空实现，占位）。
func ExampleRedisConnect() {


}
