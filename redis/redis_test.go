package redis

import (
	"github.com/go-redis/redis"
	"log"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
}

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

func ExampleRedisConnect() {


}
