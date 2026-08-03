package statistics

// 本文件为 statistics 包的简单验证测试。

import (
	"github.com/spf13/cast"
	"log"
	"testing"
	"time"
)

// TestSetMatchTag 验证 UnixNano 转毫秒时间差的计算方式（耗时统计的时间换算逻辑）。
func TestSetMatchTag(t *testing.T) {
	s := cast.ToFloat64(time.Now().UnixNano())/1000
	time.Sleep(time.Second *1)
	s2 := cast.ToFloat64(time.Now().UnixNano())/1000
	log.Print((s2 - s)/1000)
	log.Print(s)
}