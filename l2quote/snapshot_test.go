// Package l2quote 本文件为 l2quote 包的单元测试文件。
package l2quote

import (
	"log"
	"market-match/common"
	"testing"
)

// TestL2quote_Init 一个占位性质的测试：验证对空 map 取不存在的 key 时返回零值这一 Go 语言行为。
func TestL2quote_Init(t *testing.T) {
	common.HmType2Step = make(map[int]int)
	x := 1 - common.HmType2Step[1]
	log.Println("xxxx:", x, common.HmType2Step[1])
}
