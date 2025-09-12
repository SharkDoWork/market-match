package l2quote

import (
	"log"
	"market-match/common"
	"testing"
)

func TestL2quote_Init(t *testing.T) {
	common.HmType2Step = make(map[int]int)
	x := 1 - common.HmType2Step[1]
	log.Println("xxxx:", x, common.HmType2Step[1])
}
