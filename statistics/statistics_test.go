package statistics

import (
	"github.com/spf13/cast"
	"log"
	"testing"
	"time"
)

func TestSetMatchTag(t *testing.T) {
	s := cast.ToFloat64(time.Now().UnixNano())/1000
	time.Sleep(time.Second *1)
	s2 := cast.ToFloat64(time.Now().UnixNano())/1000
	log.Print((s2 - s)/1000)
	log.Print(s)
}