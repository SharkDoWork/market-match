package common

import (
	"github.com/spf13/cast"
	"log"
	"testing"
)

func TestLoadConfig(t *testing.T) {
}

func TestLogInit(t *testing.T) {
	Trace("trace=================")
	Debug("debug=================")
	Info("info=================")
	Warn("warn=================")
	Error("error=================")
	//  Fatal("fatal=================")
	// ("warn=================")
}

func TestTimeNowMs(t *testing.T) {
	log.Println(TimeNowHour())
	w := float64(2222222222221133)
	log.Println(cast.ToString(w))
}
