// Package common 提供撮合引擎的公共基础设施，包括日志系统、配置加载、
// 时间工具、错误码定义、全局通道等通用能力，供其他业务包复用。
package common

import (
	"github.com/spf13/cast"
	"log"
	"testing"
)

// TestLoadConfig 配置加载的占位测试，当前为空实现。
func TestLoadConfig(t *testing.T) {
}

// TestLogInit 手动验证各级别日志输出是否正常。
// 注意：运行前需先调用 ZapInit，否则 errorLogger 为 nil 会 panic。
func TestLogInit(t *testing.T) {
	Trace("trace=================")
	Debug("debug=================")
	Info("info=================")
	Warn("warn=================")
	Error("error=================")
	//  Fatal("fatal=================")
	// ("warn=================")
}

// TestTimeNowMs 验证 TimeNowHour 的输出格式，以及大数值 float64 转字符串的行为。
func TestTimeNowMs(t *testing.T) {
	log.Println(TimeNowHour())
	w := float64(2222222222221133)
	log.Println(cast.ToString(w))
}
