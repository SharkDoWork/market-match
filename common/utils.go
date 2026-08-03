// Package common 提供撮合引擎的公共基础设施，包括日志系统、配置加载、
// 时间工具、错误码定义、全局通道等通用能力，供其他业务包复用。
package common

import (
	"github.com/shopspring/decimal"
	"math/rand"
	"time"
)

// 全局精度常量，用于价格/数量的上下限校验与截断。
// UPPRECISION = 10^6，LOWPRECISION = 10^-6，即撮合引擎统一支持到小数点后 6 位。
var UPPRECISION decimal.Decimal
var LOWPRECISION decimal.Decimal

// init 在包加载时初始化精度常量，并为全局随机数发生器播种，
// 保证每次进程启动后 rand 序列不重复。
func init() {
	UPPRECISION = decimal.New(1, 6)
	LOWPRECISION = decimal.New(1, -6)

	rand.Seed(time.Now().UnixNano())
}
