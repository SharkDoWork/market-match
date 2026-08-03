// Package common 提供撮合引擎的公共基础设施，包括日志系统、配置加载、
// 时间工具、错误码定义、全局通道等通用能力，供其他业务包复用。
package common

// L2ChanMap 是全局的 L2 行情数据通道映射表，
// key 为交易对（symbol），value 为承载序列化后 L2 行情数据的字节通道。
// 上游行情模块向通道写入数据，下游推送模块从通道读取并广播。
var L2ChanMap map[string]chan []byte
