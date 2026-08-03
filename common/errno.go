// Package common 提供撮合引擎的公共基础设施，包括日志系统、配置加载、
// 时间工具、错误码定义、全局通道等通用能力，供其他业务包复用。
package common

// 全局错误码与进程退出状态码定义。
// 注意：这些值通过 os.Exit 直接作为进程退出码使用，修改需与运维监控约定保持一致。
var (
	ErrnoSystemError     = -1  // 通用系统错误
	ErrnoLogInitFailed   = 202 // 日志组件初始化失败时的进程退出码
	ExitStatusInitFailed = 1   // 初始化阶段失败的通用退出状态
)
