// Package common 提供撮合引擎的公共基础设施，包括日志系统、配置加载、
// 时间工具、错误码定义、全局通道等通用能力，供其他业务包复用。
package common

import "github.com/caarlos0/env/v6"

// osenv 封装了进程启动时必须从环境变量读取的配置项，
// 主要用于 Apollo 配置中心的连接参数。
type osenv struct {
	ApolloHost string `env:"APOLLO_HOST,notEmpty"`                 // Apollo 配置中心地址，必填，缺失会导致解析失败
	Appid      string `env:"APP_ID" envDefault:"contract-match"`   // Apollo 应用 ID，默认 contract-match
	Namespace  string `env:"APOLLO_NAMESPACE" envDefault:"application"` // Apollo 命名空间，默认 application
}

// InitEnv 解析进程环境变量并填充 osenv 结构体。
// 若必填环境变量（如 APOLLO_HOST）缺失，会返回错误，
// 调用方通常在 main 函数启动早期执行，失败即终止启动。
func InitEnv() (*osenv, error) {
	e := osenv{}
	if err := env.Parse(&e); err != nil {
		return &e, err
	}
	return &e, nil
}
