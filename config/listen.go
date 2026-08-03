// Package config 是撮合引擎的配置中心包，负责配置的读取、缓存与热更新。
package config

import (
	"github.com/apolloconfig/agollo/v4/storage"
)

// CustomChangeListener 是 Apollo 配置变更事件的自定义监听器，
// 实现 agollo 的 storage.ChangeListener 相关接口。
// 当 Apollo 配置中心的配置发生变更（热更新）时，agollo 会回调 OnChange 方法，
// 业务方可在此实现配置刷新后的联动逻辑（如重建连接、更新缓存等）。
type CustomChangeListener struct {
}

// OnChange 是 Apollo 配置变更的回调入口，当前为空实现（预留扩展点）。
// 接入 Apollo 热更新时，在此编写配置变更后的处理逻辑。
func (c *CustomChangeListener) OnChange(changeEvent *storage.ChangeListener) {
	//write your code here
}
