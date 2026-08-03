// Package config 是撮合引擎的配置中心包，负责配置的读取、缓存与热更新。
//
// 核心能力：
//  1. 基于 viper 的统一配置读取接口（GetInt/GetString/GetBool 等），
//     支持默认值兜底，配置缺失时返回调用方指定的默认值；
//  2. 预留 Apollo 配置中心接入（当前相关代码已注释，使用 viper 本地配置为主）；
//  3. 提供 DefaultCache（基于 sync.Map）作为 Apollo agcache 的本地缓存实现；
//  4. 提供 CustomChangeListener 用于监听 Apollo 配置变更事件（热更新钩子）。
package config

import (
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"time"
)

// Cache 是全局配置缓存实例，当前直接复用 viper 的全局单例。
// 历史上曾计划接入 Apollo 的 agcache.CacheInterface（见下方注释代码），
// 现阶段统一通过 viper 读取配置。
//var Cache agcache.CacheInterface
var Cache *viper.Viper

//func LoadConfig(host, appid string, namespace string) error {
//	c := &config.AppConfig{
//		AppID:          appid,
//		Cluster:        "dev",
//		IP:             host,
//		NamespaceName:  namespace + ".yaml",
//		IsBackupConfig: true,
//		Secret:         "6ce3ff7e96a24335a9634fe9abca6d51",
//	}
//	client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
//		return c, nil
//	})
//	if err != nil {
//		return err
//	}
//	Cache = client.GetConfigCache(c.NamespaceName)
//	color.Blue(client.GetConfig(c.NamespaceName).GetContent())
//	return nil
//}

// GetInt 读取 int 类型配置，key 不存在时返回默认值 def。
func GetInt(key string, def int) (value int) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToInt(get)
}

// GetString 读取 string 类型配置，key 不存在时返回默认值 def。
func GetString(key string, def string) (value string) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToString(get)
}

// GetBool 读取 bool 类型配置，key 不存在时返回默认值 def。
func GetBool(key string, def bool) (value bool) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToBool(get)
}

// GetStringSlice 读取 []string 类型配置，key 不存在时返回默认值 def。
func GetStringSlice(key string, def []string) (value []string) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToStringSlice(get)
}

// GetStringOrErr 预留接口：读取配置并在缺失时返回错误，当前为空实现。
func GetStringOrErr() {

}

// GetInt64 读取 int64 类型配置，key 不存在时返回默认值 def。
func GetInt64(key string, def int64) (value int64) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToInt64(get)
}

// GetStringMap 读取 map[string]interface{} 类型配置，key 不存在时返回 nil。
func GetStringMap(key string) map[string]interface{} {
	get := viper.Get(key)
	if get == nil {
		return nil
	}
	return cast.ToStringMap(get)
}

// GetDuration 读取 time.Duration 类型配置，key 不存在时返回默认值 def。
// 配置值支持 "5s"、"1m30s" 等 Go duration 字符串格式。
func GetDuration(key string, def time.Duration) (v time.Duration) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToDuration(get)
}
