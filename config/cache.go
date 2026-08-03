// Package config 是撮合引擎的配置中心包，负责配置的读取、缓存与热更新。
package config

import (
	"errors"
	"github.com/apolloconfig/agollo/v4/agcache"
	"github.com/spf13/cast"
	"sync"
)

// DefaultCache 是基于 sync.Map 的本地内存缓存实现，
// 实现了 Apollo agollo 客户端要求的 agcache.CacheInterface 接口，
// 用于在接入 Apollo 时作为其本地配置缓存层（替代 agollo 默认的缓存实现）。
// 读写均为并发安全，expireSeconds 参数当前被忽略（不支持过期淘汰）。
type DefaultCache struct {
	defaultCache sync.Map
}

// Set 将 key-value 写入缓存，expireSeconds 参数当前未生效（永不过期）。
//Set 获取缓存

func (d *DefaultCache) Set(key string, value interface{}, expireSeconds int) (err error) {
	d.defaultCache.Store(key, value)
	return nil
}

// EntryCount 返回当前缓存中的条目总数。
//EntryCount 获取实体数量
func (d *DefaultCache) EntryCount() (entryCount int64) {

	count := int64(0)
	d.defaultCache.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	return count

}

// Get 按 key 读取缓存值，并将结果断言为 []byte 返回（Apollo 配置内容的原始格式）。
// key 不存在时返回错误。
//Get 获取缓存
func (d *DefaultCache) Get(key string) (value interface{}, err error) {
	v, ok := d.defaultCache.Load(key)

	if !ok {
		return nil, errors.New("load default cache fail")
	}
	return v.([]byte), nil
}

// GetInt 按 key 读取缓存并转为 int，key 不存在时返回默认值 def 及错误。
func (d *DefaultCache) GetInt(key string, def int) (value int, err error) {
	v, ok := d.defaultCache.Load(key)
	if !ok {
		return def, errors.New("load default cache fail")
	}
	return cast.ToInt(v), nil
}

// GetString 按 key 读取缓存并转为 string，key 不存在时返回默认值 def 及错误。
func (d *DefaultCache) GetString(key string, def string) (value string, err error) {
	v, ok := d.defaultCache.Load(key)
	if !ok {
		return def, errors.New("load default cache fail")
	}
	return cast.ToString(v), nil
}

// GetBool 按 key 读取缓存并转为 bool，key 不存在时返回默认值 def 及错误。
func (d *DefaultCache) GetBool(key string, def bool) (value bool, err error) {
	v, ok := d.defaultCache.Load(key)
	if !ok {
		return def, errors.New("load default cache fail")
	}
	return cast.ToBool(v), nil
}

// GetSliceString 按 key 读取缓存并转为 []string，key 不存在时返回默认值 def 及错误。
func (d *DefaultCache) GetSliceString(key string, def []string) (value []string, err error) {
	v, ok := d.defaultCache.Load(key)
	if !ok {
		return def, errors.New("load default cache fail")
	}
	return cast.ToStringSlice(v), nil
}

// Range 遍历缓存中的所有 key-value 对，f 返回 false 时提前终止遍历。
//Range 遍历缓存

func (d *DefaultCache) Range(f func(key, value interface{}) bool) {

	d.defaultCache.Range(f)

}

// Del 删除指定 key 的缓存条目，固定返回 true。
//Del 删除缓存

func (d *DefaultCache) Del(key string) (affected bool) {
	d.defaultCache.Delete(key)
	return true

}

// Clear 清空缓存中的所有条目（通过整体替换 sync.Map 实现）。
//Clear 清除所有缓存

func (d *DefaultCache) Clear() {
	d.defaultCache = sync.Map{}

}

// DefaultCacheFactory 是 DefaultCache 的工厂类，
// 实现 agollo 的 agcache.FactoryInterface，用于向 Apollo 客户端注入自定义缓存实现。
//DefaultCacheFactory 构造默认缓存组件工厂类

type DefaultCacheFactory struct {
}

// Create 创建并返回一个新的 DefaultCache 实例。
//Create 创建默认缓存组件
func (d *DefaultCacheFactory) Create() agcache.CacheInterface {
	return &DefaultCache{}
}
