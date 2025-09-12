package config

import (
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"time"
)

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

func GetInt(key string, def int) (value int) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToInt(get)
}

func GetString(key string, def string) (value string) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToString(get)
}

func GetBool(key string, def bool) (value bool) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToBool(get)
}

func GetStringSlice(key string, def []string) (value []string) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToStringSlice(get)
}

func GetStringOrErr() {

}

func GetInt64(key string, def int64) (value int64) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToInt64(get)
}

func GetStringMap(key string) map[string]interface{} {
	get := viper.Get(key)
	if get == nil {
		return nil
	}
	return cast.ToStringMap(get)
}

func GetDuration(key string, def time.Duration) (v time.Duration) {
	get := viper.Get(key)
	if get == nil {
		return def
	}
	return cast.ToDuration(get)
}
