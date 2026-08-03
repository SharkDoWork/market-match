// Package common 提供撮合引擎的公共基础设施，包括日志系统、配置加载、
// 时间工具、错误码定义、全局通道等通用能力，供其他业务包复用。
package common

import (
	"fmt"
	"market-match/config"
	"math"

	"os"
	"runtime"
	"time"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

// 全局默认常量：配置文件路径、日志路径、各类队列/缓冲区大小、行情推送间隔等。
// 这些值在 validate() 中通过 viper.SetDefault 注册为兜底默认值，
// 当配置文件未覆盖对应 key 时生效。
const (
	// default config
	AppProfile                            = "dev"
	AppName                               = "market-match"
	ConfFile                              = "./conf/config.yaml"
	LogFile                               = "./log/market-match.log"
	DefaultLogLevel                       = "trace" //when start up trace and debug log will print
	ExchangeL2quoteSize                   = 1500
	ExchangeTradeSize                     = 5000
	ExchangeDepthSize                     = 1000
	MarketMinDepthUpdateIntervalMs        = 100
	MarketMinStackedDepthUpdateIntervalMs = 1000
	MarketDefaultUpdateIntervalMs         = 1000
	ENV_CONFFILE                          = "CONFIG_FILE" // 环境变量名：指定外部配置文件路径
)

// DepthStep 描述深度行情（order book）某一档位的聚合规则。
// Name 为档位标识（如 "0","1",...），Accuracy 为该档位的价格聚合精度，
// Capacity 为该档位在内存中保留的最大档数。
type DepthStep struct {
	Name     string
	Accuracy float64
	Capacity int64
}

// SymbolInfo 是一个交易对（symbol）的完整静态配置，
// 包含价格/数量精度、L2 行情精度以及深度档位聚合规则。
//type for symbol-info
type SymbolInfo struct {
	Symbol                 string
	AmountScale            int32       // 数量精度（小数位数）
	PriceScale             int32       // 价格精度（小数位数）
	L2QuotePriceScale      int64       // L2 行情推送使用的价格精度
	DepthSteps             []DepthStep //[0]int step, [1]float64 accuracy, [2]int step-amount // 合并后的深度档位
	UncombinedDepthSteps   []DepthStep //[0]int step, [1]float64 accuracy, [2]int step-amount // 未合并的原始深度档位
	Depth10PercentCapacity int64       // 深度 10% 范围内的容量上限
}

// 包级全局变量，由 LoadConfigViper -> validate 在启动时初始化，
// 之后全进程只读共享。
var (
	//Conf *Config
	SymbolInfos map[string]*SymbolInfo // 所有交易对的静态配置表
	Location    *time.Location         // 全局时区（配置 location 指定，默认 Asia/Shanghai）
	ContUsdMap  map[string]int64       // 合约面值映射（symbol -> 面值，单位 USD）
	ServerType  string                 // 服务类型标识（由 app.server-type 配置决定）
)

// LoadConfigViper 是配置加载的入口函数，在进程启动时调用。
//
// 加载顺序：
//  1. 优先使用环境变量 CONFIG_FILE 指定的配置文件；
//  2. 否则回退到默认路径 ./conf/config.yaml；
//  3. 同时绑定环境变量 CAPTAIN_SEQ 到配置 key app.seq；
//  4. 读取配置文件后执行 validate()，完成交易对配置解析与全局默认值注册。
//
// 任一环节失败都会返回 error，调用方应终止启动。
func LoadConfigViper() error {
	envConf, exist := os.LookupEnv(ENV_CONFFILE)
	if exist && envConf != "" {
		viper.SetConfigFile(envConf)
	} else {
		viper.SetConfigFile(ConfFile)
	}

	viper.BindEnv("app.seq", "CAPTAIN_SEQ")

	err := viper.ReadInConfig()
	fmt.Printf("get envConf:%#v;exist:%#v;ConfFile:%#v; err:%#v\n", envConf, exist, ConfFile, err)
	if err != nil {
		return err
	}
	if err = validate(); err != nil {
		return err
	}
	return nil
}

// loadSymbolInfoConf 从配置中心读取 symbol-info 节，解析为 map[symbol]*SymbolInfo。
//
// 解析逻辑分两轮：
//  1. 第一轮遍历配置中显式定义的所有 symbol 条目，要求 amount-scale、price-scale、
//     depth-10percent-capacity、l2quote-price-scale、depth-steps 五个字段齐全，
//     并解析 depth-steps / uncombined-depth-steps 下的档位规则（accuracy 不允许为 0）。
//  2. 第二轮处理 symbols 参数中引用模板的情况：若某 symbol 在配置中的值是字符串，
//     则将其视为指向另一个已定义 symbol 的"模板引用"，复制模板的精度与档位配置。
//
// 返回的 map 以 symbol 名为 key，供全局变量 SymbolInfos 使用。
func loadSymbolInfoConf(symbols []string) (map[string]*SymbolInfo, error) {
	var symbolInfoMap map[string]*SymbolInfo
	symbolInfoMap = make(map[string]*SymbolInfo)

	symbolInfosConf, err := cast.ToStringMapE(config.GetStringMap("symbol-info"))
	if err != nil {
		return nil, err
	}

	// 拼装map
	//symbolInfosConf := getSymbolConfig()

	// for all symbols
	for symbol, info := range symbolInfosConf {
		symbolName, err := cast.ToStringE(symbol)
		if err != nil {
			return nil, err
		}

		if s, err := cast.ToStringMapE(info); err == nil &&
			s["amount-scale"] != nil &&
			s["price-scale"] != nil &&
			s["depth-10percent-capacity"] != nil &&
			s["l2quote-price-scale"] != nil &&
			s["depth-steps"] != nil {
			amountScale, err := cast.ToInt32E(s["amount-scale"])
			if err != nil {
				return nil, err
			}
			priceScale, err := cast.ToInt32E(s["price-scale"])
			if err != nil {
				return nil, err
			}
			l2QuotePriceScale, err := cast.ToInt64E(s["l2quote-price-scale"])
			if err != nil {
				return nil, err
			}

			dept10PercentCapacity, err := cast.ToInt64E(s["depth-10percent-capacity"])
			if err != nil {
				return nil, err
			}

			depthStepsConf, err := cast.ToStringMapE(s["depth-steps"])

			var depthSteps []DepthStep

			for step, stepConf := range depthStepsConf {
				v, err := cast.ToSliceE(stepConf)
				if err != nil {
					return nil, err
				}

				accuracy, err := cast.ToFloat64E(v[0])
				if err != nil {
					return nil, err
				}

				if accuracy == 0 {
					return nil, fmt.Errorf("accuracy must not 0")
				}

				capacity, err := cast.ToInt64E(v[1])
				if err != nil {
					return nil, err
				}

				depthSteps = append(depthSteps, DepthStep{Name: step, Accuracy: accuracy, Capacity: capacity})
			}

			uncombinedDepthStepsConf, err := cast.ToStringMapE(s["uncombined-depth-steps"])

			var uncombinedDepthSteps []DepthStep

			for step, stepConf := range uncombinedDepthStepsConf {
				v, err := cast.ToSliceE(stepConf)
				if err != nil {
					return nil, err
				}

				accuracy, err := cast.ToFloat64E(v[0])
				if err != nil {
					return nil, err
				}

				if accuracy == 0 {
					return nil, fmt.Errorf("accuracy must not 0")
				}

				capacity, err := cast.ToInt64E(v[1])
				if err != nil {
					return nil, err
				}

				uncombinedDepthSteps = append(uncombinedDepthSteps, DepthStep{Name: step, Accuracy: accuracy, Capacity: capacity})
			}

			symbolInfo := SymbolInfo{Symbol: symbolName,
				AmountScale:            amountScale,
				PriceScale:             priceScale,
				L2QuotePriceScale:      l2QuotePriceScale,
				Depth10PercentCapacity: dept10PercentCapacity,
				DepthSteps:             depthSteps,
				UncombinedDepthSteps:   uncombinedDepthSteps,
			}
			symbolInfoMap[symbol] = &symbolInfo
		}
	}

	for _, symbol := range symbols {
		if s, err := cast.ToStringE(symbolInfosConf[symbol]); err == nil {
			if symbolInfoTemplate, ok := symbolInfoMap[s]; ok {
				symbolInfo := SymbolInfo{Symbol: symbol,
					AmountScale:       symbolInfoTemplate.AmountScale,
					PriceScale:        symbolInfoTemplate.PriceScale,
					L2QuotePriceScale: symbolInfoTemplate.L2QuotePriceScale,
					DepthSteps:        symbolInfoTemplate.DepthSteps}
				symbolInfoMap[symbol] = &symbolInfo
			} else {
			}
		}
	}

	return symbolInfoMap, nil
}

// validate 在配置文件读取成功后执行，完成两项工作：
//  1. 解析 symbol-info 配置，初始化全局 SymbolInfos 表；
//  2. 加载全局时区 Location，并为各模块注册 viper 默认值（兜底配置）。
//
// 默认值注册的意义：当配置文件未显式覆盖某个 key 时，
// 程序仍能使用合理的默认值运行，降低配置缺失导致的启动失败风险。
func validate() error {
	var err error
	SymbolInfos, err = loadSymbolInfoConf(config.GetStringSlice("symbols", []string{}))
	if err != nil {
		return err
	}

	location, err := time.LoadLocation(config.GetString("location", "Asia/Shanghai"))
	if err != nil {
		return err
	}
	Location = location

	viper.SetDefault("redis.poolsize", 10*runtime.NumCPU())
	viper.SetDefault("app.profile", AppProfile)
	viper.SetDefault("exchange.l2quote.size", ExchangeL2quoteSize)
	viper.SetDefault("exchange.trade.size", ExchangeTradeSize)
	viper.SetDefault("exchange.depth.size", ExchangeDepthSize)
	viper.SetDefault("rabbitmq.compressed", true)
	viper.SetDefault("mrredis.check-result", true)
	viper.SetDefault("log.level", "debug")
	viper.SetDefault("market.min-depth-update-interval-ms", MarketMinDepthUpdateIntervalMs)
	viper.SetDefault("market.min-stacked-depth-update-interval-ms", MarketMinStackedDepthUpdateIntervalMs)
	viper.SetDefault("market.default-update-interval-ms", MarketDefaultUpdateIntervalMs)
	viper.SetDefault("snapshot.n-history", 10)
	viper.SetDefault("aws.s3.enable", true)
	viper.SetDefault("aws.s3.upload-timeout-second", 5)
	viper.SetDefault("batch_result", 30)
	viper.SetDefault("app.name", AppName)
	ServerType = viper.GetString("app.server-type")
	viper.SetDefault("persistence.conn-num", 3*runtime.NumCPU())

	return nil
}

// PriceScale 返回指定交易对的价格精度（小数位数）。
func PriceScale(symbol string) int32 {
	return GetSymbolInfo(symbol).PriceScale
}

// AmountScale 返回指定交易对的数量精度（小数位数）。
func AmountScale(symbol string) int32 {
	return GetSymbolInfo(symbol).AmountScale
}

// GetSymbolInfo 返回指定交易对的完整静态配置。
// 若该 symbol 未在配置中定义，则回退到 "default" 条目，
// 保证任何 symbol 都能获得可用的精度与档位配置。
func GetSymbolInfo(symbol string) *SymbolInfo {
	if SymbolInfos[symbol] == nil {
		return SymbolInfos["default"]
	}
	return SymbolInfos[symbol]
}

// SetSymbolDepth 根据给定的价格精度 scale，为指定交易对动态生成 6 档深度聚合规则，
// 并覆盖写入全局 SymbolInfos。档位精度按 10 的幂次递减（第 4 档为 5 倍特殊处理），
// 每档容量固定为 150。该函数通常在运行时需要调整某交易对深度档位时调用。
func SetSymbolDepth(symbol string, scale int) {

	// 限定6档 1-4档 * 10的n次
	depthSteps := []DepthStep{
		{
			Name:     "0",
			Accuracy: 1 / math.Pow(10, float64(scale)),
			Capacity: 150,
		},
		{
			Name:     "1",
			Accuracy: 1 / math.Pow(10, float64(scale-1)),
			Capacity: 150,
		},
		{
			Name:     "2",
			Accuracy: 1 / math.Pow(10, float64(scale-2)),
			Capacity: 150,
		},
		{
			Name:     "3",
			Accuracy: 1 / math.Pow(10, float64(scale-3)),
			Capacity: 150,
		},
		{
			Name:     "4",
			Accuracy: 1 / math.Pow(10, float64(scale-3)) * 5,
			Capacity: 150,
		},
		{
			Name:     "5",
			Accuracy: 1 / math.Pow(10, float64(scale-4)),
			Capacity: 150,
		},
	}
	info := GetSymbolInfo(symbol)
	symbolInfo := SymbolInfo{Symbol: info.Symbol,
		AmountScale:            info.AmountScale,
		PriceScale:             info.PriceScale,
		L2QuotePriceScale:      info.L2QuotePriceScale,
		Depth10PercentCapacity: info.Depth10PercentCapacity,
		DepthSteps:             depthSteps,
		UncombinedDepthSteps:   info.UncombinedDepthSteps,
	}
	symbolInfo.DepthSteps = depthSteps
	SymbolInfos[symbol] = &symbolInfo
}
