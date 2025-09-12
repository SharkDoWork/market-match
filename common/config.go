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
	ENV_CONFFILE                          = "CONFIG_FILE"
)

type DepthStep struct {
	Name     string
	Accuracy float64
	Capacity int64
}

//type for symbol-info
type SymbolInfo struct {
	Symbol                 string
	AmountScale            int32
	PriceScale             int32
	L2QuotePriceScale      int64
	DepthSteps             []DepthStep //[0]int step, [1]float64 accuracy, [2]int step-amount
	UncombinedDepthSteps   []DepthStep //[0]int step, [1]float64 accuracy, [2]int step-amount
	Depth10PercentCapacity int64
}

var (
	//Conf *Config
	SymbolInfos map[string]*SymbolInfo
	Location    *time.Location
	ContUsdMap  map[string]int64
	ServerType  string
)

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

func PriceScale(symbol string) int32 {
	return GetSymbolInfo(symbol).PriceScale
}

func AmountScale(symbol string) int32 {
	return GetSymbolInfo(symbol).AmountScale
}

func GetSymbolInfo(symbol string) *SymbolInfo {
	if SymbolInfos[symbol] == nil {
		return SymbolInfos["default"]
	}
	return SymbolInfos[symbol]
}

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
