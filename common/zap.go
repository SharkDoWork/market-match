// Package common 提供撮合引擎的公共基础设施，包括日志系统、配置加载、
// 时间工具、错误码定义、全局通道等通用能力，供其他业务包复用。
package common

import (
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"io/ioutil"
	"market-match/config"
	"os"
	"strings"
	"time"
)

// errorLogger 是包级全局日志实例（zap 的 SugaredLogger），
// 由 ZapInit 初始化，之后通过 Debug/Info/Warn/Error 等包级函数使用。
var errorLogger *zap.SugaredLogger

// ZapInit 初始化全局 zap 日志系统，是进程启动时必须调用的初始化函数。
//
// 核心行为：
//  1. 按配置决定输出 JSON 格式还是控制台可读格式（log.json-encode）。
//  2. 按日志级别分流：Info 及以下写入 info 日志文件，Warn 及以上写入 error 日志文件。
//  3. 若开启 log.debug，额外将 Debug 级别同时输出到 stdout 和独立的 debug 日志文件。
//  4. 日志文件按小时自动切割，并保留 log.max-age 配置的小时数。
//
// 初始化失败（如日志目录不可写）会直接以 ErrnoLogInitFailed 退出进程。
func ZapInit() {
	// 设置一些基本日志格式
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "ts",
		LevelKey:      "level",
		NameKey:       "log",
		CallerKey:     "file",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05"))
		}, // ISO8601 UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.ShortCallerEncoder,     // 全路径编码器
		EncodeName:     zapcore.FullNameEncoder,
	}
	var encoder zapcore.Encoder
	if config.GetBool("log.json-encode", false) {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// 实现两个判断日志等级的interface
	infoLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl <= zapcore.InfoLevel
	})

	errorLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.WarnLevel
	})

	// 获取 info、error日志文件的io.Writer 抽象 getWriter() 在下方实现
	infoWriter := getWriter(config.GetString("log.info-path", "./logs/"))
	errorWriter := getWriter(config.GetString("log.error-path", "./logs/"))
	// 最后创建具体的Logger
	var cores []zapcore.Core
	cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(infoWriter), infoLevel))
	cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(errorWriter), errorLevel))

	if config.GetBool("log.debug", false) {

		debugLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl == zapcore.DebugLevel
		})
		debugWriter := getWriter("./log/debug.log")
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), debugLevel))
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(debugWriter), debugLevel))
	}

	core := zapcore.NewTee(
		cores...,
	)
	log := zap.New(core) // 需要传入 zap.AddCaller() 才会显示打日志点的文件名和行数, 有点小坑
	errorLogger = log.Sugar()
	Debug("日志初始化成功")
}

// getWriter 为指定日志文件路径创建一个支持按时间轮转的 io.Writer。
// 底层使用 file-rotatelogs：实际写入的文件名形如 "xxx-2024010115.log"（按小时命名），
// 超过 log.max-age（默认 24 小时）的旧文件会被自动清理，
// 每 log.rotation（默认 1 小时）整点切割一次。
// 创建失败时直接退出进程，因为日志不可用属于致命错误。
func getWriter(filename string) io.Writer {
	// 生成rotatelogs的Logger 实际生成的文件名 demo.log.YYmmddHH
	// demo.log是指向最新日志的链接
	// 保存7天内的日志，每1小时(整点)分割一次日志
	hook, err := rotatelogs.New(
		strings.Replace(filename, ".log", "", -1)+"-%Y%m%d%H.log", // 没有使用go风格反人类的format格式
		//rotatelogs.WithLinkName(filename),
		rotatelogs.WithMaxAge(time.Hour*time.Duration(config.GetInt64("log.max-age", 24))),
		rotatelogs.WithRotationTime(time.Hour*time.Duration(config.GetInt64("log.rotation", 1))),
	)
	if err != nil {
		os.Exit(ErrnoLogInitFailed)
	}
	return hook
}

// Debug 输出 Debug 级别日志（可变参数形式）。
func Debug(args ...interface{}) {
	errorLogger.Debug(args...)
}

// Debugf 按格式串输出 Debug 级别日志，自动附带 [MATCH-DEBUG] 前缀便于检索。
func Debugf(template string, args ...interface{}) {
	errorLogger.Debugf("[MATCH-DEBUG] \t "+template, args...)
}

// Info 输出 Info 级别日志（可变参数形式）。
func Info(args ...interface{}) {
	errorLogger.Info(args...)
}

// Trace 当前为空实现，保留接口兼容性。
func Trace(template string, args ...interface{}) {

}

// Infof 按格式串输出 Info 级别日志，自动附带 [MATCH-INFO] 前缀便于检索。
func Infof(template string, args ...interface{}) {

	errorLogger.Infof("[MATCH-INFO] \t "+template, args...)
}

// Warn 输出 Warn 级别日志（可变参数形式）。
func Warn(args ...interface{}) {
	errorLogger.Warn(args...)
}

// Warnf 按格式串输出 Warn 级别日志，自动附带 [MATCH-WARN] 前缀便于检索。
func Warnf(template string, args ...interface{}) {
	errorLogger.Warnf("[MATCH-WARN] \t "+template, args...)
}

// Error 输出 Error 级别日志（可变参数形式）。
func Error(args ...interface{}) {
	errorLogger.Error(args...)
}

// Errorf 按格式串输出 Error 级别日志。
func Errorf(template string, args ...interface{}) {
	errorLogger.Errorf(template, args...)
}

// DPanic 输出 DPanic 级别日志：开发环境会 panic，生产环境仅记录错误。
func DPanic(args ...interface{}) {
	errorLogger.DPanic(args...)
}

// DPanicf 按格式串输出 DPanic 级别日志。
func DPanicf(template string, args ...interface{}) {
	errorLogger.DPanicf(template, args...)
}

// Panic 输出 Panic 级别日志并触发 panic。
func Panic(args ...interface{}) {
	errorLogger.Panic(args...)
}

// Panicf 按格式串输出 Panic 级别日志并触发 panic。
func Panicf(template string, args ...interface{}) {
	errorLogger.Panicf(template, args...)
}

// Fatal 输出 Fatal 级别日志并调用 os.Exit(1) 终止进程。
func Fatal(args ...interface{}) {
	errorLogger.Fatal(args...)
}

// Fatalf 按格式串输出 Fatal 级别日志并调用 os.Exit(1) 终止进程。
func Fatalf(template string, args ...interface{}) {
	errorLogger.Fatalf(template, args...)
}

// WriteStartOk 用于向外部健康检查/部署系统通知"进程启动成功"。
// 若设置了环境变量 START_OK（值为文件路径），则向该文件写入标记内容；
// 同时向标准输出打印 START_OK，供容器或脚本捕获。
func WriteStartOk() {
	path, exist := os.LookupEnv("START_OK")
	if exist && path != "" {
		ioutil.WriteFile(path, []byte("START_OK"), 0600)
	}
	fmt.Println("START_OK")
}
