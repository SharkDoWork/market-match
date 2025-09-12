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

var errorLogger *zap.SugaredLogger

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

func Debug(args ...interface{}) {
	errorLogger.Debug(args...)
}

func Debugf(template string, args ...interface{}) {
	errorLogger.Debugf("[MATCH-DEBUG] \t "+template, args...)
}

func Info(args ...interface{}) {
	errorLogger.Info(args...)
}

func Trace(template string, args ...interface{}) {

}
func Infof(template string, args ...interface{}) {

	errorLogger.Infof("[MATCH-INFO] \t "+template, args...)
}

func Warn(args ...interface{}) {
	errorLogger.Warn(args...)
}

func Warnf(template string, args ...interface{}) {
	errorLogger.Warnf("[MATCH-WARN] \t "+template, args...)
}

func Error(args ...interface{}) {
	errorLogger.Error(args...)
}

func Errorf(template string, args ...interface{}) {
	errorLogger.Errorf(template, args...)
}

func DPanic(args ...interface{}) {
	errorLogger.DPanic(args...)
}

func DPanicf(template string, args ...interface{}) {
	errorLogger.DPanicf(template, args...)
}

func Panic(args ...interface{}) {
	errorLogger.Panic(args...)
}

func Panicf(template string, args ...interface{}) {
	errorLogger.Panicf(template, args...)
}

func Fatal(args ...interface{}) {
	errorLogger.Fatal(args...)
}

func Fatalf(template string, args ...interface{}) {
	errorLogger.Fatalf(template, args...)
}

func WriteStartOk() {
	path, exist := os.LookupEnv("START_OK")
	if exist && path != "" {
		ioutil.WriteFile(path, []byte("START_OK"), 0600)
	}
	fmt.Println("START_OK")
}
