package common

//
//import (
//	"fmt"
//	"gopkg.in/natefinch/lumberjack.v2"
//	"io"
//	"io/ioutil"
//	"log"
//	"os"
//)
//
//var (
//	trace   *log.Logger
//	debug   *log.Logger
//	info    *log.Logger
//	warning *log.Logger
//	_error  *log.Logger
//	fatal   *log.Logger
//	console *log.Logger
//)
//
//const (
//	LevelTrace = "trace"
//	LevelDebug = "debug"
//	LevelInfo  = "info"
//	LevelWarn  = "warn"
//
//	ENV_LOGINFO    = "LOG_INFO"
//	ENV_LOGERROR   = "LOG_ERROR"
//	ENV_LOGSTARTOK = "START_OK"
//)
//
//func init() {
//	std := log.New(os.Stderr,
//		"",
//		log.Ldate|log.Ltime|log.Lshortfile)
//	trace = std
//	debug = std
//	info = std
//	warning = std
//	_error = std
//	fatal = std
//	console = std
//}
//
//var LogLevel string
//
//func logfileInit(
//	traceHandle io.Writer,
//	debugHandle io.Writer,
//	infoHandle io.Writer,
//	warningHandle io.Writer,
//	errorHandle io.Writer,
//	fatalHandle io.Writer) {
//
//	trace = log.New(traceHandle,
//		"TRACE: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	debug = log.New(debugHandle,
//		"DEBUG: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	info = log.New(infoHandle,
//		"INFO: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	warning = log.New(warningHandle,
//		"WARNING: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	_error = log.New(errorHandle,
//		"ERROR: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	fatal = log.New(fatalHandle,
//		"FATAL: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	console = log.New(os.Stderr,
//		"console",
//		log.Ldate|log.Ltime|log.Lshortfile)
//}
//
//func LogInit(level string) (ok bool) {
//	logfile := LogFile
//	envInfoLogPath, exist := os.LookupEnv(ENV_LOGINFO)
//	if exist && envInfoLogPath != "" {
//		logfile = envInfoLogPath
//	}
//	logErrorFile := logfile + ".wf"
//	envErrorLogPath, exist := os.LookupEnv(ENV_LOGERROR)
//	if exist && envErrorLogPath != "" {
//		logErrorFile = envErrorLogPath
//	}
//
//	var traceHandle, debugHandle io.Writer
//
//	// I just hard code it,bite me
//	infoLogHandler := &lumberjack.Logger{
//		Filename:   logfile,
//		MaxSize:    500,
//		MaxBackups: 20,
//		MaxAge:     7,
//		Compress:   false,
//	}
//
//	wfLogHandler := &lumberjack.Logger{
//		Filename:   logErrorFile,
//		MaxSize:    500,
//		MaxBackups: 20,
//		MaxAge:     7,
//		Compress:   false,
//	}
//
//	LogLevel = level
//
//	traceHandle = infoLogHandler
//	debugHandle = infoLogHandler
//
//	logfileInit(traceHandle,
//		debugHandle,
//		infoLogHandler,
//		wfLogHandler,
//		wfLogHandler,
//		wfLogHandler)
//
//	return true
//}
//
//func Trace(v ...interface{}) {
//	if LogLevel == LevelTrace {
//		trace.Output(2, fmt.Sprintln(v...))
//	}
//}
//
//func Debug(v ...interface{}) {
//	if LogLevel == LevelDebug ||
//		LogLevel == LevelTrace {
//		debug.Output(2, fmt.Sprintln(v...))
//	}
//}
//
//func Info(v ...interface{}) {
//	if LogLevel == LevelDebug ||
//		LogLevel == LevelTrace ||
//		LogLevel == LevelInfo {
//		info.Output(2, fmt.Sprintln(v...))
//	}
//}
//
//func Warn(v ...interface{}) {
//	warning.Output(2, fmt.Sprintln(v...))
//}
//
//func Error(v ...interface{}) {
//	s := fmt.Sprint(v...)
//	_error.Output(2, s)
//}
//
//// panic -> error msg
//func Fatal(v ...interface{}) {
//	s := fmt.Sprint(v...)
//	console.Output(2, s)
//	fatal.Output(2, s)
//	panic(s)
//}
//
