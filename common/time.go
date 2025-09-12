package common

import (
	"fmt"
	"time"
)

const (
	Name1Minute  = "1min"
	Name3Minute  = "3min"
	Name5Minute  = "5min"
	Name15Minute = "15min"
	Name30Minute = "30min"
	Name1Hour    = "60min"
	Name2Hour    = "2hour"
	Name4Hour    = "4hour"
	Name6Hour    = "6hour"
	Name8Hour    = "8hour"
	Name12Hour   = "12hour"
	Name1Day     = "1day"
	Name1Week    = "1week"
	Name1Mon     = "1mon"
	Name1Year    = "1year"

	Type1Minute  = 1
	Type3Minute  = 2
	Type5Minute  = 3
	Type15Minute = 4
	Type30Minute = 5
	Type1Hour    = 6
	Type2Hour    = 7
	Type4Hour    = 8
	Type6Hour    = 9
	Type8Hour    = 10
	Type12Hour   = 11
	Type1Day     = 12
	Type1Week    = 13
	Type1Mon     = 14
	Type1Year    = 15

	Step1Minute  = 60
	Step3Minute  = 60 * 3
	Step5Minute  = 60 * 5
	Step15Minute = 60 * 15
	Step30Minute = 60 * 30
	Step1Hour    = 60 * 60
	Step2Hour    = 60 * 60 * 2
	Step4Hour    = 60 * 60 * 4
	Step6Hour    = 60 * 60 * 6
	Step8Hour    = 60 * 60 * 8
	Step12Hour   = 60 * 60 * 12
	Step1Day     = 60 * 60 * 24
	Step1Week    = 60 * 60 * 24 * 7
	Step1Mon     = 60 * 60 * 24 * 31
	Step1Year    = 60 * 60 * 24 * 365
)

var HmType2Name map[int]string
var HmType2Step map[int]int
var HmName2Type map[string]int

func init() {
	HmType2Name = make(map[int]string)
	HmName2Type = make(map[string]int)
	HmType2Step = make(map[int]int)

	HmType2Name[Type1Minute] = Name1Minute
	HmType2Name[Type3Minute] = Name3Minute
	HmType2Name[Type5Minute] = Name5Minute
	HmType2Name[Type15Minute] = Name15Minute
	HmType2Name[Type30Minute] = Name30Minute
	HmType2Name[Type1Hour] = Name1Hour
	HmType2Name[Type2Hour] = Name2Hour
	HmType2Name[Type4Hour] = Name4Hour
	HmType2Name[Type6Hour] = Name6Hour
	HmType2Name[Type8Hour] = Name6Hour
	HmType2Name[Type12Hour] = Name12Hour
	HmType2Name[Type1Day] = Name1Day
	HmType2Name[Type1Week] = Name1Week
	HmType2Name[Type1Mon] = Name1Mon

	HmName2Type[Name1Minute] = Type1Minute
	HmName2Type[Name3Minute] = Type3Minute
	HmName2Type[Name5Minute] = Type5Minute
	HmName2Type[Name15Minute] = Type15Minute
	HmName2Type[Name30Minute] = Type30Minute
	HmName2Type[Name1Hour] = Type1Hour
	HmName2Type[Name2Hour] = Type2Hour
	HmName2Type[Name4Hour] = Type4Hour
	HmName2Type[Name6Hour] = Type6Hour
	HmName2Type[Name8Hour] = Type8Hour
	HmName2Type[Name12Hour] = Type12Hour
	HmName2Type[Name1Day] = Type1Day
	HmName2Type[Name1Week] = Type1Week
	HmName2Type[Name1Mon] = Type1Mon
}

func TimestampNowMs() int64 {
	var timestamp int64
	timestamp = time.Now().UTC().UnixNano() / 1000000
	return timestamp
}

func TimeNowMs() string {
	tm := time.Now()

	var ms int64
	ms = (time.Now().UTC().UnixNano() / 1000000) % 1000

	return fmt.Sprintf("%d:%d:%.3f", tm.Hour(), tm.Minute(), float32(tm.Second())+float32(ms)/1000)
}

func TimeNowHour() string {
	tm := time.Now()
	return fmt.Sprintf("%d%02d%02d%02d", tm.Year(), tm.Month(), tm.Day(), tm.Hour())
}

func IsLeapYear(timestamp int) bool {
	tm := time.Unix(int64(timestamp), 0)
	year, _, _ := tm.Date()
	if year%4 == 0 {
		return true
	}
	return false
}

//计算一个时间戳到当前时间的延迟
func TimeDelayS(timestamp int) int {
	timestampNow := int(time.Now().UTC().Unix())
	return timestampNow - timestamp
}

//对齐时间，获取时间窗口起始时间
func CurWindowTime(timestamp, steptype, wsid int) int {
	var starttime int
	switch steptype {
	case Type1Minute:
		starttime = (timestamp / Step1Minute) * 60 * 1
	case Type3Minute:
		starttime = (timestamp / Step3Minute) * 60 * 3
	case Type5Minute:
		starttime = (timestamp / Step5Minute) * 60 * 5
	case Type15Minute:
		starttime = (timestamp / Step15Minute) * 60 * 15
	case Type30Minute:
		starttime = (timestamp / Step30Minute) * 60 * 30
	case Type1Hour:
		//starttime = (timestamp/Step1Hour) * Step1Hour
		t := time.Unix(int64(timestamp), 0).In(Location)
		starttime = int(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, Location).Unix())
	case Type2Hour:
		//starttime = (timestamp/Step1Hour) * Step1Hour
		t := time.Unix(int64(timestamp), 0).In(Location)
		starttime = int(time.Date(t.Year(), t.Month(), t.Day(), (t.Hour()/2)*2, 0, 0, 0, Location).Unix())
	case Type4Hour:
		t := time.Unix(int64(timestamp), 0).In(Location)
		roundedDate := time.Date(t.Year(), t.Month(), t.Day(), (t.Hour()/4)*4, 0, 0, 0, Location)
		starttime = int(roundedDate.Unix())
	case Type6Hour:
		t := time.Unix(int64(timestamp), 0).In(Location)
		roundedDate := time.Date(t.Year(), t.Month(), t.Day(), (t.Hour()/6)*6, 0, 0, 0, Location)
		starttime = int(roundedDate.Unix())
	case Type8Hour:
		t := time.Unix(int64(timestamp), 0).In(Location)
		roundedDate := time.Date(t.Year(), t.Month(), t.Day(), (t.Hour()/8)*8, 0, 0, 0, Location)
		starttime = int(roundedDate.Unix())
	case Type12Hour:
		t := time.Unix(int64(timestamp), 0).In(Location)
		roundedDate := time.Date(t.Year(), t.Month(), t.Day(), (t.Hour()/12)*12, 0, 0, 0, Location)
		starttime = int(roundedDate.Unix())
	case Type1Day:
		t := time.Unix(int64(timestamp), 0).In(Location)
		roundedDate := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, Location)
		starttime = int(roundedDate.Unix())
	case Type1Week:
		t := time.Unix(int64(timestamp), 0).In(Location)
		roundedDate := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, Location)
		for roundedDate.Weekday() != time.Monday {
			roundedDate = roundedDate.AddDate(0, 0, -1)
		}
		starttime = int(roundedDate.Unix())
	case Type1Mon:
		t := time.Unix(int64(timestamp), 0).In(Location)
		roundedDatetime := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, Location)
		starttime = int(roundedDatetime.Unix())
	case Type1Year:
		tm := time.Unix(int64(timestamp), 0).In(Location)
		year, _, _ := tm.Date()
		tms := time.Date(year, 1, 1, 0, 0, 0, 0, Location)
		starttime = int(tms.Unix())
	default:
		starttime = 0
		Fatal("CurWindowTime get wrong type", steptype)
		//what???
	}

	return starttime
}

// 分片帮助函数
// 给定一个时间戳和kline类型，返回分片的起始时间，该时间点所在的offset，和该分片的全长
func GetListTsOffLen(steptype, timestamp, wsid int) (int, int, int) {
	//ver2，使用第2版本的分表方式
	var starttime, off, listLen int
	listLen = 0
	switch steptype {
	case Type1Minute:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		//Debug.Println(wsid, "GetListTsOffLen", timestamp, starttime)
		off = (timestamp - starttime) / Step1Minute
		//按年分表，每年的时间长度是不一样的，所以要用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / Step1Minute)

	case Type3Minute:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		//Debug.Println(wsid, "GetListTsOffLen", timestamp, starttime)
		off = (timestamp - starttime) / Step3Minute
		//按年分表，每年的时间长度是不一样的，所以要用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / Step3Minute)

	case Type5Minute:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		off = (timestamp - starttime) / Step5Minute
		//按年分表，每年的时间长度是不一样的，所以要用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / Step5Minute)

	case Type15Minute:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		off = (timestamp - starttime) / Step15Minute
		//按年分表，每年的时间长度是不一样的，所以要用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / Step15Minute)

	case Type30Minute:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		off = (timestamp - starttime) / Step30Minute
		//按年分表，每年的时间长度是不一样的，所以要用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / Step30Minute)

	case Type1Hour:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		off = (timestamp - starttime) / Step1Hour
		//按年分表，每年的天数用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / Step1Hour)

	case Type2Hour:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		t := time.Unix(int64(timestamp), 0).In(Location)
		off = (t.YearDay()-1)*12 + (t.Hour() / 2)
		//按年分表，每年的天数用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / (60 * 60 * 2))

	case Type4Hour:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		t := time.Unix(int64(timestamp), 0).In(Location)
		off = (t.YearDay()-1)*6 + (t.Hour() / 4)
		//按年分表，每年的天数用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / (60 * 60 * 4))

	case Type6Hour:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		t := time.Unix(int64(timestamp), 0).In(Location)
		off = (t.YearDay()-1)*4 + (t.Hour() / 6)
		//按年分表，每年的天数用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / (60 * 60 * 6))

	case Type8Hour:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		t := time.Unix(int64(timestamp), 0).In(Location)
		off = (t.YearDay()-1)*3 + (t.Hour() / 8)
		//按年分表，每年的天数用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / (60 * 60 * 8))

	case Type12Hour:
		//按年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		t := time.Unix(int64(timestamp), 0).In(Location)
		off = (t.YearDay()-1)*2 + (t.Hour() / 12)
		//按年分表，每年的天数用calendar计算
		nextYear := time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0)
		endtime := int(time.Date(nextYear.Year(), 1, 1, 0, 0, 0, 0, Location).Unix())
		listLen = int((endtime - starttime) / (60 * 60 * 12))

	case Type1Day:
		//按10年分表//starttime为以周为窗口的起始时间戳
		starttime = CurWindowTime(timestamp, Type1Year, wsid)
		// offset从0开始， yearday是从1开始
		off = time.Unix(int64(timestamp), 0).In(Location).YearDay() - 1
		//按年分表，每年的天数用calendar计算
		endtime := int(time.Unix(int64(starttime), 0).In(Location).AddDate(1, 0, 0).Unix())
		listLen = int((endtime - starttime) / (60 * 60 * 24))

	case Type1Week:
		// 不分表
		// 2018年1月1日正好是周一
		starttime = CurWindowTime(int(time.Date(2018, 1, 1, 0, 0, 0, 0, Location).Unix()), Type1Year, wsid)
		t := time.Unix(int64(timestamp), 0).In(Location)
		//daysAfter2018 := t.YearDay()
		//for i := 2018; i < t.Year(); i++ {
		//	if isLeapYear(i) {
		//		daysAfter2018 = daysAfter2018 + 366
		//	} else {
		//		daysAfter2018 = daysAfter2018 + 365
		//	}
		//}
		//off = daysAfter2018 / 7
		daysAfter2022 := t.YearDay()
		for i := 2022; i < t.Year(); i++ {
			if isLeapYear(i) {
				daysAfter2022 = daysAfter2022 + 366
			} else {
				daysAfter2022 = daysAfter2022 + 365
			}
		}
		off = daysAfter2022 / 7
		listLen = 52 * 5

	case Type1Mon:
		// starttime为2018年1月1日的当地时间，总共10年
		starttime = CurWindowTime(int(time.Date(2018, 1, 1, 0, 0, 0, 0, Location).Unix()), Type1Year, wsid)
		if time.Unix(int64(timestamp), 0).In(Location).Unix() < int64(starttime) {
			off = 0
			listLen = 60
		} else {
			//yearGap := time.Unix(int64(timestamp), 0).In(Location).Year() - 2018
			yearGap := time.Unix(int64(timestamp), 0).In(Location).Year() - 2022
			// offset从0开始，month是从1开始
			off = yearGap*12 + int(time.Unix(int64(timestamp), 0).In(Location).Month()) - 1
			listLen = 60
		}
	default:
		//what???
	}

	return starttime, off, listLen
}

/*
计算上一根k线的开始时间窗
因为夏令时的问题，计算都需要通过calendar来计算。

简化方案，需要特殊处理的只是在进入、离开夏令时的时间点，预计算后按照配置去特殊判断。省去每次计算的消耗
*/
func LastKlineTimeWindow(ts int64, klineType string) int64 {
	nowDate := time.Unix(int64(CurWindowTime(int(time.Unix(ts, 0).In(Location).Unix()), HmName2Type[klineType], 0)), 0).In(Location)
	switch klineType {
	case "1min":
		return nowDate.Unix() - 60
	case "5min":
		return nowDate.Unix() - 60*5
	case "15min":
		return nowDate.Unix() - 60*15
	case "30min":
		return nowDate.Unix() - 60*30
	case "60min":
		return nowDate.Unix() - 60*60
	case "4hour":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day(), (nowDate.Hour()/4-1)*4, 0, 0, 0, Location).Unix()
	case "1day":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()-1, 0, 0, 0, 0, Location).Unix()
	case "1mon":
		return time.Date(nowDate.Year(), nowDate.Month()-1, 1, 0, 0, 0, 0, Location).Unix()
	default:
		Fatal("get last kline time window with unknown type :", klineType)
		return -1
	}
}

/*
计算下一根k线的开始时间窗
因为夏令时的问题，计算都需要通过calendar来计算。
*/
func NextKlineTimeWindow(ts int64, klineType string) int64 {
	nowDate := time.Unix(int64(CurWindowTime(int(time.Unix(ts, 0).In(Location).Unix()), HmName2Type[klineType], 0)), 0).In(Location)
	switch klineType {
	case "1min":
		return nowDate.Unix() + 60
	case "3min":
		return nowDate.Unix() + 60*3
	case "5min":
		return nowDate.Unix() + 60*5
	case "15min":
		return nowDate.Unix() + 60*15
	case "30min":
		return nowDate.Unix() + 60*30
	case "60min":
		return nowDate.Unix() + 60*60
	case "2hour":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day(), (nowDate.Hour()/2+1)*2, 0, 0, 0, Location).Unix()
	case "4hour":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day(), (nowDate.Hour()/4+1)*4, 0, 0, 0, Location).Unix()
	case "6hour":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day(), (nowDate.Hour()/6+1)*6, 0, 0, 0, Location).Unix()
	case "8hour":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day(), (nowDate.Hour()/8+1)*8, 0, 0, 0, Location).Unix()
	case "12hour":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day(), (nowDate.Hour()/12+1)*12, 0, 0, 0, Location).Unix()
	case "1day":
		return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+1, 0, 0, 0, 0, Location).Unix()
	case "1week":
		switch nowDate.Weekday() {
		case time.Monday:
			return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+7, 0, 0, 0, 0, Location).Unix()
		case time.Tuesday:
			return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+6, 0, 0, 0, 0, Location).Unix()
		case time.Wednesday:
			return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+5, 0, 0, 0, 0, Location).Unix()
		case time.Thursday:
			return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+4, 0, 0, 0, 0, Location).Unix()
		case time.Friday:
			return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+3, 0, 0, 0, 0, Location).Unix()
		case time.Saturday:
			return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+2, 0, 0, 0, 0, Location).Unix()
		case time.Sunday:
			return time.Date(nowDate.Year(), nowDate.Month(), nowDate.Day()+1, 0, 0, 0, 0, Location).Unix()
		}
		return -1
	case "1mon":
		// 这里的day必须是1，如果是0，会在Date内部-1变成上个月最后一天
		return time.Date(nowDate.Year(), nowDate.Month()+1, 1, 0, 0, 0, 0, Location).Unix()
	default:
		Fatal("get next kline time window with unknown type :", klineType)
		return -1
	}
}

func isLeapYear(year int) bool {
	return year%400 == 0 || year%4 == 0 && year%100 != 0
}

func TimeSub2Ms(t1, t2 int64) int64 {
	return (t1 - t2) / int64(time.Millisecond)
}
