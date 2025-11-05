package l2quote

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"market-match/common"
	"market-match/config"
	"market-match/snapshotter"

	"github.com/shopspring/decimal"

	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

type SnapShots struct {
	Klines    map[string][]kline `json:"klines"`
	Market    []kline            `json:"market"`
	MaxMRId   int64              `json:"maxMRId"`
	MaxMRTS   int64              `json:"maxMRTS"`
	LastSPTS  int                `json:"lastSpTs"`
	StartTime int64              `json:"start-time"`
}

/*
生成一个当前时间点的快照绝对路径
*/
func (L *L2quote) genSnapshotPathTmp() string {
	return path.Join(L.snapshotPath, L.symbol, "TMPl2quote@snapshot@"+L.symbol+"@TMP")
}

/*
打快照函数

保存最后一个match result所在时间段、以及上一个时间段的kline
保存最后一个match result的id以及时间戳
保存24小时market信息
*/
func (L *L2quote) takeSnapShot() {
	common.Trace(L.symbol, "take snapshot")
	s := SnapShots{}
	s.Klines = make(map[string][]kline)
	L.quotation.lastSnapshotTime = int(time.Now().Unix())
	//MaxMRTS为零表示该交易对打快照时没有交易，跳过快照逻辑
	if L.quotation.MaxMRTS == 0 {
		common.Info(L.symbol, "don't have any orders, skip take snapshot")
		return
	}
	// 拦截从未记录快照情况
	if L.quotation.lastSnapshotTime == 0 {
		L.quotation.lastSnapshotTime = int(time.Now().Unix())
	}
	for _, t := range KLINETYPES {
		key := int64(common.CurWindowTime(L.quotation.lastSnapshotTime, common.HmName2Type[t], 0))
		lastKey := key - int64(common.HmType2Step[common.HmName2Type[t]])

		var lastK, lastKK interface{}
		var ok bool
		lastK, ok = L.quotation.Klines[t].Get(key)
		if !ok {
			common.Fatal(L.symbol, "l2quote snapshot get last kline error :", t, key)
			lastK = &kline{TS: key}
		}

		if lastKey == -1 {
			lastKK = &kline{}
		} else {
			lastKK, ok = L.quotation.Klines[t].Get(lastKey)
			// 月k等大时间跨度kline，没有从快照启动时没有上一根kline为正常，log改成info留底
			if !ok {
				common.Info(L.symbol, "snapshot can't get last before last kline :", t, key)
				lastKK = &kline{TS: lastKey}
			}
		}

		s.Klines[t] = []kline{*lastK.(*kline), *lastKK.(*kline)}
	}
	s.StartTime = time.Now().Unix()
	s.Market = L.quotation.Market
	s.MaxMRId = L.quotation.MaxMRId
	s.MaxMRTS = L.quotation.MaxMRTS
	s.LastSPTS = L.quotation.lastSnapshotTime
	data, err := json.Marshal(s)
	if err != nil {
		common.Fatal(L.symbol, "l2quote snapshot marshal error :", err)
	}

	//common.Trace("l2quote snapshot :", string(data))
	//snapshotsFileName := L.genSnapshotPath()
	//// create symbol dir first
	//err = os.MkdirAll(path.Dir(snapshotsFileName), os.ModePerm)
	//if err != nil {
	//	common.Fatal(L.symbol, "create snapshot directory err :", err)
	//}
	//err = ioutil.WriteFile(snapshotsFileName, data, os.ModePerm)
	//if err != nil {
	//	common.Fatal(L.symbol, "write snapshot error :", err)
	//}

	snapshotsFileNameTmp := L.genSnapshotPathTmp()

	//删除临时镜像文件,
	if snapshotter.PathExists(snapshotsFileNameTmp) == true {
		err := os.Remove(snapshotsFileNameTmp)
		if err != nil {
			common.Fatal("del tmp snapshotter file failed :", snapshotsFileNameTmp, "err:", err)
		}
	}

	//创建临时镜像文件
	err = os.MkdirAll(path.Dir(snapshotsFileNameTmp), os.ModePerm)
	if err != nil {
		common.Fatal(L.symbol, "create snapshot directory err :", err)
	}

	err = ioutil.WriteFile(snapshotsFileNameTmp, data, os.ModePerm)
	if err != nil {
		common.Fatal(L.symbol, "write snapshot error :", err)
	}

	//fmt.Println("HHJLKHJKLHJLKHJ:",len(files))
	snapshotsFileName := L.genSnapshotPath()

	err1 := os.Rename(snapshotsFileNameTmp, snapshotsFileName)

	if err1 != nil {
		common.Fatal("reName Error:", err, ";pathNameTmp:", snapshotsFileNameTmp, ";pathName:", snapshotsFileName)
	}

	go L.UploadToS3(snapshotsFileName)
}

/*
打快照函数

保存最后一个match result所在时间段、以及上一个时间段的kline
保存最后一个match result的id以及时间戳
保存24小时market信息
*/
// func (L *L2quote) takeSnapShot() {

// 	common.Trace(L.symbol, "take snapshot")
// 	s := SnapShots{}
// 	s.Klines = make(map[string][]kline)

// 	// MaxMRTS为零表示该交易对打快照时没有交易，跳过快照逻辑
// 	if L.quotation.MaxMRTS == 0 {
// 		common.Info(L.symbol, "don't have any orders, skip take snapshot")
// 		return
// 	}

// 	for _, t := range KLINETYPES {
// 		key := int64(common.CurWindowTime(int(L.quotation.MaxMRTS), common.HmName2Type[t], 0))
// 		lastKey := key - int64(common.HmType2Step[common.HmName2Type[t]])

// 		var lastK, lastKK interface{}
// 		var ok bool
// 		lastK, ok = L.quotation.Klines[t].Get(key)
// 		if !ok {
// 			common.Fatal(L.symbol, "l2quote snapshot get last kline error :", t, key)
// 			lastK = &kline{TS: key}
// 		}

// 		if lastKey == -1 {
// 			lastKK = &kline{}
// 		} else {
// 			lastKK, ok = L.quotation.Klines[t].Get(lastKey)
// 			// 月k等大时间跨度kline，没有从快照启动时没有上一根kline为正常，log改成info留底
// 			if !ok {
// 				common.Info(L.symbol, "snapshot can't get last before last kline :", t, key)
// 				lastKK = &kline{TS: lastKey}
// 			}
// 		}

// 		s.Klines[t] = []kline{*lastK.(*kline), *lastKK.(*kline)}
// 	}
// 	s.Market = L.quotation.Market

// 	s.MaxMRId = L.quotation.MaxMRId

// 	s.MaxMRTS = L.quotation.MaxMRTS

// 	data, err := json.Marshal(s)
// 	if err != nil {
// 		common.Fatal(L.symbol, "l2quote snapshot marshal error :", err)
// 	}

// 	common.Trace("l2quote snapshot :", string(data))
// 	snapshotsFileName := L.genSnapshotPath()
// 	// create symbol dir first
// 	err = os.MkdirAll(path.Dir(snapshotsFileName), os.ModePerm)
// 	if err != nil {
// 		common.Fatal(L.symbol, "create snapshot directory err :", err)
// 	}
// 	err = ioutil.WriteFile(snapshotsFileName, data, os.ModePerm)
// 	if err != nil {
// 		common.Fatal(L.symbol, "write snapshot error :", err)
// 	}

// 	go L.UploadToS3(snapshotsFileName)
// }

// 如果快照破坏，需要在启动时验证，可能需要删除相应的快照
func (L *L2quote) checkSnapShot() {

	common.Trace(L.symbol, "check Snapshot")

	// MaxMRTS为零表示该交易对打快照时没有交易，跳过快照逻辑
	if L.quotation.MaxMRTS == 0 {
		common.Info(L.symbol, "checkSnapShot|don't have any orders, skip take snapshot")
		return
	}

	if L.quotation.lastSnapshotTime == 0 {
		common.Info(L.symbol, "checkSnapShot|don't have old snapshot, skip take snapshot")
		return
	}
	for _, t := range KLINETYPES {

		key := int64(common.CurWindowTime(L.quotation.lastSnapshotTime, common.HmName2Type[t], 0))
		lastKey := key - int64(common.HmType2Step[common.HmName2Type[t]])

		var ok bool

		_, ok = L.quotation.Klines[t].Get(key)

		if !ok {
			common.Fatal(L.symbol, "checkSnapShot|l2quote snapshot get last kline error :", t, key)
			return
		}

		if lastKey == -1 {
		} else {
			_, ok = L.quotation.Klines[t].Get(lastKey)
			// 月k等大时间跨度kline，没有从快照启动时没有上一根kline为正常，log改成info留底
			if !ok {
				common.Info(L.symbol, "checkSnapShot|snapshot can't get last before last kline :", t, key)
			}
		}
	}
}

/*
从快照恢复
没有快照则初始化各类行情数据
*/
func (L *L2quote) initKlineFromSnapshots() {
	L.ticker = &Ticker{
		TS:         0,
		SeqId:      0,
		OpenPrice:  decimal.Zero,
		ClosePrice: decimal.Zero,
		HighPrice:  decimal.Zero,
		LowPrice:   decimal.Zero,
		Vol:        decimal.Zero,
		AskPrice:   decimal.Zero,
		AskVol:     decimal.Zero,
		BidPrice:   decimal.Zero,
		BidVol:     decimal.Zero,
	}

	// 从快照恢复，中间读取、加载快照出现问题会直接panic，加载成功会返回nil，没有找到快照返回err，走后面的初始化流程
	err := L.recoverFromSnapshot()
	if err == nil {
		common.Info("l2quote " + L.symbol + " load snapshot success")
		return
	}

	common.Info(L.symbol, "don't find snapshot, start from zero")

	q := Quotation{}

	q.Klines = make(map[string]*treemap.Map)
	for _, t := range KLINETYPES {
		q.Klines[t] = treemap.NewWith(utils.Int64Comparator)
	}

	q.MaxMRId = 0

	// 初始化24小时汇总数据
	q.Market = make([]kline, 24*60, 24*60)
	curTS := common.CurWindowTime(int(time.Now().Unix()), common.Type1Minute, 0)
	for i := 0; i < 24*60; i++ {
		q.Market[i] = kline{TS: int64(curTS - 60*i)}
	}

	L.quotation = &q
	L.quotation.MaxMRId = 0
	L.quotation.MaxMRTS = time.Now().Unix()
	L.quotation.lastSnapshotTime = int(time.Now().Unix())
	common.Info(L.symbol, "market init: ", L.quotation.Market)
}

/*
从快照恢复，没有找到快照则返回err，成功恢复返回nil
快照读取与解析出现问题直接fatal
*/
func (L *L2quote) recoverFromSnapshot() error {
	latestSnapshotName, err := getLatestSnapshotName(L.snapshotPath, L.symbol)
	if err != nil {
		return err
	}
	common.Trace("latest snapshot name :", latestSnapshotName)
	data, err := ioutil.ReadFile(path.Join(L.snapshotPath, L.symbol, latestSnapshotName))
	if err != nil {
		common.Fatal(fmt.Sprintf("%s l2quote snapshot read error, check snapshot file plz. file name : %s , error : %s",
			L.symbol, path.Join(L.snapshotPath, L.symbol, latestSnapshotName), err))
	}

	snapshot := SnapShots{}
	err = json.Unmarshal(data, &snapshot)
	if err != nil {
		common.Fatal(fmt.Sprintf("%s l2quote snapshot unmarshal error, file : %s, error : %s",
			L.symbol, path.Join(L.snapshotPath, L.symbol, latestSnapshotName), err))
	}

	q := Quotation{}
	q.Klines = make(map[string]*treemap.Map)
	for _, t := range KLINETYPES {
		q.Klines[t] = treemap.NewWith(utils.Int64Comparator)
	}
	q.MaxMRId = 0
	q.Market = make([]kline, 24*60, 24*60)

	for i := 0; i < 24*60; i++ {
		q.Market[i] = kline{}
	}

	L.quotation = &q
	for _, t := range KLINETYPES {
		L.quotation.Klines[t].Put(snapshot.Klines[t][0].TS, &snapshot.Klines[t][0])
		L.quotation.Klines[t].Put(snapshot.Klines[t][1].TS, &snapshot.Klines[t][1])
	}
	L.quotation.lastSnapshotTime = snapshot.LastSPTS
	L.quotation.MaxMRId = snapshot.MaxMRId
	L.quotation.MaxMRId = snapshot.MaxMRId
	L.quotation.MaxMRTS = snapshot.MaxMRTS
	L.quotation.Market = snapshot.Market
	// 发送kline的初始值也根据快照初始化
	L.sendMRID = snapshot.MaxMRId

	L.checkSnapShot()
	fmt.Println(L.quotation.MaxMRTS, L.symbol)
	L.getAllCurrentKline(int64(L.quotation.lastSnapshotTime))

	common.Debug(fmt.Sprintf("%s load snapshots : %+v\n", L.symbol, L.quotation))

	return nil
}

/*
异步上传快照到Amazon S3服务。
并根据配置删除过旧镜像
*/
func (L *L2quote) UploadToS3(pathName string) {
	if config.GetBool("aws.s3.enable", true) {
		upLoader := s3manager.NewUploader(session.Must(session.NewSession()))

		s3file, err := os.Open(pathName)
		if err != nil {
			common.Fatal("l2quote upload snapshot error:", err)
		}
		defer s3file.Close()
		ctx := aws.BackgroundContext()
		var cancelFn func()
		if config.GetInt64("aws.s3.upload-timeout-second", 5) > 0 {
			ctx, cancelFn = context.WithTimeout(ctx,
				time.Second*time.Duration(config.GetInt64("aws.s3.upload-timeout-second", 5)))
		}
		defer cancelFn()

		s3Path := strconv.Itoa(config.GetInt("app.seq", 0)) + "/l2quote/" + L.symbol + "/" + path.Base(s3file.Name())
		_, err = upLoader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket: aws.String(config.GetString("aws.s3.bucket", "market")),
			Key:    aws.String(s3Path),
			Body:   s3file,
		})
		if err != nil {
			common.Error("l2quote s3 upload file failed", err)
			//dogstatsd.Event("upload snapshot", "l2quote upload snapshot err :"+fmt.Sprintln(err), statsd.Error)
		}
	}

	ids := getSnapshotIds(L.snapshotPath, L.symbol)
	if len(ids) > int(L.snapshotMaxHistoryNum) {
		for i := range ids {
			if i < len(ids)-int(L.snapshotMaxHistoryNum) {
				fileName := L.buildL2quoteSnapshotPath(ids[i])
				os.Remove(fileName)
			}
		}
	}
}

/*
生成一个当前时间点的快照绝对路径
*/
func (L *L2quote) genSnapshotPath() string {
	return path.Join(L.snapshotPath, L.symbol,
		"l2quote.snapshot."+L.symbol+"."+time.Now().Format("20060102150405"))
}

/*
获取最新快照中的match result id
*/
func GetLargestMRID(snapshotPath string, symbol string) int64 {
	latestSnapshotName, err := getLatestSnapshotName(snapshotPath, symbol)
	if err != nil {
		// 没有快照的时候如何优雅的提示
		return 0
	}
	common.Trace("latest snapshot name :", latestSnapshotName)
	data, err := ioutil.ReadFile(path.Join(snapshotPath, symbol, latestSnapshotName))
	if err != nil {
		common.Fatal("read l2quote snapshot error :", err)
	}

	s := SnapShots{}
	err = json.Unmarshal(data, &s)
	if err != nil {
		common.Fatal("l2quote snapshot unmarshal :", err)
	}

	return s.MaxMRId
}

/*
获取最新一个快照文件名
*/
func getLatestSnapshotName(snapshotPath string, symbol string) (string, error) {
	dir, _ := ioutil.ReadDir(path.Join(snapshotPath, symbol))
	ids := treemap.NewWith(utils.Int64Comparator)
	for _, f := range dir {
		if strings.HasPrefix(f.Name(), "l2quote.snapshot") {
			strs := strings.Split(f.Name(), ".")
			id, err := strconv.ParseInt(strs[len(strs)-1], 10, 64)
			if err != nil {
				return "", err
			}
			ids.Put(id, f.Name())
		}
	}

	_, latestKey := ids.Max()
	if latestKey == nil {
		return "", errors.New(symbol + " can't found any snapshot")
	}

	return latestKey.(string), nil
}

/*
获取所有排序后的快照id列表，id 格式 "20060102150405"
*/
func getSnapshotIds(snapshotPath string, symbol string) []int64 {
	dir, _ := ioutil.ReadDir(path.Join(snapshotPath, symbol))
	ids := treemap.NewWith(utils.Int64Comparator)
	for _, f := range dir {
		if strings.HasPrefix(f.Name(), "l2quote.snapshot") {
			strs := strings.Split(f.Name(), ".")
			id, err := strconv.ParseInt(strs[len(strs)-1], 10, 64)
			if err != nil {
				return []int64{}
			}
			ids.Put(id, f.Name())
		}
	}
	idsList := make([]int64, 0, 128)
	for _, k := range ids.Keys() {
		idsList = append(idsList, k.(int64))
	}

	return idsList
}

/*
根据ID拼接快照绝对路径
*/
func (L *L2quote) buildL2quoteSnapshotPath(id int64) string {
	return path.Join(L.snapshotPath, L.symbol, "l2quote.snapshot."+L.symbol+"."+strconv.FormatInt(id, 10))

}

// 获取最后一个快照信息
func (L *L2quote) getLastSnapshot() (q Quotation, err error) {

	latestSnapshotName, err := getLatestSnapshotName(L.snapshotPath, L.symbol)
	if err != nil {
		return
	}
	common.Trace("latest snapshot name :", latestSnapshotName)
	data, err := ioutil.ReadFile(path.Join(L.snapshotPath, L.symbol, latestSnapshotName))
	if err != nil {
		common.Warn(fmt.Sprintf("%s l2quote snapshot read error, check snapshot file plz. file name : %s , error : %s",
			L.symbol, path.Join(L.snapshotPath, L.symbol, latestSnapshotName), err))
		return
	}

	snapshot := SnapShots{}
	err = json.Unmarshal(data, &snapshot)
	if err != nil {
		common.Warn(fmt.Sprintf("%s l2quote snapshot unmarshal error, file : %s, error : %s",
			L.symbol, path.Join(L.snapshotPath, L.symbol, latestSnapshotName), err))
		return
	}

	q.Klines = make(map[string]*treemap.Map)
	for _, t := range KLINETYPES {
		q.Klines[t] = treemap.NewWith(utils.Int64Comparator)
	}
	q.MaxMRId = snapshot.MaxMRId
	q.MaxMRTS = snapshot.MaxMRTS
	q.Market = make([]kline, 24*60, 24*60)
	for i := 0; i < 24*60; i++ {
		q.Market[i] = kline{}
	}
	return
}
