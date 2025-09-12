package snapshotter

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"market-match/common"
	"market-match/config"
	"market-match/match"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func init() {
	gob.Register(match.Order{})
}

type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Less(i, j int) bool { return s[i] > s[j] } // 倒序，大的在前面
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort is a convenience method.
func (s Int64Slice) Sort() {
	sort.Sort(s)
}

var upLoader *s3manager.Uploader

func Init() {
	os.Setenv("AWS_ACCESS_KEY_ID", config.GetString("aws.credential.access-key", ""))
	os.Setenv("AWS_SECRET_ACCESS_KEY", config.GetString("aws.credential.secret-key", ""))
	os.Setenv("AWS_REGION", config.GetString("aws.s3.region", ""))

	if config.GetBool("aws.s3.enable", false) {
		upLoader = s3manager.NewUploader(session.Must(session.NewSession()))
	}
}

func MinGap() int64 {
	return config.GetInt64("snapshot.min-gap", 100000)
}

func EndWith(symbol string) string {
	return symbol + config.GetString("snapshot.ends-with", "-snapshot-go")
}

func historyNum() int {
	return config.GetInt("snapshot.n-history", 10)
}

func snapshotDir() string {
	return config.GetString("snapshot.dir", "./sp/")
}

// 12个0可以处理100年pro站的id量
func BuildSnapshotPath(book *match.OrderBook, fromId int64) string {
	fromIdStr := fmt.Sprintf("%012d", fromId)
	dir := snapshotDir()
	return dir + string(os.PathSeparator) + fromIdStr + "." + EndWith(book.Symbol)
}

func DumpSnapshotChan(symbol string) chan *match.OrderBook {
	ch := make(chan *match.OrderBook, 10)
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if len(ch) > 1 { //打镜像的channel有积压
					common.Info("snapshot chan ", symbol, " length", len(ch))
				}
			case book := <-ch:
				startTs := time.Now().UnixNano()
				dumpSnapshot(book)
				endTs := time.Now().UnixNano()
				common.Info("symbol:", symbol, "snapshot used time ms:", (endTs-startTs)/1000000)
			}
		}
	}()
	return ch
}

func dumpSnapshot(book *match.OrderBook) {
	pathName := BuildSnapshotPath(book, book.FromId)
	dump(book, pathName)
}

func PathExists(path string) bool {

	_, err := os.Stat(path)

	if err == nil {
		return true
	}

	if os.IsNotExist(err) {
		return false
	}

	return false
}

// 12个0可以处理100年pro站的id量
func BuildSnapshotPathTmp(book *match.OrderBook) string {
	//fromIdStr := fmt.Sprintf("%012d", fromId)
	dir := snapshotDir()
	return dir + string(os.PathSeparator) + "TMP@" + EndWith(book.Symbol) + "@TMP"
}

func dump(book *match.OrderBook, pathName string) {
	//dogstatsd.Event("exchange snapshot", "snapshot with "+strconv.FormatInt(book.FromId, 10), statsd.Info)
	startTime := common.TimestampNowMs()
	//file, err := os.Create(pathName)
	//if err != nil {
	//	common.Fatal("create file failed :", pathName, "err:", err)
	//}
	//enc := gob.NewEncoder(file)
	//err = enc.Encode(book)
	//if err != nil {
	//	common.Fatal("snapshotter encode error: ", err)
	//}
	//file.Close()

	pathNameTmp := BuildSnapshotPathTmp(book)

	//删除临时镜像文件,
	if PathExists(pathNameTmp) == true {
		err := os.Remove(pathNameTmp)
		if err != nil {
			common.Fatal("del tmp snapshotter file failed :", pathNameTmp, "err:", err)
		}
	}

	//创建临时镜像文件
	file, err := os.Create(pathNameTmp)
	if err != nil {
		common.Fatal("create file failed :", pathNameTmp, "err:", err)
	}
	enc := gob.NewEncoder(file)
	err = enc.Encode(book)
	if err != nil {
		common.Fatal("snapshotter encode error: ", err)
	}
	file.Close()

	//临时镜像文件，生成已完成，最后改名字
	// fileName := file.Name()

	// dotIndex := strings.LastIndex(fileName, ".")
	// if dotIndex != -1 && dotIndex != 0 {
	// 	pathName += fileName[dotIndex:]
	// 	fmt.Println(pathName)
	// }
	err1 := os.Rename(pathNameTmp, pathName)

	if err1 != nil {
		common.Fatal("reName Error:", err, ";pathNameTmp:", pathNameTmp, ";pathName:", pathName)

	}

	endTime := common.TimestampNowMs()
	common.Info(book.Symbol, " dump snapshot use time:", endTime-startTime, book.SellSet.Size(), book.BuySet.Size())

	t1 := time.Now().UnixNano()
	UploadToS3(book, pathName)
	t2 := time.Now().UnixNano() - t1
	common.Info("symbol :", book.Symbol, " ups3 usetime :", t2/1000000)
}

// func dump(book *match.OrderBook, pathName string) {
// 	dogstatsd.Event("exchange snapshot", "snapshot with "+strconv.FormatInt(book.FromId, 10), statsd.Info)
// 	startTime := common.TimestampNowMs()
// 	file, err := os.Create(pathName)
// 	if err != nil {
// 		common.Fatal("create file failed :", pathName, "err:", err)
// 	}
// 	enc := gob.NewEncoder(file)
// 	err = enc.Encode(book)
// 	if err != nil {
// 		common.Fatal("snapshotter encode error: ", err)
// 	}
// 	file.Close()
// 	endTime := common.TimestampNowMs()
// 	common.Info(book.Symbol, " dump snapshot use time:", endTime-startTime, book.SellSet.Size(), book.BuySet.Size())

// 	t1 := time.Now().UnixNano()
// 	UploadToS3(book, pathName)
// 	t2 := time.Now().UnixNano() - t1
// 	common.Info("symbol :", book.Symbol, " ups3 usetime :", t2/1000000)
// }

func UploadToS3(book *match.OrderBook, pathName string) {
	if config.GetBool("aws.s3.enable", false) {
		s3file, err := os.Open(pathName)
		if err != nil {
			common.Fatal("upload snapshotter error: ", err)
		}
		defer s3file.Close()
		ctx := aws.BackgroundContext()
		var cancelFn func()
		if config.GetInt64("aws.s3.upload-timeout-second", 5) > 0 {
			ctx, cancelFn = context.WithTimeout(ctx, time.Second*time.Duration(config.GetInt64("aws.s3.upload-timeout-second", 5)))
		}
		defer cancelFn()

		s3Path := strconv.Itoa(config.GetInt("app.seq", 1)) + "/" + book.Symbol + "/" + path.Base(s3file.Name())
		_, err = upLoader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket: aws.String(config.GetString("aws.s3.bucket", "market")),
			Key:    aws.String(s3Path),
			Body:   s3file,
		})
		if err != nil {
			common.Error("s3 upload file failed", err)
			//dogstatsd.Event("upload snapshot", "upload snapshot err:"+fmt.Sprintln(err), statsd.Error)
		}
	}

	ids, _ := GetSnapshotIds(book.Symbol)
	if len(ids) > historyNum() {
		for i := range ids {
			if i >= historyNum() {
				fileName := BuildSnapshotPath(book, ids[i])
				os.Remove(fileName)
			}
		}
	}
}

func Load(symbol string, ctype string, fromId int64) (book *match.OrderBook, err error) {
	var orderBook match.OrderBook
	orderBook.Symbol = symbol

	filePath := BuildSnapshotPath(&orderBook, fromId)
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return
	}
	dec := gob.NewDecoder(file)
	book = match.NewOrderBook()
	err = dec.Decode(book)
	if err != nil {
		return
	}

	buyOrders := book.BuySet.Values()
	for i := range buyOrders {
		order := buyOrders[i].(*match.Order)
		book.SetCache(order)
	}

	sellOrders := book.SellSet.Values()
	for i := range sellOrders {
		order := sellOrders[i].(*match.Order)
		book.SetCache(order)
	}
	return
}

func GetLastSnapshotId(symbol string) (int64, string) {
	ids, ctype := GetSnapshotIds(symbol)
	if len(ids) > 0 {
		return ids[0], ctype[0]
	} else {
		return 0, ""
	}
}

/*
func GetSnapshotIdBy(symbol string, id int64) int64 {
	ids, _ := GetSnapshotIds(symbol)
	if len(ids) > 0 {
		for i := 0; i < len(ids); i++ {
			if ids[i] < id {
				return ids[i]
			}
		}
		// l2quote镜像比matcher快会有这种情况出现
		return 0
	} else {
		return 0
	}
}
*/

// 排序后的id 最大在前
// 000060586637.BTC181228-snapshot-go
func GetSnapshotIds(symbol string) ([]int64, []string) {
	dir, _ := ioutil.ReadDir(snapshotDir())

	var ids []int64
	var ctypes []string
	for _, f := range dir {
		if strings.HasSuffix(f.Name(), "."+EndWith(symbol)) {
			strs := strings.Split(f.Name(), ".")
			id, err := strconv.ParseInt(strs[0], 10, 64)
			if err != nil || id == 0 {
				continue
			}

			ids = append(ids, id)
		}
	}
	arr := Int64Slice(ids)
	sort.Sort(arr)
	common.Info("sort arr=", arr)

	for _, id := range arr {
		for _, f := range dir {
			curId := strconv.FormatInt(id, 10)
			if strings.Contains(f.Name(), curId) {
				strs := strings.Split(f.Name(), ".")
				if len(strs) > 2 {
					ctypes = append(ctypes, strs[1])
				} else {
					ctypes = append(ctypes, "")
				}

				break
			}
		}
	}

	return ids, ctypes
}

// 检查是否存在镜像
func HaveSnapshot(symbol string) bool {
	dir, _ := ioutil.ReadDir(snapshotDir())
	for _, f := range dir {
		if strings.HasSuffix(f.Name(), "."+EndWith(symbol)) {
			return true
		}
	}
	return false
}

/*
根据传入的match result id获取exchange快照id
*/
func GetSnapshotIdsByMatchResultID(symbol string, matchResultID int64) []int64 {
	dir, _ := ioutil.ReadDir(snapshotDir())
	var ids []int64
	for _, f := range dir {
		if strings.HasSuffix(f.Name(), "."+EndWith(symbol)) {
			strs := strings.Split(f.Name(), ".")
			id, err := strconv.ParseInt(strs[0], 10, 64)
			if err != nil {
				continue
			}
			if id <= matchResultID {
				ids = append(ids, id)
			}
		}
	}
	arr := Int64Slice(ids)
	sort.Sort(arr)
	return ids
}

// basebook 撮合起点
func GetBaseOrderBookFromS3(symbol string, id int64) (baseBook *match.OrderBook, checkBook *match.OrderBook) {
	beforeKey, key := GetS3SnapshotKey(symbol, id)
	if key == "" {
		baseBook = match.InitOrderBook(0, symbol)
		return
	}

	if beforeKey == "" {
		baseBook = match.InitOrderBook(0, symbol)
	} else {
		baseBook = DownLoadFromS3(beforeKey)
	}
	checkBook = DownLoadFromS3(key)
	return
}
func DownLoadFromS3(key string) *match.OrderBook {
	book := match.NewOrderBook()
	downloader := s3manager.NewDownloader(session.Must(session.NewSession()))
	buff := &aws.WriteAtBuffer{}
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(config.GetString("aws.s3.bucket", "market")),
		Key:    aws.String(key),
	})
	if err != nil {
		common.Fatal("download error:", err)
	}
	dec := gob.NewDecoder(bytes.NewBuffer(buff.Bytes()))
	err = dec.Decode(book)
	if err != nil {
		common.Fatal("order ")
	}
	buyOrders := book.BuySet.Values()
	for i := range buyOrders {
		order := buyOrders[i].(*match.Order)
		book.SetCache(order)
	}

	sellOrders := book.SellSet.Values()
	for i := range sellOrders {
		order := sellOrders[i].(*match.Order)
		book.SetCache(order)
	}
	return book
}

// 查找s3返回小于fromid的 两个key，在查找过程中如果出错了，会直接退出程序.
func GetS3SnapshotKey(symbol string, fromId int64) (beforeKey string, key string) {
	svc := s3.New(session.Must(session.NewSession()))
	prefix := strconv.Itoa(config.GetInt("app.seq", 1)) + "/" + symbol + "/"
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(config.GetString("aws.s3.bucket", "market")),
		Prefix: aws.String(prefix),
	}

	var conkey *string = nil
	var sliceObj []*s3.Object
	for {
		if conkey != nil {
			input.ContinuationToken = conkey
		}
		result, err := svc.ListObjectsV2(input)
		if err != nil {
			common.Fatal("list object err:", err)
		}

		for _, data := range result.Contents {
			id := parseId(*data.Key)
			if fromId < id {
				goto forforBreak
			}
			sliceObj = append(sliceObj, data)
		}

		if *result.IsTruncated == true {
			conkey = result.NextContinuationToken
		} else {
			break
		}
	}
forforBreak:
	length := len(sliceObj)
	if length >= 2 {
		beforeKey = *sliceObj[length-2].Key
		key = *sliceObj[length-1].Key
		return
	} else if length == 1 {
		beforeKey = ""
		key = *sliceObj[length-1].Key
		return
	}
	beforeKey = ""
	key = ""
	return

}

func parseId(key string) int64 {
	paths := strings.Split(key, "/")
	if len(paths) != 3 {
		common.Fatal("get s3 contents error", paths, key)
	}
	names := strings.Split(paths[2], ".")
	id, err := strconv.ParseInt(names[0], 10, 64)
	if err != nil {
		common.Fatal("parse id  error", err)
	}
	return id
}
