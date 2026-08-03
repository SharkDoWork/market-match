// Package snapshotter 负责订单簿（OrderBook）快照的定期保存与启动恢复。
// 快照是撮合引擎容灾的关键机制：定期将内存中的订单簿序列化到磁盘/S3，
// 服务重启时可从最近快照恢复订单簿状态，避免数据丢失。
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
	gob.Register(match.Order{}) // 注册 Order 类型，使 gob 能正确编解码
}

// Int64Slice 实现 sort.Interface，用于对快照 ID 进行倒序排序（大的在前）。
type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Less(i, j int) bool { return s[i] > s[j] } // 倒序，大的在前面
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort is a convenience method.
func (s Int64Slice) Sort() {
	sort.Sort(s)
}

var upLoader *s3manager.Uploader

// Init 初始化 AWS S3 上传器。从配置中读取 AWS 凭证和区域，
// 只有配置 aws.s3.enable 为 true 时才创建上传器。
func Init() {
	os.Setenv("AWS_ACCESS_KEY_ID", config.GetString("aws.credential.access-key", ""))
	os.Setenv("AWS_SECRET_ACCESS_KEY", config.GetString("aws.credential.secret-key", ""))
	os.Setenv("AWS_REGION", config.GetString("aws.s3.region", ""))

	if config.GetBool("aws.s3.enable", false) {
		upLoader = s3manager.NewUploader(session.Must(session.NewSession()))
	}
}

// MinGap 返回两次快照之间的最小撮合序号间隔。
// 只有当订单簿的 FromId 与上次快照的 FromId 差值超过此值时，才触发新快照。
func MinGap() int64 {
	return config.GetInt64("snapshot.min-gap", 100000)
}

// EndWith 返回快照文件名的后缀部分，格式为 "symbol-snapshot-go"。
func EndWith(symbol string) string {
	return symbol + config.GetString("snapshot.ends-with", "-snapshot-go")
}

// historyNum 返回本地保留的历史快照数量，超出数量的旧快照会被删除。
func historyNum() int {
	return config.GetInt("snapshot.n-history", 10)
}

// snapshotDir 返回快照文件存储目录。
func snapshotDir() string {
	return config.GetString("snapshot.dir", "./sp/")
}

// BuildSnapshotPath 根据订单簿和撮合序号生成快照文件路径。
// 文件名格式：{dir}/{12位零填充fromId}.{symbol}-snapshot-go
// 12个0可以处理100年pro站的id量
func BuildSnapshotPath(book *match.OrderBook, fromId int64) string {
	fromIdStr := fmt.Sprintf("%012d", fromId)
	dir := snapshotDir()
	return dir + string(os.PathSeparator) + fromIdStr + "." + EndWith(book.Symbol)
}

// DumpSnapshotChan 创建一个用于接收订单簿快照请求的 channel，并启动后台协程处理快照。
// 撮合引擎通过向该 channel 发送 OrderBook 来触发快照，实现异步快照不阻塞撮合。
// 后台协程每 10 秒检查一次 channel 积压情况，收到快照请求后立即执行 dumpSnapshot。
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

// dumpSnapshot 根据订单簿当前 FromId 生成快照文件路径并执行序列化保存。
func dumpSnapshot(book *match.OrderBook) {
	pathName := BuildSnapshotPath(book, book.FromId)
	dump(book, pathName)
}

// PathExists 检查指定路径的文件是否存在。
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

// BuildSnapshotPathTmp 生成临时快照文件路径。
// 快照先写入临时文件，完成后再重命名为正式文件，避免写入过程中被读取到不完整数据。
// 12个0可以处理100年pro站的id量
func BuildSnapshotPathTmp(book *match.OrderBook) string {
	//fromIdStr := fmt.Sprintf("%012d", fromId)
	dir := snapshotDir()
	return dir + string(os.PathSeparator) + "TMP@" + EndWith(book.Symbol) + "@TMP"
}

// dump 将订单簿序列化为 gob 格式并保存到指定路径。
// 流程：先写入临时文件 -> 关闭文件 -> 重命名为正式文件 -> 上传到 S3 -> 清理过期历史快照。
// 使用临时文件+重命名的方式保证快照文件的原子性，避免读到写了一半的文件。
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

// UploadToS3 将本地快照文件上传到 AWS S3，并清理本地超出保留数量的历史快照。
// S3 路径格式：{app.seq}/{symbol}/{文件名}
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

	// 清理本地历史快照，只保留最近 historyNum 个
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

// Load 从本地快照文件恢复订单簿。
// 读取指定 symbol 和 fromId 对应的快照文件，用 gob 解码重建 OrderBook，
// 并将买卖订单重新加入缓存（SetCache）以恢复订单索引。
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

	// gob 解码后订单在 BuySet/SellSet 中，但缓存索引丢失，需要重建
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

// GetLastSnapshotId 获取指定交易对最新的快照 ID 和类型。
// 返回 ID 最大的快照（即最近一次的快照）。
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

// GetSnapshotIds 扫描快照目录，返回指定交易对的所有快照 ID（倒序，最大在前）和对应的类型。
// 快照文件名格式：{fromId}.{symbol}-snapshot-go
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

// HaveSnapshot 检查指定交易对是否存在本地快照文件。
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
// GetSnapshotIdsByMatchResultID 返回所有快照 ID 小于等于指定撮合结果 ID 的快照列表（倒序）。
// 用于根据撮合进度找到可用来恢复的快照。
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

// GetBaseOrderBookFromS3 从 S3 下载用于恢复的基准订单簿和校验订单簿。
// baseBook 是撮合起点（小于 id 的最近快照），checkBook 是用于校验的下一个快照。
// 如果找不到合适的快照，则返回空订单簿。
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

// DownLoadFromS3 从 S3 下载指定 key 的快照文件并反序列化为 OrderBook。
// 下载后同样重建订单缓存索引。
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

// GetS3SnapshotKey 在 S3 上查找小于等于 fromId 的最近两个快照 key。
// 返回 beforeKey（更早的快照，作为恢复起点）和 key（更近的快照，作为校验）。
// 查找过程中如果出错会直接退出程序（common.Fatal）。
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

// parseId 从 S3 对象 key 中解析出快照 ID。
// key 格式：{app.seq}/{symbol}/{fromId}.{symbol}-snapshot-go
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
