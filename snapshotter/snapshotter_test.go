package snapshotter

import (
	"encoding/gob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/shopspring/decimal"
	"github.com/spf13/viper"
	"log"
	"market-match/common"
	"market-match/match"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

// ExampleMinGap 演示 MinGap 的用法，验证配置读取是否正常。
func ExampleMinGap() {
	if MinGap() <= 0 {
		log.Println(MinGap())
	}
}

// ExampleUploadToS32 演示 S3 分页列举对象的用法（已禁用，仅作参考）。
func ExampleUploadToS32() {
	return
	svc := s3.New(session.Must(session.NewSession()))
	prefix := strconv.Itoa(viper.GetInt("app.seq")) + "/" + "uceth" + "/"
	var num = new(int64)
	*num = 3
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(viper.GetString("aws.s3.bucket")),
		Prefix:  aws.String(prefix),
		MaxKeys: num,
	}
	var conkey *string = nil
	for {
		if conkey != nil {
			input.ContinuationToken = conkey
		}
		result, err := svc.ListObjectsV2(input)
		if err != nil {
			log.Println(err)
		}
		//  log.Println(result.Contents)

		for _, data := range result.Contents {
			log.Println(*data.Key)
		}
		if !*result.IsTruncated {
			break
		}
		conkey = result.NextContinuationToken
	}
}

// ExampleGetS3SnapshotKey2 演示 GetS3SnapshotKey 的用法。
func ExampleGetS3SnapshotKey2() {
	key1, key2 := GetS3SnapshotKey("uceth", 1)
	log.Println("key1 key2", key1, key2)
}

// ExampleDumpSnapshot 演示完整的快照保存流程：构造订单簿 -> 添加随机订单 -> 序列化保存。
func ExampleDumpSnapshot() {
	decimal.MarshalJSONWithoutQuotes = true
	viper.Set("aws.s3.enable", false)
	book := match.InitOrderBook(26, "btceth")
	addRandomOrder(book, 800)
	gob.Register(match.Order{})
	name := BuildSnapshotPath(book, 200)
	log.Println(name)
	dump(book, name)
}

// ExampleTestLoad 演示快照保存后加载的流程（加载部分被注释掉，仅演示保存）。
func ExampleTestLoad() {
	decimal.MarshalJSONWithoutQuotes = true
	book := match.InitOrderBook(26, "btceth")
	addRandomOrder(book, 800)
	gob.Register(match.Order{})
	name := BuildSnapshotPath(book, 200)
	dump(book, name)
	viper.Set("aws.s3.enable", false)
	//book, err := Load(book, 200)
	//if err != nil {
	//	log.Println(err)
	//}
}

// BenchmarkDumpSnapshot 基准测试快照序列化保存的性能。
func BenchmarkDumpSnapshot(b *testing.B) {
	decimal.MarshalJSONWithoutQuotes = true
	book := match.InitOrderBook(26, "btceth")
	addRandomOrder(book, 800)
	gob.Register(match.Order{})
	viper.Set("aws.s3.enable", false)
	name := BuildSnapshotPath(book, 200)
	log.Println(name)
	b.StartTimer()
	dump(book, name)
	b.StopTimer()
}

// addRandomOrder 向订单簿中添加指定数量的随机买卖订单，用于测试。
// 每轮循环添加一笔买单和一笔卖单，价格、数量、费率等均为随机值。
func addRandomOrder(book *match.OrderBook, num int) {
	var t int64
	t = 0
	for num > 0 {
		num--
		order1 := &match.Order{
			SeqId:          t,
			OrderId:        rand.Int63(),
			State:          match.PartialFilled,
			Price:          decimal.NewFromFloat(rand.Float64()),
			UnfilledAmount: decimal.New(rand.Int63(), 0),
			CircuitRate:    decimal.NewFromFloat(rand.Float64()),
			CreateAt:       rand.Int63(),
		}
		book.Cache()[t] = order1
		t++
		book.BuySet.Add(order1)
		order2 := &match.Order{
			SeqId:          t,
			OrderId:        rand.Int63(),
			State:          match.PartialFilled,
			Price:          decimal.NewFromFloat(rand.Float64()),
			UnfilledAmount: decimal.New(rand.Int63(), 0),
			CircuitRate:    decimal.NewFromFloat(rand.Float64()),
			CreateAt:       rand.Int63(),
		}
		book.SellSet.Add(order2)
		book.Cache()[t] = order2
		t++

	}
}

// ExampleGetSnapshotIds 演示 GetSnapshotIds 的用法。
func ExampleGetSnapshotIds() {
	GetSnapshotIds("btcusdt")
}

// ExampleS3 演示 S3 文件上传的基本用法。
func ExampleS3() {
	//common.LogInit(common.LogLevel)
	common.LoadConfigViper()
	file, err := os.Create("test.filestt")
	if err != nil {
		log.Println("file create :", err)
	}
	_, err = file.Write([]byte("yuchangxu use s3 test"))
	if err != nil {
		log.Println("write:", err)
	}
	file.Close()
	if err != nil {
		log.Println("sync :", err)
	}
	file, err = os.Open("test.filestt")

	_, err = upLoader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(viper.GetString("aws.s3.bucket")),
		Key:    aws.String(file.Name()),
		Body:   file,
	})
	if err != nil {
		log.Println("s3 upload file failed", err)
	}
}

// ExampleGetSnapsName 演示如何列举 S3 上指定前缀的所有快照对象。
func ExampleGetSnapsName() {

	common.LoadConfigViper()
	os.Setenv("AWS_ACCESS_KEY_ID", viper.GetString("aws.credential.access-key"))
	os.Setenv("AWS_SECRET_ACCESS_KEY", viper.GetString("aws.credential.secret-key"))
	os.Setenv("AWS_REGION", viper.GetString("aws.s3.region"))
	svc := s3.New(session.Must(session.NewSession()))
	log.Println(viper.GetString("aws.s3.bucket"))
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(viper.GetString("aws.s3.bucket")),
		Prefix: aws.String("2/uceth"),
	}
	result, err := svc.ListObjectsV2(input)
	log.Println(err)
	for _, obj := range result.Contents {
		log.Println(obj)
		log.Println(*obj.Key)
	}
	log.Println("===============================:", *result.KeyCount)
	log.Println("===============================:", *result.IsTruncated)
}
