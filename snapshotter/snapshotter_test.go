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

func ExampleMinGap() {
	if MinGap() <= 0 {
		log.Println(MinGap())
	}
}

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

func ExampleGetS3SnapshotKey2() {
	key1, key2 := GetS3SnapshotKey("uceth", 1)
	log.Println("key1 key2", key1, key2)
}

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

func ExampleGetSnapshotIds() {
	GetSnapshotIds("btcusdt")
}

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
