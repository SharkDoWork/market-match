package puller

// 本文件中的测试/示例代码当前全部被注释停用。
// 保留内容展示了 puller 的典型用法：初始化数据库连接后通过 GoPuller
// 按 id 游标持续拉取订单，以及 ExistSymbolInDb / GetMinIdFromDb 等
// 辅助查询函数的调用方式。启用前需先准备好配置与数据库环境。
//
//func ExampleGoPuller() {
//	Init()
//	initDbInfo()
//	ch := make(chan *match.Order, 5000)
//	GoPuller(ch, "xxxhkd", 0)
//	for {
//		select {
//		case exOrder := <-ch:
//			log.Printf("read from channel :%d", exOrder.SeqId)
//		}
//	}
//}
//
//func ExampleInit() {
//	//common.LoadConfigViper()
//	Init()
//	if DataSourceName == "" {
//		log.Panic()
//	}
//	if Prepare == "" {
//		log.Panic()
//	}
//	if DB == nil {
//		log.Panic()
//	}
//	if dbStmt == nil {
//		log.Panic()
//	}
//}
//
//func TestGoPuller(t *testing.T) {
//	ExampleGoPuller()
//}
//
//func ExampleExistSymbolInDb() {
//	ExistSymbolInDb("bchbtc")
//}
//
//func ExampleGetMinIdFromDb() {
//	//common.LoadConfigViper()
//	Init()
//	initDbInfo()
//	id := GetMinIdFromDb()
//	log.Println("select id ", id)
//}
//
//func ExampleGoPuller2() {
//	//common.LoadConfigViper()
//	Init()
//	initDbInfo()
//
//	var orders []*match.Order
//	symbol := "ethusdt"
//	results, err := dbStmt.Query(1, symbol)
//	if err != nil {
//		common.Error("exe sql error:", err)
//		return
//	}
//	log.Println(results)
//
//	var circuitRate float64
//	results.Next()
//	var orderIntType int32
//	order := &match.Order{
//		State: match.Submitted,
//	}
//	results.Scan(&order.SeqId, &orderIntType, &order.OrderId,
//		&order.UnfilledAmount, &order.Price, &circuitRate, &order.CreateAt)
//
//	order.CircuitRate = decimal.NewFromFloat(circuitRate)
//	log.Println("rate :", circuitRate)
//	setOrderType(symbol, order, orderIntType)
//	orders = append(orders, order)
//	log.Println(orders)
//	return
//}
