package puller

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
