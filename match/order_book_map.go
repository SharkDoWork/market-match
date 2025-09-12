package match

var (
	OrderBookMap map[string]*OrderBook
)

func InitOrderBookMap() map[string]*OrderBook {
	OrderBookMap = make(map[string]*OrderBook)
	return OrderBookMap
}
