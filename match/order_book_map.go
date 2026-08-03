// order_book_map.go 维护全局的订单簿（OrderBook）注册表。
// 撮合引擎中每个交易对（symbol，如 btcusdt）对应一个独立的 OrderBook，
// 本文件提供以交易对为 key 的全局 map，供调度层按 symbol 查找对应的订单簿。
package match

var (
	// OrderBookMap 全局订单簿注册表：key 为交易对 symbol，value 为该交易对的订单簿。
	OrderBookMap map[string]*OrderBook
)

// InitOrderBookMap 初始化全局订单簿注册表，在服务启动时调用。
func InitOrderBookMap() map[string]*OrderBook {
	OrderBookMap = make(map[string]*OrderBook)
	return OrderBookMap
}
