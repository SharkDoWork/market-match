package common

import (
	"github.com/shopspring/decimal"
	"math/rand"
	"time"
)

var UPPRECISION decimal.Decimal
var LOWPRECISION decimal.Decimal

func init() {
	UPPRECISION = decimal.New(1, 6)
	LOWPRECISION = decimal.New(1, -6)

	rand.Seed(time.Now().UnixNano())
}
