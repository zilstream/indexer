package prices

import _ "embed"

//go:embed data/zilliqa_historical_prices.csv
var embeddedPriceCSV []byte
